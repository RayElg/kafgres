//! `0 Produce`: bytes stored as received — CRC checked, offsets stamped, never re-encoded.

use std::collections::HashMap;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::produce_request::ProduceRequest;
use kafgres_codec::generated::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};

use kafgres_codec::records::{BatchIter, RecordBatch};

use super::HandlerError;
use crate::meta;
use crate::producer::{self, SequenceCheck, NO_PRODUCER_ID, NO_SEQUENCE};
use crate::storage::{LogStore, RawBatch, StoreError};

pub const ACKS_NONE: i16 = 0;

thread_local! {
    /// Batches this request appended, by (topic entry, partition entry, batch): a rollback
    /// cannot remove them from a segment file. `None` on the table engine, where it does.
    static LANDED: std::cell::RefCell<Option<HashMap<(usize, usize, usize), i64>>> =
        const { std::cell::RefCell::new(None) };
}

fn landed(at: (usize, usize), batch: usize) -> Option<i64> {
    LANDED.with(|l| l.borrow().as_ref().and_then(|m| m.get(&(at.0, at.1, batch)).copied()))
}

fn remember_landed(at: (usize, usize), batch: usize, offset: i64) {
    LANDED.with(|l| {
        if let Some(m) = l.borrow_mut().as_mut() {
            m.insert((at.0, at.1, batch), offset);
        }
    });
}

fn any_landed(at: (usize, usize)) -> bool {
    LANDED.with(|l| {
        l.borrow()
            .as_ref()
            .is_some_and(|m| m.keys().any(|k| (k.0, k.1) == at))
    })
}

#[derive(Debug)]
enum AppendError {
    Store(StoreError),
    Batch(kafgres_codec::records::BatchError),
    ProducerState(String),
    OutOfOrderSequence { expected: i32, got: i32 },
    FencedEpoch { current: i16 },
    TooLarge { bytes: usize, limit: i64 },
    NullKeyOnCompacted,
    /// The savepoint around this partition was rolled back; nothing it wrote landed.
    Aborted,
    /// A resend of a batch whose window row never learned its offset.
    DuplicateUnknownOffset,
}

impl AppendError {
    fn error_code(&self) -> ErrorCode {
        match self {
            AppendError::Store(e) => e.error_code(),
            AppendError::Batch(e) => e.error_code(),
            // RequestTimedOut, not KAFKA_STORAGE_ERROR: that code tells the client the leader is offline.
            AppendError::ProducerState(_) | AppendError::Aborted => ErrorCode::RequestTimedOut,
            AppendError::OutOfOrderSequence { .. } => ErrorCode::OutOfOrderSequenceNumber,
            AppendError::FencedEpoch { .. } => ErrorCode::InvalidProducerEpoch,
            AppendError::TooLarge { .. } => ErrorCode::MessageTooLarge,
            AppendError::NullKeyOnCompacted => ErrorCode::InvalidRecord,
            // Kafka's answer for a duplicate it cannot place; clients treat it as success.
            AppendError::DuplicateUnknownOffset => ErrorCode::DuplicateSequenceNumber,
        }
    }
}

impl From<StoreError> for AppendError {
    fn from(e: StoreError) -> Self {
        AppendError::Store(e)
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Store(e) => write!(f, "{e}"),
            AppendError::Batch(e) => write!(f, "{e}"),
            AppendError::ProducerState(m) => write!(f, "producer state: {m}"),
            AppendError::OutOfOrderSequence { expected, got } => {
                write!(f, "sequence {got}, expected {expected}")
            }
            AppendError::FencedEpoch { current } => {
                write!(f, "fenced by producer epoch {current}")
            }
            AppendError::TooLarge { bytes, limit } => {
                write!(f, "record batch is {bytes} bytes, over max.message.bytes of {limit}")
            }
            AppendError::NullKeyOnCompacted => {
                write!(f, "a compacted topic requires every record to have a key")
            }
            AppendError::Aborted => write!(f, "append aborted (lock or statement timeout)"),
            AppendError::DuplicateUnknownOffset => {
                write!(f, "duplicate of a batch whose offset was never recorded")
            }
        }
    }
}

pub struct ProduceOutcome {
    pub response: Option<ProduceResponse>,
    pub appended: Vec<(u32, i32)>,
    pub bytes: usize,
}

struct Appended {
    base_offset: i64,
    wrote: bool,
}

impl Appended {
    fn nothing(base_offset: i64) -> Self {
        Appended {
            base_offset,
            wrote: false,
        }
    }
}

enum Abandon {
    PartitionFailed,
    Fatal(HandlerError),
}

impl From<HandlerError> for Abandon {
    fn from(e: HandlerError) -> Self {
        Abandon::Fatal(e)
    }
}

/// Produce in one savepoint for the whole request; on a partition failure, roll back and
pub fn handle(
    req: &ProduceRequest,
    store: &mut dyn LogStore,
    authz: &crate::acl::Authz,
) -> Result<ProduceOutcome, HandlerError> {
    // Segment engine, non-transactional only: on the table engine the commit flush is what
    // makes a record durable, and a transactional produce writes `kafgres_txn_partitions`,
    // which has no log representation to rebuild from. The batch attribute bit is checked,
    // not just the request-level `transactional_id`: a batch can carry the bit under a null id.
    if crate::relaxed_produce_commit()
        && req.transactional_id.is_none()
        && !any_transactional_batch(req)
        && crate::storage_engine_guc() == "segment"
    {
        crate::dbtx::relax_commit_durability().map_err(|e| {
            HandlerError::Internal(format!("could not relax commit durability: {e}"))
        })?;
    }

    let segment = crate::storage_engine_guc() == "segment";
    LANDED.with(|l| *l.borrow_mut() = segment.then(HashMap::new));

    let attempt = crate::dbtx::atomically(
        || build(req, store, authz, Isolation::Shared),
        |_| Abandon::PartitionFailed,
    );

    match attempt {
        Ok(outcome) => Ok(outcome),
        Err(Abandon::Fatal(e)) => Err(e),
        Err(Abandon::PartitionFailed) => build(req, store, authz, Isolation::PerPartition).map_err(|e| {
            match e {
                Abandon::Fatal(e) => e,
                Abandon::PartitionFailed => {
                    HandlerError::Internal("produce isolation escaped".to_string())
                }
            }
        }),
    }
}

/// Whether any batch in the request carries the transactional attribute bit (header read, no decode).
fn any_transactional_batch(req: &ProduceRequest) -> bool {
    use kafgres_codec::records::BatchIter;
    req.topic_data.iter().any(|t| {
        t.partition_data.iter().any(|p| {
            p.records.as_ref().is_some_and(|bytes| {
                // Every batch, not just the first: a records field can carry several
                // concatenated batches. An undecodable batch is treated as transactional.
                BatchIter::new(bytes.clone())
                    .any(|b| b.map(|v| v.is_transactional()).unwrap_or(true))
            })
        })
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Isolation {
    Shared,
    PerPartition,
}

/// An idempotent batch's identity in the producer's window.
#[derive(Clone, Copy)]
struct Idem {
    producer_id: i64,
    epoch: i16,
    first_seq: i32,
    last_seq: i32,
    transactional: bool,
}

/// What to do with one batch, decided before anything is written.
enum Decision {
    /// No producer id or sequence: append, no window entry.
    Plain,
    /// New for this producer: reserve a window row, append, stamp the offset in.
    Append(Idem),
    /// Exact resend of a retained batch: answer with its original offset.
    Duplicate { base_offset: i64 },
    /// Exact resend of a batch earlier in this same request, by its position.
    DuplicateOf(usize),
}

struct PartitionPlan {
    tid: u32,
    index: i32,
    batches: Vec<(RecordBatch, Decision)>,
}

/// A partition still to be written, and where its response goes.
struct Pending {
    topic: usize,
    slot: usize,
    plan: PartitionPlan,
}

/// Produce runs in phases across the whole request, not partition by partition, since
/// `LogStore::append` is a file write no savepoint undoes:
///
/// 1. **Plan**: decode and sequence-check every batch. Reads only, so a failed partition
///    has nothing to undo.
/// 2. **Reserve**: write the window rows (`base_offset = -1`) and transaction registration
///    for every partition. If this fails, nothing has been appended.
/// 3. **Append and stamp**: append each batch, then stamp its offset into its reserved
///    row. A storage error mid-partition keeps what landed (stamped, so a resend answers
///    as a duplicate) and drops the reservations of what did not.
///
/// A Postgres ERROR in phase 3 (a statement or lock timeout, a cancel, resource
/// exhaustion) unwinds the savepoint but not a segment file: the redo finds what already
/// landed in `LANDED` and records it rather than appending it again.
fn build(
    req: &ProduceRequest,
    store: &mut dyn LogStore,
    authz: &crate::acl::Authz,
    isolation: Isolation,
) -> Result<ProduceOutcome, Abandon> {
    let mut topics: Vec<TopicProduceResponse> = Vec::with_capacity(req.topic_data.len());
    let mut pending: Vec<Pending> = Vec::new();
    let mut entries: Vec<((String, i32), (usize, usize))> = Vec::new();

    for (ti, topic_data) in req.topic_data.iter().enumerate() {
        // From v13 only `topic_id` is set; name-only resolution rejects modern producers.
        let resolved = meta::resolve_topic(&topic_data.name, &topic_data.topic_id.0)
            .map_err(|e| {
                pgrx::log!("kafgres: produce topic lookup failed: {e}");
                HandlerError::Internal(format!("topic lookup: {e}"))
            })?;
        let topic_id = resolved.as_ref().map(|r| r.topic_id);
        let name = resolved
            .as_ref()
            .map(|r| r.name.clone())
            .unwrap_or_else(|| topic_data.name.clone());
        let uuid = resolved
            .as_ref()
            .map(|r| r.uuid)
            .unwrap_or(topic_data.topic_id.0);
        let max_message_bytes = resolved
            .as_ref()
            .map(|r| r.max_message_bytes)
            .unwrap_or(crate::config::DEFAULT_MAX_MESSAGE_BYTES);
        let compacted = resolved.as_ref().map(|r| r.compacted).unwrap_or(false);

        let denied = authz
            .check(
                crate::acl::Operation::Write,
                crate::acl::ResourceType::Topic,
                &name,
            )
            .err();

        let mut partitions = Vec::with_capacity(topic_data.partition_data.len());
        for (pi, pd) in topic_data.partition_data.iter().enumerate() {
            entries.push(((name.clone(), pd.index), (ti, pi)));
            if let Some(code) = denied {
                partitions.push(PartitionProduceResponse {
                    index: pd.index,
                    error_code: code.code(),
                    base_offset: -1,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                    ..Default::default()
                });
                continue;
            }
            let Some(tid) = topic_id else {
                partitions.push(failed(
                    pd.index,
                    &AppendError::Store(StoreError::UnknownTopicOrPartition),
                    &name,
                ));
                continue;
            };
            if let Some(err) = oversized(pd.records.as_ref(), max_message_bytes) {
                partitions.push(failed(pd.index, &err, &name));
                continue;
            }
            if compacted {
                if let Some(err) = null_keyed(pd.records.as_ref()) {
                    partitions.push(failed(pd.index, &err, &name));
                    continue;
                }
            }

            // Phase 1: a partition that fails here never reaches the store.
            match plan_partition(tid, pd.index, pd.records.as_ref()) {
                Ok(plan) => {
                    // Phase 3 fills this in.
                    partitions.push(PartitionProduceResponse::default());
                    pending.push(Pending {
                        topic: ti,
                        slot: pi,
                        plan,
                    });
                }
                Err(e) => partitions.push(failed(pd.index, &e, &name)),
            }
        }

        topics.push(TopicProduceResponse {
            name,
            topic_id: kafgres_codec::Uuid(uuid),
            partition_responses: partitions,
            ..Default::default()
        });
    }

    // Kafka keys partitions by topic-partition: a repeated one is decided by its last entry,
    // or both copies of an idempotent batch would pass the window check and land.
    let mut last: HashMap<&(String, i32), (usize, usize)> = HashMap::new();
    for (key, at) in &entries {
        last.insert(key, *at);
    }
    let echoes: Vec<((usize, usize), (usize, usize))> = entries
        .iter()
        .filter(|(key, at)| last[key] != *at)
        .map(|(key, at)| (*at, last[key]))
        .collect();
    pending.retain(|p| !echoes.iter().any(|(at, _)| *at == (p.topic, p.slot)));

    let mut appended: Vec<(u32, i32)> = Vec::new();
    let mut wrote_bytes = 0usize;

    match isolation {
        Isolation::Shared => {
            // Phase 2 for every partition before phase 3 for any.
            for p in &pending {
                if let Err(e) = reserve_partition(&p.plan) {
                    pgrx::log!("kafgres: produce reservation failed, retrying per partition: {e}");
                    return Err(Abandon::PartitionFailed);
                }
            }
            for p in pending {
                let outcome = write_partition(store, &p.plan, (p.topic, p.slot));
                finish(&mut topics, &mut appended, &mut wrote_bytes, req, &p, store, outcome);
            }
        }
        Isolation::PerPartition => {
            // One savepoint per partition. Phase 3 answers inside `Ok`: a storage error
            // must not roll the savepoint back over batches that already landed.
            for p in pending {
                let at = (p.topic, p.slot);
                let attempt = |store: &mut dyn LogStore| {
                    crate::dbtx::atomically(
                        || {
                            reserve_partition(&p.plan)?;
                            Ok(write_partition(store, &p.plan, at))
                        },
                        |_| AppendError::Aborted,
                    )
                    .and_then(|inner| inner)
                };
                let mut outcome = attempt(store);
                // Record batches that landed before the ERROR, or a resend appends them again.
                if matches!(outcome, Err(AppendError::Aborted)) && any_landed(at) {
                    outcome = attempt(store);
                }
                finish(&mut topics, &mut appended, &mut wrote_bytes, req, &p, store, outcome);
            }
        }
    }
    for ((topic, slot), (kt, ks)) in echoes {
        let answer = topics[kt].partition_responses[ks].clone();
        topics[topic].partition_responses[slot] = answer;
    }

    // acks=0: the client never reads the reply, so sending one desynchronises the connection.
    if req.acks == ACKS_NONE {
        return Ok(ProduceOutcome {
            response: None,
            appended,
            bytes: wrote_bytes,
        });
    }

    Ok(ProduceOutcome {
        response: Some(ProduceResponse {
            responses: topics,
            throttle_time_ms: 0,
            ..Default::default()
        }),
        appended,
        bytes: wrote_bytes,
    })
}

fn finish(
    topics: &mut [TopicProduceResponse],
    appended: &mut Vec<(u32, i32)>,
    wrote_bytes: &mut usize,
    req: &ProduceRequest,
    p: &Pending,
    store: &mut dyn LogStore,
    outcome: Result<Appended, AppendError>,
) {
    let tid = p.plan.tid;
    let index = p.plan.index;
    let response = match outcome {
        Ok(done) => {
            if done.wrote {
                appended.push((tid, index));
                *wrote_bytes += req.topic_data[p.topic].partition_data[p.slot]
                    .records
                    .as_ref()
                    .map(|r| r.len())
                    .unwrap_or(0);
            }
            PartitionProduceResponse {
                index,
                error_code: ErrorCode::None.code(),
                base_offset: done.base_offset,
                // -1 is Kafka's "not set"; we do not rewrite timestamps.
                log_append_time_ms: -1,
                log_start_offset: store.log_start_offset(tid, index).unwrap_or(0),
                ..Default::default()
            }
        }
        Err(e) => failed(index, &e, &topics[p.topic].name),
    };
    topics[p.topic].partition_responses[p.slot] = response;
}

/// Phase 1: decode every batch and decide it against the producer's window. Nothing is
/// written here.
fn plan_partition(
    tid: u32,
    index: i32,
    records: Option<&kafgres_codec::bytes::Bytes>,
) -> Result<PartitionPlan, AppendError> {
    let mut batches: Vec<(RecordBatch, Decision)> = Vec::new();
    let Some(records) = records.filter(|r| !r.is_empty()) else {
        return Ok(PartitionPlan {
            tid,
            index,
            batches,
        });
    };

    for item in BatchIter::new(records.clone()) {
        let view = item.map_err(AppendError::Batch)?;
        let producer_id = view.producer_id();
        let first_seq = view.base_sequence();
        if producer_id == NO_PRODUCER_ID || first_seq == NO_SEQUENCE {
            batches.push((view, Decision::Plain));
            continue;
        }
        let idem = Idem {
            producer_id,
            epoch: view.producer_epoch(),
            first_seq,
            // Not `first_seq + delta`: the client's sequence counter wraps through zero at int32.
            last_seq: producer::increment_sequence(first_seq, view.last_offset_delta()),
            transactional: view.is_transactional() && !view.is_control(),
        };

        // A batch earlier in this request is not in the window yet; the batch after it is
        // decided against it here, the way `check` would decide once it was recorded.
        let prior = batches.iter().enumerate().rev().find_map(|(i, (_, d))| match d {
            Decision::Append(p) if p.producer_id == producer_id => Some((i, *p)),
            _ => None,
        });
        let decision = match prior {
            Some((_, prev)) => decide_after(&batches, prev, idem)?,
            None => match producer::check(
                producer_id, idem.epoch, idem.first_seq, idem.last_seq, tid, index,
            )
            .map_err(|e| AppendError::ProducerState(e.to_string()))?
            {
                SequenceCheck::Append => Decision::Append(idem),
                SequenceCheck::Duplicate { base_offset } if base_offset < 0 => {
                    return Err(AppendError::DuplicateUnknownOffset)
                }
                SequenceCheck::Duplicate { base_offset } => Decision::Duplicate { base_offset },
                SequenceCheck::OutOfOrder { expected, got } => {
                    return Err(AppendError::OutOfOrderSequence { expected, got })
                }
                SequenceCheck::Fenced { current_epoch } => {
                    return Err(AppendError::FencedEpoch {
                        current: current_epoch,
                    })
                }
            },
        };
        batches.push((view, decision));
    }

    Ok(PartitionPlan {
        tid,
        index,
        batches,
    })
}

/// `producer::check`'s rules, applied against the newest batch this request already
/// accepted for the producer rather than against the table.
fn decide_after(
    batches: &[(RecordBatch, Decision)],
    prev: Idem,
    idem: Idem,
) -> Result<Decision, AppendError> {
    if idem.epoch < prev.epoch {
        return Err(AppendError::FencedEpoch {
            current: prev.epoch,
        });
    }
    if idem.epoch > prev.epoch {
        if idem.first_seq != 0 {
            return Err(AppendError::OutOfOrderSequence {
                expected: 0,
                got: idem.first_seq,
            });
        }
        return Ok(Decision::Append(idem));
    }
    if let Some(i) = batches.iter().position(|(_, d)| match d {
        Decision::Append(p) => {
            p.producer_id == idem.producer_id
                && p.epoch == idem.epoch
                && p.first_seq == idem.first_seq
                && p.last_seq == idem.last_seq
        }
        _ => false,
    }) {
        return Ok(Decision::DuplicateOf(i));
    }
    if producer::in_sequence(prev.last_seq, idem.first_seq) {
        Ok(Decision::Append(idem))
    } else {
        Err(AppendError::OutOfOrderSequence {
            expected: producer::increment_sequence(prev.last_seq, 1),
            got: idem.first_seq,
        })
    }
}

/// Phase 2: the window rows and the transaction registration, offsets still unknown.
fn reserve_partition(plan: &PartitionPlan) -> Result<(), AppendError> {
    for (_, decision) in &plan.batches {
        let Decision::Append(idem) = decision else { continue };
        producer::record(
            idem.producer_id, idem.epoch, plan.tid, plan.index, idem.first_seq, idem.last_seq, -1,
        )
        .map_err(|e| AppendError::ProducerState(e.to_string()))?;
        if idem.transactional {
            crate::storage::pmeta::register_txn_partition(
                idem.producer_id, idem.epoch, plan.tid, plan.index, -1,
            )
            .map_err(|e| AppendError::ProducerState(e.to_string()))?;
        }
    }
    Ok(())
}

/// Phase 3: append, then stamp each batch's offset into the row reserved for it.
fn write_partition(
    store: &mut dyn LogStore,
    plan: &PartitionPlan,
    at: (usize, usize),
) -> Result<Appended, AppendError> {
    let mut base: Option<i64> = None;
    let mut wrote = false;
    let mut assigned: Vec<Option<i64>> = vec![None; plan.batches.len()];

    for (i, (view, decision)) in plan.batches.iter().enumerate() {
        let idem = match decision {
            Decision::Plain => None,
            Decision::Append(idem) => Some(idem),
            Decision::Duplicate { base_offset } => {
                base.get_or_insert(*base_offset);
                continue;
            }
            Decision::DuplicateOf(j) => {
                base.get_or_insert(assigned[*j].unwrap_or(-1));
                continue;
            }
        };
        let offset = match landed(at, i) {
            Some(offset) => offset,
            None => match store.append(plan.tid, plan.index, raw_batch(view), None) {
                Ok(offset) => {
                    remember_landed(at, i, offset);
                    offset
                }
                Err(e) => {
                    // Release the rest: reserved with -1, they would answer a resend wrongly.
                    release_reservations(plan, i)?;
                    return Err(e.into());
                }
            },
        };
        assigned[i] = Some(offset);
        base.get_or_insert(offset);
        wrote = true;
        if let Some(idem) = idem {
            // Appended but unstamped answers a resend as a duplicate, which it is.
            let stamped = producer::stamp(
                idem.producer_id, plan.tid, plan.index, idem.first_seq, offset,
            )
            .map_err(|e| AppendError::ProducerState(e.to_string()))
            .and_then(|()| {
                if idem.transactional {
                    crate::storage::pmeta::stamp_txn_partition(
                        idem.producer_id, plan.tid, plan.index, offset,
                    )?;
                }
                Ok(())
            });
            if let Err(e) = stamped {
                release_reservations(plan, i + 1)?;
                return Err(e);
            }
        }
    }

    match base {
        Some(b) => Ok(Appended { base_offset: b, wrote }),
        None => Ok(Appended::nothing(store.high_watermark(plan.tid, plan.index)?)),
    }
}

/// Drop the unstamped window rows from batch `from` onward.
fn release_reservations(plan: &PartitionPlan, from: usize) -> Result<(), AppendError> {
    for (_, decision) in plan.batches.iter().skip(from) {
        if let Decision::Append(idem) = decision {
            producer::release(idem.producer_id, plan.tid, plan.index, idem.first_seq)
                .map_err(|e| AppendError::ProducerState(e.to_string()))?;
        }
    }
    Ok(())
}

fn failed(index: i32, e: &AppendError, topic: &str) -> PartitionProduceResponse {
    pgrx::log!("kafgres: produce to {topic}-{index} failed: {e}");
    PartitionProduceResponse {
        index,
        error_code: e.error_code().code(),
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        ..Default::default()
    }
}

/// Header-only: `as_bytes()` includes the 12-byte batch prefix — hence Kafka's default of 1048588.
fn oversized(
    records: Option<&kafgres_codec::bytes::Bytes>,
    max_message_bytes: i64,
) -> Option<AppendError> {
    let records = records?;
    if records.is_empty() {
        return None;
    }
    for item in BatchIter::new(records.clone()) {
        let Ok(view) = item else { return None };
        let bytes = view.as_bytes().len();
        if bytes as i64 > max_message_bytes {
            return Some(AppendError::TooLarge { bytes, limit: max_message_bytes });
        }
    }
    None
}

/// The first null-keyed record, if the topic is compacted; Kafka answers `INVALID_RECORD` (verified 4.1.0).
fn null_keyed(records: Option<&kafgres_codec::bytes::Bytes>) -> Option<AppendError> {
    let records = records?;
    if records.is_empty() {
        return None;
    }
    for item in BatchIter::new(records.clone()) {
        let Ok(view) = item else { return None };
        // Control batches carry the transaction marker, not user data; Kafka exempts them.
        if view.is_control() {
            continue;
        }
        let Ok(iter) = view.records_decompressed() else {
            return None;
        };
        for record in iter {
            let Ok(record) = record else { return None };
            if record.key.is_none() {
                return Some(AppendError::NullKeyOnCompacted);
            }
        }
    }
    None
}

fn raw_batch(view: &RecordBatch) -> RawBatch {
    RawBatch {
        bytes: view.as_bytes().to_vec(),
        record_count: view.record_count(),
        last_offset_delta: view.last_offset_delta(),
        max_timestamp: view.max_timestamp(),
        producer_id: view.producer_id(),
        producer_epoch: view.producer_epoch(),
        base_sequence: view.base_sequence(),
        is_transactional: view.is_transactional(),
        is_control: view.is_control(),
    }
}
