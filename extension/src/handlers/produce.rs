//! `0 Produce`: bytes stored as received — CRC checked, offsets stamped, never re-encoded.

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Isolation {
    Shared,
    PerPartition,
}

fn build(
    req: &ProduceRequest,
    store: &mut dyn LogStore,
    authz: &crate::acl::Authz,
    isolation: Isolation,
) -> Result<ProduceOutcome, Abandon> {
    let mut topics = Vec::with_capacity(req.topic_data.len());
    let mut appended: Vec<(u32, i32)> = Vec::new();
    let mut wrote_bytes = 0usize;

    for topic_data in &req.topic_data {
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
        for pd in &topic_data.partition_data {
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
            // Failure after an earlier batch appended keeps the earlier rows; the resend lands twice.
            let Some(tid) = topic_id else {
                partitions.push(failed(
                    pd.index,
                    &AppendError::Store(StoreError::UnknownTopicOrPartition),
                    &name,
                ));
                continue;
            };

            // Before any append: a mid-append failure would abandon the pass and duplicate records on replay.
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

            let result = match isolation {
                Isolation::PerPartition => crate::dbtx::atomically(
                    || append_partition(store, tid, pd.index, pd.records.as_ref()),
                    |_| AppendError::Aborted,
                ),
                Isolation::Shared => {
                    match append_partition(store, tid, pd.index, pd.records.as_ref()) {
                        Ok(done) => Ok(done),
                        Err(_) => return Err(Abandon::PartitionFailed),
                    }
                }
            };
            partitions.push(match result {
                Ok(done) => {
                    if done.wrote {
                        appended.push((tid, pd.index));
                        wrote_bytes += pd.records.as_ref().map(|r| r.len()).unwrap_or(0);
                    }
                    PartitionProduceResponse {
                        index: pd.index,
                        error_code: ErrorCode::None.code(),
                        base_offset: done.base_offset,
                        // -1 is Kafka's "not set"; we do not rewrite timestamps.
                        log_append_time_ms: -1,
                        log_start_offset: store.log_start_offset(tid, pd.index).unwrap_or(0),
                        ..Default::default()
                    }
                }
                Err(e) => failed(pd.index, &e, &name),
            });
        }

        topics.push(TopicProduceResponse {
            name,
            topic_id: kafgres_codec::Uuid(uuid),
            partition_responses: partitions,
            ..Default::default()
        });
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

fn append_partition(
    store: &mut dyn LogStore,
    topic: u32,
    partition: i32,
    records: Option<&kafgres_codec::bytes::Bytes>,
) -> Result<Appended, AppendError> {
    let records = match records {
        Some(r) if !r.is_empty() => r,
        _ => return Ok(Appended::nothing(store.high_watermark(topic, partition)?)),
    };

    // Offsets are stamped inside `LogStore::append` under the partition lock; a read here could be stale.
    let mut base: Option<i64> = None;
    let mut wrote = false;
    for item in BatchIter::new(records.clone()) {
        let view = item.map_err(AppendError::Batch)?;

        let producer_id = view.producer_id();
        let epoch = view.producer_epoch();
        let first_seq = view.base_sequence();
        // Not `first_seq + delta`: the client's sequence counter wraps through zero at int32.
        let last_seq = producer::increment_sequence(first_seq, view.last_offset_delta());

        if producer_id == NO_PRODUCER_ID || first_seq == NO_SEQUENCE {
            let assigned = store.append(topic, partition, raw_batch(&view), None)?;
            base.get_or_insert(assigned);
            wrote = true;
            continue;
        }

        match producer::check(producer_id, epoch, first_seq, last_seq, topic, partition)
            .map_err(|e| AppendError::ProducerState(e.to_string()))?
        {
            // A retry: answer with the offset it got the first time, so the resend is a no-op.
            SequenceCheck::Duplicate { base_offset } => {
                base.get_or_insert(base_offset);
            }
            SequenceCheck::OutOfOrder { expected, got } => {
                return Err(AppendError::OutOfOrderSequence { expected, got })
            }
            SequenceCheck::Fenced { current_epoch } => {
                return Err(AppendError::FencedEpoch {
                    current: current_epoch,
                })
            }
            SequenceCheck::Append => {
                let assigned = store.append(topic, partition, raw_batch(&view), None)?;
                // Recorded in the same transaction as the append, only after it succeeds.
                producer::record(
                    producer_id, epoch, topic, partition, first_seq, last_seq, assigned,
                )
                .map_err(|e| AppendError::ProducerState(e.to_string()))?;
                if view.is_transactional() && !view.is_control() {
                    crate::storage::pmeta::note_txn_first_offset(
                        producer_id,
                        topic,
                        partition,
                        assigned,
                    )
                    .map_err(|e| AppendError::ProducerState(e.to_string()))?;
                }
                base.get_or_insert(assigned);
                wrote = true;
            }
        }
    }

    match base {
        Some(b) => Ok(Appended { base_offset: b, wrote }),
        None => Ok(Appended::nothing(store.high_watermark(topic, partition)?)),
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
