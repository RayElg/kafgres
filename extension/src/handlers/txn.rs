//! `24 AddPartitionsToTxn` and `26 EndTxn` — the transaction coordinator: remember which

use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use kafgres_codec::generated::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponse, AddPartitionsToTxnTopicResult,
};
use kafgres_codec::generated::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafgres_codec::generated::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use kafgres_codec::generated::end_txn_request::EndTxnRequest;
use kafgres_codec::generated::txn_offset_commit_request::TxnOffsetCommitRequest;
use kafgres_codec::generated::txn_offset_commit_response::{
    TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafgres_codec::generated::end_txn_response::EndTxnResponse;
use kafgres_codec::generated::write_txn_markers_request::WriteTxnMarkersRequest;
use kafgres_codec::generated::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    WriteTxnMarkersResponse,
};
use kafgres_codec::records::{build_control_batch, RecordBatch};

use super::HandlerError;
use crate::clock::now_millis;
use crate::storage::RawBatch;

/// Why a transactional request's `(transactional_id, producer_id, epoch)` cannot act.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    /// No transactional id at all: the coordinator APIs are meaningless without one.
    NoTransactionalId,
    /// No producer id we handed out for this transactional id: the mapping is wrong.
    UnknownProducer,
    /// Not the current epoch. Behind it is an instance a newer one has fenced; ahead of
    /// it is an epoch this coordinator never issued. Both are refused, as Kafka refuses
    /// them: accepting an epoch ahead would begin a transaction and stamp markers under
    /// an epoch nobody holds.
    Stale,
}

impl Verdict {
    /// `PRODUCER_FENCED` exists from the second version of each coordinator API; an older
    /// request version gets `INVALID_PRODUCER_EPOCH`, as Kafka answers it.
    fn code(self, version: i16) -> ErrorCode {
        match self {
            Verdict::NoTransactionalId => ErrorCode::InvalidRequest,
            Verdict::UnknownProducer => ErrorCode::InvalidProducerIdMapping,
            Verdict::Stale if version >= 2 => ErrorCode::ProducerFenced,
            Verdict::Stale => ErrorCode::InvalidProducerEpoch,
        }
    }
}

/// Proof that [`verdict`] accepted the pair, under locks held until the transaction ends.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Admitted {
    producer_id: i64,
    epoch: i16,
}

/// Check a transactional request against the producer we handed out for its transactional
/// id: the id must be the one mapped to that transactional id, and its epoch exactly the
/// current one. `Ok` means it may act.
fn verdict(
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
) -> Result<Result<Admitted, Verdict>, HandlerError> {
    judge(transactional_id, producer_id, epoch, None)
        .map(|(verdict, _)| verdict)
        .map_err(|e| HandlerError::Internal(e.to_string()))
}

/// [`verdict`], plus whether `partition` is in the ongoing transaction at `epoch`. Locks the
/// transaction row, then reads the producer row under a share lock (the sweep's order), so
/// no abort or fence lands between this check and the caller's write.
fn judge(
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    partition: Option<(u32, i32)>,
) -> Result<(Result<Admitted, Verdict>, bool), pgrx::spi::Error> {
    if transactional_id.is_empty() {
        return Ok((Err(Verdict::NoTransactionalId), false));
    }
    lock_transactional_id(transactional_id)?;
    let txn = lock_txn(producer_id)?;
    let (topic_id, index) = partition.map_or((None, None), |(t, p)| (Some(t as i32), Some(p)));
    let row: Option<String> = crate::plan::get_one_mut(
        "SELECT (SELECT p.producer_epoch || '|' || (p.transactional_id = $2)::text || '|'
                        || EXISTS (SELECT 1 FROM kafgres_txn_partitions tp
                                    WHERE tp.producer_id = p.producer_id
                                      AND tp.topic_id = $3::oid AND tp.partition = $4)::text
                   FROM kafgres_producers p
                  WHERE p.producer_id = $1
                    FOR SHARE)",
        &[producer_id.into(), transactional_id.into(), topic_id.into(), index.into()],
    )?;
    // A NULL transactional id (a plain idempotent producer) concatenates to NULL.
    let Some(row) = row else { return Ok((Err(Verdict::UnknownProducer), false)) };
    let mut it = row.split('|');
    let current: i32 = it.next().and_then(|x| x.parse().ok()).unwrap_or(-1);
    let mapped = it.next() == Some("true");
    let listed = it.next() == Some("true");
    Ok(admission(producer_id, epoch, current, mapped, listed, txn.as_deref()))
}

/// What `judge` and `judge_held` make of the rows they read, in one place so the two cannot
/// drift: `txn` is the producer's `state|epoch` in `kafgres_txns`, if it has a row.
fn admission(
    producer_id: i64,
    epoch: i16,
    current: i32,
    mapped: bool,
    listed: bool,
    txn: Option<&str>,
) -> (Result<Admitted, Verdict>, bool) {
    if !mapped {
        return (Err(Verdict::UnknownProducer), false);
    }
    if (epoch as i32) != current {
        return (Err(Verdict::Stale), false);
    }
    let added = listed && txn == Some(format!("ongoing|{epoch}").as_str());
    (Ok(Admitted { producer_id, epoch }), added)
}

/// [`judge`] for a caller that already holds `hold_for_produce`'s locks in this transaction: the
/// three of them are not asked for again, and the transaction's state comes in the same statement.
fn judge_held(
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    topic_id: u32,
    partition: i32,
) -> Result<(Result<Admitted, Verdict>, bool), pgrx::spi::Error> {
    if transactional_id.is_empty() {
        return Ok((Err(Verdict::NoTransactionalId), false));
    }
    let row: Option<String> = crate::plan::get_one_mut(
        "SELECT (SELECT p.producer_epoch || '|' || (p.transactional_id = $2)::text || '|'
                        || EXISTS (SELECT 1 FROM kafgres_txn_partitions tp
                                    WHERE tp.producer_id = p.producer_id
                                      AND tp.topic_id = $3::oid AND tp.partition = $4)::text
                        || '|' || COALESCE((SELECT t.state || '|' || t.producer_epoch
                                              FROM kafgres_txns t
                                             WHERE t.producer_id = p.producer_id), '')
                   FROM kafgres_producers p
                  WHERE p.producer_id = $1)",
        &[producer_id.into(), transactional_id.into(), (topic_id as i32).into(), partition.into()],
    )?;
    let Some(row) = row else { return Ok((Err(Verdict::UnknownProducer), false)) };
    let mut it = row.splitn(4, '|');
    let current: i32 = it.next().and_then(|x| x.parse().ok()).unwrap_or(-1);
    let mapped = it.next() == Some("true");
    let listed = it.next() == Some("true");
    let txn = it.next().filter(|t| !t.is_empty());
    Ok(admission(producer_id, epoch, current, mapped, listed, txn))
}

const TXN_ID_LOCK_CLASS: i32 = 0x6b67_7478;

/// First in every lock order: this, then the `kafgres_txns` row, then the `kafgres_producers`
/// row. It also covers a first transaction, which has no row to lock yet.
pub(super) fn lock_transactional_id(transactional_id: &str) -> Result<(), pgrx::spi::Error> {
    crate::plan::run(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        &[TXN_ID_LOCK_CLASS.into(), transactional_id.into()],
    )
}

fn lock_txn(producer_id: i64) -> Result<Option<String>, pgrx::spi::Error> {
    crate::plan::get_one_mut(
        "SELECT (SELECT state || '|' || producer_epoch FROM kafgres_txns
                  WHERE producer_id = $1 FOR UPDATE)",
        &[producer_id.into()],
    )
}

thread_local! {
    /// The producers `hold_for_produce` has locked for the request in flight, so
    /// `verify_held_produce` can tell it is not running without the locks.
    static HELD: std::cell::RefCell<Vec<i64>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Start of a produce request: nothing is held yet.
pub fn forget_held() {
    HELD.with(|h| h.borrow_mut().clear());
}

fn lock_producer(transactional_id: &str, producer_id: i64) -> Result<(), pgrx::spi::Error> {
    lock_transactional_id(transactional_id)?;
    lock_txn(producer_id)?;
    crate::plan::run(
        "SELECT FROM kafgres_producers WHERE producer_id = $1 FOR SHARE",
        &[producer_id.into()],
    )
}

/// `judge`'s locks, taken by a produce before its savepoint so a rollback keeps them.
pub fn hold_for_produce(transactional_id: &str, producer_id: i64) -> Result<(), pgrx::spi::Error> {
    lock_producer(transactional_id, producer_id)?;
    HELD.with(|h| {
        let mut h = h.borrow_mut();
        if !h.contains(&producer_id) {
            h.push(producer_id);
        }
    });
    Ok(())
}

pub fn handle_add_partitions(
    req: &AddPartitionsToTxnRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<AddPartitionsToTxnResponse, HandlerError> {
    // v4+ is the broker-to-broker batched shape, and this cluster has no peers. An empty
    // `results_by_transaction` would read as "all partitions verified", so refuse the request whole.
    if version >= 4 {
        return Ok(AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::InvalidRequest.code(),
            results_by_transaction: Vec::new(),
            results_by_topic_v3_and_below: Vec::new(),
            ..Default::default()
        });
    }

    let (txn_id, producer_id, epoch, topics) = {
        (
            req.v3_and_below_transactional_id.clone(),
            req.v3_and_below_producer_id,
            req.v3_and_below_producer_epoch,
            req.v3_and_below_topics.clone(),
        )
    };

    let respond = |code_of: &dyn Fn(usize, i32) -> ErrorCode| AddPartitionsToTxnResponse {
        throttle_time_ms: 0,
        error_code: 0,
        results_by_topic_v3_and_below: topics
            .iter()
            .enumerate()
            .map(|(i, t)| AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|p| AddPartitionsToTxnPartitionResult {
                        partition_index: *p,
                        partition_error_code: code_of(i, *p).code(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        results_by_transaction: Vec::new(),
        ..Default::default()
    };

    // Authorized before the fence, as Kafka does. A denied topic fails the whole request: nothing
    // is added, and the other partitions answer UNKNOWN_TOPIC_OR_PARTITION or OPERATION_NOT_ATTEMPTED.
    if let Err(code) = authz.check(
        crate::acl::Operation::Write,
        crate::acl::ResourceType::TransactionalId,
        &txn_id,
    ) {
        return Ok(respond(&|_, _| code));
    }
    let mut denied: Vec<Option<ErrorCode>> = Vec::with_capacity(topics.len());
    // Per topic, its id and partition count; a count of 0 for one that does not exist.
    let mut found: Vec<(Option<u32>, i32)> = Vec::with_capacity(topics.len());
    for topic in &topics {
        let refused = authz
            .check(crate::acl::Operation::Write, crate::acl::ResourceType::Topic, &topic.name)
            .err();
        let id = match refused {
            Some(_) => None,
            None => crate::meta::topic_id_by_name(&topic.name)
                .map_err(|e| HandlerError::Internal(e.to_string()))?,
        };
        let count = match id {
            Some(id) => crate::meta::partition_count(id)
                .map_err(|e| HandlerError::Internal(e.to_string()))?,
            None => 0,
        };
        denied.push(refused);
        found.push((id, count));
    }
    let unknown = |i: usize, p: i32| p < 0 || p >= found[i].1;
    if denied.iter().any(Option::is_some) {
        return Ok(respond(&|i, p| match denied[i] {
            Some(code) => code,
            None if unknown(i, p) => ErrorCode::UnknownTopicOrPartition,
            None => ErrorCode::OperationNotAttempted,
        }));
    }

    let admitted = verdict(&txn_id, producer_id, epoch)?;
    let code = admitted.err().map_or(ErrorCode::None, |v| v.code(version));

    if let Ok(admitted) = admitted {
        begin_or_continue(admitted, &txn_id).map_err(|e| HandlerError::Internal(e.to_string()))?;
        for (i, (topic, (id, _))) in topics.iter().zip(&found).enumerate() {
            let Some(topic_id) = id else { continue };
            // A row naming a nonexistent partition is one no marker append can reach.
            for partition in topic.partitions.iter().filter(|&&p| !unknown(i, p)) {
                add_partition(producer_id, *topic_id, *partition)
                    .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
        }
    }

    Ok(respond(&|_, _| code))
}

/// Mark the transaction ongoing at the admitted epoch; a new one clears the previous one's rows.
pub fn begin_or_continue(admitted: Admitted, txn_id: &str) -> Result<(), pgrx::spi::Error> {
    let Admitted { producer_id, epoch } = admitted;
    // Locks the producer row and refreshes `last_ts`, so the retention sweep skips it
    // until this commits.
    Spi::run_with_args(
        "UPDATE kafgres_producers SET last_ts = now() WHERE producer_id = $1",
        &[producer_id.into()],
    )?;
    let beginning: bool = Spi::get_one_with_args::<bool>(
        "SELECT (SELECT COALESCE(
                    (SELECT state <> 'ongoing' FROM kafgres_txns WHERE producer_id = $1),
                    true))",
        &[producer_id.into()],
    )?
    .unwrap_or(true);
    if beginning {
        for table in ["kafgres_txn_partitions", "kafgres_txn_offsets"] {
            Spi::run_with_args(
                &format!("DELETE FROM {table} WHERE producer_id = $1"),
                &[producer_id.into()],
            )?;
        }
    }

    Spi::run_with_args(
        "INSERT INTO kafgres_txns
                (producer_id, producer_epoch, transactional_id, state, started_at)
         VALUES ($1, $2, $3, 'ongoing', $4)
         ON CONFLICT (producer_id) DO UPDATE
            SET producer_epoch = EXCLUDED.producer_epoch,
                state = 'ongoing',
                -- Only when a transaction actually *begins*. A producer reuses its
                -- row, so leaving `started_at` alone means its second transaction
                -- inherits the first one's clock and the expiry sweep aborts it while
                -- it is live — a working producer failing for no reason it can see.
                -- Already 'ongoing' means this is a later `AddPartitionsToTxn` inside
                -- the same transaction, which must not extend its deadline either.
                started_at = CASE WHEN kafgres_txns.state = 'ongoing'
                                  THEN kafgres_txns.started_at
                                  ELSE EXCLUDED.started_at END,
                offsets_partitions = CASE WHEN kafgres_txns.state = 'ongoing'
                                          THEN kafgres_txns.offsets_partitions
                                          ELSE '{}' END,
                forced_result = CASE WHEN kafgres_txns.state = 'ongoing'
                                     THEN kafgres_txns.forced_result END",
        &[
            producer_id.into(),
            (epoch as i32).into(),
            txn_id.into(),
            now_millis().into(),
        ],
    )
}

fn add_partition(producer_id: i64, topic_id: u32, partition: i32) -> Result<(), pgrx::spi::Error> {
    Spi::run_with_args(
        "INSERT INTO kafgres_txn_partitions (producer_id, topic_id, partition)
         VALUES ($1, $2::oid, $3) ON CONFLICT DO NOTHING",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
    )
}

/// Kafka's default `offsets.topic.num.partitions`.
const OFFSETS_TOPIC_PARTITIONS: i32 = 50;

/// The `__consumer_offsets` partition Kafka keeps `group` in: `Utils.abs(groupId.hashCode())`
/// (Java's string hash, over UTF-16) modulo the partition count.
pub fn offsets_partition(group: &str) -> i32 {
    let h = group.encode_utf16().fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(u as i32));
    if h == i32::MIN { 0 } else { h.abs() % OFFSETS_TOPIC_PARTITIONS }
}

fn add_offsets_partition(admitted: Admitted, group: &str) -> Result<(), pgrx::spi::Error> {
    Spi::run_with_args(
        "UPDATE kafgres_txns SET offsets_partitions = array_append(offsets_partitions, $2)
          WHERE producer_id = $1 AND NOT $2 = ANY(offsets_partitions)",
        &[admitted.producer_id.into(), offsets_partition(group).into()],
    )
}

fn offsets_partition_added(admitted: Admitted, group: &str) -> Result<bool, pgrx::spi::Error> {
    Ok(Spi::get_one_with_args::<bool>(
        "SELECT (SELECT state = 'ongoing' AND producer_epoch = $2 AND $3 = ANY(offsets_partitions)
                   FROM kafgres_txns WHERE producer_id = $1)",
        &[
            admitted.producer_id.into(),
            (admitted.epoch as i32).into(),
            offsets_partition(group).into(),
        ],
    )?
    .unwrap_or(false))
}

/// A transactional batch checked against its transaction before append (KIP-890).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProduceTxn {
    Added,
    /// Produce v12+: the append adds the partition and begins the transaction if needed.
    AddsPartition(Admitted),
    Refused(ErrorCode),
}

const PRODUCE_ADDS_PARTITION_FROM: i16 = 12;
const PRODUCE_ABORTABLE_FROM: i16 = 11;

/// Codes as apache/kafka 4.3.1 answers them: an unmapped producer is
/// INVALID_PRODUCER_ID_MAPPING, any other epoch INVALID_PRODUCER_EPOCH, and a partition
/// not added INVALID_TXN_STATE (TRANSACTION_ABORTABLE at v11). Its locks last through the append.
///
/// For a produce that ran `hold_for_produce` for this producer first, as `produce::handle` does
/// for every transactional producer in its request: only the check is made, in one statement.
/// A producer that was not held is held here, and that is a bug in the caller.
pub fn verify_held_produce(
    transactional_id: Option<&str>,
    producer_id: i64,
    epoch: i16,
    topic_id: u32,
    partition: i32,
    version: i16,
) -> Result<ProduceTxn, pgrx::spi::Error> {
    let Some(txn_id) = transactional_id else {
        return Ok(ProduceTxn::Refused(ErrorCode::TransactionalIdAuthorizationFailed));
    };
    if !HELD.with(|h| h.borrow().contains(&producer_id)) {
        debug_assert!(false, "producer {producer_id} verified without hold_for_produce");
        // Not recorded as held: this runs inside a savepoint, whose rollback releases the
        // locks, so the next call must take them again.
        lock_producer(txn_id, producer_id)?;
    }
    Ok(produce_verdict(judge_held(txn_id, producer_id, epoch, topic_id, partition)?, version))
}

fn produce_verdict(
    judged: (Result<Admitted, Verdict>, bool),
    version: i16,
) -> ProduceTxn {
    match judged {
        (Err(Verdict::Stale), _) => ProduceTxn::Refused(ErrorCode::InvalidProducerEpoch),
        (Err(_), _) => ProduceTxn::Refused(ErrorCode::InvalidProducerIdMapping),
        (Ok(_), true) => ProduceTxn::Added,
        (Ok(admitted), false) if version >= PRODUCE_ADDS_PARTITION_FROM => {
            ProduceTxn::AddsPartition(admitted)
        }
        (Ok(_), false) if version >= PRODUCE_ABORTABLE_FROM => {
            ProduceTxn::Refused(ErrorCode::TransactionAbortable)
        }
        (Ok(_), false) => ProduceTxn::Refused(ErrorCode::InvalidTxnState),
    }
}

/// Commit or abort: write a marker to every partition the transaction touched. Markers
///
/// At v5 the epoch also rotates (KIP-890 part two): the response carries a fresh epoch so
/// the producer's next transaction starts without an `InitProducerId` round trip.
pub fn handle_end_txn(
    req: &EndTxnRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<EndTxnResponse, HandlerError> {
    // The v5 fields are ignorable and default to -1, which a client may read as an epoch,
    // so every early return below carries the caller's own pair.
    let unchanged = |code: ErrorCode| EndTxnResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
        ..Default::default()
    };

    // Checked before the retry lookup as well: a retired pair is no exception.
    if req.transactional_id.is_empty() {
        return Ok(unchanged(Verdict::NoTransactionalId.code(version)));
    }
    if let Err(code) = authz.check(
        crate::acl::Operation::Write,
        crate::acl::ResourceType::TransactionalId,
        &req.transactional_id,
    ) {
        return Ok(unchanged(code));
    }

    // Checked before the fence: after a v5 rotation a retry carries the retired epoch, which
    // looks exactly like a zombie. KIP-890 answers such a retry NONE with the current pair.
    if version >= 5 && crate::transaction_version() >= 2 {
        match completed_retry(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            req.committed,
        )? {
            Retry::Of(id, epoch) => {
                return Ok(EndTxnResponse {
                    throttle_time_ms: 0,
                    error_code: ErrorCode::None.code(),
                    producer_id: id,
                    producer_epoch: epoch,
                    ..Default::default()
                })
            }
            Retry::OutcomeMismatch => return Ok(unchanged(ErrorCode::InvalidTxnState)),
            Retry::No => {}
        }
    }

    if let Err(v) = verdict(&req.transactional_id, req.producer_id, req.producer_epoch)? {
        return Ok(unchanged(v.code(version)));
    }

    // Rotation is gated on the finalized transaction.version as well as v5: a client that
    // picked v5 without implementing KIP-890 would be fenced by its own next request.
    let rotating = version >= 5 && crate::transaction_version() >= 2;

    // Lost to the expiry sweep, a newer instance, or an operator via `WriteTxnMarkers`: in
    match finish_transaction(req.producer_id, req.producer_epoch, req.committed)? {
        Finish::NotOurs | Finish::Overridden => {
            return Ok(unchanged(ErrorCode::InvalidProducerEpoch))
        }
        Finish::NotOpen { last } => {
            // Ending a transaction that was never begun, or one that has already ended,
            // is a state-machine answer, never fencing. A commit with nothing open is
            // INVALID_TXN_STATE at every version. Before KIP-890 a same-outcome replay of
            // the completed transaction is a retry and gets NONE. Under KIP-890 the epoch
            // has already rotated, so a retry was caught above; an abort with nothing
            // open succeeds and rotates the epoch again, which is how a client aborts
            // after a failed send.
            if !rotating {
                return Ok(unchanged(if last == Some(req.committed) {
                    ErrorCode::None
                } else {
                    ErrorCode::InvalidTxnState
                }));
            }
            if req.committed {
                return Ok(unchanged(ErrorCode::InvalidTxnState));
            }
            Spi::run_with_args(
                "UPDATE kafgres_txns SET state = 'aborted', offsets_partitions = '{}'
                  WHERE producer_id = $1",
                &[req.producer_id.into()],
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        }
        Finish::Ended => {}
    }

    if !rotating {
        return Ok(unchanged(ErrorCode::None));
    }

    // The pair may carry a different producer id when the epoch hit its ceiling.
    let (producer_id, producer_epoch) = crate::producer::bump_epoch_for_next_txn(req.producer_id, req.committed)
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

    Ok(EndTxnResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        producer_id,
        producer_epoch,
        ..Default::default()
    })
}

/// Is this `EndTxn` a retry of the one that retired `epoch`, asking for the same outcome?
/// The pair must be the retired one under the transactional id that owns it; an older epoch
/// is a fenced instance. `InitProducerId` and a fence clear the record, so a retry arriving
/// after a takeover is fenced.
fn completed_retry(
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    committed: bool,
) -> Result<Retry, HandlerError> {
    // A mismatch is INVALID_TXN_STATE, not fencing. The row is found by the retired id,
    // which is the live one unless the epoch ceiling moved the transactional id.
    let found: Option<String> = Spi::get_one_with_args(
        "SELECT (SELECT CASE WHEN p.retired_committed IS NOT DISTINCT FROM $3
                             THEN p.producer_id || '|' || p.producer_epoch
                             ELSE 'mismatch' END
                   FROM kafgres_producers p
                  WHERE p.retired_producer_id = $1
                    AND p.retired_epoch = $2
                    AND p.transactional_id = $4
                    AND NOT EXISTS (SELECT 1 FROM kafgres_txns t
                                     WHERE t.producer_id = p.producer_id
                                       AND t.state = 'ongoing')
                  LIMIT 1)",
        &[
            producer_id.into(),
            (epoch as i32).into(),
            committed.into(),
            transactional_id.into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let Some(s) = found else { return Ok(Retry::No) };
    if s == "mismatch" {
        return Ok(Retry::OutcomeMismatch);
    }
    let mut it = s.split('|');
    let id = it.next().and_then(|x| x.parse().ok()).unwrap_or(producer_id);
    let ep = it.next().and_then(|x| x.parse().ok()).unwrap_or(epoch);
    Ok(Retry::Of(id, ep))
}

/// What an `EndTxn` carrying an epoch we may have retired turns out to be.
enum Retry {
    /// A genuine retry of the transaction that retired this epoch: answer with this pair.
    Of(i64, i16),
    /// The retired epoch, but asking for the opposite outcome.
    OutcomeMismatch,
    No,
}

/// What `finish_transaction` did, which is not always what it was asked to do.
#[derive(PartialEq, Eq, Debug)]
pub enum Finish {
    /// The transaction is not this caller's to end.
    NotOurs,
    Ended,
    /// Nothing is open to end: the transaction was never begun (`None`), or the previous
    /// one already completed with this outcome (`Some(committed)`). A transaction-version-2
    /// client ends transactions it only initialised, and aborts with nothing added after
    /// a failed send; fencing either would be fatal to a client making a legitimate
    /// request. The answer depends on the request version, so the caller chooses it.
    NotOpen { last: Option<bool> },
    /// Ended with the outcome an operator had already forced; the caller must not be told
    Overridden,
}

fn finish_transaction(
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<Finish, HandlerError> {
    let requested = committed;
    // Lock the transaction row and re-check under it: without the lock, the expiry sweep
    //
    // An outer join, so an open transaction whose producer row is gone can still be aborted.
    // `EndTxn` never gets here for one: `verdict` refuses it first.
    let state: Option<String> = Spi::get_one_with_args(
        "SELECT (SELECT CASE
                    WHEN p.producer_epoch > $2 THEN 'fenced'
                    WHEN t.state = 'ongoing' THEN 'ours'
                    ELSE t.state END
              FROM kafgres_txns t
              LEFT JOIN kafgres_producers p ON p.producer_id = t.producer_id
             WHERE t.producer_id = $1
               FOR UPDATE OF t)",
        &[producer_id.into(), (producer_epoch as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    match state.as_deref() {
        Some("ours") => {}
        Some("empty") => return Ok(Finish::NotOpen { last: None }),
        Some("committed") => return Ok(Finish::NotOpen { last: Some(true) }),
        Some("aborted") => return Ok(Finish::NotOpen { last: Some(false) }),
        _ => return Ok(Finish::NotOurs),
    }

    // An operator already forced a result on at least one partition; the rest must get the
    let forced: Option<bool> = Spi::get_one_with_args(
        "SELECT (SELECT forced_result FROM kafgres_txns WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let committed = forced.unwrap_or(committed);
    let overridden = matches!(forced, Some(f) if f != requested);

    // `first_offset` is read before the markers land: the append is what makes the row's
    let partitions: Vec<(u32, i32, i64)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT tp.topic_id::int, tp.partition, tp.first_offset
               FROM kafgres_txn_partitions tp
              WHERE tp.producer_id = $1
                AND EXISTS (SELECT 1 FROM kafgres_topics t WHERE t.topic_id = tp.topic_id)
              ORDER BY tp.topic_id, tp.partition",
            None,
            &[producer_id.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(t), Some(p)) = (row.get::<i32>(1)?, row.get::<i32>(2)?) {
                out.push((t as u32, p, row.get::<i64>(3)?.unwrap_or(-1)));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let mut store = crate::storage::open();
    for (topic_id, partition, first_offset) in &partitions {
        append_marker(
            &mut *store,
            producer_id,
            producer_epoch,
            *topic_id,
            *partition,
            *first_offset,
            committed,
        )?;
    }

    // Staged offsets become visible exactly with the records, or not at all: after the
    if committed {
        Spi::run_with_args(
            "INSERT INTO kafgres_offsets
                    (group_id, topic_id, partition, committed_offset,
                     committed_leader_epoch, metadata, commit_ts)
             SELECT group_id, topic_id, partition, committed_offset,
                    committed_leader_epoch, metadata, now()
               FROM kafgres_txn_offsets WHERE producer_id = $1
             ON CONFLICT (group_id, topic_id, partition) DO UPDATE SET
                committed_offset = EXCLUDED.committed_offset,
                committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                metadata = EXCLUDED.metadata,
                commit_ts = now()",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_offsets WHERE producer_id = $1",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Only once every marker is down. Clearing first would lose the partition list on a
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_partitions WHERE producer_id = $1",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Spi::run_with_args(
        "UPDATE kafgres_txns SET state = $2, offsets_partitions = '{}' WHERE producer_id = $1",
        &[
            producer_id.into(),
            if committed { "committed" } else { "aborted" }.into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    log!(
        "kafgres: transaction for producer {} {} across {} partition(s)",
        producer_id,
        if committed { "committed" } else { "aborted" },
        partitions.len()
    );

    if overridden {
        // Fence: the producer must re-initialise rather than believe it still owns a
        fence(producer_id)?;
        return Ok(Finish::Overridden);
    }
    Ok(Finish::Ended)
}

/// `InitProducerId` for a transactional id whose transaction is still open: the instance
/// that began it is gone or about to be fenced, and what it left must not be committed by
/// the new instance. The abort's markers are written inside this request, so the new epoch
/// is returned in the same answer; Kafka answers CONCURRENT_TRANSACTIONS while its markers
/// are written and NONE on the retry. Without the abort, the new instance's first commit
/// would carry the abandoned records, and a `read_committed` consumer would see them.
pub(super) fn abort_abandoned(transactional_id: &str) -> Result<(), HandlerError> {
    // By the transaction row's id, so one whose producer row is gone is found too; there
    // may then be two.
    let open: Vec<(i64, i16)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT t.producer_id, COALESCE(p.producer_epoch, t.producer_epoch)::int
               FROM kafgres_txns t
               LEFT JOIN kafgres_producers p ON p.producer_id = t.producer_id
              WHERE t.transactional_id = $1 AND t.state = 'ongoing'
                AND (p.producer_id IS NULL OR p.transactional_id = $1)
              ORDER BY t.producer_id",
            None,
            &[transactional_id.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(p), Some(e)) = (row.get::<i64>(1)?, row.get::<i32>(2)?) {
                out.push((p, e as i16));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    for (producer_id, epoch) in open {
        // At the producer's current epoch, which the caller bumps next. Every outcome is
        // acceptable: ended concurrently, already forced by an operator (which fenced the
        // producer), or aborted here.
        finish_transaction(producer_id, epoch, false)?;
        log!(
            "kafgres: aborted the open transaction of producer {producer_id} — \
             InitProducerId for '{transactional_id}' supersedes it"
        );
    }
    Ok(())
}

/// Bump a producer's epoch so its next request fails. Not optional: an unfenced producer
///
/// The retired pair is cleared too: it exists to answer a retried `EndTxn` with the
/// current epoch, and after a fence that answer would revive the instance being fenced.
fn fence(producer_id: i64) -> Result<i32, HandlerError> {
    let epoch: Option<i32> = Spi::get_one_with_args(
        "WITH up AS (
             UPDATE kafgres_producers
                SET producer_epoch = producer_epoch + 1,
                    retired_producer_id = NULL,
                    retired_epoch = NULL,
                    retired_committed = NULL
              WHERE producer_id = $1 AND producer_epoch < $2
             RETURNING producer_epoch)
         SELECT (SELECT producer_epoch FROM up)",
        &[producer_id.into(), (i16::MAX as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if let Some(e) = epoch {
        return Ok(e);
    }
    // Either the row is already gone, or it hit the smallint epoch ceiling (i16::MAX).
    // At the ceiling, retire the old producer_id and allocate a fresh one at epoch 0 so
    // the old instance is fenced without overflowing smallint.
    let at_ceiling: Option<String> = Spi::get_one_with_args(
        "WITH gone AS (
             DELETE FROM kafgres_producers
              WHERE producer_id = $1 AND producer_epoch >= $2
          RETURNING COALESCE(transactional_id, '') AS tid)
         SELECT (SELECT tid FROM gone)",
        &[producer_id.into(), (i16::MAX as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if let Some(tid) = at_ceiling {
        Spi::run_with_args(
            "DELETE FROM kafgres_producer_batches WHERE producer_id = $1",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        if !tid.is_empty() {
            let _ = crate::producer::init_producer_id(Some(&tid))
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
        }
        return Ok(0);
    }
    Ok(-1)
}

/// `25 AddOffsetsToTxn` — the transaction will also commit consumer offsets. Committed
pub fn handle_add_offsets(
    req: &AddOffsetsToTxnRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<AddOffsetsToTxnResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::Write,
        crate::acl::ResourceType::TransactionalId,
        &req.transactional_id,
    ) {
        return Ok(AddOffsetsToTxnResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            ..Default::default()
        });
    }
    if let Err(code) = authz.check(
        crate::acl::Operation::Read,
        crate::acl::ResourceType::Group,
        &req.group_id,
    ) {
        return Ok(AddOffsetsToTxnResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            ..Default::default()
        });
    }
    let code = match verdict(&req.transactional_id, req.producer_id, req.producer_epoch)? {
        Err(v) => v.code(version),
        Ok(admitted) => {
            begin_or_continue(admitted, &req.transactional_id)
                .and_then(|()| add_offsets_partition(admitted, &req.group_id))
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            ErrorCode::None
        }
    };

    Ok(AddOffsetsToTxnResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        ..Default::default()
    })
}

/// `28 TxnOffsetCommit` — stage offsets that become visible only if the transaction
///
/// At v5 this request also marks the transaction ongoing (KIP-890 part two): a
/// transaction-V2 client no longer sends `25 AddOffsetsToTxn`.
pub fn handle_txn_offset_commit(
    req: &TxnOffsetCommitRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<TxnOffsetCommitResponse, HandlerError> {
    let auth_err = authz
        .check(
            crate::acl::Operation::Write,
            crate::acl::ResourceType::TransactionalId,
            &req.transactional_id,
        )
        .err()
        .or_else(|| {
            authz
                .check(
                    crate::acl::Operation::Read,
                    crate::acl::ResourceType::Group,
                    &req.group_id,
                )
                .err()
        });
    if let Some(auth_code) = auth_err {
        let topics = req
            .topics
            .iter()
            .map(|t| TxnOffsetCommitResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| TxnOffsetCommitResponsePartition {
                        partition_index: p.partition_index,
                        error_code: auth_code.code(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        return Ok(TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics,
            ..Default::default()
        });
    }
    // Reported per partition, and a stale epoch is INVALID_PRODUCER_EPOCH at every
    // version, as Kafka answers it; the other coordinator APIs answer PRODUCER_FENCED.
    let admitted = verdict(&req.transactional_id, req.producer_id, req.producer_epoch)?;
    let code = match admitted {
        Err(Verdict::Stale) => ErrorCode::InvalidProducerEpoch,
        Err(other) => other.code(version),
        Ok(_) => ErrorCode::None,
    };

    // Before v5 the group's offsets partition must already be in the transaction (KIP-890
    // verification), checked before the group itself.
    let unverified = match admitted {
        Ok(admitted) if version < 5 && !req.group_id.is_empty() => {
            if offsets_partition_added(admitted, &req.group_id)
                .map_err(|e| HandlerError::Internal(e.to_string()))?
            {
                ErrorCode::None
            } else if version >= 4 {
                ErrorCode::TransactionAbortable
            } else {
                ErrorCode::InvalidTxnState
            }
        }
        _ => ErrorCode::None,
    };

    // KIP-447: the v3+ member fields fence a zombie consumer; before v3 they default to none.
    let group = if code == ErrorCode::None && unverified == ErrorCode::None
        && !req.group_id.is_empty()
    {
        let committer = super::offsets::Committer {
            group_id: &req.group_id,
            generation: req.generation_id,
            member_id: &req.member_id,
            instance_id: req.group_instance_id.as_deref(),
        };
        Some(
            super::offsets::validate_commit(&committer, true, version)
                .map_err(|e| HandlerError::Internal(e.to_string()))?,
        )
    } else {
        None
    };

    let mut topics = Vec::with_capacity(req.topics.len());
    let mut accepted = Vec::new();
    let mut any_partition = false;
    for topic in &req.topics {
        let topic_id = crate::meta::topic_id_by_name(&topic.name)
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        let count = match topic_id {
            Some(id) => crate::meta::partition_count(id)
                .map_err(|e| HandlerError::Internal(e.to_string()))?,
            None => 0,
        };
        let denied = authz
            .check(crate::acl::Operation::Read, crate::acl::ResourceType::Topic, &topic.name)
            .err();
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for part in &topic.partitions {
            let index = part.partition_index;
            let part_code = match (topic_id, &group) {
                // An empty transactional id is refused before any topic lookup, unless the
                // group id is empty too: Kafka answers that INVALID_GROUP_ID.
                _ if code == ErrorCode::InvalidRequest && !req.group_id.is_empty() => code,
                _ if denied.is_some() => denied.unwrap_or(ErrorCode::TopicAuthorizationFailed),
                (None, _) => ErrorCode::UnknownTopicOrPartition,
                _ if index < 0 || index >= count => ErrorCode::UnknownTopicOrPartition,
                _ if req.group_id.is_empty() => ErrorCode::InvalidGroupId,
                _ if unverified != ErrorCode::None => unverified,
                (Some(id), Some(g)) => g.code_for(id, index),
                _ => code,
            };
            any_partition |= denied.is_none() && topic_id.is_some() && (0..count).contains(&index);
            partitions.push(TxnOffsetCommitResponsePartition {
                partition_index: index,
                error_code: part_code.code(),
                ..Default::default()
            });
            if let (ErrorCode::None, Some(topic_id)) = (part_code, topic_id) {
                accepted.push((topic_id, part));
            }
        }
        topics.push(TxnOffsetCommitResponseTopic {
            name: topic.name.clone(),
            partitions,
            ..Default::default()
        });
    }

    // As apache/kafka 4.3.1: v5 begins the transaction before the group checks, so a commit
    // refused by them still leaves it ongoing; an empty group id or no existing partition
    // stops the request first.
    let begins = version >= 5 && any_partition && !req.group_id.is_empty();
    if let (Ok(admitted), true) = (admitted, begins) {
        begin_or_continue(admitted, &req.transactional_id)
            .and_then(|()| add_offsets_partition(admitted, &req.group_id))
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    for (topic_id, part) in accepted {
        Spi::run_with_args(
            "INSERT INTO kafgres_txn_offsets
                    (producer_id, group_id, topic_id, partition, committed_offset,
                     committed_leader_epoch, metadata)
             VALUES ($1, $2, $3::oid, $4, $5, $6, $7)
             ON CONFLICT (producer_id, group_id, topic_id, partition) DO UPDATE SET
                committed_offset = EXCLUDED.committed_offset,
                committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                metadata = EXCLUDED.metadata",
            &[
                req.producer_id.into(),
                req.group_id.clone().into(),
                (topic_id as i32).into(),
                part.partition_index.into(),
                part.committed_offset.into(),
                part.committed_leader_epoch.into(),
                part.committed_metadata.clone().into(),
            ],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }

    Ok(TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    })
}

/// Abort transactions whose producer stopped talking to us. Without this, the LSO held by
pub fn expire_stale_transactions() -> Result<usize, HandlerError> {
    use std::time::{Duration, Instant};

    // Bounded per sweep with a time budget so a backlog clears quickly without stalling Fetch.
    const MAX_PER_SWEEP: i64 = 64;
    const BUDGET: Duration = Duration::from_millis(200);
    let started = Instant::now();
    let mut total_done = 0usize;

    loop {
        let now = now_millis();
        let mut stale: Vec<(i64, i16)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT producer_id, producer_epoch FROM kafgres_txns
                  WHERE state = 'ongoing' AND $1 - started_at > timeout_ms
                  ORDER BY started_at
                  LIMIT $2",
                None,
                &[now.into(), MAX_PER_SWEEP.into()],
            )?;
            let mut out = Vec::new();
            for row in rows {
                if let (Some(p), Some(e)) = (row.get::<i64>(1)?, row.get::<i32>(2)?) {
                    out.push((p, e as i16));
                }
            }
            Ok::<_, pgrx::spi::Error>(out)
        })
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        let fetched = stale.len();
        if fetched == 0 {
            break;
        }

        // Locked one at a time in ascending producer id, the order a Produce request takes them
        // in (`hold_for_produce`): locking the whole batch oldest-first could deadlock with a
        // request carrying several transactional producers. Each expiry runs in its own
        // subtransaction so one failing producer cannot roll back or stall the rest.
        stale.sort_unstable_by_key(|&(producer_id, _)| producer_id);
        let mut batch_done = 0usize;
        for (producer_id, _) in stale {
            let res: Result<bool, HandlerError> = crate::dbtx::atomically(
                || {
                    let locked: Option<i32> = Spi::get_one_with_args(
                        "SELECT (SELECT producer_epoch FROM kafgres_txns
                                  WHERE producer_id = $1 AND state = 'ongoing'
                                    AND $2 - started_at > timeout_ms
                                    FOR UPDATE)",
                        &[producer_id.into(), now.into()],
                    )
                    .map_err(|e| HandlerError::Internal(e.to_string()))?;
                    let Some(epoch) = locked.map(|e| e as i16) else {
                        return Ok(false);
                    };
                    match finish_transaction(producer_id, epoch, false)? {
                        Finish::NotOurs | Finish::NotOpen { .. } => return Ok(false),
                        Finish::Overridden => return Ok(true),
                        Finish::Ended => {}
                    }
                    let fenced_epoch = fence(producer_id)?;
                    if fenced_epoch < 0 {
                        log!(
                            "kafgres: aborted transaction for producer {producer_id} — no EndTxn within \
                             its transaction timeout; its producer state was already gone"
                        );
                    } else {
                        log!(
                            "kafgres: aborted transaction for producer {producer_id} — no EndTxn within \
                             its transaction timeout; fenced at epoch {fenced_epoch}"
                        );
                    }
                    Ok(true)
                },
                |msg| HandlerError::Internal(msg.to_string()),
            );
            match res {
                Ok(true) => batch_done += 1,
                Ok(false) => {}
                Err(e) => {
                    log!("kafgres: failed to expire stale transaction for producer {producer_id}: {e}");
                }
            }
        }
        total_done += batch_done;
        if (fetched as i64) < MAX_PER_SWEEP || batch_done == 0 || started.elapsed() >= BUDGET {
            break;
        }
    }
    Ok(total_done)
}

/// Append one partition's control batch and, on an abort, record the range it covers.
fn append_marker(
    store: &mut dyn crate::storage::LogStore,
    producer_id: i64,
    producer_epoch: i16,
    topic_id: u32,
    partition: i32,
    first_offset: i64,
    committed: bool,
) -> Result<(), HandlerError> {
    let bytes = build_control_batch(producer_id, producer_epoch, committed);
    let view = RecordBatch::new(bytes.clone())
        .map_err(|e| HandlerError::Internal(format!("control batch: {e:?}")))?;
    let batch = RawBatch {
        bytes: bytes.to_vec(),
        record_count: view.record_count(),
        last_offset_delta: view.last_offset_delta(),
        max_timestamp: view.max_timestamp(),
        producer_id,
        producer_epoch,
        base_sequence: -1,
        is_transactional: true,
        is_control: true,
    };
    // A marker that fails to land is worse than a failed EndTxn: one partition's consumers
    let marker_offset = store
        .append(topic_id, partition, batch, None)
        .map_err(|e| HandlerError::Internal(format!("writing transaction marker: {e}")))?;

    // The abort range must be recorded where a later Fetch finds it. `first_offset < 0`
    if !committed && first_offset >= 0 {
        crate::storage::pmeta::record_aborted_txn(
            topic_id,
            partition,
            producer_id,
            first_offset,
            marker_offset,
        )
        .map_err(|e| HandlerError::Internal(format!("recording aborted range: {e}")))?;
    }
    Ok(())
}

/// Reconcile `kafgres_txn_aborted` and `kafgres_txn_partitions` against the recovered log end
/// at startup or promotion.
///
/// Handles both directions of WAL/segment skew:
/// 1. WAL ahead of segment (power cut or lagging follower):
///    - Abort-index rows wholly past `end` are deleted so reused offsets are not hidden.
///    - Abort-index rows whose data survived (`first_offset < end`) but whose ABORT marker
///      was lost (`last_offset >= end`) get a fresh ABORT control batch appended and fsynced
///      so `read_committed` consumers terminate the abort instead of hiding later commits.
/// 2. Segment ahead of WAL (failover when WAL lags the segment):
///    - Scans the recent tail for ABORT control batches missing from `kafgres_txn_aborted`
///      and inserts the missing abort-index rows so aborted data is never exposed.
pub fn reconcile_transactions(
    store: &mut dyn crate::storage::LogStore,
    topic_id: u32,
    partition: i32,
    end: i64,
) -> Result<(), HandlerError> {
    use kafgres_codec::records::{ControlBatchType, LENGTH_OFFSET};
    use std::collections::{HashMap, HashSet};

    // Reset first_offset for any ongoing transaction whose data did not survive.
    Spi::run_with_args(
        "UPDATE kafgres_txn_partitions SET first_offset = -1
          WHERE topic_id = $1::oid AND partition = $2 AND first_offset >= $3",
        &[(topic_id as i32).into(), partition.into(), end.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Drop abort-index rows whose entire range (data + marker) is past the recovered log end.
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_aborted
          WHERE topic_id = $1::oid AND partition = $2 AND first_offset >= $3",
        &[(topic_id as i32).into(), partition.into(), end.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Re-append missing ABORT markers for rows whose data survived but marker was lost.
    let missing_markers: Vec<(i64, i64, i16)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT a.producer_id, a.first_offset,
                    COALESCE(p.producer_epoch, t.producer_epoch, 0)::int
               FROM kafgres_txn_aborted a
               LEFT JOIN kafgres_producers p ON p.producer_id = a.producer_id
               LEFT JOIN kafgres_txns t ON t.producer_id = a.producer_id
              WHERE a.topic_id = $1::oid AND a.partition = $2
                AND a.first_offset < $3 AND a.last_offset >= $3
              ORDER BY a.first_offset",
            None,
            &[(topic_id as i32).into(), partition.into(), end.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(pid), Some(first), Some(epoch)) =
                (row.get::<i64>(1)?, row.get::<i64>(2)?, row.get::<i32>(3)?)
            {
                out.push((pid, first, epoch as i16));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let mut need_sync = false;
    for (producer_id, first_offset, epoch) in missing_markers {
        let bytes = build_control_batch(producer_id, epoch, false);
        let view = RecordBatch::new(bytes.clone())
            .map_err(|e| HandlerError::Internal(format!("control batch: {e:?}")))?;
        let batch = RawBatch {
            bytes: bytes.to_vec(),
            record_count: view.record_count(),
            last_offset_delta: view.last_offset_delta(),
            max_timestamp: view.max_timestamp(),
            producer_id,
            producer_epoch: epoch,
            base_sequence: -1,
            is_transactional: true,
            is_control: true,
        };
        let marker_offset = store
            .append(topic_id, partition, batch, None)
            .map_err(|e| HandlerError::Internal(format!("re-appending ABORT marker: {e}")))?;
        crate::storage::pmeta::record_aborted_txn(
            topic_id,
            partition,
            producer_id,
            first_offset,
            marker_offset,
        )
        .map_err(|e| HandlerError::Internal(format!("updating aborted range: {e}")))?;
        need_sync = true;
        log!(
            "kafgres: re-appended missing ABORT marker for producer {producer_id} on \
             topic {topic_id} partition {partition} at offset {marker_offset} \
             (aborted range [{first_offset}, {marker_offset}])"
        );
    }
    if need_sync {
        store
            .sync_partition(topic_id, partition)
            .map_err(|e| HandlerError::Internal(format!("syncing reconciled markers: {e}")))?;
    }

    // Segment ahead of WAL (e.g. async failover where WAL lagged segment replication):
    // scan the tail after window_checkpoint for ABORT control batches missing from the index.
    let log_start = store
        .log_start_offset(topic_id, partition)
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if end <= log_start {
        return Ok(());
    }
    let checkpoint: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT window_checkpoint FROM kafgres_partitions
                  WHERE topic_id = $1::oid AND partition = $2)",
        &[(topic_id as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let scan_from = checkpoint
        .unwrap_or_else(|| end.saturating_sub(100_000))
        .max(log_start)
        .min(end);
    let mut at = scan_from;
    let mut first_seen: HashMap<i64, i64> = HashMap::new();
    // Producers with a marker earlier in the scan.
    let mut ended: HashSet<i64> = HashSet::new();
    while at < end {
        let slice = store
            .read(
                topic_id,
                partition,
                at,
                4 << 20,
                crate::storage::IsolationLevel::ReadUncommitted,
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        if slice.bytes.is_empty() {
            break;
        }
        let mut pos = 0usize;
        let mut advanced_to = at;
        while pos + LENGTH_OFFSET + 4 <= slice.bytes.len() {
            let len = i32::from_be_bytes(
                slice.bytes[pos + LENGTH_OFFSET..pos + LENGTH_OFFSET + 4]
                    .try_into()
                    .expect("4 bytes"),
            );
            let total = LENGTH_OFFSET + 4 + len.max(0) as usize;
            if len <= 0 || pos + total > slice.bytes.len() {
                break;
            }
            let bytes = kafgres_codec::bytes::Bytes::copy_from_slice(&slice.bytes[pos..pos + total]);
            pos += total;
            let Ok(batch) = RecordBatch::new(bytes) else { continue };
            let batch_base = batch.base_offset();
            if batch_base >= end {
                advanced_to = end;
                break;
            }
            advanced_to = (batch_base + batch.last_offset_delta() as i64 + 1).max(advanced_to);
            let pid = batch.producer_id();
            if pid < 0 || !batch.is_transactional() {
                continue;
            }
            if !batch.is_control() {
                first_seen.entry(pid).or_insert(batch_base);
                continue;
            }
            let marked_before = !ended.insert(pid);
            match batch.control_type() {
                Some(ControlBatchType::Commit) => {
                    first_seen.remove(&pid);
                }
                Some(ControlBatchType::Abort) => {
                    let first = match first_seen.remove(&pid) {
                        Some(first) => first,
                        // Its own marker earlier in the scan: this transaction wrote nothing here.
                        None if marked_before => continue,
                        // Begun before the scan. `first_offset` is the conflict key, and an
                        // upsert there would take over another producer's entry.
                        None => {
                            let taken = Spi::get_one_with_args::<bool>(
                                "SELECT EXISTS (SELECT 1 FROM kafgres_txn_aborted
                                                 WHERE topic_id = $1::oid AND partition = $2
                                                   AND first_offset = $3)",
                                &[(topic_id as i32).into(), partition.into(), scan_from.into()],
                            )
                            .map_err(|e| HandlerError::Internal(e.to_string()))?
                            .unwrap_or(false);
                            if taken {
                                log!(
                                    "kafgres: ABORT marker for producer {pid} at {batch_base} on \
                                     topic {topic_id} partition {partition} has no abort-index \
                                     row, and its start precedes offset {scan_from}"
                                );
                                continue;
                            }
                            scan_from
                        }
                    };
                    let already = crate::storage::pmeta::aborted_at(
                        topic_id,
                        partition,
                        pid,
                        batch_base,
                    )
                    .map_err(|e| HandlerError::Internal(e.to_string()))?;
                    if !already {
                        crate::storage::pmeta::record_aborted_txn(
                            topic_id,
                            partition,
                            pid,
                            first,
                            batch_base,
                        )
                        .map_err(|e| HandlerError::Internal(e.to_string()))?;
                        log!(
                            "kafgres: reconstructed missing abort-index row for producer {pid} \
                             on topic {topic_id} partition {partition} [{first}, {batch_base}]"
                        );
                    }
                }
                _ => {}
            }
        }
        if advanced_to <= at {
            break;
        }
        at = advanced_to;
    }
    Ok(())
}

/// End **one partition** of a transaction — the protocol's own granularity, since Kafka
fn finish_partition(
    producer_id: i64,
    producer_epoch: i16,
    topic_id: u32,
    partition: i32,
    committed: bool,
) -> Result<ErrorCode, HandlerError> {
    // The same lock `finish_transaction` takes: two markers for one transaction — one
    let found: Option<String> = Spi::get_one_with_args(
        "SELECT (SELECT tp.first_offset || '|' || (t.producer_epoch <= $4)::text
                   FROM kafgres_txns t
                   JOIN kafgres_txn_partitions tp ON tp.producer_id = t.producer_id
                  WHERE t.producer_id = $1 AND tp.topic_id = $2::oid AND tp.partition = $3
                    AND t.state = 'ongoing'
                  FOR UPDATE OF t)",
        &[producer_id.into(), (topic_id as i32).into(), partition.into(), (producer_epoch as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    // The epoch is re-checked under the lock: the producer may have been fenced, and a
    // transaction at a higher epoch given this partition, since the caller looked.
    let Some(found) = found else {
        return Ok(ErrorCode::None);
    };
    let (first_offset, current) = found.split_once('|').unwrap_or(("-1", "false"));
    if current != "true" {
        return Ok(ErrorCode::InvalidProducerEpoch);
    }
    let first_offset: i64 = first_offset.parse().unwrap_or(-1);

    let mut store = crate::storage::open();
    append_marker(
        &mut *store,
        producer_id,
        producer_epoch,
        topic_id,
        partition,
        first_offset,
        committed,
    )?;
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_partitions
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // `COALESCE` so the first marker decides: the transaction ends one way, not half each.
    Spi::run_with_args(
        "UPDATE kafgres_txns SET forced_result = COALESCE(forced_result, $2)
          WHERE producer_id = $1",
        &[producer_id.into(), committed.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let remaining: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if remaining.unwrap_or(0) > 0 {
        log!(
            "kafgres: wrote a {} marker for producer {producer_id} on {topic_id}-{partition}; \
             {} partition(s) of its transaction remain",
            if committed { "COMMIT" } else { "ABORT" },
            remaining.unwrap_or(0)
        );
        return Ok(ErrorCode::None);
    }

    // Last partition: the transaction is over, so it lands exactly as `EndTxn` would.
    if committed {
        Spi::run_with_args(
            "INSERT INTO kafgres_offsets
                    (group_id, topic_id, partition, committed_offset,
                     committed_leader_epoch, metadata, commit_ts)
             SELECT group_id, topic_id, partition, committed_offset,
                    committed_leader_epoch, metadata, now()
               FROM kafgres_txn_offsets WHERE producer_id = $1
             ON CONFLICT (group_id, topic_id, partition) DO UPDATE SET
                committed_offset = EXCLUDED.committed_offset,
                committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                metadata = EXCLUDED.metadata,
                commit_ts = now()",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_offsets WHERE producer_id = $1",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Spi::run_with_args(
        "UPDATE kafgres_txns SET state = $2, offsets_partitions = '{}' WHERE producer_id = $1",
        &[
            producer_id.into(),
            if committed { "committed" } else { "aborted" }.into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    // The producer did not end this transaction and does not know it is over. Fence only
    let epoch = fence(producer_id)?;
    log!(
        "kafgres: transaction for producer {producer_id} {} by WriteTxnMarkers; \
         fenced at epoch {epoch}",
        if committed { "committed" } else { "aborted" }
    );
    Ok(ErrorCode::None)
}

/// `27 WriteTxnMarkers` — force a transaction's outcome onto named partitions: the escape
pub fn write_txn_markers(
    req: &WriteTxnMarkersRequest,
    authz: &crate::acl::Authz,
) -> Result<WriteTxnMarkersResponse, HandlerError> {
    // Each partition named costs a control batch in memory and an append, while the
    super::check_nested_len("transaction markers", "marker partitions", &req.markers, |m| {
        m.topics.iter().map(|t| t.partition_indexes.len()).sum()
    })?;

    let authorized = authz
        .check(
            crate::acl::Operation::ClusterAction,
            crate::acl::ResourceType::Cluster,
            "kafka-cluster",
        )
        .err();

    let mut markers = Vec::with_capacity(req.markers.len());
    for m in &req.markers {
        let mut topics = Vec::with_capacity(m.topics.len());
        for t in &m.topics {
            let mut partitions = Vec::with_capacity(t.partition_indexes.len());
            for p in &t.partition_indexes {
                let code = match authorized {
                    Some(c) => c,
                    None => mark_one(m.producer_id, m.producer_epoch, &t.name, *p, m.transaction_result)
                        .unwrap_or_else(|e| {
                            // Reported per partition rather than raised: the partitions
                            log!("kafgres: WriteTxnMarkers {}-{}: {e}", t.name, p);
                            ErrorCode::UnknownServerError
                        }),
                };
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index: *p,
                    error_code: code.code(),
                    ..Default::default()
                });
            }
            topics.push(WritableTxnMarkerTopicResult {
                name: t.name.clone(),
                partitions,
                ..Default::default()
            });
        }
        markers.push(WritableTxnMarkerResult {
            producer_id: m.producer_id,
            topics,
            ..Default::default()
        });
    }
    Ok(WriteTxnMarkersResponse { markers, ..Default::default() })
}

/// One partition's marker. `ErrorCode::None` on success.
fn mark_one(
    producer_id: i64,
    producer_epoch: i16,
    topic: &str,
    partition: i32,
    committed: bool,
) -> Result<ErrorCode, HandlerError> {
    let Some(topic_id) = crate::meta::topic_id_by_name(topic)
        .map_err(|e| HandlerError::Internal(e.to_string()))?
    else {
        return Ok(ErrorCode::UnknownTopicOrPartition);
    };
    let n = crate::meta::partition_count(topic_id).map_err(|e| HandlerError::Internal(e.to_string()))?;
    if partition < 0 || partition >= n {
        return Ok(ErrorCode::UnknownTopicOrPartition);
    }

    // A *newer* epoch means the transaction was already fenced and superseded; an older one
    let current: Option<i32> = Spi::get_one_with_args(
        "SELECT (SELECT producer_epoch FROM kafgres_producers WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    match current {
        None => return Ok(ErrorCode::InvalidProducerEpoch),
        Some(c) if (producer_epoch as i32) < c => return Ok(ErrorCode::InvalidProducerEpoch),
        _ => {}
    }

    // The partition must actually be in this producer's transaction: a stray marker appends
    let ongoing: Option<bool> = Spi::get_one_with_args(
        "SELECT (SELECT true FROM kafgres_txn_partitions
                  WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3)",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if ongoing != Some(true) {
        return Ok(ErrorCode::InvalidTxnState);
    }

    finish_partition(producer_id, producer_epoch, topic_id, partition, committed)
}
