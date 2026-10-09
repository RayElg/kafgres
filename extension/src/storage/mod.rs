//! Storage boundary: every read or write of log data goes through [`LogStore`].

pub mod cursor;
pub mod pmeta;
pub mod segment;
pub mod table;

use kafgres_codec::ErrorCode;

/// Open the configured engine — the only place a concrete engine is constructed.
pub fn open() -> Box<dyn LogStore> {
    match crate::storage_engine_guc().as_str() {
        "table" => Box::new(table::TableStore::new()),
        "segment" => Box::new(segment::SegmentStore::new()),
        // Unreachable in the worker, which checks the GUC at startup; reachable from SQL.
        other => pgrx::error!("{}", unknown_engine(other)),
    }
}

/// Release an uncommitted produce reservation, on commit or abort. Cannot fail: it
pub fn release_pending(topic: TopicId, partition: i32) {
    segment::SegmentStore::release_pending(topic, partition);
}

/// Set in the broker worker only. A backend running `kafgres_enforce_retention()` stalls
/// only itself, so it runs a plan to the end rather than leave it to die with it.
static BOUNDED_PASSES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn bound_compaction_passes() {
    BOUNDED_PASSES.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// What is left of the current sweep's budget, shared by every partition it compacts.
static SWEEP_BUDGET: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// At the start of each retention sweep in the broker.
pub fn reset_compaction_budget() {
    SWEEP_BUDGET.store(crate::compaction_pass_bytes(), std::sync::atomic::Ordering::Relaxed);
}

/// Mapping and rewriting together, for the whole sweep: bounds the worker's pause (I7).
/// Taken whole until the pass hands the rest back, so a pass that fails spends it all.
fn compaction_budget() -> i64 {
    if BOUNDED_PASSES.load(std::sync::atomic::Ordering::Relaxed) {
        SWEEP_BUDGET.swap(0, std::sync::atomic::Ordering::Relaxed)
    } else {
        i64::MAX
    }
}

/// Whether a new plan may start: one with no budget to map would only hold a slot.
fn compaction_budget_left() -> bool {
    !BOUNDED_PASSES.load(std::sync::atomic::Ordering::Relaxed)
        || SWEEP_BUDGET.load(std::sync::atomic::Ordering::Relaxed) > 0
}

/// Hand back what a pass left of `compaction_budget()`.
fn spend_compaction_budget(left: i64) {
    if BOUNDED_PASSES.load(std::sync::atomic::Ordering::Relaxed) {
        SWEEP_BUDGET.store(left.max(0), std::sync::atomic::Ordering::Relaxed);
    }
}

/// Plans each engine keeps between passes: two at the per-map key cap fill the key budget.
/// A new plan waits for a slot rather than evict one mid-rewrite.
const MAX_PLANS: usize = 2;

/// Times the retention sweep has come back to the first topic.
static ROTATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn finish_rotation() {
    ROTATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn rotation() -> u64 {
    ROTATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// A plan untouched for a whole rotation gives up its slot: its partition was swept without
/// a pass, so it may no longer be compacted at all. Counted in rotations, not time, since a
/// rotation takes longer the more topics there are.
fn plan_is_idle(touched: u64) -> bool {
    rotation().saturating_sub(touched) >= 2
}

/// Per partition, oldest first: offsets at or below `end` have been clean since `at_ms`.
/// Process-local, so a restart only makes tombstones wait longer.
static CLEANED: std::sync::Mutex<Option<std::collections::HashMap<(TopicId, i32), Vec<(i64, i64)>>>> =
    std::sync::Mutex::new(None);

/// Entries kept per partition; dropping the oldest is conservative.
const CLEANED_HISTORY: usize = 64;

fn record_cleaned(topic: TopicId, partition: i32, end: i64, at_ms: i64) {
    let mut all = CLEANED.lock().unwrap_or_else(|e| e.into_inner());
    let history = all
        .get_or_insert_with(Default::default)
        .entry((topic, partition))
        .or_default();
    history.push((end, at_ms));
    if history.len() > CLEANED_HISTORY {
        history.remove(0);
    }
}

fn forget_cleaned(topic: TopicId, partition: i32) {
    if let Some(all) = CLEANED.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
        all.remove(&(topic, partition));
    }
}

/// What a compaction pass may do with a batch, beyond what the offset map says (I10).
struct Judge {
    topic: TopicId,
    partition: i32,
    /// Offsets below it were cleaned by a finished plan.
    dirty_from: i64,
    /// A tombstone clean since before this may go.
    tombstone_horizon: i64,
    /// A rewrite in this plan did not land: every tombstone stays.
    keep_tombstones: bool,
}

impl Judge {
    fn new(topic: TopicId, partition: i32, dirty_from: i64, delete_retention_ms: i64) -> StoreResult<Judge> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Ok(Judge {
            topic,
            partition,
            dirty_from,
            tombstone_horizon: now.saturating_sub(delete_retention_ms),
            keep_tombstones: false,
        })
    }

    /// Whether the batch's records are committed. Callers stay below the LSO, so nothing is
    /// still open.
    fn committed(&self, batch: &RecordBatchView) -> StoreResult<bool> {
        if !batch.is_transactional() || batch.is_control() {
            return Ok(true);
        }
        let base = batch.base_offset();
        if segment::is_marker_backed(batch.as_bytes()) {
            // No marker means aborted only once the snapshot sees the writer finished: the
            // live LSO can pass a commit a REPEATABLE READ snapshot does not see. The producer
            // id is the top-level xid; the marker's subtransaction may have rolled back.
            match pmeta::marker_state(self.topic, self.partition, base, batch.producer_id())? {
                (true, _) => Ok(true),
                (false, true) => Ok(false),
                (false, false) => Err(StoreError::Io(format!(
                    "transaction {} below the LSO is not yet visible to this snapshot",
                    batch.producer_id()
                ))),
            }
        } else {
            Ok(!pmeta::aborted_at(self.topic, self.partition, batch.producer_id(), base)?)
        }
    }

    /// Kafka's rule: a tombstone goes after `delete.retention.ms` in the clean section.
    fn tombstone_goes(&self, offset: i64) -> bool {
        if self.keep_tombstones || offset >= self.dirty_from {
            return false;
        }
        let all = CLEANED.lock().unwrap_or_else(|e| e.into_inner());
        let since = all
            .as_ref()
            .and_then(|m| m.get(&(self.topic, self.partition)))
            .and_then(|h| h.iter().find(|(end, _)| *end >= offset))
            .map(|(_, at)| *at);
        since.is_some_and(|at| at <= self.tombstone_horizon)
    }
}

type RecordBatchView = kafgres_codec::records::RecordBatch;

/// Validate the engine GUC without constructing anything, at worker start: callers of
pub fn check_engine_name() -> Result<(), String> {
    match crate::storage_engine_guc().as_str() {
        "table" | "segment" => Ok(()),
        other => Err(unknown_engine(other)),
    }
}

fn unknown_engine(name: &str) -> String {
    format!("unknown kafgres.storage_engine {name:?} (expected 'table' or 'segment')")
}

/// Refuse to serve a log the configured engine cannot see. The stranded log is intact
pub fn check_engine_data() -> Result<(), String> {
    if crate::allow_engine_mismatch() {
        return Ok(());
    }
    let engine = crate::storage_engine_guc();
    if engine != "table" && engine != "segment" {
        return Err(unknown_engine(&engine));
    }
    // Both engines are asked, whichever is configured, so an install holding data under
    let seg = segment::log_presence()?;
    let tab = table::log_presence()?;

    let (mine, theirs) = if engine == "table" {
        (&tab, &seg)
    } else {
        (&seg, &tab)
    };
    let Some(stranded) = theirs else {
        return Ok(());
    };
    let other = if engine == "table" { "segment" } else { "table" };

    if let Some(ours) = mine {
        return Err(format!(
            "kafgres.storage_engine is '{engine}', and this database holds a log under \
             *both* engines ({ours}, and {stranded}). Whichever engine is set, the other \
             one's log is invisible to every consumer — an empty topic, with no error. \
             There is no migration between engines, so one of the two has to be consumed \
             out and reproduced, or discarded. Set kafgres.allow_engine_mismatch = on to \
             start on '{engine}' and leave the other stranded."
        ));
    }
    Err(format!(
        "kafgres.storage_engine is '{engine}', but this database has a log written by the \
         '{other}' engine ({stranded}). That log is intact and would be invisible to every \
         consumer — an empty topic, with no error. Set kafgres.storage_engine = '{other}' \
         and restart to read it, or set kafgres.allow_engine_mismatch = on to start anyway \
         and leave it stranded."
    ))
}

pub type TopicId = u32;

/// Opaque, byte-verbatim record batch as received from a producer: never decompressed,
#[derive(Debug, Clone)]
pub struct RawBatch {
    pub bytes: Vec<u8>,
    pub record_count: i32,
    pub last_offset_delta: i32,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub is_transactional: bool,
    pub is_control: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

#[derive(Debug, Clone, Default)]
pub struct FetchSlice {
    /// Concatenated batches, wire-ready — the broker never looks inside a batch.
    pub bytes: Vec<u8>,
    pub next_offset: i64,
    /// High watermark at read time; clients derive lag from it.
    pub high_watermark: i64,
    /// Lowest offset still readable; a consumer needs it to know its position exists.
    pub log_start_offset: i64,
    /// Last Stable Offset — equal to `high_watermark` when no transaction is in flight.
    pub last_stable_offset: i64,
    pub aborted: Vec<AbortedTxn>,
    /// Set when `aborted` hit its cap: transactions first written at or after this offset
    /// may be missing from it, so a batch reaching it cannot be judged from this slice.
    pub aborted_cut_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEnd {
    /// The largest known epoch at or below the one asked about, or -1 if there is none.
    pub leader_epoch: i32,
    /// One past the last offset written under that epoch; the log end if it is current.
    pub end_offset: i64,
}

/// Retention policy for a topic; enforcement drops partitions or unlinks files, never
#[derive(Debug, Clone, Copy)]
pub struct RetentionPolicy {
    pub retention_ms: Option<i64>,
    pub retention_bytes: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct TxnContext {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

/// Storage error; each variant maps to a Kafka error code, so the same condition
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    UnknownTopicOrPartition,
    OffsetOutOfRange,
    CorruptBatch,
    /// The requested read cannot make progress within the byte cap.
    InvalidFetchSize,
    /// The leader epoch is visible in shared memory but the transaction recording it
    LeaderEpochUnsettled,
    /// Storage-level I/O or SQL failure.
    Io(String),
    NotImplemented(&'static str),
}

impl StoreError {
    pub fn error_code(&self) -> ErrorCode {
        match self {
            StoreError::UnknownTopicOrPartition => ErrorCode::UnknownTopicOrPartition,
            StoreError::OffsetOutOfRange => ErrorCode::OffsetOutOfRange,
            StoreError::CorruptBatch => ErrorCode::CorruptMessage,
            StoreError::InvalidFetchSize => ErrorCode::InvalidFetchSize,
            // Retriable; the node is mid-promotion and cannot say which epoch applies.
            StoreError::LeaderEpochUnsettled => ErrorCode::LeaderNotAvailable,
            StoreError::Io(_) => ErrorCode::KafkaStorageError,
            StoreError::NotImplemented(_) => ErrorCode::UnknownServerError,
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::UnknownTopicOrPartition => write!(f, "unknown topic or partition"),
            StoreError::OffsetOutOfRange => write!(f, "offset out of range"),
            StoreError::CorruptBatch => write!(f, "record batch failed CRC validation"),
            StoreError::InvalidFetchSize => write!(f, "fetch size cannot make progress"),
            StoreError::LeaderEpochUnsettled => {
                write!(f, "leader epoch is being raised; retry")
            }
            StoreError::Io(m) => write!(f, "storage error: {m}"),
            StoreError::NotImplemented(what) => write!(f, "not implemented yet: {what}"),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Take whatever locks the active engine needs to serve a read, within the wait `dbtx` allows.
pub fn lock_for_read(wait: crate::dbtx::LockWait) -> Result<(), pgrx::spi::Error> {
    table::lock_for_read(wait)
}

/// Given one batch's bytes, the offset of the first record holding the batch's
/// `max_timestamp`, and that timestamp (Kafka keeps the earliest offset that reached the
/// maximum); undecodable means the base offset.
pub fn offset_of_max_timestamp(bytes: kafgres_codec::bytes::Bytes) -> Option<(i64, i64)> {
    let batch = kafgres_codec::records::RecordBatch::new(bytes).ok()?;
    let base = batch.base_offset();
    let want = batch.max_timestamp();
    let base_ts = batch.base_timestamp();

    if let Ok(records) = batch.records_decompressed() {
        for record in records {
            let Ok(record) = record else { break };
            if base_ts.saturating_add(record.timestamp_delta) == want {
                return Some((base + record.offset_delta as i64, want));
            }
        }
    }
    // No record matches, or the body did not decode: report the header against the base offset.
    Some((base, want))
}

/// The first record at or after `timestamp` in the batch that reaches it, as Kafka's
/// `findOffsetByTimestamp` answers; undecodable means the base offset and the batch's maximum.
pub fn first_at_or_after(bytes: kafgres_codec::bytes::Bytes, timestamp: i64) -> Option<(i64, i64)> {
    let batch = kafgres_codec::records::RecordBatch::new(bytes).ok()?;
    let base = batch.base_offset();
    let base_ts = batch.base_timestamp();
    if let Ok(records) = batch.records_decompressed() {
        for record in records {
            let Ok(record) = record else { break };
            let ts = base_ts.saturating_add(record.timestamp_delta);
            if ts >= timestamp {
                return Some((base + record.offset_delta as i64, ts));
            }
        }
    }
    Some((base, batch.max_timestamp()))
}

pub trait LogStore: Send {
    /// Append a batch, assigning offsets; returns the base offset assigned. Offsets
    fn append(
        &mut self,
        topic: TopicId,
        partition: i32,
        batch: RawBatch,
        txn: Option<&TxnContext>,
    ) -> StoreResult<i64>;

    /// Read from `offset`, up to `max_bytes`, honouring `isolation`. Returns whole
    fn read(
        &self,
        topic: TopicId,
        partition: i32,
        offset: i64,
        max_bytes: usize,
        isolation: IsolationLevel,
    ) -> StoreResult<FetchSlice>;

    /// Offset for a timestamp, or the earliest/latest sentinels. Backs ListOffsets.
    fn offset_for_timestamp(
        &self,
        topic: TopicId,
        partition: i32,
        timestamp: i64,
    ) -> StoreResult<Option<(i64, i64)>>;

    /// The offset of the record carrying the partition's greatest timestamp, paired with
    /// that timestamp; `None` for an empty log. Not the winning batch's base offset (KIP-734).
    fn max_timestamp_offset(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<(i64, i64)>>;

    fn high_watermark(&self, topic: TopicId, partition: i32) -> StoreResult<i64>;

    /// The high watermark only if it is readable without I/O under a lock; `None` means
    fn high_watermark_if_tracked(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<i64>>;

    /// The last stable offset, same rule as `high_watermark_if_tracked`; `None` means
    fn last_stable_offset_if_tracked(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<i64>>;
    fn log_start_offset(&self, topic: TopicId, partition: i32) -> StoreResult<i64>;

    /// Bytes this partition's log occupies on disk, for `DescribeLogDirs`. Approximate
    fn partition_bytes(&self, topic: TopicId, partition: i32) -> StoreResult<i64>;

    fn log_dir(&self) -> String;

    /// Retention: partition drop or file unlink, never `DELETE`.
    fn truncate_below(&mut self, topic: TopicId, partition: i32, offset: i64) -> StoreResult<()>;

    /// Run one compaction pass over a partition, keeping the last record per key. Not
    fn compact(&mut self, _topic: TopicId, _partition: i32) -> StoreResult<u64> {
        Err(StoreError::NotImplemented("compaction on this storage engine"))
    }

    /// Backs DeleteRecords (API 21) and retention by size/time. Returns segments
    fn enforce_retention(&mut self, topic: TopicId, policy: &RetentionPolicy)
        -> StoreResult<u64>;

    fn create_partition(&mut self, topic: TopicId, partition: i32, epoch: i32) -> StoreResult<()>;
    fn drop_partition(&mut self, topic: TopicId, partition: i32) -> StoreResult<()>;

    /// Append a batch whose offsets belong to an uncommitted transaction; returns
    fn append_pending(
        &mut self,
        _topic: TopicId,
        _partition: i32,
        _batch: RawBatch,
    ) -> StoreResult<(i64, i64)> {
        Err(StoreError::NotImplemented("transactional produce"))
    }

    /// Append a batch that already has its offsets, replicated from a leader. Not
    fn append_replicated(
        &mut self,
        _topic: TopicId,
        _partition: i32,
        _bytes: &[u8],
        _expected_base: i64,
    ) -> StoreResult<i64> {
        Err(StoreError::NotImplemented("log replication"))
    }

    /// Discard everything at or above `offset`: a follower whose log diverged from a
    fn truncate_to(&mut self, _topic: TopicId, _partition: i32, _offset: i64)
        -> StoreResult<i64> {
        Err(StoreError::NotImplemented("truncation on divergence"))
    }

    /// Make everything already appended to this partition durable. Default is a no-op: the
    /// table engine's records are Postgres rows and the commit is what makes them durable.
    fn sync_partition(&mut self, _topic: TopicId, _partition: i32) -> StoreResult<()> {
        Ok(())
    }

    /// The Last Stable Offset — the first offset a `read_committed` consumer must not
    fn last_stable_offset(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        self.high_watermark(topic, partition)
    }

    /// Persisted per partition and bumped on every promotion; without it, async
    fn leader_epoch(&self, topic: TopicId, partition: i32) -> StoreResult<i32>;

    /// Raise the partition to `epoch`, recording where it starts. Takes the epoch
    fn set_leader_epoch(
        &mut self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<bool>;

    /// First offset written under `epoch`, from durable per-epoch history. `epoch` is
    fn epoch_start_offset(
        &self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<Option<i64>>;

    /// Where the epoch a client last saw ended. A wrong answer is divergence, not an
    fn epoch_end_offset(
        &self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<EpochEnd>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_errors_map_to_the_codes_clients_retry_on() {
        // A wrong code here makes clients hang or spin, presenting as a bug elsewhere.
        assert_eq!(
            StoreError::UnknownTopicOrPartition.error_code(),
            ErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(
            StoreError::OffsetOutOfRange.error_code(),
            ErrorCode::OffsetOutOfRange
        );
        assert_eq!(
            StoreError::CorruptBatch.error_code(),
            ErrorCode::CorruptMessage
        );
        // OFFSET_OUT_OF_RANGE drives auto.offset.reset, which is recovery but not a
        assert!(!ErrorCode::OffsetOutOfRange.is_retriable());
        assert!(ErrorCode::UnknownTopicOrPartition.is_retriable());
        // Storage trouble is transient by assumption — the client should come back.
        assert!(ErrorCode::KafkaStorageError.is_retriable());
        // Same distinction: a consumer rejoins on this, but does not retry the request.
        assert!(!ErrorCode::RebalanceInProgress.is_retriable());
    }
}
