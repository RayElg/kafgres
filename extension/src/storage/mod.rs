//! Storage boundary: every read or write of log data goes through [`LogStore`].

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

/// Take whatever locks the active engine needs to serve a read, without waiting.
pub fn lock_for_read() -> Result<(), pgrx::spi::Error> {
    table::lock_for_read()
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
    ) -> StoreResult<Option<i64>>;

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
