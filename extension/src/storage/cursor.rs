//! A partition's records as a consumer sees them, over [`LogStore::read`]. Engine-agnostic.

use std::collections::VecDeque;
use std::ops::Range;

use kafgres_codec::bytes::Bytes;
use kafgres_codec::consume::{consumed_records, AbortFilter, ConsumedRecord};
use kafgres_codec::records::{BatchIter, RecordBatch};

use super::{IsolationLevel, LogStore, StoreError, StoreResult, TopicId};

/// Asked of the store per read; a single larger batch still comes back whole.
const CHUNK_BYTES: usize = 1024 * 1024;

/// Headers a record may declare. Decoded headers cost ~32x their minimum wire size, so an
/// unlimited count lets one 64 MiB batch exhaust the backend (I8).
pub const MAX_HEADERS: usize = 65_536;

type Records = Box<dyn Iterator<Item = Result<ConsumedRecord, kafgres_codec::records::BatchError>>>;

/// Optional bounds that intersect. Times are epoch milliseconds resolved to offsets as
/// `offsetsForTimes` does, so an out-of-order timestamp inside the range is still returned.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadBounds {
    pub from_offset: Option<i64>,
    /// Exclusive.
    pub to_offset: Option<i64>,
    pub from_time: Option<i64>,
    /// Exclusive.
    pub to_time: Option<i64>,
}

/// The offsets `bounds` select, clamped to what is readable now: from the log start to the
/// high watermark, or to the last stable offset under `read_committed`.
pub fn resolve(
    store: &dyn LogStore,
    topic: TopicId,
    partition: i32,
    bounds: &ReadBounds,
    isolation: IsolationLevel,
) -> StoreResult<Range<i64>> {
    let log_start = store.log_start_offset(topic, partition)?;
    let log_end = match isolation {
        IsolationLevel::ReadCommitted => store.last_stable_offset(topic, partition)?,
        IsolationLevel::ReadUncommitted => store.high_watermark(topic, partition)?,
    };
    // Negative timestamps are the store's sentinels.
    let by_time = |ms: i64| -> StoreResult<i64> {
        Ok(store
            .offset_for_timestamp(topic, partition, ms.max(0))?
            .map_or(log_end, |(offset, _)| offset))
    };

    let mut from = bounds.from_offset.unwrap_or(log_start).max(log_start);
    let mut to = bounds.to_offset.unwrap_or(log_end).min(log_end);
    if let Some(ms) = bounds.from_time {
        from = from.max(by_time(ms)?);
    }
    if let Some(ms) = bounds.to_time {
        to = to.min(by_time(ms)?);
    }
    Ok(from..to.max(from))
}

/// A partition's records in `range`, oldest first. Never control batches; under
/// `read_committed`, no aborted transactions.
pub struct RecordCursor {
    store: Box<dyn LogStore>,
    topic: TopicId,
    partition: i32,
    isolation: IsolationLevel,
    range: Range<i64>,
    next: i64,
    /// The current chunk's batches not yet decoded, and the filter judging them.
    batches: VecDeque<RecordBatch>,
    filter: Option<AbortFilter>,
    /// The batch being decoded, one record at a time (I8), and its base offset.
    records: Option<(Records, i64)>,
}

impl RecordCursor {
    pub fn new(
        store: Box<dyn LogStore>,
        topic: TopicId,
        partition: i32,
        range: Range<i64>,
        isolation: IsolationLevel,
    ) -> Self {
        RecordCursor {
            store,
            topic,
            partition,
            isolation,
            next: range.start,
            range,
            batches: VecDeque::new(),
            filter: None,
            records: None,
        }
    }

    pub fn partition_index(&self) -> i32 {
        self.partition
    }

    fn read_chunk(&mut self) -> StoreResult<()> {
        let slice = self.store.read(
            self.topic,
            self.partition,
            self.next,
            CHUNK_BYTES,
            self.isolation,
        )?;
        // A batch reaching where the aborted list was cut short is read again from there.
        let judged_below = slice.aborted_cut_at.unwrap_or(i64::MAX);
        let served = !slice.bytes.is_empty();

        let mut advanced = self.next;
        for batch in BatchIter::new(Bytes::from(slice.bytes)) {
            let batch = batch.map_err(|e| {
                StoreError::Io(format!("undecodable batch after offset {advanced}: {e}"))
            })?;
            if batch.base_offset() >= self.range.end {
                advanced = self.range.end;
                break;
            }
            if batch.last_offset() >= judged_below {
                break;
            }
            advanced = batch.last_offset() + 1;
            self.batches.push_back(batch);
        }

        if self.batches.is_empty() && served && advanced < self.range.end {
            return Err(StoreError::Io(format!(
                "more aborted transactions overlap offset {} than one read can list",
                self.next
            )));
        }
        // Nothing served below the end: the rest of the range was compacted away.
        self.next = if served { advanced } else { self.range.end };
        self.filter = match self.isolation {
            IsolationLevel::ReadCommitted => Some(AbortFilter::new(
                slice
                    .aborted
                    .iter()
                    .map(|a| (a.producer_id, a.first_offset)),
            )),
            IsolationLevel::ReadUncommitted => None,
        };
        Ok(())
    }

    fn start(&mut self, batch: RecordBatch) -> StoreResult<()> {
        let keep = match &mut self.filter {
            Some(filter) => filter.admit(&batch),
            None => !batch.is_control(),
        };
        if keep {
            let records = consumed_records(&batch, MAX_HEADERS)
                .map_err(|e| undecodable(batch.base_offset(), e))?;
            self.records = Some((Box::new(records), batch.base_offset()));
        }
        Ok(())
    }

    fn step(&mut self) -> StoreResult<Option<ConsumedRecord>> {
        loop {
            if let Some((records, base)) = &mut self.records {
                match records.next() {
                    Some(Ok(record)) if record.offset >= self.range.end => self.records = None,
                    // The first batch may begin before the range.
                    Some(Ok(record)) if record.offset < self.range.start => {}
                    Some(Ok(record)) => return Ok(Some(record)),
                    Some(Err(e)) => return Err(undecodable(*base, e)),
                    None => self.records = None,
                }
                continue;
            }
            if let Some(batch) = self.batches.pop_front() {
                self.start(batch)?;
                continue;
            }
            if self.next >= self.range.end {
                return Ok(None);
            }
            self.read_chunk()?;
        }
    }
}

fn undecodable(base: i64, e: kafgres_codec::records::BatchError) -> StoreError {
    StoreError::Io(format!("undecodable batch at offset {base}: {e}"))
}

impl Iterator for RecordCursor {
    type Item = StoreResult<ConsumedRecord>;

    /// An error ends the cursor.
    fn next(&mut self) -> Option<Self::Item> {
        match self.step() {
            Ok(record) => record.map(Ok),
            Err(e) => {
                self.next = self.range.end;
                self.batches.clear();
                self.records = None;
                Some(Err(e))
            }
        }
    }
}
