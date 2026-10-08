//! Consumer-side batch handling: the `read_committed` filter, and records with their batch fields.

use std::collections::HashSet;

use bytes::Bytes;

use crate::records::{BatchError, RecordBatch, LOG_APPEND_TIME_FLAG};

/// The Java consumer's aborted-transaction filter over one fetch. Batches go in log order.
#[derive(Debug, Default)]
pub struct AbortFilter {
    /// `(producer_id, first_offset)` not yet reached, latest first.
    pending: Vec<(i64, i64)>,
    /// Producers whose open transaction is aborted, until their abort marker passes.
    aborted: HashSet<i64>,
}

impl AbortFilter {
    pub fn new(aborted: impl IntoIterator<Item = (i64, i64)>) -> Self {
        let mut pending: Vec<(i64, i64)> = aborted.into_iter().collect();
        pending.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        AbortFilter {
            pending,
            aborted: HashSet::new(),
        }
    }

    /// Whether a `read_committed` consumer keeps this batch. Control batches never are; an
    /// abort marker ends its producer's aborted span.
    pub fn admit(&mut self, batch: &RecordBatch) -> bool {
        let producer = batch.producer_id();
        if producer >= 0 {
            while self
                .pending
                .last()
                .is_some_and(|(_, first)| *first <= batch.last_offset())
            {
                if let Some((p, _)) = self.pending.pop() {
                    self.aborted.insert(p);
                }
            }
        }
        if batch.is_control() {
            if control_type(batch) == Some(ControlType::Abort) {
                self.aborted.remove(&producer);
            }
            return false;
        }
        !(batch.is_transactional() && self.aborted.contains(&producer))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlType {
    Abort,
    Commit,
}

/// The marker a control batch carries, from its first record's key: `version: i16, type: i16`.
pub fn control_type(batch: &RecordBatch) -> Option<ControlType> {
    let record = batch.records_decompressed().ok()?.next()?.ok()?;
    let key = record.key?;
    match i16::from_be_bytes(key.get(2..4)?.try_into().ok()?) {
        0 => Some(ControlType::Abort),
        1 => Some(ControlType::Commit),
        _ => None,
    }
}

/// One record, with the batch fields that apply to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedRecord {
    pub offset: i64,
    /// Milliseconds since the epoch; the batch's max timestamp under `LogAppendTime`.
    pub timestamp: i64,
    pub log_append_time: bool,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(Bytes, Option<Bytes>)>,
    pub leader_epoch: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// `-1` outside an idempotent producer's sequence space.
    pub sequence: i32,
    pub is_transactional: bool,
}

/// A batch's records, decompressed, one at a time. A control batch yields its marker record.
/// A record declaring more than `max_headers` headers is an error, before any is decoded.
pub fn consumed_records(
    batch: &RecordBatch,
    max_headers: usize,
) -> Result<impl Iterator<Item = Result<ConsumedRecord, BatchError>> + 'static, BatchError> {
    let base = batch.base_offset();
    let base_timestamp = batch.base_timestamp();
    let log_append_time = batch.attributes() & LOG_APPEND_TIME_FLAG != 0;
    let max_timestamp = batch.max_timestamp();
    let leader_epoch = batch.partition_leader_epoch();
    let producer_id = batch.producer_id();
    let producer_epoch = batch.producer_epoch();
    let base_sequence = batch.base_sequence();
    let is_transactional = batch.is_transactional();
    Ok(batch
        .records_decompressed()?
        .max_headers(max_headers)
        .map(move |r| {
            let r = r?;
            Ok(ConsumedRecord {
                offset: base + r.offset_delta as i64,
                timestamp: if log_append_time {
                    max_timestamp
                } else {
                    base_timestamp.saturating_add(r.timestamp_delta)
                },
                log_append_time,
                key: r.key,
                value: r.value,
                headers: r.headers,
                leader_epoch,
                producer_id,
                producer_epoch,
                sequence: sequence_at(base_sequence, r.offset_delta),
                is_transactional,
            })
        }))
}

/// A record's sequence number: sequences wrap from `i32::MAX` to 0, as Kafka's do.
fn sequence_at(base: i32, delta: i32) -> i32 {
    if base < 0 {
        return -1;
    }
    if base > i32::MAX - delta {
        delta - (i32::MAX - base) - 1
    } else {
        base + delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{build_batch_full, build_control_batch, NewRecord};

    fn at(bytes: Bytes, base: i64) -> RecordBatch {
        RecordBatch::validated(bytes).unwrap().stamp(base, 7).view()
    }

    fn data(base: i64, n: usize, producer: i64, transactional: bool) -> RecordBatch {
        let records: Vec<NewRecord> = (0..n)
            .map(|i| NewRecord {
                key: Some(format!("k{i}").into_bytes()),
                value: Some(format!("v{i}").into_bytes()),
                timestamp: 1_000 + i as i64,
            })
            .collect();
        at(build_batch_full(&records, producer, transactional), base)
    }

    fn marker(base: i64, producer: i64, commit: bool) -> RecordBatch {
        at(build_control_batch(producer, 0, commit), base)
    }

    /// One record with `n` empty, null-valued headers.
    fn with_headers(n: i32) -> RecordBatch {
        use crate::records::{put_varint_i32, put_varint_i64, LENGTH_OFFSET, MAGIC_V2};
        use bytes::{BufMut, BytesMut};
        let mut one = BytesMut::new();
        one.put_i8(0);
        put_varint_i64(&mut one, 0);
        put_varint_i32(&mut one, 0);
        put_varint_i32(&mut one, -1);
        put_varint_i32(&mut one, -1);
        put_varint_i32(&mut one, n);
        for _ in 0..n {
            put_varint_i32(&mut one, 0);
            put_varint_i32(&mut one, -1);
        }
        let mut b = BytesMut::new();
        b.put_i64(0);
        b.put_i32(0);
        b.put_i32(-1);
        b.put_i8(MAGIC_V2);
        b.put_u32(0);
        b.put_i16(0);
        b.put_i32(0);
        b.put_i64(0);
        b.put_i64(0);
        b.put_i64(-1);
        b.put_i16(-1);
        b.put_i32(-1);
        b.put_i32(1);
        put_varint_i32(&mut b, one.len() as i32);
        b.put_slice(&one);
        let length = (b.len() - LENGTH_OFFSET - 4) as i32;
        b[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&length.to_be_bytes());
        RecordBatch::new(b.freeze()).unwrap()
    }

    #[test]
    fn a_record_over_the_header_limit_is_refused_before_its_headers_are_decoded() {
        let first = |n, max| {
            consumed_records(&with_headers(n), max)
                .unwrap()
                .next()
                .unwrap()
        };
        assert_eq!(first(2, 2).unwrap().headers.len(), 2);
        assert_eq!(
            first(3, 2).unwrap_err(),
            BatchError::TooManyHeaders { count: 3, max: 2 }
        );
    }

    #[test]
    fn records_carry_their_offsets_and_batch_metadata() {
        let batch = data(40, 3, 9, true);
        let got: Vec<ConsumedRecord> = consumed_records(&batch, usize::MAX)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            got.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [40, 41, 42]
        );
        assert_eq!(
            got.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            [1_000, 1_001, 1_002]
        );
        assert_eq!(got[1].key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(got[1].value.as_deref(), Some(&b"v1"[..]));
        assert!(got
            .iter()
            .all(|r| r.leader_epoch == 7 && r.producer_id == 9 && r.is_transactional));
        assert!(got.iter().all(|r| r.sequence == -1 && !r.log_append_time));
    }

    #[test]
    fn sequences_wrap_as_kafka_does() {
        assert_eq!(sequence_at(5, 2), 7);
        assert_eq!(sequence_at(i32::MAX, 0), i32::MAX);
        assert_eq!(sequence_at(i32::MAX, 1), 0);
        assert_eq!(sequence_at(i32::MAX - 1, 3), 1);
        assert_eq!(sequence_at(-1, 3), -1);
    }

    #[test]
    fn an_aborted_transaction_is_skipped_until_its_marker() {
        // Producer 5 aborts a transaction spanning two batches; producer 6 commits around it.
        let mut f = AbortFilter::new([(5, 10)]);
        assert!(f.admit(&data(0, 10, -1, false)));
        assert!(!f.admit(&data(10, 2, 5, true)));
        assert!(f.admit(&data(12, 2, 6, true)));
        assert!(!f.admit(&data(14, 2, 5, true)));
        assert!(!f.admit(&marker(16, 5, false)));
        assert!(!f.admit(&marker(17, 6, true)));
        // The producer's next transaction is kept.
        assert!(f.admit(&data(18, 1, 5, true)));
    }

    #[test]
    fn an_abort_that_began_before_the_read_still_applies() {
        let mut f = AbortFilter::new([(5, 3)]);
        assert!(!f.admit(&data(20, 2, 5, true)));
        assert!(f.admit(&data(22, 1, 6, true)));
    }

    #[test]
    fn a_producer_listed_as_aborted_is_kept_before_its_first_offset() {
        let mut f = AbortFilter::new([(5, 30)]);
        assert!(f.admit(&data(20, 2, 5, true)));
        assert!(!f.admit(&data(30, 2, 5, true)));
    }

    #[test]
    fn non_transactional_batches_from_an_aborted_producer_are_kept() {
        let mut f = AbortFilter::new([(5, 0)]);
        assert!(f.admit(&data(0, 1, 5, false)));
    }

    #[test]
    fn control_batches_are_never_admitted_and_name_their_marker() {
        let mut f = AbortFilter::new([]);
        let commit = marker(3, 1, true);
        assert_eq!(control_type(&commit), Some(ControlType::Commit));
        assert_eq!(control_type(&marker(4, 1, false)), Some(ControlType::Abort));
        assert!(!f.admit(&commit));
    }
}
