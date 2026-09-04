//! RecordBatch v2 — read-only view, CRC validation, and in-place broker stamping.

use crate::errors::ErrorCode;
use bytes::Bytes;

pub const BASE_OFFSET_OFFSET: usize = 0;
pub const LENGTH_OFFSET: usize = 8;
pub const PARTITION_LEADER_EPOCH_OFFSET: usize = 12;
pub const MAGIC_OFFSET: usize = 16;
pub const CRC_OFFSET: usize = 17;
/// CRC coverage starts here and runs to the end of the batch.
pub const ATTRIBUTES_OFFSET: usize = 21;
pub const LAST_OFFSET_DELTA_OFFSET: usize = 23;
pub const BASE_TIMESTAMP_OFFSET: usize = 27;
pub const MAX_TIMESTAMP_OFFSET: usize = 35;
pub const PRODUCER_ID_OFFSET: usize = 43;
pub const PRODUCER_EPOCH_OFFSET: usize = 51;
pub const BASE_SEQUENCE_OFFSET: usize = 53;
pub const RECORD_COUNT_OFFSET: usize = 57;
pub const RECORD_BATCH_OVERHEAD: usize = 61;

pub const MAGIC_V2: i8 = 2;

const COMPRESSION_MASK: i16 = 0x07;
/// Bit 4 of the batch attributes: `isTransactional`. A `read_committed` consumer applies
pub const TRANSACTIONAL_FLAG: i16 = 0x10;
const CONTROL_FLAG: i16 = 0x20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchError {
    TooShort(usize),
    UnsupportedMagic(i8),
    CrcMismatch {
        stored: u32,
        computed: u32,
    },
    LengthMismatch {
        declared: i64,
        actual: usize,
    },
    /// A header field that describes the batch's extent is negative; both fields are
    NegativeExtent {
        field: &'static str,
        value: i32,
    },
    /// A record ran past the end of the batch, or a varint never terminated.
    TruncatedRecord {
        at: usize,
    },
    /// A key or value length that is negative and not the `-1` that means null.
    NegativeLength(i64),
    RecordCountMismatch {
        declared: i32,
        found: usize,
    },
    Decompression(i16),
    /// The batch expanded past `MAX_DECOMPRESSED_BYTES` — a decompression bomb, not a
    DecompressedTooLarge,
    /// The batch is compressed, and this decoder does not decompress — not malformed,
    Compressed(i16),
}

impl BatchError {
    /// Chosen per cause, not per error type: `CORRUPT_MESSAGE` is retriable in the Java
    pub fn error_code(&self) -> ErrorCode {
        match self {
            // Upstream's own CORRUPT_MESSAGE text covers exactly these.
            BatchError::CrcMismatch { .. }
            | BatchError::TooShort(_)
            | BatchError::LengthMismatch { .. } => ErrorCode::CorruptMessage,
            // Well-formed but unacceptable, and no retry changes that; terminal.
            BatchError::UnsupportedMagic(_) | BatchError::NegativeExtent { .. } => {
                ErrorCode::InvalidRecord
            }
            // All inside CRC coverage, so a retry reproduces the rejection exactly.
            BatchError::TruncatedRecord { .. }
            | BatchError::NegativeLength(_)
            | BatchError::RecordCountMismatch { .. } => ErrorCode::InvalidRecord,
            // A payload that will not decompress is a batch the producer built wrong; it
            BatchError::Decompression(_) => ErrorCode::InvalidRecord,
            // Not INVALID_RECORD: the batch is fine, just too large. MESSAGE_TOO_LARGE is
            BatchError::DecompressedTooLarge => ErrorCode::MessageTooLarge,
            BatchError::Compressed(_) => ErrorCode::UnknownServerError,
        }
    }
}

impl From<BatchError> for ErrorCode {
    fn from(e: BatchError) -> Self {
        e.error_code()
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::TooShort(n) => {
                write!(
                    f,
                    "batch is {n} bytes, shorter than the {RECORD_BATCH_OVERHEAD}-byte header"
                )
            }
            BatchError::UnsupportedMagic(m) => {
                write!(f, "magic {m}; Kafka 4.x is v2-only")
            }
            BatchError::CrcMismatch { stored, computed } => {
                write!(
                    f,
                    "CRC mismatch: stored {stored:#010x}, computed {computed:#010x}"
                )
            }
            BatchError::LengthMismatch { declared, actual } => {
                write!(
                    f,
                    "batchLength declares {declared} bytes, buffer holds {actual}"
                )
            }
            BatchError::NegativeExtent { field, value } => {
                write!(f, "{field} is {value}, which cannot be negative")
            }
            BatchError::TruncatedRecord { at } => {
                write!(f, "record at byte {at} runs past the end of the batch")
            }
            BatchError::NegativeLength(n) => {
                write!(f, "field length {n}; only -1 (null) may be negative")
            }
            BatchError::RecordCountMismatch { declared, found } => {
                write!(f, "batch declares {declared} records, {found} remain")
            }
            BatchError::Decompression(codec) => {
                write!(f, "codec {codec} payload did not decompress")
            }
            BatchError::DecompressedTooLarge => {
                write!(f, "batch expands past {MAX_DECOMPRESSED_BYTES} bytes")
            }
            BatchError::Compressed(codec) => {
                write!(f, "batch uses compression codec {codec}; this decoder reads uncompressed batches only")
            }
        }
    }
}

impl std::error::Error for BatchError {}

/// Read-only view over one record batch. Borrows; never copies the payload.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    bytes: Bytes,
}

macro_rules! read_at {
    ($name:ident, $ty:ty, $off:expr, $n:expr) => {
        pub fn $name(&self) -> $ty {
            let mut b = [0u8; $n];
            b.copy_from_slice(&self.bytes[$off..$off + $n]);
            <$ty>::from_be_bytes(b)
        }
    };
}

impl RecordBatch {
    /// Wrap without validating the CRC. Use [`RecordBatch::validated`] on the produce
    pub fn new(bytes: Bytes) -> Result<Self, BatchError> {
        if bytes.len() < RECORD_BATCH_OVERHEAD {
            return Err(BatchError::TooShort(bytes.len()));
        }
        let batch = RecordBatch { bytes };
        if batch.magic() != MAGIC_V2 {
            return Err(BatchError::UnsupportedMagic(batch.magic()));
        }
        // batchLength counts everything after itself. Computed in i64: a hostile batch
        let declared = batch.batch_length() as i64 + (LENGTH_OFFSET + 4) as i64;
        if declared != batch.bytes.len() as i64 {
            return Err(BatchError::LengthMismatch {
                declared,
                actual: batch.bytes.len(),
            });
        }
        // Both fields are CRC-covered, so a bad value here is deliberate, not a bit-flip.
        if batch.last_offset_delta() < 0 {
            return Err(BatchError::NegativeExtent {
                field: "lastOffsetDelta",
                value: batch.last_offset_delta(),
            });
        }
        if batch.record_count() < 0 {
            return Err(BatchError::NegativeExtent {
                field: "recordCount",
                value: batch.record_count(),
            });
        }
        Ok(batch)
    }

    /// Wrap and verify the CRC. Returns a [`ValidatedBatch`] because only a CRC-checked
    pub fn validated(bytes: Bytes) -> Result<ValidatedBatch, BatchError> {
        let batch = RecordBatch::new(bytes)?;
        batch.verify_crc()?;
        Ok(ValidatedBatch(batch))
    }

    read_at!(batch_length, i32, LENGTH_OFFSET, 4);
    read_at!(base_offset, i64, BASE_OFFSET_OFFSET, 8);
    read_at!(
        partition_leader_epoch,
        i32,
        PARTITION_LEADER_EPOCH_OFFSET,
        4
    );
    read_at!(attributes, i16, ATTRIBUTES_OFFSET, 2);
    read_at!(last_offset_delta, i32, LAST_OFFSET_DELTA_OFFSET, 4);
    read_at!(base_timestamp, i64, BASE_TIMESTAMP_OFFSET, 8);
    read_at!(max_timestamp, i64, MAX_TIMESTAMP_OFFSET, 8);
    read_at!(producer_id, i64, PRODUCER_ID_OFFSET, 8);
    read_at!(producer_epoch, i16, PRODUCER_EPOCH_OFFSET, 2);
    read_at!(base_sequence, i32, BASE_SEQUENCE_OFFSET, 4);
    read_at!(record_count, i32, RECORD_COUNT_OFFSET, 4);
    read_at!(stored_crc, u32, CRC_OFFSET, 4);

    pub fn magic(&self) -> i8 {
        self.bytes[MAGIC_OFFSET] as i8
    }

    pub fn compression_type(&self) -> i16 {
        self.attributes() & COMPRESSION_MASK
    }

    pub fn is_transactional(&self) -> bool {
        self.attributes() & TRANSACTIONAL_FLAG != 0
    }

    pub fn is_control(&self) -> bool {
        self.attributes() & CONTROL_FLAG != 0
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn computed_crc(&self) -> u32 {
        crc32c::crc32c(&self.bytes[ATTRIBUTES_OFFSET..])
    }

    pub fn verify_crc(&self) -> Result<(), BatchError> {
        let stored = self.stored_crc();
        let computed = self.computed_crc();
        if stored == computed {
            Ok(())
        } else {
            Err(BatchError::CrcMismatch { stored, computed })
        }
    }
}

/// A batch whose CRC has been verified. Only obtainable from [`RecordBatch::validated`],
#[derive(Debug, Clone)]
pub struct ValidatedBatch(RecordBatch);

impl std::ops::Deref for ValidatedBatch {
    type Target = RecordBatch;
    fn deref(&self) -> &RecordBatch {
        &self.0
    }
}

impl ValidatedBatch {
    /// Stamp the broker-assigned offset and leader epoch. Both fields sit outside CRC
    pub fn stamp(self, base_offset: i64, leader_epoch: i32) -> StampedBatch {
        let crc_before = self.stored_crc();
        let mut owned = self.0.bytes.to_vec();
        owned[BASE_OFFSET_OFFSET..BASE_OFFSET_OFFSET + 8]
            .copy_from_slice(&base_offset.to_be_bytes());
        owned[PARTITION_LEADER_EPOCH_OFFSET..PARTITION_LEADER_EPOCH_OFFSET + 4]
            .copy_from_slice(&leader_epoch.to_be_bytes());
        debug_assert_eq!(
            crc_before,
            u32::from_be_bytes([
                owned[CRC_OFFSET],
                owned[CRC_OFFSET + 1],
                owned[CRC_OFFSET + 2],
                owned[CRC_OFFSET + 3]
            ]),
            "stamping must not disturb the CRC (invariant I2)"
        );
        StampedBatch {
            bytes: Bytes::from(owned),
        }
    }
}

/// Walk the batches in a `records` blob. A Produce request's `records` field may hold
pub struct BatchIter {
    buf: Bytes,
}

impl BatchIter {
    pub fn new(buf: Bytes) -> Self {
        BatchIter { buf }
    }
}

impl Iterator for BatchIter {
    type Item = Result<RecordBatch, BatchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        if self.buf.len() < LENGTH_OFFSET + 4 {
            let n = self.buf.len();
            self.buf = Bytes::new();
            return Some(Err(BatchError::TooShort(n)));
        }
        let declared = i32::from_be_bytes([
            self.buf[LENGTH_OFFSET],
            self.buf[LENGTH_OFFSET + 1],
            self.buf[LENGTH_OFFSET + 2],
            self.buf[LENGTH_OFFSET + 3],
        ]) as i64;
        let total = declared + (LENGTH_OFFSET + 4) as i64;
        if total <= 0 || total > self.buf.len() as i64 {
            let actual = self.buf.len();
            self.buf = Bytes::new();
            return Some(Err(BatchError::LengthMismatch {
                declared: total,
                actual,
            }));
        }
        let one = self.buf.split_to(total as usize);
        Some(RecordBatch::new(one))
    }
}

/// A batch with broker-assigned offset and epoch written in. Ready for storage.
#[derive(Debug, Clone)]
pub struct StampedBatch {
    bytes: Bytes,
}

impl StampedBatch {
    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
    pub fn view(&self) -> RecordBatch {
        RecordBatch {
            bytes: self.bytes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    /// Build a syntactically valid magic-v2 batch with a correct CRC.
    fn build(base_offset: i64, leader_epoch: i32, payload: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i64(base_offset);
        b.put_i32(0); // batchLength, patched below
        b.put_i32(leader_epoch);
        b.put_i8(MAGIC_V2);
        b.put_u32(0); // crc, patched below
        b.put_i16(0); // attributes
        b.put_i32(0); // lastOffsetDelta
        b.put_i64(1_700_000_000_000); // firstTimestamp
        b.put_i64(1_700_000_000_001); // maxTimestamp
        b.put_i64(-1); // producerId
        b.put_i16(-1); // producerEpoch
        b.put_i32(-1); // baseSequence
        b.put_i32(1); // recordCount
        b.put_slice(payload);

        let total = b.len();
        let batch_length = (total - LENGTH_OFFSET - 4) as i32;
        b[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&batch_length.to_be_bytes());
        let crc = crc32c::crc32c(&b[ATTRIBUTES_OFFSET..]);
        b[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        b.freeze()
    }

    #[test]
    fn field_offsets_match_upstream() {
        // Straight from DefaultRecordBatch.java; if one moves, stamping writes into the wrong bytes.
        assert_eq!(BASE_OFFSET_OFFSET, 0);
        assert_eq!(LENGTH_OFFSET, 8);
        assert_eq!(PARTITION_LEADER_EPOCH_OFFSET, 12);
        assert_eq!(MAGIC_OFFSET, 16);
        assert_eq!(CRC_OFFSET, 17);
        assert_eq!(ATTRIBUTES_OFFSET, 21);
        assert_eq!(RECORD_BATCH_OVERHEAD, 61);
    }

    #[test]
    fn validates_a_well_formed_batch() {
        let b = build(0, -1, b"payload");
        let batch = RecordBatch::validated(b).unwrap();
        assert_eq!(batch.magic(), MAGIC_V2);
        assert_eq!(batch.record_count(), 1);
        assert!(!batch.is_transactional());
        assert!(!batch.is_control());
    }

    #[test]
    fn detects_corruption() {
        let mut v = build(0, -1, b"payload").to_vec();
        let last = v.len() - 1;
        v[last] ^= 0xff;
        assert!(matches!(
            RecordBatch::validated(Bytes::from(v)),
            Err(BatchError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn rejects_old_message_formats() {
        let mut v = build(0, -1, b"x").to_vec();
        v[MAGIC_OFFSET] = 1;
        assert!(matches!(
            RecordBatch::new(Bytes::from(v)),
            Err(BatchError::UnsupportedMagic(1))
        ));
    }

    #[test]
    fn rejects_truncated_batches() {
        assert!(matches!(
            RecordBatch::new(Bytes::from(vec![0u8; 10])),
            Err(BatchError::TooShort(10))
        ));
    }

    #[test]
    fn stamping_preserves_the_crc_and_the_payload() {
        let original = build(0, -1, b"business write");
        let before = RecordBatch::validated(original.clone()).unwrap();
        let crc_before = before.stored_crc();

        let stamped = before.stamp(4242, 7);
        let after = RecordBatch::validated(stamped.into_bytes()).unwrap();

        assert_eq!(after.base_offset(), 4242);
        assert_eq!(after.partition_leader_epoch(), 7);
        assert_eq!(after.stored_crc(), crc_before, "CRC must not be recomputed");
        after.verify_crc().expect("still valid after stamping");

        assert_eq!(
            &after.as_bytes()[ATTRIBUTES_OFFSET..],
            &original[ATTRIBUTES_OFFSET..]
        );
    }

    #[test]
    fn attribute_flags_decode() {
        let mut v = build(0, -1, b"x").to_vec();
        let attrs: i16 = TRANSACTIONAL_FLAG | CONTROL_FLAG | 2; // 2 == snappy
        v[ATTRIBUTES_OFFSET..ATTRIBUTES_OFFSET + 2].copy_from_slice(&attrs.to_be_bytes());
        let crc = crc32c::crc32c(&v[ATTRIBUTES_OFFSET..]);
        v[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());

        let batch = RecordBatch::validated(Bytes::from(v)).unwrap();
        assert!(batch.is_transactional());
        assert!(batch.is_control());
        assert_eq!(batch.compression_type(), 2);
    }

    #[test]
    fn batch_errors_map_to_distinct_exact_codes() {
        assert_eq!(
            BatchError::CrcMismatch {
                stored: 1,
                computed: 2
            }
            .error_code(),
            ErrorCode::CorruptMessage
        );
        assert_eq!(
            BatchError::TooShort(10).error_code(),
            ErrorCode::CorruptMessage
        );
        assert_eq!(
            BatchError::LengthMismatch {
                declared: 5,
                actual: 61
            }
            .error_code(),
            ErrorCode::CorruptMessage
        );
        assert_eq!(
            BatchError::UnsupportedMagic(1).error_code(),
            ErrorCode::InvalidRecord
        );
        // Terminal, not retriable: the bytes passed their CRC, so a resend is rejected identically.
        assert_eq!(
            BatchError::NegativeExtent {
                field: "lastOffsetDelta",
                value: -1
            }
            .error_code(),
            ErrorCode::InvalidRecord
        );

        assert_eq!(ErrorCode::CorruptMessage.code(), 2);
        assert_eq!(ErrorCode::InvalidRecord.code(), 87);
        // Corruption is transient and must be retried; a v0 batch never becomes valid.
        assert!(ErrorCode::CorruptMessage.is_retriable());
        assert!(!ErrorCode::InvalidRecord.is_retriable());
    }

    /// A negative `batchLength` is attacker-controlled. Computing the declared size in
    #[test]
    fn negative_batch_length_is_rejected_without_panicking() {
        let mut v = build(0, -1, b"x").to_vec();
        v[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(matches!(
            RecordBatch::new(Bytes::from(v)),
            // -1 declared + the 12 bytes batchLength does not count for itself.
            Err(BatchError::LengthMismatch { declared: 11, .. })
        ));
    }

    /// A negative `lastOffsetDelta` walks a partition's `next_offset` *backwards*: the
    #[test]
    fn a_negative_last_offset_delta_is_refused() {
        let mut v = build(0, -1, b"x").to_vec();
        v[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + 4]
            .copy_from_slice(&(-5i32).to_be_bytes());
        // Re-CRC so the batch is otherwise impeccable: this must not pass merely
        let crc = crc32c::crc32c(&v[ATTRIBUTES_OFFSET..]);
        v[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());

        assert!(matches!(
            RecordBatch::validated(Bytes::from(v)),
            Err(BatchError::NegativeExtent {
                field: "lastOffsetDelta",
                value: -5
            })
        ));
    }

    #[test]
    fn a_negative_record_count_is_refused() {
        let mut v = build(0, -1, b"x").to_vec();
        v[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + 4].copy_from_slice(&(-1i32).to_be_bytes());
        let crc = crc32c::crc32c(&v[ATTRIBUTES_OFFSET..]);
        v[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());

        assert!(matches!(
            RecordBatch::validated(Bytes::from(v)),
            Err(BatchError::NegativeExtent {
                field: "recordCount",
                ..
            })
        ));
    }

    /// A single-record batch has `lastOffsetDelta == 0`; the guard must not reject it.
    #[test]
    fn a_zero_last_offset_delta_is_fine() {
        assert!(RecordBatch::validated(build(0, -1, b"x")).is_ok());
    }

    /// Only a CRC-verified batch can be stamped: a compile-time property, documented
    #[test]
    fn only_validated_batches_can_be_stamped() {
        let bytes = build(0, -1, b"x");
        let unvalidated = RecordBatch::new(bytes.clone()).unwrap();
        assert_eq!(unvalidated.record_count(), 1);

        let validated = RecordBatch::validated(bytes).unwrap();
        assert_eq!(validated.record_count(), 1);
        let _ = validated.stamp(7, 1);
    }

    #[test]
    fn batch_iter_walks_concatenated_batches() {
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&build(0, -1, b"one"));
        blob.extend_from_slice(&build(0, -1, b"two"));
        blob.extend_from_slice(&build(0, -1, b"three"));

        let got: Vec<_> = BatchIter::new(blob.freeze())
            .map(|b| b.expect("each batch parses"))
            .collect();
        assert_eq!(got.len(), 3, "a records blob may hold several batches");
        for b in &got {
            b.verify_crc().unwrap();
        }
    }

    #[test]
    fn batch_iter_yields_nothing_for_an_empty_blob() {
        assert_eq!(BatchIter::new(Bytes::new()).count(), 0);
    }

    /// Truncated or lying lengths must terminate the walk with an error rather than
    #[test]
    fn batch_iter_reports_a_malformed_tail() {
        let good = build(0, -1, b"x");
        let mut blob = BytesMut::from(&good[..]);
        blob.extend_from_slice(&good[..20]); // truncated second batch

        let got: Vec<_> = BatchIter::new(blob.freeze()).collect();
        assert_eq!(got.len(), 2);
        assert!(got[0].is_ok());
        assert!(matches!(
            got[1],
            Err(BatchError::LengthMismatch { .. }) | Err(BatchError::TooShort(_))
        ));
    }

    #[test]
    fn batch_iter_rejects_a_negative_length_without_looping() {
        let mut v = build(0, -1, b"x").to_vec();
        v[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&(-99i32).to_be_bytes());
        let got: Vec<_> = BatchIter::new(Bytes::from(v)).collect();
        assert_eq!(got.len(), 1);
        assert!(got[0].is_err());
    }

    #[test]
    fn last_offset_follows_base_plus_delta() {
        let mut v = build(100, 0, b"x").to_vec();
        v[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + 4]
            .copy_from_slice(&9i32.to_be_bytes());
        let crc = crc32c::crc32c(&v[ATTRIBUTES_OFFSET..]);
        v[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        let batch = RecordBatch::validated(Bytes::from(v)).unwrap();
        assert_eq!(batch.last_offset(), 109);
    }
}
/// Zigzag varint, as used *inside* a record — not the unsigned varint of
pub fn put_varint_i64(buf: &mut bytes::BytesMut, value: i64) {
    use bytes::BufMut;
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

pub fn put_varint_i32(buf: &mut bytes::BytesMut, value: i32) {
    put_varint_i64(buf, value as i64);
}

/// The most a single batch may expand to when decompressed. `max.message.bytes` bounds
pub const MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// Snappy as Kafka's Java client writes it: `\x82SNAPPY\x00` then version and compat.
const XERIAL_MAGIC: &[u8] = b"\x82SNAPPY\x00";

pub fn decompress(codec: i16, payload: &[u8]) -> Result<Vec<u8>, BatchError> {
    match codec {
        1 => bounded_read(flate2::read::GzDecoder::new(payload), codec),
        2 => decompress_snappy(payload),
        3 => bounded_read(lz4_flex::frame::FrameDecoder::new(payload), codec),
        4 => {
            let decoder = ruzstd::StreamingDecoder::new(payload)
                .map_err(|_| BatchError::Decompression(codec))?;
            bounded_read(decoder, codec)
        }
        other => Err(BatchError::Compressed(other)),
    }
}

/// Read a decompressing stream, refusing anything over the cap. `take(cap + 1)` rather
fn bounded_read(reader: impl std::io::Read, codec: i16) -> Result<Vec<u8>, BatchError> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take(MAX_DECOMPRESSED_BYTES as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| BatchError::Decompression(codec))?;
    if out.len() > MAX_DECOMPRESSED_BYTES {
        return Err(BatchError::DecompressedTooLarge);
    }
    Ok(out)
}

/// Snappy, in whichever of the two framings the producer used; the xerial magic is the
fn decompress_snappy(payload: &[u8]) -> Result<Vec<u8>, BatchError> {
    if payload.starts_with(XERIAL_MAGIC) {
        return decompress_xerial(payload);
    }
    // A bare snappy block. `decompress_len` reads an attacker-supplied declared length,
    let declared = snap::raw::decompress_len(payload).map_err(|_| BatchError::Decompression(2))?;
    if declared > MAX_DECOMPRESSED_BYTES {
        return Err(BatchError::DecompressedTooLarge);
    }
    snap::raw::Decoder::new()
        .decompress_vec(payload)
        .map_err(|_| BatchError::Decompression(2))
}

/// `\x82SNAPPY\x00`, version, compat, then repeated `int32` length + snappy block.
fn decompress_xerial(payload: &[u8]) -> Result<Vec<u8>, BatchError> {
    // magic(8) + version(4) + compat(4)
    let mut pos = XERIAL_MAGIC.len() + 8;
    if pos > payload.len() {
        return Err(BatchError::Decompression(2));
    }
    let mut out = Vec::new();
    let mut decoder = snap::raw::Decoder::new();
    while pos < payload.len() {
        let len_bytes: [u8; 4] = payload
            .get(pos..pos + 4)
            .and_then(|b| b.try_into().ok())
            .ok_or(BatchError::Decompression(2))?;
        pos += 4;
        let len = i32::from_be_bytes(len_bytes);
        if len < 0 {
            return Err(BatchError::Decompression(2));
        }
        let chunk = payload
            .get(pos..pos + len as usize)
            .ok_or(BatchError::Decompression(2))?;
        pos += len as usize;

        // Cap checked per chunk against the running total, so a bomb split across many
        let declared =
            snap::raw::decompress_len(chunk).map_err(|_| BatchError::Decompression(2))?;
        if out.len().saturating_add(declared) > MAX_DECOMPRESSED_BYTES {
            return Err(BatchError::DecompressedTooLarge);
        }
        let mut buf = vec![0u8; declared];
        let n = decoder
            .decompress(chunk, &mut buf)
            .map_err(|_| BatchError::Decompression(2))?;
        buf.truncate(n);
        out.extend_from_slice(&buf);
    }
    Ok(out)
}

/// Read one zigzag varint, advancing `pos`. Bounded two ways: it stops at the end of the
fn read_varint_i64(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for _ in 0..10 {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(((result >> 1) as i64) ^ -((result & 1) as i64));
        }
        shift += 7;
    }
    None
}

#[derive(Debug, Clone)]
pub struct RecordRef {
    pub offset_delta: i32,
    pub timestamp_delta: i64,
    /// `None` is a genuine null key, which is what a compacted topic refuses.
    pub key: Option<Bytes>,
    /// `None` is a tombstone on a compacted topic — the marker that a key is deleted.
    pub value: Option<Bytes>,
    /// Record headers, in order. Decoded, not skipped: compaction rewrites records, and
    pub headers: Vec<(Bytes, Option<Bytes>)>,
    /// The per-record attributes byte. Unused in v2, preserved rather than zeroed so a
    pub attributes: i8,
}

/// Iterate the records inside an **uncompressed** batch. Exists for log compaction: a
pub struct RecordIter {
    buf: Bytes,
    pos: usize,
    remaining: i32,
    declared: i32,
    done: bool,
}

impl RecordBatch {
    /// Records in this batch, if it is uncompressed; refuses a compressed batch rather
    pub fn records(&self) -> Result<RecordIter, BatchError> {
        let codec = self.compression_type();
        if codec != 0 {
            return Err(BatchError::Compressed(codec));
        }
        self.record_iter(self.bytes.clone(), RECORD_BATCH_OVERHEAD)
    }

    /// Records in this batch, decompressing first if the producer compressed them. The
    pub fn records_decompressed(&self) -> Result<RecordIter, BatchError> {
        let codec = self.compression_type();
        if codec == 0 {
            return self.records();
        }
        let plain = decompress(codec, &self.bytes[RECORD_BATCH_OVERHEAD..])?;
        self.record_iter(Bytes::from(plain), 0)
    }

    /// The declared record count, checked, over whichever buffer holds the records.
    fn record_iter(&self, buf: Bytes, pos: usize) -> Result<RecordIter, BatchError> {
        let count = self.record_count();
        if count < 0 {
            return Err(BatchError::NegativeExtent {
                field: "recordCount",
                value: count,
            });
        }
        Ok(RecordIter {
            buf,
            pos,
            remaining: count,
            declared: count,
            done: false,
        })
    }
}

impl RecordIter {
    fn next_record(&mut self) -> Result<RecordRef, BatchError> {
        let at = self.pos;
        let fail = || BatchError::TruncatedRecord { at };

        let length = read_varint_i64(&self.buf, &mut self.pos).ok_or_else(fail)?;
        if length < 0 {
            return Err(BatchError::NegativeLength(length));
        }
        // The record's declared extent, checked before any field inside it is read, so a
        let end = self
            .pos
            .checked_add(length as usize)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(fail)?;

        let attributes = *self.buf.get(self.pos).ok_or_else(fail)?;
        self.pos += 1;
        let timestamp_delta = read_varint_i64(&self.buf, &mut self.pos).ok_or_else(fail)?;
        let offset_delta = read_varint_i64(&self.buf, &mut self.pos).ok_or_else(fail)?;

        let key = self.read_field(end)?;
        let value = self.read_field(end)?;

        // Headers are bounded by the record's own declared extent; the count is
        let header_count = read_varint_i64(&self.buf, &mut self.pos).ok_or_else(fail)?;
        if header_count < 0 {
            return Err(BatchError::NegativeLength(header_count));
        }
        let mut headers = Vec::new();
        for _ in 0..header_count {
            if self.pos >= end {
                return Err(fail());
            }
            let name = self.read_field(end)?.ok_or_else(fail)?;
            let value = self.read_field(end)?;
            headers.push((name, value));
        }

        // The record's own length advances the cursor, so a miscounted header list cannot
        self.pos = end;
        Ok(RecordRef {
            offset_delta: offset_delta as i32,
            timestamp_delta,
            key,
            value,
            headers,
            attributes: attributes as i8,
        })
    }

    /// A length-prefixed field: `-1` is null, anything else must fit inside the record.
    fn read_field(&mut self, end: usize) -> Result<Option<Bytes>, BatchError> {
        let at = self.pos;
        let len =
            read_varint_i64(&self.buf, &mut self.pos).ok_or(BatchError::TruncatedRecord { at })?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(BatchError::NegativeLength(len));
        }
        let stop = self
            .pos
            .checked_add(len as usize)
            .filter(|s| *s <= end)
            .ok_or(BatchError::TruncatedRecord { at })?;
        let field = self.buf.slice(self.pos..stop);
        self.pos = stop;
        Ok(Some(field))
    }
}

impl Iterator for RecordIter {
    type Item = Result<RecordRef, BatchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // Driven by the buffer, with `recordCount` as a cross-check. Trusting the count is
        if self.pos >= self.buf.len() {
            self.done = true;
            if self.remaining != 0 {
                return Some(Err(BatchError::RecordCountMismatch {
                    declared: self.declared,
                    found: (self.declared - self.remaining) as usize,
                }));
            }
            return None;
        }
        if self.remaining == 0 {
            // More records than promised — the bypass above.
            self.done = true;
            return Some(Err(BatchError::RecordCountMismatch {
                declared: self.declared,
                found: self.declared as usize + 1,
            }));
        }
        self.remaining -= 1;
        match self.next_record() {
            Ok(r) => Some(Ok(r)),
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp: i64,
}

/// Build one magic-v2 batch holding `records`. `baseOffset` and `partitionLeaderEpoch`
pub fn build_batch(records: &[NewRecord]) -> bytes::Bytes {
    build_batch_as(records, -1)
}

/// As `build_batch`, with an explicit `producerId`. `producerId` is inside CRC coverage,
pub fn build_batch_as(records: &[NewRecord], producer_id: i64) -> bytes::Bytes {
    build_batch_full(records, producer_id, false)
}

/// Bit 5 of the batch attributes: `isControl`.
pub const CONTROL_BATCH_FLAG: i16 = 0x20;

/// A transaction marker: the batch a broker appends to every partition a transaction
pub fn build_control_batch(producer_id: i64, producer_epoch: i16, commit: bool) -> bytes::Bytes {
    use bytes::{BufMut, BytesMut};

    let mut key = BytesMut::new();
    key.put_i16(0); // control record format version
    key.put_i16(if commit { 1 } else { 0 });

    let mut one = BytesMut::new();
    one.put_i8(0); // attributes
    put_varint_i64(&mut one, 0); // timestampDelta
    put_varint_i32(&mut one, 0); // offsetDelta
    put_varint_i32(&mut one, key.len() as i32);
    one.put_slice(&key);
    put_varint_i32(&mut one, -1); // null value
    put_varint_i32(&mut one, 0); // headerCount

    let mut body = BytesMut::new();
    put_varint_i32(&mut body, one.len() as i32);
    body.put_slice(&one);

    let now = 0i64;
    let mut b = BytesMut::new();
    b.put_i64(0); // baseOffset — stamped by the storage engine
    b.put_i32(0); // batchLength, patched below
    b.put_i32(-1); // partitionLeaderEpoch — stamped by the storage engine
    b.put_i8(MAGIC_V2);
    b.put_u32(0); // crc, patched below
    b.put_i16(TRANSACTIONAL_FLAG | CONTROL_BATCH_FLAG);
    b.put_i32(0); // lastOffsetDelta
    b.put_i64(now);
    b.put_i64(now);
    b.put_i64(producer_id);
    b.put_i16(producer_epoch);
    b.put_i32(-1); // baseSequence: control records are outside the sequence space
    b.put_i32(1); // recordCount
    b.put_slice(&body);

    let total = b.len();
    let batch_length = (total - LENGTH_OFFSET - 4) as i32;
    b[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&batch_length.to_be_bytes());
    let crc = crc32c::crc32c(&b[ATTRIBUTES_OFFSET..]);
    b[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    b.freeze()
}

pub fn build_batch_full(
    records: &[NewRecord],
    producer_id: i64,
    transactional: bool,
) -> bytes::Bytes {
    use bytes::{BufMut, BytesMut};

    assert!(
        !records.is_empty(),
        "a batch with no records is not encodable"
    );
    let base_timestamp = records[0].timestamp;
    let max_timestamp = records
        .iter()
        .map(|r| r.timestamp)
        .max()
        .unwrap_or(base_timestamp);

    let mut body = BytesMut::new();
    for (i, rec) in records.iter().enumerate() {
        let mut one = BytesMut::new();
        one.put_i8(0); // attributes; per-record attributes are unused in v2
        put_varint_i64(&mut one, rec.timestamp - base_timestamp);
        put_varint_i32(&mut one, i as i32); // offsetDelta
        match &rec.key {
            Some(k) => {
                put_varint_i32(&mut one, k.len() as i32);
                one.put_slice(k);
            }
            None => put_varint_i32(&mut one, -1),
        }
        match &rec.value {
            Some(v) => {
                put_varint_i32(&mut one, v.len() as i32);
                one.put_slice(v);
            }
            None => put_varint_i32(&mut one, -1),
        }
        put_varint_i32(&mut one, 0); // headerCount

        put_varint_i32(&mut body, one.len() as i32);
        body.put_slice(&one);
    }

    let mut b = BytesMut::new();
    b.put_i64(0); // baseOffset — stamped by the storage engine
    b.put_i32(0); // batchLength, patched below
    b.put_i32(-1); // partitionLeaderEpoch — stamped by the storage engine
    b.put_i8(MAGIC_V2);
    b.put_u32(0); // crc, patched below
    b.put_i16(if transactional { TRANSACTIONAL_FLAG } else { 0 });
    b.put_i32(records.len() as i32 - 1); // lastOffsetDelta
    b.put_i64(base_timestamp);
    b.put_i64(max_timestamp);
    b.put_i64(producer_id);
    b.put_i16(-1); // producerEpoch
    b.put_i32(-1); // baseSequence
    b.put_i32(records.len() as i32);
    b.put_slice(&body);

    let total = b.len();
    let batch_length = (total - LENGTH_OFFSET - 4) as i32;
    b[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&batch_length.to_be_bytes());
    // The CRC covers everything from `attributes` onward, which is why baseOffset and
    let crc = crc32c::crc32c(&b[ATTRIBUTES_OFFSET..]);
    b[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    b.freeze()
}

#[cfg(test)]
mod build_tests {
    use super::*;

    fn rec(key: Option<&str>, value: Option<&str>, ts: i64) -> NewRecord {
        NewRecord {
            key: key.map(|k| k.as_bytes().to_vec()),
            value: value.map(|v| v.as_bytes().to_vec()),
            timestamp: ts,
        }
    }

    #[test]
    fn a_control_batch_is_both_transactional_and_control() {
        // Different bits: the aborted list applies only to *transactional* batches, and
        let commit = RecordBatch::validated(build_control_batch(7, 3, true)).unwrap();
        assert!(commit.is_transactional());
        assert!(commit.is_control());
        assert_eq!(commit.producer_id(), 7);
        assert_eq!(commit.producer_epoch(), 3);
        assert_eq!(commit.record_count(), 1);
        // A marker is a single record; LSO arithmetic depends on it.
        assert_eq!(commit.last_offset_delta(), 0);
    }

    #[test]
    fn commit_and_abort_markers_differ_in_the_key() {
        // The whole signal is two bytes in the key; if they encoded identically, every
        let commit = build_control_batch(7, 3, true);
        let abort = build_control_batch(7, 3, false);
        assert_ne!(commit, abort);
        assert!(RecordBatch::validated(abort).is_ok());
    }

    #[test]
    fn the_transactional_flag_is_visible_to_the_reader() {
        let plain = RecordBatch::validated(build_batch(&[rec(None, Some("v"), 1)])).unwrap();
        assert!(!plain.is_transactional());
        let txn =
            RecordBatch::validated(build_batch_full(&[rec(None, Some("v"), 1)], 7, true)).unwrap();
        assert!(txn.is_transactional());
        assert!(
            !txn.is_control(),
            "transactional is not the same bit as control"
        );
    }

    #[test]
    fn a_built_batch_validates_and_round_trips() {
        let b = build_batch(&[rec(Some("k1"), Some("v1"), 1_700_000_000_000)]);
        let batch = RecordBatch::validated(b).expect("built batch must validate");
        assert_eq!(batch.record_count(), 1);
        assert_eq!(batch.magic(), MAGIC_V2);
        assert!(!batch.is_transactional());
        assert!(!batch.is_control());
    }

    #[test]
    fn last_offset_delta_is_count_minus_one() {
        // Off by one here and the next base offset lands one short, overwriting a record
        let b = build_batch(&[
            rec(Some("a"), Some("1"), 5),
            rec(Some("b"), Some("2"), 6),
            rec(Some("c"), Some("3"), 7),
        ]);
        let batch = RecordBatch::new(b).unwrap();
        assert_eq!(batch.record_count(), 3);
        assert_eq!(batch.last_offset_delta(), 2);
        assert_eq!(batch.last_offset(), 2, "base 0 plus delta 2");
    }

    #[test]
    fn stamping_does_not_break_the_crc() {
        // Why baseOffset and partitionLeaderEpoch sit outside the CRC: if this fails,
        let b = build_batch(&[rec(None, Some("payload"), 42)]);
        let stamped = RecordBatch::validated(b).unwrap().stamp(1234, 7);
        let again = RecordBatch::validated(stamped.into_bytes())
            .expect("a stamped batch must still validate");
        assert_eq!(again.base_offset(), 1234);
        assert_eq!(again.partition_leader_epoch(), 7);
    }

    #[test]
    fn timestamps_are_a_delta_from_the_first() {
        let b = build_batch(&[rec(None, Some("a"), 1000), rec(None, Some("b"), 1250)]);
        let batch = RecordBatch::new(b).unwrap();
        assert_eq!(batch.base_timestamp(), 1000);
        assert_eq!(batch.max_timestamp(), 1250);
    }

    #[test]
    fn zigzag_matches_the_reference_values() {
        // From the varint table in the Kafka protocol docs; a sign slip encodes a null
        let cases: [(i64, &[u8]); 6] = [
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (-2, &[0x03]),
            (63, &[0x7E]),
            (64, &[0x80, 0x01]),
        ];
        for (value, expected) in cases {
            let mut buf = bytes::BytesMut::new();
            put_varint_i64(&mut buf, value);
            assert_eq!(&buf[..], expected, "zigzag varint for {value}");
        }
    }

    #[test]
    fn a_null_key_is_distinguishable_from_an_empty_one() {
        let null_key = build_batch(&[rec(None, Some("v"), 1)]);
        let empty_key = build_batch(&[rec(Some(""), Some("v"), 1)]);
        assert_ne!(
            null_key, empty_key,
            "null and empty keys must not encode identically — a null key means \
             round-robin partitioning, an empty one hashes"
        );
        RecordBatch::validated(null_key).unwrap();
        RecordBatch::validated(empty_key).unwrap();
    }
}

#[cfg(test)]
mod record_decoder_tests {
    use super::*;

    fn batch(records: &[(Option<&[u8]>, Option<&[u8]>)]) -> RecordBatch {
        let built: Vec<NewRecord> = records
            .iter()
            .map(|(k, v)| NewRecord {
                key: k.map(|b| b.to_vec()),
                value: v.map(|b| b.to_vec()),
                timestamp: 1_700_000_000_000,
            })
            .collect();
        RecordBatch::new(build_batch(&built)).unwrap()
    }

    #[test]
    fn round_trips_what_the_encoder_wrote() {
        let b = batch(&[
            (Some(b"k1"), Some(b"v1")),
            (Some(b"k2"), Some(b"v2")),
            (Some(b"k3"), Some(b"v3")),
        ]);
        let got: Vec<RecordRef> = b.records().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(got[2].value.as_deref(), Some(&b"v3"[..]));
        // Offset deltas keep a surviving record at its original offset across compaction.
        assert_eq!(got[0].offset_delta, 0);
        assert_eq!(got[2].offset_delta, 2);
    }

    #[test]
    fn a_null_key_is_distinguishable_from_an_empty_one() {
        // The whole point of the decoder on the produce path: a compacted topic refuses a
        let b = batch(&[(None, Some(b"v")), (Some(b""), Some(b"v"))]);
        let got: Vec<RecordRef> = b.records().unwrap().map(|r| r.unwrap()).collect();
        assert!(got[0].key.is_none(), "null key decoded as present");
        assert_eq!(
            got[1].key.as_deref(),
            Some(&b""[..]),
            "empty key decoded as null"
        );
    }

    #[test]
    fn a_null_value_is_a_tombstone_not_an_error() {
        let b = batch(&[(Some(b"k"), None)]);
        let got: Vec<RecordRef> = b.records().unwrap().map(|r| r.unwrap()).collect();
        assert!(got[0].value.is_none());
        assert_eq!(got[0].key.as_deref(), Some(&b"k"[..]));
    }

    #[test]
    fn a_compressed_batch_is_refused_rather_than_read_as_empty() {
        // Silence would be dangerous: a compacted topic would accept a null-keyed
        let mut bytes = build_batch(&[NewRecord {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            timestamp: 1,
        }])
        .to_vec();
        bytes[ATTRIBUTES_OFFSET + 1] |= 2;
        let b = RecordBatch::new(bytes::Bytes::from(bytes)).unwrap();
        assert!(matches!(b.records(), Err(BatchError::Compressed(2))));
    }

    // Hostile input: every length here is attacker-supplied, and a panic is a crashed

    fn corrupt(
        records: &[(Option<&[u8]>, Option<&[u8]>)],
        f: impl Fn(&mut Vec<u8>),
    ) -> RecordBatch {
        let mut bytes = batch(records).as_bytes().to_vec();
        f(&mut bytes);
        // Deliberately `new`, not `validated`: corrupting the body breaks the CRC, and the
        RecordBatch::new(bytes::Bytes::from(bytes)).unwrap()
    }

    #[test]
    fn a_truncated_record_stops_the_iteration() {
        let b = corrupt(&[(Some(b"k"), Some(b"a-long-enough-value"))], |v| {
            v.truncate(RECORD_BATCH_OVERHEAD + 4);
            // `batchLength` must be patched to match, or `RecordBatch::new` rejects the
            let declared = (v.len() - LENGTH_OFFSET - 4) as i32;
            v[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&declared.to_be_bytes());
        });
        let results: Vec<_> = b.records().unwrap().collect();
        assert!(
            results.iter().any(|r| r.is_err()),
            "truncation went unreported"
        );
    }

    #[test]
    fn an_absurd_field_length_cannot_walk_off_the_end() {
        // The key length varint, rewritten to claim far more bytes than the batch holds.
        let b = corrupt(&[(Some(b"k"), Some(b"v"))], |v| {
            let at = RECORD_BATCH_OVERHEAD;
            v[at + 4] = 0xFE;
            v[at + 5] = 0xFF;
            v[at + 6] = 0xFF;
            v[at + 7] = 0x7F;
        });
        let results: Vec<_> = b.records().unwrap().collect();
        assert!(results.iter().all(|r| r.is_ok() || r.is_err()), "no panic");
        assert!(
            results.iter().any(|r| r.is_err()),
            "an absurd length was accepted"
        );
    }

    #[test]
    fn a_never_terminating_varint_is_bounded() {
        // A chain of continuation bytes. Without the ten-byte cap the shift runs forever
        let b = corrupt(&[(Some(b"k"), Some(b"v"))], |v| {
            let at = RECORD_BATCH_OVERHEAD;
            for i in 0..24 {
                if at + i < v.len() {
                    v[at + i] = 0x80;
                }
            }
        });
        let results: Vec<_> = b.records().unwrap().collect();
        assert!(
            results.iter().any(|r| r.is_err()),
            "unterminated varint was accepted"
        );
    }

    #[test]
    fn a_record_count_larger_than_the_records_present_is_reported() {
        let b = corrupt(&[(Some(b"k"), Some(b"v"))], |v| {
            v[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + 4].copy_from_slice(&99i32.to_be_bytes());
        });
        let results: Vec<_> = b.records().unwrap().collect();
        assert!(
            results
                .iter()
                .any(|r| matches!(r, Err(BatchError::RecordCountMismatch { .. }))),
            "an inflated record count went unreported: {results:?}"
        );
    }

    #[test]
    fn a_record_count_of_zero_cannot_hide_a_record() {
        // A batch declaring zero records while carrying one would slip a null-keyed record
        let b = corrupt(&[(None, Some(b"sneaky"))], |v| {
            v[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + 4].copy_from_slice(&0i32.to_be_bytes());
        });
        let results: Vec<_> = b.records().unwrap().collect();
        assert!(
            results
                .iter()
                .any(|r| matches!(r, Err(BatchError::RecordCountMismatch { .. }))),
            "a record hidden behind recordCount=0 was not reported: {results:?}"
        );
    }
}

#[cfg(test)]
mod decompression_tests {
    use super::*;

    /// Batches produced by real clients, captured off the wire. Self-round-tripping here
    fn fixture(name: &str) -> RecordBatch {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/");
        let bytes = std::fs::read(format!("{path}{name}.batch"))
            .unwrap_or_else(|e| panic!("fixture {name}: {e}"));
        RecordBatch::new(Bytes::from(bytes)).unwrap()
    }

    fn decoded(name: &str) -> Vec<RecordRef> {
        let batch = fixture(name);
        assert_ne!(
            batch.compression_type(),
            0,
            "{name} is not actually compressed; the fixture is wrong"
        );
        batch
            .records_decompressed()
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .map(|r| r.unwrap_or_else(|e| panic!("{name} record: {e}")))
            .collect()
    }

    #[test]
    fn librdkafka_gzip() {
        let records = decoded("librdkafka_gzip");
        assert_eq!(records.len(), 200);
        assert!(records[0].value.as_ref().unwrap().starts_with(b"record-0-"));
    }

    #[test]
    fn librdkafka_lz4() {
        let records = decoded("librdkafka_lz4");
        assert_eq!(records.len(), 200);
        assert!(records[199]
            .value
            .as_ref()
            .unwrap()
            .starts_with(b"record-199-"));
    }

    #[test]
    fn librdkafka_zstd() {
        let records = decoded("librdkafka_zstd");
        assert_eq!(records.len(), 200);
        assert!(records[0].value.as_ref().unwrap().starts_with(b"record-0-"));
    }

    #[test]
    fn librdkafka_writes_a_bare_snappy_block() {
        let batch = fixture("librdkafka_snappy_raw");
        let payload = &batch.as_bytes()[RECORD_BATCH_OVERHEAD..];
        assert!(
            !payload.starts_with(XERIAL_MAGIC),
            "fixture is xerial, not a bare block"
        );
        let records = decoded("librdkafka_snappy_raw");
        assert_eq!(records.len(), 200);
    }

    #[test]
    fn the_java_client_writes_xerial_framed_snappy() {
        // Same codec number on the wire, two different framings, decided by which client
        let batch = fixture("java_snappy_xerial");
        let payload = &batch.as_bytes()[RECORD_BATCH_OVERHEAD..];
        assert!(
            payload.starts_with(XERIAL_MAGIC),
            "fixture is not xerial framed"
        );
        let records = decoded("java_snappy_xerial");
        assert_eq!(records.len(), 200);
        assert!(records[0]
            .value
            .as_ref()
            .unwrap()
            .starts_with(b"jrecord-0-"));
    }

    #[test]
    fn keys_survive_the_round_trip_through_every_codec() {
        // Compaction reads keys and nothing else, so a decompressor that recovers values
        for name in [
            "librdkafka_gzip",
            "librdkafka_snappy_raw",
            "librdkafka_lz4",
            "librdkafka_zstd",
            "java_snappy_xerial",
        ] {
            let records = decoded(name);
            // kcat and the console producer both send null keys; what matters is that the
            assert!(
                records.iter().all(|r| r.key.is_none()),
                "{name} invented a key"
            );
            assert!(
                records.iter().all(|r| r.value.is_some()),
                "{name} lost a value"
            );
        }
    }

    #[test]
    fn a_compression_bomb_is_refused_before_it_is_allocated() {
        // 64 MiB of zeros in gzip is a few tens of kilobytes; the cap must stop this
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        let chunk = vec![0u8; 1024 * 1024];
        for _ in 0..(MAX_DECOMPRESSED_BYTES / chunk.len() + 4) {
            enc.write_all(&chunk).unwrap();
        }
        let bomb = enc.finish().unwrap();
        assert!(
            bomb.len() < 1024 * 1024,
            "the bomb is not compressed enough to be a bomb"
        );
        assert!(matches!(
            decompress(1, &bomb),
            Err(BatchError::DecompressedTooLarge)
        ));
    }

    #[test]
    fn a_snappy_block_lying_about_its_length_is_refused() {
        // The declared uncompressed length is attacker-supplied and is what
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        payload.extend_from_slice(b"junk");
        assert!(matches!(
            decompress(2, &payload),
            Err(BatchError::DecompressedTooLarge) | Err(BatchError::Decompression(2))
        ));
    }

    #[test]
    fn garbage_does_not_panic_in_any_codec() {
        // This runs inside a Postgres backend. A panic is a crashed backend, not an error.
        for codec in [1i16, 2, 3, 4] {
            for payload in [
                &b""[..],
                &b"\x00"[..],
                &b"not compressed at all"[..],
                &[0xFF; 64][..],
                XERIAL_MAGIC,
            ] {
                let _ = decompress(codec, payload);
            }
        }
    }

    #[test]
    fn an_unknown_codec_is_reported_rather_than_guessed() {
        assert!(matches!(
            decompress(7, b"whatever"),
            Err(BatchError::Compressed(7))
        ));
    }
}
