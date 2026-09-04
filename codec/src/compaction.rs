//! The cleaner's arithmetic: which records survive a compaction pass, and how a batch is

use std::collections::HashMap;

use bytes::Bytes;

use crate::records::{
    put_varint_i32, put_varint_i64, BatchError, RecordBatch, ATTRIBUTES_OFFSET,
    BASE_SEQUENCE_OFFSET, LENGTH_OFFSET, MAGIC_V2, PRODUCER_EPOCH_OFFSET, PRODUCER_ID_OFFSET,
};

/// One record kept from an existing batch, carrying the offset it was written at.
#[derive(Debug, Clone)]
pub struct KeptRecord {
    /// Absolute, not a delta.
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// Carried through the rewrite — the trade is "same records, new bytes", not "same
    pub headers: Vec<(Bytes, Option<Bytes>)>,
    pub attributes: i8,
}

/// The offsets that survive a pass over `batches`, in the order they were read.
pub fn survivors(batches: &[RecordBatch]) -> Result<Survivors, BatchError> {
    survivors_until(batches, i64::MIN)
}

/// `survivors`, plus the point past which a tombstone has outlived its purpose: one older
pub fn survivors_until(
    batches: &[RecordBatch],
    tombstone_cutoff: i64,
) -> Result<Survivors, BatchError> {
    // key -> (offset, is_tombstone, timestamp)
    let mut latest: HashMap<Bytes, (i64, bool, i64)> = HashMap::new();
    let mut control = Vec::new();
    let mut unkeyed: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for batch in batches {
        if batch.is_control() {
            control.push(batch.base_offset());
            continue;
        }
        let base = batch.base_offset();
        for record in batch.records_decompressed()? {
            let record = record?;
            let offset = base + record.offset_delta as i64;
            // A null key has nothing to supersede it and nothing it supersedes, so it is
            let Some(key) = record.key else {
                unkeyed.insert(offset);
                continue;
            };
            let timestamp = batch.base_timestamp() + record.timestamp_delta;
            let is_tombstone = record.value.is_none();
            latest
                .entry(key)
                .and_modify(|e| {
                    if offset > e.0 {
                        *e = (offset, is_tombstone, timestamp);
                    }
                })
                .or_insert((offset, is_tombstone, timestamp));
        }
    }

    let mut offsets: std::collections::HashSet<i64> = latest
        .values()
        .filter(|(_, is_tombstone, timestamp)| {
            // The one removal not driven by supersession: this survives or not against a clock.
            !(*is_tombstone && *timestamp < tombstone_cutoff)
        })
        .map(|(offset, _, _)| *offset)
        .collect();
    offsets.extend(unkeyed);
    Ok(Survivors {
        offsets,
        control_batches: control,
        keys: latest.len(),
    })
}

pub struct Survivors {
    offsets: std::collections::HashSet<i64>,
    control_batches: Vec<i64>,
    /// Distinct keys seen. The cleaner uses it to size the next pass's map.
    pub keys: usize,
}

impl Survivors {
    pub fn keeps(&self, offset: i64) -> bool {
        self.offsets.contains(&offset)
    }

    pub fn keeps_control_batch(&self, base_offset: i64) -> bool {
        self.control_batches.contains(&base_offset)
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

/// Rewrite one batch to hold only `kept`, preserving every surviving record's offset.
pub fn rebuild_batch(source: &RecordBatch, kept: &[KeptRecord]) -> Option<Bytes> {
    use bytes::{BufMut, BytesMut};

    if kept.is_empty() {
        return None;
    }

    // The new base offset is the first survivor's: a consumer derives every record's
    let base_offset = kept[0].offset;
    let last_delta = (kept[kept.len() - 1].offset - base_offset) as i32;
    let base_timestamp = kept[0].timestamp;
    let max_timestamp = kept
        .iter()
        .map(|r| r.timestamp)
        .max()
        .unwrap_or(base_timestamp);

    let mut body = BytesMut::new();
    for rec in kept {
        let mut one = BytesMut::new();
        one.put_i8(rec.attributes);
        put_varint_i64(&mut one, rec.timestamp - base_timestamp);
        // The delta that preserves the absolute offset. Not the record's index in `kept`.
        put_varint_i32(&mut one, (rec.offset - base_offset) as i32);
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
        put_varint_i32(&mut one, rec.headers.len() as i32);
        for (name, value) in &rec.headers {
            put_varint_i32(&mut one, name.len() as i32);
            one.put_slice(name);
            match value {
                Some(v) => {
                    put_varint_i32(&mut one, v.len() as i32);
                    one.put_slice(v);
                }
                None => put_varint_i32(&mut one, -1),
            }
        }
        put_varint_i32(&mut body, one.len() as i32);
        body.put_slice(&one);
    }

    // Producer identity carried across unchanged: sequence numbers mean nothing after a
    let src = source.as_bytes();
    let attributes = i16::from_be_bytes([src[ATTRIBUTES_OFFSET], src[ATTRIBUTES_OFFSET + 1]])
        & !0x07;
    let producer_id = i64::from_be_bytes(
        src[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + 8]
            .try_into()
            .ok()?,
    );
    let producer_epoch = i16::from_be_bytes(
        src[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + 2]
            .try_into()
            .ok()?,
    );
    let base_sequence = i32::from_be_bytes(
        src[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + 4]
            .try_into()
            .ok()?,
    );

    let mut b = BytesMut::new();
    b.put_i64(base_offset);
    b.put_i32(0); // batchLength, patched below
                  // The source batch's epoch, not -1: these records were written under it,
    b.put_i32(source.partition_leader_epoch());
    b.put_i8(MAGIC_V2);
    b.put_u32(0); // crc, patched below
    b.put_i16(attributes);
    b.put_i32(last_delta);
    b.put_i64(base_timestamp);
    b.put_i64(max_timestamp);
    b.put_i64(producer_id);
    b.put_i16(producer_epoch);
    b.put_i32(base_sequence);
    b.put_i32(kept.len() as i32);
    b.put_slice(&body);

    let total = b.len();
    let batch_length = (total - LENGTH_OFFSET - 4) as i32;
    b[LENGTH_OFFSET..LENGTH_OFFSET + 4].copy_from_slice(&batch_length.to_be_bytes());
    // The CRC has to be recomputed: this is a new batch, not a patch of the old one.
    let crc = crc32c::crc32c(&b[ATTRIBUTES_OFFSET..]);
    b[crate::records::CRC_OFFSET..crate::records::CRC_OFFSET + 4]
        .copy_from_slice(&crc.to_be_bytes());

    Some(b.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{build_batch, NewRecord};

    /// (base offset, (key, value) records) — the shape tests build a log from.
    type LogInput = Vec<(i64, Vec<(&'static str, Option<&'static str>)>)>;

    /// Batches as they would sit in a log, each stamped with its assigned base offset.
    fn log(batches: LogInput) -> Vec<RecordBatch> {
        batches
            .into_iter()
            .map(|(base, records)| {
                let built: Vec<NewRecord> = records
                    .iter()
                    .map(|(k, v)| NewRecord {
                        key: Some(k.as_bytes().to_vec()),
                        value: v.map(|s| s.as_bytes().to_vec()),
                        timestamp: 1_700_000_000_000 + base,
                    })
                    .collect();
                let bytes = build_batch(&built);
                let mut whole = bytes.to_vec();
                whole[..8].copy_from_slice(&base.to_be_bytes());
                RecordBatch::new(Bytes::from(whole)).unwrap()
            })
            .collect()
    }

    fn kept_from(batch: &RecordBatch, survivors: &Survivors) -> Vec<KeptRecord> {
        let base = batch.base_offset();
        batch
            .records_decompressed()
            .unwrap()
            .map(|r| r.unwrap())
            .filter_map(|r| {
                let offset = base + r.offset_delta as i64;
                survivors.keeps(offset).then(|| KeptRecord {
                    offset,
                    timestamp: 1_700_000_000_000,
                    key: r.key,
                    value: r.value,
                    headers: r.headers,
                    attributes: r.attributes,
                })
            })
            .collect()
    }

    #[test]
    fn the_last_record_per_key_wins() {
        let batches = log(vec![
            (0, vec![("a", Some("1")), ("b", Some("1"))]),
            (2, vec![("a", Some("2")), ("c", Some("1"))]),
        ]);
        let s = survivors(&batches).unwrap();
        assert_eq!(s.len(), 3, "expected one survivor per key");
        assert!(!s.keeps(0), "the superseded 'a' at offset 0 survived");
        assert!(s.keeps(1), "'b' at offset 1 was dropped");
        assert!(s.keeps(2), "the latest 'a' at offset 2 was dropped");
        assert!(s.keeps(3), "'c' at offset 3 was dropped");
    }

    #[test]
    fn offsets_are_never_renumbered() {
        // A compacted log is sparse; renumbering would move every committed consumer
        let batches = log(vec![
            (
                10,
                vec![("a", Some("1")), ("b", Some("1")), ("c", Some("1"))],
            ),
            (13, vec![("b", Some("2"))]),
        ]);
        let s = survivors(&batches).unwrap();
        let kept = kept_from(&batches[0], &s);
        assert_eq!(
            kept.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![10, 12],
            "the fixture no longer produces a gap, so this test cannot catch renumbering"
        );
        let rebuilt = rebuild_batch(&batches[0], &kept).unwrap();
        RecordBatch::validated(rebuilt.clone()).expect("rewritten batch failed its CRC");
        let view = RecordBatch::new(rebuilt).unwrap();

        assert_eq!(
            view.base_offset(),
            10,
            "the batch does not start where its first survivor does"
        );
        let offsets: Vec<i64> = view
            .records()
            .unwrap()
            .map(|r| view.base_offset() + r.unwrap().offset_delta as i64)
            .collect();
        assert_eq!(
            offsets,
            vec![10, 12],
            "surviving records were renumbered: {offsets:?}"
        );
    }

    #[test]
    fn a_tombstone_survives_its_pass() {
        let batches = log(vec![(0, vec![("a", Some("1"))]), (1, vec![("a", None)])]);
        let s = survivors(&batches).unwrap();
        assert!(!s.keeps(0), "the superseded value survived");
        assert!(
            s.keeps(1),
            "the tombstone was dropped in the same pass that created it"
        );

        let kept = kept_from(&batches[1], &s);
        let rebuilt = rebuild_batch(&batches[1], &kept).unwrap();
        RecordBatch::validated(rebuilt.clone()).expect("rewritten batch failed its CRC");
        let rec = RecordBatch::new(rebuilt)
            .unwrap()
            .records()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert!(rec.value.is_none(), "the tombstone lost its null value");
    }

    #[test]
    fn a_batch_with_no_survivors_is_dropped_whole() {
        let batches = log(vec![
            (0, vec![("a", Some("1")), ("b", Some("1"))]),
            (2, vec![("a", Some("2")), ("b", Some("2"))]),
        ]);
        let s = survivors(&batches).unwrap();
        let kept = kept_from(&batches[0], &s);
        assert!(kept.is_empty());
        assert!(
            rebuild_batch(&batches[0], &kept).is_none(),
            "an empty batch was encoded rather than dropped"
        );
    }

    #[test]
    fn the_rewritten_batch_carries_a_correct_crc() {
        // A new batch: the CRC is recomputed, or every consumer rejects it as corrupt.
        let batches = log(vec![(5, vec![("a", Some("1")), ("b", Some("2"))])]);
        let s = survivors(&batches).unwrap();
        let kept = kept_from(&batches[0], &s);
        let rebuilt = rebuild_batch(&batches[0], &kept).unwrap();
        RecordBatch::validated(rebuilt).expect("rewritten batch failed its own CRC");
    }

    #[test]
    fn producer_identity_survives_the_rewrite() {
        let built = [NewRecord {
            key: Some(b"a".to_vec()),
            value: Some(b"1".to_vec()),
            timestamp: 1,
        }];
        let bytes = crate::records::build_batch_as(&built, 4242);
        let batch = RecordBatch::new(bytes).unwrap();
        let kept = vec![KeptRecord {
            offset: 0,
            timestamp: 1,
            key: Some(Bytes::from_static(b"a")),
            value: Some(Bytes::from_static(b"1")),
            headers: Vec::new(),
            attributes: 0,
        }];
        let rebuilt = rebuild_batch(&batch, &kept).unwrap();
        RecordBatch::validated(rebuilt.clone()).expect("rewritten batch failed its CRC");
        assert_eq!(RecordBatch::new(rebuilt).unwrap().producer_id(), 4242);
    }

    #[test]
    fn a_null_keyed_record_is_kept_not_deleted() {
        // Nothing to supersede and nothing superseded: compaction must leave it alone.
        let built = [
            NewRecord {
                key: None,
                value: Some(b"orphan".to_vec()),
                timestamp: 1,
            },
            NewRecord {
                key: Some(b"k".to_vec()),
                value: Some(b"1".to_vec()),
                timestamp: 1,
            },
        ];
        let batch = RecordBatch::new(crate::records::build_batch(&built)).unwrap();
        let s = survivors(std::slice::from_ref(&batch)).unwrap();
        assert!(s.keeps(0), "a null-keyed record was dropped by compaction");
        assert!(s.keeps(1));
    }

    #[test]
    fn headers_survive_the_rewrite() {
        let mut bytes = crate::records::build_batch(&[NewRecord {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            timestamp: 1,
        }])
        .to_vec();
        // `build_batch` writes no headers, so splice one in by hand and repair the frame.
        let _ = &mut bytes;
        let with_header = {
            use bytes::BufMut;
            let mut one = bytes::BytesMut::new();
            one.put_i8(0);
            crate::records::put_varint_i64(&mut one, 0);
            crate::records::put_varint_i32(&mut one, 0);
            crate::records::put_varint_i32(&mut one, 1);
            one.put_slice(b"k");
            crate::records::put_varint_i32(&mut one, 1);
            one.put_slice(b"v");
            crate::records::put_varint_i32(&mut one, 1); // one header
            crate::records::put_varint_i32(&mut one, 5);
            one.put_slice(b"trace");
            crate::records::put_varint_i32(&mut one, 3);
            one.put_slice(b"abc");

            let mut body = bytes::BytesMut::new();
            crate::records::put_varint_i32(&mut body, one.len() as i32);
            body.put_slice(&one);

            let mut whole = bytes[..crate::records::RECORD_BATCH_OVERHEAD].to_vec();
            whole.extend_from_slice(&body);
            let len = (whole.len() - crate::records::LENGTH_OFFSET - 4) as i32;
            whole[crate::records::LENGTH_OFFSET..crate::records::LENGTH_OFFSET + 4]
                .copy_from_slice(&len.to_be_bytes());
            Bytes::from(whole)
        };

        let batch = RecordBatch::new(with_header).unwrap();
        let record = batch.records().unwrap().next().unwrap().unwrap();
        assert_eq!(
            record.headers.len(),
            1,
            "the decoder did not read the header"
        );
        assert_eq!(record.headers[0].0.as_ref(), b"trace");

        let kept = vec![KeptRecord {
            offset: 0,
            timestamp: 1,
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record.headers.clone(),
            attributes: record.attributes,
        }];
        let rebuilt = rebuild_batch(&batch, &kept).unwrap();
        RecordBatch::validated(rebuilt.clone()).expect("rewritten batch failed its CRC");
        let out = RecordBatch::new(rebuilt)
            .unwrap()
            .records()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            out.headers.len(),
            1,
            "the rewrite dropped the record's headers"
        );
        assert_eq!(out.headers[0].0.as_ref(), b"trace");
        assert_eq!(out.headers[0].1.as_deref(), Some(&b"abc"[..]));
    }

    #[test]
    fn the_rewrite_keeps_the_batchs_leader_epoch() {
        let mut bytes = crate::records::build_batch(&[NewRecord {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            timestamp: 1,
        }])
        .to_vec();
        bytes[crate::records::PARTITION_LEADER_EPOCH_OFFSET
            ..crate::records::PARTITION_LEADER_EPOCH_OFFSET + 4]
            .copy_from_slice(&3i32.to_be_bytes());
        let batch = RecordBatch::new(Bytes::from(bytes)).unwrap();
        let kept = vec![KeptRecord {
            offset: 0,
            timestamp: 1,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            headers: Vec::new(),
            attributes: 0,
        }];
        let rebuilt = rebuild_batch(&batch, &kept).unwrap();
        assert_eq!(
            RecordBatch::new(rebuilt).unwrap().partition_leader_epoch(),
            3,
            "the rewrite reset the leader epoch"
        );
    }

    #[test]
    fn an_expired_tombstone_is_removed_and_a_fresh_one_is_not() {
        let old_ts = 1_000_000;
        let new_ts = 9_000_000;
        let batches = vec![
            RecordBatch::new(crate::records::build_batch(&[NewRecord {
                key: Some(b"stale".to_vec()),
                value: None,
                timestamp: old_ts,
            }]))
            .unwrap(),
            {
                let mut bytes = crate::records::build_batch(&[NewRecord {
                    key: Some(b"fresh".to_vec()),
                    value: None,
                    timestamp: new_ts,
                }])
                .to_vec();
                bytes[..8].copy_from_slice(&1i64.to_be_bytes());
                RecordBatch::new(Bytes::from(bytes)).unwrap()
            },
        ];

        // Cutoff between the two: the old tombstone has outlived the window, the new one
        let s = survivors_until(&batches, 5_000_000).unwrap();
        assert!(!s.keeps(0), "an expired tombstone was kept forever");
        assert!(
            s.keeps(1),
            "a tombstone inside delete.retention.ms was removed early"
        );

        // And with no cutoff, both stay — which is what `survivors` does.
        let all = survivors(&batches).unwrap();
        assert!(all.keeps(0) && all.keeps(1));
    }

    #[test]
    fn a_control_batch_is_never_rewritten() {
        let control = RecordBatch::new(crate::records::build_control_batch(7, 0, true)).unwrap();
        let s = survivors(&[control]).unwrap();
        assert!(s.keeps_control_batch(0), "a control batch was not retained");
        assert_eq!(s.len(), 0, "a control batch contributed a data survivor");
    }
}
