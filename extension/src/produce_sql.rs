//! `kafgres_produce()` — a produce inside the caller's transaction. Kafka structurally

use pgrx::prelude::*;

use kafgres_codec::records::{build_batch_full, NewRecord, RecordBatch};

use crate::storage::RawBatch;

/// Produce one record, returning the offset it was assigned.
#[pg_extern]
fn kafgres_produce(
    topic: &str,
    key: Option<&str>,
    value: Option<&str>,
) -> i64 {
    if !crate::allow_transactional_produce() {
        error!("kafgres_produce() is disabled by kafgres.allow_transactional_produce");
    }

    let topic_id: i32 = match Spi::get_one_with_args(
        "SELECT (SELECT topic_id::int FROM kafgres_topics WHERE name = $1)",
        &[topic.into()],
    ) {
        Ok(Some(id)) => id,
        Ok(None) => error!("kafgres: no such topic {topic:?}"),
        Err(e) => error!("kafgres: {e}"),
    };
    let topic_id = topic_id as u32;

    let partitions: i32 = Spi::get_one_with_args(
        "SELECT (SELECT count(*)::int FROM kafgres_partitions WHERE topic_id = $1::oid)",
        &[(topic_id as i32).into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);
    if partitions <= 0 {
        error!("kafgres: topic {topic:?} has no partitions");
    }

    // murmur2 on the key, matching what Kafka clients do, so a key produced through SQL
    let partition = match key {
        Some(k) => (murmur2(k.as_bytes()) & 0x7fff_ffff) % partitions,
        None => 0,
    };

    // One `producerId` per transaction, taken from the xid: the aborted list is keyed by
    let producer_id: i64 = Spi::get_one("SELECT pg_current_xact_id()::text::bigint")
        .ok()
        .flatten()
        .unwrap_or(0);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    // Stamped so a reader can tell this batch is marker-backed even after a crash —
    let bytes = build_batch_full(
        &[NewRecord {
            key: key.map(|k| k.as_bytes().to_vec()),
            value: value.map(|v| v.as_bytes().to_vec()),
            timestamp: now,
        }],
        producer_id,
        true,
    );
    let view = match RecordBatch::new(bytes.clone()) {
        Ok(v) => v,
        Err(e) => error!("kafgres: built an invalid batch: {e:?}"),
    };
    let raw = RawBatch {
        bytes: bytes.to_vec(),
        record_count: view.record_count(),
        last_offset_delta: view.last_offset_delta(),
        max_timestamp: view.max_timestamp(),
        producer_id,
        producer_epoch: -1,
        base_sequence: -1,
        is_transactional: true,
        is_control: false,
    };

    // Through the factory, not a named engine: which engine can do this is the engine's
    let mut store = crate::storage::open();
    let (base_offset, last_offset) = match store.append_pending(topic_id, partition, raw) {
        Ok(v) => v,
        Err(e) => error!("kafgres: produce failed: {e}"),
    };

    // The marker, in the caller's transaction. Everything above already happened; this
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO kafgres_markers (topic_id, partition, base_offset, last_offset, bytes)
         VALUES ($1::oid, $2, $3, $4, $5)",
        &[
            (topic_id as i32).into(),
            partition.into(),
            base_offset.into(),
            last_offset.into(),
            (bytes.len() as i32).into(),
        ],
    ) {
        // The payload is already in the segment. Release the reservation so the LSO is
        crate::storage::release_pending(topic_id, partition);
        error!("kafgres: could not record the commit marker: {e}");
    }

    // Release on **both** outcomes. Registering only the commit callback would leave an
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, move || {
        crate::storage::release_pending(topic_id, partition);
    });
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, move || {
        crate::storage::release_pending(topic_id, partition);
    });

    base_offset
}

/// Kafka's `Utils.murmur2`, which is what every client partitions keys with.
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;

    let len = data.len();
    let mut h: u32 = SEED ^ (len as u32);
    let chunks = len / 4;

    for i in 0..chunks {
        let i4 = i * 4;
        let mut k = (data[i4] as u32 & 0xff)
            + ((data[i4 + 1] as u32 & 0xff) << 8)
            + ((data[i4 + 2] as u32 & 0xff) << 16)
            + ((data[i4 + 3] as u32 & 0xff) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let rem = len % 4;
    let base = chunks * 4;
    if rem >= 3 {
        h ^= (data[base + 2] as u32 & 0xff) << 16;
    }
    if rem >= 2 {
        h ^= (data[base + 1] as u32 & 0xff) << 8;
    }
    if rem >= 1 {
        h ^= data[base] as u32 & 0xff;
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// Drop markers whose payload did not survive a crash. **Runs once at worker start.**
pub fn reconcile_markers(store: &mut dyn crate::storage::LogStore) -> i64 {
    let partitions: Vec<(u32, i32)> = match Spi::connect(|client| {
        let rows = client.select(
            "SELECT DISTINCT topic_id::int, partition FROM kafgres_markers",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(t), Some(p)) = (row.get::<i32>(1)?, row.get::<i32>(2)?) {
                out.push((t as u32, p));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    }) {
        Ok(v) => v,
        Err(e) => {
            log!("kafgres: could not read markers for reconciliation: {e}");
            return 0;
        }
    };

    let mut dropped = 0i64;
    for (topic, partition) in partitions {
        // The log end after recovery has already truncated any torn tail.
        let log_end = match store.high_watermark(topic, partition) {
            Ok(v) => v,
            Err(e) => {
                log!("kafgres: marker reconciliation skipped {topic}/{partition}: {e}");
                continue;
            }
        };

        let orphaned: i64 = Spi::get_one_with_args(
            "SELECT (SELECT count(*) FROM kafgres_markers
                      WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3)",
            &[(topic as i32).into(), partition.into(), log_end.into()],
        )
        .ok()
        .flatten()
        .unwrap_or(0);

        if orphaned == 0 {
            continue;
        }

        // Loudly. A committed transaction was told its record existed and it does not;
        log!(
            "kafgres: WARNING: dropping {orphaned} commit marker(s) for topic {topic} \
             partition {partition} at or above offset {log_end} — the transactions \
             committed but the log does not contain their records. Those records are lost \
             and will never appear to consumers. Either a crash took the payload after the \
             marker committed, or this node was restored from a segment archive that stops \
             short of them — this cannot tell which, and neither can anything else after \
             the fact. On the segment engine, run `kafgres_restore_check()` *before* \
             starting the broker to see everything else the two halves disagree about; \
             these markers are among the evidence, and dropping them here destroys it."
        );

        if let Err(e) = Spi::run_with_args(
            "DELETE FROM kafgres_markers
              WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3",
            &[(topic as i32).into(), partition.into(), log_end.into()],
        ) {
            log!("kafgres: could not drop orphaned markers for {topic}/{partition}: {e}");
            continue;
        }
        dropped += orphaned;
    }
    dropped
}
