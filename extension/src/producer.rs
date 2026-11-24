//! Idempotent producer state: ids, epochs, and the five-deep sequence window. Semantics

use pgrx::prelude::*;

/// Upstream's `NUM_BATCHES_TO_RETAIN`. Equal to `max.in.flight.requests.per.connection`,
pub const RETAINED_BATCHES: i64 = 5;

pub const NO_PRODUCER_ID: i64 = -1;
pub const NO_SEQUENCE: i32 = -1;

/// How stale `kafgres_producers.last_ts` may get before an append refreshes it. Both
const LAST_TS_GRANULARITY_SECS: f64 = 60.0;

/// Producers dropped per `sweep` call. The sweep runs on the single event loop, so an
pub const SWEEP_BATCH: i64 = 500;

/// A producer must be idle at least this long before the *ceiling* may evict it. Without a
const EVICTION_FLOOR_SECS: f64 = 300.0;

/// What to do with an incoming batch from an idempotent producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceCheck {
    Append,
    /// An exact replay of a batch still in the window: answer with the offset it got the
    Duplicate { base_offset: i64 },
    OutOfOrder { expected: i32, got: i32 },
    /// Out of sequence against a window written before the current leader epoch: the
    /// broker lost state in a failover or crash. `UNKNOWN_PRODUCER_ID` makes the client
    /// reset its sequence; `OUT_OF_ORDER_SEQUENCE_NUMBER` is fatal to librdkafka.
    Stale,
    /// The batch carries an epoch older than the one this producer id has already written
    Fenced { current_epoch: i16 },
}

/// Upstream's `DefaultRecordBatch.incrementSequence`: plain `first_seq + last_offset_delta`
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    let (s, i) = (sequence as i64, increment as i64);
    let max = i32::MAX as i64;
    if s > max - i {
        (i - (max - s) - 1) as i32
    } else {
        (s + i) as i32
    }
}

/// Allocate a producer id. A plain idempotent producer gets a fresh id and epoch 0 every
pub fn init_producer_id(transactional_id: Option<&str>) -> Result<(i64, i16), spi::Error> {
    if let Some(txn_id) = transactional_id {
        // Bump the epoch so any older instance of the same transactional id is fenced. The
        // retired pair goes with it: a retried EndTxn from that instance must be fenced too,
        // not answered NONE with the epoch this new instance now owns.
        // At `i16::MAX` (`smallint` ceiling), rotate to a fresh producer_id at epoch 0, as Kafka does.
        let at_ceiling: Option<i64> = Spi::get_one_with_args(
            "SELECT (SELECT producer_id FROM kafgres_producers
                      WHERE transactional_id = $1 AND producer_epoch >= $2)",
            &[txn_id.into(), (i16::MAX as i32).into()],
        )?;
        if let Some(old_id) = at_ceiling {
            Spi::run_with_args(
                "DELETE FROM kafgres_producer_batches WHERE producer_id = $1",
                &[old_id.into()],
            )?;
            Spi::run_with_args(
                "DELETE FROM kafgres_producers WHERE producer_id = $1",
                &[old_id.into()],
            )?;
        }
        let row: Option<String> = Spi::get_one_with_args(
            "WITH up AS (
                 INSERT INTO kafgres_producers (producer_id, producer_epoch, transactional_id)
                 VALUES (nextval('kafgres_producer_id_seq'), 0, $1)
                 ON CONFLICT (transactional_id) DO UPDATE
                    SET producer_epoch = kafgres_producers.producer_epoch + 1,
                        retired_producer_id = NULL,
                        retired_epoch = NULL,
                        retired_committed = NULL,
                        last_ts = now()
                  WHERE kafgres_producers.producer_epoch < $2
                 RETURNING producer_id, producer_epoch)
             SELECT (SELECT producer_id || '|' || producer_epoch FROM up)",
            &[txn_id.into(), (i16::MAX as i32).into()],
        )?;
        if let Some(s) = row {
            let mut it = s.split('|');
            let id = it.next().and_then(|x| x.parse().ok()).unwrap_or(-1);
            let epoch = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
            return Ok((id, epoch));
        }
    }

    let id: i64 = Spi::get_one(
        "INSERT INTO kafgres_producers (producer_id, producer_epoch)
         VALUES (nextval('kafgres_producer_id_seq'), 0)
         RETURNING producer_id",
    )?
    .unwrap_or(-1);
    Ok((id, 0))
}

/// KIP-890 part two: bump the epoch as part of ending a transaction and return the new
/// pair. At the epoch ceiling the transactional id moves to a fresh producer id.
pub fn bump_epoch_for_next_txn(producer_id: i64, committed: bool) -> Result<(i64, i16), spi::Error> {
    // One below `i16::MAX`: `producer_epoch` is a `smallint` and the other bump sites add 1 unguarded.
    const EPOCH_CEILING: i32 = i16::MAX as i32 - 1;

    // The retired pair is recorded so a retried EndTxn is seen as a retry, not a zombie.
    let bumped: Option<i32> = Spi::get_one_with_args(
        "WITH up AS (
             UPDATE kafgres_producers
                SET retired_producer_id = producer_id,
                    retired_epoch = producer_epoch,
                    retired_committed = $3,
                    producer_epoch = producer_epoch + 1,
                    last_ts = now()
              WHERE producer_id = $1 AND producer_epoch < $2
          RETURNING producer_epoch)
         SELECT (SELECT producer_epoch FROM up)",
        &[producer_id.into(), EPOCH_CEILING.into(), committed.into()],
    )?;
    if let Some(e) = bumped {
        return Ok((producer_id, e as i16));
    }

    // Here the epoch hit the ceiling or the row is gone: move the transactional id to a
    // fresh producer id at epoch 0. No row returns epoch -1 (caller already fenced).
    //
    // The transaction row moves with it: it has no foreign key, and left under the old id
    // the producer's next append would find nothing to register against and its next
    // EndTxn nothing to end. The old id's deduplication window is dropped rather than moved —
    // sequences restart at zero with the epoch — and the sweep would never see it again
    // once the producer row is gone.
    let moved: Option<String> = Spi::get_one_with_args(
        "WITH old AS (
             DELETE FROM kafgres_producers WHERE producer_id = $1
          RETURNING producer_id, transactional_id, producer_epoch),
         fresh AS (
             INSERT INTO kafgres_producers
                    (producer_id, producer_epoch, transactional_id,
                     retired_producer_id, retired_epoch, retired_committed)
             SELECT nextval('kafgres_producer_id_seq'), 0, transactional_id,
                    producer_id, producer_epoch, $2
               FROM old
          RETURNING producer_id, producer_epoch),
         txn AS (
             UPDATE kafgres_txns t
                SET producer_id = fresh.producer_id, producer_epoch = 0
               FROM fresh
              WHERE t.producer_id = $1
          RETURNING 1),
         window AS (
             DELETE FROM kafgres_producer_batches WHERE producer_id = $1
          RETURNING 1)
         SELECT (SELECT producer_id || '|' || producer_epoch FROM fresh)",
        &[producer_id.into(), committed.into()],
    )?;
    let Some(s) = moved else {
        return Ok((producer_id, -1));
    };
    let mut it = s.split('|');
    let id = it.next().and_then(|x| x.parse().ok()).unwrap_or(producer_id);
    let epoch = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    Ok((id, epoch))
}

struct Retained {
    epoch: i16,
    first_seq: i32,
    last_seq: i32,
    base_offset: i64,
}

/// The window in **insertion order**, oldest first. Ordering by `last_seq` is not
/// Also the current leader epoch's start offset.
fn window(
    producer_id: i64,
    topic_id: u32,
    partition: i32,
) -> Result<(Vec<Retained>, i64), spi::Error> {
    crate::plan::select(
        "SELECT producer_epoch, first_seq, last_seq, base_offset,
                (SELECT epoch_start_offset FROM kafgres_partitions p
                  WHERE p.topic_id = $2::oid AND p.partition = $3)
           FROM kafgres_producer_batches
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
          ORDER BY added_seq",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
        |rows| {
            let mut out = Vec::new();
            let mut epoch_start = 0;
            for row in rows {
                epoch_start = row.get::<i64>(5)?.unwrap_or(0);
                out.push(Retained {
                    epoch: row.get::<i32>(1)?.unwrap_or(0) as i16,
                    first_seq: row.get::<i32>(2)?.unwrap_or(NO_SEQUENCE),
                    last_seq: row.get::<i32>(3)?.unwrap_or(NO_SEQUENCE),
                    base_offset: row.get::<i64>(4)?.unwrap_or(-1),
                });
            }
            Ok((out, epoch_start))
        },
    )
}

/// Upstream's `inSequence`, including the wraparound: a long-lived producer really does
pub(crate) fn in_sequence(last_seq: i32, next_seq: i32) -> bool {
    next_seq as i64 == last_seq as i64 + 1 || (next_seq == 0 && last_seq == i32::MAX)
}

/// Validate a batch against the producer's retained window.
pub fn check(
    producer_id: i64,
    producer_epoch: i16,
    first_seq: i32,
    last_seq: i32,
    topic_id: u32,
    partition: i32,
) -> Result<SequenceCheck, spi::Error> {
    let (retained, epoch_start) = window(producer_id, topic_id, partition)?;

    // No state at all: accept whatever sequence arrives — deliberately *not*
    if retained.is_empty() {
        return Ok(SequenceCheck::Append);
    }

    // The newest row, not the highest epoch or sequence: those disagree with insertion
    let newest = retained.last().expect("non-empty");
    let current_epoch = newest.epoch;

    // Fencing first, exactly as upstream orders it: OUT_OF_ORDER_SEQUENCE_NUMBER says
    if producer_epoch < current_epoch {
        return Ok(SequenceCheck::Fenced { current_epoch });
    }

    if producer_epoch > current_epoch {
        // A new epoch must restart at sequence 0. Upstream raises OUT_OF_ORDER here, not
        if first_seq != 0 {
            return Ok(SequenceCheck::OutOfOrder {
                expected: 0,
                got: first_seq,
            });
        }
        return Ok(SequenceCheck::Append);
    }

    // A retry is an *exact* sequence-range match within the current epoch. Matching on
    if let Some(dup) = retained
        .iter()
        .find(|r| r.epoch == current_epoch && r.first_seq == first_seq && r.last_seq == last_seq)
    {
        return Ok(SequenceCheck::Duplicate {
            base_offset: dup.base_offset,
        });
    }

    // Upstream reads `lastEntry` off the deque rather than taking a maximum, which is
    let current_last = newest.last_seq;

    if in_sequence(current_last, first_seq) {
        Ok(SequenceCheck::Append)
    } else if newest.base_offset >= 0 && newest.base_offset < epoch_start {
        Ok(SequenceCheck::Stale)
    } else {
        Ok(SequenceCheck::OutOfOrder {
            expected: increment_sequence(current_last, 1),
            got: first_seq,
        })
    }
}

/// Record an appended batch and prune the window back to the retained depth.
pub fn record(
    producer_id: i64,
    producer_epoch: i16,
    topic_id: u32,
    partition: i32,
    first_seq: i32,
    last_seq: i32,
    base_offset: i64,
) -> Result<(), spi::Error> {
    crate::plan::run(
        "INSERT INTO kafgres_producer_batches
            (producer_id, topic_id, partition, producer_epoch, first_seq, last_seq, base_offset)
         VALUES ($1, $2::oid, $3, $4, $5, $6, $7)
         ON CONFLICT (producer_id, topic_id, partition, first_seq) DO UPDATE SET
            producer_epoch = EXCLUDED.producer_epoch,
            last_seq = EXCLUDED.last_seq,
            base_offset = EXCLUDED.base_offset,
            appended_at = now(),
            /* Re-inserting at a first_seq already in the window makes this row the
               newest one; leaving added_seq behind would make the prune treat it as
               the stalest and delete it. */
            added_seq = nextval('kafgres_producer_batch_seq')",
        &[
            producer_id.into(),
            (topic_id as i32).into(),
            partition.into(),
            (producer_epoch as i32).into(),
            first_seq.into(),
            last_seq.into(),
            base_offset.into(),
        ],
    )?;

    // Keep the newest `RETAINED_BATCHES` **by insertion order**. `ORDER BY last_seq DESC`
    //
    // Deletes "older than the Nth newest" via a scalar sub-select on `added_idx`,
    // not `NOT IN (newest N)`. With fewer than N rows the sub-select is NULL,
    // so nothing is pruned.
    crate::plan::run(
        "DELETE FROM kafgres_producer_batches
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
            AND added_seq < (
                SELECT added_seq FROM kafgres_producer_batches
                 WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
                 ORDER BY added_seq DESC OFFSET $4 - 1 LIMIT 1)",
        &[
            producer_id.into(),
            (topic_id as i32).into(),
            partition.into(),
            RETAINED_BATCHES.into(),
        ],
    )?;

    // Keep `last_ts` roughly current without an UPDATE per batch. Scale refresh granularity
    // to a quarter of `producer_id_expiration_ms` so active producers are never expired when
    // the operator configures a short expiration window.
    let exp_ms = crate::producer_retention().expiration_ms;
    let granularity = if exp_ms > 0 {
        ((exp_ms as f64) / 1000.0 / 4.0).clamp(1.0, LAST_TS_GRANULARITY_SECS)
    } else {
        LAST_TS_GRANULARITY_SECS
    };
    crate::plan::run(
        "UPDATE kafgres_producers
            SET last_ts = now()
          WHERE producer_id = $1
            AND last_ts < now() - make_interval(secs => $2)",
        &[producer_id.into(), granularity.into()],
    )?;
    Ok(())
}

/// Drop window rows at or past `end`, which the recovered log does not hold, so a retry is
/// not acknowledged at a missing offset (I6). Reservations (-1) stay: their fate is unknown.
pub fn forget_past(topic_id: u32, partition: i32, end: i64) -> Result<(), spi::Error> {
    crate::plan::run(
        "DELETE FROM kafgres_producer_batches
          WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3",
        &[(topic_id as i32).into(), partition.into(), end.into()],
    )?;
    // Nor does a checkpoint past it stand: those offsets are about to be written again.
    crate::plan::run(
        "UPDATE kafgres_partitions SET window_checkpoint = $3
          WHERE topic_id = $1::oid AND partition = $2 AND window_checkpoint > $3",
        &[(topic_id as i32).into(), partition.into(), end.into()],
    )
}

/// For a partition with nothing to start a rebuild from (see [`rebuild_from`]): how far below
/// the log end to rescan, as (slack in ms before the newest batch's timestamp, fewest offsets,
/// most offsets). A crash loses only what was appended but not yet committed, which is the
/// log's tail. Strict commits lose only requests in flight; with `relaxed_produce_commit` the
/// rows since the last WAL flush go too.
fn rebuild_lookback() -> (i64, i64, i64) {
    if !crate::relaxed_produce_commit() {
        return (1_000, 1_000, 50_000);
    }
    let wal_delay_ms = crate::plan::get_one::<i64>(
        "SELECT (SELECT setting::bigint FROM pg_settings WHERE name = 'wal_writer_delay')",
        &[],
    )
    .ok()
    .flatten()
    .unwrap_or(200);
    ((3 * wal_delay_ms).max(5_000), 5_000, 2_000_000)
}

/// The offset below which no window row of the partition can be missing, from what survived
/// the crash; `None` when nothing did.
///
/// The broker is one process and runs one transaction at a time, each committing in WAL order,
/// and a crash keeps a prefix of the WAL. So the rows a crash loses belong to the newest
/// transactions, and every batch they appended lies above anything an earlier, surviving
/// transaction recorded: the checkpoint, which a transaction wrote as the log end it saw
/// (`checkpoint_windows`), and the newest surviving row's batch. Rows can be deleted (pruned,
/// expired) without the bound moving up, which only makes the scan longer.
fn rebuild_from(checkpoint: Option<i64>, newest_row: Option<i64>) -> Option<i64> {
    match (checkpoint, newest_row) {
        (None, None) => None,
        (c, r) => Some(c.unwrap_or(i64::MIN).max(r.unwrap_or(i64::MIN))),
    }
}

/// The partition's checkpoint column and the highest offset a window row of it holds.
fn rebuild_evidence(topic_id: u32, partition: i32) -> Result<(Option<i64>, Option<i64>), String> {
    crate::plan::select(
        "SELECT (SELECT window_checkpoint FROM kafgres_partitions
                  WHERE topic_id = $1::oid AND partition = $2),
                (SELECT max(base_offset) FROM kafgres_producer_batches
                  WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= 0)",
        &[(topic_id as i32).into(), partition.into()],
        |rows| {
            let mut found = (None, None);
            for row in rows {
                found = (row.get::<i64>(1)?, row.get::<i64>(2)?);
            }
            Ok(found)
        },
    )
    .map_err(|e| e.to_string())
}

/// Where a rebuild starts on a partition created before 0.3.3 that has no window row and has
/// not been checkpointed yet, which leaves a guess: the offset `slack` before the log tail's
/// timestamp, within the bounds. Client timestamps can be stale or ahead, so the bounds decide
/// when they mislead. Once the partition is rebuilt it is checkpointed, and later rebuilds
/// start from that.
fn rebuild_start(
    store: &dyn crate::storage::LogStore,
    topic_id: u32,
    partition: i32,
    end: i64,
) -> Result<i64, String> {
    use kafgres_codec::records::RecordBatch;

    let (slack_ms, fewest, most) = rebuild_lookback();
    let nearest = end.saturating_sub(fewest);
    let farthest = end.saturating_sub(most);
    let tail = store
        .read(topic_id, partition, end - 1, 1 << 20, crate::storage::IsolationLevel::ReadUncommitted)
        .map_err(|e| e.to_string())?;
    // The first batch of the slice, which holds `end - 1`; later appends may follow it.
    let first_len = tail
        .bytes
        .get(kafgres_codec::records::LENGTH_OFFSET..kafgres_codec::records::LENGTH_OFFSET + 4)
        .map(|b| i32::from_be_bytes(b.try_into().expect("4 bytes")).max(0) as usize)
        .map(|len| kafgres_codec::records::LENGTH_OFFSET + 4 + len);
    let Some(tail_ts) = first_len
        .and_then(|n| tail.bytes.get(..n))
        .and_then(|b| RecordBatch::new(kafgres_codec::bytes::Bytes::copy_from_slice(b)).ok())
        .map(|b| b.max_timestamp())
        // One ahead by more than the slack would find little or nothing after it, the
        // narrowest start.
        .filter(|&t| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(i64::MAX, |d| d.as_millis() as i64);
            t > slack_ms && t <= now.saturating_add(slack_ms)
        })
    else {
        return Ok(farthest);
    };
    let from = store
        .offset_for_timestamp(topic_id, partition, tail_ts - slack_ms)
        .map_err(|e| e.to_string())?
        .map_or(nearest, |(offset, _)| offset);
    Ok(from.clamp(farthest, nearest))
}

type Partitions = std::collections::HashSet<(u32, i32)>;

static STARTUP_END: std::sync::Mutex<Option<std::collections::HashMap<(u32, i32), i64>>> =
    std::sync::Mutex::new(None);

/// The log end when this process started, where a window rebuild looks back from.
pub fn note_startup_end(topic_id: u32, partition: i32, end: i64) {
    STARTUP_END
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(Default::default)
        .insert((topic_id, partition), end);
}

static WINDOW_REBUILT: std::sync::Mutex<Option<Partitions>> = std::sync::Mutex::new(None);

thread_local! {
    /// Rebuilt in the current transaction, not yet committed.
    static REBUILT_PENDING: std::cell::RefCell<Option<Partitions>> = const { std::cell::RefCell::new(None) };
}

/// Before the first idempotent produce to a partition after this process starts, rebuild its
/// window from the log (segment engine only; the table engine commits a batch and its row
/// together). The partition counts as rebuilt only once the transaction holding the rows
/// commits; a rollback, or a failure, leaves it for the next produce to try again.
pub fn ensure_window(topic_id: u32, partition: i32) -> Result<(), String> {
    use pgrx::{PgSubXactCallbackEvent, PgXactCallbackEvent};

    if crate::storage_engine_guc() != "segment" {
        return Ok(());
    }
    let key = (topic_id, partition);
    if WINDOW_REBUILT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(|s| s.contains(&key))
        || REBUILT_PENDING.with(|p| p.borrow().as_ref().is_some_and(|s| s.contains(&key)))
    {
        return Ok(());
    }
    let store = crate::storage::open();
    rebuild_window(&*store, topic_id, partition).map_err(|e| {
        format!("could not rebuild the idempotence window for topic {topic_id} partition {partition}: {e}")
    })?;

    let first = REBUILT_PENDING.with(|p| {
        let mut p = p.borrow_mut();
        let first = p.is_none();
        p.get_or_insert_with(Default::default).insert(key);
        first
    });
    if first {
        pgrx::register_xact_callback(PgXactCallbackEvent::Commit, || {
            let done = REBUILT_PENDING.with(|p| p.borrow_mut().take());
            if let Some(done) = done {
                WINDOW_REBUILT
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert_with(Default::default)
                    .extend(done);
            }
        });
        pgrx::register_xact_callback(PgXactCallbackEvent::Abort, || {
            REBUILT_PENDING.with(|p| p.borrow_mut().take());
        });
        // A rolled-back subtransaction may hold the rows: forget, and rebuild again.
        pgrx::register_subxact_callback(PgSubXactCallbackEvent::AbortSub, |_, _| {
            REBUILT_PENDING.with(|p| p.borrow_mut().take());
        });
    }
    Ok(())
}

/// Re-create window rows for batches the log holds and the window lacks, so a retry after a
/// crash between the append and the row's commit is not appended twice.
pub fn rebuild_window(
    store: &dyn crate::storage::LogStore,
    topic_id: u32,
    partition: i32,
) -> Result<u64, String> {
    use kafgres_codec::records::{RecordBatch, LENGTH_OFFSET};
    use std::collections::HashMap;

    // Only what the log held at startup can have lost a row; later appends were recorded.
    let end = store
        .high_watermark(topic_id, partition)
        .map_err(|e| e.to_string())?;
    let log_start = store
        .log_start_offset(topic_id, partition)
        .map_err(|e| e.to_string())?;
    // A partition missing from a recorded startup was created since, so it lost nothing.
    let end = match STARTUP_END.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
        Some(m) => m.get(&(topic_id, partition)).map_or(log_start, |&at_start| at_start.min(end)),
        None => end,
    };
    let (checkpoint, newest_row) = rebuild_evidence(topic_id, partition)?;
    CHECKPOINTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .known
        .get_or_insert_with(Default::default)
        .insert((topic_id, partition), checkpoint);
    let mut at = if end > log_start {
        match rebuild_from(checkpoint, newest_row) {
            Some(from) => from,
            None => rebuild_start(store, topic_id, partition, end)?,
        }
        .max(log_start)
    } else {
        log_start
    };
    if end - at > LONG_REBUILD {
        pgrx::log!(
            "kafgres: rebuilding the idempotence window of topic {topic_id} partition \
             {partition} from offset {at} to {end}"
        );
    }

    // Per producer: (epoch, last_seq) of its newest row, `None` with no window here.
    let mut newest: HashMap<i64, Option<(i16, i32)>> = HashMap::new();
    let mut rebuilt = 0u64;
    while at < end {
        let slice = store
            .read(topic_id, partition, at, 4 << 20, crate::storage::IsolationLevel::ReadUncommitted)
            .map_err(|e| e.to_string())?;
        if slice.bytes.is_empty() {
            break;
        }
        let mut pos = 0usize;
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
            let (pid, epoch, first) = (batch.producer_id(), batch.producer_epoch(), batch.base_sequence());
            // A batch straddling the start is read whole and kept: it may be the newest one.
            if pid < 0 || epoch < 0 || first < 0 || batch.is_control() {
                continue;
            }
            // If a crash occurred between appending a transactional batch and stamping
            // `kafgres_txn_partitions.first_offset`, restore `first_offset` now so the batch
            // is held below the LSO and recorded if the transaction aborts.
            if batch.is_transactional() {
                crate::storage::pmeta::stamp_txn_partition(
                    pid,
                    topic_id,
                    partition,
                    batch.base_offset(),
                )
                .map_err(|e| e.to_string())?;
            }
            let last = increment_sequence(first, batch.last_offset_delta());
            let state = match newest.get(&pid) {
                Some(s) => *s,
                None => {
                    let known = crate::plan::get_one::<bool>(
                        "SELECT (SELECT true FROM kafgres_producers WHERE producer_id = $1)",
                        &[pid.into()],
                    )
                    .map_err(|e| e.to_string())?
                    .unwrap_or(false);
                    let s = if !known {
                        // Expired or never registered.
                        Some((i16::MAX, i32::MIN))
                    } else {
                        window(pid, topic_id, partition)
                            .map_err(|e| e.to_string())?
                            .0
                            .last()
                            .map(|r| (r.epoch, r.last_seq))
                    };
                    newest.insert(pid, s);
                    s
                }
            };
            let missing = match state {
                None => true,
                Some((e, l)) => epoch > e || (epoch == e && in_sequence(l, first)),
            };
            if !missing {
                continue;
            }
            record(pid, epoch, topic_id, partition, first, last, batch.base_offset())
                .map_err(|e| e.to_string())?;
            newest.insert(pid, Some((epoch, last)));
            rebuilt += 1;
        }
        if pos == 0 {
            break;
        }
        at = slice.next_offset.max(at + 1);
    }
    Ok(rebuilt)
}

/// Offsets a rebuild scans before it is worth a line in the log.
const LONG_REBUILD: i64 = 1_000_000;

/// Partitions whose checkpoint one pass may move, and rebuilds it may run, so that a pass
/// stays short on the broker's single loop.
const CHECKPOINTS_PER_PASS: usize = 1_000;
const REBUILD_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Default)]
struct Checkpoints {
    /// `window_checkpoint` as this process last read or wrote it. A pass moves the column
    /// only from this value, so one changed underneath it is left alone.
    known: Option<std::collections::HashMap<(u32, i32), Option<i64>>>,
    /// Partitions a background rebuild failed on; a produce still tries them.
    skipped: Option<Partitions>,
    /// Where the next pass resumes when one cannot cover every partition.
    cursor: usize,
}

static CHECKPOINTS: std::sync::Mutex<Checkpoints> =
    std::sync::Mutex::new(Checkpoints { known: None, skipped: None, cursor: 0 });

/// What one [`checkpoint_windows`] pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CheckpointPass {
    pub rebuilt: usize,
    pub advanced: usize,
    /// Partitions the log held at startup that are still to be rebuilt.
    pub waiting: usize,
}

/// Record how far each partition's window rows are known complete, so the next rebuild after
/// a crash starts there (segment engine only).
///
/// A partition qualifies once its window is rebuilt for this start: below the log end, every
/// idempotent batch then has its row, written by a transaction that committed before this
/// one. The log end is therefore a checkpoint that holds whenever this transaction survives.
/// Partitions no produce has rebuilt yet are rebuilt here, a few per pass, so that all of
/// them come to be checkpointed and none is left with a long scan for a later start.
pub fn checkpoint_windows(store: &dyn crate::storage::LogStore) -> Result<CheckpointPass, String> {
    let mut pass = CheckpointPass::default();
    if crate::storage_engine_guc() != "segment" {
        return Ok(pass);
    }

    let rebuilt: Partitions = WINDOW_REBUILT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default();
    let mut waiting: Vec<(u32, i32)> = {
        let skipped = CHECKPOINTS.lock().unwrap_or_else(|e| e.into_inner()).skipped.clone();
        STARTUP_END
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .flat_map(|m| m.keys().copied())
            .filter(|k| !rebuilt.contains(k) && !skipped.as_ref().is_some_and(|s| s.contains(k)))
            .collect()
    };
    waiting.sort_unstable();
    let started = std::time::Instant::now();
    let mut tried = 0;
    for &(topic_id, partition) in &waiting {
        if tried > 0 && started.elapsed() >= REBUILD_BUDGET {
            break;
        }
        tried += 1;
        match ensure_window(topic_id, partition) {
            Ok(()) => pass.rebuilt += 1,
            // Dropped since startup, most likely. Not retried here.
            Err(e) => {
                pgrx::log!("kafgres: {e}");
                CHECKPOINTS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .skipped
                    .get_or_insert_with(Default::default)
                    .insert((topic_id, partition));
            }
        }
    }
    pass.waiting = waiting.len() - tried;

    // Those rebuilt in a transaction that has committed; this pass's own follow next time.
    let mut keys: Vec<(u32, i32)> = rebuilt.into_iter().collect();
    keys.sort_unstable();
    let (mut topics, mut partitions, mut from, mut to) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    {
        let mut state = CHECKPOINTS.lock().unwrap_or_else(|e| e.into_inner());
        let start = if keys.is_empty() { 0 } else { state.cursor % keys.len() };
        let mut examined = 0;
        for i in 0..keys.len() {
            if topics.len() >= CHECKPOINTS_PER_PASS {
                break;
            }
            examined = i + 1;
            let (topic_id, partition) = keys[(start + i) % keys.len()];
            // Unknown: rebuilt without the column being read, which does not happen.
            let Some(&known) = state.known.as_ref().and_then(|k| k.get(&(topic_id, partition))) else {
                continue;
            };
            // Not tracked: nothing was appended since it was last read, or it was dropped.
            let Ok(Some(end)) = store.high_watermark_if_tracked(topic_id, partition) else {
                continue;
            };
            if known.is_some_and(|k| end <= k) {
                continue;
            }
            topics.push(topic_id as i32);
            partitions.push(partition);
            from.push(known);
            to.push(end);
        }
        state.cursor = if keys.is_empty() { 0 } else { (start + examined) % keys.len() };
    }
    if topics.is_empty() {
        return Ok(pass);
    }

    let moved: Vec<(u32, i32, i64)> = Spi::connect_mut(|client| {
        let rows = client.update(
            "UPDATE kafgres_partitions p
                SET window_checkpoint = v.next
               FROM unnest($1::int[], $2::int[], $3::bigint[], $4::bigint[])
                    AS v(topic_id, partition, known, next)
              WHERE p.topic_id = v.topic_id::oid AND p.partition = v.partition
                AND p.window_checkpoint IS NOT DISTINCT FROM v.known
          RETURNING p.topic_id::int, p.partition, p.window_checkpoint",
            None,
            &[topics.into(), partitions.into(), from.into(), to.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(t), Some(p), Some(c)) =
                (row.get::<i32>(1)?, row.get::<i32>(2)?, row.get::<i64>(3)?)
            {
                out.push((t as u32, p, c));
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(|e| e.to_string())?;

    // Remembered only once the update commits; a rollback leaves the column where it was.
    pass.advanced = moved.len();
    let first = MOVED_PENDING.with(|p| p.borrow_mut().replace(moved).is_none());
    if first {
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, || {
            let Some(moved) = MOVED_PENDING.with(|p| p.borrow_mut().take()) else { return };
            let mut state = CHECKPOINTS.lock().unwrap_or_else(|e| e.into_inner());
            let known = state.known.get_or_insert_with(Default::default);
            for (topic_id, partition, checkpoint) in moved {
                known.insert((topic_id, partition), Some(checkpoint));
            }
        });
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, || {
            MOVED_PENDING.with(|p| p.borrow_mut().take());
        });
        pgrx::register_subxact_callback(pgrx::PgSubXactCallbackEvent::AbortSub, |_, _| {
            MOVED_PENDING.with(|p| p.borrow_mut().take());
        });
    }
    Ok(pass)
}

thread_local! {
    /// Checkpoints the current transaction moved, not yet committed.
    static MOVED_PENDING: std::cell::RefCell<Option<Vec<(u32, i32, i64)>>> =
        const { std::cell::RefCell::new(None) };
}

/// Stamp the offset a `base_offset = -1` reservation was appended at.
pub fn stamp(
    producer_id: i64,
    topic_id: u32,
    partition: i32,
    first_seq: i32,
    base_offset: i64,
) -> Result<(), spi::Error> {
    crate::plan::run(
        "UPDATE kafgres_producer_batches
            SET base_offset = $5
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3 AND first_seq = $4",
        &[
            producer_id.into(),
            (topic_id as i32).into(),
            partition.into(),
            first_seq.into(),
            base_offset.into(),
        ],
    )
}

/// Drop a reservation whose batch was never appended, so a resend is not
/// answered as a duplicate at offset -1.
pub fn release(
    producer_id: i64,
    topic_id: u32,
    partition: i32,
    first_seq: i32,
) -> Result<(), spi::Error> {
    crate::plan::run(
        "DELETE FROM kafgres_producer_batches
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3 AND first_seq = $4
            AND base_offset < 0",
        &[
            producer_id.into(),
            (topic_id as i32).into(),
            partition.into(),
            first_seq.into(),
        ],
    )
}

#[derive(Debug, Clone, Copy)]
pub struct Retention {
    /// Upstream's `producer.id.expiration.ms`. `0` disables.
    pub expiration_ms: i64,
    /// Ceiling on retained producer ids, least-recently-used dropped first. `0` disables.
    pub max_ids: i64,
}

/// Drop producer state that is idle or surplus; returns how many producers went. Two
pub fn sweep(policy: Retention) -> Result<u64, spi::Error> {
    // Producers with an open transaction are exempt, as in Kafka's `ProducerStateManager`:
    // nothing in a transaction refreshes `last_ts`, and without the row EndTxn fails.
    // A transaction beginning concurrently is not yet visible here: `begin_or_continue`
    // refreshes `last_ts`, and victims are locked and skipped if locked.
    let mut dropped = 0;

    if policy.expiration_ms > 0 {
        dropped += Spi::get_one_with_args::<i64>(
            "WITH victims AS (
                 SELECT producer_id FROM kafgres_producers p
                  WHERE last_ts < now() - make_interval(secs => $1 / 1000.0)
                    AND NOT EXISTS (SELECT 1 FROM kafgres_txns t
                                     WHERE t.producer_id = p.producer_id
                                       AND t.state = 'ongoing')
                  ORDER BY last_ts LIMIT $2
                    FOR UPDATE OF p SKIP LOCKED),
                  gone AS (
                DELETE FROM kafgres_producers p
                 USING victims v WHERE p.producer_id = v.producer_id
                RETURNING p.producer_id),
                  cleaned AS (
                DELETE FROM kafgres_producer_batches b
                 USING gone g WHERE b.producer_id = g.producer_id
                RETURNING 1)
             SELECT (SELECT count(*) FROM gone)",
            &[(policy.expiration_ms as f64).into(), SWEEP_BATCH.into()],
        )?
        .unwrap_or(0);
    }

    if policy.max_ids > 0 {
        // Least recently *used*, which is why `record` refreshes `last_ts` at all —
        dropped += Spi::get_one_with_args::<i64>(
            "WITH cut AS (
                 SELECT last_ts AS t FROM kafgres_producers
                  ORDER BY last_ts DESC, producer_id DESC
                 OFFSET $1 LIMIT 1),
                  victims AS (
                 SELECT p.producer_id FROM kafgres_producers p, cut
                  WHERE p.last_ts <= cut.t
                    AND p.last_ts < now() - make_interval(secs => $3)
                    AND NOT EXISTS (SELECT 1 FROM kafgres_txns t
                                     WHERE t.producer_id = p.producer_id
                                       AND t.state = 'ongoing')
                  ORDER BY p.last_ts LIMIT $2
                    FOR UPDATE OF p SKIP LOCKED),
                  gone AS (
                DELETE FROM kafgres_producers p
                 USING victims v WHERE p.producer_id = v.producer_id
                RETURNING p.producer_id),
                  cleaned AS (
                DELETE FROM kafgres_producer_batches b
                 USING gone g WHERE b.producer_id = g.producer_id
                RETURNING 1)
             SELECT (SELECT count(*) FROM gone)",
            &[
                policy.max_ids.into(),
                SWEEP_BATCH.into(),
                EVICTION_FLOOR_SECS.into(),
            ],
        )?
        .unwrap_or(0);
    }

    Ok(dropped as u64)
}

pub fn lock_for_read() -> Result<(), spi::Error> {
    crate::dbtx::lock_tables("kafgres_producers, kafgres_producer_batches")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rebuild_starts_at_the_highest_offset_known_to_have_its_rows() {
        assert_eq!(rebuild_from(Some(40), Some(70)), Some(70));
        assert_eq!(rebuild_from(Some(90), Some(70)), Some(90));
        assert_eq!(rebuild_from(Some(0), None), Some(0), "a new partition is scanned whole");
        assert_eq!(rebuild_from(None, Some(70)), Some(70));
        assert_eq!(rebuild_from(None, None), None, "nothing known: the caller estimates");
    }

    #[test]
    fn in_sequence_accepts_the_next_and_the_wrap() {
        assert!(in_sequence(0, 1));
        assert!(in_sequence(41, 42));
        assert!(!in_sequence(41, 43), "a gap is out of order");
        assert!(!in_sequence(41, 41), "a repeat is not 'next'");
        // A long-lived producer really does exhaust int32; without this its first batch
        assert!(in_sequence(i32::MAX, 0));
        assert!(!in_sequence(i32::MAX, 1));
    }

    #[test]
    fn increment_sequence_wraps_through_zero_not_to_i32_min() {
        assert_eq!(increment_sequence(0, 0), 0);
        assert_eq!(increment_sequence(10, 5), 15);
        // The wrap the client performs, which plain addition gets wrong: a batch based
        assert_eq!(increment_sequence(i32::MAX, 0), i32::MAX);
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX, 2), 1);
        assert_eq!(increment_sequence(i32::MAX - 2, 5), 2);
        assert_ne!(increment_sequence(i32::MAX, 1), i32::MAX.wrapping_add(1));
        // And the two agree, so a batch that ends at the wrap chains into the next one.
        assert!(in_sequence(increment_sequence(i32::MAX - 1, 1), 0));
    }
}
