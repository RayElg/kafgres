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
        // Bump the epoch so any older instance of the same transactional id is fenced.
        let row: Option<String> = Spi::get_one_with_args(
            "WITH up AS (
                 INSERT INTO kafgres_producers (producer_id, producer_epoch, transactional_id)
                 VALUES (nextval('kafgres_producer_id_seq'), 0, $1)
                 ON CONFLICT (transactional_id) DO UPDATE
                    SET producer_epoch = kafgres_producers.producer_epoch + 1,
                        last_ts = now()
                 RETURNING producer_id, producer_epoch)
             SELECT (SELECT producer_id || '|' || producer_epoch FROM up)",
            &[txn_id.into()],
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

struct Retained {
    epoch: i16,
    first_seq: i32,
    last_seq: i32,
    base_offset: i64,
}

/// The window in **insertion order**, oldest first. Ordering by `last_seq` is not
fn window(producer_id: i64, topic_id: u32, partition: i32) -> Result<Vec<Retained>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT producer_epoch, first_seq, last_seq, base_offset
               FROM kafgres_producer_batches
              WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
              ORDER BY added_seq",
            None,
            &[producer_id.into(), (topic_id as i32).into(), partition.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(Retained {
                epoch: row.get::<i32>(1)?.unwrap_or(0) as i16,
                first_seq: row.get::<i32>(2)?.unwrap_or(NO_SEQUENCE),
                last_seq: row.get::<i32>(3)?.unwrap_or(NO_SEQUENCE),
                base_offset: row.get::<i64>(4)?.unwrap_or(-1),
            });
        }
        Ok(out)
    })
}

/// Upstream's `inSequence`, including the wraparound: a long-lived producer really does
fn in_sequence(last_seq: i32, next_seq: i32) -> bool {
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
    let retained = window(producer_id, topic_id, partition)?;

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
    Spi::run_with_args(
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
    Spi::run_with_args(
        "DELETE FROM kafgres_producer_batches
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
            AND added_seq NOT IN (
                SELECT added_seq FROM kafgres_producer_batches
                 WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
                 ORDER BY added_seq DESC LIMIT $4)",
        &[
            producer_id.into(),
            (topic_id as i32).into(),
            partition.into(),
            RETAINED_BATCHES.into(),
        ],
    )?;

    // Keep `last_ts` roughly current without an UPDATE per batch: writes at most once per
    Spi::run_with_args(
        "UPDATE kafgres_producers
            SET last_ts = now()
          WHERE producer_id = $1
            AND last_ts < now() - make_interval(secs => $2)",
        &[producer_id.into(), LAST_TS_GRANULARITY_SECS.into()],
    )?;
    Ok(())
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
    let mut dropped = 0;

    if policy.expiration_ms > 0 {
        dropped += Spi::get_one_with_args::<i64>(
            "WITH victims AS (
                 SELECT producer_id FROM kafgres_producers
                  WHERE last_ts < now() - make_interval(secs => $1 / 1000.0)
                  ORDER BY last_ts LIMIT $2),
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
                  ORDER BY p.last_ts LIMIT $2),
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
    Spi::run(
        "LOCK TABLE kafgres_producers, kafgres_producer_batches IN ACCESS SHARE MODE NOWAIT",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
