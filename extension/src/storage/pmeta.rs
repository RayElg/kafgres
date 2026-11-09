//! Partition metadata, shared by both engines: not log data, so not engine-specific.

use pgrx::prelude::*;

use super::{EpochEnd, StoreError, StoreResult, TopicId};

fn spi_err(e: impl std::fmt::Display) -> StoreError {
    StoreError::Io(e.to_string())
}

pub fn log_start_offset(topic: TopicId, partition: i32) -> StoreResult<i64> {
    crate::plan::get_one::<i64>(
        "SELECT (SELECT log_start_offset FROM kafgres_partitions
                  WHERE topic_id = $1::oid AND partition = $2)",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?
    .ok_or(StoreError::UnknownTopicOrPartition)
}

pub fn leader_epoch(topic: TopicId, partition: i32) -> StoreResult<i32> {
    crate::plan::get_one::<i32>(
        "SELECT (SELECT leader_epoch FROM kafgres_partitions
                  WHERE topic_id = $1::oid AND partition = $2)",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?
    .ok_or(StoreError::UnknownTopicOrPartition)
}

/// The partition's metadata rows, including its own epoch-0 history entry. The entry
pub fn create_partition(topic: TopicId, partition: i32, epoch: i32) -> StoreResult<()> {
    Spi::run_with_args(
        "INSERT INTO kafgres_partitions (topic_id, partition, leader_epoch)
         VALUES ($1::oid, $2, $3)
         ON CONFLICT (topic_id, partition) DO NOTHING",
        &[(topic as i32).into(), partition.into(), epoch.into()],
    )
    .map_err(spi_err)?;

    Spi::run_with_args(
        "INSERT INTO kafgres_leader_epochs (topic_id, partition, leader_epoch, start_offset)
         VALUES ($1::oid, $2, $3, 0)
         ON CONFLICT DO NOTHING",
        &[(topic as i32).into(), partition.into(), epoch.into()],
    )
    .map_err(spi_err)
}

pub fn drop_partition(topic: TopicId, partition: i32) -> StoreResult<()> {
    // Markers first: topic ids are reused, and a leftover marker would gate the next
    Spi::run_with_args(
        "DELETE FROM kafgres_markers WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?;
    // Same hazard: a leftover abort-index entry tells the next topic's consumers to
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_aborted WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?;
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_partitions WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?;
    Spi::run_with_args(
        "DELETE FROM kafgres_leader_epochs WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?;
    Spi::run_with_args(
        "DELETE FROM kafgres_partitions WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)
}

/// Record a promotion to `epoch`, whose first offset is `start`. The `DELETE`
pub fn record_epoch(topic: TopicId, partition: i32, epoch: i32, start: i64) -> StoreResult<()> {
    Spi::run_with_args(
        "UPDATE kafgres_partitions
            SET leader_epoch = $3, epoch_start_offset = $4
          WHERE topic_id = $1::oid AND partition = $2",
        &[
            (topic as i32).into(),
            partition.into(),
            epoch.into(),
            start.into(),
        ],
    )
    .map_err(spi_err)?;

    Spi::run_with_args(
        "WITH collapsed AS (
             DELETE FROM kafgres_leader_epochs
              WHERE topic_id = $1::oid AND partition = $2
                AND start_offset >= $4
                AND leader_epoch < $3)
         INSERT INTO kafgres_leader_epochs (topic_id, partition, leader_epoch, start_offset)
         VALUES ($1::oid, $2, $3, $4)
         ON CONFLICT (topic_id, partition, leader_epoch)
         DO UPDATE SET start_offset = EXCLUDED.start_offset",
        &[
            (topic as i32).into(),
            partition.into(),
            epoch.into(),
            start.into(),
        ],
    )
    .map_err(spi_err)
}

pub fn epoch_start_offset(
    topic: TopicId,
    partition: i32,
    epoch: i32,
) -> StoreResult<Option<i64>> {
    // From durable history, never from the log: retention would make the answer creep
    Spi::get_one_with_args::<i64>(
        "SELECT (SELECT start_offset FROM kafgres_leader_epochs
                  WHERE topic_id = $1::oid AND partition = $2 AND leader_epoch = $3)",
        &[(topic as i32).into(), partition.into(), epoch.into()],
    )
    .map_err(spi_err)
}

/// Where the epoch a client last saw ended. A wrong answer is divergence, not an
pub fn epoch_end_offset(
    topic: TopicId,
    partition: i32,
    epoch: i32,
    log_end: impl Fn() -> StoreResult<i64>,
) -> StoreResult<EpochEnd> {
    let current = leader_epoch(topic, partition)?;

    // The current epoch has not ended, so the answer is the log end.
    if epoch == current {
        return Ok(EpochEnd {
            leader_epoch: current,
            end_offset: log_end()?,
        });
    }

    // An epoch *above* ours: the client read under a leader we never were, so we have
    if epoch > current {
        return Ok(EpochEnd {
            leader_epoch: -1,
            end_offset: -1,
        });
    }

    let matched: Option<i32> = Spi::get_one_with_args(
        "SELECT (SELECT max(leader_epoch) FROM kafgres_leader_epochs
                  WHERE topic_id = $1::oid AND partition = $2 AND leader_epoch <= $3)",
        &[(topic as i32).into(), partition.into(), epoch.into()],
    )
    .map_err(spi_err)?;

    let matched = match matched {
        Some(e) => e,
        // Nothing at or below it: `-1` makes the client reset rather than truncate, so
        None => {
            return Ok(EpochEnd {
                leader_epoch: -1,
                end_offset: log_start_offset(topic, partition)?,
            })
        }
    };

    // An epoch ends where the next one begins; reading the next entry stays correct
    let end: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT min(start_offset) FROM kafgres_leader_epochs
                  WHERE topic_id = $1::oid AND partition = $2 AND leader_epoch > $3)",
        &[(topic as i32).into(), partition.into(), matched.into()],
    )
    .map_err(spi_err)?;

    Ok(EpochEnd {
        leader_epoch: matched,
        end_offset: match end {
            Some(e) => e,
            None => log_end()?,
        },
    })
}

pub fn partitions(topic: TopicId) -> StoreResult<Vec<i32>> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT partition FROM kafgres_partitions WHERE topic_id = $1::oid ORDER BY partition",
            None,
            &[(topic as i32).into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(p) = row.get::<i32>(1)? {
                out.push(p);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(spi_err)
}

const MARKERS_FORGOTTEN_PER_PASS: i64 = 20_000;

/// Move `log_start_offset` forward. Never backward — a consumer that has been told an
pub fn advance_log_start(topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
    Spi::run_with_args(
        "UPDATE kafgres_partitions SET log_start_offset = GREATEST(log_start_offset, $3)
          WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into(), offset.into()],
    )
    .map_err(spi_err)?;
    forget_markers_below(topic, partition, offset)?;
    // The abort index describes offsets; when they go its entries are dead, and this is
    forget_aborted_below(topic, partition, offset)
}

/// Batches wholly below the log start are unreadable. Bounded, so a backlog cannot outlast
/// the statement timeout; every retention pass calls this, and the rest goes on later
/// passes. The `base_offset` bound is implied by `last_offset` but lets the planner use the
/// primary key's range instead of scanning.
pub fn forget_markers_below(topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
    Spi::run_with_args(
        "DELETE FROM kafgres_markers
          WHERE topic_id = $1::oid AND partition = $2 AND last_offset < $3
            AND base_offset < LEAST($3, (SELECT base_offset FROM kafgres_markers
                                          WHERE topic_id = $1::oid AND partition = $2
                                            AND base_offset < $3
                                          ORDER BY base_offset OFFSET $4 LIMIT 1))",
        &[(topic as i32).into(), partition.into(), offset.into(), MARKERS_FORGOTTEN_PER_PASS.into()],
    )
    .map_err(spi_err)
}

/// Cap on markers loaded per Fetch. Without it a `read_committed` consumer far behind the
/// LSO could pull an unbounded set into memory; a Rust allocation failure in a Postgres
/// backend is an `abort()`, crashing the whole cluster.
const MARKERS_PER_FETCH: usize = 65_536;

/// Base offsets in `[from, to)` that have a committed marker. MVCC does the work: an
///
/// Also returns the effective ceiling: at the cap, only markers up to the highest one
/// loaded are known, and the caller must not read past it.
pub fn committed_markers(
    topic: TopicId,
    partition: i32,
    from: i64,
    to: i64,
) -> StoreResult<(std::collections::HashSet<i64>, i64)> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT base_offset FROM kafgres_markers
              WHERE topic_id = $1::oid AND partition = $2
                AND base_offset >= $3 AND base_offset < $4
              ORDER BY base_offset LIMIT $5",
            None,
            &[
                (topic as i32).into(),
                partition.into(),
                from.into(),
                to.into(),
                (MARKERS_PER_FETCH as i64).into(),
            ],
        )?;
        let mut out = std::collections::HashSet::new();
        let mut highest = from;
        for row in rows {
            if let Some(b) = row.get::<i64>(1)? {
                out.insert(b);
                highest = highest.max(b);
            }
        }
        // Under the cap the range is covered exhaustively; at the cap, knowledge stops
        // after the highest marker loaded.
        let ceiling = if out.len() < MARKERS_PER_FETCH {
            to
        } else {
            (highest + 1).min(to)
        };
        Ok::<_, spi::Error>((out, ceiling))
    })
    .map_err(spi_err)
}

/// Whether a `kafgres_produce()` batch's marker exists, and whether its writer has finished,
/// in one statement so both see one snapshot. `xid` is the low bits of the writer's xid8; the
/// writer is the xid8 with those bits nearest the snapshot's horizon.
pub fn marker_state(topic: TopicId, partition: i32, base: i64, xid: i64) -> StoreResult<(bool, bool)> {
    // Below 2^40 the id holds the whole xid8, so the plain lookup is exact and cheaper.
    let writer = if xid_epoch_is_zero()? {
        "$4::bigint"
    } else {
        "(SELECT CASE WHEN d < (1::bigint << 39) THEN x - d ELSE x + (1::bigint << 40) - d END
            FROM (SELECT x, (x - $4::bigint) & ((1::bigint << 40) - 1) AS d
                    FROM (SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS x) a) b)"
    };
    Spi::connect(|client| {
        let row = client
            .select(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM kafgres_markers
                                     WHERE topic_id = $1::oid AND partition = $2
                                       AND base_offset = $3),
                            pg_visible_in_snapshot(({writer})::text::xid8, pg_current_snapshot())"
                ),
                Some(1),
                &[(topic as i32).into(), partition.into(), base.into(), xid.into()],
            )?
            .first();
        Ok::<_, spi::Error>((
            row.get::<bool>(1)?.unwrap_or(false),
            row.get::<bool>(2)?.unwrap_or(false),
        ))
    })
    .map_err(spi_err)
}

/// Whether every xid8 so far fits the producer id's xid bits. "No" is permanent; "yes" is
/// rechecked every few minutes, since the counter can cross 2^40 while a process runs.
fn xid_epoch_is_zero() -> StoreResult<bool> {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    const RECHECK_SECS: i64 = 300;
    static CROSSED: AtomicBool = AtomicBool::new(false);
    static ZERO_UNTIL: AtomicI64 = AtomicI64::new(0);

    if CROSSED.load(Ordering::Relaxed) {
        return Ok(false);
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    if now < ZERO_UNTIL.load(Ordering::Relaxed) {
        return Ok(true);
    }
    // `client.select`: `get_one` would assign this read an xid.
    let zero = Spi::connect(|client| {
        client
            .select(
                "SELECT (pg_snapshot_xmax(pg_current_snapshot())::text::bigint >> 40) = 0",
                Some(1),
                &[],
            )?
            .first()
            .get_one::<bool>()
    })
    .map_err(spi_err)?
    .unwrap_or(true);
    if zero {
        ZERO_UNTIL.store(now + RECHECK_SECS, Ordering::Relaxed);
    } else {
        CROSSED.store(true, Ordering::Relaxed);
    }
    Ok(zero)
}

/// Whether an aborted Kafka transaction of `producer_id` covers `offset`.
pub fn aborted_at(topic: TopicId, partition: i32, producer_id: i64, offset: i64) -> StoreResult<bool> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM kafgres_txn_aborted
                         WHERE topic_id = $1::oid AND partition = $2 AND producer_id = $3
                           AND first_offset <= $4 AND last_offset >= $4)",
        &[(topic as i32).into(), partition.into(), producer_id.into(), offset.into()],
    )
    .map(|v| v.unwrap_or(false))
    .map_err(spi_err)
}

/// The Last Stable Offset contributed by in-flight Kafka transactions, if any; `None`
pub fn kafka_txn_lso(topic: TopicId, partition: i32) -> StoreResult<Option<i64>> {
    Spi::get_one_with_args::<i64>(
        "SELECT (SELECT MIN(p.first_offset)
                   FROM kafgres_txn_partitions p
                   JOIN kafgres_txns t ON t.producer_id = p.producer_id
                  WHERE p.topic_id = $1::oid AND p.partition = $2
                    AND p.first_offset >= 0 AND t.state = 'ongoing')",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)
}

/// Register a partition in the ongoing transaction `txn::verify_held_produce` checked. Never
/// begins one. The first batch's offset wins as `first_offset`.
pub fn register_txn_partition(
    producer_id: i64,
    topic: TopicId,
    partition: i32,
    base_offset: i64,
) -> StoreResult<()> {
    Spi::run_with_args(
        "INSERT INTO kafgres_txn_partitions (producer_id, topic_id, partition, first_offset)
         VALUES ($1, $2::oid, $3, $4)
         ON CONFLICT (producer_id, topic_id, partition)
         DO UPDATE SET first_offset = $4
          WHERE kafgres_txn_partitions.first_offset < 0",
        &[
            producer_id.into(),
            (topic as i32).into(),
            partition.into(),
            base_offset.into(),
        ],
    )
    .map_err(spi_err)?;
    Ok(())
}

/// Stamps the transaction's first offset in this partition. Only overwrites the `-1`
/// placeholder, so a later batch of the same transaction leaves it alone.
pub fn stamp_txn_partition(
    producer_id: i64,
    topic: TopicId,
    partition: i32,
    base_offset: i64,
) -> StoreResult<()> {
    Spi::run_with_args(
        "UPDATE kafgres_txn_partitions SET first_offset = $4
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3
            AND first_offset < 0",
        &[
            producer_id.into(),
            (topic as i32).into(),
            partition.into(),
            base_offset.into(),
        ],
    )
    .map_err(spi_err)
}

/// Record an aborted transaction's offset range so consumers can be told to drop it.
pub fn record_aborted_txn(
    topic: TopicId,
    partition: i32,
    producer_id: i64,
    first_offset: i64,
    last_offset: i64,
) -> StoreResult<()> {
    Spi::run_with_args(
        "INSERT INTO kafgres_txn_aborted
                (topic_id, partition, producer_id, first_offset, last_offset)
         VALUES ($1::oid, $2, $3, $4, $5)
         ON CONFLICT (topic_id, partition, first_offset) DO UPDATE
            SET last_offset = EXCLUDED.last_offset,
                producer_id = EXCLUDED.producer_id",
        &[
            (topic as i32).into(),
            partition.into(),
            producer_id.into(),
            first_offset.into(),
            last_offset.into(),
        ],
    )
    .map_err(spi_err)
}

/// How many aborted transactions one Fetch reports, per partition. A row count, so it
pub const MAX_ABORTED_PER_FETCH: i64 = 1_000;

/// Aborted transactions overlapping `[from, to)`, oldest first. `last_offset >= from`
pub fn aborted_txns(
    topic: TopicId,
    partition: i32,
    from: i64,
    to: i64,
) -> StoreResult<Vec<super::AbortedTxn>> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT producer_id, first_offset FROM kafgres_txn_aborted
              WHERE topic_id = $1::oid AND partition = $2
                AND last_offset >= $3 AND first_offset < $4
              ORDER BY first_offset
              LIMIT $5",
            None,
            &[
                (topic as i32).into(),
                partition.into(),
                from.into(),
                to.into(),
                MAX_ABORTED_PER_FETCH.into(),
            ],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(p), Some(f)) = (row.get::<i64>(1)?, row.get::<i64>(2)?) {
                out.push(super::AbortedTxn {
                    producer_id: p,
                    first_offset: f,
                });
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(spi_err)
}

/// Drop abort-index entries for offsets below the log start. Retention and compaction
pub fn forget_aborted_below(topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_aborted
          WHERE topic_id = $1::oid AND partition = $2 AND last_offset < $3",
        &[(topic as i32).into(), partition.into(), offset.into()],
    )
    .map_err(spi_err)
}

/// Markers of batches compaction removed.
pub fn forget_markers(topic: TopicId, partition: i32, bases: &[i64]) -> StoreResult<()> {
    // In pieces, so no one statement nears the statement timeout.
    for chunk in bases.chunks(10_000) {
        Spi::run_with_args(
            "DELETE FROM kafgres_markers
              WHERE topic_id = $1::oid AND partition = $2 AND base_offset = ANY($3)",
            &[(topic as i32).into(), partition.into(), chunk.to_vec().into()],
        )
        .map_err(spi_err)?;
    }
    Ok(())
}
