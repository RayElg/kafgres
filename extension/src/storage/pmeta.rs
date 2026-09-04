//! Partition metadata, shared by both engines: not log data, so not engine-specific.

use pgrx::prelude::*;

use super::{EpochEnd, StoreError, StoreResult, TopicId};

fn spi_err(e: impl std::fmt::Display) -> StoreError {
    StoreError::Io(e.to_string())
}

pub fn log_start_offset(topic: TopicId, partition: i32) -> StoreResult<i64> {
    Spi::get_one_with_args::<i64>(
        "SELECT (SELECT log_start_offset FROM kafgres_partitions
                  WHERE topic_id = $1::oid AND partition = $2)",
        &[(topic as i32).into(), partition.into()],
    )
    .map_err(spi_err)?
    .ok_or(StoreError::UnknownTopicOrPartition)
}

pub fn leader_epoch(topic: TopicId, partition: i32) -> StoreResult<i32> {
    Spi::get_one_with_args::<i32>(
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

/// Move `log_start_offset` forward. Never backward — a consumer that has been told an
pub fn advance_log_start(topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
    Spi::run_with_args(
        "UPDATE kafgres_partitions SET log_start_offset = GREATEST(log_start_offset, $3)
          WHERE topic_id = $1::oid AND partition = $2",
        &[(topic as i32).into(), partition.into(), offset.into()],
    )
    .map_err(spi_err)?;
    // The abort index describes offsets; when they go its entries are dead, and this is
    forget_aborted_below(topic, partition, offset)
}

/// Base offsets in `[from, to)` that have a committed marker. MVCC does the work: an
pub fn committed_markers(
    topic: TopicId,
    partition: i32,
    from: i64,
    to: i64,
) -> StoreResult<std::collections::HashSet<i64>> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT base_offset FROM kafgres_markers
              WHERE topic_id = $1::oid AND partition = $2
                AND base_offset >= $3 AND base_offset < $4",
            None,
            &[
                (topic as i32).into(),
                partition.into(),
                from.into(),
                to.into(),
            ],
        )?;
        let mut out = std::collections::HashSet::new();
        for row in rows {
            if let Some(b) = row.get::<i64>(1)? {
                out.insert(b);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(spi_err)
}

/// Producer ids allocated by `InitProducerId`; tells a Kafka transaction's batch from
pub fn known_producer_ids() -> StoreResult<std::collections::HashSet<i64>> {
    Spi::connect(|client| {
        let rows = client.select("SELECT producer_id FROM kafgres_producers", None, &[])?;
        let mut out = std::collections::HashSet::new();
        for row in rows {
            if let Some(id) = row.get::<i64>(1)? {
                out.insert(id);
            }
        }
        Ok::<_, spi::Error>(out)
    })
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

/// Note where an in-flight transaction's records begin in this partition. The
pub fn note_txn_first_offset(
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
