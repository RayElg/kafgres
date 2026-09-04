//! Table-backed log storage: the only file permitted to run SQL against the log table

use pgrx::prelude::*;

use kafgres_codec::records::RecordBatch;

use super::{
    pmeta, AbortedTxn, EpochEnd, FetchSlice, IsolationLevel, LogStore, RawBatch,
    RetentionPolicy, StoreError, StoreResult, TopicId, TxnContext,
};

/// Startup engine-mismatch check. `EXISTS`, not `count(*)`: on a mismatch the worker
pub fn log_presence() -> Result<Option<String>, String> {
    let any = Spi::get_one::<bool>("SELECT EXISTS (SELECT 1 FROM kafgres_log)")
        .map(|v| v.unwrap_or(false))
        .map_err(|e| format!("cannot read this engine's log: {e}"))?;
    Ok(any.then(|| "rows in kafgres_log".to_string()))
}

pub const SEGMENT_OFFSETS: i64 = 1_000_000;

fn producer_id_of(view: &kafgres_codec::records::RecordBatch) -> Option<i64> {
    let id = view.producer_id();
    (id >= 0).then_some(id)
}

fn producer_epoch_of(view: &kafgres_codec::records::RecordBatch) -> Option<i16> {
    let e = view.producer_epoch();
    (e >= 0).then_some(e)
}

fn base_seq_of(view: &kafgres_codec::records::RecordBatch) -> Option<i32> {
    let s = view.base_sequence();
    (s >= 0).then_some(s)
}

/// Per-pass cap: the pass runs on the broker's single worker, so its duration freezes every connection.
const MAX_COMPACT_BATCHES: usize = 1_000;

const MAX_COMPACT_BYTES: usize = 32 * 1024 * 1024;

fn segment_offsets() -> i64 {
    crate::segment_offsets()
}

const MAX_SEGMENT_DROPS: i64 = 32;

/// Kafka requires a Fetch to make progress: a first batch larger than the byte cap is
const ALWAYS_RETURN_FIRST_BATCH: bool = true;

#[derive(Debug, Default)]
pub struct TableStore {
    _private: (),
}

impl TableStore {
    pub fn new() -> Self {
        TableStore { _private: () }
    }

    /// Segments entirely below `target`. `max_last_offset`, not `end_offset`: a straddling
    fn doomed(
        &self,
        topic: TopicId,
        partition: i32,
        target: i64,
    ) -> StoreResult<Vec<(i64, String)>> {
        Spi::connect(|client| {
            let rows = client.select(
                "SELECT base_offset, table_name FROM kafgres_log_segments
                  WHERE topic_id = $1::oid AND partition = $2
                    AND max_last_offset < $3
                    AND end_offset <= (SELECT next_offset FROM kafgres_partitions
                                        WHERE topic_id = $1::oid AND partition = $2)
                  ORDER BY base_offset LIMIT $4",
                None,
                &[
                    (topic as i32).into(),
                    partition.into(),
                    target.into(),
                    MAX_SEGMENT_DROPS.into(),
                ],
            )?;
            let mut out = Vec::new();
            for row in rows {
                if let (Some(base), Some(name)) = (row.get::<i64>(1)?, row.get::<String>(2)?) {
                    out.push((base, name));
                }
            }
            Ok::<_, spi::Error>(out)
        })
        .map_err(spi_err)
    }

    /// Check before locking: the partition row lock is held until the SQL caller's
    fn reclaim(&mut self, topic: TopicId, partition: i32, offset: i64) -> StoreResult<u64> {
        let start = self.read_log_start(topic, partition, false)?;
        if offset <= start && self.doomed(topic, partition, start)?.is_empty() {
            return Ok(0);
        }

        let start = self.read_log_start(topic, partition, true)?;
        // Never move the watermark backwards; an earlier call may have left segments at its budget.
        let target = offset.max(start);
        let doomed = self.doomed(topic, partition, target)?;

        for (base, table) in &doomed {
            // The table name comes from our own catalogue row, never client input.
            Spi::run(&format!("DROP TABLE IF EXISTS {table}")).map_err(spi_err)?;
            Spi::run_with_args(
                "DELETE FROM kafgres_log_segments
                  WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3",
                &[(topic as i32).into(), partition.into(), (*base).into()],
            )
            .map_err(spi_err)?;
        }

        // Advance even with nothing reclaimable: DeleteRecords promises unreadability, not disk shrink.
        Spi::run_with_args(
            "UPDATE kafgres_partitions SET log_start_offset = $3
              WHERE topic_id = $1::oid AND partition = $2",
            &[(topic as i32).into(), partition.into(), target.into()],
        )
        .map_err(spi_err)?;
        pmeta::forget_aborted_below(topic, partition, target)?;
        Ok(doomed.len() as u64)
    }

    fn read_log_start(&self, topic: TopicId, partition: i32, lock: bool) -> StoreResult<i64> {
        let sql = if lock {
            "SELECT (SELECT log_start_offset FROM kafgres_partitions
                      WHERE topic_id = $1::oid AND partition = $2 FOR UPDATE)"
        } else {
            "SELECT (SELECT log_start_offset FROM kafgres_partitions
                      WHERE topic_id = $1::oid AND partition = $2)"
        };
        Spi::get_one_with_args(sql, &[(topic as i32).into(), partition.into()])
            .map_err(spi_err)?
            .ok_or(StoreError::UnknownTopicOrPartition)
    }

    fn retention_cutoff(
        &self,
        topic: TopicId,
        partition: i32,
        policy: &RetentionPolicy,
    ) -> StoreResult<i64> {
        let next: i64 = Spi::get_one_with_args(
            "SELECT (SELECT next_offset FROM kafgres_partitions
                      WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .unwrap_or(0);

        let segments: Vec<(i64, i64, i64, i64)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT base_offset, max_last_offset, max_append_ts, bytes
                   FROM kafgres_log_segments
                  WHERE topic_id = $1::oid AND partition = $2 AND end_offset <= $3
                  ORDER BY base_offset",
                None,
                &[(topic as i32).into(), partition.into(), next.into()],
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push((
                    row.get::<i64>(1)?.unwrap_or(0),
                    row.get::<i64>(2)?.unwrap_or(-1),
                    row.get::<i64>(3)?.unwrap_or(0),
                    row.get::<i64>(4)?.unwrap_or(0),
                ));
            }
            Ok::<_, spi::Error>(out)
        })
        .map_err(spi_err)?;

        if segments.is_empty() {
            return Ok(0);
        }

        let past = |max_last: i64| max_last + 1;
        let mut cutoff = 0i64;

        if let Some(ms) = policy.retention_ms {
            let horizon = now_millis().saturating_sub(ms);
            for (_, max_last, newest, _) in &segments {
                if *newest <= horizon {
                    cutoff = cutoff.max(past(*max_last));
                } else {
                    break; // Segments are offset-ordered, so the rest are newer.
                }
            }
        }

        if let Some(budget) = policy.retention_bytes {
            let live: i64 = Spi::get_one_with_args(
                "SELECT (SELECT COALESCE(SUM(bytes), 0)::bigint
                           FROM kafgres_log_segments
                          WHERE topic_id = $1::oid AND partition = $2)",
                &[(topic as i32).into(), partition.into()],
            )
            .map_err(spi_err)?
            .unwrap_or(0);

            let mut over = live - budget;
            for (_, max_last, _, bytes) in &segments {
                if over <= 0 {
                    break;
                }
                over -= bytes;
                cutoff = cutoff.max(past(*max_last));
            }
        }

        Ok(cutoff)
    }
}

/// Includes the partitioned parent, which `CREATE TABLE ... PARTITION OF` and `DROP TABLE` contend on.
pub fn lock_for_read() -> Result<(), pgrx::spi::Error> {
    Spi::run("LOCK TABLE kafgres_log, kafgres_log_segments IN ACCESS SHARE MODE NOWAIT")
}

fn spi_err(e: impl std::fmt::Display) -> StoreError {
    StoreError::Io(e.to_string())
}

fn topic_table(topic: TopicId) -> String {
    format!("kafgres_log_t{topic}")
}

fn segment_table(topic: TopicId, partition: i32, seg: i64) -> String {
    format!("kafgres_log_t{topic}_p{partition}_s{seg}")
}

fn segment_index_of(offset: i64, size: i64) -> i64 {
    offset.div_euclid(size)
}

fn segment_index(offset: i64) -> i64 {
    segment_index_of(offset, segment_offsets())
}

// TODO: pre-create segments ahead of the write path from the retention worker.
fn ensure_segment(topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
    let size = segment_offsets();
    let seg = segment_index_of(offset, size);
    let lo = seg * size;
    let hi = lo + size;
    let name = segment_table(topic, partition, seg);

    // EXISTS: pgrx's get_one errors on a zero-row result, so scalar queries here must return exactly one row.
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM kafgres_log_segments
          WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3)",
        &[(topic as i32).into(), partition.into(), lo.into()],
    )
    .map_err(spi_err)?
    .unwrap_or(false);
    if exists {
        return Ok(());
    }

    Spi::run(&format!(
        "CREATE TABLE IF NOT EXISTS {name}
           PARTITION OF {parent}
           FOR VALUES FROM ({partition}, {lo}) TO ({partition}, {hi})",
        parent = topic_table(topic)
    ))
    .map_err(spi_err)?;

    Spi::run_with_args(
        "INSERT INTO kafgres_log_segments (topic_id, partition, base_offset, end_offset, table_name)
         VALUES ($1::oid, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        &[
            (topic as i32).into(),
            partition.into(),
            lo.into(),
            hi.into(),
            name.into(),
        ],
    )
    .map_err(spi_err)?;
    Ok(())
}

impl LogStore for TableStore {
    /// Offsets are assigned in commit order: producers serialise on the partition row's `FOR UPDATE`.
    fn append(
        &mut self,
        topic: TopicId,
        partition: i32,
        batch: RawBatch,
        _txn: Option<&TxnContext>,
    ) -> StoreResult<i64> {
        // FOR UPDATE inside a scalar subquery so an unknown partition yields one NULL row, not zero.
        let next: i64 = Spi::get_one_with_args(
            "SELECT (SELECT next_offset FROM kafgres_partitions
              WHERE topic_id = $1::oid AND partition = $2
              FOR UPDATE)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .ok_or(StoreError::UnknownTopicOrPartition)?;

        let epoch: i32 = Spi::get_one_with_args(
            "SELECT (SELECT leader_epoch FROM kafgres_partitions
              WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .unwrap_or(0);

        let base_offset = next;
        let last_offset = base_offset + batch.last_offset_delta as i64;

        // Stamp under the row lock: a consumer reads baseOffset from the batch header, not the row.
        let stamped = RecordBatch::validated(kafgres_codec::bytes::Bytes::from(batch.bytes))
            .map_err(|_| StoreError::CorruptBatch)?
            .stamp(base_offset, epoch);
        let stored: Vec<u8> = stamped.into_bytes().to_vec();
        let stored_len = stored.len();

        ensure_segment(topic, partition, base_offset)?;
        if segment_index(last_offset) != segment_index(base_offset) {
            ensure_segment(topic, partition, last_offset)?;
        }

        Spi::run_with_args(
            "INSERT INTO kafgres_log
               (topic_id, partition, base_offset, last_offset, batch, append_ts,
                max_timestamp, record_count, producer_id, producer_epoch, base_seq,
                leader_epoch, is_txn, is_control)
             VALUES ($1::oid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            &[
                (topic as i32).into(),
                partition.into(),
                base_offset.into(),
                last_offset.into(),
                stored.into(),
                now_millis().into(),
                batch.max_timestamp.into(),
                batch.record_count.into(),
                batch.producer_id.into(),
                (batch.producer_epoch as i32).into(),
                batch.base_sequence.into(),
                epoch.into(),
                batch.is_transactional.into(),
                batch.is_control.into(),
            ],
        )
        .map_err(spi_err)?;

        Spi::run_with_args(
            "UPDATE kafgres_log_segments
                SET max_last_offset = GREATEST(max_last_offset, $4),
                    max_append_ts   = GREATEST(max_append_ts, $5),
                    bytes           = bytes + $6
              WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3",
            &[
                (topic as i32).into(),
                partition.into(),
                (segment_index_of(base_offset, segment_offsets()) * segment_offsets()).into(),
                last_offset.into(),
                now_millis().into(),
                (stored_len as i64).into(),
            ],
        )
        .map_err(spi_err)?;

        Spi::run_with_args(
            "UPDATE kafgres_partitions SET next_offset = $3
              WHERE topic_id = $1::oid AND partition = $2",
            &[
                (topic as i32).into(),
                partition.into(),
                (last_offset + 1).into(),
            ],
        )
        .map_err(spi_err)?;

        Ok(base_offset)
    }

    fn read(
        &self,
        topic: TopicId,
        partition: i32,
        offset: i64,
        max_bytes: usize,
        isolation: IsolationLevel,
    ) -> StoreResult<FetchSlice> {
        let high_watermark = self.high_watermark(topic, partition)?;
        let log_start = self.log_start_offset(topic, partition)?;
        let lso = pmeta::kafka_txn_lso(topic, partition)?
            .filter(|o| *o >= 0)
            .map_or(high_watermark, |o| o.min(high_watermark));
        let ceiling = match isolation {
            IsolationLevel::ReadCommitted => lso,
            IsolationLevel::ReadUncommitted => high_watermark,
        };

        if offset < log_start || offset > high_watermark {
            return Err(StoreError::OffsetOutOfRange);
        }

        let mut bytes: Vec<u8> = Vec::new();
        let mut next_offset = offset;
        let mut aborted: Vec<AbortedTxn> = Vec::new();

        if offset < ceiling {

        Spi::connect(|client| {
            // Start at the batch *containing* `offset`, not the first at or past it: resume
            let rows = client.select(
                "SELECT base_offset, last_offset, batch
                   FROM kafgres_log
                  WHERE topic_id = $1::oid AND partition = $2
                    AND base_offset >= COALESCE(
                        (SELECT MAX(base_offset) FROM kafgres_log
                          WHERE topic_id = $1::oid AND partition = $2
                            AND base_offset <= $3 AND last_offset >= $3),
                        $3)
                    AND base_offset < $4
                  ORDER BY base_offset
                  LIMIT 1000",
                None,
                &[
                    (topic as i32).into(),
                    partition.into(),
                    offset.into(),
                    ceiling.into(),
                ],
            )?;

            for row in rows {
                let last: i64 = row.get(2)?.unwrap_or(0);
                let blob: Vec<u8> = match row.get::<Vec<u8>>(3)? {
                    Some(b) => b,
                    None => continue,
                };
                if !bytes.is_empty() && bytes.len() + blob.len() > max_bytes {
                    break;
                }
                bytes.extend_from_slice(&blob);
                next_offset = last + 1;
                if bytes.len() >= max_bytes && ALWAYS_RETURN_FIRST_BATCH {
                    break;
                }
            }
            Ok::<_, spi::Error>(())
        })
        .map_err(spi_err)?;

            // Aborted txns from the index, not the batches in hand: a txn spanning fetches reads as committed.
            if matches!(isolation, IsolationLevel::ReadCommitted) {
                aborted = pmeta::aborted_txns(topic, partition, offset, next_offset.max(offset + 1))?;
            }
        }

        Ok(FetchSlice {
            bytes,
            next_offset,
            high_watermark,
            log_start_offset: log_start,
            last_stable_offset: lso,
            aborted,
        })
    }

    fn offset_for_timestamp(
        &self,
        topic: TopicId,
        partition: i32,
        timestamp: i64,
    ) -> StoreResult<Option<i64>> {
        Spi::get_one_with_args::<i64>(
            "SELECT (SELECT base_offset FROM kafgres_log
              WHERE topic_id = $1::oid AND partition = $2 AND max_timestamp >= $3
              ORDER BY base_offset
              LIMIT 1)",
            &[(topic as i32).into(), partition.into(), timestamp.into()],
        )
        .map_err(spi_err)
    }

    fn high_watermark(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        Spi::get_one_with_args::<i64>(
            "SELECT (SELECT next_offset FROM kafgres_partitions
              WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .ok_or(StoreError::UnknownTopicOrPartition)
    }

    fn last_stable_offset_if_tracked(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<i64>> {
        self.last_stable_offset(topic, partition).map(Some)
    }

    fn high_watermark_if_tracked(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<i64>> {
        self.high_watermark(topic, partition).map(Some)
    }

    fn log_start_offset(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        Spi::get_one_with_args::<i64>(
            "SELECT (SELECT log_start_offset FROM kafgres_partitions
              WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .ok_or(StoreError::UnknownTopicOrPartition)
    }

    fn partition_bytes(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        Spi::get_one_with_args::<i64>(
            "SELECT (SELECT COALESCE(SUM(bytes), 0)::bigint FROM kafgres_log_segments
                      WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .ok_or(StoreError::UnknownTopicOrPartition)
    }

    fn log_dir(&self) -> String {
        unsafe {
            std::ffi::CStr::from_ptr(pgrx::pg_sys::DataDir)
                .to_string_lossy()
                .into_owned()
        }
    }

    /// `DROP TABLE` on whole segments, never `DELETE` — dead tuples would outrun autovacuum.
    fn truncate_below(&mut self, topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
        self.reclaim(topic, partition, offset).map(|_| ())
    }

    /// One pass over a partition in a single Postgres transaction, so a Fetch never sees it half-rewritten.
    fn compact(&mut self, topic: TopicId, partition: i32) -> StoreResult<u64> {
        use kafgres_codec::compaction::{rebuild_batch, survivors_until, KeptRecord};
        use kafgres_codec::records::RecordBatch;

        // Active region = a whole segment of offset headroom, not one batch: one batch is
        let high_watermark = self.high_watermark(topic, partition)?;
        let offset_ceiling = high_watermark - segment_offsets();

        let limits = crate::config::compaction_limits(topic);
        let now = now_millis();
        // $5/$6 stay separate bounds: min-ing them would let a short lag widen the region.
        let lag_cutoff = now - limits.min_compaction_lag_ms;
        let age_ceiling = now - crate::config::segment_ms(topic);
        let tombstone_cutoff = now - limits.delete_retention_ms;

        // Byte arm measured over batches, not the segments table: a small topic's single segment never triggers.
        let seg_bytes = crate::config::segment_bytes(topic);
        let byte_ceiling: i64 = Spi::get_one_with_args(
            "SELECT (
                SELECT COALESCE(MIN(base_offset), 0) FROM (
                    SELECT base_offset,
                           SUM(length(batch)) OVER (ORDER BY base_offset DESC) AS behind
                      FROM (SELECT base_offset, batch FROM kafgres_log
                             WHERE topic_id = $1::oid AND partition = $2
                             ORDER BY base_offset DESC
                             LIMIT $4) newest_first
                ) t WHERE behind <= $3)",
            &[
                (topic as i32).into(),
                partition.into(),
                seg_bytes.into(),
                (MAX_COMPACT_BATCHES as i64).into(),
            ],
        )
        .map_err(spi_err)?
        .unwrap_or(0);

        let newest: i64 = Spi::get_one_with_args(
            "SELECT (SELECT COALESCE(MAX(base_offset), -1) FROM kafgres_log
                      WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .unwrap_or(-1);
        if newest <= 0 {
            return Ok(0);
        }

        let rows: Vec<(i64, Vec<u8>)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT base_offset, batch FROM kafgres_log
                  WHERE topic_id = $1::oid AND partition = $2
                    AND (base_offset < $3 OR append_ts <= $6 OR base_offset < $8)
                    AND append_ts <= $5
                    AND base_offset < $7
                  ORDER BY base_offset
                  LIMIT $4",
                None,
                &[
                    (topic as i32).into(),
                    partition.into(),
                    offset_ceiling.into(),
                    (MAX_COMPACT_BATCHES as i64).into(),
                    lag_cutoff.into(),
                    age_ceiling.into(),
                    // Never the newest batch: an appender may be about to extend it.
                    newest.into(),
                    byte_ceiling.into(),
                ],
            )?;
            let mut out = Vec::new();
            let mut bytes = 0usize;
            for row in rows {
                if let (Some(base), Some(blob)) = (row.get::<i64>(1)?, row.get::<Vec<u8>>(2)?) {
                    bytes += blob.len();
                    out.push((base, blob));
                    if bytes >= MAX_COMPACT_BYTES {
                        break;
                    }
                }
            }
            Ok::<_, spi::Error>(out)
        })
        .map_err(spi_err)?;
        if rows.is_empty() {
            return Ok(0);
        }

        let batches: Vec<RecordBatch> = rows
            .iter()
            .map(|(_, blob)| {
                RecordBatch::new(kafgres_codec::bytes::Bytes::copy_from_slice(blob))
                    .map_err(|e| StoreError::Io(format!("compaction decode: {e}")))
            })
            .collect::<Result<_, _>>()?;

        let keep = survivors_until(&batches, tombstone_cutoff)
            .map_err(|e| StoreError::Io(format!("compaction survivors: {e}")))?;

        let mut removed = 0u64;
        for (row, batch) in rows.iter().zip(batches.iter()) {
            let (base, _) = row;
            if batch.is_control() {
                continue;
            }
            let mut kept = Vec::new();
            let mut total = 0usize;
            for record in batch
                .records_decompressed()
                .map_err(|e| StoreError::Io(format!("compaction records: {e}")))?
            {
                let record = record.map_err(|e| StoreError::Io(format!("compaction record: {e}")))?;
                total += 1;
                let offset = base + record.offset_delta as i64;
                if keep.keeps(offset) {
                    kept.push(KeptRecord {
                        offset,
                        timestamp: batch.base_timestamp() + record.timestamp_delta,
                        key: record.key,
                        value: record.value,
                        headers: record.headers,
                        attributes: record.attributes,
                    });
                }
            }
            if kept.len() == total {
                continue;
            }
            removed += (total - kept.len()) as u64;

            // DELETE then INSERT, not UPDATE: the new base offset (first survivor) is the partition key.
            Spi::run_with_args(
                "DELETE FROM kafgres_log
                  WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3",
                &[(topic as i32).into(), partition.into(), (*base).into()],
            )
            .map_err(spi_err)?;

            let old_bytes = batch.as_bytes().len() as i64;
            let segment_base = segment_index_of(*base, segment_offsets()) * segment_offsets();
            Spi::run_with_args(
                "UPDATE kafgres_log_segments SET bytes = GREATEST(bytes - $4, 0)
                  WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3",
                &[
                    (topic as i32).into(),
                    partition.into(),
                    segment_base.into(),
                    old_bytes.into(),
                ],
            )
            .map_err(spi_err)?;

            if let Some(rebuilt) = rebuild_batch(batch, &kept) {
                let view = RecordBatch::new(rebuilt.clone())
                    .map_err(|e| StoreError::Io(format!("compaction rebuild: {e}")))?;
                let new_base = view.base_offset();
                let last = new_base + view.last_offset_delta() as i64;
                Spi::run_with_args(
                    "INSERT INTO kafgres_log (topic_id, partition, base_offset, last_offset,
                                              batch, append_ts, max_timestamp, record_count,
                                              leader_epoch, producer_id, producer_epoch,
                                              base_seq, is_txn)
                     VALUES ($1::oid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                    &[
                        (topic as i32).into(),
                        partition.into(),
                        new_base.into(),
                        last.into(),
                        rebuilt.to_vec().into(),
                        // `now`, as `append` uses: time retention reads this column, so client clocks must not steer it.
                        now_millis().into(),
                        view.max_timestamp().into(),
                        (view.record_count()).into(),
                        view.partition_leader_epoch().into(),
                        producer_id_of(&view).into(),
                        producer_epoch_of(&view).into(),
                        base_seq_of(&view).into(),
                        view.is_transactional().into(),
                    ],
                )
                .map_err(spi_err)?;

                let new_segment =
                    segment_index_of(new_base, segment_offsets()) * segment_offsets();
                Spi::run_with_args(
                    "UPDATE kafgres_log_segments
                        SET bytes = bytes + $4,
                            max_last_offset = GREATEST(max_last_offset, $5)
                      WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3",
                    &[
                        (topic as i32).into(),
                        partition.into(),
                        new_segment.into(),
                        (rebuilt.len() as i64).into(),
                        last.into(),
                    ],
                )
                .map_err(spi_err)?;
            }
        }

        Ok(removed)
    }

    fn enforce_retention(
        &mut self,
        topic: TopicId,
        policy: &RetentionPolicy,
    ) -> StoreResult<u64> {
        if policy.retention_ms.is_none() && policy.retention_bytes.is_none() {
            return Ok(0);
        }

        let partitions: Vec<i32> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT partition FROM kafgres_partitions WHERE topic_id = $1::oid",
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
        .map_err(spi_err)?;

        let mut dropped = 0;
        for partition in partitions {
            let cutoff = self.retention_cutoff(topic, partition, policy)?;
            dropped += self.reclaim(topic, partition, cutoff)?;
        }
        Ok(dropped)
    }

    /// Refused: a SQL caller holds the partition row lock until its own transaction ends, serialising producers.
    fn append_replicated(
        &mut self,
        _topic: TopicId,
        _partition: i32,
        _bytes: &[u8],
        _expected_base: i64,
    ) -> StoreResult<i64> {
        Err(StoreError::NotImplemented(
            "log replication on the table engine (its log is already replicated by Postgres WAL)",
        ))
    }

    fn append_pending(
        &mut self,
        _topic: TopicId,
        _partition: i32,
        _batch: RawBatch,
    ) -> StoreResult<(i64, i64)> {
        Err(StoreError::NotImplemented(
            "transactional produce on the table engine (set kafgres.storage_engine = 'segment')",
        ))
    }

    fn create_partition(&mut self, topic: TopicId, partition: i32, epoch: i32) -> StoreResult<()> {
        Spi::run(&format!(
            "CREATE TABLE IF NOT EXISTS {child}
               PARTITION OF kafgres_log FOR VALUES IN ({topic})
               PARTITION BY RANGE (partition, base_offset)",
            child = topic_table(topic)
        ))
        .map_err(spi_err)?;

        Spi::run_with_args(
            "INSERT INTO kafgres_partitions (topic_id, partition, leader_epoch)
             VALUES ($1::oid, $2, $3)
             ON CONFLICT (topic_id, partition) DO NOTHING",
            &[(topic as i32).into(), partition.into(), epoch.into()],
        )
        .map_err(spi_err)?;

        // Recorded here, not left to the worker-start backfill: a skip-if-recorded
        Spi::run_with_args(
            "INSERT INTO kafgres_leader_epochs
                    (topic_id, partition, leader_epoch, start_offset)
             VALUES ($1::oid, $2, $3, 0)
             ON CONFLICT DO NOTHING",
            &[(topic as i32).into(), partition.into(), epoch.into()],
        )
        .map_err(spi_err)?;

        ensure_segment(topic, partition, 0)
    }

    fn drop_partition(&mut self, topic: TopicId, partition: i32) -> StoreResult<()> {
        // Clears txn markers, the abort index, partition registration and leader-epoch
        pmeta::drop_partition(topic, partition)?;

        let segments: Vec<String> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT table_name FROM kafgres_log_segments
                  WHERE topic_id = $1::oid AND partition = $2",
                None,
                &[(topic as i32).into(), partition.into()],
            )?;
            let mut out = Vec::new();
            for row in rows {
                if let Some(n) = row.get::<String>(1)? {
                    out.push(n);
                }
            }
            Ok::<_, spi::Error>(out)
        })
        .map_err(spi_err)?;

        for name in segments {
            Spi::run(&format!("DROP TABLE IF EXISTS {name}")).map_err(spi_err)?;
        }
        Spi::run_with_args(
            "DELETE FROM kafgres_log_segments WHERE topic_id = $1::oid AND partition = $2",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?;
        Spi::run_with_args(
            "DELETE FROM kafgres_partitions WHERE topic_id = $1::oid AND partition = $2",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?;

        let remaining: i64 = Spi::get_one_with_args(
            "SELECT count(*) FROM kafgres_partitions WHERE topic_id = $1::oid",
            &[(topic as i32).into()],
        )
        .map_err(spi_err)?
        .unwrap_or(0);
        if remaining == 0 {
            Spi::run(&format!(
                "DROP TABLE IF EXISTS {}",
                topic_table(topic)
            ))
            .map_err(spi_err)?;
        }
        Ok(())
    }

    fn leader_epoch(&self, topic: TopicId, partition: i32) -> StoreResult<i32> {
        Spi::get_one_with_args::<i32>(
            "SELECT (SELECT leader_epoch FROM kafgres_partitions
              WHERE topic_id = $1::oid AND partition = $2)",
            &[(topic as i32).into(), partition.into()],
        )
        .map_err(spi_err)?
        .ok_or(StoreError::UnknownTopicOrPartition)
    }

    /// A promotion must record the new epoch's start offset, or `epoch_end_offset` has no answer.
    fn set_leader_epoch(
        &mut self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<bool> {
        let current = self.leader_epoch(topic, partition)?;
        if epoch <= current {
            return Ok(false);
        }

        Spi::run_with_args(
            "UPDATE kafgres_partitions
                SET leader_epoch = $3, epoch_start_offset = next_offset
              WHERE topic_id = $1::oid AND partition = $2",
            &[(topic as i32).into(), partition.into(), epoch.into()],
        )
        .map_err(spi_err)?;

        // Kafka's LeaderEpochFileCache.assign collapse rule: an epoch starting where the previous one did replaces it.
        Spi::run_with_args(
            "WITH here AS (
                 SELECT next_offset AS start FROM kafgres_partitions
                  WHERE topic_id = $1::oid AND partition = $2),
                  collapsed AS (
                 DELETE FROM kafgres_leader_epochs
                  WHERE topic_id = $1::oid AND partition = $2
                    AND start_offset >= (SELECT start FROM here)
                    AND leader_epoch < $3)
             INSERT INTO kafgres_leader_epochs
                    (topic_id, partition, leader_epoch, start_offset)
             SELECT $1::oid, $2, $3, start FROM here
             ON CONFLICT (topic_id, partition, leader_epoch)
             DO UPDATE SET start_offset = EXCLUDED.start_offset",
            &[(topic as i32).into(), partition.into(), epoch.into()],
        )
        .map_err(spi_err)?;

        Ok(true)
    }

    fn epoch_start_offset(
        &self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<Option<i64>> {
        // From durable history, never the log: retention drops log rows and the answer would creep forward.
        Spi::get_one_with_args::<i64>(
            "SELECT (SELECT start_offset FROM kafgres_leader_epochs
                      WHERE topic_id = $1::oid AND partition = $2 AND leader_epoch = $3)",
            &[(topic as i32).into(), partition.into(), epoch.into()],
        )
        .map_err(spi_err)
    }

    fn epoch_end_offset(
        &self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<EpochEnd> {
        let current = self.leader_epoch(topic, partition)?;

        if epoch == current {
            return Ok(EpochEnd {
                leader_epoch: current,
                end_offset: self.high_watermark(topic, partition)?,
            });
        }

        // An epoch above ours: the client read from a diverged leader; `-1/-1` sends it to offset reset.
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
            None => {
                return Ok(EpochEnd {
                    leader_epoch: -1,
                    end_offset: self.log_start_offset(topic, partition)?,
                })
            }
        };

        // An epoch ends where the next one begins — correct even where the collapse rule left no entry.
        let end: Option<i64> = Spi::get_one_with_args(
            "SELECT (SELECT start_offset FROM kafgres_leader_epochs
                      WHERE topic_id = $1::oid AND partition = $2 AND leader_epoch > $3
                      ORDER BY leader_epoch LIMIT 1)",
            &[(topic as i32).into(), partition.into(), matched.into()],
        )
        .map_err(spi_err)?;

        Ok(EpochEnd {
            leader_epoch: matched,
            end_offset: match end {
                Some(o) => o,
                None => self.high_watermark(topic, partition)?,
            },
        })
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_are_indexed_by_offset_range() {
        // `segment_index` reads a GUC, which pgrx refuses off the main thread — test the two-arg form.
        assert_eq!(segment_index_of(0, SEGMENT_OFFSETS), 0);
        assert_eq!(segment_index_of(SEGMENT_OFFSETS - 1, SEGMENT_OFFSETS), 0);
        assert_eq!(segment_index_of(SEGMENT_OFFSETS, SEGMENT_OFFSETS), 1);
        assert_eq!(segment_index_of(SEGMENT_OFFSETS * 3 + 7, SEGMENT_OFFSETS), 3);
        assert_eq!(segment_index_of(0, 10), 0);
        assert_eq!(segment_index_of(9, 10), 0);
        assert_eq!(segment_index_of(10, 10), 1);
    }

    #[test]
    fn segment_names_are_unique_per_topic_partition_and_range() {
        assert_eq!(segment_table(42, 0, 0), "kafgres_log_t42_p0_s0");
        assert_ne!(segment_table(42, 0, 0), segment_table(42, 1, 0));
        assert_ne!(segment_table(42, 0, 0), segment_table(43, 0, 0));
        assert_ne!(segment_table(42, 0, 0), segment_table(42, 0, 1));
    }
}
