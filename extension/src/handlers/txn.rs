//! `24 AddPartitionsToTxn` and `26 EndTxn` — the transaction coordinator: remember which

use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use kafgres_codec::generated::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponse, AddPartitionsToTxnTopicResult,
};
use kafgres_codec::generated::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafgres_codec::generated::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use kafgres_codec::generated::end_txn_request::EndTxnRequest;
use kafgres_codec::generated::txn_offset_commit_request::TxnOffsetCommitRequest;
use kafgres_codec::generated::txn_offset_commit_response::{
    TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafgres_codec::generated::end_txn_response::EndTxnResponse;
use kafgres_codec::generated::write_txn_markers_request::WriteTxnMarkersRequest;
use kafgres_codec::generated::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    WriteTxnMarkersResponse,
};
use kafgres_codec::records::{build_control_batch, RecordBatch};

use super::HandlerError;
use crate::storage::RawBatch;

pub(super) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Reject a producer whose epoch is behind the one we have fenced to — a zombie instance of
fn fenced(producer_id: i64, epoch: i16) -> Result<bool, HandlerError> {
    let current: Option<i32> = Spi::get_one_with_args(
        "SELECT (SELECT producer_epoch FROM kafgres_producers WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(match current {
        Some(c) => (epoch as i32) < c,
        None => true,
    })
}

pub fn handle_add_partitions(
    req: &AddPartitionsToTxnRequest,
    version: i16,
) -> Result<AddPartitionsToTxnResponse, HandlerError> {
    // v4+ batches several transactions into one request; v3 and below carry exactly one
    let (txn_id, producer_id, epoch, topics) = if version >= 4 {
        match req.transactions.first() {
            Some(t) => (
                t.transactional_id.clone(),
                t.producer_id,
                t.producer_epoch,
                t.topics.clone(),
            ),
            None => return Ok(AddPartitionsToTxnResponse::default()),
        }
    } else {
        (
            req.v3_and_below_transactional_id.clone(),
            req.v3_and_below_producer_id,
            req.v3_and_below_producer_epoch,
            req.v3_and_below_topics.clone(),
        )
    };

    let code = if fenced(producer_id, epoch)? {
        ErrorCode::InvalidProducerEpoch
    } else {
        ErrorCode::None
    };

    if code == ErrorCode::None {
        // A transaction *begins* here if this producer's row is not already `ongoing`.
        let beginning: bool = Spi::get_one_with_args::<bool>(
            "SELECT (SELECT COALESCE(
                        (SELECT state <> 'ongoing' FROM kafgres_txns WHERE producer_id = $1),
                        true))",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?
        .unwrap_or(true);
        if beginning {
            for table in ["kafgres_txn_partitions", "kafgres_txn_offsets"] {
                Spi::run_with_args(
                    &format!("DELETE FROM {table} WHERE producer_id = $1"),
                    &[producer_id.into()],
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
        }

        Spi::run_with_args(
            "INSERT INTO kafgres_txns
                    (producer_id, producer_epoch, transactional_id, state, started_at)
             VALUES ($1, $2, $3, 'ongoing', $4)
             ON CONFLICT (producer_id) DO UPDATE
                SET producer_epoch = EXCLUDED.producer_epoch,
                    state = 'ongoing',
                    -- Only when a transaction actually *begins*. A producer reuses its
                    -- row, so leaving `started_at` alone means its second transaction
                    -- inherits the first one's clock and the expiry sweep aborts it while
                    -- it is live — a working producer failing for no reason it can see.
                    -- Already 'ongoing' means this is a later `AddPartitionsToTxn` inside
                    -- the same transaction, which must not extend its deadline either.
                    started_at = CASE WHEN kafgres_txns.state = 'ongoing'
                                      THEN kafgres_txns.started_at
                                      ELSE EXCLUDED.started_at END",
            &[
                producer_id.into(),
                (epoch as i32).into(),
                txn_id.clone().into(),
                now_millis().into(),
            ],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        for topic in &topics {
            let topic_id = crate::meta::topic_id_by_name(&topic.name)
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            let Some(topic_id) = topic_id else { continue };
            // Range-check: a row naming a nonexistent partition is invisible to
            let count = crate::meta::partition_count(topic_id)
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            for partition in &topic.partitions {
                if *partition < 0 || *partition >= count {
                    continue;
                }
                Spi::run_with_args(
                    "INSERT INTO kafgres_txn_partitions (producer_id, topic_id, partition)
                     VALUES ($1, $2::oid, $3) ON CONFLICT DO NOTHING",
                    &[producer_id.into(), (topic_id as i32).into(), (*partition).into()],
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
        }
    }

    let results: Vec<AddPartitionsToTxnTopicResult> = topics
        .iter()
        .map(|t| AddPartitionsToTxnTopicResult {
            name: t.name.clone(),
            results_by_partition: t
                .partitions
                .iter()
                .map(|p| AddPartitionsToTxnPartitionResult {
                    partition_index: *p,
                    partition_error_code: code.code(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();

    Ok(AddPartitionsToTxnResponse {
        throttle_time_ms: 0,
        error_code: if version >= 4 { code.code() } else { 0 },
        results_by_topic_v3_and_below: if version >= 4 { Vec::new() } else { results.clone() },
        results_by_transaction: Vec::new(),
        ..Default::default()
    })
}

/// Commit or abort: write a marker to every partition the transaction touched. Markers
pub fn handle_end_txn(req: &EndTxnRequest) -> Result<EndTxnResponse, HandlerError> {
    if fenced(req.producer_id, req.producer_epoch)? {
        return Ok(EndTxnResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::InvalidProducerEpoch.code(),
            ..Default::default()
        });
    }

    // Lost to the expiry sweep, a newer instance, or an operator via `WriteTxnMarkers`: in
    match finish_transaction(req.producer_id, req.producer_epoch, req.committed)? {
        Finish::NotOurs | Finish::Overridden => {
            return Ok(EndTxnResponse {
                throttle_time_ms: 0,
                error_code: ErrorCode::InvalidProducerEpoch.code(),
                ..Default::default()
            })
        }
        Finish::Ended => {}
    }

    Ok(EndTxnResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        ..Default::default()
    })
}

/// What `finish_transaction` did, which is not always what it was asked to do.
#[derive(PartialEq, Eq, Debug)]
pub enum Finish {
    /// The transaction is not this caller's to end.
    NotOurs,
    Ended,
    /// Ended with the outcome an operator had already forced; the caller must not be told
    Overridden,
}

fn finish_transaction(
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<Finish, HandlerError> {
    let requested = committed;
    // Lock the transaction row and re-check under it: without the lock, the expiry sweep
    let still_ours: Option<bool> = Spi::get_one_with_args(
        "SELECT (SELECT t.state = 'ongoing' AND p.producer_epoch <= $2
                   FROM kafgres_txns t
                   JOIN kafgres_producers p ON p.producer_id = t.producer_id
                  WHERE t.producer_id = $1
                    FOR UPDATE OF t)",
        &[producer_id.into(), (producer_epoch as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if still_ours != Some(true) {
        return Ok(Finish::NotOurs);
    }

    // An operator already forced a result on at least one partition; the rest must get the
    let forced: Option<bool> = Spi::get_one_with_args(
        "SELECT (SELECT forced_result FROM kafgres_txns WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let committed = forced.unwrap_or(committed);
    let overridden = matches!(forced, Some(f) if f != requested);

    // `first_offset` is read before the markers land: the append is what makes the row's
    let partitions: Vec<(u32, i32, i64)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT tp.topic_id::int, tp.partition, tp.first_offset
               FROM kafgres_txn_partitions tp
              WHERE tp.producer_id = $1
                AND EXISTS (SELECT 1 FROM kafgres_topics t WHERE t.topic_id = tp.topic_id)
              ORDER BY tp.topic_id, tp.partition",
            None,
            &[producer_id.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(t), Some(p)) = (row.get::<i32>(1)?, row.get::<i32>(2)?) {
                out.push((t as u32, p, row.get::<i64>(3)?.unwrap_or(-1)));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let mut store = crate::storage::open();
    for (topic_id, partition, first_offset) in &partitions {
        append_marker(
            &mut *store,
            producer_id,
            producer_epoch,
            *topic_id,
            *partition,
            *first_offset,
            committed,
        )?;
    }

    // Staged offsets become visible exactly with the records, or not at all: after the
    if committed {
        Spi::run_with_args(
            "INSERT INTO kafgres_offsets
                    (group_id, topic_id, partition, committed_offset,
                     committed_leader_epoch, metadata, commit_ts)
             SELECT group_id, topic_id, partition, committed_offset,
                    committed_leader_epoch, metadata, now()
               FROM kafgres_txn_offsets WHERE producer_id = $1
             ON CONFLICT (group_id, topic_id, partition) DO UPDATE SET
                committed_offset = EXCLUDED.committed_offset,
                committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                metadata = EXCLUDED.metadata,
                commit_ts = now()",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_offsets WHERE producer_id = $1",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Only once every marker is down. Clearing first would lose the partition list on a
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_partitions WHERE producer_id = $1",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Spi::run_with_args(
        "UPDATE kafgres_txns SET state = $2 WHERE producer_id = $1",
        &[
            producer_id.into(),
            if committed { "committed" } else { "aborted" }.into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    log!(
        "kafgres: transaction for producer {} {} across {} partition(s)",
        producer_id,
        if committed { "committed" } else { "aborted" },
        partitions.len()
    );

    if overridden {
        // Fence: the producer must re-initialise rather than believe it still owns a
        fence(producer_id)?;
        return Ok(Finish::Overridden);
    }
    Ok(Finish::Ended)
}

/// Bump a producer's epoch so its next request fails. Not optional: an unfenced producer
fn fence(producer_id: i64) -> Result<i32, HandlerError> {
    let epoch: Option<i32> = Spi::get_one_with_args(
        "WITH up AS (
             UPDATE kafgres_producers SET producer_epoch = producer_epoch + 1
              WHERE producer_id = $1
             RETURNING producer_epoch)
         SELECT (SELECT producer_epoch FROM up)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(epoch.unwrap_or(-1))
}

/// `25 AddOffsetsToTxn` — the transaction will also commit consumer offsets. Committed
pub fn handle_add_offsets(req: &AddOffsetsToTxnRequest) -> Result<AddOffsetsToTxnResponse, HandlerError> {
    let code = if fenced(req.producer_id, req.producer_epoch)? {
        ErrorCode::InvalidProducerEpoch
    } else {
        Spi::run_with_args(
            "INSERT INTO kafgres_txns
                    (producer_id, producer_epoch, transactional_id, state, started_at)
             VALUES ($1, $2, $3, 'ongoing', $4)
             ON CONFLICT (producer_id) DO UPDATE
                SET state = 'ongoing',
                    started_at = CASE WHEN kafgres_txns.state = 'ongoing'
                                      THEN kafgres_txns.started_at
                                      ELSE EXCLUDED.started_at END",
            &[
                req.producer_id.into(),
                (req.producer_epoch as i32).into(),
                req.transactional_id.clone().into(),
                now_millis().into(),
            ],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        ErrorCode::None
    };

    Ok(AddOffsetsToTxnResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        ..Default::default()
    })
}

/// `28 TxnOffsetCommit` — stage offsets that become visible only if the transaction
pub fn handle_txn_offset_commit(
    req: &TxnOffsetCommitRequest,
) -> Result<TxnOffsetCommitResponse, HandlerError> {
    let code = if fenced(req.producer_id, req.producer_epoch)? {
        ErrorCode::InvalidProducerEpoch
    } else {
        ErrorCode::None
    };

    if code == ErrorCode::None {
        for topic in &req.topics {
            let Some(topic_id) = crate::meta::topic_id_by_name(&topic.name)
                .map_err(|e| HandlerError::Internal(e.to_string()))?
            else {
                continue;
            };
            for part in &topic.partitions {
                Spi::run_with_args(
                    "INSERT INTO kafgres_txn_offsets
                            (producer_id, group_id, topic_id, partition, committed_offset,
                             committed_leader_epoch, metadata)
                     VALUES ($1, $2, $3::oid, $4, $5, $6, $7)
                     ON CONFLICT (producer_id, group_id, topic_id, partition) DO UPDATE SET
                        committed_offset = EXCLUDED.committed_offset,
                        committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                        metadata = EXCLUDED.metadata",
                    &[
                        req.producer_id.into(),
                        req.group_id.clone().into(),
                        (topic_id as i32).into(),
                        part.partition_index.into(),
                        part.committed_offset.into(),
                        part.committed_leader_epoch.into(),
                        part.committed_metadata.clone().into(),
                    ],
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
        }
    }

    Ok(TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics: req
            .topics
            .iter()
            .map(|t| TxnOffsetCommitResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| TxnOffsetCommitResponsePartition {
                        partition_index: p.partition_index,
                        error_code: code.code(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// Abort transactions whose producer stopped talking to us. Without this, the LSO held by
pub fn expire_stale_transactions() -> Result<usize, HandlerError> {
    let now = now_millis();
    // Bounded: ending thousands in one pass would hold the worker off Fetch, and the next
    const MAX_PER_SWEEP: i64 = 64;
    let stale: Vec<(i64, i16)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT producer_id, producer_epoch FROM kafgres_txns
              WHERE state = 'ongoing' AND $1 - started_at > timeout_ms
              ORDER BY started_at
              LIMIT $2
              FOR UPDATE",
            None,
            &[now.into(), MAX_PER_SWEEP.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(p), Some(e)) = (row.get::<i64>(1)?, row.get::<i32>(2)?) {
                out.push((p, e as i16));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let mut done = 0;
    for (producer_id, epoch) in stale {
        // Abort, then fence — both in this one Postgres transaction: inside it the fence
        match finish_transaction(producer_id, epoch, false)? {
            // Ended underneath us between the SELECT and here. Nothing to fence.
            Finish::NotOurs => continue,
            // Already forced by an operator; `finish_transaction` fenced on its way out.
            Finish::Overridden => {
                done += 1;
                continue;
            }
            Finish::Ended => {}
        }

        // The fence makes expiry safe: the producer may just be slow, and unfenced it keeps
        let fenced_epoch: Option<i32> = Spi::get_one_with_args(
            "WITH up AS (
                 UPDATE kafgres_producers
                    SET producer_epoch = producer_epoch + 1
                  WHERE producer_id = $1
                 RETURNING producer_epoch)
             SELECT (SELECT producer_epoch FROM up)",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        log!(
            "kafgres: aborted transaction for producer {producer_id} — no EndTxn within \
             its transaction timeout; fenced at epoch {}",
            fenced_epoch.unwrap_or(-1)
        );
        done += 1;
    }
    Ok(done)
}

/// Append one partition's control batch and, on an abort, record the range it covers.
fn append_marker(
    store: &mut dyn crate::storage::LogStore,
    producer_id: i64,
    producer_epoch: i16,
    topic_id: u32,
    partition: i32,
    first_offset: i64,
    committed: bool,
) -> Result<(), HandlerError> {
    let bytes = build_control_batch(producer_id, producer_epoch, committed);
    let view = RecordBatch::new(bytes.clone())
        .map_err(|e| HandlerError::Internal(format!("control batch: {e:?}")))?;
    let batch = RawBatch {
        bytes: bytes.to_vec(),
        record_count: view.record_count(),
        last_offset_delta: view.last_offset_delta(),
        max_timestamp: view.max_timestamp(),
        producer_id,
        producer_epoch,
        base_sequence: -1,
        is_transactional: true,
        is_control: true,
    };
    // A marker that fails to land is worse than a failed EndTxn: one partition's consumers
    let marker_offset = store
        .append(topic_id, partition, batch, None)
        .map_err(|e| HandlerError::Internal(format!("writing transaction marker: {e}")))?;

    // The abort range must be recorded where a later Fetch finds it. `first_offset < 0`
    if !committed && first_offset >= 0 {
        crate::storage::pmeta::record_aborted_txn(
            topic_id,
            partition,
            producer_id,
            first_offset,
            marker_offset,
        )
        .map_err(|e| HandlerError::Internal(format!("recording aborted range: {e}")))?;
    }
    Ok(())
}

/// End **one partition** of a transaction — the protocol's own granularity, since Kafka
fn finish_partition(
    producer_id: i64,
    producer_epoch: i16,
    topic_id: u32,
    partition: i32,
    committed: bool,
) -> Result<(), HandlerError> {
    // The same lock `finish_transaction` takes: two markers for one transaction — one
    let first_offset: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT tp.first_offset
                   FROM kafgres_txns t
                   JOIN kafgres_txn_partitions tp ON tp.producer_id = t.producer_id
                  WHERE t.producer_id = $1 AND tp.topic_id = $2::oid AND tp.partition = $3
                    AND t.state = 'ongoing'
                  FOR UPDATE OF t)",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let Some(first_offset) = first_offset else {
        return Ok(());
    };

    let mut store = crate::storage::open();
    append_marker(
        &mut *store,
        producer_id,
        producer_epoch,
        topic_id,
        partition,
        first_offset,
        committed,
    )?;

    Spi::run_with_args(
        "DELETE FROM kafgres_txn_partitions
          WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // `COALESCE` so the first marker decides: the transaction ends one way, not half each.
    Spi::run_with_args(
        "UPDATE kafgres_txns SET forced_result = COALESCE(forced_result, $2)
          WHERE producer_id = $1",
        &[producer_id.into(), committed.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let remaining: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if remaining.unwrap_or(0) > 0 {
        log!(
            "kafgres: wrote a {} marker for producer {producer_id} on {topic_id}-{partition}; \
             {} partition(s) of its transaction remain",
            if committed { "COMMIT" } else { "ABORT" },
            remaining.unwrap_or(0)
        );
        return Ok(());
    }

    // Last partition: the transaction is over, so it lands exactly as `EndTxn` would.
    if committed {
        Spi::run_with_args(
            "INSERT INTO kafgres_offsets
                    (group_id, topic_id, partition, committed_offset,
                     committed_leader_epoch, metadata, commit_ts)
             SELECT group_id, topic_id, partition, committed_offset,
                    committed_leader_epoch, metadata, now()
               FROM kafgres_txn_offsets WHERE producer_id = $1
             ON CONFLICT (group_id, topic_id, partition) DO UPDATE SET
                committed_offset = EXCLUDED.committed_offset,
                committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                metadata = EXCLUDED.metadata,
                commit_ts = now()",
            &[producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_offsets WHERE producer_id = $1",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Spi::run_with_args(
        "UPDATE kafgres_txns SET state = $2 WHERE producer_id = $1",
        &[
            producer_id.into(),
            if committed { "committed" } else { "aborted" }.into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    // The producer did not end this transaction and does not know it is over. Fence only
    let epoch = fence(producer_id)?;
    log!(
        "kafgres: transaction for producer {producer_id} {} by WriteTxnMarkers; \
         fenced at epoch {epoch}",
        if committed { "committed" } else { "aborted" }
    );
    Ok(())
}

/// `27 WriteTxnMarkers` — force a transaction's outcome onto named partitions: the escape
pub fn write_txn_markers(
    req: &WriteTxnMarkersRequest,
    authz: &crate::acl::Authz,
) -> Result<WriteTxnMarkersResponse, HandlerError> {
    super::check_admin_len("transaction markers", req.markers.len())?;
    let total: usize = req
        .markers
        .iter()
        .map(|m| m.topics.iter().map(|t| t.partition_indexes.len()).sum::<usize>())
        .sum();
    // Each partition named costs a control batch in memory and an append, while the
    if total > super::MAX_ADMIN_ITEMS {
        return Err(HandlerError::TooLarge { what: "marker partitions", n: total });
    }

    let authorized = authz
        .check(
            crate::acl::Operation::ClusterAction,
            crate::acl::ResourceType::Cluster,
            "kafka-cluster",
        )
        .err();

    let mut markers = Vec::with_capacity(req.markers.len());
    for m in &req.markers {
        let mut topics = Vec::with_capacity(m.topics.len());
        for t in &m.topics {
            let mut partitions = Vec::with_capacity(t.partition_indexes.len());
            for p in &t.partition_indexes {
                let code = match authorized {
                    Some(c) => c,
                    None => mark_one(m.producer_id, m.producer_epoch, &t.name, *p, m.transaction_result)
                        .unwrap_or_else(|e| {
                            // Reported per partition rather than raised: the partitions
                            log!("kafgres: WriteTxnMarkers {}-{}: {e}", t.name, p);
                            ErrorCode::UnknownServerError
                        }),
                };
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index: *p,
                    error_code: code.code(),
                    ..Default::default()
                });
            }
            topics.push(WritableTxnMarkerTopicResult {
                name: t.name.clone(),
                partitions,
                ..Default::default()
            });
        }
        markers.push(WritableTxnMarkerResult {
            producer_id: m.producer_id,
            topics,
            ..Default::default()
        });
    }
    Ok(WriteTxnMarkersResponse { markers, ..Default::default() })
}

/// One partition's marker. `ErrorCode::None` on success.
fn mark_one(
    producer_id: i64,
    producer_epoch: i16,
    topic: &str,
    partition: i32,
    committed: bool,
) -> Result<ErrorCode, HandlerError> {
    let Some(topic_id) = crate::meta::topic_id_by_name(topic)
        .map_err(|e| HandlerError::Internal(e.to_string()))?
    else {
        return Ok(ErrorCode::UnknownTopicOrPartition);
    };
    let n = crate::meta::partition_count(topic_id).map_err(|e| HandlerError::Internal(e.to_string()))?;
    if partition < 0 || partition >= n {
        return Ok(ErrorCode::UnknownTopicOrPartition);
    }

    // A *newer* epoch means the transaction was already fenced and superseded; an older one
    let current: Option<i32> = Spi::get_one_with_args(
        "SELECT (SELECT producer_epoch FROM kafgres_producers WHERE producer_id = $1)",
        &[producer_id.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    match current {
        None => return Ok(ErrorCode::InvalidProducerEpoch),
        Some(c) if (producer_epoch as i32) < c => return Ok(ErrorCode::InvalidProducerEpoch),
        _ => {}
    }

    // The partition must actually be in this producer's transaction: a stray marker appends
    let ongoing: Option<bool> = Spi::get_one_with_args(
        "SELECT (SELECT true FROM kafgres_txn_partitions
                  WHERE producer_id = $1 AND topic_id = $2::oid AND partition = $3)",
        &[producer_id.into(), (topic_id as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if ongoing != Some(true) {
        return Ok(ErrorCode::InvalidTxnState);
    }

    finish_partition(producer_id, producer_epoch, topic_id, partition, committed)?;
    Ok(ErrorCode::None)
}
