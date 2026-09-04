//! Read-only diagnostics: a hung transaction stalls its consumers with no error anywhere.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::describe_producers_request::DescribeProducersRequest;
use kafgres_codec::generated::describe_producers_response::{
    DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
};
use kafgres_codec::generated::describe_transactions_request::DescribeTransactionsRequest;
use kafgres_codec::generated::describe_transactions_response::{
    DescribeTransactionsResponse, TopicData, TransactionState as DescribeTxnState,
};
use kafgres_codec::generated::list_transactions_request::ListTransactionsRequest;
use kafgres_codec::generated::list_transactions_response::{
    ListTransactionsResponse, TransactionState as ListTxnState,
};
use pgrx::prelude::*;

use super::HandlerError;
use crate::meta;

fn wire_state(stored: &str) -> &'static str {
    match stored {
        "ongoing" => "Ongoing",
        "committed" => "CompleteCommit",
        "aborted" => "CompleteAbort",
        _ => "Unknown",
    }
}

pub fn list_transactions(
    req: &ListTransactionsRequest,
    authz: &crate::acl::Authz,
) -> Result<ListTransactionsResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::Describe,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(ListTransactionsResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            ..Default::default()
        });
    }

    let wanted: Vec<String> = req.state_filters.clone();
    let by_producer: Vec<i64> = req.producer_id_filters.clone();

    let rows: Vec<(String, i64, String)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT transactional_id, producer_id, state FROM kafgres_txns
              ORDER BY transactional_id",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let (Some(id), Some(pid), Some(state)) =
                (r.get::<String>(1)?, r.get::<i64>(2)?, r.get::<String>(3)?)
            {
                out.push((id, pid, state));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let mut states = Vec::new();
    for (transactional_id, producer_id, stored) in rows {
        let wire = wire_state(&stored);
        if !wanted.is_empty() && !wanted.iter().any(|w| w == wire) {
            continue;
        }
        if !by_producer.is_empty() && !by_producer.contains(&producer_id) {
            continue;
        }
        states.push(ListTxnState {
            transactional_id,
            producer_id,
            transaction_state: wire.to_string(),
            ..Default::default()
        });
    }

    let known = [
        "Empty",
        "Ongoing",
        "PrepareCommit",
        "PrepareAbort",
        "CompleteCommit",
        "CompleteAbort",
        "PrepareEpochFence",
        "Dead",
        "Unknown",
    ];
    let unknown_state_filters = wanted
        .into_iter()
        .filter(|w| !known.contains(&w.as_str()))
        .collect();

    Ok(ListTransactionsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        unknown_state_filters,
        transaction_states: states,
        ..Default::default()
    })
}

pub fn describe_transactions(
    req: &DescribeTransactionsRequest,
    authz: &crate::acl::Authz,
) -> Result<DescribeTransactionsResponse, HandlerError> {
    super::check_admin_len("transactional id list", req.transactional_ids.len())?;
    let mut resolved_partitions = 0usize;
    let mut out = Vec::with_capacity(req.transactional_ids.len());

    for id in &req.transactional_ids {
        if let Err(code) = authz.check(
            crate::acl::Operation::Describe,
            crate::acl::ResourceType::TransactionalId,
            id,
        ) {
            out.push(DescribeTxnState {
                error_code: code.code(),
                transactional_id: id.clone(),
                ..Default::default()
            });
            continue;
        }

        let found: Option<(i64, i32, String, i64, i32)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT producer_id, producer_epoch, state, started_at, timeout_ms
                   FROM kafgres_txns WHERE transactional_id = $1",
                None,
                &[id.as_str().into()],
            )?;
            for r in rows {
                if let (Some(pid), Some(epoch), Some(state), Some(started)) = (
                    r.get::<i64>(1)?,
                    r.get::<i32>(2)?,
                    r.get::<String>(3)?,
                    r.get::<i64>(4)?,
                ) {
                    let timeout = r.get::<i32>(5)?.unwrap_or(0);
                    return Ok::<_, pgrx::spi::Error>(Some((pid, epoch, state, started, timeout)));
                }
            }
            Ok(None)
        })
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        let Some((producer_id, producer_epoch, stored, started_at, timeout_ms)) = found else {
            out.push(DescribeTxnState {
                error_code: ErrorCode::TransactionalIdNotFound.code(),
                transactional_id: id.clone(),
                ..Default::default()
            });
            continue;
        };

        let mut topics: Vec<TopicData> = Vec::new();
        let parts: Vec<(u32, i32)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT topic_id::int, partition FROM kafgres_txn_partitions
                  WHERE producer_id = $1 ORDER BY topic_id, partition",
                None,
                &[producer_id.into()],
            )?;
            let mut v = Vec::new();
            for r in rows {
                if let (Some(t), Some(p)) = (r.get::<i32>(1)?, r.get::<i32>(2)?) {
                    v.push((t as u32, p));
                }
            }
            Ok::<_, pgrx::spi::Error>(v)
        })
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        resolved_partitions += parts.len();
        super::check_admin_len("transaction partition list", resolved_partitions)?;

        let mut names: std::collections::HashMap<u32, Option<String>> =
            std::collections::HashMap::new();
        for (topic_id, partition) in parts {
            let name = match names.get(&topic_id) {
                Some(cached) => cached.clone(),
                None => {
                    let looked_up = meta::topic_name_by_id(topic_id)
                        .map_err(|e| HandlerError::Internal(e.to_string()))?;
                    names.insert(topic_id, looked_up.clone());
                    looked_up
                }
            };
            let Some(name) = name else { continue };
            match topics.iter_mut().find(|t| t.topic == name) {
                Some(t) => t.partitions.push(partition),
                None => topics.push(TopicData {
                    topic: name,
                    partitions: vec![partition],
                    ..Default::default()
                }),
            }
        }

        out.push(DescribeTxnState {
            error_code: ErrorCode::None.code(),
            transactional_id: id.clone(),
            transaction_state: wire_state(&stored).to_string(),
            transaction_timeout_ms: timeout_ms,
            transaction_start_time_ms: started_at,
            producer_id,
            producer_epoch: producer_epoch as i16,
            topics,
            ..Default::default()
        });
    }

    Ok(DescribeTransactionsResponse {
        throttle_time_ms: 0,
        transaction_states: out,
        ..Default::default()
    })
}

pub fn describe_producers(
    req: &DescribeProducersRequest,
    store: &dyn crate::storage::LogStore,
    authz: &crate::acl::Authz,
) -> Result<DescribeProducersResponse, HandlerError> {
    // Cap the *product*, not each level — each partition costs SPI round trips `statement_timeout` never sees.
    let total: usize = req.topics.iter().map(|t| t.partition_indexes.len()).sum();
    super::check_admin_len("describe producers partition list", total)?;
    let mut topics = Vec::with_capacity(req.topics.len());

    for t in &req.topics {
        let mut partitions = Vec::new();
        let denied = authz
            .check(crate::acl::Operation::Read, crate::acl::ResourceType::Topic, &t.name)
            .err();
        let topic_id = meta::topic_id_by_name(&t.name)
            .map_err(|e| HandlerError::Internal(e.to_string()))?;

        for &p in &t.partition_indexes {
            if let Some(code) = denied {
                partitions.push(PartitionResponse {
                    partition_index: p,
                    error_code: code.code(),
                    ..Default::default()
                });
                continue;
            }
            let Some(topic_id) = topic_id else {
                partitions.push(PartitionResponse {
                    partition_index: p,
                    error_code: ErrorCode::UnknownTopicOrPartition.code(),
                    ..Default::default()
                });
                continue;
            };

            // A start offset only for genuinely open transactions: the partition's LSO, the first uncommitted offset.
            let lso = store
                .last_stable_offset_if_tracked(topic_id, p)
                .ok()
                .flatten()
                .unwrap_or(-1);
            let states: Vec<ProducerState> = Spi::connect(|client| {
                let rows = client.select(
                    "SELECT DISTINCT ON (b.producer_id)
                            b.producer_id, b.producer_epoch, b.last_seq,
                            (EXTRACT(epoch FROM b.appended_at) * 1000)::bigint,
                            (t.producer_id IS NOT NULL)
                       FROM kafgres_producer_batches b
                       LEFT JOIN kafgres_txns t
                              ON t.producer_id = b.producer_id AND t.state = 'ongoing'
                       LEFT JOIN kafgres_txn_partitions tp
                              ON tp.producer_id = b.producer_id
                             AND tp.topic_id = b.topic_id AND tp.partition = b.partition
                      WHERE b.topic_id = $1::oid AND b.partition = $2
                        AND (t.producer_id IS NULL OR tp.producer_id IS NOT NULL)
                      ORDER BY b.producer_id, b.added_seq DESC",
                    None,
                    &[(topic_id as i32).into(), p.into()],
                )?;
                let mut v = Vec::new();
                for r in rows {
                    if let (Some(pid), Some(epoch), Some(last_seq), Some(ts), Some(open)) = (
                        r.get::<i64>(1)?,
                        r.get::<i16>(2)?,
                        r.get::<i32>(3)?,
                        r.get::<i64>(4)?,
                        r.get::<bool>(5)?,
                    ) {
                        v.push(ProducerState {
                            producer_id: pid,
                            producer_epoch: epoch as i32,
                            last_sequence: last_seq,
                            last_timestamp: ts,
                            // Kafka's is the transaction coordinator's epoch; this embedded coordinator has none.
                            coordinator_epoch: -1,
                            current_txn_start_offset: if open { lso } else { -1 },
                            ..Default::default()
                        });
                    }
                }
                Ok::<_, pgrx::spi::Error>(v)
            })
            .map_err(|e| HandlerError::Internal(e.to_string()))?;

            partitions.push(PartitionResponse {
                partition_index: p,
                error_code: ErrorCode::None.code(),
                active_producers: states,
                ..Default::default()
            });
        }

        topics.push(TopicResponse {
            name: t.name.clone(),
            partitions,
            ..Default::default()
        });
    }

    Ok(DescribeProducersResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    })
}
