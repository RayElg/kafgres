//! `1 Fetch`. TODO: long-poll parking (`fetch.max.wait.ms`/`fetch.min.bytes`) — an immediate

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::fetch_request::FetchRequest;
use kafgres_codec::generated::fetch_response::{AbortedTransaction,
    FetchResponse, FetchableTopicResponse, PartitionData,
};

use super::HandlerError;
use crate::meta;
use crate::storage::{IsolationLevel, LogStore, StoreError};

/// `fetch.max.bytes` is a client request, not an instruction — honouring it OOMs the backend.
const RESPONSE_CEILING: usize = 8 * 1024 * 1024;

const PARTITION_CEILING: usize = 1024 * 1024;

const MAX_FETCH_PARTITIONS: usize = 4096;

const ABORTED_ENTRY_WIRE_BYTES: usize = 24;

pub fn handle(
    req: &FetchRequest,
    store: &dyn LogStore,
    authz: &crate::acl::Authz,
) -> Result<FetchResponse, HandlerError> {
    let isolation = match req.isolation_level {
        1 => IsolationLevel::ReadCommitted,
        _ => IsolationLevel::ReadUncommitted,
    };

    let mut budget = clamp_bytes(req.max_bytes, RESPONSE_CEILING);

    let total_partitions: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    if total_partitions > MAX_FETCH_PARTITIONS {
        return Err(HandlerError::TooLarge {
            what: "fetch partition list",
            n: total_partitions,
        });
    }

    let mut responses = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        // From v13 only `topic_id` is set; an empty-name lookup hangs retriable consumers.
        let resolved = meta::resolve_topic(&topic.topic, &topic.topic_id.0).map_err(|e| {
            pgrx::log!("kafgres: fetch topic lookup failed: {e}");
            HandlerError::Internal(format!("topic lookup: {e}"))
        })?;
        let topic_id = resolved.as_ref().map(|r| r.topic_id);
        let name = resolved
            .as_ref()
            .map(|r| r.name.clone())
            .unwrap_or_else(|| topic.topic.clone());

        let denied = authz
            .check(
                crate::acl::Operation::Read,
                crate::acl::ResourceType::Topic,
                &name,
            )
            .err();

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            if let Some(code) = denied {
                partitions.push(partition_error(p.partition, code));
                continue;
            }
            let per_partition = clamp_bytes(p.partition_max_bytes, PARTITION_CEILING).min(budget);

            // Checked here, not by a zero budget to `read`: the store always returns one whole batch.
            if budget == 0 {
                partitions.push(empty_partition(p.partition));
                continue;
            }

            let data = match topic_id {
                None => partition_error(p.partition, ErrorCode::UnknownTopicOrPartition),
                Some(tid) => {
                    match store.read(tid, p.partition, p.fetch_offset, per_partition, isolation) {
                        Ok(slice) => {
                            budget = budget.saturating_sub(
                                slice.bytes.len()
                                    + slice.aborted.len() * ABORTED_ENTRY_WIRE_BYTES,
                            );
                            PartitionData {
                                partition_index: p.partition,
                                error_code: ErrorCode::None.code(),
                                high_watermark: slice.high_watermark,
                                last_stable_offset: slice.last_stable_offset,
                                // Real value, not 0: consumers use it to detect truncation.
                                log_start_offset: slice.log_start_offset,
                                // Sent, not withheld server-side: Fetch cannot say "advance past this offset".
                                aborted_transactions: Some(
                                    slice
                                        .aborted
                                        .iter()
                                        .map(|a| AbortedTransaction {
                                            producer_id: a.producer_id,
                                            first_offset: a.first_offset,
                                            ..Default::default()
                                        })
                                        .collect(),
                                ),
                                preferred_read_replica: -1,
                                // Empty, never null: librdkafka fails a -1 length with "invalid MessageSetSize -1".
                                records: Some(kafgres_codec::bytes::Bytes::from(slice.bytes)),
                                ..Default::default()
                            }
                        }
                        Err(e) => {
                            if !matches!(e, StoreError::OffsetOutOfRange) {
                                pgrx::log!(
                                    "kafgres: fetch {}-{} at {} failed: {e}",
                                    name,
                                    p.partition,
                                    p.fetch_offset
                                );
                            }
                            partition_error(p.partition, e.error_code())
                        }
                    }
                }
            };
            partitions.push(data);
        }

        responses.push(FetchableTopicResponse {
            topic: name,
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    Ok(FetchResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        session_id: 0,
        responses,
        ..Default::default()
    })
}

pub fn records_bytes(resp: &FetchResponse) -> usize {
    resp.responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|p| p.records.as_ref().map(|r| r.len()).unwrap_or(0))
        .sum()
}

/// An error must never be held for `fetch.max.wait.ms`: the client must reset or refresh now.
pub fn has_error(resp: &FetchResponse) -> bool {
    resp.responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .any(|p| p.error_code != ErrorCode::None.code())
}

pub fn watched_partitions(req: &FetchRequest) -> Result<Vec<(u32, i32)>, HandlerError> {
    let mut out = Vec::new();
    for topic in &req.topics {
        let resolved = meta::resolve_topic(&topic.topic, &topic.topic_id.0).map_err(|e| {
            HandlerError::Internal(format!("topic lookup: {e}"))
        })?;
        if let Some(r) = resolved {
            for p in &topic.partitions {
                out.push((r.topic_id, p.partition));
            }
        }
    }
    Ok(out)
}

fn clamp_bytes(requested: i32, ceiling: usize) -> usize {
    if requested <= 0 {
        ceiling
    } else {
        (requested as usize).min(ceiling)
    }
}

fn empty_partition(index: i32) -> PartitionData {
    PartitionData {
        partition_index: index,
        error_code: ErrorCode::None.code(),
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        records: Some(kafgres_codec::bytes::Bytes::new()),
        ..Default::default()
    }
}

fn partition_error(index: i32, code: ErrorCode) -> PartitionData {
    PartitionData {
        partition_index: index,
        error_code: code.code(),
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        records: Some(kafgres_codec::bytes::Bytes::new()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_byte_budgets_are_clamped_not_honoured() {
        assert_eq!(clamp_bytes(50 * 1024 * 1024, RESPONSE_CEILING), RESPONSE_CEILING);
        assert_eq!(clamp_bytes(i32::MAX, RESPONSE_CEILING), RESPONSE_CEILING);
        assert_eq!(clamp_bytes(4096, RESPONSE_CEILING), 4096);
        assert_eq!(clamp_bytes(0, RESPONSE_CEILING), RESPONSE_CEILING);
        assert_eq!(clamp_bytes(-1, RESPONSE_CEILING), RESPONSE_CEILING);
    }
}
