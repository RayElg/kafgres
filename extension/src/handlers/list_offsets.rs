use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::list_offsets_request::ListOffsetsRequest;
use kafgres_codec::generated::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};

use super::HandlerError;
use crate::meta;
use crate::storage::LogStore;

pub const TIMESTAMP_EARLIEST: i64 = -2;
/// The high watermark — the offset of the *next* record, not the last one.
pub const TIMESTAMP_LATEST: i64 = -1;
/// KIP-734, `ListOffsets` v7: the offset of the record with the greatest timestamp.
const TIMESTAMP_MAX: i64 = -3;
/// KIP-405, v8: the start of the *local* log, i.e. the part not moved to remote storage.
const TIMESTAMP_EARLIEST_LOCAL: i64 = -4;
/// KIP-1005, v9: the last offset that reached the remote tier.
const TIMESTAMP_LATEST_TIERED: i64 = -5;
/// KIP-1023, v11: the earliest offset still waiting to be uploaded to the remote tier.
const TIMESTAMP_EARLIEST_PENDING_UPLOAD: i64 = -6;
const OFFSET_NOT_FOUND: i64 = -1;

/// The `ListOffsets` version each timestamp sentinel first appeared in. A sentinel is a
/// negative number in a field that otherwise carries a millisecond timestamp, so a
/// request carrying one below that version is refused with `UNSUPPORTED_VERSION`.
fn sentinel_since(timestamp: i64) -> Option<i16> {
    match timestamp {
        TIMESTAMP_MAX => Some(7),
        TIMESTAMP_EARLIEST_LOCAL => Some(8),
        TIMESTAMP_LATEST_TIERED => Some(9),
        TIMESTAMP_EARLIEST_PENDING_UPLOAD => Some(11),
        _ => None,
    }
}

pub fn handle(
    req: &ListOffsetsRequest,
    version: i16,
    store: &dyn LogStore,
    authz: &crate::acl::Authz,
) -> Result<ListOffsetsResponse, HandlerError> {
    let mut topics = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        let name = topic.name.clone();
        let topic_id = meta::topic_id_by_name(&name).map_err(|e| {
            pgrx::log!("kafgres: list_offsets topic lookup failed: {e}");
            HandlerError::Internal(format!("topic lookup: {e}"))
        })?;

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        let denied = authz.check(crate::acl::Operation::Describe, crate::acl::ResourceType::Topic, &topic.name).err();
        for p in &topic.partitions {
            if let Some(code) = denied {
                partitions.push(err_partition(p.partition_index, code));
                continue;
            }
            // The sentinel version gate does not depend on the topic, so it runs first.
            if sentinel_since(p.timestamp).is_some_and(|since| version < since) {
                partitions.push(err_partition(p.partition_index, ErrorCode::UnsupportedVersion));
                continue;
            }
            partitions.push(match topic_id {
                None => err_partition(p.partition_index, ErrorCode::UnknownTopicOrPartition),
                Some(tid) => resolve(store, tid, p.partition_index, p.timestamp),
            });
        }

        topics.push(ListOffsetsTopicResponse {
            name,
            partitions,
            ..Default::default()
        });
    }

    Ok(ListOffsetsResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    })
}

fn resolve(
    store: &dyn LogStore,
    topic: u32,
    partition: i32,
    timestamp: i64,
) -> ListOffsetsPartitionResponse {
    let epoch = store.leader_epoch(topic, partition).unwrap_or(-1);

    // MAX_TIMESTAMP is the only sentinel that reports a real timestamp back.
    if timestamp == TIMESTAMP_MAX {
        return match store.max_timestamp_offset(topic, partition) {
            Ok(Some((offset, ts))) => ListOffsetsPartitionResponse {
                partition_index: partition,
                error_code: ErrorCode::None.code(),
                timestamp: ts,
                offset,
                leader_epoch: epoch,
                ..Default::default()
            },
            // An empty log answers offset -1 with no error, matching Kafka.
            Ok(None) => ListOffsetsPartitionResponse {
                partition_index: partition,
                error_code: ErrorCode::None.code(),
                timestamp: -1,
                offset: OFFSET_NOT_FOUND,
                leader_epoch: epoch,
                ..Default::default()
            },
            Err(e) => {
                pgrx::log!("kafgres: list_offsets max_timestamp {topic}-{partition}: {e}");
                err_partition(partition, e.error_code())
            }
        };
    }

    let result = match timestamp {
        TIMESTAMP_EARLIEST => store.log_start_offset(topic, partition).map(Some),
        TIMESTAMP_LATEST => store.high_watermark(topic, partition).map(Some),
        // No remote tier, so the local log is the whole log: this is the log start.
        TIMESTAMP_EARLIEST_LOCAL => store.log_start_offset(topic, partition).map(Some),
        // Nothing is tiered or queued for upload, so there is no such offset: -1, no error.
        TIMESTAMP_LATEST_TIERED | TIMESTAMP_EARLIEST_PENDING_UPLOAD => Ok(None),
        ts if ts < 0 => {
            pgrx::log!("kafgres: list_offsets: unsupported timestamp sentinel {ts}");
            return err_partition(partition, ErrorCode::UnsupportedVersion);
        }
        ts => store.offset_for_timestamp(topic, partition, ts),
    };

    match result {
        Ok(found) => ListOffsetsPartitionResponse {
            partition_index: partition,
            error_code: ErrorCode::None.code(),
            timestamp: if found.is_some() && timestamp >= 0 {
                timestamp
            } else {
                -1
            },
            offset: found.unwrap_or(OFFSET_NOT_FOUND),
            leader_epoch: epoch,
            ..Default::default()
        },
        Err(e) => {
            pgrx::log!("kafgres: list_offsets {topic}-{partition}: {e}");
            err_partition(partition, e.error_code())
        }
    }
}

fn err_partition(index: i32, code: ErrorCode) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index: index,
        error_code: code.code(),
        timestamp: -1,
        offset: -1,
        leader_epoch: -1,
        ..Default::default()
    }
}
