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
const OFFSET_NOT_FOUND: i64 = -1;

pub fn handle(
    req: &ListOffsetsRequest,
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

    let result = match timestamp {
        TIMESTAMP_EARLIEST => store.log_start_offset(topic, partition).map(Some),
        TIMESTAMP_LATEST => store.high_watermark(topic, partition).map(Some),
        // Every negative value is a sentinel (-3 is MAX_TIMESTAMP, KIP-734), never a
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
