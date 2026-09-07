//! `90 DescribeShareGroupOffsets`, `91 AlterShareGroupOffsets`, `92
//! DeleteShareGroupOffsets`: the share-group half of `kafka-share-groups.sh`, answered
//! from the same `kafgres_share_offsets` table `78 ShareFetch` reads positions from.
//!
//! A share group's position is the start offset plus per-record rows in
//! `kafgres_share_inflight`, so a reset or delete also clears the inflight state for
//! the partitions it touches, matching what Kafka's persister does.

use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::alter_share_group_offsets_request::AlterShareGroupOffsetsRequest;
use kafgres_codec::generated::alter_share_group_offsets_response::{
    AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
    AlterShareGroupOffsetsResponseTopic,
};
use kafgres_codec::generated::delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest;
use kafgres_codec::generated::delete_share_group_offsets_response::{
    DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
};
use kafgres_codec::generated::describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest;
use kafgres_codec::generated::describe_share_group_offsets_response::{
    DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
    DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
};
use kafgres_codec::prelude::Uuid;

use super::{check_admin_len, HandlerError};
use crate::acl::{Authz, Operation, ResourceType};
use crate::meta;
use crate::storage::LogStore;

/// Kafka reports a partition with no stored share-group position as start offset -1 and
/// lag -1, which is a different claim from 0 (the head of the log).
const NO_POSITION: i64 = -1;

/// `90 DescribeShareGroupOffsets`: where each share group's next acquire begins.
pub fn describe(
    req: &DescribeShareGroupOffsetsRequest,
    store: &dyn LogStore,
    authz: &Authz,
) -> Result<DescribeShareGroupOffsetsResponse, HandlerError> {
    check_admin_len("share group offset groups", req.groups.len())?;

    // Invariant I8: the cap is on the product of groups and partitions, not each
    // level; the `topics: null` path expands from stored state, so resolve it first.
    let mut budget = 0usize;

    let mut groups = Vec::with_capacity(req.groups.len());
    for g in &req.groups {
        if let Err(code) = authz.check(Operation::Describe, ResourceType::Group, &g.group_id) {
            groups.push(group_error(&g.group_id, code, "not authorized to describe this group"));
            continue;
        }

        let wanted: Vec<(String, Vec<i32>)> = match &g.topics {
            Some(topics) => topics
                .iter()
                .map(|t| (t.topic_name.clone(), t.partitions.clone()))
                .collect(),
            None => stored_positions(&g.group_id)?,
        };
        budget += wanted.iter().map(|(_, p)| p.len()).sum::<usize>();
        check_admin_len("share group offset partitions", budget)?;

        let mut topics = Vec::with_capacity(wanted.len());
        for (name, partitions) in wanted {
            // Kafka stamps a topic denial into each partition rather than dropping the topic.
            let denied = authz
                .check(Operation::Describe, ResourceType::Topic, &name)
                .err();
            let topic_id = meta::topic_id_by_name(&name).map_err(|e| {
                HandlerError::Internal(format!("share group offsets topic lookup: {e}"))
            })?;
            let uuid = topic_uuid(topic_id)?;

            let mut parts = Vec::with_capacity(partitions.len());
            for partition in partitions {
                if let Some(code) = denied {
                    parts.push(absent_partition(partition, code));
                    continue;
                }
                // Unknown topic is NONE with a -1 start offset here; key 90 does not
                // list UNKNOWN_TOPIC_OR_PARTITION, and clients would resend on it.
                let Some(tid) = topic_id else {
                    parts.push(absent_partition(partition, ErrorCode::None));
                    continue;
                };

                let start = stored_start(&g.group_id, tid, partition)?;
                let (lag, code) = match start {
                    None => (NO_POSITION, ErrorCode::None),
                    Some(s) => match store.high_watermark(tid, partition) {
                        Ok(hw) => ((hw - s).max(0), ErrorCode::None),
                        // Kafka stamps the end-offset fetch error into the partition.
                        Err(e) => (NO_POSITION, e.error_code()),
                    },
                };
                parts.push(DescribeShareGroupOffsetsResponsePartition {
                    partition_index: partition,
                    start_offset: start.unwrap_or(NO_POSITION),
                    leader_epoch: store.leader_epoch(tid, partition).unwrap_or(-1),
                    lag,
                    error_code: code.code(),
                    ..Default::default()
                });
            }

            topics.push(DescribeShareGroupOffsetsResponseTopic {
                topic_name: name,
                topic_id: uuid,
                partitions: parts,
                ..Default::default()
            });
        }

        groups.push(DescribeShareGroupOffsetsResponseGroup {
            group_id: g.group_id.clone(),
            topics,
            error_code: ErrorCode::None.code(),
            error_message: None,
            ..Default::default()
        });
    }

    Ok(DescribeShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        groups,
        ..Default::default()
    })
}

/// `91 AlterShareGroupOffsets`: move where a share group's next acquire begins.
pub fn alter(
    req: &AlterShareGroupOffsetsRequest,
    authz: &Authz,
) -> Result<AlterShareGroupOffsetsResponse, HandlerError> {
    if let Err(code) = authz.check(Operation::Read, ResourceType::Group, &req.group_id) {
        return Ok(alter_error(code, "not authorized to alter this group"));
    }

    check_admin_len("share group offset topics", req.topics.len())?;
    let total: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    check_admin_len("share group offset partitions", total)?;

    // Kafka checks group existence first; a bad group id would otherwise report
    // success and write orphan rows.
    if !group_exists(&req.group_id)? {
        return Ok(alter_error(ErrorCode::GroupIdNotFound, "share group not found"));
    }
    if has_members(&req.group_id)? {
        return Ok(alter_error(
            ErrorCode::NonEmptyGroup,
            "the share group has members; stop them before moving its start offset",
        ));
    }

    let mut responses = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let denied = authz
            .check(Operation::Read, ResourceType::Topic, &t.topic_name)
            .err();
        let topic_id = meta::topic_id_by_name(&t.topic_name)
            .map_err(|e| HandlerError::Internal(format!("share offsets topic lookup: {e}")))?;
        let count = match topic_id {
            Some(tid) => meta::partition_count(tid)
                .map_err(|e| HandlerError::Internal(format!("partition count: {e}")))?,
            None => 0,
        };

        let mut parts = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let code = match (denied, topic_id) {
                (Some(c), _) => c,
                (None, None) => ErrorCode::UnknownTopicOrPartition,
                // Kafka gates on the partition count; without it a reset writes a row
                // no ShareFetch will read.
                (None, Some(_)) if p.partition_index < 0 || p.partition_index >= count => {
                    ErrorCode::UnknownTopicOrPartition
                }
                (None, Some(tid)) => {
                    Spi::run_with_args(
                        "INSERT INTO kafgres_share_offsets
                             (group_id, topic_id, partition, start_offset)
                         VALUES ($1, $2::oid, $3, $4)
                         ON CONFLICT (group_id, topic_id, partition)
                         DO UPDATE SET start_offset = EXCLUDED.start_offset",
                        &[
                            req.group_id.clone().into(),
                            (tid as i32).into(),
                            p.partition_index.into(),
                            p.start_offset.into(),
                        ],
                    )
                    .map_err(|e| HandlerError::Internal(format!("share offset write: {e}")))?;
                    clear_inflight(&req.group_id, tid, p.partition_index)?;
                    ErrorCode::None
                }
            };
            parts.push(AlterShareGroupOffsetsResponsePartition {
                partition_index: p.partition_index,
                error_code: code.code(),
                ..Default::default()
            });
        }

        responses.push(AlterShareGroupOffsetsResponseTopic {
            topic_name: t.topic_name.clone(),
            topic_id: topic_uuid(topic_id)?,
            partitions: parts,
            ..Default::default()
        });
    }

    Ok(AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        responses,
        ..Default::default()
    })
}

/// `92 DeleteShareGroupOffsets`: forget a share group's position for a topic.
pub fn delete(
    req: &DeleteShareGroupOffsetsRequest,
    authz: &Authz,
) -> Result<DeleteShareGroupOffsetsResponse, HandlerError> {
    if let Err(code) = authz.check(Operation::Delete, ResourceType::Group, &req.group_id) {
        return Ok(delete_error(code, "not authorized to delete this group's offsets"));
    }

    check_admin_len("share group offset topics", req.topics.len())?;

    if !group_exists(&req.group_id)? {
        return Ok(delete_error(ErrorCode::GroupIdNotFound, "share group not found"));
    }
    if has_members(&req.group_id)? {
        return Ok(delete_error(
            ErrorCode::NonEmptyGroup,
            "the share group has members; stop them before deleting its offsets",
        ));
    }

    let mut responses = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let denied = authz
            .check(Operation::Read, ResourceType::Topic, &t.topic_name)
            .err();
        let topic_id = meta::topic_id_by_name(&t.topic_name)
            .map_err(|e| HandlerError::Internal(format!("share offsets topic lookup: {e}")))?;

        let (code, message) = match (denied, topic_id) {
            (Some(c), _) => (c, Some("not authorized to read this topic".to_string())),
            (None, None) => (ErrorCode::UnknownTopicOrPartition, None),
            (None, Some(tid)) => {
                let removed = delete_positions(&req.group_id, tid)?;
                if removed == 0 {
                    // The topic exists but the group has no stored positions under it;
                    // Kafka reports that rather than a deletion.
                    (
                        ErrorCode::UnknownTopicOrPartition,
                        Some("there is no offset information to delete".to_string()),
                    )
                } else {
                    clear_inflight_topic(&req.group_id, tid)?;
                    (ErrorCode::None, None)
                }
            }
        };

        responses.push(DeleteShareGroupOffsetsResponseTopic {
            topic_name: t.topic_name.clone(),
            topic_id: topic_uuid(topic_id)?,
            error_code: code.code(),
            error_message: message,
            ..Default::default()
        });
    }

    Ok(DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        responses,
        ..Default::default()
    })
}

fn group_error(
    group: &str,
    code: ErrorCode,
    why: &str,
) -> DescribeShareGroupOffsetsResponseGroup {
    DescribeShareGroupOffsetsResponseGroup {
        group_id: group.to_string(),
        topics: Vec::new(),
        error_code: code.code(),
        error_message: Some(why.to_string()),
        ..Default::default()
    }
}

fn absent_partition(partition: i32, code: ErrorCode) -> DescribeShareGroupOffsetsResponsePartition {
    DescribeShareGroupOffsetsResponsePartition {
        partition_index: partition,
        start_offset: NO_POSITION,
        leader_epoch: -1,
        lag: NO_POSITION,
        error_code: code.code(),
        ..Default::default()
    }
}

fn alter_error(code: ErrorCode, why: &str) -> AlterShareGroupOffsetsResponse {
    AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        error_message: Some(why.to_string()),
        responses: Vec::new(),
        ..Default::default()
    }
}

fn delete_error(code: ErrorCode, why: &str) -> DeleteShareGroupOffsetsResponse {
    DeleteShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        error_message: Some(why.to_string()),
        responses: Vec::new(),
        ..Default::default()
    }
}

fn topic_uuid(topic_id: Option<u32>) -> Result<Uuid, HandlerError> {
    // Kafka's zero uuid means the broker does not know the topic; unknown topics keep it.
    let Some(tid) = topic_id else {
        return Ok(Uuid::ZERO);
    };
    let map = meta::topic_uuids_by_ids(&[tid])
        .map_err(|e| HandlerError::Internal(format!("topic uuid: {e}")))?;
    Ok(map.get(&tid).copied().map(Uuid).unwrap_or(Uuid::ZERO))
}

/// Every (topic, partitions) this group has a stored position for.
fn stored_positions(group: &str) -> Result<Vec<(String, Vec<i32>)>, HandlerError> {
    let rows: Vec<(String, i32)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT t.name, o.partition
               FROM kafgres_share_offsets o
               JOIN kafgres_topics t USING (topic_id)
              WHERE o.group_id = $1
              ORDER BY t.name, o.partition",
            None,
            &[group.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let (Some(n), Some(p)) = (r.get::<String>(1)?, r.get::<i32>(2)?) {
                out.push((n, p));
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(format!("share offsets listing: {e}")))?;

    let mut grouped: Vec<(String, Vec<i32>)> = Vec::new();
    for (name, partition) in rows {
        match grouped.last_mut() {
            Some((last, parts)) if *last == name => parts.push(partition),
            _ => grouped.push((name, vec![partition])),
        }
    }
    Ok(grouped)
}

fn stored_start(group: &str, topic: u32, partition: i32) -> Result<Option<i64>, HandlerError> {
    Spi::get_one_with_args::<i64>(
        "SELECT (SELECT start_offset FROM kafgres_share_offsets
                  WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3)",
        &[group.into(), (topic as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(format!("share offset read: {e}")))
}

fn delete_positions(group: &str, topic: u32) -> Result<i64, HandlerError> {
    Spi::get_one_with_args::<i64>(
        "WITH gone AS (
             DELETE FROM kafgres_share_offsets
              WHERE group_id = $1 AND topic_id = $2::oid
          RETURNING 1)
         SELECT count(*)::bigint FROM gone",
        &[group.into(), (topic as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(format!("share offset delete: {e}")))
    .map(|v| v.unwrap_or(0))
}

/// Drops the partition's per-record state; see the module comment for why.
fn clear_inflight(group: &str, topic: u32, partition: i32) -> Result<(), HandlerError> {
    Spi::run_with_args(
        "DELETE FROM kafgres_share_inflight
          WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3",
        &[group.into(), (topic as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(format!("share inflight clear: {e}")))
}

fn clear_inflight_topic(group: &str, topic: u32) -> Result<(), HandlerError> {
    Spi::run_with_args(
        "DELETE FROM kafgres_share_inflight
          WHERE group_id = $1 AND topic_id = $2::oid",
        &[group.into(), (topic as i32).into()],
    )
    .map_err(|e| HandlerError::Internal(format!("share inflight clear: {e}")))
}

fn group_exists(group: &str) -> Result<bool, HandlerError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM kafgres_share_groups WHERE group_id = $1)",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(format!("share group lookup: {e}")))
    .map(|v| v.unwrap_or(false))
}

fn has_members(group: &str) -> Result<bool, HandlerError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM kafgres_share_members WHERE group_id = $1)",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(format!("share group members: {e}")))
    .map(|v| v.unwrap_or(false))
}
