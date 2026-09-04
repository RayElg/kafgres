use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::offset_commit_request::OffsetCommitRequest;
use kafgres_codec::generated::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafgres_codec::generated::offset_delete_request::OffsetDeleteRequest;
use kafgres_codec::generated::offset_delete_response::{
    OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use kafgres_codec::generated::offset_fetch_request::OffsetFetchRequest;
use kafgres_codec::generated::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
    OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafgres_codec::Uuid;
use pgrx::prelude::*;

use super::HandlerError;
use crate::meta;

const OFFSET_FETCH_BATCHED_FROM: i16 = 8;

/// Kafka's "no committed offset" sentinel; a consumer falls back to `auto.offset.reset`.
const NO_OFFSET: i64 = -1;

/// Kafka's `offset.metadata.max.bytes`; commits become rows that OffsetFetch reassembles.
const MAX_METADATA_BYTES: usize = 4096;

const MAX_OFFSET_PARTITIONS: usize = 4096;

pub fn offset_commit(
    req: &OffsetCommitRequest,
    authz: &crate::acl::Authz,
) -> Result<OffsetCommitResponse, HandlerError> {
    let group_denied = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id).err();
    let total: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    if total > MAX_OFFSET_PARTITIONS {
        return Err(HandlerError::TooLarge {
            what: "offset commit partition list",
            n: total,
        });
    }

    let mut topics = Vec::with_capacity(req.topics.len());

    for t in &req.topics {
        let denied = group_denied.or_else(|| {
            authz
                .check(
                    crate::acl::Operation::Read,
                    crate::acl::ResourceType::Topic,
                    &t.name,
                )
                .err()
        });
        let resolved = meta::resolve_topic(&t.name, &t.topic_id.0)?;
        let mut partitions = Vec::with_capacity(t.partitions.len());

        for p in &t.partitions {
            if let Some(code) = denied {
                partitions.push(OffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code.code(),
                    ..Default::default()
                });
                continue;
            }
            let oversized = p
                .committed_metadata
                .as_ref()
                .map(|m| m.len() > MAX_METADATA_BYTES)
                .unwrap_or(false);

            let code = match &resolved {
                None => ErrorCode::UnknownTopicOrPartition,
                Some(_) if oversized => ErrorCode::OffsetMetadataTooLarge,
                Some(r) => {
                    match commit_one(
                        &req.group_id,
                        r.topic_id,
                        p.partition_index,
                        p.committed_offset,
                        p.committed_leader_epoch,
                        p.committed_metadata.as_deref(),
                    ) {
                        Ok(()) => ErrorCode::None,
                        Err(e) => {
                            pgrx::log!(
                                "kafgres: offset commit failed for {}-{}: {e}",
                                t.name,
                                p.partition_index
                            );
                            ErrorCode::UnknownServerError
                        }
                    }
                }
            };
            partitions.push(OffsetCommitResponsePartition {
                partition_index: p.partition_index,
                error_code: code.code(),
                unknown_tagged_fields: Vec::new(),
            });
        }

        topics.push(OffsetCommitResponseTopic {
            name: resolved
                .as_ref()
                .map(|r| r.name.clone())
                .unwrap_or_else(|| t.name.clone()),
            topic_id: resolved.as_ref().map(|r| Uuid(r.uuid)).unwrap_or(t.topic_id),
            partitions,
            unknown_tagged_fields: Vec::new(),
        });
    }

    Ok(OffsetCommitResponse {
        throttle_time_ms: 0,
        topics,
        unknown_tagged_fields: Vec::new(),
    })
}

fn commit_one(
    group_id: &str,
    topic_id: u32,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    metadata: Option<&str>,
) -> Result<(), spi::Error> {
    Spi::run_with_args(
        "INSERT INTO kafgres_offsets
            (group_id, topic_id, partition, committed_offset, committed_leader_epoch,
             metadata, commit_ts)
         VALUES ($1, $2::oid, $3, $4, $5, $6, now())
         ON CONFLICT (group_id, topic_id, partition) DO UPDATE SET
            committed_offset = EXCLUDED.committed_offset,
            committed_leader_epoch = EXCLUDED.committed_leader_epoch,
            metadata = EXCLUDED.metadata,
            commit_ts = now()",
        &[
            group_id.into(),
            (topic_id as i32).into(),
            partition.into(),
            offset.into(),
            leader_epoch.into(),
            metadata.into(),
        ],
    )
}

pub fn offset_fetch(
    req: &OffsetFetchRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<OffsetFetchResponse, HandlerError> {
    // Checked before the version split, so the legacy v1..v7 path is covered too.
    if version < OFFSET_FETCH_BATCHED_FROM {
        if let Err(code) = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id) {
            return Ok(OffsetFetchResponse {
                throttle_time_ms: 0,
                topics: Vec::new(),
                error_code: code.code(),
                groups: Vec::new(),
                unknown_tagged_fields: Vec::new(),
            });
        }
    }

    if version >= OFFSET_FETCH_BATCHED_FROM {
        let mut groups = Vec::with_capacity(req.groups.len());
        for g in &req.groups {
            if let Err(code) = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &g.group_id) {
                groups.push(OffsetFetchResponseGroup {
                    group_id: g.group_id.clone(),
                    topics: Vec::new(),
                    error_code: code.code(),
                    unknown_tagged_fields: Vec::new(),
                });
                continue;
            }
            let requested: Option<Vec<(String, Uuid, Vec<i32>)>> = g.topics.as_ref().map(|ts| {
                ts.iter()
                    .map(|t| (t.name.clone(), t.topic_id, t.partition_indexes.clone()))
                    .collect()
            });
            groups.push(OffsetFetchResponseGroup {
                group_id: g.group_id.clone(),
                topics: fetch_topics_v8(&g.group_id, requested)?,
                error_code: ErrorCode::None.code(),
                unknown_tagged_fields: Vec::new(),
            });
        }
        return Ok(OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: Vec::new(),
            error_code: ErrorCode::None.code(),
            groups,
            unknown_tagged_fields: Vec::new(),
        });
    }

    let requested: Option<Vec<(String, Vec<i32>)>> = req.topics.as_ref().map(|ts| {
        ts.iter()
            .map(|t| (t.name.clone(), t.partition_indexes.clone()))
            .collect()
    });
    Ok(OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: fetch_topics(&req.group_id, requested)?,
        error_code: ErrorCode::None.code(),
        groups: Vec::new(),
        unknown_tagged_fields: Vec::new(),
    })
}

fn committed_topics(group_id: &str) -> Result<Vec<(String, u32, Vec<i32>)>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT t.name, o.topic_id::int, array_agg(o.partition ORDER BY o.partition)
               FROM kafgres_offsets o
               JOIN kafgres_topics t ON t.topic_id = o.topic_id
              WHERE o.group_id = $1
              GROUP BY t.name, o.topic_id",
            None,
            &[group_id.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(name), Some(tid), Some(parts)) = (
                row.get::<String>(1)?,
                row.get::<i32>(2)?,
                row.get::<Vec<i32>>(3)?,
            ) {
                out.push((name, tid as u32, parts));
            }
        }
        Ok(out)
    })
}

/// One query per topic, not per partition: the round-trip count must not be client-controlled.
type Committed = std::collections::HashMap<i32, (i64, i32, Option<String>)>;

fn committed_for_topic(group_id: &str, topic_id: u32) -> Result<Committed, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT partition, committed_offset, committed_leader_epoch, metadata
               FROM kafgres_offsets WHERE group_id = $1 AND topic_id = $2::oid",
            None,
            &[group_id.into(), (topic_id as i32).into()],
        )?;
        let mut out = Committed::new();
        for row in rows {
            if let Some(p) = row.get::<i32>(1)? {
                out.insert(
                    p,
                    (
                        row.get::<i64>(2)?.unwrap_or(NO_OFFSET),
                        row.get::<i32>(3)?.unwrap_or(-1),
                        row.get::<String>(4)?.or_else(|| Some(String::new())),
                    ),
                );
            }
        }
        Ok(out)
    })
}

fn fetch_topics(
    group_id: &str,
    requested: Option<Vec<(String, Vec<i32>)>>,
) -> Result<Vec<OffsetFetchResponseTopic>, HandlerError> {
    let wanted: Vec<(String, u32, Vec<i32>)> = match requested {
        Some(list) => resolve_requested(list)?,
        None => committed_topics(group_id)?,
    };

    let requested_total: usize = wanted.iter().map(|(_, _, p)| p.len()).sum();
    if requested_total > MAX_OFFSET_PARTITIONS {
        return Err(HandlerError::TooLarge {
            what: "offset fetch partition list",
            n: requested_total,
        });
    }

    let mut out = Vec::with_capacity(wanted.len());
    for (name, topic_id, partitions) in wanted {
        let committed = committed_for_topic(group_id, topic_id)?;
        let mut ps = Vec::with_capacity(partitions.len());
        for p in partitions {
            // `Some("")`, never `None`: Sarama's decoder rejects the null and the session dies.
            let (offset, epoch, metadata) = committed
                .get(&p)
                .cloned()
                .unwrap_or((NO_OFFSET, -1, Some(String::new())));
            ps.push(OffsetFetchResponsePartition {
                partition_index: p,
                committed_offset: offset,
                committed_leader_epoch: epoch,
                metadata,
                error_code: ErrorCode::None.code(),
                unknown_tagged_fields: Vec::new(),
            });
        }
        out.push(OffsetFetchResponseTopic {
            name,
            partitions: ps,
            unknown_tagged_fields: Vec::new(),
        });
    }
    Ok(out)
}

fn fetch_topics_v8(
    group_id: &str,
    requested: Option<Vec<(String, Uuid, Vec<i32>)>>,
) -> Result<Vec<OffsetFetchResponseTopics>, HandlerError> {
    let simplified = requested
        .as_ref()
        .map(|list| list.iter().map(|(n, _, p)| (n.clone(), p.clone())).collect());
    let topics = fetch_topics(group_id, simplified)?;

    let mut out = Vec::with_capacity(topics.len());
    for t in topics {
        // v10 echoes the topic id; a zero uuid reads as "no topic id".
        let topic_id = meta::topic_id_by_name(&t.name)?
            .and_then(|_| meta::resolve_topic(&t.name, &[0u8; 16]).ok().flatten())
            .map(|r| Uuid(r.uuid))
            .unwrap_or(Uuid::ZERO);
        out.push(OffsetFetchResponseTopics {
            topic_id,
            name: t.name,
            partitions: t
                .partitions
                .into_iter()
                .map(|p| OffsetFetchResponsePartitions {
                    partition_index: p.partition_index,
                    committed_offset: p.committed_offset,
                    committed_leader_epoch: p.committed_leader_epoch,
                    metadata: p.metadata,
                    error_code: p.error_code,
                    unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            unknown_tagged_fields: Vec::new(),
        });
    }
    Ok(out)
}

/// Unknown topics still get entries with "no committed offset", rather than being dropped.
fn resolve_requested(
    list: Vec<(String, Vec<i32>)>,
) -> Result<Vec<(String, u32, Vec<i32>)>, HandlerError> {
    let mut out = Vec::with_capacity(list.len());
    for (name, partitions) in list {
        let topic_id = meta::topic_id_by_name(&name)?.unwrap_or(0);
        out.push((name, topic_id, partitions));
    }
    Ok(out)
}

/// `47 OffsetDelete` — refused for any topic while the group has members; upstream only refuses subscribed ones.
pub fn offset_delete(
    req: &OffsetDeleteRequest,
    authz: &crate::acl::Authz,
) -> Result<OffsetDeleteResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::Delete,
        crate::acl::ResourceType::Group,
        &req.group_id,
    ) {
        return Ok(OffsetDeleteResponse {
            error_code: code.code(),
            throttle_time_ms: 0,
            ..Default::default()
        });
    }

    // Both caps: a million empty topic entries sum to zero partitions, yet each runs a lookup.
    super::check_admin_len("offset delete topic list", req.topics.len())?;
    let total: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    if total > MAX_OFFSET_PARTITIONS {
        return Err(HandlerError::TooLarge {
            what: "offset delete partition list",
            n: total,
        });
    }

    let (exists, has_members) = crate::group::existence(&req.group_id)?;
    if !exists {
        return Ok(OffsetDeleteResponse {
            error_code: ErrorCode::GroupIdNotFound.code(),
            throttle_time_ms: 0,
            ..Default::default()
        });
    }

    let mut topics = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let denied = authz
            .check(
                crate::acl::Operation::Delete,
                crate::acl::ResourceType::Topic,
                &t.name,
            )
            .err();

        let topic_id = meta::topic_id_by_name(&t.name)?;

        let mut partitions = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let code = if let Some(code) = denied {
                code
            } else if has_members {
                ErrorCode::GroupSubscribedToTopic
            } else if let Some(id) = topic_id {
                match crate::group::delete_offset(&req.group_id, id, p.partition_index) {
                    Ok(_) => ErrorCode::None,
                    Err(e) => {
                        pgrx::log!(
                            "kafgres: delete offset {}/{} for group '{}' failed: {e}",
                            t.name,
                            p.partition_index,
                            req.group_id
                        );
                        ErrorCode::UnknownServerError
                    }
                }
            } else {
                // Success, not UNKNOWN_TOPIC_OR_PARTITION: upstream reports success for a topic it holds nothing under.
                ErrorCode::None
            };
            partitions.push(OffsetDeleteResponsePartition {
                partition_index: p.partition_index,
                error_code: code.code(),
                ..Default::default()
            });
        }
        topics.push(OffsetDeleteResponseTopic {
            name: t.name.clone(),
            partitions,
            ..Default::default()
        });
    }

    Ok(OffsetDeleteResponse {
        error_code: ErrorCode::None.code(),
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    })
}
