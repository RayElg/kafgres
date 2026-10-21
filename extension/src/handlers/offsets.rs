use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition};
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
use std::collections::HashSet;

use pgrx::prelude::*;

use super::HandlerError;
use crate::group::GroupState;
use crate::meta;

const OFFSET_FETCH_BATCHED_FROM: i16 = 8;

/// Kafka's "no committed offset" sentinel; a consumer falls back to `auto.offset.reset`.
const NO_OFFSET: i64 = -1;

/// Kafka's `offset.metadata.max.bytes`; commits become rows that OffsetFetch reassembles.
const MAX_METADATA_BYTES: usize = 4096;

const MAX_OFFSET_PARTITIONS: usize = 4096;

pub struct Committer<'a> {
    pub group_id: &'a str,
    pub generation: i32,
    pub member_id: &'a str,
    pub instance_id: Option<&'a str>,
}

pub enum GroupVerdict {
    Accept,
    Refuse(ErrorCode),
    /// A KIP-848 member behind its current epoch may still commit what it holds (KIP-1251).
    HeldOnly(HashSet<(u32, i32)>),
}

impl GroupVerdict {
    pub fn code_for(&self, topic_id: u32, partition: i32) -> ErrorCode {
        match self {
            GroupVerdict::Accept => ErrorCode::None,
            GroupVerdict::Refuse(code) => *code,
            GroupVerdict::HeldOnly(held) if held.contains(&(topic_id, partition)) => ErrorCode::None,
            GroupVerdict::HeldOnly(_) => ErrorCode::StaleMemberEpoch,
        }
    }
}

/// From v9 a missing group is GROUP_ID_NOT_FOUND, and KIP-848 members may commit.
const OFFSET_COMMIT_GROUP_AWARE_FROM: i16 = 9;

/// Kafka's `validateOffsetCommit` for each group type: refuses an evicted member's commit.
pub fn validate_commit(
    c: &Committer<'_>,
    transactional: bool,
    version: i16,
) -> Result<GroupVerdict, spi::Error> {
    let kind: String = Spi::get_one_with_args(
        "SELECT CASE
                  WHEN EXISTS (SELECT 1 FROM kafgres_share_groups WHERE group_id = $1) THEN 'share'
                  WHEN EXISTS (SELECT 1 FROM kafgres_consumer_groups WHERE group_id = $1) THEN 'consumer'
                  WHEN EXISTS (SELECT 1 FROM kafgres_groups WHERE group_id = $1) THEN 'classic'
                  WHEN EXISTS (SELECT 1 FROM kafgres_offsets WHERE group_id = $1) THEN 'simple'
                  ELSE 'none' END",
        &[c.group_id.into()],
    )?
    .unwrap_or_default();
    let refuse = |code| -> Result<GroupVerdict, spi::Error> { Ok(GroupVerdict::Refuse(code)) };
    match kind.as_str() {
        "share" => refuse(ErrorCode::GroupIdNotFound),
        "consumer" => validate_consumer_group(c, transactional, version),
        "classic" => {
            let Some(g) = crate::group::load(c.group_id)? else {
                return refuse(ErrorCode::UnknownMemberId);
            };
            let members = crate::group::members(c.group_id)?;
            let verdict = validate_classic(c, transactional, g.state, g.generation, &members);
            let live = matches!(g.state, GroupState::Stable | GroupState::PreparingRebalance);
            if !transactional && live && matches!(verdict, GroupVerdict::Accept) {
                // Kafka's commit keeps a classic member's session alive in these states.
                crate::group::touch_heartbeat(c.group_id, c.member_id)?;
            }
            Ok(verdict)
        }
        // Offsets with no group row: the simple group Kafka would hold, empty.
        "simple" => Ok(validate_classic(c, transactional, GroupState::Empty, 0, &[])),
        // No group row is created: it would refuse a later KIP-848 consumer under this id.
        _ if c.generation < 0 => Ok(GroupVerdict::Accept),
        _ if !transactional && version >= OFFSET_COMMIT_GROUP_AWARE_FROM => {
            refuse(ErrorCode::GroupIdNotFound)
        }
        _ => refuse(ErrorCode::IllegalGeneration),
    }
}

fn validate_classic(
    c: &Committer<'_>,
    transactional: bool,
    state: GroupState,
    generation: i32,
    members: &[crate::group::Member],
) -> GroupVerdict {
    use GroupVerdict::{Accept, Refuse};
    if c.generation < 0 && state == GroupState::Empty {
        return Accept;
    }
    if c.generation >= 0 || !c.member_id.is_empty() || c.instance_id.is_some() {
        if let Some(instance) = c.instance_id {
            match members.iter().find(|m| m.group_instance_id.as_deref() == Some(instance)) {
                None => return Refuse(ErrorCode::UnknownMemberId),
                Some(m) if m.member_id != c.member_id => {
                    return Refuse(ErrorCode::FencedInstanceId)
                }
                Some(_) => {}
            }
        }
        if !members.iter().any(|m| m.member_id == c.member_id) {
            return Refuse(ErrorCode::UnknownMemberId);
        }
        if c.generation != generation {
            return Refuse(ErrorCode::IllegalGeneration);
        }
    } else if !transactional && state != GroupState::Empty {
        return Refuse(ErrorCode::UnknownMemberId);
    }
    if !transactional && state == GroupState::CompletingRebalance {
        return Refuse(ErrorCode::RebalanceInProgress);
    }
    Accept
}

fn validate_consumer_group(
    c: &Committer<'_>,
    transactional: bool,
    version: i16,
) -> Result<GroupVerdict, spi::Error> {
    let members: i64 = Spi::get_one_with_args(
        "SELECT count(*) FROM kafgres_consumer_group_members WHERE group_id = $1",
        &[c.group_id.into()],
    )?
    .unwrap_or(0);
    if c.generation < 0 && members == 0 {
        return Ok(GroupVerdict::Accept);
    }
    if transactional && c.generation == -1 && c.member_id.is_empty() && c.instance_id.is_none() {
        return Ok(GroupVerdict::Accept);
    }
    let row = Spi::connect(|client| {
        let rows = client.select(
            "SELECT member_epoch, owned || granted
               FROM kafgres_consumer_group_members WHERE group_id = $1 AND member_id = $2",
            Some(1),
            &[c.group_id.into(), c.member_id.into()],
        )?;
        for row in rows {
            let epoch = row.get::<i32>(1)?.unwrap_or(0);
            let held = row.get::<Vec<String>>(2)?.unwrap_or_default();
            return Ok::<_, spi::Error>(Some((epoch, held)));
        }
        Ok(None)
    })?;
    let Some((epoch, held)) = row else {
        return Ok(GroupVerdict::Refuse(ErrorCode::UnknownMemberId));
    };
    if !transactional && version < OFFSET_COMMIT_GROUP_AWARE_FROM {
        return Ok(GroupVerdict::Refuse(ErrorCode::UnsupportedVersion));
    }
    Ok(match c.generation.cmp(&epoch) {
        std::cmp::Ordering::Equal => GroupVerdict::Accept,
        std::cmp::Ordering::Greater => GroupVerdict::Refuse(ErrorCode::StaleMemberEpoch),
        // Kafka also checks the partition's assignment epoch, which is not stored here.
        std::cmp::Ordering::Less => GroupVerdict::HeldOnly(
            held.iter()
                .filter_map(|s| {
                    let (t, p) = s.split_once(':')?;
                    Some((t.parse().ok()?, p.parse().ok()?))
                })
                .collect(),
        ),
    })
}

pub fn offset_commit(
    req: &OffsetCommitRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<OffsetCommitResponse, HandlerError> {
    let group_denied = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id).err();
    // Both caps: empty topic entries sum to zero partitions.
    if req.topics.len() > MAX_OFFSET_PARTITIONS {
        return Err(HandlerError::TooLarge {
            what: "offset commit topic list",
            n: req.topics.len(),
        });
    }
    let total: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    if total > MAX_OFFSET_PARTITIONS {
        return Err(HandlerError::TooLarge {
            what: "offset commit partition list",
            n: total,
        });
    }

    let mut topics = Vec::with_capacity(req.topics.len());
    let committer = Committer {
        group_id: &req.group_id,
        generation: req.generation_id_or_member_epoch,
        member_id: &req.member_id,
        instance_id: req.group_instance_id.as_deref(),
    };
    // Only asked once a partition passes the topic checks, as in Kafka.
    let mut verdict: Option<GroupVerdict> = None;

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
        let count = match &resolved {
            Some(r) => meta::partition_count(r.topic_id)?,
            None => 0,
        };
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
                Some(_) if p.partition_index < 0 || p.partition_index >= count => {
                    ErrorCode::UnknownTopicOrPartition
                }
                Some(r) => {
                    let v = match &verdict {
                        Some(v) => v,
                        None => verdict.insert(validate_commit(&committer, false, version)?),
                    };
                    // Kafka's order: the group, then the metadata size, then the held partition.
                    match v {
                        GroupVerdict::Refuse(code) => *code,
                        _ if oversized => ErrorCode::OffsetMetadataTooLarge,
                        v => match v.code_for(r.topic_id, p.partition_index) {
                            ErrorCode::None => {
                                commit_and_report(&req.group_id, &t.name, r.topic_id, p)
                            }
                            code => code,
                        },
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

fn commit_and_report(
    group_id: &str,
    topic: &str,
    topic_id: u32,
    p: &OffsetCommitRequestPartition,
) -> ErrorCode {
    match commit_one(
        group_id,
        topic_id,
        p.partition_index,
        p.committed_offset,
        p.committed_leader_epoch,
        p.committed_metadata.as_deref(),
    ) {
        Ok(()) => ErrorCode::None,
        Err(e) => {
            pgrx::log!(
                "kafgres: offset commit failed for {topic}-{}: {e}",
                p.partition_index
            );
            ErrorCode::UnknownServerError
        }
    }
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
        // Each group carries its own topic list, so cap the group count too.
        super::check_admin_len("offset fetch group list", req.groups.len())?;
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
                topics: fetch_topics_v8(&g.group_id, requested, req.require_stable)?,
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
        topics: fetch_topics(&req.group_id, requested, req.require_stable)?,
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

/// Partitions whose committed offset for this group is staged in an unfinished transaction;
/// `RequireStable` (v7+) asks to wait rather than read a retractable offset.
fn unstable_for_topic(
    group_id: &str,
    topic_id: u32,
) -> Result<std::collections::HashSet<i32>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT o.partition
               FROM kafgres_txn_offsets o
               JOIN kafgres_txns t USING (producer_id)
              WHERE o.group_id = $1 AND o.topic_id = $2::oid AND t.state = 'ongoing'",
            None,
            &[group_id.into(), (topic_id as i32).into()],
        )?;
        let mut out = std::collections::HashSet::new();
        for row in rows {
            if let Some(p) = row.get::<i32>(1)? {
                out.insert(p);
            }
        }
        Ok(out)
    })
}

fn fetch_topics(
    group_id: &str,
    requested: Option<Vec<(String, Vec<i32>)>>,
    require_stable: bool,
) -> Result<Vec<OffsetFetchResponseTopic>, HandlerError> {
    let wanted: Vec<(String, u32, Vec<i32>)> = match requested {
        Some(list) => {
            // Capped before resolving: `resolve_requested` does one lookup per entry.
            if list.len() > MAX_OFFSET_PARTITIONS {
                return Err(HandlerError::TooLarge {
                    what: "offset fetch topic list",
                    n: list.len(),
                });
            }
            resolve_requested(list)?
        }
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
        let unstable = if require_stable {
            unstable_for_topic(group_id, topic_id)?
        } else {
            std::collections::HashSet::new()
        };
        let mut ps = Vec::with_capacity(partitions.len());
        for p in partitions {
            if unstable.contains(&p) {
                ps.push(OffsetFetchResponsePartition {
                    partition_index: p,
                    committed_offset: NO_OFFSET,
                    committed_leader_epoch: -1,
                    metadata: Some(String::new()),
                    error_code: ErrorCode::UnstableOffsetCommit.code(),
                    unknown_tagged_fields: Vec::new(),
                });
                continue;
            }
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
    require_stable: bool,
) -> Result<Vec<OffsetFetchResponseTopics>, HandlerError> {
    let simplified = requested
        .as_ref()
        .map(|list| list.iter().map(|(n, _, p)| (n.clone(), p.clone())).collect());
    let topics = fetch_topics(group_id, simplified, require_stable)?;

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
