//! `3 Metadata` — singleton broker: every partition reports this node as leader, replica, ISR.

use std::collections::HashSet;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::describe_topic_partitions_request::DescribeTopicPartitionsRequest;
use kafgres_codec::generated::describe_topic_partitions_response::{
    Cursor, DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
    DescribeTopicPartitionsResponseTopic,
};

use super::HandlerError;
use kafgres_codec::generated::metadata_request::MetadataRequest;
use kafgres_codec::generated::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafgres_codec::Uuid;

use crate::meta::{self, TopicMeta};

pub struct ClusterConfig {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub cluster_id: String,
}

const MAX_REQUESTED_TOPICS: usize = 10_000;

/// Auto-create is DDL on the single-threaded broker loop, bounded tighter than response bytes.
const MAX_AUTO_CREATES: usize = 16;

/// Kafka's "authorized operations not computed" sentinel; zero would claim none.
const AUTHORIZED_OPERATIONS_UNSET: i32 = i32::MIN;

pub fn handle(
    req: &MetadataRequest,
    version: i16,
    cfg: &ClusterConfig,
    authz: &crate::acl::Authz,
) -> Result<MetadataResponse, HandlerError> {
    // Null topics and an empty v0 list mean "every topic" (MetadataRequest.java); empty v1+ means none.
    let all_topics = match req.topics.as_ref() {
        None => true,
        Some(ts) => ts.is_empty() && version == 0,
    };

    let requested: &[_] = if all_topics {
        &[]
    } else {
        req.topics.as_deref().unwrap_or(&[])
    };

    if requested.len() > MAX_REQUESTED_TOPICS {
        return Err(HandlerError::TooLarge {
            what: "metadata topic list",
            n: requested.len(),
        });
    }

    let (by_name, raw_by_id): (Vec<_>, Vec<_>) =
        requested.iter().partition(|t| t.name.is_some());

    let mut names: Vec<String> = by_name.iter().filter_map(|t| t.name.clone()).collect();
    let mut by_id = Vec::new();
    for t in raw_by_id {
        match meta::topic_by_uuid(&t.topic_id.0) {
            Ok(Some((_, name))) => names.push(name),
            Ok(None) => by_id.push(t),
            Err(e) => {
                pgrx::log!("kafgres: metadata uuid lookup failed: {e}");
                return Err(HandlerError::Internal(format!("uuid lookup: {e}")));
            }
        }
    }

    let filter = if all_topics { None } else { Some(names.as_slice()) };

    // A failed read fails the request; an empty success would tell clients topics are gone.
    let found = match (all_topics, names.is_empty()) {
        (false, true) => Vec::new(),
        _ => meta::load_topics(filter).map_err(|e| {
            pgrx::log!("kafgres: metadata query failed, failing the request: {e}");
            HandlerError::Internal(format!("metadata query: {e}"))
        })?,
    };

    // Unnamed listings omit unauthorized topics; named ones get an error entry below.
    let mut topics: Vec<MetadataResponseTopic> = found
        .iter()
        .filter(|t| {
            !names.is_empty()
                || authz.allows(
                    crate::acl::Operation::Describe,
                    crate::acl::ResourceType::Topic,
                    &t.name,
                )
        })
        .map(|t| describe_topic(t, cfg.node_id))
        .collect();

    for t in topics.iter_mut() {
        if let Some(name) = t.name.clone() {
            if let Err(code) = authz.check(
                crate::acl::Operation::Describe,
                crate::acl::ResourceType::Topic,
                &name,
            ) {
                t.error_code = code.code();
                t.partitions.clear();
            }
        }
    }

    let have: HashSet<&str> = found.iter().map(|t| t.name.as_str()).collect();
    let mut created = 0usize;
    for name in &names {
        if !have.contains(name.as_str()) {
            // Auto-create lives here: producers send Metadata with `allow_auto_topic_creation` first.
            if crate::auto_create_topics() && req.allow_auto_topic_creation {
                match authz.check(
                    crate::acl::Operation::Create,
                    crate::acl::ResourceType::Topic,
                    name,
                ) {
                    // Terminal, not retriable UNKNOWN_TOPIC_OR_PARTITION: the producer would spin out `max.block.ms`.
                    Err(code) => {
                        topics.push(MetadataResponseTopic {
                            error_code: code.code(),
                            name: Some(name.clone()),
                            topic_id: Uuid::ZERO,
                            is_internal: false,
                            partitions: Vec::new(),
                            topic_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
                            unknown_tagged_fields: Vec::new(),
                        });
                        continue;
                    }
                    Ok(()) if created < MAX_AUTO_CREATES => {
                        if let Some(entry) = auto_create(name, cfg)? {
                            created += 1;
                            topics.push(entry);
                            continue;
                        }
                    }
                    Ok(()) => {}
                }
            }
            topics.push(MetadataResponseTopic {
                error_code: ErrorCode::UnknownTopicOrPartition.code(),
                name: Some(name.clone()),
                topic_id: Uuid::ZERO,
                is_internal: false,
                partitions: Vec::new(),
                topic_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
                unknown_tagged_fields: Vec::new(),
            });
        }
    }
    for t in by_id {
        topics.push(MetadataResponseTopic {
            error_code: ErrorCode::UnknownTopicId.code(),
            name: None,
            topic_id: t.topic_id,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
            unknown_tagged_fields: Vec::new(),
        });
    }

    Ok(MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![MetadataResponseBroker {
            node_id: cfg.node_id,
            host: cfg.host.clone(),
            port: cfg.port,
            rack: None,
            unknown_tagged_fields: Vec::new(),
        }],
        cluster_id: Some(cfg.cluster_id.clone()),
        controller_id: cfg.node_id,
        topics,
        cluster_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
        error_code: ErrorCode::None.code(),
        unknown_tagged_fields: Vec::new(),
    })
}

fn describe_topic(t: &TopicMeta, node_id: i32) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code: ErrorCode::None.code(),
        name: Some(t.name.clone()),
        // The stored uuid: clients treat a changed topic id as a different topic.
        topic_id: Uuid(t.uuid),
        is_internal: false,
        partitions: t
            .partitions
            .iter()
            .map(|p| MetadataResponsePartition {
                error_code: ErrorCode::None.code(),
                partition_index: p.partition,
                leader_id: node_id,
                leader_epoch: p.leader_epoch,
                replica_nodes: vec![node_id],
                isr_nodes: vec![node_id],
                offline_replicas: Vec::new(),
                unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        topic_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
        unknown_tagged_fields: Vec::new(),
    }
}

/// Paginated Metadata: an empty topic list still means *every* topic; the cursor is inclusive.
pub fn describe_topic_partitions(
    req: &DescribeTopicPartitionsRequest,
    cfg: &ClusterConfig,
    authz: &crate::acl::Authz,
) -> Result<DescribeTopicPartitionsResponse, HandlerError> {
    super::check_admin_len("described topics", req.topics.len())?;

    // Capping is safe, not lossy: the client resumes from `next_cursor`.
    const MAX_PARTITIONS_PER_RESPONSE: i32 = 2_000;
    let limit = req
        .response_partition_limit
        .clamp(1, MAX_PARTITIONS_PER_RESPONSE);

    let named: Option<Vec<String>> = if req.topics.is_empty() {
        None
    } else {
        Some(req.topics.iter().map(|t| t.name.clone()).collect())
    };
    let mut loaded = meta::load_topics(named.as_deref())?;
    loaded.sort_by(|a, b| a.name.cmp(&b.name));

    let mut missing: Vec<String> = Vec::new();
    if let Some(names) = &named {
        for n in names {
            if !loaded.iter().any(|t| &t.name == n) {
                missing.push(n.clone());
            }
        }
    }

    let (from_topic, from_partition) = match &req.cursor {
        Some(c) => (Some(c.topic_name.clone()), c.partition_index),
        None => (None, 0),
    };

    let mut topics: Vec<DescribeTopicPartitionsResponseTopic> = Vec::new();
    let mut budget = limit;
    let mut next_cursor = None;

    for t in &loaded {
        if let Some(start) = &from_topic {
            if &t.name < start {
                continue;
            }
        }
        if authz
            .check(crate::acl::Operation::Describe, crate::acl::ResourceType::Topic, &t.name)
            .is_err()
        {
            // Denials spend budget too, or the limit bounds nothing.
            if budget == 0 {
                next_cursor = Some(Cursor {
                    topic_name: t.name.clone(),
                    partition_index: 0,
                    ..Default::default()
                });
                break;
            }
            budget -= 1;
            topics.push(unknown_topic_entry(&t.name, ErrorCode::TopicAuthorizationFailed));
            continue;
        }
        let skip_below = match &from_topic {
            Some(start) if start == &t.name => from_partition,
            _ => 0,
        };

        let mut partitions = Vec::new();
        for p in t.partitions.iter().filter(|p| p.partition >= skip_below) {
            if budget == 0 {
                next_cursor = Some(Cursor {
                    topic_name: t.name.clone(),
                    partition_index: p.partition,
                    ..Default::default()
                });
                break;
            }
            budget -= 1;
            partitions.push(DescribeTopicPartitionsResponsePartition {
                error_code: ErrorCode::None.code(),
                partition_index: p.partition,
                leader_id: cfg.node_id,
                leader_epoch: p.leader_epoch,
                replica_nodes: vec![cfg.node_id],
                isr_nodes: vec![cfg.node_id],
                // Empty, never null: `KafkaAdminClient` calls `.stream()` on these fields unchecked.
                eligible_leader_replicas: Some(Vec::new()),
                last_known_elr: Some(Vec::new()),
                offline_replicas: Vec::new(),
                ..Default::default()
            });
        }
        if !partitions.is_empty() || next_cursor.is_none() {
            topics.push(DescribeTopicPartitionsResponseTopic {
                error_code: ErrorCode::None.code(),
                name: Some(t.name.clone()),
                topic_id: Uuid(t.uuid),
                is_internal: false,
                partitions,
                topic_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
                ..Default::default()
            });
        }
        if next_cursor.is_some() {
            break;
        }
    }

    for name in missing {
        topics.push(unknown_topic_entry(&name, ErrorCode::UnknownTopicOrPartition));
    }
    topics.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(DescribeTopicPartitionsResponse {
        throttle_time_ms: 0,
        topics,
        next_cursor,
        ..Default::default()
    })
}

fn unknown_topic_entry(name: &str, code: ErrorCode) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic {
        error_code: code.code(),
        name: Some(name.to_string()),
        topic_id: Uuid::ZERO,
        is_internal: false,
        partitions: Vec::new(),
        topic_authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
        ..Default::default()
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn cfg() -> ClusterConfig {
        ClusterConfig {
            node_id: 1,
            host: "localhost".into(),
            port: 9092,
            cluster_id: "kafgres".into(),
        }
    }

    #[test]
    fn unknown_topics_come_back_as_entries_not_omissions() {
        let t = TopicMeta {
            topic_id: 1,
            name: "known".into(),
            uuid: [7u8; 16],
            partitions: vec![],
        };
        let described = describe_topic(&t, 1);
        assert_eq!(described.error_code, 0);
        assert_eq!(described.name.as_deref(), Some("known"));
    }

    #[test]
    fn every_partition_is_led_by_this_node() {
        let t = TopicMeta {
            topic_id: 7,
            name: "orders".into(),
            uuid: [9u8; 16],
            partitions: vec![
                crate::meta::PartitionMeta {
                    partition: 0,
                    leader_epoch: 0,
                },
                crate::meta::PartitionMeta {
                    partition: 1,
                    leader_epoch: 3,
                },
            ],
        };
        let d = describe_topic(&t, 1);
        assert_eq!(d.partitions.len(), 2);
        for p in &d.partitions {
            assert_eq!(p.leader_id, 1);
            assert_eq!(p.replica_nodes, vec![1]);
            assert_eq!(p.isr_nodes, vec![1]);
            assert!(p.offline_replicas.is_empty());
        }
        assert_eq!(d.partitions[1].leader_epoch, 3);
    }
}

/// Create a missing topic, then describe it; `Ok(None)` = refused or lost race. One partition.
fn auto_create(
    name: &str,
    cfg: &ClusterConfig,
) -> Result<Option<MetadataResponseTopic>, HandlerError> {
    // In a subtransaction: a lost race raises a duplicate-key longjmp that would abort the whole response.
    let made = crate::dbtx::atomically(
        || match meta::create_topic(name, 1, &[]) {
            Ok(_) => Ok(true),
            Err(e) => {
                pgrx::log!("kafgres: auto-create of topic {name:?} refused: {e}");
                Ok(false)
            }
        },
        |_| HandlerError::Internal(String::new()),
    )
    .unwrap_or(false);
    if !made {
        return Ok(None);
    }
    let found = meta::load_topics(Some(&[name.to_string()]))
        .map_err(|e| HandlerError::Internal(format!("metadata query after auto-create: {e}")))?;
    Ok(found.first().map(|t| describe_topic(t, cfg.node_id)))
}
