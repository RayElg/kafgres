use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::create_partitions_request::CreatePartitionsRequest;
use kafgres_codec::generated::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use kafgres_codec::generated::create_topics_request::CreateTopicsRequest;
use kafgres_codec::generated::create_topics_response::{CreatableTopicResult, CreateTopicsResponse};
use kafgres_codec::generated::delete_topics_request::DeleteTopicsRequest;
use kafgres_codec::generated::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse};

use super::HandlerError;
use crate::meta::{self, TopicError};

fn creation_outcome(
    topic: &kafgres_codec::generated::create_topics_request::CreatableTopic,
    validate_only: bool,
) -> Result<meta::CreatedTopic, TopicError> {
    meta::validate_topic_name(&topic.name)?;
    meta::validate_replication_factor(topic.replication_factor)?;
    if topic.num_partitions < 1 && topic.num_partitions != -1 {
        return Err(TopicError::InvalidPartitions(topic.num_partitions));
    }
    // -1 is "use the broker default", which for a single-broker deployment is 1.
    let partitions = if topic.num_partitions == -1 {
        1
    } else {
        topic.num_partitions
    };

    let mut config = Vec::new();
    for entry in &topic.configs {
        let def = crate::config::topic_def(&entry.name)
            .ok_or_else(|| TopicError::InvalidConfig(format!("unknown config '{}'", entry.name)))?;
        crate::config::check_alterable(def, crate::config::OP_SET, entry.value.as_deref())
            .map_err(|e| TopicError::InvalidConfig(e.to_string()))?;
        if !def.writable {
            continue;
        }
        if let Some(v) = &entry.value {
            config.push((entry.name.clone(), v.clone()));
        }
    }

    if validate_only {
        if meta::topic_id_by_name(&topic.name)?.is_some() {
            return Err(TopicError::AlreadyExists);
        }
        return Ok(meta::CreatedTopic {
            topic_id: 0,
            uuid: [0u8; 16],
            partitions,
        });
    }
    // Its own savepoint: a partway failure folds into a per-topic error code, and the
    crate::dbtx::atomically(
        || meta::create_topic(&topic.name, partitions, &config),
        |_| TopicError::Internal("create aborted (lock or statement timeout)".to_string()),
    )
}

pub fn create_topics(
    req: &CreateTopicsRequest,
    authz: &crate::acl::Authz,
) -> Result<CreateTopicsResponse, HandlerError> {
    super::check_admin_len("create topics list", req.topics.len())?;
    let mut topics = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        // Echo what was actually created, not the -1 that asked for the broker default.
        if let Err(code) = authz
            .check(crate::acl::Operation::Create, crate::acl::ResourceType::Topic, &topic.name)
            .or_else(|e| authz.check(crate::acl::Operation::Create, crate::acl::ResourceType::Cluster, "kafka-cluster").map_err(|_| e))
        {
            topics.push(CreatableTopicResult {
                name: topic.name.clone(),
                error_code: code.code(),
                error_message: None,
                num_partitions: -1,
                replication_factor: -1,
                ..Default::default()
            });
            continue;
        }
        let result = match creation_outcome(topic, req.validate_only) {
            Ok(made) => CreatableTopicResult {
                name: topic.name.clone(),
                topic_id: kafgres_codec::Uuid(made.uuid),
                error_code: ErrorCode::None.code(),
                error_message: None,
                num_partitions: made.partitions,
                replication_factor: 1,
                ..Default::default()
            },
            Err(e) => {
                pgrx::log!("kafgres: create topic '{}' failed: {e}", topic.name);
                CreatableTopicResult {
                    name: topic.name.clone(),
                    error_code: e.error_code().code(),
                    error_message: Some(e.to_string()),
                    num_partitions: -1,
                    replication_factor: -1,
                    ..Default::default()
                }
            }
        };
        topics.push(result);
    }

    Ok(CreateTopicsResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

pub fn delete_topics(
    req: &DeleteTopicsRequest,
    authz: &crate::acl::Authz,
) -> Result<DeleteTopicsResponse, HandlerError> {
    let mut wanted: Vec<(Option<String>, [u8; 16])> = Vec::new();
    for t in &req.topics {
        wanted.push((t.name.clone(), t.topic_id.0));
    }
    for name in &req.topic_names {
        wanted.push((Some(name.clone()), [0u8; 16]));
    }

    super::check_admin_len("delete topics list", wanted.len())?;
    let mut responses = Vec::with_capacity(wanted.len());
    for (name, uuid) in wanted {
        let resolved = match (&name, uuid != [0u8; 16]) {
            (_, true) => meta::topic_by_uuid(&uuid).map_err(|e| {
                HandlerError::Internal(format!("delete topics uuid lookup: {e}"))
            })?,
            (Some(n), false) => meta::topic_id_by_name(n)
                .map_err(|e| HandlerError::Internal(format!("delete topics lookup: {e}")))?
                .map(|id| (id, n.clone())),
            (None, false) => None,
        };

        // Authorized against the *resolved* name: a uuid-addressed request has no name of its own.
        let acl_name = resolved
            .as_ref()
            .map(|(_, n)| n.clone())
            .or_else(|| name.clone())
            .unwrap_or_default();
        if let Err(code) = authz.check(crate::acl::Operation::Delete, crate::acl::ResourceType::Topic, &acl_name) {
            responses.push(DeletableTopicResult {
                name: name.clone(),
                topic_id: kafgres_codec::Uuid(uuid),
                error_code: code.code(),
                error_message: None,
                ..Default::default()
            });
            continue;
        }

        let (code, message, echoed_name) = match resolved {
            None => (
                ErrorCode::UnknownTopicOrPartition,
                Some("unknown topic".to_string()),
                name.clone(),
            ),
            Some((_, real_name)) => match meta::delete_topic(&real_name) {
                Ok(true) => (ErrorCode::None, None, Some(real_name)),
                Ok(false) => (
                    ErrorCode::UnknownTopicOrPartition,
                    Some("unknown topic".to_string()),
                    Some(real_name),
                ),
                Err(e) => {
                    pgrx::log!("kafgres: delete topic '{real_name}' failed: {e}");
                    (e.error_code(), Some(e.to_string()), Some(real_name))
                }
            },
        };

        responses.push(DeletableTopicResult {
            name: echoed_name,
            topic_id: kafgres_codec::Uuid(uuid),
            error_code: code.code(),
            error_message: message,
            ..Default::default()
        });
    }

    Ok(DeleteTopicsResponse {
        responses,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

pub fn create_partitions(
    req: &CreatePartitionsRequest,
    authz: &crate::acl::Authz,
) -> Result<CreatePartitionsResponse, HandlerError> {
    super::check_admin_len("create partitions list", req.topics.len())?;
    let mut results = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        if let Err(code) = authz.check(crate::acl::Operation::Alter, crate::acl::ResourceType::Topic, &topic.name) {
            results.push(CreatePartitionsTopicResult {
                name: topic.name.clone(),
                error_code: code.code(),
                error_message: None,
                ..Default::default()
            });
            continue;
        }
        let outcome = if req.validate_only {
            match meta::topic_id_by_name(&topic.name) {
                Ok(None) => Err(TopicError::UnknownTopic),
                Ok(Some(_)) => Ok(()),
                Err(e) => Err(TopicError::from(e)),
            }
        } else {
            crate::dbtx::atomically(
                || meta::create_partitions(&topic.name, topic.count),
                |_| TopicError::Internal("expand aborted (lock or statement timeout)".to_string()),
            )
        };

        let (code, message) = match outcome {
            Ok(()) => (ErrorCode::None, None),
            Err(e) => {
                pgrx::log!("kafgres: create partitions on '{}' failed: {e}", topic.name);
                (e.error_code(), Some(e.to_string()))
            }
        };
        results.push(CreatePartitionsTopicResult {
            name: topic.name.clone(),
            error_code: code.code(),
            error_message: message,
            ..Default::default()
        });
    }

    Ok(CreatePartitionsResponse {
        results,
        throttle_time_ms: 0,
        ..Default::default()
    })
}
