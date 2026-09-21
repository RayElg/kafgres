//! Handlers for APIs this broker cannot honor, each returning the error Kafka itself sends
//! when the feature is unconfigured: `34 AlterReplicaLogDirs`, `38`-`41` delegation tokens,
//! `45 AlterPartitionReassignments`, `57 UpdateFeatures`, `64 UnregisterBroker`,
//! `74 ListConfigResources`, `80 AddRaftVoter`, `81 RemoveRaftVoter`. The keys are advertised
//! anyway because clients version the broker by probing the advertised key set (franz-go's probe).

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::add_raft_voter_request::AddRaftVoterRequest;
use kafgres_codec::generated::add_raft_voter_response::AddRaftVoterResponse;
use kafgres_codec::generated::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;
use kafgres_codec::generated::alter_partition_reassignments_response::{
    AlterPartitionReassignmentsResponse, ReassignablePartitionResponse, ReassignableTopicResponse,
};
use kafgres_codec::generated::alter_replica_log_dirs_request::AlterReplicaLogDirsRequest;
use kafgres_codec::generated::alter_replica_log_dirs_response::{
    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponse,
};
use kafgres_codec::generated::create_delegation_token_request::CreateDelegationTokenRequest;
use kafgres_codec::generated::create_delegation_token_response::CreateDelegationTokenResponse;
use kafgres_codec::generated::describe_delegation_token_request::DescribeDelegationTokenRequest;
use kafgres_codec::generated::describe_delegation_token_response::DescribeDelegationTokenResponse;
use kafgres_codec::generated::expire_delegation_token_request::ExpireDelegationTokenRequest;
use kafgres_codec::generated::expire_delegation_token_response::ExpireDelegationTokenResponse;
use kafgres_codec::generated::list_config_resources_request::ListConfigResourcesRequest;
use kafgres_codec::generated::list_config_resources_response::{
    ConfigResource, ListConfigResourcesResponse,
};
use kafgres_codec::generated::remove_raft_voter_request::RemoveRaftVoterRequest;
use kafgres_codec::generated::remove_raft_voter_response::RemoveRaftVoterResponse;
use kafgres_codec::generated::renew_delegation_token_request::RenewDelegationTokenRequest;
use kafgres_codec::generated::renew_delegation_token_response::RenewDelegationTokenResponse;
use kafgres_codec::generated::unregister_broker_request::UnregisterBrokerRequest;
use kafgres_codec::generated::unregister_broker_response::UnregisterBrokerResponse;
use kafgres_codec::generated::update_features_request::UpdateFeaturesRequest;
use kafgres_codec::generated::update_features_response::{
    UpdatableFeatureResult, UpdateFeaturesResponse,
};

use super::HandlerError;
use crate::acl::{Authz, Operation, ResourceType};

/// Kafka's sentinel for a token that does not expire or was never issued.
const NO_TIMESTAMP: i64 = -1;

/// `ConfigResource.Type` codes for the two resource types kafgres stores config for.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

fn cluster_denied(authz: &Authz, op: Operation) -> Option<ErrorCode> {
    authz.check(op, ResourceType::Cluster, "kafka-cluster").err()
}

/// `34 AlterReplicaLogDirs`: one log directory, so any other path is `LOG_DIR_NOT_FOUND`.
pub fn alter_replica_log_dirs(
    req: &AlterReplicaLogDirsRequest,
    log_dir: &str,
    authz: &Authz,
) -> Result<AlterReplicaLogDirsResponse, HandlerError> {
    let denied = cluster_denied(authz, Operation::Alter);

    // I8: one response entry per (topic, partition) named by the caller; cap before building.
    super::check_admin_len("alter replica log dirs", req.dirs.len())?;
    let partitions: usize = req
        .dirs
        .iter()
        .flat_map(|d| d.topics.iter())
        .map(|t| t.partitions.len())
        .sum();
    super::check_admin_len("alter replica log dir partitions", partitions)?;

    let mut results: Vec<AlterReplicaLogDirTopicResult> = Vec::new();
    for dir in &req.dirs {
        let code = match denied {
            Some(c) => c,
            None if dir.path == log_dir => ErrorCode::None,
            None => ErrorCode::LogDirNotFound,
        };
        for topic in &dir.topics {
            results.push(AlterReplicaLogDirTopicResult {
                topic_name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|p| AlterReplicaLogDirPartitionResult {
                        partition_index: *p,
                        error_code: code.code(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            });
        }
    }

    Ok(AlterReplicaLogDirsResponse {
        throttle_time_ms: 0,
        results,
        ..Default::default()
    })
}

/// `38 CreateDelegationToken`: kafgres issues no tokens; the answer is always `DELEGATION_TOKEN_AUTH_DISABLED`.
pub fn create_delegation_token(
    _req: &CreateDelegationTokenRequest,
) -> Result<CreateDelegationTokenResponse, HandlerError> {
    Ok(CreateDelegationTokenResponse {
        error_code: ErrorCode::DelegationTokenAuthDisabled.code(),
        issue_timestamp_ms: NO_TIMESTAMP,
        expiry_timestamp_ms: NO_TIMESTAMP,
        max_timestamp_ms: NO_TIMESTAMP,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// `39 RenewDelegationToken`: see [`create_delegation_token`].
pub fn renew_delegation_token(
    _req: &RenewDelegationTokenRequest,
) -> Result<RenewDelegationTokenResponse, HandlerError> {
    Ok(RenewDelegationTokenResponse {
        error_code: ErrorCode::DelegationTokenAuthDisabled.code(),
        expiry_timestamp_ms: NO_TIMESTAMP,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// `40 ExpireDelegationToken`: see [`create_delegation_token`].
pub fn expire_delegation_token(
    _req: &ExpireDelegationTokenRequest,
) -> Result<ExpireDelegationTokenResponse, HandlerError> {
    Ok(ExpireDelegationTokenResponse {
        error_code: ErrorCode::DelegationTokenAuthDisabled.code(),
        expiry_timestamp_ms: NO_TIMESTAMP,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// `41 DescribeDelegationToken`: the disabled code with an empty token list, not `NONE`.
pub fn describe_delegation_token(
    _req: &DescribeDelegationTokenRequest,
) -> Result<DescribeDelegationTokenResponse, HandlerError> {
    Ok(DescribeDelegationTokenResponse {
        error_code: ErrorCode::DelegationTokenAuthDisabled.code(),
        tokens: Vec::new(),
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// `45 AlterPartitionReassignments`: one replica per partition, so an assignment is refused
/// with `INVALID_REPLICA_ASSIGNMENT` and a cancellation gets `NO_REASSIGNMENT_IN_PROGRESS`.
pub fn alter_partition_reassignments(
    req: &AlterPartitionReassignmentsRequest,
    authz: &Authz,
) -> Result<AlterPartitionReassignmentsResponse, HandlerError> {
    if let Some(code) = cluster_denied(authz, Operation::Alter) {
        return Ok(AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to alter the cluster".to_string()),
            responses: Vec::new(),
            ..Default::default()
        });
    }

    // I8: one response entry per partition the caller names; cap before building.
    super::check_admin_len("reassignment topics", req.topics.len())?;
    let partitions: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    super::check_admin_len("reassignment partitions", partitions)?;

    const NO_MOVE: &str =
        "this broker has one replica per partition; replication is the database's";

    let responses = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| {
                    let (code, msg) = match &p.replicas {
                        None => (ErrorCode::NoReassignmentInProgress, None),
                        Some(_) => (ErrorCode::InvalidReplicaAssignment, Some(NO_MOVE)),
                    };
                    ReassignablePartitionResponse {
                        partition_index: p.partition_index,
                        error_code: code.code(),
                        error_message: msg.map(str::to_string),
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        })
        .collect();

    Ok(AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        responses,
        ..Default::default()
    })
}

/// `57 UpdateFeatures`: kafgres finalizes no feature flags, so every update is refused with
/// `FEATURE_UPDATE_FAILED` (top-level at v2, which dropped the per-feature results array).
pub fn update_features(
    req: &UpdateFeaturesRequest,
    version: i16,
    authz: &Authz,
) -> Result<UpdateFeaturesResponse, HandlerError> {
    if let Some(code) = cluster_denied(authz, Operation::Alter) {
        return Ok(UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to alter the cluster".to_string()),
            results: Vec::new(),
            ..Default::default()
        });
    }

    const WHY: &str =
        "this broker finalizes no feature flags; the served protocol is fixed at build time";

    if version >= 2 {
        return Ok(UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::FeatureUpdateFailed.code(),
            error_message: Some(WHY.to_string()),
            results: Vec::new(),
            ..Default::default()
        });
    }

    let results = req
        .feature_updates
        .iter()
        .map(|f| UpdatableFeatureResult {
            feature: f.feature.clone(),
            error_code: ErrorCode::FeatureUpdateFailed.code(),
            error_message: Some(WHY.to_string()),
            ..Default::default()
        })
        .collect();

    Ok(UpdateFeaturesResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        results,
        ..Default::default()
    })
}

/// `64 UnregisterBroker`: the cluster is this one broker, so the request cannot succeed.
pub fn unregister_broker(
    _req: &UnregisterBrokerRequest,
    authz: &Authz,
) -> Result<UnregisterBrokerResponse, HandlerError> {
    if let Some(code) = cluster_denied(authz, Operation::Alter) {
        return Ok(UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to alter the cluster".to_string()),
            ..Default::default()
        });
    }

    Ok(UnregisterBrokerResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::InvalidRequest.code(),
        error_message: Some(
            "this cluster is a single broker embedded in PostgreSQL and cannot \
             unregister itself"
                .to_string(),
        ),
        ..Default::default()
    })
}

/// `74 ListConfigResources` (KIP-1142): the resource types `DescribeConfigs` serves.
pub fn list_config_resources(
    req: &ListConfigResourcesRequest,
    authz: &Authz,
) -> Result<ListConfigResourcesResponse, HandlerError> {
    if let Some(code) = cluster_denied(authz, Operation::Describe) {
        return Ok(ListConfigResourcesResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            config_resources: Vec::new(),
            ..Default::default()
        });
    }

    let wanted = |t: i8| req.resource_types.is_empty() || req.resource_types.contains(&t);

    let mut config_resources = Vec::new();
    if wanted(RESOURCE_TYPE_BROKER) {
        config_resources.push(ConfigResource {
            resource_name: crate::node_id().to_string(),
            resource_type: RESOURCE_TYPE_BROKER,
            ..Default::default()
        });
    }
    if wanted(RESOURCE_TYPE_TOPIC) {
        let topics = crate::meta::load_topics(None).map_err(|e| {
            pgrx::log!("kafgres: list_config_resources topic list failed: {e}");
            HandlerError::Internal(format!("topic list: {e}"))
        })?;
        for t in topics {
            config_resources.push(ConfigResource {
                resource_name: t.name,
                resource_type: RESOURCE_TYPE_TOPIC,
                ..Default::default()
            });
        }
    }

    Ok(ListConfigResourcesResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        config_resources,
        ..Default::default()
    })
}

/// `80 AddRaftVoter`: there is no Raft; Postgres replication carries the metadata.
pub fn add_raft_voter(
    _req: &AddRaftVoterRequest,
    authz: &Authz,
) -> Result<AddRaftVoterResponse, HandlerError> {
    if let Some(code) = cluster_denied(authz, Operation::Alter) {
        return Ok(AddRaftVoterResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to alter the cluster".to_string()),
            ..Default::default()
        });
    }
    Ok(AddRaftVoterResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::InvalidRequest.code(),
        error_message: Some(NO_RAFT.to_string()),
        ..Default::default()
    })
}

/// `81 RemoveRaftVoter`: see [`add_raft_voter`].
pub fn remove_raft_voter(
    _req: &RemoveRaftVoterRequest,
    authz: &Authz,
) -> Result<RemoveRaftVoterResponse, HandlerError> {
    if let Some(code) = cluster_denied(authz, Operation::Alter) {
        return Ok(RemoveRaftVoterResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to alter the cluster".to_string()),
            ..Default::default()
        });
    }
    Ok(RemoveRaftVoterResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::InvalidRequest.code(),
        error_message: Some(NO_RAFT.to_string()),
        ..Default::default()
    })
}

const NO_RAFT: &str =
    "this broker has no metadata quorum; replication and failover are PostgreSQL's";
