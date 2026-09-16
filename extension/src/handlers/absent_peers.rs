//! Protocols whose counterpart does not exist on a single embedded broker: `55
//! DescribeQuorum`, the share-coordinator persister RPCs `83`-`87`, and the streams group
//! protocol `88`/`89`, each answered with an error rather than a fabricated success.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::delete_share_group_state_request::DeleteShareGroupStateRequest;
use kafgres_codec::generated::delete_share_group_state_response::{
    DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult as DeletePartitionResult,
};
use kafgres_codec::generated::describe_quorum_request::DescribeQuorumRequest;
use kafgres_codec::generated::describe_quorum_response::DescribeQuorumResponse;
use kafgres_codec::generated::initialize_share_group_state_request::InitializeShareGroupStateRequest;
use kafgres_codec::generated::initialize_share_group_state_response::{
    InitializeShareGroupStateResponse, InitializeStateResult,
    PartitionResult as InitPartitionResult,
};
use kafgres_codec::generated::read_share_group_state_request::ReadShareGroupStateRequest;
use kafgres_codec::generated::read_share_group_state_response::{
    PartitionResult as ReadPartitionResult, ReadShareGroupStateResponse, ReadStateResult,
};
use kafgres_codec::generated::read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest;
use kafgres_codec::generated::read_share_group_state_summary_response::{
    PartitionResult as SummaryPartitionResult, ReadShareGroupStateSummaryResponse,
    ReadStateSummaryResult,
};
use kafgres_codec::generated::streams_group_describe_request::StreamsGroupDescribeRequest;
use kafgres_codec::generated::streams_group_describe_response::{
    DescribedGroup, StreamsGroupDescribeResponse,
};
use kafgres_codec::generated::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;
use kafgres_codec::generated::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
use kafgres_codec::generated::write_share_group_state_request::WriteShareGroupStateRequest;
use kafgres_codec::generated::write_share_group_state_response::{
    PartitionResult as WritePartitionResult, WriteShareGroupStateResponse, WriteStateResult,
};

use super::{check_admin_len, HandlerError};
use crate::acl::{Authz, Operation, ResourceType};

const NO_PERSISTER: &str =
    "this broker is its own share coordinator; share-group state is not reachable \
     through the inter-broker persister protocol";

const NO_STREAMS: &str = "the streams rebalance protocol (KIP-1071) is not served by this broker";

const NO_RAFT: &str =
    "this broker has no metadata quorum; replication and failover are PostgreSQL's";

/// Cap both levels of a `topics -> partitions` request before building the response (I8).
fn check_share_state_len<T>(
    topics: &[T],
    partitions: impl Fn(&T) -> usize,
) -> Result<(), HandlerError> {
    check_admin_len("share state topics", topics.len())?;
    let total: usize = topics.iter().map(partitions).sum();
    check_admin_len("share state partitions", total)
}

/// The code and reason every persister RPC partition result carries; authorization denial wins.
fn persister_outcome(authz: &Authz) -> (ErrorCode, &'static str) {
    match authz.check(Operation::ClusterAction, ResourceType::Cluster, "kafka-cluster") {
        Err(code) => (code, "not authorized to act on the cluster"),
        Ok(()) => (ErrorCode::InvalidRequest, NO_PERSISTER),
    }
}

/// `55 DescribeQuorum`: there is no Raft quorum here; metadata lives in Postgres.
pub fn describe_quorum(
    _req: &DescribeQuorumRequest,
    authz: &Authz,
) -> Result<DescribeQuorumResponse, HandlerError> {
    let (code, why) = match authz.check(Operation::Describe, ResourceType::Cluster, "kafka-cluster")
    {
        Err(code) => (code, "not authorized to describe the cluster"),
        Ok(()) => (ErrorCode::InvalidRequest, NO_RAFT),
    };
    Ok(DescribeQuorumResponse {
        error_code: code.code(),
        error_message: Some(why.to_string()),
        topics: Vec::new(),
        nodes: Vec::new(),
        ..Default::default()
    })
}

/// `83 InitializeShareGroupState`: see [`NO_PERSISTER`].
pub fn initialize_share_group_state(
    req: &InitializeShareGroupStateRequest,
    authz: &Authz,
) -> Result<InitializeShareGroupStateResponse, HandlerError> {
    check_share_state_len(&req.topics, |t| t.partitions.len())?;
    let (code, why) = persister_outcome(authz);
    let results = req
        .topics
        .iter()
        .map(|t| InitializeStateResult {
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| InitPartitionResult {
                    partition: p.partition,
                    error_code: code.code(),
                    error_message: Some(why.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    Ok(InitializeShareGroupStateResponse {
        results,
        ..Default::default()
    })
}

/// `84 ReadShareGroupState`: see [`NO_PERSISTER`].
pub fn read_share_group_state(
    req: &ReadShareGroupStateRequest,
    authz: &Authz,
) -> Result<ReadShareGroupStateResponse, HandlerError> {
    check_share_state_len(&req.topics, |t| t.partitions.len())?;
    let (code, why) = persister_outcome(authz);
    let results = req
        .topics
        .iter()
        .map(|t| ReadStateResult {
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| ReadPartitionResult {
                    partition: p.partition,
                    error_code: code.code(),
                    error_message: Some(why.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    Ok(ReadShareGroupStateResponse {
        results,
        ..Default::default()
    })
}

/// `85 WriteShareGroupState`: see [`NO_PERSISTER`].
pub fn write_share_group_state(
    req: &WriteShareGroupStateRequest,
    authz: &Authz,
) -> Result<WriteShareGroupStateResponse, HandlerError> {
    check_share_state_len(&req.topics, |t| t.partitions.len())?;
    let (code, why) = persister_outcome(authz);
    let results = req
        .topics
        .iter()
        .map(|t| WriteStateResult {
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| WritePartitionResult {
                    partition: p.partition,
                    error_code: code.code(),
                    error_message: Some(why.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    Ok(WriteShareGroupStateResponse {
        results,
        ..Default::default()
    })
}

/// `86 DeleteShareGroupState`: see [`NO_PERSISTER`].
pub fn delete_share_group_state(
    req: &DeleteShareGroupStateRequest,
    authz: &Authz,
) -> Result<DeleteShareGroupStateResponse, HandlerError> {
    check_share_state_len(&req.topics, |t| t.partitions.len())?;
    let (code, why) = persister_outcome(authz);
    let results = req
        .topics
        .iter()
        .map(|t| DeleteStateResult {
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| DeletePartitionResult {
                    partition: p.partition,
                    error_code: code.code(),
                    error_message: Some(why.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    Ok(DeleteShareGroupStateResponse {
        results,
        ..Default::default()
    })
}

/// `87 ReadShareGroupStateSummary`: see [`NO_PERSISTER`].
pub fn read_share_group_state_summary(
    req: &ReadShareGroupStateSummaryRequest,
    authz: &Authz,
) -> Result<ReadShareGroupStateSummaryResponse, HandlerError> {
    check_share_state_len(&req.topics, |t| t.partitions.len())?;
    let (code, why) = persister_outcome(authz);
    let results = req
        .topics
        .iter()
        .map(|t| ReadStateSummaryResult {
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| SummaryPartitionResult {
                    partition: p.partition,
                    error_code: code.code(),
                    error_message: Some(why.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    Ok(ReadShareGroupStateSummaryResponse {
        results,
        ..Default::default()
    })
}

/// `88 StreamsGroupHeartbeat`: the KIP-1071 streams rebalance protocol is not served;
/// `UNSUPPORTED_VERSION` is what Kafka returns without `streams` enabled.
pub fn streams_group_heartbeat(
    _req: &StreamsGroupHeartbeatRequest,
) -> Result<StreamsGroupHeartbeatResponse, HandlerError> {
    Ok(StreamsGroupHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::UnsupportedVersion.code(),
        error_message: Some(NO_STREAMS.to_string()),
        ..Default::default()
    })
}

/// `89 StreamsGroupDescribe`: see [`streams_group_heartbeat`]; the error is per group.
pub fn streams_group_describe(
    req: &StreamsGroupDescribeRequest,
) -> Result<StreamsGroupDescribeResponse, HandlerError> {
    check_admin_len("streams groups", req.group_ids.len())?;
    let groups = req
        .group_ids
        .iter()
        .map(|g| DescribedGroup {
            group_id: g.clone(),
            error_code: ErrorCode::UnsupportedVersion.code(),
            error_message: Some(NO_STREAMS.to_string()),
            ..Default::default()
        })
        .collect();
    Ok(StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups,
        ..Default::default()
    })
}
