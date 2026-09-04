//! Both park: a join cannot be answered until every member has rejoined, a sync until the leader replies.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::join_group_request::JoinGroupRequest;
use kafgres_codec::generated::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use kafgres_codec::generated::sync_group_request::SyncGroupRequest;
use kafgres_codec::generated::sync_group_response::SyncGroupResponse;

use super::HandlerError;
use crate::group::{self, GroupState};

/// From v4 a client with no member id is told one and asked to retry (KIP-394).
const MEMBER_ID_REQUIRED_FROM: i16 = 4;

pub enum JoinOutcome {
    Reply(Box<JoinGroupResponse>),
    Park { member_id: String },
}

pub fn join_group(
    req: &JoinGroupRequest,
    version: i16,
    client_id: &str,
    client_host: &str,
    authz: &crate::acl::Authz,
) -> Result<JoinOutcome, HandlerError> {
    if let Err(code) = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id) {
        return Ok(JoinOutcome::Reply(Box::new(error_join(code, req.member_id.clone()))));
    }
    if req.group_id.is_empty() {
        return Ok(JoinOutcome::Reply(Box::new(error_join(
            ErrorCode::InvalidGroupId,
            String::new(),
        ))));
    }

    if req.member_id.is_empty() {
        let member_id = group::new_member_id(client_id)?;
        if version >= MEMBER_ID_REQUIRED_FROM {
            let mut resp = error_join(ErrorCode::MemberIdRequired, member_id);
            resp.protocol_type = Some(req.protocol_type.clone());
            return Ok(JoinOutcome::Reply(Box::new(resp)));
        }
        return register_and_park(req, &member_id, client_id, client_host);
    }

    // An unknown member id registers rather than erroring: MEMBER_ID_REQUIRED above issues
    register_and_park(req, &req.member_id, client_id, client_host)
}

fn register_and_park(
    req: &JoinGroupRequest,
    member_id: &str,
    client_id: &str,
    client_host: &str,
) -> Result<JoinOutcome, HandlerError> {
    let protocols: Vec<String> = req.protocols.iter().map(|p| p.name.clone()).collect();
    if protocols.is_empty() {
        return Ok(JoinOutcome::Reply(Box::new(error_join(
            ErrorCode::InconsistentGroupProtocol,
            member_id.to_string(),
        ))));
    }
    if group::protocol_type_conflicts(&req.group_id, &req.protocol_type)? {
        return Ok(JoinOutcome::Reply(Box::new(error_join(
            ErrorCode::InconsistentGroupProtocol,
            member_id.to_string(),
        ))));
    }

    let metadata = req.protocols[0].metadata.to_vec();

    group::join(
        &req.group_id,
        member_id,
        req.group_instance_id.as_deref(),
        client_id,
        client_host,
        req.session_timeout_ms,
        req.rebalance_timeout_ms,
        &req.protocol_type,
        &protocols,
        &metadata,
    )?;

    Ok(JoinOutcome::Park {
        member_id: member_id.to_string(),
    })
}

pub fn join_response(group_id: &str, member_id: &str) -> Result<JoinGroupResponse, HandlerError> {
    let group = match group::load(group_id)? {
        Some(g) => g,
        None => return Ok(error_join(ErrorCode::UnknownMemberId, member_id.to_string())),
    };
    let members = group::members(group_id)?;
    if !members.iter().any(|m| m.member_id == member_id) {
        return Ok(error_join(ErrorCode::UnknownMemberId, member_id.to_string()));
    }

    let is_leader = group.leader_member.as_deref() == Some(member_id);
    Ok(JoinGroupResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        generation_id: group.generation,
        // Never null: a `None` fails to encode at the versions real clients negotiate.
        protocol_type: Some(group.protocol_type.clone().unwrap_or_default()),
        protocol_name: Some(group.protocol_name.clone().unwrap_or_default()),
        leader: group.leader_member.clone().unwrap_or_default(),
        skip_assignment: false,
        member_id: member_id.to_string(),
        members: if is_leader {
            members
                .into_iter()
                .map(|m| JoinGroupResponseMember {
                    member_id: m.member_id,
                    group_instance_id: m.group_instance_id,
                    metadata: kafgres_codec::bytes::Bytes::from(m.metadata),
                    unknown_tagged_fields: Vec::new(),
                })
                .collect()
        } else {
            Vec::new()
        },
        unknown_tagged_fields: Vec::new(),
    })
}

pub fn error_join(code: ErrorCode, member_id: String) -> JoinGroupResponse {
    JoinGroupResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        generation_id: -1,
        protocol_type: Some(String::new()),
        protocol_name: Some(String::new()),
        leader: String::new(),
        skip_assignment: false,
        member_id,
        members: Vec::new(),
        unknown_tagged_fields: Vec::new(),
    }
}

pub enum SyncOutcome {
    Reply(Box<SyncGroupResponse>),
    Park,
}

pub fn sync_group(
    req: &SyncGroupRequest,
    authz: &crate::acl::Authz,
) -> Result<SyncOutcome, HandlerError> {
    if let Err(code) = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id) {
        return Ok(SyncOutcome::Reply(Box::new(error_sync(code))));
    }
    let group = match group::load(&req.group_id)? {
        Some(g) => g,
        None => return Ok(SyncOutcome::Reply(Box::new(error_sync(ErrorCode::UnknownMemberId)))),
    };
    if group::member(&req.group_id, &req.member_id)?.is_none() {
        return Ok(SyncOutcome::Reply(Box::new(error_sync(ErrorCode::UnknownMemberId))));
    }
    if req.generation_id != group.generation {
        return Ok(SyncOutcome::Reply(Box::new(error_sync(
            ErrorCode::IllegalGeneration,
        ))));
    }
    // A rebalance reopened underneath this member — it must rejoin, not sync.
    if group.state == GroupState::PreparingRebalance {
        return Ok(SyncOutcome::Reply(Box::new(error_sync(
            ErrorCode::RebalanceInProgress,
        ))));
    }

    let is_leader = group.leader_member.as_deref() == Some(req.member_id.as_str());
    if is_leader && !req.assignments.is_empty() {
        let assignments: Vec<(String, Vec<u8>)> = req
            .assignments
            .iter()
            .map(|a| (a.member_id.clone(), a.assignment.to_vec()))
            .collect();
        group::apply_assignments(&req.group_id, &assignments)?;
        pgrx::log!(
            "kafgres: group '{}' generation {} assigned by leader across {} member(s)",
            req.group_id,
            group.generation,
            assignments.len()
        );
    }

    match sync_response(&req.group_id, &req.member_id)? {
        Some(resp) => Ok(SyncOutcome::Reply(Box::new(resp))),
        None => Ok(SyncOutcome::Park),
    }
}

/// `None` while the leader has not yet delivered assignments.
pub fn sync_response(
    group_id: &str,
    member_id: &str,
) -> Result<Option<SyncGroupResponse>, HandlerError> {
    let group = match group::load(group_id)? {
        Some(g) => g,
        None => return Ok(Some(error_sync(ErrorCode::UnknownMemberId))),
    };
    let member = match group::member(group_id, member_id)? {
        Some(m) => m,
        None => return Ok(Some(error_sync(ErrorCode::UnknownMemberId))),
    };
    if group.state == GroupState::PreparingRebalance {
        return Ok(Some(error_sync(ErrorCode::RebalanceInProgress)));
    }
    match member.assignment {
        None => Ok(None),
        Some(bytes) => Ok(Some(SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.code(),
            protocol_type: Some(group.protocol_type.unwrap_or_default()),
            protocol_name: Some(group.protocol_name.unwrap_or_default()),
            assignment: kafgres_codec::bytes::Bytes::from(bytes),
            unknown_tagged_fields: Vec::new(),
        })),
    }
}

pub fn error_sync(code: ErrorCode) -> SyncGroupResponse {
    SyncGroupResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        protocol_type: Some(String::new()),
        protocol_name: Some(String::new()),
        assignment: kafgres_codec::bytes::Bytes::new(),
        unknown_tagged_fields: Vec::new(),
    }
}
