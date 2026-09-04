use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::find_coordinator_request::FindCoordinatorRequest;
use kafgres_codec::generated::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
use kafgres_codec::generated::heartbeat_request::HeartbeatRequest;
use kafgres_codec::generated::heartbeat_response::HeartbeatResponse;
use kafgres_codec::generated::leave_group_request::LeaveGroupRequest;
use kafgres_codec::generated::leave_group_response::{LeaveGroupResponse, MemberResponse};

use super::{metadata::ClusterConfig, HandlerError};
use crate::group::{self, GroupState};

/// Both field sets are always filled: the encoder drops whichever the version does not carry.
const BATCHED_FROM: i16 = 4;

pub fn find_coordinator(
    req: &FindCoordinatorRequest,
    version: i16,
    cfg: &ClusterConfig,
    authz: &crate::acl::Authz,
) -> FindCoordinatorResponse {
    let keys: Vec<String> = if version >= BATCHED_FROM {
        req.coordinator_keys.clone()
    } else {
        vec![req.key.clone()]
    };

    // Below v4 the top-level fields are the only channel: there is no `coordinators` array.
    let legacy_denied = if version < BATCHED_FROM {
        keys.first()
            .and_then(|k| authz.check(crate::acl::Operation::Describe, crate::acl::ResourceType::Group, k).err())
    } else {
        None
    };

    FindCoordinatorResponse {
        throttle_time_ms: 0,
        error_code: legacy_denied
            .map(|c| c.code())
            .unwrap_or_else(|| ErrorCode::None.code()),
        error_message: None,
        node_id: if legacy_denied.is_some() { -1 } else { cfg.node_id },
        host: if legacy_denied.is_some() { String::new() } else { cfg.host.clone() },
        port: if legacy_denied.is_some() { -1 } else { cfg.port },
        coordinators: keys
            .into_iter()
            .map(|key| {
                let denied = authz.check(crate::acl::Operation::Describe, crate::acl::ResourceType::Group, &key).err();
                Coordinator {
                    key,
                    node_id: if denied.is_some() { -1 } else { cfg.node_id },
                    host: if denied.is_some() { String::new() } else { cfg.host.clone() },
                    port: if denied.is_some() { -1 } else { cfg.port },
                    error_code: denied.map(|c| c.code()).unwrap_or_else(|| ErrorCode::None.code()),
                    error_message: None,
                    unknown_tagged_fields: Vec::new(),
                }
            })
            .collect(),
        unknown_tagged_fields: Vec::new(),
    }
}

pub fn heartbeat(
    req: &HeartbeatRequest,
    authz: &crate::acl::Authz,
) -> Result<HeartbeatResponse, HandlerError> {
    if let Err(code) = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id) {
        return Ok(HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            ..Default::default()
        });
    }
    let code = heartbeat_code(req)?;
    Ok(HeartbeatResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        unknown_tagged_fields: Vec::new(),
    })
}

/// The heartbeat response is the rebalance signal: answering NONE mid-rebalance stalls the group silently.
fn heartbeat_code(req: &HeartbeatRequest) -> Result<ErrorCode, HandlerError> {
    let group = match group::load(&req.group_id)? {
        Some(g) => g,
        None => return Ok(ErrorCode::UnknownMemberId),
    };
    let member = group::member(&req.group_id, &req.member_id)?;
    if member.is_none() {
        return Ok(ErrorCode::UnknownMemberId);
    }

    // Order matters, and upstream's: member, then generation, then state. A
    if req.generation_id != group.generation {
        return Ok(ErrorCode::IllegalGeneration);
    }

    group::touch_heartbeat(&req.group_id, &req.member_id)?;

    match group.state {
        GroupState::PreparingRebalance | GroupState::CompletingRebalance => {
            Ok(ErrorCode::RebalanceInProgress)
        }
        GroupState::Empty => Ok(ErrorCode::UnknownMemberId),
        GroupState::Stable => Ok(ErrorCode::None),
    }
}

pub fn leave_group(
    req: &LeaveGroupRequest,
    version: i16,
    authz: &crate::acl::Authz,
) -> Result<LeaveGroupResponse, HandlerError> {
    if let Err(code) = authz.check(crate::acl::Operation::Read, crate::acl::ResourceType::Group, &req.group_id) {
        return Ok(LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            ..Default::default()
        });
    }

    let leaving: Vec<(String, Option<String>)> = if version >= 3 {
        req.members
            .iter()
            .map(|m| (m.member_id.clone(), m.group_instance_id.clone()))
            .collect()
    } else {
        vec![(req.member_id.clone(), None)]
    };

    let mut responses = Vec::with_capacity(leaving.len());
    for (member_id, instance_id) in leaving {
        let removed = group::leave(&req.group_id, &member_id)?;
        if removed {
            pgrx::log!(
                "kafgres: member '{}' left group '{}'",
                member_id,
                req.group_id
            );
        }
        responses.push(MemberResponse {
            member_id,
            group_instance_id: instance_id,
            error_code: if removed {
                ErrorCode::None.code()
            } else {
                ErrorCode::UnknownMemberId.code()
            },
            unknown_tagged_fields: Vec::new(),
        });
    }

    Ok(LeaveGroupResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        members: responses,
        unknown_tagged_fields: Vec::new(),
    })
}
