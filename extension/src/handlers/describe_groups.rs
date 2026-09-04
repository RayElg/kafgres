use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::describe_groups_request::DescribeGroupsRequest;
use kafgres_codec::generated::describe_groups_response::{
    DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
};
use kafgres_codec::generated::list_groups_request::ListGroupsRequest;
use kafgres_codec::generated::list_groups_response::{ListGroupsResponse, ListedGroup};
use pgrx::prelude::*;

use super::HandlerError;
use crate::group;

/// Kafka's "authorized operations not computed" sentinel; zero would claim the caller may do nothing.
const AUTHORIZED_OPERATIONS_UNSET: i32 = i32::MIN;

/// Spelled exactly as upstream renders it: `kafka-consumer-groups.sh --list` filters
const GROUP_TYPE_CLASSIC: &str = "Classic";

pub fn describe_groups(
    req: &DescribeGroupsRequest,
    authz: &crate::acl::Authz,
) -> Result<DescribeGroupsResponse, HandlerError> {
    let mut groups = Vec::with_capacity(req.groups.len());

    for group_id in &req.groups {
        if let Err(code) = authz.check(
            crate::acl::Operation::Describe,
            crate::acl::ResourceType::Group,
            group_id,
        ) {
            groups.push(DescribedGroup {
                error_code: code.code(),
                error_message: None,
                group_id: group_id.clone(),
                group_state: String::new(),
                protocol_type: String::new(),
                protocol_data: String::new(),
                members: Vec::new(),
                authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
                unknown_tagged_fields: Vec::new(),
            });
            continue;
        }
        let g = match group::load(group_id)? {
            Some(g) => g,
            None => {
                // An unknown group still gets an entry — `Dead` is how Kafka reports it.
                groups.push(DescribedGroup {
                    error_code: ErrorCode::None.code(),
                    error_message: None,
                    group_id: group_id.clone(),
                    group_state: "Dead".to_string(),
                    protocol_type: String::new(),
                    protocol_data: String::new(),
                    members: Vec::new(),
                    authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
                    unknown_tagged_fields: Vec::new(),
                });
                continue;
            }
        };

        groups.push(DescribedGroup {
            error_code: ErrorCode::None.code(),
            error_message: None,
            group_id: group_id.clone(),
            group_state: g.state.as_str().to_string(),
            protocol_type: g.protocol_type.clone().unwrap_or_default(),
            // The tool uses a non-empty value here to decide the group is live.
            protocol_data: g.protocol_name.clone().unwrap_or_default(),
            members: described_members(group_id)?,
            authorized_operations: AUTHORIZED_OPERATIONS_UNSET,
            unknown_tagged_fields: Vec::new(),
        });
    }

    Ok(DescribeGroupsResponse {
        throttle_time_ms: 0,
        groups,
        unknown_tagged_fields: Vec::new(),
    })
}

fn described_members(group_id: &str) -> Result<Vec<DescribedGroupMember>, HandlerError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT member_id, group_instance_id, client_id, client_host, metadata, assignment
               FROM kafgres_group_members WHERE group_id = $1 ORDER BY member_id",
            None,
            &[group_id.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(DescribedGroupMember {
                member_id: row.get::<String>(1)?.unwrap_or_default(),
                group_instance_id: row.get::<String>(2)?,
                client_id: row.get::<String>(3)?.unwrap_or_default(),
                client_host: row.get::<String>(4)?.unwrap_or_default(),
                member_metadata: kafgres_codec::bytes::Bytes::from(
                    row.get::<Vec<u8>>(5)?.unwrap_or_default(),
                ),
                member_assignment: kafgres_codec::bytes::Bytes::from(
                    row.get::<Vec<u8>>(6)?.unwrap_or_default(),
                ),
                unknown_tagged_fields: Vec::new(),
            });
        }
        Ok::<_, spi::Error>(out)
    })
    .map_err(HandlerError::from)
}

pub fn list_groups(
    req: &ListGroupsRequest,
    authz: &crate::acl::Authz,
) -> Result<ListGroupsResponse, HandlerError> {
    let cluster_wide = authz.allows(
        crate::acl::Operation::Describe,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    );
    let all = Spi::connect(|client| {
        let rows = client.select(
            // Both protocols (KIP-848 has its own table); 'consumer' names the application, not the protocol.
            "SELECT group_id, COALESCE(protocol_type, ''), state FROM kafgres_groups
             UNION ALL
             SELECT group_id, 'consumer', state FROM kafgres_consumer_groups
              ORDER BY group_id",
            None,
            &[],
        )?;
        let mut out: Vec<(String, String, String)> = Vec::new();
        for row in rows {
            out.push((
                row.get::<String>(1)?.unwrap_or_default(),
                row.get::<String>(2)?.unwrap_or_default(),
                row.get::<String>(3)?.unwrap_or_default(),
            ));
        }
        Ok::<_, spi::Error>(out)
    })?;

    let groups = all
        .into_iter()
        .filter(|(group_id, _, _)| {
            cluster_wide
                || authz.allows(
                    crate::acl::Operation::Describe,
                    crate::acl::ResourceType::Group,
                    group_id,
                )
        })
        .filter(|(_, _, state)| {
            req.states_filter.is_empty()
                || req.states_filter.iter().any(|s| s.eq_ignore_ascii_case(state))
        })
        .filter(|_| {
            req.types_filter.is_empty()
                || req
                    .types_filter
                    .iter()
                    .any(|t| t.eq_ignore_ascii_case(GROUP_TYPE_CLASSIC))
        })
        .map(|(group_id, protocol_type, group_state)| ListedGroup {
            group_id,
            protocol_type,
            group_state,
            group_type: GROUP_TYPE_CLASSIC.to_string(),
            unknown_tagged_fields: Vec::new(),
        })
        .collect();

    Ok(ListGroupsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        groups,
        unknown_tagged_fields: Vec::new(),
    })
}
