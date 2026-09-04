//! All three are CLUSTER-level, as Kafka requires: a principal that can write one ACL can write itself any.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::create_acls_request::CreateAclsRequest;
use kafgres_codec::generated::create_acls_response::{AclCreationResult, CreateAclsResponse};
use kafgres_codec::generated::delete_acls_request::DeleteAclsRequest;
use kafgres_codec::generated::delete_acls_response::{
    DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
};
use kafgres_codec::generated::describe_acls_request::DescribeAclsRequest;
use kafgres_codec::generated::describe_acls_response::{
    AclDescription, DescribeAclsResource, DescribeAclsResponse,
};
use pgrx::prelude::*;

use super::HandlerError;
use crate::acl;

struct Rule {
    principal: String,
    host: String,
    operation: String,
    permission: String,
    resource_type: String,
    resource_name: String,
    pattern_type: String,
}

/// Every field is independently "any" — enum `1` for typed fields, a null string for names; bound as parameters.
fn filter_clause(
    resource_type: i8,
    resource_name: Option<&str>,
    pattern_type: i8,
    principal: Option<&str>,
    host: Option<&str>,
    operation: i8,
    permission: i8,
) -> Result<(String, Vec<String>), ErrorCode> {
    let mut wheres: Vec<String> = Vec::new();
    let mut args: Vec<String> = Vec::new();

    let mut bind = |sql: &str, value: String, wheres: &mut Vec<String>, args: &mut Vec<String>| {
        args.push(value);
        wheres.push(sql.replace("$?", &format!("${}", args.len())));
    };

    if !acl::is_any(resource_type) {
        match acl::resource_type_from_wire(resource_type) {
            Some(t) => bind("resource_type = $?", t.as_str().to_string(), &mut wheres, &mut args),
            // Valid Kafka types with no APIs here: match nothing rather than error.
            None if resource_type != 0 => wheres.push("false".to_string()),
            None => return Err(ErrorCode::InvalidRequest),
        }
    }
    // MATCH builds its own name predicate below; an exact equality here as well would exclude it.
    if pattern_type != 2 {
        if let Some(name) = resource_name.filter(|n| !n.is_empty()) {
            bind("resource_name = $?", name.to_string(), &mut wheres, &mut args);
        }
    }
    if !acl::is_any(pattern_type) {
        if pattern_type == 2 {
            // MATCH: the LITERAL rule, `*`, and every PREFIXED rule whose name prefixes it.
            let name = resource_name.filter(|n| !n.is_empty()).unwrap_or("");
            args.push(name.to_string());
            let n = args.len();
            wheres.push(format!(
                "((pattern_type = 'LITERAL' AND (resource_name = ${n} OR resource_name = '*')) \
                  OR (pattern_type = 'PREFIXED' AND starts_with(${n}, resource_name)))"
            ));
        } else {
            match acl::pattern_type_from_wire(pattern_type) {
                Some(p) => bind("pattern_type = $?", p.to_string(), &mut wheres, &mut args),
                None if pattern_type != 0 => wheres.push("false".to_string()),
                None => return Err(ErrorCode::InvalidRequest),
            }
        }
    }
    if let Some(p) = principal.filter(|p| !p.is_empty()) {
        bind("principal = $?", p.to_string(), &mut wheres, &mut args);
    }
    if let Some(h) = host.filter(|h| !h.is_empty()) {
        bind("host = $?", h.to_string(), &mut wheres, &mut args);
    }
    if !acl::is_any(operation) {
        match acl::operation_from_wire(operation) {
            Some(o) => bind("operation = $?", o.to_string(), &mut wheres, &mut args),
            None if operation != 0 => wheres.push("false".to_string()),
            None => return Err(ErrorCode::InvalidRequest),
        }
    }
    if !acl::is_any(permission) {
        match acl::permission_from_wire(permission) {
            Some(p) => bind("permission = $?", p.to_string(), &mut wheres, &mut args),
            None if permission != 0 => wheres.push("false".to_string()),
            None => return Err(ErrorCode::InvalidRequest),
        }
    }

    let clause = if wheres.is_empty() {
        "true".to_string()
    } else {
        wheres.join(" AND ")
    };
    Ok((clause, args))
}

/// Input cap: the response is assembled before its size can be measured. One over the
const MAX_DESCRIBED_ACLS: usize = 10_000;

fn select_rules(clause: &str, args: &[String]) -> Result<Vec<Rule>, HandlerError> {
    let sql = format!(
        "SELECT principal, host, operation, permission, resource_type, resource_name,
                pattern_type
           FROM kafgres_acls WHERE {clause}
          ORDER BY resource_type, resource_name, principal, operation
          LIMIT {}",
        MAX_DESCRIBED_ACLS + 1
    );
    let bound: Vec<pgrx::datum::DatumWithOid> = args.iter().map(|a| a.as_str().into()).collect();
    Spi::connect(|client| {
        let rows = client.select(&sql, None, &bound)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(Rule {
                principal: r.get::<String>(1)?.unwrap_or_default(),
                host: r.get::<String>(2)?.unwrap_or_default(),
                operation: r.get::<String>(3)?.unwrap_or_default(),
                permission: r.get::<String>(4)?.unwrap_or_default(),
                resource_type: r.get::<String>(5)?.unwrap_or_default(),
                resource_name: r.get::<String>(6)?.unwrap_or_default(),
                pattern_type: r.get::<String>(7)?.unwrap_or_default(),
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(HandlerError::from)
}

pub fn describe_acls(
    req: &DescribeAclsRequest,
    authz: &crate::acl::Authz,
) -> Result<DescribeAclsResponse, HandlerError> {
    if let Err(code) = authz.check(
        acl::Operation::Describe,
        acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to describe ACLs".to_string()),
            ..Default::default()
        });
    }

    let (clause, args) = match filter_clause(
        req.resource_type_filter,
        req.resource_name_filter.as_deref(),
        req.pattern_type_filter,
        req.principal_filter.as_deref(),
        req.host_filter.as_deref(),
        req.operation,
        req.permission_type,
    ) {
        Ok(v) => v,
        Err(code) => {
            return Ok(DescribeAclsResponse {
                throttle_time_ms: 0,
                error_code: code.code(),
                error_message: Some("unsupported filter value".to_string()),
                ..Default::default()
            })
        }
    };

    let rules = select_rules(&clause, &args)?;
    if rules.len() > MAX_DESCRIBED_ACLS {
        return Err(HandlerError::TooLarge {
            what: "acl listing",
            n: rules.len(),
        });
    }

    // Indexed lookup, not a linear scan: this runs on the broker's single loop, where
    let mut index: std::collections::HashMap<(i8, String, i8), usize> =
        std::collections::HashMap::new();
    let mut resources: Vec<DescribeAclsResource> = Vec::new();
    for rule in rules {
        let key = (
            acl::resource_type_to_wire(&rule.resource_type),
            rule.resource_name.clone(),
            acl::pattern_type_to_wire(&rule.pattern_type),
        );
        let description = AclDescription {
            principal: rule.principal,
            host: rule.host,
            operation: acl::operation_to_wire(&rule.operation),
            permission_type: acl::permission_to_wire(&rule.permission),
            ..Default::default()
        };
        match index.get(&key) {
            Some(&at) => resources[at].acls.push(description),
            None => {
                index.insert(key.clone(), resources.len());
                resources.push(DescribeAclsResource {
                    resource_type: key.0,
                    resource_name: key.1,
                    pattern_type: key.2,
                    acls: vec![description],
                    ..Default::default()
                });
            }
        }
    }

    Ok(DescribeAclsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        resources,
        ..Default::default()
    })
}

pub fn create_acls(
    req: &CreateAclsRequest,
    authz: &crate::acl::Authz,
) -> Result<CreateAclsResponse, HandlerError> {
    super::check_admin_len("acl creation list", req.creations.len())?;
    let denied = authz
        .check(acl::Operation::Alter, acl::ResourceType::Cluster, "kafka-cluster")
        .err();

    let mut results = Vec::with_capacity(req.creations.len());
    for creation in &req.creations {
        if let Some(code) = denied {
            results.push(AclCreationResult {
                error_code: code.code(),
                error_message: Some("not authorized to alter ACLs".to_string()),
                ..Default::default()
            });
            continue;
        }

        let parsed = (|| {
            let rt = acl::resource_type_from_wire(creation.resource_type)?.as_str();
            let pt = acl::pattern_type_from_wire(creation.resource_pattern_type)?;
            let op = acl::operation_from_wire(creation.operation)?;
            let perm = acl::permission_from_wire(creation.permission_type)?;
            Some((rt, pt, op, perm))
        })();

        let malformed = if !creation.principal.starts_with("User:") {
            Some("principal must be typed, as 'User:name' — an untyped name never matches")
        } else if creation.resource_name.is_empty() {
            Some("resource name must not be empty")
        } else {
            None
        };
        if let Some(why) = malformed {
            results.push(AclCreationResult {
                error_code: ErrorCode::InvalidRequest.code(),
                error_message: Some(why.to_string()),
                ..Default::default()
            });
            continue;
        }

        let Some((rt, pt, op, perm)) = parsed else {
            results.push(AclCreationResult {
                error_code: ErrorCode::InvalidRequest.code(),
                error_message: Some(
                    "unsupported resource type, pattern, operation or permission".to_string(),
                ),
                ..Default::default()
            });
            continue;
        };

        // Creating a rule that already exists is success in Kafka, not a duplicate-key error.
        let stored = Spi::run_with_args(
            "INSERT INTO kafgres_acls
                 (principal, host, operation, permission, resource_type, resource_name,
                  pattern_type)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT DO NOTHING",
            &[
                creation.principal.as_str().into(),
                creation.host.as_str().into(),
                op.into(),
                perm.into(),
                rt.into(),
                creation.resource_name.as_str().into(),
                pt.into(),
            ],
        );
        results.push(match stored {
            Ok(()) => AclCreationResult::default(),
            Err(e) => {
                pgrx::log!("kafgres: create acl failed: {e}");
                AclCreationResult {
                    error_code: ErrorCode::UnknownServerError.code(),
                    error_message: Some("could not store the rule".to_string()),
                    ..Default::default()
                }
            }
        });
    }

    Ok(CreateAclsResponse {
        throttle_time_ms: 0,
        results,
        ..Default::default()
    })
}

pub fn delete_acls(
    req: &DeleteAclsRequest,
    authz: &crate::acl::Authz,
) -> Result<DeleteAclsResponse, HandlerError> {
    super::check_admin_len("acl filter list", req.filters.len())?;
    let denied = authz
        .check(acl::Operation::Alter, acl::ResourceType::Cluster, "kafka-cluster")
        .err();

    let mut filter_results = Vec::with_capacity(req.filters.len());
    for filter in &req.filters {
        if let Some(code) = denied {
            filter_results.push(DeleteAclsFilterResult {
                error_code: code.code(),
                error_message: Some("not authorized to alter ACLs".to_string()),
                ..Default::default()
            });
            continue;
        }

        let (clause, args) = match filter_clause(
            filter.resource_type_filter,
            filter.resource_name_filter.as_deref(),
            filter.pattern_type_filter,
            filter.principal_filter.as_deref(),
            filter.host_filter.as_deref(),
            filter.operation,
            filter.permission_type,
        ) {
            Ok(v) => v,
            Err(code) => {
                filter_results.push(DeleteAclsFilterResult {
                    error_code: code.code(),
                    error_message: Some("unsupported filter value".to_string()),
                    ..Default::default()
                });
                continue;
            }
        };

        // Read matches first: the response must name every rule the DELETE removes.
        let matched = select_rules(&clause, &args)?;
        let removed = Spi::run_with_args(
            &format!("DELETE FROM kafgres_acls WHERE {clause}"),
            &args
                .iter()
                .map(|a| a.as_str().into())
                .collect::<Vec<pgrx::datum::DatumWithOid>>(),
        );
        if let Err(e) = removed {
            pgrx::log!("kafgres: delete acls failed: {e}");
            filter_results.push(DeleteAclsFilterResult {
                error_code: ErrorCode::UnknownServerError.code(),
                error_message: Some("could not delete the rules".to_string()),
                ..Default::default()
            });
            continue;
        }

        filter_results.push(DeleteAclsFilterResult {
            error_code: ErrorCode::None.code(),
            error_message: None,
            matching_acls: matched
                .into_iter()
                .map(|rule| DeleteAclsMatchingAcl {
                    error_code: ErrorCode::None.code(),
                    error_message: None,
                    resource_type: acl::resource_type_to_wire(&rule.resource_type),
                    resource_name: rule.resource_name,
                    pattern_type: acl::pattern_type_to_wire(&rule.pattern_type),
                    principal: rule.principal,
                    host: rule.host,
                    operation: acl::operation_to_wire(&rule.operation),
                    permission_type: acl::permission_to_wire(&rule.permission),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
    }

    Ok(DeleteAclsResponse {
        throttle_time_ms: 0,
        filter_results,
        ..Default::default()
    })
}
