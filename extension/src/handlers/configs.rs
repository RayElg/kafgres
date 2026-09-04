use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::describe_configs_request::DescribeConfigsRequest;
use kafgres_codec::generated::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use kafgres_codec::generated::alter_configs_request::AlterConfigsRequest;
use kafgres_codec::generated::alter_configs_response::{
    AlterConfigsResourceResponse as LegacyAlterConfigsResourceResponse, AlterConfigsResponse,
};
use kafgres_codec::generated::incremental_alter_configs_request::IncrementalAlterConfigsRequest;
use kafgres_codec::generated::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};

use super::HandlerError;
use crate::config::{self, ConfigError};
use crate::meta;

pub const RESOURCE_TOPIC: i8 = 2;
pub const RESOURCE_BROKER: i8 = 4;

const SOURCE_TOPIC_CONFIG: i8 = 1;
const SOURCE_DEFAULT_CONFIG: i8 = 5;

pub fn describe_configs(
    req: &DescribeConfigsRequest,
    authz: &crate::acl::Authz,
) -> Result<DescribeConfigsResponse, HandlerError> {
    super::check_admin_len("describe configs resource list", req.resources.len())?;
    let mut results = Vec::with_capacity(req.resources.len());

    for resource in &req.resources {
        let denied = match resource.resource_type {
            RESOURCE_BROKER => authz
                .check(crate::acl::Operation::DescribeConfigs, crate::acl::ResourceType::Cluster, "kafka-cluster")
                .err(),
            _ => authz
                .check(crate::acl::Operation::DescribeConfigs, crate::acl::ResourceType::Topic, &resource.resource_name)
                .err(),
        };
        if let Some(code) = denied {
            results.push(DescribeConfigsResult {
                error_code: code.code(),
                error_message: None,
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                configs: Vec::new(),
                ..Default::default()
            });
            continue;
        }

        let (code, message, entries) = match resource.resource_type {
            RESOURCE_BROKER => (ErrorCode::None, None, config::describe_broker()),
            RESOURCE_TOPIC => match meta::topic_id_by_name(&resource.resource_name) {
                Err(e) => (
                    ErrorCode::UnknownServerError,
                    Some(format!("config lookup: {e}")),
                    Vec::new(),
                ),
                Ok(None) => (
                    ErrorCode::UnknownTopicOrPartition,
                    Some("unknown topic".to_string()),
                    Vec::new(),
                ),
                Ok(Some(id)) => match config::describe_topic(id) {
                    Ok(entries) => (ErrorCode::None, None, entries),
                    Err(e) => (
                        ErrorCode::UnknownServerError,
                        Some(format!("config read: {e}")),
                        Vec::new(),
                    ),
                },
            },
            other => (
                ErrorCode::InvalidRequest,
                Some(format!("resource type {other} is not configurable")),
                Vec::new(),
            ),
        };

        // Null or empty keys means "all"; treating empty as a filter reads as a topic with no configuration.
        let wanted = resource.configuration_keys.as_deref().unwrap_or(&[]);
        let configs = entries
            .into_iter()
            .filter(|e| wanted.is_empty() || wanted.contains(&e.name))
            .map(|e| DescribeConfigsResourceResult {
                name: e.name,
                value: e.value,
                read_only: e.read_only,
                config_source: if e.is_default {
                    SOURCE_DEFAULT_CONFIG
                } else {
                    SOURCE_TOPIC_CONFIG
                },
                is_sensitive: false,
                config_type: e.kind.wire(),
                ..Default::default()
            })
            .collect();

        results.push(DescribeConfigsResult {
            error_code: code.code(),
            error_message: message,
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
            configs,
            ..Default::default()
        });
    }

    Ok(DescribeConfigsResponse {
        results,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

pub fn incremental_alter_configs(
    req: &IncrementalAlterConfigsRequest,
    authz: &crate::acl::Authz,
) -> Result<IncrementalAlterConfigsResponse, HandlerError> {
    super::check_admin_len("alter configs resource list", req.resources.len())?;
    let mut responses = Vec::with_capacity(req.resources.len());

    for resource in &req.resources {
        // BROKER resources authorize against the cluster ACL, never the topic's.
        let denied = match resource.resource_type {
            RESOURCE_BROKER => authz
                .check(crate::acl::Operation::AlterConfigs, crate::acl::ResourceType::Cluster, "kafka-cluster")
                .err(),
            _ => authz
                .check(crate::acl::Operation::AlterConfigs, crate::acl::ResourceType::Topic, &resource.resource_name)
                .err(),
        };
        let outcome = match denied {
            Some(code) => Err(ConfigError::Denied(code)),
            None => apply_resource(resource, req.validate_only),
        };
        let (code, message) = match outcome {
            Ok(()) => (ErrorCode::None, None),
            Err(e) => {
                pgrx::log!(
                    "kafgres: alter configs on '{}' failed: {e}",
                    resource.resource_name
                );
                (e.error_code(), Some(e.to_string()))
            }
        };
        responses.push(AlterConfigsResourceResponse {
            error_code: code.code(),
            error_message: message,
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
            ..Default::default()
        });
    }

    Ok(IncrementalAlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

fn apply_resource(
    resource: &kafgres_codec::generated::incremental_alter_configs_request::AlterConfigsResource,
    validate_only: bool,
) -> Result<(), ConfigError> {
    if resource.resource_type != RESOURCE_TOPIC {
        // Broker configs are GUCs owned by `ALTER SYSTEM`; two sources of truth otherwise.
        return Err(ConfigError::UnsupportedResource(resource.resource_type));
    }

    let topic_id = meta::topic_id_by_name(&resource.resource_name)
        .map_err(|e| ConfigError::Internal(e.to_string()))?
        .ok_or_else(|| ConfigError::UnknownTopic(resource.resource_name.clone()))?;

    // Validate the whole resource first: one error code covers every entry.
    for entry in &resource.configs {
        let def = config::topic_def(&entry.name)
            .ok_or_else(|| ConfigError::Unknown(entry.name.clone()))?;
        config::check_alterable(def, entry.config_operation, entry.value.as_deref())?;
    }
    if validate_only {
        return Ok(());
    }

    for entry in &resource.configs {
        config::alter_topic_config(
            topic_id,
            &entry.name,
            entry.config_operation,
            entry.value.as_deref(),
        )?;
    }
    Ok(())
}

/// The pre-KIP-339 write: it **replaces** the desired state, so a config the request omits
pub fn alter_configs(
    req: &AlterConfigsRequest,
    authz: &crate::acl::Authz,
) -> Result<AlterConfigsResponse, HandlerError> {
    super::check_admin_len("alter configs resource list", req.resources.len())?;
    let mut responses = Vec::with_capacity(req.resources.len());

    for resource in &req.resources {
        let denied = match resource.resource_type {
            RESOURCE_BROKER => authz
                .check(crate::acl::Operation::AlterConfigs, crate::acl::ResourceType::Cluster, "kafka-cluster")
                .err(),
            _ => authz
                .check(crate::acl::Operation::AlterConfigs, crate::acl::ResourceType::Topic, &resource.resource_name)
                .err(),
        };
        let outcome = match denied {
            Some(code) => Err(ConfigError::Denied(code)),
            None => replace_resource(resource, req.validate_only),
        };
        let (code, message) = match outcome {
            Ok(()) => (ErrorCode::None, None),
            Err(e) => {
                pgrx::log!(
                    "kafgres: alter configs (replace) on '{}' failed: {e}",
                    resource.resource_name
                );
                (e.error_code(), Some(e.to_string()))
            }
        };
        responses.push(LegacyAlterConfigsResourceResponse {
            error_code: code.code(),
            error_message: message,
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
            ..Default::default()
        });
    }

    Ok(AlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

fn replace_resource(
    resource: &kafgres_codec::generated::alter_configs_request::AlterConfigsResource,
    validate_only: bool,
) -> Result<(), ConfigError> {
    if resource.resource_type != RESOURCE_TOPIC {
        return Err(ConfigError::UnsupportedResource(resource.resource_type));
    }

    let topic_id = meta::topic_id_by_name(&resource.resource_name)
        .map_err(|e| ConfigError::Internal(e.to_string()))?
        .ok_or_else(|| ConfigError::UnknownTopic(resource.resource_name.clone()))?;

    for entry in &resource.configs {
        let def = config::topic_def(&entry.name)
            .ok_or_else(|| ConfigError::Unknown(entry.name.clone()))?;
        config::check_alterable(def, config::OP_SET, entry.value.as_deref())?;
    }
    if validate_only {
        return Ok(());
    }

    let keep: Vec<String> = resource
        .configs
        .iter()
        .filter(|e| {
            // Storing a read-only entry would make DescribeConfigs report a default as explicitly set.
            config::topic_def(&e.name).map(|d| d.writable).unwrap_or(false)
        })
        .map(|e| e.name.clone())
        .collect();
    config::replace_topic_config(topic_id, &keep)?;

    for entry in &resource.configs {
        if !keep.contains(&entry.name) {
            continue;
        }
        config::alter_topic_config(topic_id, &entry.name, config::OP_SET, entry.value.as_deref())?;
    }
    Ok(())
}
