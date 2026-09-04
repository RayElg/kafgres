//! `21 DeleteRecords`, `35 DescribeLogDirs`, `42 DeleteGroups`, `46 ListPartitionReassignments`,

use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::delete_groups_request::DeleteGroupsRequest;
use kafgres_codec::generated::delete_groups_response::{
    DeletableGroupResult, DeleteGroupsResponse,
};
use kafgres_codec::generated::delete_records_request::DeleteRecordsRequest;
use kafgres_codec::generated::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use kafgres_codec::generated::describe_cluster_request::DescribeClusterRequest;
use kafgres_codec::generated::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;
use kafgres_codec::generated::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest;
use kafgres_codec::generated::alter_client_quotas_request::AlterClientQuotasRequest;
use kafgres_codec::generated::alter_client_quotas_response::{
    AlterClientQuotasResponse, EntityData, EntryData as AlterClientQuotasEntryResponse,
};
use kafgres_codec::generated::alter_user_scram_credentials_response::{
    AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
};
use kafgres_codec::generated::describe_user_scram_credentials_response::{
    CredentialInfo, DescribeUserScramCredentialsResponse, DescribeUserScramCredentialsResult,
};
use kafgres_codec::generated::elect_leaders_request::ElectLeadersRequest;
use kafgres_codec::generated::elect_leaders_response::{
    ElectLeadersResponse, PartitionResult, ReplicaElectionResult,
};
use kafgres_codec::generated::describe_client_quotas_request::DescribeClientQuotasRequest;
use kafgres_codec::generated::describe_client_quotas_response::{
    DescribeClientQuotasResponse, EntityData as DescribeClientQuotasEntityData,
    EntryData as DescribeClientQuotasEntryData, ValueData as DescribeClientQuotasValueData,
};
use kafgres_codec::generated::describe_log_dirs_request::DescribeLogDirsRequest;
use kafgres_codec::generated::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use kafgres_codec::generated::list_partition_reassignments_response::ListPartitionReassignmentsResponse;
use kafgres_codec::generated::describe_log_dirs_response::{
    DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
    DescribeLogDirsTopic,
};
use kafgres_codec::generated::describe_cluster_response::{
    DescribeClusterBroker, DescribeClusterResponse,
};

use super::HandlerError;
use crate::meta;
use crate::storage::LogStore;

use super::metadata::ClusterConfig;

const DELETE_TO_HIGH_WATERMARK: i64 = -1;

/// `21 DeleteRecords` — advance the log start offset, reclaiming whole segments. The
pub fn delete_records(
    req: &DeleteRecordsRequest,
    store: &mut dyn LogStore,
    authz: &crate::acl::Authz,
) -> Result<DeleteRecordsResponse, HandlerError> {
    // The two caps multiply; each partition also costs a watermark read plus a truncate.
    let total: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    super::check_admin_len("delete records partition list", total)?;
    super::check_admin_len("delete records topic list", req.topics.len())?;
    let mut topics = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        let topic_id = meta::topic_id_by_name(&topic.name)
            .map_err(|e| HandlerError::Internal(format!("delete records lookup: {e}")))?;

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for pd in &topic.partitions {
            let result = match authz.check(crate::acl::Operation::Delete, crate::acl::ResourceType::Topic, &topic.name) {
                Err(code) => Err(code),
                Ok(()) => match topic_id {
                    None => Err(ErrorCode::UnknownTopicOrPartition),
                    Some(tid) => truncate_one(store, tid, pd.partition_index, pd.offset),
                },
            };
            partitions.push(match result {
                Ok(low) => DeleteRecordsPartitionResult {
                    partition_index: pd.partition_index,
                    low_watermark: low,
                    error_code: ErrorCode::None.code(),
                    ..Default::default()
                },
                Err(code) => DeleteRecordsPartitionResult {
                    partition_index: pd.partition_index,
                    low_watermark: -1,
                    error_code: code.code(),
                    ..Default::default()
                },
            });
        }

        topics.push(DeleteRecordsTopicResult {
            name: topic.name.clone(),
            partitions,
            ..Default::default()
        });
    }

    Ok(DeleteRecordsResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

fn truncate_one(
    store: &mut dyn LogStore,
    topic: u32,
    partition: i32,
    requested: i64,
) -> Result<i64, ErrorCode> {
    let high = store
        .high_watermark(topic, partition)
        .map_err(|e| e.error_code())?;

    let target = if requested == DELETE_TO_HIGH_WATERMARK {
        high
    } else {
        requested
    };

    if target > high {
        return Err(ErrorCode::OffsetOutOfRange);
    }
    if target < 0 {
        return Err(ErrorCode::OffsetOutOfRange);
    }

    store
        .truncate_below(topic, partition, target)
        .map_err(|e| e.error_code())?;

    // Read back rather than echo `target`: `truncate_below` is free to keep a partially
    store.log_start_offset(topic, partition).map_err(|e| e.error_code())
}

/// `42 DeleteGroups` — remove a group and its committed offsets.
pub fn delete_groups(
    req: &DeleteGroupsRequest,
    authz: &crate::acl::Authz,
) -> Result<DeleteGroupsResponse, HandlerError> {
    super::check_admin_len("delete groups list", req.groups_names.len())?;
    let mut results = Vec::with_capacity(req.groups_names.len());

    for group in &req.groups_names {
        if let Err(code) = authz.check(crate::acl::Operation::Delete, crate::acl::ResourceType::Group, group) {
            results.push(DeletableGroupResult {
                group_id: group.clone(),
                error_code: code.code(),
                ..Default::default()
            });
            continue;
        }
        let code = match crate::group::delete(group) {
            Ok(outcome) => outcome.error_code(),
            Err(e) => {
                pgrx::log!("kafgres: delete group '{group}' failed: {e}");
                ErrorCode::UnknownServerError
            }
        };
        results.push(DeletableGroupResult {
            group_id: group.clone(),
            error_code: code.code(),
            ..Default::default()
        });
    }

    Ok(DeleteGroupsResponse {
        results,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

pub fn describe_cluster(
    _req: &DescribeClusterRequest,
    cfg: &ClusterConfig,
    authz: &crate::acl::Authz,
) -> Result<DescribeClusterResponse, HandlerError> {
    if let Err(code) = authz.check(crate::acl::Operation::Describe, crate::acl::ResourceType::Cluster, "kafka-cluster") {
        return Ok(DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to describe the cluster".to_string()),
            ..Default::default()
        });
    }
    Ok(DescribeClusterResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        cluster_id: cfg.cluster_id.clone(),
        controller_id: cfg.node_id,
        brokers: vec![DescribeClusterBroker {
            broker_id: cfg.node_id,
            host: cfg.host.clone(),
            port: cfg.port,
            rack: None,
            ..Default::default()
        }],
        // Every operation, because there are no ACLs. An empty set here is not "unknown",
        cluster_authorized_operations: -2147483648,
        ..Default::default()
    })
}

/// Cap on partitions per `DescribeLogDirs` answer, so the response stays inside
const MAX_DESCRIBED_PARTITIONS: usize = 100_000;

/// `35 DescribeLogDirs` — how much disk each partition is using, in one log dir: kafgres
pub fn describe_log_dirs(
    req: &DescribeLogDirsRequest,
    store: &dyn LogStore,
    authz: &crate::acl::Authz,
) -> Result<DescribeLogDirsResponse, HandlerError> {
    // Upstream requires DESCRIBE on the *cluster* for this, not on each topic: the answer
    if let Err(code) = authz.check(
        crate::acl::Operation::Describe,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        // Top-level `error_code` exists only at v3+, so at the v1/v2 we also advertise
        return Ok(DescribeLogDirsResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            results: Vec::new(),
            ..Default::default()
        });
    }

    // A named topic that does not exist is simply absent from the answer — there is no
    let mut topics: Vec<DescribeLogDirsTopic> = Vec::new();

    // Bounded before anything is read: the answer costs a storage call per partition,
    if let Some(list) = &req.topics {
        super::check_admin_len("describe log dirs topic list", list.len())?;
    }

    // `load_topics` is the same read Metadata uses, so visibility and sizing cannot disagree.
    let filter: Option<Vec<String>> = req
        .topics
        .as_ref()
        .map(|list| list.iter().map(|t| t.topic.clone()).collect());
    let loaded = meta::load_topics(filter.as_deref())?;

    // Hashed, not scanned: a linear find per loaded topic is quadratic in request size,
    let asked: Option<std::collections::HashMap<&str, std::collections::HashSet<i32>>> =
        req.topics.as_ref().map(|list| {
            list.iter()
                .map(|t| (t.topic.as_str(), t.partitions.iter().copied().collect()))
                .collect()
        });

    // The response is capped as it is built, not after: an unfiltered request against a
    let mut budget = MAX_DESCRIBED_PARTITIONS;

    for tm in loaded {
        let only = asked.as_ref().and_then(|a| a.get(tm.name.as_str()));
        let name = tm.name.clone();
        let topic_id = tm.topic_id;
        let mut partitions = Vec::new();
        for pm in &tm.partitions {
            let p = pm.partition;
            if let Some(only) = only {
                // An empty list means *no* partitions, not all of them — checked against
                if !only.contains(&p) {
                    continue;
                }
            }
            if budget == 0 {
                break;
            }
            budget -= 1;
            let size = store.partition_bytes(topic_id, p).unwrap_or(0);
            partitions.push(DescribeLogDirsPartition {
                partition_index: p,
                partition_size: size,
                // Lag is for a future replica (`AlterReplicaLogDirs`); there are none on
                offset_lag: 0,
                is_future_key: false,
                ..Default::default()
            });
        }
        // A topic with no matching partitions is omitted entirely, as the reference does.
        if partitions.is_empty() {
            continue;
        }
        topics.push(DescribeLogDirsTopic {
            name,
            partitions,
            ..Default::default()
        });
    }

    Ok(DescribeLogDirsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        results: vec![DescribeLogDirsResult {
            error_code: ErrorCode::None.code(),
            log_dir: store.log_dir(),
            topics,
            // `total_bytes`/`usable_bytes` are v4 filesystem fields; -1 is upstream's
            total_bytes: -1,
            usable_bytes: -1,
            is_cordoned: false,
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// `46 ListPartitionReassignments` — always empty: with a single broker no reassignment
pub fn list_partition_reassignments(
    _req: &ListPartitionReassignmentsRequest,
    authz: &crate::acl::Authz,
) -> Result<ListPartitionReassignmentsResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::Describe,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to describe the cluster".to_string()),
            topics: Vec::new(),
            ..Default::default()
        });
    }
    Ok(ListPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        topics: Vec::new(),
        ..Default::default()
    })
}

/// `48 DescribeClientQuotas` — reads back the quotas `AlterClientQuotas` stores in
pub fn describe_client_quotas(
    _req: &DescribeClientQuotasRequest,
    authz: &crate::acl::Authz,
) -> Result<DescribeClientQuotasResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::DescribeConfigs,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to describe client quotas".to_string()),
            entries: None,
            ..Default::default()
        });
    }
    let rows: Vec<(String, Option<String>, String, f64)> = Spi::connect(|client| {
        let got = client.select(
            "SELECT entity_type, entity_name, quota_type, quota_value
               FROM kafgres_client_quotas ORDER BY entity_type, entity_name, quota_type",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for r in got {
            out.push((
                r.get::<String>(1)?.unwrap_or_default(),
                r.get::<String>(2)?,
                r.get::<String>(3)?.unwrap_or_default(),
                r.get::<f64>(4)?.unwrap_or(0.0),
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // One entry per entity carrying all of its quotas — the shape `kafka-configs.sh
    let mut entries: Vec<DescribeClientQuotasEntryData> = Vec::new();
    for (etype, ename, key, value) in rows {
        let op = DescribeClientQuotasValueData { key, value, ..Default::default() };
        match entries.iter_mut().find(|e| {
            e.entity.len() == 1
                && e.entity[0].entity_type == etype
                && e.entity[0].entity_name == ename
        }) {
            Some(e) => e.values.push(op),
            None => entries.push(DescribeClientQuotasEntryData {
                entity: vec![DescribeClientQuotasEntityData {
                    entity_type: etype,
                    entity_name: ename,
                    ..Default::default()
                }],
                values: vec![op],
                ..Default::default()
            }),
        }
    }

    Ok(DescribeClientQuotasResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        // `Some(...)`, not `None`: null means "the request could not be answered", an
        entries: Some(entries),
        ..Default::default()
    })
}

/// `43 ElectLeaders` — matches Kafka on single-replica partitions: an explicit partition
pub fn elect_leaders(
    req: &ElectLeadersRequest,
    authz: &crate::acl::Authz,
) -> Result<ElectLeadersResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::Alter,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(ElectLeadersResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            replica_election_results: Vec::new(),
            ..Default::default()
        });
    }

    // Null means "every partition": Kafka answers only with the partitions that *needed*
    let Some(topics) = req.topic_partitions.as_ref() else {
        return Ok(ElectLeadersResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.code(),
            replica_election_results: Vec::new(),
            ..Default::default()
        });
    };

    // `partitions` is `[]int32`, a varint per entry at the flexible versions, so an 8 MiB
    super::check_admin_len("elected partitions", topics.len())?;
    let total: usize = topics.iter().map(|t| t.partitions.len()).sum();
    super::check_admin_len("elected partitions", total)?;

    let mut results = Vec::with_capacity(topics.len());
    for t in topics {
        // The partition count, not just topic existence: upstream answers
        let count = match meta::topic_id_by_name(&t.topic)
            .map_err(|e| HandlerError::Internal(e.to_string()))?
        {
            Some(id) => meta::partition_count(id)
                .map_err(|e| HandlerError::Internal(e.to_string()))?,
            None => 0,
        };
        let partitions = t
            .partitions
            .clone()
            .into_iter()
            .map(|p| PartitionResult {
                partition_id: p,
                error_code: if p >= 0 && p < count {
                    ErrorCode::ElectionNotNeeded.code()
                } else {
                    ErrorCode::UnknownTopicOrPartition.code()
                },
                error_message: None,
                ..Default::default()
            })
            .collect();
        results.push(ReplicaElectionResult {
            topic: t.topic.clone(),
            partition_result: partitions,
            ..Default::default()
        });
    }

    Ok(ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        replica_election_results: results,
        ..Default::default()
    })
}

/// `50 DescribeUserScramCredentials` — SASL identities are Postgres roles, so the answer
pub fn describe_user_scram_credentials(
    req: &DescribeUserScramCredentialsRequest,
    authz: &crate::acl::Authz,
) -> Result<DescribeUserScramCredentialsResponse, HandlerError> {
    if let Err(code) = authz.check(
        crate::acl::Operation::Describe,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(DescribeUserScramCredentialsResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            error_message: Some("not authorized to describe the cluster".to_string()),
            results: Vec::new(),
            ..Default::default()
        });
    }

    // A `UserName` is three bytes on the wire at the flexible versions, so an 8 MiB
    super::check_admin_len(
        "described users",
        req.users.as_ref().map(|u| u.len()).unwrap_or(0),
    )?;
    let wanted: Vec<String> = req
        .users
        .as_ref()
        .map(|u| u.iter().map(|n| n.name.clone()).collect())
        .unwrap_or_default();

    let found = crate::sasl::scram_users(&wanted).map_err(|e| HandlerError::Internal(e))?;

    let results = if wanted.is_empty() {
        found
            .into_iter()
            .map(|(user, iterations)| scram_result(user, Some(iterations)))
            .collect()
    } else {
        wanted
            .into_iter()
            .map(|user| {
                let it = found.iter().find(|(n, _)| *n == user).map(|(_, i)| *i);
                scram_result(user, it)
            })
            .collect()
    };

    Ok(DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        results,
        ..Default::default()
    })
}

/// SCRAM mechanism 1 — `SCRAM-SHA-256`. Not 2 (`SCRAM-SHA-512`): Postgres stores no
const SCRAM_SHA_256: i8 = 1;

fn scram_result(user: String, iterations: Option<i32>) -> DescribeUserScramCredentialsResult {
    match iterations {
        Some(iterations) => DescribeUserScramCredentialsResult {
            user,
            error_code: ErrorCode::None.code(),
            error_message: None,
            credential_infos: vec![CredentialInfo {
                mechanism: SCRAM_SHA_256,
                iterations,
                ..Default::default()
            }],
            ..Default::default()
        },
        None => DescribeUserScramCredentialsResult {
            user,
            error_code: ErrorCode::ResourceNotFound.code(),
            error_message: Some("no SCRAM-SHA-256 credential for this role".to_string()),
            credential_infos: Vec::new(),
            ..Default::default()
        },
    }
}

/// `51 AlterUserScramCredentials` — sets or clears a Postgres role's password from a SCRAM
pub fn alter_user_scram_credentials(
    req: &AlterUserScramCredentialsRequest,
    authz: &crate::acl::Authz,
) -> Result<AlterUserScramCredentialsResponse, HandlerError> {
    super::check_admin_len("scram deletions", req.deletions.len())?;
    super::check_admin_len("scram upsertions", req.upsertions.len())?;

    if let Err(code) = authz.check(
        crate::acl::Operation::Alter,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        let mut results: Vec<AlterUserScramCredentialsResult> = Vec::new();
        for name in req
            .deletions
            .iter()
            .map(|d| &d.name)
            .chain(req.upsertions.iter().map(|u| &u.name))
        {
            results.push(AlterUserScramCredentialsResult {
                user: name.clone(),
                error_code: code.code(),
                error_message: Some("not authorized to alter credentials".to_string()),
                ..Default::default()
            });
        }
        return Ok(AlterUserScramCredentialsResponse { results, ..Default::default() });
    }

    /// Kafka's `ScramMechanism`: 1 is SHA-256, 2 is SHA-512.
    const SCRAM_SHA_256: i8 = 1;

    let mut results = Vec::with_capacity(req.deletions.len() + req.upsertions.len());
    for d in &req.deletions {
        let (code, msg) = if d.mechanism != SCRAM_SHA_256 {
            (ErrorCode::UnsupportedSaslMechanism, Some("only SCRAM-SHA-256".to_string()))
        } else {
            match set_role_password(&d.name, None) {
                Ok(()) => (ErrorCode::None, None),
                Err(e) => (credential_error(&e), Some(e)),
            }
        };
        results.push(AlterUserScramCredentialsResult {
            user: d.name.clone(),
            error_code: code.code(),
            error_message: msg,
            ..Default::default()
        });
    }
    for u in &req.upsertions {
        let (code, msg) = if u.mechanism != SCRAM_SHA_256 {
            (
                ErrorCode::UnsupportedSaslMechanism,
                Some("this broker stores credentials as Postgres roles, which have only \
                      SCRAM-SHA-256 verifiers".to_string()),
            )
        } else if u.iterations < 4096 {
            // Kafka's own floor, and Postgres's default is 4096 too. Below it the
            (
                ErrorCode::UnacceptableCredential,
                Some(format!("{} iterations is below the 4096 minimum", u.iterations)),
            )
        } else {
            let verifier = crate::sasl::postgres_verifier(u.iterations, &u.salt, &u.salted_password);
            match set_role_password(&u.name, Some(&verifier)) {
                Ok(()) => (ErrorCode::None, None),
                Err(e) => (credential_error(&e), Some(e)),
            }
        };
        results.push(AlterUserScramCredentialsResult {
            user: u.name.clone(),
            error_code: code.code(),
            error_message: msg,
            ..Default::default()
        });
    }
    Ok(AlterUserScramCredentialsResponse { results, ..Default::default() })
}

/// Which error code a `set_role_password` failure deserves: `RESOURCE_NOT_FOUND` for a
fn credential_error(message: &str) -> ErrorCode {
    if message.starts_with(NO_SUCH_ROLE) || message == NO_SUCH_ROLE {
        ErrorCode::ResourceNotFound
    } else if message.contains("timeout") || message.contains("deadlock") {
        // Retriable, and the only honest answer: the credential was fine and the broker
        ErrorCode::UnknownServerError
    } else {
        ErrorCode::UnacceptableCredential
    }
}

/// Marker for the `set_role_password` failure that is a missing resource, not a bad credential.
const NO_SUCH_ROLE: &str = "no Postgres role by that name; credentials are roles here, so \
                            create it with CREATE ROLE first";

/// `ALTER ROLE <name> PASSWORD …` for a role that must already exist — creating one from a
fn set_role_password(name: &str, verifier: Option<&str>) -> Result<(), String> {
    // Privileged roles are not Kafka users: this runs `ALTER ROLE` as superuser, so
    let role: Option<(bool, bool)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication
                     OR rolbypassrls) AS privileged,
                    rolname = current_user AS is_self
               FROM pg_roles WHERE rolname = $1",
            Some(1),
            &[name.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some((
                r.get::<bool>(1)?.unwrap_or(true),
                r.get::<bool>(2)?.unwrap_or(true),
            )));
        }
        Ok(None)
    })
    .map_err(|e| e.to_string())?;
    let Some((privileged, is_self)) = role else {
        return Err(NO_SUCH_ROLE.to_string());
    };
    if privileged || is_self {
        return Err(format!(
            "{name:?} is a privileged Postgres role; credentials are roles here, and this \
             API will not change the password of one that can administer the database"
        ));
    }
    if let Some(v) = verifier {
        if !v.starts_with("SCRAM-SHA-256$") || v.contains('\'') || v.contains('\\') {
            return Err("refusing a malformed verifier".to_string());
        }
    }
    let quoted_name: String = Spi::get_one_with_args("SELECT quote_ident($1)", &[name.into()])
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "could not quote the role name".to_string())?;
    let sql = match verifier {
        Some(v) => format!("ALTER ROLE {quoted_name} PASSWORD '{v}'"),
        None => format!("ALTER ROLE {quoted_name} PASSWORD NULL"),
    };
    Spi::run(&sql).map_err(|e| e.to_string())
}

const ENFORCED_QUOTAS: [&str; 2] = ["producer_byte_rate", "consumer_byte_rate"];

/// `49 AlterClientQuotas` — stores quotas in `kafgres_client_quotas`, which `quota.rs`
pub fn alter_client_quotas(
    req: &AlterClientQuotasRequest,
    authz: &crate::acl::Authz,
) -> Result<AlterClientQuotasResponse, HandlerError> {
    super::check_admin_len("quota entries", req.entries.len())?;

    let denied = authz
        .check(
            crate::acl::Operation::AlterConfigs,
            crate::acl::ResourceType::Cluster,
            "kafka-cluster",
        )
        .err();

    let mut entries = Vec::with_capacity(req.entries.len());
    for e in &req.entries {
        let entity: Vec<EntityData> = e
            .entity
            .iter()
            .map(|d| EntityData {
                entity_type: d.entity_type.clone(),
                entity_name: d.entity_name.clone(),
                ..Default::default()
            })
            .collect();
        let (code, msg) = match denied {
            Some(c) => (c, Some("not authorized to alter client quotas".to_string())),
            None => match apply_quota_entry(e, req.validate_only) {
                Ok(()) => (ErrorCode::None, None),
                Err((c, m)) => (c, Some(m)),
            },
        };
        entries.push(AlterClientQuotasEntryResponse {
            error_code: code.code(),
            error_message: msg,
            entity,
            ..Default::default()
        });
    }
    Ok(AlterClientQuotasResponse { throttle_time_ms: 0, entries, ..Default::default() })
}

fn apply_quota_entry(
    e: &kafgres_codec::generated::alter_client_quotas_request::EntryData,
    validate_only: bool,
) -> Result<(), (ErrorCode, String)> {
    // Kafka sends the entity as a list so a quota can name a user *and* a client id; this
    if e.entity.len() != 1 {
        return Err((
            ErrorCode::InvalidRequest,
            "this broker applies quotas to one entity at a time; a combined \
             user+client-id quota is not supported"
                .to_string(),
        ));
    }
    let d = &e.entity[0];
    if d.entity_type != "user" && d.entity_type != "client-id" {
        return Err((
            ErrorCode::InvalidRequest,
            format!("unsupported quota entity type {:?}", d.entity_type),
        ));
    }
    for op in &e.ops {
        if !ENFORCED_QUOTAS.contains(&op.key.as_str()) {
            return Err((
                ErrorCode::InvalidConfig,
                format!(
                    "{:?} is not enforced by this broker, so it is refused rather than \
                     stored; it applies {}",
                    op.key,
                    ENFORCED_QUOTAS.join(" and ")
                ),
            ));
        }
        if !op.remove && !(op.value.is_finite() && op.value > 0.0) {
            return Err((
                ErrorCode::InvalidConfig,
                format!("{:?} must be a positive number of bytes per second", op.key),
            ));
        }
    }
    if validate_only {
        return Ok(());
    }
    for op in &e.ops {
        let sql = if op.remove {
            "DELETE FROM kafgres_client_quotas
              WHERE entity_type = $1 AND quota_type = $3
                AND entity_name IS NOT DISTINCT FROM $2"
        } else {
            "INSERT INTO kafgres_client_quotas (entity_type, entity_name, quota_type, quota_value)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT DO NOTHING"
        };
        Spi::run_with_args(
            sql,
            &[
                d.entity_type.as_str().into(),
                d.entity_name.clone().into(),
                op.key.as_str().into(),
                op.value.into(),
            ],
        )
        .map_err(|e| (ErrorCode::UnknownServerError, e.to_string()))?;
        if !op.remove {
            // `ON CONFLICT DO NOTHING` above plus this, rather than `DO UPDATE`: uniqueness
            Spi::run_with_args(
                "UPDATE kafgres_client_quotas SET quota_value = $4, updated_at = now()
                  WHERE entity_type = $1 AND quota_type = $3
                    AND entity_name IS NOT DISTINCT FROM $2",
                &[
                    d.entity_type.as_str().into(),
                    d.entity_name.clone().into(),
                    op.key.as_str().into(),
                    op.value.into(),
                ],
            )
            .map_err(|e| (ErrorCode::UnknownServerError, e.to_string()))?;
        }
    }
    Ok(())
}
