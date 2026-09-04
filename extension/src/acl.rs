//! Kafka ACL authorization: a decision is `(principal, host, operation, resource)` checked

use std::time::{Duration, Instant};

use pgrx::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

impl ResourceType {
    pub fn as_str(self) -> &'static str {
        match self {
            ResourceType::Topic => "TOPIC",
            ResourceType::Group => "GROUP",
            ResourceType::Cluster => "CLUSTER",
            ResourceType::TransactionalId => "TRANSACTIONAL_ID",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "TOPIC" => Some(ResourceType::Topic),
            "GROUP" => Some(ResourceType::Group),
            "CLUSTER" => Some(ResourceType::Cluster),
            "TRANSACTIONAL_ID" => Some(ResourceType::TransactionalId),
            _ => None,
        }
    }

    pub fn denied_code(self) -> kafgres_codec::ErrorCode {
        use kafgres_codec::ErrorCode as E;
        match self {
            ResourceType::Topic => E::TopicAuthorizationFailed,
            ResourceType::Group => E::GroupAuthorizationFailed,
            ResourceType::Cluster => E::ClusterAuthorizationFailed,
            ResourceType::TransactionalId => E::TransactionalIdAuthorizationFailed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    /// Deliberately not implied by `ALTER`: CLUSTER_ACTION can force-commit another producer's transaction.
    ClusterAction,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Read => "READ",
            Operation::Write => "WRITE",
            Operation::Create => "CREATE",
            Operation::Delete => "DELETE",
            Operation::Alter => "ALTER",
            Operation::Describe => "DESCRIBE",
            Operation::DescribeConfigs => "DESCRIBE_CONFIGS",
            Operation::AlterConfigs => "ALTER_CONFIGS",
            Operation::IdempotentWrite => "IDEMPOTENT_WRITE",
            Operation::ClusterAction => "CLUSTER_ACTION",
        }
    }

    fn implied_by(self, granted: &str) -> bool {
        if granted == "ALL" || granted == self.as_str() {
            return true;
        }
        match self {
            Operation::Describe => matches!(
                granted,
                "READ" | "WRITE" | "DELETE" | "ALTER"
            ),
            Operation::DescribeConfigs => granted == "ALTER_CONFIGS",
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternType {
    Literal,
    Prefixed,
}

#[derive(Debug, Clone)]
pub struct Acl {
    principal: String,
    host: String,
    operation: String,
    permission_allow: bool,
    resource_type: ResourceType,
    resource_name: String,
    pattern: PatternType,
}

impl Acl {
    fn matches(
        &self,
        principal: &str,
        host: &str,
        op: Operation,
        rt: ResourceType,
        name: &str,
    ) -> bool {
        if self.resource_type != rt {
            return false;
        }
        if self.principal != "User:*" && self.principal != principal {
            return false;
        }
        if self.host != "*" && self.host != host {
            return false;
        }
        if !op.implied_by(&self.operation) {
            return false;
        }
        match self.pattern {
            // `*` is Kafka's wildcard resource, spelled as a literal name.
            PatternType::Literal => self.resource_name == "*" || self.resource_name == name,
            PatternType::Prefixed => name.starts_with(&self.resource_name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    pub host: String,
}

impl Principal {
    pub fn user(role: &str, host: &str) -> Self {
        Principal {
            name: format!("User:{role}"),
            host: host.to_string(),
        }
    }

    pub fn certificate(dn: &str, host: &str) -> Self {
        Principal {
            name: format!("User:{dn}"),
            host: host.to_string(),
        }
    }

    pub fn anonymous(host: &str) -> Self {
        Principal {
            name: "User:ANONYMOUS".to_string(),
            host: host.to_string(),
        }
    }
}

pub struct AclCache {
    acls: Vec<Acl>,
    loaded: Option<Instant>,
    enabled: bool,
    superusers: Vec<String>,
}

const MAX_STALENESS: Duration = Duration::from_secs(1);

impl Default for AclCache {
    fn default() -> Self {
        AclCache {
            acls: Vec::new(),
            loaded: None,
            enabled: false,
            superusers: Vec::new(),
        }
    }
}

impl AclCache {
    pub fn is_stale(&self) -> bool {
        match self.loaded {
            None => true,
            Some(at) => at.elapsed() > MAX_STALENESS,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Mark even failed loads: the stale snapshot must not keep its old timestamp, or is_stale() retries every tick.
    pub fn mark_attempted(&mut self) {
        self.loaded = Some(Instant::now());
    }

    /// Returns a new cache rather than mutating: `BackgroundWorker::transaction` needs an unwind-safe closure.
    pub fn load(enabled: bool, superusers: &str) -> Result<AclCache, spi::Error> {
        let mut out = AclCache::default();
        out.enabled = enabled;
        out.superusers = superusers
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        out.loaded = Some(Instant::now());
        if !enabled {
            return Ok(out);
        }

        let mut acls = Vec::new();
        Spi::connect(|client| {
            let rows = client.select(
                "SELECT principal, host, operation, permission, resource_type,
                        resource_name, pattern_type
                   FROM kafgres_acls",
                None,
                &[],
            )?;
            for row in rows {
                let resource_type = match row.get::<String>(5)?.as_deref().and_then(ResourceType::parse) {
                    Some(rt) => rt,
                    None => continue,
                };
                acls.push(Acl {
                    principal: row.get::<String>(1)?.unwrap_or_default(),
                    host: row.get::<String>(2)?.unwrap_or_else(|| "*".to_string()),
                    operation: row.get::<String>(3)?.unwrap_or_default(),
                    permission_allow: row
                        .get::<String>(4)?
                        .map(|p| p == "ALLOW")
                        .unwrap_or(false),
                    resource_type,
                    resource_name: row.get::<String>(6)?.unwrap_or_default(),
                    pattern: match row.get::<String>(7)?.as_deref() {
                        Some("PREFIXED") => PatternType::Prefixed,
                        _ => PatternType::Literal,
                    },
                });
            }
            Ok::<_, spi::Error>(())
        })?;
        out.acls = acls;
        Ok(out)
    }

    /// Kafka's evaluation order: superuser, then DENY, then ALLOW, then refuse.
    pub fn allows(
        &self,
        who: &Principal,
        op: Operation,
        rt: ResourceType,
        name: &str,
    ) -> bool {
        if !self.enabled {
            return true;
        }
        if self.superusers.iter().any(|s| s == &who.name) {
            return true;
        }
        let matching = |allow: bool| {
            self.acls
                .iter()
                .filter(move |a| a.permission_allow == allow)
                .any(|a| a.matches(&who.name, &who.host, op, rt, name))
        };
        if matching(false) {
            return false;
        }
        matching(true)
    }
}

pub struct Authz<'a> {
    pub acls: &'a AclCache,
    pub principal: Principal,
}

impl Authz<'_> {
    pub fn allows(&self, op: Operation, rt: ResourceType, name: &str) -> bool {
        self.acls.allows(&self.principal, op, rt, name)
    }

    pub fn check(
        &self,
        op: Operation,
        rt: ResourceType,
        name: &str,
    ) -> Result<(), kafgres_codec::ErrorCode> {
        if self.allows(op, rt, name) {
            Ok(())
        } else {
            if self.acls.enabled() {
                pgrx::log!(
                    "kafgres: denied {} {} on {} '{}'",
                    self.principal.name,
                    op.as_str(),
                    rt.as_str(),
                    name
                );
            }
            Err(rt.denied_code())
        }
    }
}

#[pg_extern]
fn kafgres_add_acl(
    principal: &str,
    operation: &str,
    resource_type: &str,
    resource_name: &str,
    permission: default!(&str, "'ALLOW'"),
    pattern_type: default!(&str, "'LITERAL'"),
    host: default!(&str, "'*'"),
) -> i64 {
    // Normalised so 'read' and 'READ' are one rule, not a rule plus a CHECK violation.
    let operation = operation.to_uppercase();
    let permission = permission.to_uppercase();
    let resource_type = resource_type.to_uppercase();
    let pattern_type = pattern_type.to_uppercase();

    if !principal.starts_with("User:") {
        error!(
            "kafgres: principal must be typed, e.g. 'User:{principal}' — an untyped name \
             never matches, so the ACL would exist and do nothing"
        );
    }

    Spi::get_one_with_args::<i64>(
        "INSERT INTO kafgres_acls
            (principal, host, operation, permission, resource_type, resource_name, pattern_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING
         RETURNING acl_id",
        &[
            principal.into(),
            host.into(),
            operation.as_str().into(),
            permission.as_str().into(),
            resource_type.as_str().into(),
            resource_name.into(),
            pattern_type.as_str().into(),
        ],
    )
    .unwrap_or_else(|e| error!("kafgres: add acl failed: {e}"))
    // ON CONFLICT DO NOTHING returning no row is success, not failure.
    .unwrap_or(0)
}

#[pg_extern]
fn kafgres_remove_acl(
    principal: &str,
    operation: &str,
    resource_type: &str,
    resource_name: &str,
) -> i64 {
    Spi::get_one_with_args::<i64>(
        "WITH gone AS (
            DELETE FROM kafgres_acls
             WHERE principal = $1 AND upper(operation) = upper($2)
               AND upper(resource_type) = upper($3) AND resource_name = $4
            RETURNING 1)
         SELECT (SELECT count(*) FROM gone)",
        &[
            principal.into(),
            operation.into(),
            resource_type.into(),
            resource_name.into(),
        ],
    )
    .unwrap_or_else(|e| error!("kafgres: remove acl failed: {e}"))
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl(principal: &str, perm: &str, op: &str, rt: ResourceType, name: &str, pattern: PatternType) -> Acl {
        Acl {
            principal: principal.to_string(),
            host: "*".to_string(),
            operation: op.to_string(),
            permission_allow: perm == "ALLOW",
            resource_type: rt,
            resource_name: name.to_string(),
            pattern,
        }
    }

    fn cache(acls: Vec<Acl>) -> AclCache {
        AclCache {
            acls,
            loaded: Some(Instant::now()),
            enabled: true,
            superusers: Vec::new(),
        }
    }

    fn alice() -> Principal {
        Principal {
            name: "User:alice".to_string(),
            host: "10.0.0.1".to_string(),
        }
    }

    #[test]
    fn nothing_is_allowed_without_a_rule() {
        let c = cache(vec![]);
        assert!(!c.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
    }

    #[test]
    fn enforcement_off_allows_everything() {
        let mut c = cache(vec![]);
        c.enabled = false;
        assert!(c.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
    }

    #[test]
    fn deny_beats_allow_whatever_the_order() {
        let deny_first = cache(vec![
            acl("User:alice", "DENY", "READ", ResourceType::Topic, "secrets", PatternType::Literal),
            acl("User:alice", "ALLOW", "ALL", ResourceType::Topic, "*", PatternType::Literal),
        ]);
        let allow_first = cache(vec![
            acl("User:alice", "ALLOW", "ALL", ResourceType::Topic, "*", PatternType::Literal),
            acl("User:alice", "DENY", "READ", ResourceType::Topic, "secrets", PatternType::Literal),
        ]);
        for c in [deny_first, allow_first] {
            assert!(!c.allows(&alice(), Operation::Read, ResourceType::Topic, "secrets"));
            assert!(c.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
        }
    }

    #[test]
    fn read_implies_describe() {
        let c = cache(vec![acl(
            "User:alice", "ALLOW", "READ", ResourceType::Topic, "orders", PatternType::Literal,
        )]);
        assert!(c.allows(&alice(), Operation::Describe, ResourceType::Topic, "orders"));
        assert!(c.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
        let d = cache(vec![acl(
            "User:alice", "ALLOW", "DESCRIBE", ResourceType::Topic, "orders", PatternType::Literal,
        )]);
        assert!(!d.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
    }

    #[test]
    fn prefixed_patterns_match_a_prefix_and_literals_do_not() {
        let c = cache(vec![acl(
            "User:alice", "ALLOW", "WRITE", ResourceType::Topic, "app-", PatternType::Prefixed,
        )]);
        assert!(c.allows(&alice(), Operation::Write, ResourceType::Topic, "app-orders"));
        assert!(!c.allows(&alice(), Operation::Write, ResourceType::Topic, "other"));

        let literal = cache(vec![acl(
            "User:alice", "ALLOW", "WRITE", ResourceType::Topic, "app-", PatternType::Literal,
        )]);
        assert!(!literal.allows(&alice(), Operation::Write, ResourceType::Topic, "app-orders"));
    }

    #[test]
    fn a_rule_does_not_leak_across_resource_types() {
        let c = cache(vec![acl(
            "User:alice", "ALLOW", "READ", ResourceType::Group, "orders", PatternType::Literal,
        )]);
        assert!(c.allows(&alice(), Operation::Read, ResourceType::Group, "orders"));
        assert!(!c.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
    }

    #[test]
    fn the_wildcard_principal_matches_anyone() {
        let c = cache(vec![acl(
            "User:*", "ALLOW", "READ", ResourceType::Topic, "public", PatternType::Literal,
        )]);
        assert!(c.allows(&alice(), Operation::Read, ResourceType::Topic, "public"));
        let bob = Principal { name: "User:bob".into(), host: "10.0.0.2".into() };
        assert!(c.allows(&bob, Operation::Read, ResourceType::Topic, "public"));
    }

    #[test]
    fn a_certificate_principal_is_not_a_role_of_the_same_name() {
        let dn = Principal::certificate("CN=alice, O=kafgres", "10.0.0.1");
        let role = Principal::user("alice", "10.0.0.1");
        assert_ne!(dn.name, role.name);
        let c = cache(vec![acl(
            "User:CN=alice, O=kafgres", "ALLOW", "READ", ResourceType::Topic, "t",
            PatternType::Literal,
        )]);
        assert!(c.allows(&dn, Operation::Read, ResourceType::Topic, "t"));
        assert!(!c.allows(&role, Operation::Read, ResourceType::Topic, "t"));
    }

    #[test]
    fn a_superuser_bypasses_everything_including_deny() {
        let mut c = cache(vec![acl(
            "User:root", "DENY", "ALL", ResourceType::Topic, "*", PatternType::Literal,
        )]);
        c.superusers = vec!["User:root".to_string()];
        let root = Principal { name: "User:root".into(), host: "10.0.0.1".into() };
        assert!(c.allows(&root, Operation::Write, ResourceType::Topic, "anything"));
    }

    #[test]
    fn host_rules_are_honoured() {
        let c = cache(vec![Acl {
            principal: "User:alice".to_string(),
            host: "10.0.0.9".to_string(),
            operation: "READ".to_string(),
            permission_allow: true,
            resource_type: ResourceType::Topic,
            resource_name: "orders".to_string(),
            pattern: PatternType::Literal,
        }]);
        assert!(!c.allows(&alice(), Operation::Read, ResourceType::Topic, "orders"));
        let elsewhere = Principal { name: "User:alice".into(), host: "10.0.0.9".into() };
        assert!(c.allows(&elsewhere, Operation::Read, ResourceType::Topic, "orders"));
    }

    #[test]
    fn denials_report_the_resource_type() {
        use kafgres_codec::ErrorCode as E;
        assert_eq!(ResourceType::Topic.denied_code(), E::TopicAuthorizationFailed);
        assert_eq!(ResourceType::Group.denied_code(), E::GroupAuthorizationFailed);
        assert_eq!(ResourceType::Cluster.denied_code(), E::ClusterAuthorizationFailed);
        assert!(!E::TopicAuthorizationFailed.is_retriable());
        assert!(!E::GroupAuthorizationFailed.is_retriable());
    }
}

/// NOWAIT read lock: an operator's open `UPDATE kafgres_acls` transaction would otherwise
pub fn lock_for_read() -> Result<(), pgrx::spi::Error> {
    pgrx::Spi::run("LOCK TABLE kafgres_acls IN ACCESS SHARE MODE NOWAIT")
}

// Wire enums are written out as literal protocol constants: a wrong number silently makes

pub fn resource_type_from_wire(code: i8) -> Option<ResourceType> {
    match code {
        2 => Some(ResourceType::Topic),
        3 => Some(ResourceType::Group),
        4 => Some(ResourceType::Cluster),
        5 => Some(ResourceType::TransactionalId),
        _ => None,
    }
}

pub fn resource_type_to_wire(name: &str) -> i8 {
    match name {
        "TOPIC" => 2,
        "GROUP" => 3,
        "CLUSTER" => 4,
        "TRANSACTIONAL_ID" => 5,
        _ => 0, // UNKNOWN; unreachable while the table's CHECK constraint holds.
    }
}

pub fn pattern_type_from_wire(code: i8) -> Option<&'static str> {
    match code {
        3 => Some("LITERAL"),
        4 => Some("PREFIXED"),
        _ => None,
    }
}

pub fn pattern_type_to_wire(name: &str) -> i8 {
    match name {
        "LITERAL" => 3,
        "PREFIXED" => 4,
        _ => 0,
    }
}

pub fn operation_from_wire(code: i8) -> Option<&'static str> {
    match code {
        2 => Some("ALL"),
        3 => Some("READ"),
        4 => Some("WRITE"),
        5 => Some("CREATE"),
        6 => Some("DELETE"),
        7 => Some("ALTER"),
        8 => Some("DESCRIBE"),
        9 => Some("CLUSTER_ACTION"),
        10 => Some("DESCRIBE_CONFIGS"),
        11 => Some("ALTER_CONFIGS"),
        12 => Some("IDEMPOTENT_WRITE"),
        _ => None,
    }
}

pub fn operation_to_wire(name: &str) -> i8 {
    match name {
        "ALL" => 2,
        "READ" => 3,
        "WRITE" => 4,
        "CREATE" => 5,
        "DELETE" => 6,
        "ALTER" => 7,
        "DESCRIBE" => 8,
        "CLUSTER_ACTION" => 9,
        "DESCRIBE_CONFIGS" => 10,
        "ALTER_CONFIGS" => 11,
        "IDEMPOTENT_WRITE" => 12,
        _ => 0,
    }
}

pub fn permission_from_wire(code: i8) -> Option<&'static str> {
    match code {
        2 => Some("DENY"),
        3 => Some("ALLOW"),
        _ => None,
    }
}

pub fn permission_to_wire(name: &str) -> i8 {
    match name {
        "DENY" => 2,
        "ALLOW" => 3,
        _ => 0,
    }
}

pub fn is_any(code: i8) -> bool {
    code == 1
}
