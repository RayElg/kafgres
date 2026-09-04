//! Topic and broker configuration, as `kafka-configs.sh` sees it. A broker that reports a

use pgrx::prelude::*;

use crate::storage::RetentionPolicy;

pub const DEFAULT_RETENTION_MS: i64 = 604_800_000;

/// What kind of value a config holds, so `DescribeConfigs` can report it and
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Long,
    Int,
    String,
}

impl ConfigType {
    pub fn wire(&self) -> i8 {
        match self {
            // 1 = BOOLEAN, 2 = STRING, 3 = INT, 4 = SHORT, 5 = LONG, 6 = DOUBLE...
            ConfigType::Long => 5,
            // Upstream declares `max.message.bytes` and `min.insync.replicas` as INT, and
            ConfigType::Int => 3,
            ConfigType::String => 2,
        }
    }
}

pub struct ConfigDef {
    pub name: &'static str,
    pub default: &'static str,
    pub kind: ConfigType,
    /// Smallest accepted value, per config: -1 means "unlimited" for the retention settings
    pub min_value: i64,
    /// Upper bound, or `i64::MAX` for none. Exists for `max.message.bytes`: a batch larger
    pub max_value: i64,
    /// Whether `IncrementalAlterConfigs` may change it. A read-only entry is here
    pub writable: bool,
}

pub static TOPIC_CONFIGS: &[ConfigDef] = &[
    ConfigDef {
        name: "retention.ms",
        default: "604800000",
        kind: ConfigType::Long,
        min_value: -1,
        max_value: i64::MAX,
        writable: true,
    },
    ConfigDef {
        name: "retention.bytes",
        default: "-1",
        kind: ConfigType::Long,
        min_value: -1,
        max_value: i64::MAX,
        writable: true,
    },
    // Enforced on the produce path, per batch, exactly as upstream does. The default is
    ConfigDef {
        name: "max.message.bytes",
        default: "1048588",
        kind: ConfigType::Int,
        min_value: 0,
        // Bounded by what the broker can hand back. See `ConfigDef::max_value`: a larger
        max_value: 8 * 1024 * 1024,
        writable: true,
    },
    // Accepted only at `producer` ("store what the producer sent"). Any other value asks
    ConfigDef {
        name: "compression.type",
        default: "producer",
        kind: ConfigType::String,
        min_value: 0,
        max_value: i64::MAX,
        writable: false,
    },
    // One replica, always in sync with itself — replication here is Postgres's, so there is
    ConfigDef {
        name: "min.insync.replicas",
        default: "1",
        kind: ConfigType::Int,
        min_value: 1,
        max_value: i64::MAX,
        writable: false,
    },
    // `LogAppendTime` would mean re-encoding every batch to rewrite timestamps; `CreateTime` is what we do.
    ConfigDef {
        name: "message.timestamp.type",
        default: "CreateTime",
        kind: ConfigType::String,
        min_value: 0,
        max_value: i64::MAX,
        writable: false,
    },
    // How long the active segment may stay open before it rolls, whatever its size. A
    ConfigDef {
        name: "segment.bytes",
        default: "1073741824",
        kind: ConfigType::Long,
        min_value: 1024,
        max_value: i64::MAX,
        writable: true,
    },
    ConfigDef {
        name: "segment.ms",
        default: "604800000",
        kind: ConfigType::Long,
        min_value: 1,
        max_value: i64::MAX,
        writable: true,
    },
    // How long a record must exist before compaction may remove it. Default 0, which with
    ConfigDef {
        name: "min.compaction.lag.ms",
        default: "0",
        kind: ConfigType::Long,
        min_value: 0,
        max_value: i64::MAX,
        writable: true,
    },
    // How long a tombstone survives after the pass that could have removed it: without it a
    ConfigDef {
        name: "delete.retention.ms",
        default: "86400000",
        kind: ConfigType::Long,
        min_value: 0,
        max_value: i64::MAX,
        writable: true,
    },
    // `validate` restricts this to the two values this broker implements, and still refuses
    ConfigDef {
        name: "cleanup.policy",
        default: "delete",
        kind: ConfigType::String,
        min_value: 0,
        max_value: i64::MAX,
        writable: true,
    },
];

/// Broker configs, all read-only over the wire: the real knobs are GUCs that `ALTER SYSTEM`
pub static BROKER_CONFIGS: &[ConfigDef] = &[
    ConfigDef {
        name: "log.retention.ms",
        default: "604800000",
        kind: ConfigType::Long,
        min_value: -1,
        max_value: i64::MAX,
        writable: false,
    },
    ConfigDef {
        name: "num.partitions",
        default: "1",
        kind: ConfigType::Int,
        min_value: 1,
        max_value: i64::MAX,
        writable: false,
    },
];

pub fn topic_def(name: &str) -> Option<&'static ConfigDef> {
    TOPIC_CONFIGS.iter().find(|c| c.name == name)
}

pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    /// `false` when the topic overrides it — Kafka calls this `is_default`, and
    pub is_default: bool,
    pub read_only: bool,
    pub kind: ConfigType,
}

pub fn describe_topic(topic_id: u32) -> Result<Vec<ConfigEntry>, spi::Error> {
    let overrides = load_overrides(topic_id)?;
    Ok(TOPIC_CONFIGS
        .iter()
        .map(|def| match overrides.iter().find(|(k, _)| k == def.name) {
            Some((_, v)) => ConfigEntry {
                name: def.name.to_string(),
                value: Some(v.clone()),
                is_default: false,
                read_only: !def.writable,
                kind: def.kind,
            },
            None => ConfigEntry {
                name: def.name.to_string(),
                value: Some(def.default.to_string()),
                is_default: true,
                read_only: !def.writable,
                kind: def.kind,
            },
        })
        .collect())
}

pub fn describe_broker() -> Vec<ConfigEntry> {
    BROKER_CONFIGS
        .iter()
        .map(|def| ConfigEntry {
            name: def.name.to_string(),
            value: Some(def.default.to_string()),
            is_default: true,
            read_only: true,
            kind: def.kind,
        })
        .collect()
}

fn load_overrides(topic_id: u32) -> Result<Vec<(String, String)>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT key, value FROM kafgres_topics t,
                    LATERAL jsonb_each_text(t.config)
              WHERE t.topic_id = $1::oid",
            None,
            &[(topic_id as i32).into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(k), Some(v)) = (row.get::<String>(1)?, row.get::<String>(2)?) {
                out.push((k, v));
            }
        }
        Ok(out)
    })
}

/// Why a config change was refused.
#[derive(Debug, Clone)]
pub enum ConfigError {
    Unknown(String),
    /// The *resource* does not exist, as distinct from the config name being unknown.
    UnknownTopic(String),
    ReadOnly { name: String, implemented: &'static str },
    BadValue { name: String, value: String },
    /// A resource type we do not configure — broker loggers, client quotas.
    UnsupportedResource(i8),
    /// The authorizer said no. Carries the code so the one mapping point stays one.
    Denied(kafgres_codec::ErrorCode),
    Internal(String),
}

impl ConfigError {
    pub fn error_code(&self) -> kafgres_codec::ErrorCode {
        use kafgres_codec::ErrorCode as E;
        match self {
            // Upstream returns INVALID_CONFIG for an unknown name too — the client is
            ConfigError::Unknown(_)
            | ConfigError::ReadOnly { .. }
            | ConfigError::BadValue { .. } => E::InvalidConfig,
            ConfigError::UnknownTopic(_) => E::UnknownTopicOrPartition,
            ConfigError::UnsupportedResource(_) => E::InvalidRequest,
            ConfigError::Denied(code) => *code,
            ConfigError::Internal(_) => E::UnknownServerError,
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Unknown(n) => write!(f, "unknown config '{n}'"),
            ConfigError::UnknownTopic(n) => write!(f, "unknown topic '{n}'"),
            ConfigError::ReadOnly { name, implemented } => write!(
                f,
                "'{name}' is read-only on this broker; it is '{implemented}' and cannot be \
                 changed. Setting it to '{implemented}' is accepted as a no-op."
            ),
            ConfigError::BadValue { name, value } => {
                write!(f, "'{value}' is not a valid {name}")
            }
            ConfigError::UnsupportedResource(t) => write!(f, "resource type {t} is not configurable"),
            ConfigError::Denied(_) => write!(f, "not authorized"),
            ConfigError::Internal(m) => write!(f, "internal: {m}"),
        }
    }
}

impl From<spi::Error> for ConfigError {
    fn from(e: spi::Error) -> Self {
        ConfigError::Internal(e.to_string())
    }
}

pub const OP_SET: i8 = 0;
pub const OP_DELETE: i8 = 1;
pub const OP_APPEND: i8 = 2;
pub const OP_SUBTRACT: i8 = 3;

/// `delete` always; `compact` only where the engine can actually compact — accepting it
fn validate_cleanup_policy(value: &str) -> Result<(), ConfigError> {
    let bad = |v: &str| ConfigError::BadValue {
        name: "cleanup.policy".to_string(),
        value: v.to_string(),
    };
    match value.trim() {
        "delete" => Ok(()),
        // The table engine rewrites rows inside one transaction (MVCC-atomic); the segment
        "compact" | "compact,delete" => Ok(()),
        other => Err(bad(other)),
    }
}

/// May this change proceed? A read-only config still accepts a write that **asserts the
pub fn check_alterable(def: &ConfigDef, op: i8, value: Option<&str>) -> Result<(), ConfigError> {
    match op {
        OP_SET | OP_DELETE => {}
        // APPEND and SUBTRACT are for list-valued configs. Nothing here is a list, and
        OP_APPEND | OP_SUBTRACT => {
            return Err(ConfigError::BadValue {
                name: def.name.to_string(),
                value: "append/subtract on a scalar config".to_string(),
            })
        }
        other => {
            return Err(ConfigError::BadValue {
                name: def.name.to_string(),
                value: format!("unknown operation {other}"),
            })
        }
    }
    if def.writable {
        // The value as well: `IncrementalAlterConfigs` validates a whole resource before
        if op == OP_SET {
            let v = value.ok_or_else(|| ConfigError::BadValue {
                name: def.name.to_string(),
                value: "null".to_string(),
            })?;
            validate(def, v)?;
        }
        return Ok(());
    }
    // Removing an override that cannot exist: the result is the default, which is the value
    if op == OP_DELETE {
        return Ok(());
    }
    // Read-only entries can never have been changed, so the default *is* the live value.
    if value.map(|v| v.trim() == def.default) == Some(true) {
        return Ok(());
    }
    Err(ConfigError::ReadOnly {
        name: def.name.to_string(),
        implemented: def.default,
    })
}

/// Drop every stored override except the named ones. `AlterConfigs` (pre-KIP-339) sends a
pub fn replace_topic_config(topic_id: u32, keep: &[String]) -> Result<(), ConfigError> {
    // `?| ` is "has any of these keys". Deleting the complement in one step means a config
    let keep: Vec<String> = keep.to_vec();
    Spi::run_with_args(
        "UPDATE kafgres_topics
            SET config = COALESCE(
                  (SELECT jsonb_object_agg(k, v)
                     FROM jsonb_each(config) AS e(k, v)
                    WHERE k = ANY($2::text[])),
                  '{}'::jsonb)
          WHERE topic_id = $1::oid",
        &[(topic_id as i32).into(), keep.into()],
    )?;
    Ok(())
}

/// Apply one incremental config change to a topic. Validated before it is stored, not when
pub fn alter_topic_config(
    topic_id: u32,
    name: &str,
    op: i8,
    value: Option<&str>,
) -> Result<(), ConfigError> {
    let def = topic_def(name).ok_or_else(|| ConfigError::Unknown(name.to_string()))?;
    check_alterable(def, op, value)?;
    if !def.writable {
        // A no-op — an assertion of the current value, or a reset to the default it already
        return Ok(());
    }

    if op == OP_DELETE {
        Spi::run_with_args(
            "UPDATE kafgres_topics SET config = config - $2 WHERE topic_id = $1::oid",
            &[(topic_id as i32).into(), name.into()],
        )?;
        return Ok(());
    }

    if op != OP_SET {
        return Err(ConfigError::BadValue {
            name: name.to_string(),
            value: format!("unknown operation {op}"),
        });
    }

    let value = value.ok_or_else(|| ConfigError::BadValue {
        name: name.to_string(),
        value: "null".to_string(),
    })?;
    validate(def, value)?;

    Spi::run_with_args(
        "UPDATE kafgres_topics
            SET config = config || jsonb_build_object($2::text, $3::text)
          WHERE topic_id = $1::oid",
        &[(topic_id as i32).into(), name.into(), value.into()],
    )?;
    Ok(())
}

fn validate(def: &ConfigDef, value: &str) -> Result<(), ConfigError> {
    match def.kind {
        ConfigType::Long | ConfigType::Int => {
            let n: i64 = value.parse().map_err(|_| ConfigError::BadValue {
                name: def.name.to_string(),
                value: value.to_string(),
            })?;
            // Per config, not a shared floor: -1 is Kafka's "unlimited" for the retention
            if n < def.min_value || n > def.max_value {
                return Err(ConfigError::BadValue {
                    name: def.name.to_string(),
                    value: value.to_string(),
                });
            }
            Ok(())
        }
        ConfigType::String => {
            if def.name == "cleanup.policy" {
                return validate_cleanup_policy(value);
            }
            Ok(())
        }
    }
}

pub fn retention_policy(topic_id: u32) -> Result<RetentionPolicy, spi::Error> {
    let overrides = load_overrides(topic_id)?;
    let get = |name: &str, fallback: i64| -> i64 {
        overrides
            .iter()
            .find(|(k, _)| k == name)
            .and_then(|(_, v)| v.parse::<i64>().ok())
            .unwrap_or(fallback)
    };
    let ms = get("retention.ms", DEFAULT_RETENTION_MS);
    let bytes = get("retention.bytes", -1);
    Ok(RetentionPolicy {
        // -1 means keep forever, which the policy expresses as None rather than as a
        retention_ms: if ms < 0 { None } else { Some(ms) },
        retention_bytes: if bytes < 0 { None } else { Some(bytes) },
    })
}

pub fn segment_bytes(topic_id: u32) -> i64 {
    load_overrides(topic_id)
        .ok()
        .and_then(|o| {
            o.iter()
                .find(|(k, _)| k == "segment.bytes")
                .and_then(|(_, v)| v.parse::<i64>().ok())
        })
        .unwrap_or_else(|| crate::segment_bytes() as i64)
}

pub fn segment_ms(topic_id: u32) -> i64 {
    load_overrides(topic_id)
        .ok()
        .and_then(|o| {
            o.iter()
                .find(|(k, _)| k == "segment.ms")
                .and_then(|(_, v)| v.parse::<i64>().ok())
        })
        .unwrap_or(604_800_000)
}

pub struct CompactionLimits {
    pub min_compaction_lag_ms: i64,
    pub delete_retention_ms: i64,
}

pub fn compaction_limits(topic_id: u32) -> CompactionLimits {
    let overrides = load_overrides(topic_id).unwrap_or_default();
    let get = |name: &str, fallback: i64| -> i64 {
        overrides
            .iter()
            .find(|(k, _)| k == name)
            .and_then(|(_, v)| v.parse::<i64>().ok())
            .unwrap_or(fallback)
    };
    CompactionLimits {
        min_compaction_lag_ms: get("min.compaction.lag.ms", 0),
        delete_retention_ms: get("delete.retention.ms", 86_400_000),
    }
}

pub fn is_compacted(topic_id: u32) -> bool {
    cleanup_policy(topic_id).map(|p| p.compacts).unwrap_or(false)
}

pub struct CleanupPolicy {
    pub compacts: bool,
    /// `compact,delete` does both. `compact` alone does not age records out at all — the
    pub deletes: bool,
}

pub fn cleanup_policy(topic_id: u32) -> Option<CleanupPolicy> {
    let overrides = load_overrides(topic_id).ok()?;
    let value = overrides
        .iter()
        .find(|(k, _)| k == "cleanup.policy")
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_else(|| "delete".to_string());
    Some(match value.as_str() {
        "compact" => CleanupPolicy { compacts: true, deletes: false },
        "compact,delete" => CleanupPolicy { compacts: true, deletes: true },
        _ => CleanupPolicy { compacts: false, deletes: true },
    })
}

pub const DEFAULT_MAX_MESSAGE_BYTES: i64 = 1_048_588;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_configs_we_honour_are_advertised() {
        // Reporting a config the broker ignores is worse than omitting it: the operator
        for def in TOPIC_CONFIGS {
            assert!(
                matches!(
                    def.name,
                    "retention.ms"
                        | "retention.bytes"
                        | "max.message.bytes"
                        | "min.compaction.lag.ms"
                        | "delete.retention.ms"
                        | "segment.ms"
                        | "segment.bytes"
                        // Reported at the single value this broker implements, and refused
                        | "cleanup.policy"
                        | "compression.type"
                        | "min.insync.replicas"
                        | "message.timestamp.type"
                ),
                "{} is advertised but nothing enforces it",
                def.name
            );
        }
    }

    #[test]
    fn the_enforced_default_matches_the_reported_one() {
        // Two independent literals: `DEFAULT_MAX_MESSAGE_BYTES` is what produce falls back
        let def = topic_def("max.message.bytes").unwrap();
        assert_eq!(
            def.default.parse::<i64>().unwrap(),
            DEFAULT_MAX_MESSAGE_BYTES
        );
    }

    #[test]
    fn a_lower_bound_is_per_config() {
        // -1 is "unlimited" for retention and simply invalid for max.message.bytes. A
        let ret = topic_def("retention.bytes").unwrap();
        assert!(validate(ret, "-1").is_ok());
        let mmb = topic_def("max.message.bytes").unwrap();
        assert!(validate(mmb, "-1").is_err(), "max.message.bytes accepted -1");
        assert!(validate(mmb, "0").is_ok());
        assert!(validate(mmb, "1048588").is_ok());
    }

    #[test]
    fn cleanup_policy_is_writable_and_value_checked() {
        let def = topic_def("cleanup.policy").unwrap();
        assert!(def.writable);

        assert!(check_alterable(def, OP_SET, Some("delete")).is_ok());
        assert!(check_alterable(def, OP_SET, Some("compact")).is_ok());
        assert!(check_alterable(def, OP_SET, Some("compact,delete")).is_ok());
        assert!(check_alterable(def, OP_SET, Some("  delete  ")).is_ok());
        assert!(check_alterable(def, OP_DELETE, Some("")).is_ok());
        assert!(check_alterable(def, OP_DELETE, None).is_ok());

        // Writable is not "accepts anything". A policy nothing implements must still be
        assert!(check_alterable(def, OP_SET, None).is_err());
        assert!(check_alterable(def, OP_SET, Some("delete,compact")).is_err());
        assert!(check_alterable(def, OP_SET, Some("archive")).is_err());
        // Upstream validates against an exact-cased list, so this is INVALID_CONFIG there.
        assert!(check_alterable(def, OP_SET, Some("DELETE")).is_err());
    }

    #[test]
    fn a_writable_config_is_value_checked_at_the_same_gate() {
        // `check_alterable` is what CreateTopics and IncrementalAlterConfigs' pre-validation
        let ret = topic_def("retention.ms").unwrap();
        assert!(check_alterable(ret, OP_SET, Some("604800000")).is_ok());
        assert!(check_alterable(ret, OP_SET, Some("forever")).is_err());
        assert!(check_alterable(ret, OP_SET, Some("-5")).is_err());
        assert!(check_alterable(ret, OP_SET, None).is_err());
        assert!(check_alterable(ret, OP_SET, Some("-1")).is_ok());
        assert!(check_alterable(ret, OP_DELETE, None).is_ok());
    }

    #[test]
    fn subtracting_the_default_is_not_a_no_op() {
        // The regression this ordering exists to prevent: SUBTRACT's value happens to equal
        let def = topic_def("cleanup.policy").unwrap();
        assert!(check_alterable(def, OP_SUBTRACT, Some("delete")).is_err());
        assert!(check_alterable(def, OP_APPEND, Some("delete")).is_err());
        let ret = topic_def("retention.ms").unwrap();
        assert!(check_alterable(ret, OP_SUBTRACT, Some("604800000")).is_err());
    }

    #[test]
    fn a_read_only_config_accepts_only_its_default() {
        for def in TOPIC_CONFIGS.iter().filter(|d| !d.writable) {
            assert!(
                check_alterable(def, OP_SET, Some(def.default)).is_ok(),
                "read-only '{}' refused its own default",
                def.name
            );
            let other = format!("{}-not-the-default", def.default);
            assert!(
                matches!(
                    check_alterable(def, OP_SET, Some(&other)),
                    Err(ConfigError::ReadOnly { .. })
                ),
                "read-only '{}' accepted a value that is not its default",
                def.name
            );
        }
    }

    #[test]
    fn retention_values_are_range_checked() {
        let def = topic_def("retention.ms").unwrap();
        assert!(validate(def, "0").is_ok());
        assert!(validate(def, "-1").is_ok(), "-1 is Kafka's 'unlimited'");
        assert!(validate(def, "604800000").is_ok());
        assert!(validate(def, "-2").is_err());
        assert!(validate(def, "forever").is_err());
    }
}
