//! Built from the generated `ADVERTISED` table, so advertised and accepted cannot drift.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::api_versions_response::{
    ApiVersion, ApiVersionsResponse, FinalizedFeatureKey, SupportedFeatureKey,
};
use kafgres_codec::generated::apis::ADVERTISED;

/// The feature set is fixed by build and configuration, not a controller election, so the
/// epoch never advances. 0 marks the data valid; -1 tells clients every feature is off.
const FEATURES_EPOCH: i64 = 0;

/// Cluster feature flags, reported the way Kafka reports them. Clients pick protocol
/// behaviour from these levels, not from API version ranges: the Java client enables
/// EndTxn v5 and KIP-890 epoch rotation only when `transaction.version` is finalized at 2.
/// Only features this broker serves are listed.
fn features() -> (Vec<SupportedFeatureKey>, Vec<FinalizedFeatureKey>) {
    let txn = crate::transaction_version() as i16;
    let declared: &[(&str, i16, i16)] = &[
        // KIP-890. 2 enables the epoch rotation at EndTxn v5; see `kafgres.transaction_version`.
        ("transaction.version", 2, txn),
        // KIP-848, the consumer group protocol: served, so finalized.
        ("group.version", 1, 1),
        // KIP-932 share groups: served, so finalized.
        ("share.version", 1, 1),
    ];

    let supported = declared
        .iter()
        .map(|(name, max_supported, _)| SupportedFeatureKey {
            name: name.to_string(),
            min_version: 0,
            max_version: *max_supported,
            unknown_tagged_fields: Vec::new(),
        })
        .collect();
    let finalized = declared
        .iter()
        .map(|(name, _, level)| FinalizedFeatureKey {
            name: name.to_string(),
            min_version_level: *level,
            max_version_level: *level,
            unknown_tagged_fields: Vec::new(),
        })
        .collect();
    (supported, finalized)
}

/// Ranges are included even on the fallback path — that is the entire point of the fallback.
pub fn build(error: ErrorCode) -> ApiVersionsResponse {
    let (supported_features, finalized_features) = features();
    ApiVersionsResponse {
        error_code: error.code(),
        api_keys: ADVERTISED
            .iter()
            .map(|a| ApiVersion {
                api_key: a.api_key,
                min_version: a.min_version,
                max_version: a.max_version,
                unknown_tagged_fields: Vec::new(),
            })
            .collect(),
        throttle_time_ms: 0,
        supported_features,
        finalized_features_epoch: FEATURES_EPOCH,
        finalized_features,
        ..Default::default()
    }
}

pub fn handle() -> ApiVersionsResponse {
    build(ErrorCode::None)
}
