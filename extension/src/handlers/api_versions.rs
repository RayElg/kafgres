//! Built from the generated `ADVERTISED` table, so advertised and accepted cannot drift.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafgres_codec::generated::apis::ADVERTISED;

/// Ranges are included even on the fallback path — that is the entire point of the fallback.
pub fn build(error: ErrorCode) -> ApiVersionsResponse {
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
        ..Default::default()
    }
}

pub fn handle() -> ApiVersionsResponse {
    build(ErrorCode::None)
}
