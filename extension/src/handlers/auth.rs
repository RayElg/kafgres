use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::sasl_authenticate_request::SaslAuthenticateRequest;
use kafgres_codec::generated::sasl_authenticate_response::SaslAuthenticateResponse;
use kafgres_codec::generated::sasl_handshake_request::SaslHandshakeRequest;
use kafgres_codec::generated::sasl_handshake_response::SaslHandshakeResponse;

use crate::sasl::{self, SaslState};

/// The mechanism list goes back on rejection too — how a client learns what to retry with.
pub fn handshake(
    req: &SaslHandshakeRequest,
    state: &SaslState,
) -> (SaslHandshakeResponse, Option<SaslState>) {
    match sasl::handshake(state, &req.mechanism) {
        Ok(next) => (
            SaslHandshakeResponse {
                error_code: ErrorCode::None.code(),
                mechanisms: vec![sasl::MECHANISM.to_string()],
                ..Default::default()
            },
            Some(next),
        ),
        Err(e) => {
            // An unauthenticated peer's chosen string; 64 characters still names a real one.
            let shown: String = e.to_string().chars().take(64).collect();
            pgrx::log!("kafgres: sasl handshake rejected: {shown}");
            (
                SaslHandshakeResponse {
                    error_code: e.error_code().code(),
                    mechanisms: vec![sasl::MECHANISM.to_string()],
                    ..Default::default()
                },
                None,
            )
        }
    }
}

/// Failures are reported in-band: the code tells wrong password from wrong mechanism.
pub fn authenticate(
    req: &SaslAuthenticateRequest,
    state: &SaslState,
) -> (SaslAuthenticateResponse, Option<SaslState>) {
    match sasl::step(state, &req.auth_bytes) {
        Ok((bytes, next)) => (
            SaslAuthenticateResponse {
                error_code: ErrorCode::None.code(),
                error_message: None,
                auth_bytes: kafgres_codec::bytes::Bytes::from(bytes),
                // 0 = no expiry: re-auth (KIP-368) is unimplemented; advertising one would disconnect clients.
                session_lifetime_ms: 0,
                ..Default::default()
            },
            Some(next),
        ),
        Err(e) => {
            pgrx::log!("kafgres: sasl authenticate failed: {e}");
            (
                SaslAuthenticateResponse {
                    error_code: e.error_code().code(),
                    error_message: Some(e.to_string()),
                    auth_bytes: kafgres_codec::bytes::Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                },
                None,
            )
        }
    }
}
