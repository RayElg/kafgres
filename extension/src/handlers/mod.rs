pub mod acls;
pub mod admin;
pub mod api_versions;
pub mod auth;
pub mod configs;
pub mod consumer_group;
pub mod coordinator;
pub mod describe_groups;
pub mod fetch;
pub mod init_producer_id;
pub mod introspect;
pub mod join_sync;
pub mod leader_epoch;
pub mod list_offsets;
pub mod metadata;
pub mod offsets;
pub mod produce;
pub mod share_group;
pub mod topics;
pub mod txn;

use kafgres_codec::errors::{CodecError, ErrorCode};
use kafgres_codec::framing::{negotiate, write_frame, Negotiation};
use kafgres_codec::header::{
    request_header_version, response_header_version, RequestHeader, ResponseHeader,
};
use kafgres_codec::prelude::*;
use kafgres_codec::Encodable;

/// Kept in step with the inbound frame cap: a response the peer could never have asked
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Responses are measured only after assembly, and items expand far past their wire size.
pub const MAX_ADMIN_ITEMS: usize = 1_000;

pub fn check_admin_len(what: &'static str, n: usize) -> Result<(), HandlerError> {
    if n > MAX_ADMIN_ITEMS {
        return Err(HandlerError::TooLarge { what, n });
    }
    Ok(())
}

/// Distinct from `CodecError`: a broker-side failure must never read as a protocol error.
#[derive(Debug)]
pub enum HandlerError {
    Codec(CodecError),
    TooLarge { what: &'static str, n: usize },
    Internal(String),
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerError::Codec(e) => write!(f, "{e}"),
            HandlerError::TooLarge { what, n } => write!(f, "{what} too large: {n}"),
            HandlerError::Internal(m) => write!(f, "internal: {m}"),
        }
    }
}

impl From<CodecError> for HandlerError {
    fn from(e: CodecError) -> Self {
        HandlerError::Codec(e)
    }
}

impl From<pgrx::spi::Error> for HandlerError {
    fn from(e: pgrx::spi::Error) -> Self {
        HandlerError::Internal(format!("spi: {e}"))
    }
}

pub struct Request {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub body: Bytes,
}

pub fn parse(frame: Bytes) -> Result<(Request, Negotiation), CodecError> {
    let mut buf = frame;
    let (api_key, api_version) = RequestHeader::peek_api(&buf)?;
    let outcome = negotiate(api_key, api_version);

    // The header version follows the *body* version, so an unsupported version leaves the
    let header_version = match outcome {
        Negotiation::Supported => request_header_version(api_key, api_version)?,
        _ => {
            if api_version >= 3 && api_key == kafgres_codec::header::API_VERSIONS_KEY {
                2
            } else {
                1
            }
        }
    };

    let header = RequestHeader::decode(&mut buf, header_version)?;
    Ok((
        Request {
            api_key,
            api_version,
            correlation_id: header.correlation_id,
            client_id: header.client_id,
            body: buf,
        },
        outcome,
    ))
}

pub fn write_response<T: Encodable>(
    out: &mut BytesMut,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &T,
) -> Result<(), HandlerError> {
    let header_version = response_header_version(api_key, api_version)?;
    let header = ResponseHeader {
        correlation_id,
        unknown_tagged_fields: Vec::new(),
    };
    let total = header.size(header_version) + body.size(api_version);
    if total > MAX_RESPONSE_BYTES {
        return Err(HandlerError::TooLarge {
            what: "response",
            n: total,
        });
    }
    out.reserve(total + 4);
    write_frame(out, |b| {
        header.encode(b, header_version)?;
        body.encode(b, api_version)
    })?;
    Ok(())
}

/// ApiVersions fallback: a v0-encoded body with the real ranges — all such a client can parse.
pub fn write_unsupported(
    out: &mut BytesMut,
    req: &Request,
    outcome: Negotiation,
) -> Result<(), HandlerError> {
    let code = outcome.error_code().unwrap_or(ErrorCode::UnsupportedVersion);

    if matches!(outcome, Negotiation::ApiVersionsFallback) {
        let body = api_versions::build(code);
        return write_response(
            out,
            kafgres_codec::header::API_VERSIONS_KEY,
            0, // v0 body *and* v0 header — see the doc comment.
            req.correlation_id,
            &body,
        );
    }

    Err(CodecError::UnsupportedVersion {
        api_key: req.api_key,
        version: req.api_version,
    }
    .into())
}
