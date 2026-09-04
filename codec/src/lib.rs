//! Kafka wire protocol codec for kafgres. Free of pgrx and libpq, so protocol correctness

pub mod compaction;
pub mod errors;
pub mod framing;
pub mod header;
pub mod primitives;
pub mod records;

#[cfg(any(test, feature = "sample"))]
pub mod sample;

pub mod generated;

#[cfg(test)]
mod conformance;

/// Re-exported so dependents use the identical `bytes` version: a mismatch would make
pub use bytes;

pub use errors::{CodecError, ErrorCode};
pub use primitives::{RawTaggedField, Uuid};

use bytes::{Bytes, BytesMut};

/// Inclusive version range, as declared by a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRange {
    pub min: i16,
    pub max: i16,
}

impl VersionRange {
    pub const fn new(min: i16, max: i16) -> Self {
        VersionRange { min, max }
    }
    pub fn contains(&self, v: i16) -> bool {
        v >= self.min && v <= self.max
    }
}

/// One row of the ApiVersions response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// Static facts about an API, straight from its schema.
#[derive(Debug, Clone, Copy)]
pub struct ApiSpec {
    pub key: i16,
    pub name: &'static str,
    pub valid: VersionRange,
    pub flexible: VersionRange,
    /// Top version is not stable upstream. Never advertise it.
    pub latest_version_unstable: bool,
}

impl ApiSpec {
    /// Whether `version` uses compact strings/arrays and carries a tagged-field section.
    pub fn is_flexible(&self, version: i16) -> bool {
        self.flexible.contains(version)
    }
}

/// Encode/decode for a message, or any struct nested inside one. `size` is not an
pub trait Encodable: Sized + Default {
    fn size(&self, version: i16) -> usize;
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), CodecError>;
    fn decode(buf: &mut Bytes, version: i16) -> Result<Self, CodecError>;
}

/// A top-level request or response.
pub trait ApiMessage: Encodable {
    const API_KEY: i16;
    const NAME: &'static str;
    const VALID: VersionRange;
    const FLEXIBLE: VersionRange;
    const LATEST_VERSION_UNSTABLE: bool;

    fn is_flexible(version: i16) -> bool {
        Self::FLEXIBLE.contains(version)
    }
}

/// Everything the generated modules need in scope.
pub mod prelude {
    pub use crate::errors::CodecError;
    pub use crate::primitives::*;
    pub use crate::{ApiMessage, ApiSpec, ApiVersionRange, Encodable, VersionRange};
    pub use bytes::{Bytes, BytesMut};

    #[cfg(any(test, feature = "sample"))]
    pub use crate::sample::{Gen, Sample};
}
