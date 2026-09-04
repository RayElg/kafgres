//! Request/response framing and version negotiation.

use crate::errors::{CodecError, ErrorCode};
use crate::generated::apis::{ADVERTISED, SCHEMA_APIS};
use crate::header::API_VERSIONS_KEY;
use bytes::{Buf, Bytes, BytesMut};

/// Default frame ceiling. Deliberately far below Kafka's 100 MB `socket.request.max.bytes`
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Declared size is negative or exceeds the configured cap. Fatal: the stream
    Oversized { declared: i64, max: usize },
}

/// Pull one complete frame off the front of `buf`, if there is one; `Ok(None)` when more
pub fn take_frame(buf: &mut BytesMut, max: usize) -> Result<Option<Bytes>, FrameError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let declared = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64;
    if declared < 0 || declared > max as i64 {
        return Err(FrameError::Oversized { declared, max });
    }
    let total = 4 + declared as usize;
    if buf.len() < total {
        // Deliberately no `buf.reserve(total - buf.len())` here: a length that passed the
        return Ok(None);
    }
    let mut frame = buf.split_to(total).freeze();
    frame.advance(4);
    Ok(Some(frame))
}

/// Write a length-prefixed frame, backfilling the size once the body is encoded. The
pub fn write_frame<F>(out: &mut BytesMut, encode_body: F) -> Result<(), CodecError>
where
    F: FnOnce(&mut BytesMut) -> Result<(), CodecError>,
{
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    encode_body(out)?;
    let len = out.len() - start - 4;
    if len > i32::MAX as usize {
        return Err(CodecError::InvalidLength(len as i64));
    }
    out[start..start + 4].copy_from_slice(&(len as i32).to_be_bytes());
    Ok(())
}

/// What to do with an inbound `(api_key, api_version)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Negotiation {
    Supported,
    /// ApiVersions asked for a version we do not implement. Special-cased: a client sends
    ApiVersionsFallback,
    /// Any other API at a version we do not serve. Answer `UNSUPPORTED_VERSION` at a
    UnsupportedVersion,
    /// An api key the vendored schemas do not define at all.
    UnknownApi,
}

/// Decide how to handle an inbound request. Consults [`ADVERTISED`] — the same table the
pub fn negotiate(api_key: i16, api_version: i16) -> Negotiation {
    let spec = SCHEMA_APIS.iter().find(|a| a.key == api_key);
    let known = spec.is_some();

    // Accepted is advertised *intersected with* what the schemas define: Produce is
    let served = match (spec, ADVERTISED.iter().find(|a| a.api_key == api_key)) {
        (Some(spec), Some(adv)) => {
            api_version >= adv.min_version
                && api_version <= adv.max_version
                && spec.valid.contains(api_version)
        }
        _ => false,
    };

    if served {
        return Negotiation::Supported;
    }
    if api_key == API_VERSIONS_KEY {
        return Negotiation::ApiVersionsFallback;
    }
    if known {
        Negotiation::UnsupportedVersion
    } else {
        Negotiation::UnknownApi
    }
}

impl Negotiation {
    /// The error code to put in the response body, if any.
    pub fn error_code(self) -> Option<ErrorCode> {
        match self {
            Negotiation::Supported => None,
            Negotiation::ApiVersionsFallback
            | Negotiation::UnsupportedVersion
            | Negotiation::UnknownApi => Some(ErrorCode::UnsupportedVersion),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(body: &[u8]) -> BytesMut {
        let mut b = BytesMut::new();
        b.extend_from_slice(&(body.len() as i32).to_be_bytes());
        b.extend_from_slice(body);
        b
    }

    #[test]
    fn takes_one_frame_and_leaves_the_rest() {
        let mut buf = framed(b"hello");
        buf.extend_from_slice(&framed(b"world")[..]);
        let a = take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(&a[..], b"hello");
        let b = take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(&b[..], b"world");
        assert!(take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES)
            .unwrap()
            .is_none());
    }

    #[test]
    fn partial_frames_are_incomplete_not_errors() {
        let full = framed(b"abcdefgh");
        for cut in 0..full.len() {
            let mut buf = BytesMut::from(&full[..cut]);
            assert_eq!(
                take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES),
                Ok(None),
                "prefix of {cut} bytes should be incomplete"
            );
            // Nothing consumed, so the next read appends and retries.
            assert_eq!(buf.len(), cut);
        }
    }

    #[test]
    fn empty_frame_is_valid() {
        let mut buf = framed(b"");
        let f = take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES)
            .unwrap()
            .unwrap();
        assert!(f.is_empty());
    }

    /// A 4-byte prefix must never be able to name its own allocation.
    #[test]
    fn negative_and_oversized_lengths_are_refused() {
        let mut buf = BytesMut::from(&(-1i32).to_be_bytes()[..]);
        assert!(matches!(
            take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES),
            Err(FrameError::Oversized { declared: -1, .. })
        ));

        let mut buf = BytesMut::from(&i32::MAX.to_be_bytes()[..]);
        assert!(matches!(
            take_frame(&mut buf, 1024),
            Err(FrameError::Oversized { .. })
        ));
    }

    #[test]
    fn oversized_check_happens_before_buffering() {
        // Only the 4-byte prefix has arrived; we must reject without waiting for the
        let mut buf = BytesMut::from(&(1_000_000_000i32).to_be_bytes()[..]);
        assert!(take_frame(&mut buf, 1024).is_err());
        assert_eq!(buf.len(), 4, "nothing consumed on a fatal frame error");
    }

    /// A length prefix under the cap must still not be able to name its own allocation.
    #[test]
    fn an_incomplete_frame_does_not_allocate_its_declared_size() {
        let mut buf = BytesMut::with_capacity(64);
        buf.extend_from_slice(&((DEFAULT_MAX_REQUEST_BYTES - 4) as i32).to_be_bytes());
        assert_eq!(take_frame(&mut buf, DEFAULT_MAX_REQUEST_BYTES), Ok(None));
        assert!(
            buf.capacity() < 4096,
            "reserved {} bytes from a 4-byte prefix",
            buf.capacity()
        );
    }

    #[test]
    fn write_frame_backfills_the_length() {
        let mut out = BytesMut::new();
        write_frame(&mut out, |b| {
            b.extend_from_slice(b"abcd");
            Ok(())
        })
        .unwrap();
        assert_eq!(&out[..], &[0, 0, 0, 4, b'a', b'b', b'c', b'd']);

        let mut back = out;
        let f = take_frame(&mut back, DEFAULT_MAX_REQUEST_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(&f[..], b"abcd");
    }

    #[test]
    fn negotiate_matches_what_we_advertise() {
        assert_eq!(negotiate(18, 0), Negotiation::Supported);
        assert_eq!(negotiate(18, 4), Negotiation::Supported);
        assert_eq!(negotiate(3, 13), Negotiation::Supported);
        assert_eq!(negotiate(0, 9), Negotiation::Supported);
        assert_eq!(negotiate(1, 12), Negotiation::Supported);
        assert_eq!(negotiate(2, 5), Negotiation::Supported);

        assert_eq!(negotiate(11, 0), Negotiation::Supported); // JoinGroup
        assert_eq!(negotiate(15, 0), Negotiation::Supported); // DescribeGroups

        assert_eq!(negotiate(31337, 0), Negotiation::UnknownApi);
        assert_eq!(negotiate(22, 4), Negotiation::Supported); // InitProducerId
        assert_eq!(negotiate(22, 6), Negotiation::UnsupportedVersion);
        assert_eq!(negotiate(19, 2), Negotiation::Supported); // CreateTopics
        assert_eq!(negotiate(60, 0), Negotiation::Supported); // DescribeCluster
        assert_eq!(negotiate(19, 1), Negotiation::UnsupportedVersion);
        assert_eq!(negotiate(23, 2), Negotiation::Supported);
        assert_eq!(negotiate(29, 0), Negotiation::UnsupportedVersion);
    }

    #[test]
    fn produce_advertises_below_what_it_accepts() {
        let adv = ADVERTISED.iter().find(|a| a.api_key == 0).unwrap();
        assert_eq!(adv.min_version, 0, "librdkafka must see 0");

        let spec = SCHEMA_APIS.iter().find(|a| a.key == 0).unwrap();
        assert_eq!(spec.valid.min, 3, "but no v0 Produce frame exists");

        for v in 0..3 {
            assert_eq!(
                negotiate(0, v),
                Negotiation::UnsupportedVersion,
                "Produce v{v} is advertised but must not be accepted"
            );
        }
        assert_eq!(negotiate(0, 3), Negotiation::Supported);
    }

    /// The discovery deadlock: a newer client sends ApiVersions v9, which we do not
    #[test]
    fn api_versions_above_our_max_falls_back_rather_than_failing() {
        assert_eq!(negotiate(18, 9), Negotiation::ApiVersionsFallback);
        assert_eq!(
            negotiate(18, 9).error_code(),
            Some(ErrorCode::UnsupportedVersion)
        );
    }

    /// Every advertised range must be a range we will actually accept; drift presents to
    #[test]
    fn everything_advertised_is_accepted() {
        for adv in ADVERTISED {
            let spec = SCHEMA_APIS.iter().find(|a| a.key == adv.api_key).unwrap();
            for v in adv.min_version..=adv.max_version {
                // Produce below its schema baseline is the one sanctioned exception.
                if !spec.valid.contains(v) {
                    assert_eq!(
                        adv.api_key, 0,
                        "only Produce may advertise below its schema"
                    );
                    continue;
                }
                assert_eq!(
                    negotiate(adv.api_key, v),
                    Negotiation::Supported,
                    "advertised api {} v{v} is not accepted",
                    adv.api_key
                );
            }
        }
    }
}
