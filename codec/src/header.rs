//! Request and response headers. Hand-written: the header version is chosen per

use crate::errors::CodecError;
use crate::generated::apis::SCHEMA_APIS;
use crate::primitives::*;
use bytes::{Bytes, BytesMut};

pub const API_VERSIONS_KEY: i16 = 18;

/// Whether `version` of `api_key` is flexible, per the vendored schemas. Unknown API keys
pub fn is_flexible(api_key: i16, version: i16) -> Result<bool, CodecError> {
    let spec = SCHEMA_APIS
        .iter()
        .find(|a| a.key == api_key)
        .ok_or(CodecError::UnknownApiKey(api_key))?;
    Ok(spec.is_flexible(version))
}

/// Request header version: 2 when the body version is flexible, else 1. Version 0 was
pub fn request_header_version(api_key: i16, version: i16) -> Result<i16, CodecError> {
    Ok(if is_flexible(api_key, version)? { 2 } else { 1 })
}

/// Response header version: 1 when the body version is flexible, else 0 — **except** for
pub fn response_header_version(api_key: i16, version: i16) -> Result<i16, CodecError> {
    if api_key == API_VERSIONS_KEY {
        return Ok(0);
    }
    Ok(if is_flexible(api_key, version)? { 1 } else { 0 })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl RequestHeader {
    pub fn size(&self, header_version: i16) -> usize {
        let mut n = 2 + 2 + 4;
        if header_version >= 1 {
            // Always the legacy two-byte-length form — see encode().
            n += nullable_string_size(self.client_id.as_deref(), false);
        }
        if header_version >= 2 {
            n += uvarint_size(self.unknown_tagged_fields.len() as u32);
            for f in &self.unknown_tagged_fields {
                n += uvarint_size(f.tag) + uvarint_size(f.data.len() as u32) + f.data.len();
            }
        }
        n
    }

    pub fn encode(&self, buf: &mut BytesMut, header_version: i16) -> Result<(), CodecError> {
        put_i16(buf, self.api_key);
        put_i16(buf, self.api_version);
        put_i32(buf, self.correlation_id);
        if header_version >= 1 {
            put_nullable_string(buf, self.client_id.as_deref(), false)?;
        }
        if header_version >= 2 {
            put_uvarint(buf, self.unknown_tagged_fields.len() as u32);
            for f in &self.unknown_tagged_fields {
                put_uvarint(buf, f.tag);
                put_uvarint(buf, f.data.len() as u32);
                buf.extend_from_slice(&f.data);
            }
        }
        Ok(())
    }

    pub fn decode(buf: &mut Bytes, header_version: i16) -> Result<Self, CodecError> {
        let api_key = get_i16(buf)?;
        let api_version = get_i16(buf)?;
        let correlation_id = get_i32(buf)?;
        let client_id = if header_version >= 1 {
            get_nullable_string(buf, false)?
        } else {
            None
        };
        let mut unknown_tagged_fields = Vec::new();
        if header_version >= 2 {
            unknown_tagged_fields = decode_tagged(buf)?;
        }
        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
            unknown_tagged_fields,
        })
    }

    /// Peek the api key and version without consuming, so a dispatcher can pick the header
    pub fn peek_api(buf: &Bytes) -> Result<(i16, i16), CodecError> {
        if buf.len() < 4 {
            return Err(CodecError::Truncated {
                needed: 4,
                available: buf.len(),
            });
        }
        let key = i16::from_be_bytes([buf[0], buf[1]]);
        let version = i16::from_be_bytes([buf[2], buf[3]]);
        Ok((key, version))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseHeader {
    pub correlation_id: i32,
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl ResponseHeader {
    pub fn size(&self, header_version: i16) -> usize {
        let mut n = 4;
        if header_version >= 1 {
            n += uvarint_size(self.unknown_tagged_fields.len() as u32);
            for f in &self.unknown_tagged_fields {
                n += uvarint_size(f.tag) + uvarint_size(f.data.len() as u32) + f.data.len();
            }
        }
        n
    }

    pub fn encode(&self, buf: &mut BytesMut, header_version: i16) -> Result<(), CodecError> {
        put_i32(buf, self.correlation_id);
        if header_version >= 1 {
            put_uvarint(buf, self.unknown_tagged_fields.len() as u32);
            for f in &self.unknown_tagged_fields {
                put_uvarint(buf, f.tag);
                put_uvarint(buf, f.data.len() as u32);
                buf.extend_from_slice(&f.data);
            }
        }
        Ok(())
    }

    pub fn decode(buf: &mut Bytes, header_version: i16) -> Result<Self, CodecError> {
        let correlation_id = get_i32(buf)?;
        let mut unknown_tagged_fields = Vec::new();
        if header_version >= 1 {
            unknown_tagged_fields = decode_tagged(buf)?;
        }
        Ok(ResponseHeader {
            correlation_id,
            unknown_tagged_fields,
        })
    }
}

fn decode_tagged(buf: &mut Bytes) -> Result<Vec<RawTaggedField>, CodecError> {
    let n = get_uvarint(buf)?;
    let mut out = alloc_vec(n as usize);
    let mut last: Option<u32> = None;
    for _ in 0..n {
        let tag = get_uvarint(buf)?;
        if let Some(l) = last {
            if tag <= l {
                return Err(CodecError::TagOutOfOrder { previous: l, tag });
            }
        }
        last = Some(tag);
        let sz = get_uvarint(buf)? as usize;
        out.push(RawTaggedField {
            tag,
            data: take(buf, sz)?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_response_header_is_always_v0() {
        // Every version, flexible or not.
        for v in 0..=4 {
            assert_eq!(response_header_version(API_VERSIONS_KEY, v).unwrap(), 0);
        }
    }

    #[test]
    fn api_versions_request_header_still_follows_the_normal_rule() {
        // Asymmetry: a v3 request is flexible (header v2) while the response header stays v0.
        assert_eq!(request_header_version(API_VERSIONS_KEY, 0).unwrap(), 1);
        assert_eq!(request_header_version(API_VERSIONS_KEY, 3).unwrap(), 2);
        assert_eq!(response_header_version(API_VERSIONS_KEY, 3).unwrap(), 0);
    }

    #[test]
    fn api_versions_v3_response_header_is_exactly_four_bytes() {
        let h = ResponseHeader {
            correlation_id: 0x11223344,
            unknown_tagged_fields: Vec::new(),
        };
        let hv = response_header_version(API_VERSIONS_KEY, 3).unwrap();
        let mut buf = BytesMut::new();
        h.encode(&mut buf, hv).unwrap();
        // A v1 header would add a tagged-field count byte and make this 5.
        assert_eq!(
            buf.len(),
            4,
            "ApiVersions v3 response header must carry no tagged section"
        );
        assert_eq!(&buf[..], &[0x11, 0x22, 0x33, 0x44]);
        assert_eq!(h.size(hv), 4);
    }

    #[test]
    fn header_versions_match_the_rule_for_every_api_and_version() {
        for spec in SCHEMA_APIS {
            if spec.valid.min > spec.valid.max {
                continue; // removed API, no valid versions
            }
            for v in spec.valid.min..=spec.valid.max {
                let flexible = spec.is_flexible(v);
                assert_eq!(
                    request_header_version(spec.key, v).unwrap(),
                    if flexible { 2 } else { 1 },
                    "request header for {} v{v}",
                    spec.name
                );
                let expected_resp = if spec.key == API_VERSIONS_KEY {
                    0
                } else if flexible {
                    1
                } else {
                    0
                };
                assert_eq!(
                    response_header_version(spec.key, v).unwrap(),
                    expected_resp,
                    "response header for {} v{v}",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn client_id_is_not_compact_even_in_a_flexible_header() {
        let h = RequestHeader {
            api_key: 18,
            api_version: 3,
            correlation_id: 1,
            client_id: Some("kc".to_string()),
            unknown_tagged_fields: Vec::new(),
        };
        let mut buf = BytesMut::new();
        h.encode(&mut buf, 2).unwrap();
        // key(2) version(2) correlation(4) then a *two-byte* length of 2, "kc", then
        assert_eq!(&buf[..], &[0, 18, 0, 3, 0, 0, 0, 1, 0, 2, b'k', b'c', 0]);
        assert_eq!(h.size(2), buf.len());
    }

    #[test]
    fn headers_roundtrip_at_every_header_version() {
        for hv in [1i16, 2] {
            let h = RequestHeader {
                api_key: 3,
                api_version: 9,
                correlation_id: -7,
                client_id: Some("client".into()),
                unknown_tagged_fields: Vec::new(),
            };
            let mut buf = BytesMut::new();
            h.encode(&mut buf, hv).unwrap();
            assert_eq!(buf.len(), h.size(hv));
            let mut r = buf.freeze();
            assert_eq!(RequestHeader::decode(&mut r, hv).unwrap(), h);
            assert!(r.is_empty());
        }
        for hv in [0i16, 1] {
            let h = ResponseHeader {
                correlation_id: 99,
                unknown_tagged_fields: Vec::new(),
            };
            let mut buf = BytesMut::new();
            h.encode(&mut buf, hv).unwrap();
            assert_eq!(buf.len(), h.size(hv));
            let mut r = buf.freeze();
            assert_eq!(ResponseHeader::decode(&mut r, hv).unwrap(), h);
        }
    }

    #[test]
    fn null_client_id_roundtrips() {
        let h = RequestHeader {
            api_key: 0,
            api_version: 9,
            correlation_id: 5,
            client_id: None,
            unknown_tagged_fields: Vec::new(),
        };
        let mut buf = BytesMut::new();
        h.encode(&mut buf, 2).unwrap();
        let mut r = buf.freeze();
        assert_eq!(RequestHeader::decode(&mut r, 2).unwrap(), h);
    }

    #[test]
    fn peek_reads_key_and_version_without_consuming() {
        let buf = Bytes::from(vec![0, 18, 0, 3, 0, 0, 0, 1]);
        assert_eq!(RequestHeader::peek_api(&buf).unwrap(), (18, 3));
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn unknown_api_key_is_reported_not_guessed() {
        assert!(matches!(
            is_flexible(31337, 0),
            Err(CodecError::UnknownApiKey(31337))
        ));
    }
}
