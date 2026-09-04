//! Wire primitives. Every variable-length type has two encodings: the legacy one

use crate::errors::CodecError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Kafka's UUID is 16 raw bytes, big-endian, with all-zero as the "no uuid" sentinel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const ZERO: Uuid = Uuid([0u8; 16]);
}

/// A tagged field we did not recognise, kept verbatim so decode stays forward-compatible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTaggedField {
    pub tag: u32,
    pub data: Bytes,
}

/// Upper bound on any single length prefix: a cheap guard against a hostile frame
const MAX_LENGTH: i64 = 100 * 1024 * 1024;

/// Cap on speculative `Vec::with_capacity` from an attacker-supplied element count; the
const MAX_PREALLOC: usize = 1024;

pub fn alloc_vec<T>(len: usize) -> Vec<T> {
    Vec::with_capacity(len.min(MAX_PREALLOC))
}

fn need(buf: &Bytes, n: usize) -> Result<(), CodecError> {
    if buf.remaining() < n {
        Err(CodecError::Truncated {
            needed: n,
            available: buf.remaining(),
        })
    } else {
        Ok(())
    }
}

/// Split `n` bytes off the front. The result shares the underlying allocation — the
pub fn take(buf: &mut Bytes, n: usize) -> Result<Bytes, CodecError> {
    need(buf, n)?;
    Ok(buf.split_to(n))
}

macro_rules! read_prim {
    ($name:ident, $ty:ty, $n:expr, $get:ident) => {
        pub fn $name(buf: &mut Bytes) -> Result<$ty, CodecError> {
            need(buf, $n)?;
            Ok(buf.$get())
        }
    };
}

read_prim!(get_i8, i8, 1, get_i8);
read_prim!(get_i16, i16, 2, get_i16);
read_prim!(get_i32, i32, 4, get_i32);
read_prim!(get_i64, i64, 8, get_i64);
read_prim!(get_u16, u16, 2, get_u16);
read_prim!(get_u32, u32, 4, get_u32);
read_prim!(get_f64, f64, 8, get_f64);

pub fn get_bool(buf: &mut Bytes) -> Result<bool, CodecError> {
    Ok(get_i8(buf)? != 0)
}

pub fn get_uuid(buf: &mut Bytes) -> Result<Uuid, CodecError> {
    need(buf, 16)?;
    let mut u = [0u8; 16];
    buf.copy_to_slice(&mut u);
    Ok(Uuid(u))
}

/// Unsigned LEB128, capped at 5 bytes as Kafka does.
pub fn get_uvarint(buf: &mut Bytes) -> Result<u32, CodecError> {
    let mut value: u32 = 0;
    for i in 0..5 {
        let b = get_i8(buf)? as u8;
        value |= ((b & 0x7f) as u32) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(CodecError::MalformedVarint)
}

/// Length prefix for strings/bytes/arrays. `None` means null.
fn get_len(buf: &mut Bytes, flexible: bool, wide: bool) -> Result<Option<usize>, CodecError> {
    let raw: i64 = if flexible {
        // Compact: varint of len+1, 0 == null.
        let v = get_uvarint(buf)?;
        if v == 0 {
            return Ok(None);
        }
        (v - 1) as i64
    } else if wide {
        get_i32(buf)? as i64
    } else {
        get_i16(buf)? as i64
    };
    if raw < 0 {
        return Ok(None);
    }
    if raw > MAX_LENGTH {
        return Err(CodecError::InvalidLength(raw));
    }
    Ok(Some(raw as usize))
}

pub fn get_string(buf: &mut Bytes, flexible: bool) -> Result<String, CodecError> {
    match get_nullable_string(buf, flexible)? {
        Some(s) => Ok(s),
        None => Err(CodecError::UnexpectedNull("string")),
    }
}

pub fn get_nullable_string(buf: &mut Bytes, flexible: bool) -> Result<Option<String>, CodecError> {
    match get_len(buf, flexible, false)? {
        None => Ok(None),
        Some(n) => {
            let raw = take(buf, n)?;
            String::from_utf8(raw.to_vec())
                .map(Some)
                .map_err(|_| CodecError::InvalidUtf8)
        }
    }
}

pub fn get_bytes(buf: &mut Bytes, flexible: bool) -> Result<Bytes, CodecError> {
    match get_nullable_bytes(buf, flexible)? {
        Some(b) => Ok(b),
        None => Err(CodecError::UnexpectedNull("bytes")),
    }
}

pub fn get_nullable_bytes(buf: &mut Bytes, flexible: bool) -> Result<Option<Bytes>, CodecError> {
    match get_len(buf, flexible, true)? {
        None => Ok(None),
        Some(n) => Ok(Some(take(buf, n)?)),
    }
}

/// Array element count. `None` means a null array.
pub fn get_array_len(buf: &mut Bytes, flexible: bool) -> Result<Option<usize>, CodecError> {
    get_len(buf, flexible, true)
}

pub fn put_bool(buf: &mut BytesMut, v: bool) {
    buf.put_i8(if v { 1 } else { 0 });
}
pub fn put_i8(buf: &mut BytesMut, v: i8) {
    buf.put_i8(v);
}
pub fn put_i16(buf: &mut BytesMut, v: i16) {
    buf.put_i16(v);
}
pub fn put_i32(buf: &mut BytesMut, v: i32) {
    buf.put_i32(v);
}
pub fn put_i64(buf: &mut BytesMut, v: i64) {
    buf.put_i64(v);
}
pub fn put_u16(buf: &mut BytesMut, v: u16) {
    buf.put_u16(v);
}
pub fn put_u32(buf: &mut BytesMut, v: u32) {
    buf.put_u32(v);
}
pub fn put_f64(buf: &mut BytesMut, v: f64) {
    buf.put_f64(v);
}
pub fn put_uuid(buf: &mut BytesMut, v: &Uuid) {
    buf.put_slice(&v.0);
}

pub fn put_uvarint(buf: &mut BytesMut, mut v: u32) {
    loop {
        if v < 0x80 {
            buf.put_u8(v as u8);
            return;
        }
        buf.put_u8(((v & 0x7f) | 0x80) as u8);
        v >>= 7;
    }
}

pub fn uvarint_size(mut v: u32) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

fn put_len(buf: &mut BytesMut, len: usize, flexible: bool, wide: bool) -> Result<(), CodecError> {
    if len as i64 > MAX_LENGTH {
        return Err(CodecError::InvalidLength(len as i64));
    }
    if flexible {
        put_uvarint(buf, len as u32 + 1);
    } else if wide {
        buf.put_i32(len as i32);
    } else {
        if len > i16::MAX as usize {
            return Err(CodecError::InvalidLength(len as i64));
        }
        buf.put_i16(len as i16);
    }
    Ok(())
}

fn len_size(len: usize, flexible: bool, wide: bool) -> usize {
    if flexible {
        uvarint_size(len as u32 + 1)
    } else if wide {
        4
    } else {
        2
    }
}

fn null_len_size(flexible: bool, wide: bool) -> usize {
    if flexible {
        1
    } else if wide {
        4
    } else {
        2
    }
}

fn put_null_len(buf: &mut BytesMut, flexible: bool, wide: bool) {
    if flexible {
        put_uvarint(buf, 0);
    } else if wide {
        buf.put_i32(-1);
    } else {
        buf.put_i16(-1);
    }
}

pub fn put_string(buf: &mut BytesMut, v: &str, flexible: bool) -> Result<(), CodecError> {
    put_len(buf, v.len(), flexible, false)?;
    buf.put_slice(v.as_bytes());
    Ok(())
}

pub fn put_nullable_string(
    buf: &mut BytesMut,
    v: Option<&str>,
    flexible: bool,
) -> Result<(), CodecError> {
    match v {
        None => {
            put_null_len(buf, flexible, false);
            Ok(())
        }
        Some(s) => put_string(buf, s, flexible),
    }
}

pub fn put_bytes(buf: &mut BytesMut, v: &Bytes, flexible: bool) -> Result<(), CodecError> {
    put_len(buf, v.len(), flexible, true)?;
    buf.put_slice(v);
    Ok(())
}

pub fn put_nullable_bytes(
    buf: &mut BytesMut,
    v: Option<&Bytes>,
    flexible: bool,
) -> Result<(), CodecError> {
    match v {
        None => {
            put_null_len(buf, flexible, true);
            Ok(())
        }
        Some(b) => put_bytes(buf, b, flexible),
    }
}

pub fn put_array_len(buf: &mut BytesMut, len: usize, flexible: bool) -> Result<(), CodecError> {
    put_len(buf, len, flexible, true)
}

pub fn put_null_array(buf: &mut BytesMut, flexible: bool) {
    put_null_len(buf, flexible, true);
}

pub fn string_size(v: &str, flexible: bool) -> usize {
    len_size(v.len(), flexible, false) + v.len()
}

pub fn nullable_string_size(v: Option<&str>, flexible: bool) -> usize {
    match v {
        None => null_len_size(flexible, false),
        Some(s) => string_size(s, flexible),
    }
}

pub fn bytes_size(v: &Bytes, flexible: bool) -> usize {
    len_size(v.len(), flexible, true) + v.len()
}

pub fn nullable_bytes_size(v: Option<&Bytes>, flexible: bool) -> usize {
    match v {
        None => null_len_size(flexible, true),
        Some(b) => bytes_size(b, flexible),
    }
}

pub fn array_len_size(len: usize, flexible: bool) -> usize {
    len_size(len, flexible, true)
}

pub fn null_array_size(flexible: bool) -> usize {
    null_len_size(flexible, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_uvarint(v: u32) {
        let mut b = BytesMut::new();
        put_uvarint(&mut b, v);
        assert_eq!(b.len(), uvarint_size(v), "size disagreed for {v}");
        let mut r = b.freeze();
        assert_eq!(get_uvarint(&mut r).unwrap(), v);
        assert!(r.is_empty());
    }

    #[test]
    fn uvarint_roundtrips_at_boundaries() {
        for v in [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, u32::MAX] {
            roundtrip_uvarint(v);
        }
    }

    #[test]
    fn compact_null_is_zero_legacy_is_minus_one() {
        let mut b = BytesMut::new();
        put_nullable_string(&mut b, None, true).unwrap();
        assert_eq!(&b[..], &[0u8]);

        let mut b = BytesMut::new();
        put_nullable_string(&mut b, None, false).unwrap();
        assert_eq!(&b[..], &[0xff, 0xff]);
    }

    #[test]
    fn empty_compact_string_is_one() {
        let mut b = BytesMut::new();
        put_nullable_string(&mut b, Some(""), true).unwrap();
        assert_eq!(&b[..], &[1u8]);
        let mut r = b.freeze();
        assert_eq!(
            get_nullable_string(&mut r, true).unwrap(),
            Some(String::new())
        );
    }

    #[test]
    fn strings_roundtrip_in_both_encodings() {
        for flexible in [false, true] {
            let mut b = BytesMut::new();
            put_string(&mut b, "hello", flexible).unwrap();
            assert_eq!(b.len(), string_size("hello", flexible));
            let mut r = b.freeze();
            assert_eq!(get_string(&mut r, flexible).unwrap(), "hello");
        }
    }

    #[test]
    fn take_is_zero_copy() {
        let src = Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8]);
        let mut buf = src.clone();
        let a = take(&mut buf, 4).unwrap();
        // Same allocation, not a copy.
        assert_eq!(a.as_ptr(), src.as_ptr());
        assert_eq!(&a[..], &[1, 2, 3, 4]);
        assert_eq!(&buf[..], &[5, 6, 7, 8]);
    }

    #[test]
    fn truncated_reads_report_rather_than_panic() {
        let mut r = Bytes::from(vec![0u8, 1]);
        assert!(matches!(
            get_i32(&mut r),
            Err(CodecError::Truncated {
                needed: 4,
                available: 2
            })
        ));
    }

    #[test]
    fn oversized_length_is_rejected() {
        let mut b = BytesMut::new();
        b.put_i32(i32::MAX);
        let mut r = b.freeze();
        assert!(matches!(
            get_nullable_bytes(&mut r, false),
            Err(CodecError::InvalidLength(_))
        ));
    }

    #[test]
    fn prealloc_is_capped() {
        let v: Vec<u8> = alloc_vec(usize::MAX);
        assert!(v.capacity() <= MAX_PREALLOC);
    }
}
