//! Round-trips every vendored schema, with no running Postgres. Lives inside the crate so

#![cfg(test)]

use crate::generated::apis::{ADVERTISED, SCHEMA_APIS};
use crate::prelude::*;
use crate::sample::{Gen, Sample};
use std::fmt::Debug;

/// Seeds per (message, version); enough that optional fields get exercised in both states.
const SEEDS: u64 = 12;

/// Encode → decode → compare, plus the size-agreement check, for one message type
fn exercise<T>()
where
    T: ApiMessage + Sample + PartialEq + Debug,
{
    // Clamp anyway so a malformed schema cannot turn this into a 32767-iteration loop.
    let max = T::VALID.max.min(T::VALID.min + 64);
    for version in T::VALID.min..=max {
        for seed in 0..SEEDS {
            let value = T::sample(version, &mut Gen::new(seed.wrapping_mul(0x9E37_79B9) | 1));

            // `size` must agree with `encode` exactly: the Fetch path uses it to enforce
            let predicted = value.size(version);

            let mut buf = BytesMut::new();
            value
                .encode(&mut buf, version)
                .unwrap_or_else(|e| panic!("{} v{version} seed {seed}: encode: {e}", T::NAME));
            assert_eq!(
                buf.len(),
                predicted,
                "{} v{version} seed {seed}: size() said {predicted}, encode wrote {}",
                T::NAME,
                buf.len()
            );

            let mut read = buf.freeze();
            let decoded = T::decode(&mut read, version)
                .unwrap_or_else(|e| panic!("{} v{version} seed {seed}: decode: {e}", T::NAME));
            assert!(
                read.is_empty(),
                "{} v{version} seed {seed}: {} bytes left after decode",
                T::NAME,
                read.len()
            );
            assert_eq!(
                decoded,
                value,
                "{} v{version} seed {seed}: round-trip changed the value",
                T::NAME
            );
        }
    }
}

#[test]
fn every_schema_roundtrips() {
    let mut covered = 0usize;
    macro_rules! check {
        ($t:ty) => {
            exercise::<$t>();
            covered += 1;
        };
    }
    crate::for_each_message!(check);

    // Guards against the registry macro expanding to nothing after a broken generator run.
    assert_eq!(
        covered,
        SCHEMA_APIS.len() * 2,
        "expected one request and one response per live API"
    );
    assert!(
        covered >= 178,
        "coverage went backwards: {covered} messages"
    );
}

/// Forward compatibility is the entire purpose of the tagged-field section: a broker
#[test]
fn unknown_tagged_fields_survive_decode() {
    use crate::generated::api_versions_response::ApiVersionsResponse;

    let original = ApiVersionsResponse {
        unknown_tagged_fields: vec![
            RawTaggedField {
                tag: 500,
                data: Bytes::from_static(b"\x01\x02"),
            },
            RawTaggedField {
                tag: 9999,
                data: Bytes::from_static(b"future"),
            },
        ],
        ..Default::default()
    };

    // v3 is flexible; v0 is not and has no tagged section at all.
    let mut buf = BytesMut::new();
    original.encode(&mut buf, 3).unwrap();
    assert_eq!(buf.len(), original.size(3));

    let mut read = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read, 3).unwrap();
    assert_eq!(
        decoded.unknown_tagged_fields,
        original.unknown_tagged_fields
    );
    assert!(read.is_empty());
}

/// Known and unknown tags share one ascending-ordered section; a descending sequence is
#[test]
fn known_and_unknown_tags_interleave_in_ascending_order() {
    use crate::generated::api_versions_response::ApiVersionsResponse;

    // ApiVersionsResponse defines tags 0..3; bracket them with unknown tags.
    let msg = ApiVersionsResponse {
        finalized_features_epoch: 77, // tag 1
        unknown_tagged_fields: vec![
            RawTaggedField {
                tag: 2000,
                data: Bytes::from_static(b"a"),
            },
            RawTaggedField {
                tag: 3000,
                data: Bytes::from_static(b"b"),
            },
        ],
        ..Default::default()
    };

    let mut buf = BytesMut::new();
    msg.encode(&mut buf, 3).unwrap();
    let mut read = buf.freeze();
    // Decode rejects a descending sequence, so a successful decode proves the ordering.
    let back = ApiVersionsResponse::decode(&mut read, 3).unwrap();
    assert_eq!(back.finalized_features_epoch, 77);
    assert_eq!(back.unknown_tagged_fields, msg.unknown_tagged_fields);
}

#[test]
fn out_of_order_tags_are_rejected() {
    let mut buf = BytesMut::new();
    put_uvarint(&mut buf, 2); // two tagged fields
    put_uvarint(&mut buf, 5);
    put_uvarint(&mut buf, 0);
    put_uvarint(&mut buf, 5); // same tag again — not ascending
    put_uvarint(&mut buf, 0);

    use crate::generated::api_versions_response::ApiVersionsResponse;
    let mut frame = BytesMut::new();
    put_i16(&mut frame, 0);
    put_uvarint(&mut frame, 1); // empty compact array
    put_i32(&mut frame, 0);
    frame.extend_from_slice(&buf);

    let mut read = frame.freeze();
    assert!(matches!(
        ApiVersionsResponse::decode(&mut read, 3),
        Err(CodecError::TagOutOfOrder { .. })
    ));
}

/// A truncated frame must be an error, never a panic — attacker-reachable on byte one.
#[test]
fn truncated_frames_error_rather_than_panic() {
    use crate::generated::metadata_request::MetadataRequest;

    let full = MetadataRequest {
        topics: Some(Vec::new()),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    full.encode(&mut buf, 12).unwrap();
    let bytes = buf.freeze();

    for cut in 0..bytes.len() {
        let mut partial = bytes.slice(..cut);
        // Must not panic. Success is acceptable for prefixes that happen to be complete.
        let _ = MetadataRequest::decode(&mut partial, 12);
    }
}

#[test]
fn schema_table_matches_the_vendored_pin() {
    let produce = SCHEMA_APIS.iter().find(|a| a.key == 0).unwrap();
    assert_eq!(produce.name, "Produce");
    assert_eq!(produce.valid, VersionRange::new(3, 13));
    assert!(produce.is_flexible(9));
    assert!(!produce.is_flexible(8));

    let init_pid = SCHEMA_APIS.iter().find(|a| a.key == 22).unwrap();
    assert_eq!(init_pid.name, "InitProducerId");
    assert!(
        init_pid.latest_version_unstable,
        "InitProducerId v6 is unstable at 4.3.1; the advertised max must stay 5"
    );
    let advertised_pid = ADVERTISED.iter().find(|a| a.api_key == 22).unwrap();
    assert_eq!(
        advertised_pid.max_version, 5,
        "the unstable top version must never be advertised"
    );

    // APIs removed in Kafka 4.0 must not be in the table at all.
    for removed in [4i16, 5, 6, 7] {
        assert!(
            SCHEMA_APIS.iter().all(|a| a.key != removed),
            "api {removed} was removed in Kafka 4.0 and must not be generated"
        );
    }
}

/// Advertising an API with no handler is the "hang, not an error" failure mode: the
#[test]
fn advertised_set_is_exactly_the_implemented_handlers() {
    let mut keys: Vec<i16> = ADVERTISED.iter().map(|a| a.api_key).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31, 32, 33, 35, 36, 37, 42, 43, 44, 46, 47, 48, 49, 50, 51, 60,
            61, 65, 66, 68, 69, 75, 76, 77, 78, 79,
        ],
        "phase 5 adds the admin tier: 19 CreateTopics, 20 DeleteTopics, 21 DeleteRecords, \
         32 DescribeConfigs, 37 CreatePartitions, 42 DeleteGroups, 44 IncrementalAlterConfigs, \
         60 DescribeCluster, SASL's 17 and 36, and phase 6's 23 OffsetForLeaderEpoch. Phase 9 \
         adds the transaction tier — 24, 25, 26, 28 — plus 47 OffsetDelete and the ACL tier \
         29/30/31. The client-conformance tier adds 33 AlterConfigs, 35 DescribeLogDirs, \
         46 ListPartitionReassignments, 48 DescribeClientQuotas, 61 DescribeProducers, \
         65 DescribeTransactions and 66 ListTransactions — every one of them answering a \
         question the broker could already answer and was telling clients it could not. \
         Phase 11 adds KIP-848's 68/69, and phase 12 adds 27 WriteTxnMarkers: the other \
         end of 61 and 65, which let an operator *find* a hanging transaction and gave \
         them no way to act on it"
    );

    // Produce advertises below its schema baseline of 3 on purpose (KAFKA-18659).
    let produce = ADVERTISED.iter().find(|a| a.api_key == 0).unwrap();
    assert_eq!((produce.min_version, produce.max_version), (0, 13));

    let api_versions = ADVERTISED.iter().find(|a| a.api_key == 18).unwrap();
    assert_eq!((api_versions.min_version, api_versions.max_version), (0, 4));
    let metadata = ADVERTISED.iter().find(|a| a.api_key == 3).unwrap();
    assert_eq!((metadata.min_version, metadata.max_version), (0, 13));
}

/// Whatever is advertised must be inside what the schemas define, and must never be an
#[test]
fn advertised_ranges_stay_within_the_schemas() {
    for adv in ADVERTISED {
        let spec = SCHEMA_APIS
            .iter()
            .find(|a| a.key == adv.api_key)
            .unwrap_or_else(|| panic!("advertised api {} has no schema", adv.api_key));
        assert!(
            adv.max_version <= spec.valid.max,
            "{}: advertised max {} exceeds schema max {}",
            spec.name,
            adv.max_version,
            spec.valid.max
        );
        if spec.latest_version_unstable {
            assert!(
                adv.max_version < spec.valid.max,
                "{}: must not advertise its unstable latest version",
                spec.name
            );
        }
        // Produce is the one API allowed to advertise below its own validVersions.
        if adv.api_key != 0 {
            assert!(
                adv.min_version >= spec.valid.min,
                "{}: advertised min {} is below schema min {}",
                spec.name,
                adv.min_version,
                spec.valid.min
            );
        }
    }
}
