//! Follower replication over the public Fetch protocol; segment engine only — the table engine's log is already replicated by WAL streaming.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use pgrx::prelude::*;

use kafgres_codec::bytes::{Bytes, BytesMut};
use kafgres_codec::framing;
use kafgres_codec::generated::fetch_request::{
    FetchPartition, FetchRequest, FetchTopic,
};
use kafgres_codec::generated::fetch_response::FetchResponse;
use kafgres_codec::generated::offset_for_leader_epoch_request::{
    OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use kafgres_codec::generated::offset_for_leader_epoch_response::OffsetForLeaderEpochResponse;
use kafgres_codec::header::{RequestHeader, ResponseHeader};
use kafgres_codec::records::RecordBatch;
use kafgres_codec::Encodable;

/// v11: the last non-flexible Fetch version, so headers stay v1/v0 and there is no tagged-field handling to get wrong.
const FETCH_VERSION: i16 = 11;

const MAX_PARTITION_BYTES: i32 = 4 * 1024 * 1024;

const IO_TIMEOUT: Duration = Duration::from_secs(10);

fn round_trip<T, F>(
    host: &str,
    port: i32,
    api_key: i16,
    version: i16,
    header_version: i16,
    encode: F,
    decode: impl FnOnce(&mut Bytes, i16) -> Result<T, kafgres_codec::errors::CodecError>,
) -> Result<T, String>
where
    F: FnOnce(&mut BytesMut) -> Result<(), kafgres_codec::errors::CodecError>,
{
    let header = RequestHeader {
        api_key,
        api_version: version,
        correlation_id: 1,
        client_id: Some("kafgres-follower".to_string()),
        unknown_tagged_fields: Vec::new(),
    };
    let mut out = BytesMut::new();
    framing::write_frame(&mut out, |buf| {
        header.encode(buf, header_version)?;
        encode(buf)
    })
    .map_err(|e| io_err("encoding the request", e))?;

    let mut sock = TcpStream::connect((host, port as u16))
        .map_err(|e| io_err(&format!("connecting to {host}:{port}"), e))?;
    sock.set_read_timeout(Some(IO_TIMEOUT)).ok();
    sock.set_write_timeout(Some(IO_TIMEOUT)).ok();
    sock.set_nodelay(true).ok();
    sock.write_all(&out).map_err(|e| io_err("sending the request", e))?;

    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).map_err(|e| io_err("reading the reply length", e))?;
    let len = i32::from_be_bytes(len_buf);
    if len <= 0 || len as usize > 64 * 1024 * 1024 {
        return Err(format!("replication: implausible reply length {len}"));
    }
    let mut body = vec![0u8; len as usize];
    sock.read_exact(&mut body).map_err(|e| io_err("reading the reply", e))?;

    let mut buf = Bytes::from(body);
    let resp_header_version = if header_version >= 2 { 1 } else { 0 };
    ResponseHeader::decode(&mut buf, resp_header_version)
        .map_err(|e| io_err("decoding the reply header", e))?;
    decode(&mut buf, version).map_err(|e| io_err("decoding the reply", e))
}

/// Ask the leader where our last epoch ended and truncate if we went further: two nodes
pub fn reconcile_epoch(
    store: &mut dyn crate::storage::LogStore,
    host: &str,
    port: i32,
    want: &[(crate::storage::TopicId, String, i32, i64, i32)],
) -> Result<i64, String> {
    let mut topics: Vec<OffsetForLeaderTopic> = Vec::new();
    for (_topic_id, name, partition, _, epoch) in want {
        let entry = match topics.iter_mut().find(|t| t.topic == *name) {
            Some(t) => t,
            None => {
                topics.push(OffsetForLeaderTopic {
                    topic: name.clone(),
                    partitions: Vec::new(),
                    ..Default::default()
                });
                topics.last_mut().expect("just pushed")
            }
        };
        entry.partitions.push(OffsetForLeaderPartition {
            partition: *partition,
            leader_epoch: *epoch,
            current_leader_epoch: -1,
            ..Default::default()
        });
    }

    let request = OffsetForLeaderEpochRequest {
        replica_id: -1,
        topics,
        ..Default::default()
    };
    let response: OffsetForLeaderEpochResponse = round_trip(
        host, port, 23, 3, 1,
        |buf| request.encode(buf, 3),
        OffsetForLeaderEpochResponse::decode,
    )?;

    let mut truncated = 0i64;
    for topic in response.topics {
        let name = topic.topic;
        let Some((topic_id, _, _, _, _)) = want.iter().find(|(_, n, _, _, _)| *n == name)
        else {
            continue;
        };
        for part in topic.partitions {
            if part.error_code != 0 || part.end_offset < 0 {
                // -1/-1: the leader never held that epoch — let the fetch surface it.
                continue;
            }
            let ours = store
                .high_watermark(*topic_id, part.partition)
                .map_err(|e| format!("replication: {e}"))?;
            if part.end_offset < ours {
                truncated += store
                    .truncate_to(*topic_id, part.partition, part.end_offset)
                    .map_err(|e| format!("replication: {e}"))?;
            }
        }
    }
    Ok(truncated)
}

pub struct Pulled {
    pub topic: crate::storage::TopicId,
    pub partition: i32,
    pub bytes: Bytes,
}

fn io_err(what: &str, e: impl std::fmt::Display) -> String {
    format!("replication: {what}: {e}")
}

pub fn fetch_from(
    host: &str,
    port: i32,
    want: &[(crate::storage::TopicId, String, i32, i64, i32)],
) -> Result<Vec<Pulled>, String> {
    if want.is_empty() {
        return Ok(Vec::new());
    }

    // The leader answers by topic name; storage is keyed by local oid, so carry the id to map back.
    let mut topics: Vec<FetchTopic> = Vec::new();
    for (_, name, partition, from, _) in want {
        let entry = match topics.iter_mut().find(|t| t.topic == *name) {
            Some(t) => t,
            None => {
                topics.push(FetchTopic {
                    topic: name.clone(),
                    partitions: Vec::new(),
                    ..Default::default()
                });
                topics.last_mut().expect("just pushed")
            }
        };
        entry.partitions.push(FetchPartition {
            partition: *partition,
            fetch_offset: *from,
            partition_max_bytes: MAX_PARTITION_BYTES,
            current_leader_epoch: -1,
            log_start_offset: -1,
            ..Default::default()
        });
    }

    let request = FetchRequest {
        // A consumer, not a replica: real Kafka rejects follower fetches from outside the
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: MAX_PARTITION_BYTES * 4,
        // `read_committed`, same argument as `replica_id`: copy only what has committed.
        isolation_level: 1,
        session_id: 0,
        session_epoch: -1,
        topics,
        ..Default::default()
    };

    let header = RequestHeader {
        api_key: 1,
        api_version: FETCH_VERSION,
        correlation_id: 1,
        client_id: Some("kafgres-follower".to_string()),
        unknown_tagged_fields: Vec::new(),
    };

    let mut out = BytesMut::new();
    framing::write_frame(&mut out, |buf| {
        header.encode(buf, 1)?;
        request.encode(buf, FETCH_VERSION)
    })
    .map_err(|e| io_err("encoding the fetch", e))?;

    let mut sock = TcpStream::connect((host, port as u16))
        .map_err(|e| io_err(&format!("connecting to {host}:{port}"), e))?;
    sock.set_read_timeout(Some(IO_TIMEOUT)).ok();
    sock.set_write_timeout(Some(IO_TIMEOUT)).ok();
    sock.set_nodelay(true).ok();
    sock.write_all(&out).map_err(|e| io_err("sending the fetch", e))?;

    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).map_err(|e| io_err("reading the reply length", e))?;
    let len = i32::from_be_bytes(len_buf);
    if len <= 0 || len as usize > 64 * 1024 * 1024 {
        return Err(format!("replication: implausible reply length {len}"));
    }
    let mut body = vec![0u8; len as usize];
    sock.read_exact(&mut body).map_err(|e| io_err("reading the reply", e))?;

    let mut buf = Bytes::from(body);
    ResponseHeader::decode(&mut buf, 0).map_err(|e| io_err("decoding the reply header", e))?;
    let response = FetchResponse::decode(&mut buf, FETCH_VERSION)
        .map_err(|e| io_err("decoding the fetch response", e))?;

    let mut pulled = Vec::new();
    for topic in response.responses {
        let name = topic.topic;
        let Some((topic_id, _, _, _, _)) = want.iter().find(|(_, n, _, _, _)| *n == name)
        else {
            continue;
        };
        for part in topic.partitions {
            if part.error_code != 0 {
                log!(
                    "kafgres: replication: {name}/{} returned error {}",
                    part.partition_index,
                    part.error_code
                );
                continue;
            }
            let records = part.records.unwrap_or_default();
            if records.is_empty() {
                continue;
            }
            pulled.push(Pulled {
                topic: *topic_id,
                partition: part.partition_index,
                bytes: records,
            });
        }
    }
    Ok(pulled)
}

/// Applied one batch at a time: `append_replicated` verifies each batch against the log end, which a block write would check only for the first.
pub fn apply(store: &mut dyn crate::storage::LogStore, pulled: &[Pulled]) -> Result<i64, String> {
    let mut applied = 0i64;
    for p in pulled {
        let mut buf = p.bytes.clone();
        while buf.len() >= kafgres_codec::records::RECORD_BATCH_OVERHEAD {
            let length = i32::from_be_bytes(
                buf[kafgres_codec::records::LENGTH_OFFSET
                    ..kafgres_codec::records::LENGTH_OFFSET + 4]
                    .try_into()
                    .expect("4 bytes"),
            );
            let total = kafgres_codec::records::LENGTH_OFFSET + 4 + length.max(0) as usize;
            if length <= 0 || total > buf.len() {
                // A partial trailing batch is normal: the leader caps its reply by bytes and does not truncate to fit.
                break;
            }
            let one = buf.split_to(total);
            let base = RecordBatch::new(one.clone())
                .map_err(|e| format!("replication: undecodable batch: {e:?}"))?
                .base_offset();
            let end = store
                .high_watermark(p.topic, p.partition)
                .map_err(|e| format!("replication: {e}"))?;
            if base < end {
                continue;
            }
            store
                .append_replicated(p.topic, p.partition, &one, end)
                .map_err(|e| format!("replication: {e}"))?;
            applied += 1;
        }
    }
    Ok(applied)
}

#[pg_extern]
fn kafgres_replicate_once(host: &str, port: i32) -> i64 {
    match replicate_once_from(host, port) {
        Ok(n) => n,
        Err(e) => error!("kafgres: {e}"),
    }
}

/// Worker entry point: log and retry rather than die when the leader is briefly unreachable.
pub fn replicate_once_from(host: &str, port: i32) -> Result<i64, String> {
    let mut store = crate::storage::open();

    // Epochs ride along with this read-only query: a read-write Spi call on a standby raises
    let want: Vec<(crate::storage::TopicId, String, i32, i64, i32)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT p.topic_id::int, t.name, p.partition, p.leader_epoch
               FROM kafgres_partitions p JOIN kafgres_topics t USING (topic_id)
              ORDER BY p.topic_id, p.partition",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(id), Some(name), Some(part)) =
                (row.get::<i32>(1)?, row.get::<String>(2)?, row.get::<i32>(3)?)
            {
                out.push((id as u32, name, part, row.get::<i32>(4)?.unwrap_or(0)));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_default()
    .into_iter()
    .filter_map(|(id, name, part, epoch)| {
        store.high_watermark(id, part).ok().map(|end| (id, name, part, end, epoch))
    })
    .collect();

    if want.is_empty() {
        return Ok(0);
    }

    let cut = reconcile_epoch(&mut *store, host, port, &want)?;
    let want = if cut > 0 {
        want.into_iter()
            .filter_map(|(id, name, part, _, epoch)| {
                store.high_watermark(id, part).ok().map(|end| (id, name, part, end, epoch))
            })
            .collect()
    } else {
        want
    };
    let pulled = fetch_from(host, port, &want)?;
    apply(&mut *store, &pulled)
}
