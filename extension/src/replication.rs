//! Follower replication over the public Fetch protocol; segment engine only — the table engine's log is already replicated by WAL streaming.
//!
//! One connection, kept across rounds, and a round pulls until the leader has nothing
//! more. The Fetch long-polls on the leader (`max_wait_ms`), which paces the follower
//! when the log is quiet; the tick only bounds how often the partition list and
//! leadership are re-read.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

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

/// Requested; the leader clamps to its own response ceiling (8 MiB).
const MAX_FETCH_BYTES: i32 = 64 * 1024 * 1024;

/// How long the leader may hold a Fetch that has nothing to give. This is what paces a
/// caught-up follower; a sleep in the worker loop would add its length to the lag on
/// every quiet moment.
const FETCH_MAX_WAIT_MS: i32 = 100;

/// A round returns to the worker loop after this long, so leadership and the partition
/// list are re-read even while the leader always has more to give.
const ROUND_BUDGET: Duration = Duration::from_millis(500);

const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// The follower's side of the replication stream: one connection to the leader, kept
/// across rounds and dropped on any error so the next round reconnects and reconciles.
pub struct Follower {
    host: String,
    port: i32,
    sock: Option<TcpStream>,
    correlation: i32,
    /// Partitions whose epoch was reconciled on this connection. A fresh connection
    /// starts empty, since the leader may have been promoted (or restarted) in between;
    /// a partition that appears mid-connection, replayed from the WAL, is reconciled
    /// before its first fetch like any other.
    reconciled: std::collections::HashSet<(crate::storage::TopicId, i32)>,
}

impl Follower {
    pub fn new(host: &str, port: i32) -> Self {
        Follower {
            host: host.to_string(),
            port,
            sock: None,
            correlation: 0,
            reconciled: std::collections::HashSet::new(),
        }
    }

    pub fn disconnect(&mut self) {
        self.sock = None;
        self.reconciled.clear();
    }

    fn socket(&mut self) -> Result<&mut TcpStream, String> {
        if self.sock.is_none() {
            let sock = TcpStream::connect((self.host.as_str(), self.port as u16))
                .map_err(|e| io_err(&format!("connecting to {}:{}", self.host, self.port), e))?;
            sock.set_read_timeout(Some(IO_TIMEOUT)).ok();
            sock.set_write_timeout(Some(IO_TIMEOUT)).ok();
            sock.set_nodelay(true).ok();
            self.sock = Some(sock);
            self.reconciled.clear();
        }
        Ok(self.sock.as_mut().expect("connected above"))
    }

    fn round_trip<T, F>(
        &mut self,
        api_key: i16,
        version: i16,
        header_version: i16,
        encode: F,
        decode: impl FnOnce(&mut Bytes, i16) -> Result<T, kafgres_codec::errors::CodecError>,
    ) -> Result<T, String>
    where
        F: FnOnce(&mut BytesMut) -> Result<(), kafgres_codec::errors::CodecError>,
    {
        self.correlation = self.correlation.wrapping_add(1);
        let header = RequestHeader {
            api_key,
            api_version: version,
            correlation_id: self.correlation,
            client_id: Some("kafgres-follower".to_string()),
            unknown_tagged_fields: Vec::new(),
        };
        let mut out = BytesMut::new();
        framing::write_frame(&mut out, |buf| {
            header.encode(buf, header_version)?;
            encode(buf)
        })
        .map_err(|e| io_err("encoding the request", e))?;

        // Any failure drops the connection: a half-read reply would desynchronise every
        // request after it.
        let result = (|| {
            let sock = self.socket()?;
            sock.write_all(&out).map_err(|e| io_err("sending the request", e))?;
            let mut len_buf = [0u8; 4];
            sock.read_exact(&mut len_buf).map_err(|e| io_err("reading the reply length", e))?;
            let len = i32::from_be_bytes(len_buf);
            if len <= 0 || len as usize > 64 * 1024 * 1024 {
                return Err(format!("replication: implausible reply length {len}"));
            }
            let mut body = vec![0u8; len as usize];
            sock.read_exact(&mut body).map_err(|e| io_err("reading the reply", e))?;
            Ok(body)
        })();
        let body = match result {
            Ok(b) => b,
            Err(e) => {
                self.disconnect();
                return Err(e);
            }
        };

        let mut buf = Bytes::from(body);
        let resp_header_version = if header_version >= 2 { 1 } else { 0 };
        let hdr = ResponseHeader::decode(&mut buf, resp_header_version)
            .map_err(|e| io_err("decoding the reply header", e))?;
        if hdr.correlation_id != self.correlation {
            self.disconnect();
            return Err(format!(
                "replication: reply answers request {}, not {}",
                hdr.correlation_id, self.correlation
            ));
        }
        decode(&mut buf, version).map_err(|e| io_err("decoding the reply", e))
    }
}

impl Follower {
/// Ask the leader where our last epoch ended and truncate if we went further: two nodes
pub fn reconcile_epoch(
    &mut self,
    store: &mut dyn crate::storage::LogStore,
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
    let response: OffsetForLeaderEpochResponse = self.round_trip(
        23, 3, 1,
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
}

pub struct Pulled {
    pub topic: crate::storage::TopicId,
    pub partition: i32,
    pub bytes: Bytes,
}

fn io_err(what: &str, e: impl std::fmt::Display) -> String {
    format!("replication: {what}: {e}")
}

impl Follower {
pub fn fetch_from(
    &mut self,
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
        max_wait_ms: FETCH_MAX_WAIT_MS,
        min_bytes: 1,
        max_bytes: MAX_FETCH_BYTES,
        // `read_committed`, same argument as `replica_id`: copy only what has committed.
        isolation_level: 1,
        session_id: 0,
        session_epoch: -1,
        topics,
        ..Default::default()
    };

    let response = self.round_trip(
        1, FETCH_VERSION, 1,
        |buf| request.encode(buf, FETCH_VERSION),
        FetchResponse::decode,
    )?;

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
    let mut follower = Follower::new(host, port);
    match follower.round(|| Ok(())) {
        Ok(n) => n,
        Err(e) => error!("kafgres: {e}"),
    }
}

/// What the follower wants of each partition: local id, leader-side name, partition,
/// the offset to fetch from, and the epoch it was last written under.
type Want = Vec<(crate::storage::TopicId, String, i32, i64, i32)>;

/// The partitions this node knows of, from the metadata the WAL replicates. Read-only,
/// so it can run on a standby; the caller supplies the transaction.
fn partitions_to_follow(store: &mut dyn crate::storage::LogStore) -> Want {
    Spi::connect(|client| {
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
    .collect()
}

fn with_current_ends(store: &mut dyn crate::storage::LogStore, want: Want) -> Want {
    want.into_iter()
        .filter_map(|(id, name, part, _, epoch)| {
            store.high_watermark(id, part).ok().map(|end| (id, name, part, end, epoch))
        })
        .collect()
}

impl Follower {
    /// One round: reconcile epochs if this connection has not yet, then pull until the
    /// leader answers a Fetch with nothing or `ROUND_BUDGET` is spent. `still_following`
    /// is consulted between fetches so a promotion ends the round at once. Returns the
    /// number of batches applied; any error has already dropped the connection.
    ///
    /// The partition list is read in its own short transaction; the pulling is not in
    /// one at all: nothing in the pull path needs SPI, and WAL replay may cancel a
    /// transaction held open for a whole round on a standby.
    pub fn round(
        &mut self,
        still_following: impl Fn() -> Result<(), String>,
    ) -> Result<i64, String> {
        let mut store = crate::storage::open();
        let mut want = if unsafe { pg_sys::IsTransactionState() } {
            partitions_to_follow(&mut *store)
        } else {
            pgrx::bgworkers::BackgroundWorker::transaction(|| {
                let mut store = crate::storage::open();
                partitions_to_follow(&mut *store)
            })
        };
        if want.is_empty() {
            return Ok(0);
        }

        let unreconciled: Want = want
            .iter()
            .filter(|(id, _, part, _, _)| !self.reconciled.contains(&(*id, *part)))
            .cloned()
            .collect();
        if !unreconciled.is_empty() {
            self.socket()?;
            let cut = self.reconcile_epoch(&mut *store, &unreconciled)?;
            self.reconciled
                .extend(unreconciled.iter().map(|(id, _, part, _, _)| (*id, *part)));
            if cut > 0 {
                log!("kafgres: follower truncated {cut} offset(s) to rejoin the leader's epoch");
                want = with_current_ends(&mut *store, want);
            }
        }

        let started = Instant::now();
        let mut applied = 0i64;
        loop {
            let pulled = self.fetch_from(&want)?;
            if pulled.is_empty() {
                break;
            }
            applied += apply(&mut *store, &pulled)?;
            still_following()?;
            if started.elapsed() >= ROUND_BUDGET {
                break;
            }
            want = with_current_ends(&mut *store, want);
        }
        Ok(applied)
    }
}
