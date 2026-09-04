//! The broker event loop.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use kafgres_codec::framing::{take_frame, FrameError};
use kafgres_codec::generated::create_partitions_request::CreatePartitionsRequest;
use kafgres_codec::generated::create_topics_request::CreateTopicsRequest;
use kafgres_codec::generated::create_acls_request::CreateAclsRequest;
use kafgres_codec::generated::delete_acls_request::DeleteAclsRequest;
use kafgres_codec::generated::delete_groups_request::DeleteGroupsRequest;
use kafgres_codec::generated::describe_acls_request::DescribeAclsRequest;
use kafgres_codec::generated::offset_delete_request::OffsetDeleteRequest;
use kafgres_codec::generated::delete_records_request::DeleteRecordsRequest;
use kafgres_codec::generated::delete_topics_request::DeleteTopicsRequest;
use kafgres_codec::generated::describe_cluster_request::DescribeClusterRequest;
use kafgres_codec::generated::alter_configs_request::AlterConfigsRequest;
use kafgres_codec::generated::describe_client_quotas_request::DescribeClientQuotasRequest;
use kafgres_codec::generated::describe_producers_request::DescribeProducersRequest;
use kafgres_codec::generated::write_txn_markers_request::WriteTxnMarkersRequest;
use kafgres_codec::generated::describe_topic_partitions_request::DescribeTopicPartitionsRequest;
use kafgres_codec::generated::share_acknowledge_request::ShareAcknowledgeRequest;
use kafgres_codec::generated::share_fetch_request::ShareFetchRequest;
use kafgres_codec::generated::share_group_describe_request::ShareGroupDescribeRequest;
use kafgres_codec::generated::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use kafgres_codec::generated::alter_client_quotas_request::AlterClientQuotasRequest;
use kafgres_codec::generated::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest;
use kafgres_codec::generated::describe_transactions_request::DescribeTransactionsRequest;
use kafgres_codec::generated::list_transactions_request::ListTransactionsRequest;
use kafgres_codec::generated::describe_log_dirs_request::DescribeLogDirsRequest;
use kafgres_codec::generated::consumer_group_describe_request::ConsumerGroupDescribeRequest;
use kafgres_codec::generated::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use kafgres_codec::generated::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;
use kafgres_codec::generated::elect_leaders_request::ElectLeadersRequest;
use kafgres_codec::generated::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use kafgres_codec::generated::describe_configs_request::DescribeConfigsRequest;
use kafgres_codec::generated::fetch_request::FetchRequest;
use kafgres_codec::generated::incremental_alter_configs_request::IncrementalAlterConfigsRequest;
use kafgres_codec::generated::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafgres_codec::generated::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use kafgres_codec::generated::txn_offset_commit_request::TxnOffsetCommitRequest;
use kafgres_codec::generated::end_txn_request::EndTxnRequest;
use kafgres_codec::generated::offset_for_leader_epoch_request::OffsetForLeaderEpochRequest;
use kafgres_codec::generated::sasl_authenticate_request::SaslAuthenticateRequest;
use kafgres_codec::generated::sasl_handshake_request::SaslHandshakeRequest;
use kafgres_codec::prelude::*;

use crate::handlers::{self, metadata::ClusterConfig, HandlerError};

pub mod transport;
use transport::Transport;

const READ_CHUNK_BYTES: usize = 65_536;

const MAX_CONN_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// Bytes, across all connections, that may sit *above* the per-connection free tier: keeps
const MAX_OVERSIZE_TOTAL_BYTES: usize = 256 * 1024 * 1024;

/// Connection ceiling, Kafka's `max.connections`: without it the per-connection buffer cap
const MAX_CONNECTIONS: usize = 512;

/// Requests answered per connection per tick: a pipelining peer must not monopolise the
const MAX_REQUESTS_PER_TICK: usize = 64;

/// Ceiling on simultaneously parked requests, of every kind. Past the cap a Fetch is
const MAX_PARKED: usize = 4096;

/// Longest we will hold a Fetch, whatever the client asked for: `fetch.max.wait.ms` is
const MAX_FETCH_WAIT: Duration = Duration::from_millis(5_000);

/// Failed SASL steps a connection may make before it is dropped.
const MAX_SASL_FAILURES: u32 = 3;

/// How long a connection may stay unauthenticated: what stops silent sockets holding every
const AUTH_DEADLINE: Duration = Duration::from_secs(30);

struct Conn {
    stream: Transport,
    peer: String,
    inbuf: BytesMut,
    /// Ceiling this connection was granted for the current pass. Set by the read pass and
    frame_cap: usize,
    outbuf: BytesMut,

    next_seq: u64,
    flush_seq: u64,
    /// Completed responses that cannot go out yet because an earlier one is parked: Kafka
    ready: HashMap<u64, BytesMut>,

    sasl: crate::sasl::SaslState,
    /// Failed SASL steps on this connection: a failed proof leaves the state at
    sasl_failures: u32,
    /// The handshake was v0, so SASL tokens arrive as bare length-prefixed blobs rather
    sasl_raw: bool,
    /// Subject DN of a verified client certificate, once the TLS handshake completes.
    tls_principal: Option<String>,
    /// Whether the certificate has already been looked at; without it an unparseable
    tls_checked: bool,
    opened_at: Instant,
}

impl Conn {
    /// Drain `outbuf`; returns false on a fatal write error. Pumps the transport first,
    fn flush(&mut self) -> bool {
        if !self.stream.pump() {
            return false;
        }
        let mut pos = 0;
        while pos < self.outbuf.len() {
            match self.stream.write(&self.outbuf[pos..]) {
                Ok(0) => return false,
                Ok(n) => pos += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return false,
            }
        }
        let _ = self.outbuf.split_to(pos);
        true
    }

    fn reserve(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Bytes owed to the peer: written but unflushed, plus completed-but-blocked. `outbuf`
    fn queued_bytes(&self) -> usize {
        self.outbuf.len() + self.ready.values().map(|b| b.len()).sum::<usize>()
    }

    fn complete(&mut self, seq: u64, bytes: BytesMut) {
        self.ready.insert(seq, bytes);
        while let Some(b) = self.ready.remove(&self.flush_seq) {
            self.outbuf.extend_from_slice(&b);
            self.flush_seq += 1;
        }
    }
}

/// What a parked request is waiting for. Fetch, JoinGroup and SyncGroup block by design;
enum Waiting {
    Fetch {
        /// Kept decoded so completion re-runs the same read rather than re-parsing.
        request: FetchRequest,
        /// Partitions this fetch watches, for the append doorbell.
        watching: Vec<(u32, i32)>,
        min_bytes: usize,
    },
    Join { group_id: String, member_id: String },
    Sync { group_id: String, member_id: String },
}

struct Parked {
    conn_id: i32,
    seq: u64,
    correlation_id: i32,
    api_key: i16,
    api_version: i16,
    /// The `client.id` from the request header, carried because the response is built long
    client_id: String,
    deadline: Instant,
    waiting: Waiting,
}

struct Server {
    conns: HashMap<i32, Conn>,
    next_id: i32,
    parked: Vec<Parked>,
    /// Partitions appended to since the last completion pass — the doorbell. Without it,
    appended: HashSet<(u32, i32)>,
    /// Groups changed since the last completion pass, keyed like `appended`: a single bool
    groups_changed: HashSet<String>,
    tick: u64,
    /// The last producer sweep filled its batch, so there is more to drop. Drains at a
    producer_backlog: bool,
    /// Where the round-robin retention sweep resumes. Without it the sweep would only
    retention_cursor: u32,
    /// Client-quota rate windows. A plain map rather than shared memory: the broker is a
    quotas: crate::quota::Meter,
    /// Configured quotas, cached and refreshed on a timer; the hot path must not query
    quota_config: crate::quota::QuotaCache,
    /// TLS, if configured. Built once at startup: a certificate change needs a worker
    tls: Option<crate::tls::TlsSetup>,
    /// ACLs, cached. Refreshed on a timer rather than queried per request.
    acls: crate::acl::AclCache,
}

pub fn run(cfg: ClusterConfig, bind_host: &str, port: u16, tick: Duration) {
    let mut cfg = cfg;
    // Before the listener: a broker that was told to serve TLS and cannot must not come
    let tls = match crate::tls_setup() {
        Ok(Some(setup)) => {
            log!(
                "kafgres: TLS enabled, client certificates {:?}",
                setup.client_auth
            );
            Some(setup)
        }
        Ok(None) => None,
        Err(e) => {
            log!("kafgres: TLS is configured but unusable, refusing to start: {e}");
            return;
        }
    };

    let addr = format!("{bind_host}:{port}");
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => {
            if let Err(e) = l.set_nonblocking(true) {
                log!("kafgres: set_nonblocking on listener failed: {e}");
                return;
            }
            log!("kafgres: listening on {addr}");
            l
        }
        Err(e) => {
            log!("kafgres: cannot bind {addr}: {e}");
            return;
        }
    };

    let mut srv = Server {
        conns: HashMap::new(),
        next_id: 0,
        parked: Vec::new(),
        appended: HashSet::new(),
        groups_changed: HashSet::new(),
        tick: 0,
        producer_backlog: true,
        retention_cursor: 0,
        quotas: crate::quota::Meter::default(),
        quota_config: crate::quota::QuotaCache::default(),
        tls,
        acls: crate::acl::AclCache::default(),
    };

    // Load once before the loop, not just on the first tick: the default snapshot is
    refresh_acls(&mut srv);

    let mut epochs_ready = raise_leader_epochs();

    // After recovery has truncated any torn tail, and before serving: a marker pointing
    match BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| Ok(crate::produce_sql::reconcile_markers(&mut *crate::storage::open())))
    }) {
        Ok(0) => {}
        Ok(n) => log!("kafgres: marker reconciliation dropped {n} orphaned marker(s)"),
        Err(e) => log!("kafgres: marker reconciliation failed: {e}"),
    }

    while BackgroundWorker::wait_latch(Some(tick)) {
        if BackgroundWorker::sighup_received() {
            crate::reload_config();
            log!("kafgres: SIGHUP, configuration reloaded");
            reload_tls(&mut srv);
            // The advertised address is declared `GucContext::Sighup` but was captured
            cfg = crate::cluster_config();
        }

        srv.tick = srv.tick.wrapping_add(1);
        // Before anything is served: an unloaded snapshot has `enabled = false`, which
        if !epochs_ready {
            if srv.tick % 200 == 0 {
                epochs_ready = raise_leader_epochs();
            }
            continue;
        }

        refresh_acls(&mut srv);
        accept_new(&listener, &mut srv);
        poll_connections(&mut srv, &cfg, None);
        drop_unauthenticated(&mut srv);
        expire_group_members(&mut srv);
        expire_producer_state(&mut srv);
        expire_consumer_group_members(&srv);
        expire_share_group_state(&srv);
        expire_quota_windows(&mut srv);
        refresh_quotas(&mut srv);
        enforce_retention(&mut srv);
        complete_parked(&mut srv, &cfg);
        flush_all(&mut srv);
    }

    log!(
        "kafgres: SIGTERM received, closing {} connection(s)",
        srv.conns.len()
    );
}

fn accept_new(listener: &TcpListener, srv: &mut Server) {
    loop {
        if srv.conns.len() >= MAX_CONNECTIONS {
            return;
        }
        match listener.accept() {
            Ok((stream, addr)) => {
                if let Err(e) = stream.set_nonblocking(true) {
                    log!("kafgres: set_nonblocking failed for {addr}: {e}");
                    continue;
                }
                let stream = match &srv.tls {
                    Some(setup) => match Transport::tls(stream, setup.config.clone()) {
                        Ok(t) => t,
                        Err(e) => {
                            log!("kafgres: tls setup failed for {addr}: {e}");
                            continue;
                        }
                    },
                    None => Transport::plain(stream),
                };
                // Kafka clients are request/response and latency-sensitive; Nagle
                let _ = stream.set_nodelay(true);
                srv.next_id += 1;
                srv.conns.insert(
                    srv.next_id,
                    Conn {
                        stream,
                        peer: addr.to_string(),
                        inbuf: BytesMut::with_capacity(READ_CHUNK_BYTES),
                        frame_cap: MAX_CONN_BUFFER_BYTES,
                        outbuf: BytesMut::new(),
                        next_seq: 0,
                        flush_seq: 0,
                        ready: HashMap::new(),
                        sasl: crate::sasl::SaslState::AwaitingHandshake,
                        sasl_failures: 0,
                        sasl_raw: false,
                        tls_principal: None,
                        tls_checked: false,
                        opened_at: Instant::now(),
                    },
                );
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                log!("kafgres: accept error: {e}");
                break;
            }
        }
    }
}

fn poll_connections(srv: &mut Server, cfg: &ClusterConfig, ready: Option<&HashSet<i32>>) {
    let ids: Vec<i32> = srv.conns.keys().copied().collect();
    let mut closed: Vec<i32> = Vec::new();

    // Read the GUC once per pass so reader and framer cannot disagree on the ceiling.
    let requested_cap = crate::max_request_bytes();
    let tier = MAX_CONN_BUFFER_BYTES.min(requested_cap.max(1));
    // Summed once; maintained incrementally below rather than O(connections^2) per pass.
    let mut oversize_total: usize = srv
        .conns
        .values()
        .map(|c| c.inbuf.len().saturating_sub(tier))
        .sum();

    for id in ids {
        if let Some(r) = ready {
            let idle = srv
                .conns
                .get(&id)
                .map(|c| c.outbuf.is_empty())
                .unwrap_or(true);
            if !r.contains(&id) && idle {
                continue;
            }
        }

        let read_outcome = {
            let conn = match srv.conns.get_mut(&id) {
                Some(c) => c,
                None => continue,
            };
            if !conn.flush() {
                closed.push(id);
                continue;
            }
            // Free tier plus what is left of the shared budget; its own excess is excluded so a connection mid-frame keeps what it holds.
            let mine = conn.inbuf.len().saturating_sub(tier);
            let others = oversize_total.saturating_sub(mine);
            let inbound_cap = tier
                .saturating_add(MAX_OVERSIZE_TOTAL_BYTES.saturating_sub(others))
                .min(requested_cap.max(tier));
            conn.frame_cap = inbound_cap;
            let outcome = read_available(conn, inbound_cap);
            oversize_total = others + conn.inbuf.len().saturating_sub(tier);
            // The certificate does not exist until the handshake finishes; read once and cached.
            if !conn.tls_checked && conn.stream.handshake_done() {
                conn.tls_checked = true;
                if let Some(der) = conn.stream.peer_certificate() {
                    match crate::tls::principal_from_cert(&der) {
                        Some(dn) => {
                            log!("kafgres: {} presented certificate '{dn}'", conn.peer);
                            conn.tls_principal = Some(dn);
                        }
                        // Verified by rustls but unparseable here: refusing beats an authorization decision with nothing to key on.
                        None => log!(
                            "kafgres: {} presented a certificate whose subject could not be parsed",
                            conn.peer
                        ),
                    }
                }
            }
            outcome
        };

        match read_outcome {
            ReadResult::Closed => {
                closed.push(id);
                continue;
            }
            ReadResult::Fatal(why) => {
                let peer = srv.conns.get(&id).map(|c| c.peer.clone()).unwrap_or_default();
                log!("kafgres: closing {peer}: {why}");
                closed.push(id);
                continue;
            }
            ReadResult::Ok => {}
        }

        if !serve_frames(srv, id, cfg) {
            closed.push(id);
        }
    }

    for id in closed {
        drop_conn(srv, id);
    }
}

/// Remove a connection and every parked fetch that belonged to it: a parked entry outliving its connection would complete into a closed socket — or, connection ids being reused, another peer's socket.
fn drop_conn(srv: &mut Server, id: i32) {
    srv.conns.remove(&id);
    srv.parked.retain(|p| p.conn_id != id);
}

enum ReadResult {
    Ok,
    Closed,
    Fatal(String),
}

fn read_available(conn: &mut Conn, cap: usize) -> ReadResult {
    let mut chunk = [0u8; READ_CHUNK_BYTES];
    loop {
        if conn.inbuf.len() > cap {
            return ReadResult::Fatal(format!(
                "inbound buffer exceeded {cap} bytes without a complete frame \
                 (kafgres.max_request_bytes, or the per-connection free tier if too many \
                 connections are already holding large frames)"
            ));
        }
        match conn.stream.read(&mut chunk) {
            Ok(0) => return ReadResult::Closed,
            Ok(n) => conn.inbuf.extend_from_slice(&chunk[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return ReadResult::Ok,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return ReadResult::Fatal(e.to_string()),
        }
    }
}

fn serve_frames(srv: &mut Server, id: i32, cfg: &ClusterConfig) -> bool {
    for _ in 0..MAX_REQUESTS_PER_TICK {
        let frame = {
            let conn = match srv.conns.get_mut(&id) {
                Some(c) => c,
                None => return false,
            };
            if !conn.flush() {
                return false;
            }
            if conn.queued_bytes() > MAX_CONN_BUFFER_BYTES {
                log!(
                    "kafgres: {} has {} bytes of undelivered responses; closing",
                    conn.peer,
                    conn.queued_bytes()
                );
                return false;
            }
            // The same cap the reader used: if they disagree, the reader kills mid-frame a connection the framer would have served.
            match take_frame(&mut conn.inbuf, conn.frame_cap.max(1)) {
                Ok(Some(f)) => f,
                Ok(None) => return true,
                Err(FrameError::Oversized { declared, max }) => {
                    log!(
                        "kafgres: {} declared a {declared}-byte frame (max {max}); closing",
                        conn.peer
                    );
                    return false;
                }
            }
        };

        if !serve_one(srv, id, frame, cfg) {
            return false;
        }
    }
    true
}

fn serve_one(srv: &mut Server, id: i32, frame: Bytes, cfg: &ClusterConfig) -> bool {
    // A v0 handshake switches the connection to unwrapped SASL: same length prefix, but the contents are tokens, not requests.
    if srv
        .conns
        .get(&id)
        .map(|c| c.sasl_raw && !matches!(c.sasl, crate::sasl::SaslState::Authenticated { .. }))
        .unwrap_or(false)
    {
        return serve_raw_sasl(srv, id, &frame);
    }

    let (req, outcome) = match handlers::parse(frame) {
        Ok(v) => v,
        Err(e) => {
            let peer = srv.conns.get(&id).map(|c| c.peer.clone()).unwrap_or_default();
            log!("kafgres: {peer} sent an unparseable request: {e}");
            return false;
        }
    };

    let seq = match srv.conns.get_mut(&id) {
        Some(c) => c.reserve(),
        None => return false,
    };

    // The gate. Before authentication, only the three APIs that get there are answerable.
    if crate::sasl_required() && !authenticated(srv, id) && !is_pre_auth_api(req.api_key) {
        if let Some(c) = srv.conns.get_mut(&id) {
            log!(
                "kafgres: {} sent api {} before authenticating; closing",
                c.peer,
                req.api_key
            );
            // Flush first: a pipelined SaslAuthenticate error is already queued; dropping it turns a diagnosable rejection into a bare reset.
            c.flush();
        }
        return false;
    }

    static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    // Not `is_ok()`: `std::env::var` returns `Ok("")` for a variable that is set but
    if *TRACE.get_or_init(|| {
        std::env::var("KAFGRES_TRACE_API").is_ok_and(|v| !v.is_empty())
    }) {
        log!("kafgres: TRACE api={} v{}", req.api_key, req.api_version);
    }
    let mut out = BytesMut::new();
    let result = if outcome != kafgres_codec::framing::Negotiation::Supported {
        let peer = srv.conns.get(&id).map(|c| c.peer.clone()).unwrap_or_default();
        log!(
            "kafgres: {peer} requested api {} v{} which we do not serve ({outcome:?})",
            req.api_key,
            req.api_version
        );
        handlers::write_unsupported(&mut out, &req, outcome).map(|()| Disposition::Reply)
    } else {
        dispatch(&mut out, &req, cfg, srv, id, seq)
    };

    match result {
        Ok(Disposition::Reply) => {
            let over_limit = match srv.conns.get_mut(&id) {
                Some(c) => {
                    c.complete(seq, out);
                    c.sasl_failures >= MAX_SASL_FAILURES
                }
                None => false,
            };
            if over_limit {
                // The response carrying the error code is already queued; flush it so
                if let Some(c) = srv.conns.get_mut(&id) {
                    log!("kafgres: {} failed authentication too often; closing", c.peer);
                    c.flush();
                }
                return false;
            }
            true
        }
        // acks=0 Produce: nobody will read the reply, but the slot must be released or every later response queues behind it.
        Ok(Disposition::NoReply) => {
            if let Some(c) = srv.conns.get_mut(&id) {
                c.complete(seq, BytesMut::new());
            }
            true
        }
        Ok(Disposition::Parked) => true,
        Err(e) => {
            let peer = srv.conns.get(&id).map(|c| c.peer.clone()).unwrap_or_default();
            log!(
                "kafgres: {peer} api {} v{}: {e}",
                req.api_key,
                req.api_version
            );
            false
        }
    }
}

enum Disposition {
    Reply,
    NoReply,
    Parked,
}

/// One unwrapped SASL token, for a connection that handshook at v0: no correlation id and no response header, so a failure closes the connection.
fn serve_raw_sasl(srv: &mut Server, id: i32, token: &[u8]) -> bool {
    let state = conn_sasl(srv, id);
    let stepped = BackgroundWorker::transaction(|| {
        crate::dbtx::contained(|| Ok(crate::sasl::step(&state, token)))
    });

    let (reply, next) = match stepped {
        Ok(Ok((bytes, next))) => (bytes, Some(next)),
        Ok(Err(e)) => {
            let peer = srv.conns.get(&id).map(|c| c.peer.clone()).unwrap_or_default();
            log!("kafgres: {peer} sasl (v0 framing) failed: {e}");
            return false;
        }
        Err(e) => {
            log!("kafgres: sasl transaction failed: {e}");
            return false;
        }
    };

    let c = match srv.conns.get_mut(&id) {
        Some(c) => c,
        None => return false,
    };
    if let Some(next) = next {
        if let crate::sasl::SaslState::Authenticated { principal } = &next {
            log!("kafgres: {} authenticated as '{principal}' (v0 framing)", c.peer);
        }
        c.sasl = next;
    }
    if c.queued_bytes() + reply.len() + 4 > MAX_CONN_BUFFER_BYTES {
        return false;
    }
    c.outbuf.extend_from_slice(&(reply.len() as i32).to_be_bytes());
    c.outbuf.extend_from_slice(&reply);
    true
}

/// APIs answerable before authentication. ApiVersions is on the list because the client must
fn is_pre_auth_api(api_key: i16) -> bool {
    matches!(
        api_key,
        kafgres_codec::header::API_VERSIONS_KEY | SASL_HANDSHAKE_KEY | SASL_AUTHENTICATE_KEY
    )
}

/// Charge a request against its client quota and return the delay to ask for; zero when no quota matches. This reports a delay, it does not enforce one.
fn charge(
    srv: &mut Server,
    rate: crate::quota::Rate,
    conn_id: i32,
    client_id: &str,
    bytes: i64,
) -> i32 {
    if bytes <= 0 {
        return 0;
    }
    if srv.quota_config.is_empty() {
        return 0;
    }
    let principal = principal_of(srv, conn_id).name;
    let Some((entity, quota)) = srv.quota_config.applicable(rate, &principal, client_id) else {
        return 0;
    };
    let now = quota_now_millis();
    srv.quotas.record(rate, &entity, bytes, quota, now)
}

fn quota_now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Drop quota windows nothing has touched, so a broker that has seen many distinct
fn expire_quota_windows(srv: &mut Server) {
    const EVERY_N_TICKS: u64 = 12_000; // ~60s at the default 5ms tick
    if srv.tick % EVERY_N_TICKS != 0 {
        return;
    }
    let now = quota_now_millis();
    srv.quotas.expire(now);
}

/// Reload the quota table when the cache goes stale, on the tick rather than on a
fn refresh_quotas(srv: &mut Server) {
    if !srv.quota_config.is_stale() {
        return;
    }
    match BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| {
            crate::quota::QuotaCache::load().map_err(handlers::HandlerError::Internal)
        })
    }) {
        Ok(fresh) => srv.quota_config = fresh,
        // Keep the previous snapshot: a briefly failing reload should go on limiting.
        Err(e) => log!("kafgres: could not reload client quotas: {e}"),
    }
}

fn principal_of(srv: &Server, id: i32) -> crate::acl::Principal {
    let conn = match srv.conns.get(&id) {
        Some(c) => c,
        None => return crate::acl::Principal::anonymous(""),
    };
    // Host without the port: an ACL is written against an address, not an ephemeral
    let host = conn
        .peer
        .rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .unwrap_or_else(|| conn.peer.clone());
    // Stripping the port from `[::1]:54321` leaves brackets, and a host-scoped ACL written `::1` never matches.
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .map(|h| h.to_string())
        .unwrap_or(host);
    if let Some(dn) = &conn.tls_principal {
        return crate::acl::Principal::certificate(dn, &host);
    }
    if let crate::sasl::SaslState::Authenticated { principal } = &conn.sasl {
        return crate::acl::Principal::user(principal, &host);
    }
    crate::acl::Principal::anonymous(&host)
}

fn authenticated(srv: &Server, id: i32) -> bool {
    srv.conns
        .get(&id)
        .map(|c| {
            // A verified client certificate authenticates on its own — that is what `SSL` means; no Kafka client offers mTLS and SASL together.
            c.tls_principal.is_some()
                || matches!(c.sasl, crate::sasl::SaslState::Authenticated { .. })
        })
        .unwrap_or(false)
}

const SASL_HANDSHAKE_KEY: i16 = 17;
const SASL_AUTHENTICATE_KEY: i16 = 36;

fn dispatch(
    out: &mut BytesMut,
    req: &handlers::Request,
    cfg: &ClusterConfig,
    srv: &mut Server,
    conn_id: i32,
    seq: u64,
) -> Result<Disposition, HandlerError> {
    use kafgres_codec::generated::describe_groups_request::DescribeGroupsRequest;
    use kafgres_codec::generated::find_coordinator_request::FindCoordinatorRequest;
    use kafgres_codec::generated::list_groups_request::ListGroupsRequest;
    use kafgres_codec::generated::heartbeat_request::HeartbeatRequest;
    use kafgres_codec::generated::init_producer_id_request::InitProducerIdRequest;
    use kafgres_codec::generated::join_group_request::JoinGroupRequest;
    use kafgres_codec::generated::leave_group_request::LeaveGroupRequest;
    use kafgres_codec::generated::list_offsets_request::ListOffsetsRequest;
    use kafgres_codec::generated::offset_commit_request::OffsetCommitRequest;
    use kafgres_codec::generated::offset_fetch_request::OffsetFetchRequest;
    use kafgres_codec::generated::sync_group_request::SyncGroupRequest;
    use kafgres_codec::generated::metadata_request::MetadataRequest;
    use kafgres_codec::generated::produce_request::ProduceRequest;

    match req.api_key {
        kafgres_codec::header::API_VERSIONS_KEY => {
            let body = handlers::api_versions::handle();
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        0 => {
            let mut body_buf = req.body.clone();
            let request = ProduceRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let outcome = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    let mut store = crate::storage::open();
                    handlers::produce::handle(&request, &mut *store, &authz)
                })
            })?;
            // Ring the doorbell before answering: a consumer parked on this partition
            srv.appended.extend(outcome.appended.iter().copied());
            // Charged after the append, on the bytes actually written — not the request size, or rejected batches get billed.
            let throttle = charge(
                srv,
                crate::quota::Rate::Producer,
                conn_id,
                req.client_id.as_deref().unwrap_or(""),
                outcome.bytes as i64,
            );
            match outcome.response {
                None => Ok(Disposition::NoReply),
                Some(mut body) => {
                    body.throttle_time_ms = throttle;
                    handlers::write_response(
                        out,
                        req.api_key,
                        req.api_version,
                        req.correlation_id,
                        &body,
                    )?;
                    Ok(Disposition::Reply)
                }
            }
        }
        1 => {
            let mut body_buf = req.body.clone();
            let request = FetchRequest::decode(&mut body_buf, req.api_version)?;
            // One transaction: SPI outside an established transaction segfaults the worker.
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let (body, watching) = run_fetch(&request, &authz, true)?;

            let min_bytes = request.min_bytes.max(1) as usize;
            let wait = fetch_wait(request.max_wait_ms);

            // Answer now if satisfied, if the client did not want to wait, on error, or at the parking cap.
            let satisfied = handlers::fetch::records_bytes(&body) >= min_bytes
                || handlers::fetch::has_error(&body)
                || wait.is_zero()
                || srv.parked.len() >= MAX_PARKED;

            if satisfied {
                // Charged on the record bytes actually returned; the parked path is charged in `complete_parked`.
                let mut body = body;
                body.throttle_time_ms = charge(
                    srv,
                    crate::quota::Rate::Consumer,
                    conn_id,
                    req.client_id.as_deref().unwrap_or(""),
                    handlers::fetch::records_bytes(&body) as i64,
                );
                handlers::write_response(
                    out,
                    req.api_key,
                    req.api_version,
                    req.correlation_id,
                    &body,
                )?;
                return Ok(Disposition::Reply);
            }

            srv.parked.push(Parked {
                conn_id,
                seq,
                correlation_id: req.correlation_id,
                api_key: req.api_key,
                api_version: req.api_version,
                client_id: req.client_id.clone().unwrap_or_default(),
                deadline: Instant::now() + wait,
                waiting: Waiting::Fetch {
                    request,
                    watching,
                    min_bytes,
                },
            });
            Ok(Disposition::Parked)
        }
        2 => {
            let mut body_buf = req.body.clone();
            let request = ListOffsetsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    let store = crate::storage::open();
                    handlers::list_offsets::handle(&request, &*store, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        3 => {
            let mut body_buf = req.body.clone();
            let request = MetadataRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::metadata::handle(&request, req.api_version, cfg, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        8 => {
            let mut body_buf = req.body.clone();
            let request = OffsetCommitRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::offsets::offset_commit(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        9 => {
            let mut body_buf = req.body.clone();
            let request = OffsetFetchRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let version = req.api_version;
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::offsets::offset_fetch(&request, version, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        10 => {
            let mut body_buf = req.body.clone();
            let request = FindCoordinatorRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = handlers::coordinator::find_coordinator(&request, req.api_version, cfg, &authz);
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        11 => {
            let mut body_buf = req.body.clone();
            let request = JoinGroupRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let version = req.api_version;
            let client_id = req.client_id.clone().unwrap_or_default();
            let peer = srv.conns.get(&conn_id).map(|c| c.peer.clone()).unwrap_or_default();
            let outcome = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::join_sync::join_group(&request, version, &client_id, &peer, &authz)
                })
            })?;
            srv.groups_changed.insert(request.group_id.clone());
            match outcome {
                handlers::join_sync::JoinOutcome::Reply(body) => {
                    handlers::write_response(
                        out,
                        req.api_key,
                        req.api_version,
                        req.correlation_id,
                        &*body,
                    )?;
                    Ok(Disposition::Reply)
                }
                handlers::join_sync::JoinOutcome::Park { member_id } => {
                    if srv.parked.len() >= MAX_PARKED {
                        // Refusing is better than parking past the ceiling: the client
                        let body = handlers::join_sync::error_join(
                            kafgres_codec::ErrorCode::RebalanceInProgress,
                            member_id,
                        );
                        handlers::write_response(
                            out,
                            req.api_key,
                            req.api_version,
                            req.correlation_id,
                            &body,
                        )?;
                        return Ok(Disposition::Reply);
                    }
                    srv.parked.push(Parked {
                        conn_id,
                        seq,
                        correlation_id: req.correlation_id,
                        api_key: req.api_key,
                        api_version: req.api_version,
                        client_id: req.client_id.clone().unwrap_or_default(),
                        // The rebalance timeout is the client's own patience. Past it
                        deadline: Instant::now()
                            + Duration::from_millis(crate::group::clamp_rebalance_timeout(
                                request.rebalance_timeout_ms,
                            ) as u64),
                        waiting: Waiting::Join {
                            group_id: request.group_id.clone(),
                            member_id,
                        },
                    });
                    Ok(Disposition::Parked)
                }
            }
        }
        14 => {
            let mut body_buf = req.body.clone();
            let request = SyncGroupRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let outcome = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::join_sync::sync_group(&request, &authz))
            })?;
            srv.groups_changed.insert(request.group_id.clone());
            match outcome {
                handlers::join_sync::SyncOutcome::Reply(body) => {
                    handlers::write_response(
                        out,
                        req.api_key,
                        req.api_version,
                        req.correlation_id,
                        &*body,
                    )?;
                    Ok(Disposition::Reply)
                }
                handlers::join_sync::SyncOutcome::Park => {
                    if srv.parked.len() >= MAX_PARKED {
                        let body = handlers::join_sync::error_sync(
                            kafgres_codec::ErrorCode::RebalanceInProgress,
                        );
                        handlers::write_response(
                            out,
                            req.api_key,
                            req.api_version,
                            req.correlation_id,
                            &body,
                        )?;
                        return Ok(Disposition::Reply);
                    }
                    srv.parked.push(Parked {
                        conn_id,
                        seq,
                        correlation_id: req.correlation_id,
                        api_key: req.api_key,
                        api_version: req.api_version,
                        client_id: req.client_id.clone().unwrap_or_default(),
                        deadline: Instant::now() + Duration::from_millis(60_000),
                        waiting: Waiting::Sync {
                            group_id: request.group_id.clone(),
                            member_id: request.member_id.clone(),
                        },
                    });
                    Ok(Disposition::Parked)
                }
            }
        }
        12 => {
            let mut body_buf = req.body.clone();
            let request = HeartbeatRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::coordinator::heartbeat(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        13 => {
            let mut body_buf = req.body.clone();
            let request = LeaveGroupRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let version = req.api_version;
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::coordinator::leave_group(&request, version, &authz))
            })?;
            srv.groups_changed.insert(request.group_id.clone());
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        22 => {
            let mut body_buf = req.body.clone();
            let request = InitProducerIdRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::init_producer_id::handle(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        15 => {
            let mut body_buf = req.body.clone();
            let request = DescribeGroupsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::describe_groups::describe_groups(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        16 => {
            let mut body_buf = req.body.clone();
            let request = ListGroupsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::describe_groups::list_groups(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        19 => {
            let mut body_buf = req.body.clone();
            let request = CreateTopicsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::topics::create_topics(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        20 => {
            let mut body_buf = req.body.clone();
            let request = DeleteTopicsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::topics::delete_topics(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        21 => {
            let mut body_buf = req.body.clone();
            let request = DeleteRecordsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    let mut store = crate::storage::open();
                    handlers::admin::delete_records(&request, &mut *store, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        24 => {
            let mut body_buf = req.body.clone();
            let request = AddPartitionsToTxnRequest::decode(&mut body_buf, req.api_version)?;
            let version = req.api_version;
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::txn::handle_add_partitions(&request, version))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        25 => {
            let mut body_buf = req.body.clone();
            let request = AddOffsetsToTxnRequest::decode(&mut body_buf, req.api_version)?;
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::txn::handle_add_offsets(&request))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        28 => {
            let mut body_buf = req.body.clone();
            let request = TxnOffsetCommitRequest::decode(&mut body_buf, req.api_version)?;
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::txn::handle_txn_offset_commit(&request))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        26 => {
            let mut body_buf = req.body.clone();
            let request = EndTxnRequest::decode(&mut body_buf, req.api_version)?;
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::txn::handle_end_txn(&request))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        23 => {
            let mut body_buf = req.body.clone();
            let request = OffsetForLeaderEpochRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    let store = crate::storage::open();
                    handlers::leader_epoch::handle(&request, &*store, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        32 => {
            let mut body_buf = req.body.clone();
            let request = DescribeConfigsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::configs::describe_configs(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        37 => {
            let mut body_buf = req.body.clone();
            let request = CreatePartitionsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::topics::create_partitions(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        42 => {
            let mut body_buf = req.body.clone();
            let request = DeleteGroupsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::admin::delete_groups(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        29 => {
            let mut body_buf = req.body.clone();
            let request = DescribeAclsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::acls::describe_acls(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        30 => {
            let mut body_buf = req.body.clone();
            let request = CreateAclsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::acls::create_acls(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        31 => {
            let mut body_buf = req.body.clone();
            let request = DeleteAclsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::acls::delete_acls(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        47 => {
            let mut body_buf = req.body.clone();
            let request = OffsetDeleteRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::offsets::offset_delete(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        44 => {
            let mut body_buf = req.body.clone();
            let request = IncrementalAlterConfigsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::configs::incremental_alter_configs(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        33 => {
            let mut body_buf = req.body.clone();
            let request = AlterConfigsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::configs::alter_configs(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        35 => {
            let mut body_buf = req.body.clone();
            let request = DescribeLogDirsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::admin::describe_log_dirs(&request, &*crate::storage::open(), &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        46 => {
            let mut body_buf = req.body.clone();
            let request = ListPartitionReassignmentsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = handlers::admin::list_partition_reassignments(&request, &authz)?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        68 => {
            let mut body_buf = req.body.clone();
            let request = ConsumerGroupHeartbeatRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::consumer_group::heartbeat(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        69 => {
            let mut body_buf = req.body.clone();
            let request = ConsumerGroupDescribeRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::consumer_group::describe(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        43 => {
            let mut body_buf = req.body.clone();
            let request = ElectLeadersRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::admin::elect_leaders(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        50 => {
            let mut body_buf = req.body.clone();
            let request =
                DescribeUserScramCredentialsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::admin::describe_user_scram_credentials(&request, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        48 => {
            let mut body_buf = req.body.clone();
            let request = DescribeClientQuotasRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::admin::describe_client_quotas(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        27 => {
            let mut body_buf = req.body.clone();
            let request = WriteTxnMarkersRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::txn::write_txn_markers(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        49 => {
            let mut body_buf = req.body.clone();
            let request = AlterClientQuotasRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::admin::alter_client_quotas(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        51 => {
            let mut body_buf = req.body.clone();
            let request = AlterUserScramCredentialsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::admin::alter_user_scram_credentials(&request, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        76 => {
            let mut body_buf = req.body.clone();
            let request = ShareGroupHeartbeatRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::share_group::heartbeat(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        77 => {
            let mut body_buf = req.body.clone();
            let request = ShareGroupDescribeRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::share_group::describe(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        78 => {
            let mut body_buf = req.body.clone();
            let request = ShareFetchRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    let store = crate::storage::open();
                    handlers::share_group::share_fetch(&request, &*store, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        79 => {
            let mut body_buf = req.body.clone();
            let request = ShareAcknowledgeRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::share_group::share_acknowledge(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        75 => {
            let mut body_buf = req.body.clone();
            let request = DescribeTopicPartitionsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::metadata::describe_topic_partitions(&request, &cfg, &authz)
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        61 => {
            let mut body_buf = req.body.clone();
            let request = DescribeProducersRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| {
                    handlers::introspect::describe_producers(
                        &request,
                        &*crate::storage::open(),
                        &authz,
                    )
                })
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        65 => {
            let mut body_buf = req.body.clone();
            let request = DescribeTransactionsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::introspect::describe_transactions(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        66 => {
            let mut body_buf = req.body.clone();
            let request = ListTransactionsRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = BackgroundWorker::transaction(|| {
                crate::dbtx::guarded(|| handlers::introspect::list_transactions(&request, &authz))
            })?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        60 => {
            let mut body_buf = req.body.clone();
            let request = DescribeClusterRequest::decode(&mut body_buf, req.api_version)?;
            let authz = crate::acl::Authz {
                acls: &srv.acls,
                principal: principal_of(srv, conn_id),
            };
            let body = handlers::admin::describe_cluster(&request, cfg, &authz)?;
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        17 => {
            let mut body_buf = req.body.clone();
            let request = SaslHandshakeRequest::decode(&mut body_buf, req.api_version)?;
            let state = conn_sasl(srv, conn_id);
            let (body, next) = handlers::auth::handshake(&request, &state);
            if let Some(c) = srv.conns.get_mut(&conn_id) {
                match next {
                    Some(next) => {
                        // The accepted version decides the framing of everything after
                        c.sasl_raw = req.api_version == 0;
                        c.sasl = next;
                    }
                    None => c.sasl_failures += 1,
                }
            }
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        36 => {
            let mut body_buf = req.body.clone();
            let request = SaslAuthenticateRequest::decode(&mut body_buf, req.api_version)?;
            // Credentials are checked against pg_authid, so this needs a transaction —
            let state = conn_sasl(srv, conn_id);
            // `contained`, not `guarded`: this reads pg_authid and touches none of our
            let (body, next) = BackgroundWorker::transaction(|| {
                crate::dbtx::contained(|| Ok(handlers::auth::authenticate(&request, &state)))
            })?;
            if let Some(c) = srv.conns.get_mut(&conn_id) {
                match next {
                    Some(next) => {
                        if let crate::sasl::SaslState::Authenticated { principal } = &next {
                            log!("kafgres: {} authenticated as '{principal}'", c.peer);
                        }
                        c.sasl = next;
                    }
                    // A failed proof leaves the state at AwaitingFinal, so the same
                    None => c.sasl_failures += 1,
                }
            }
            handlers::write_response(out, req.api_key, req.api_version, req.correlation_id, &body)?;
            Ok(Disposition::Reply)
        }
        // Unreachable: negotiate() already rejected anything not in ADVERTISED, and
        other => Err(kafgres_codec::CodecError::UnknownApiKey(other).into()),
    }
}

/// The connection's SASL state, cloned so the handler is not holding a borrow of `srv`
fn conn_sasl(srv: &Server, id: i32) -> crate::sasl::SaslState {
    srv.conns
        .get(&id)
        .map(|c| c.sasl.clone())
        .unwrap_or(crate::sasl::SaslState::AwaitingHandshake)
}

type FetchOutcome = (
    kafgres_codec::generated::fetch_response::FetchResponse,
    Vec<(u32, i32)>,
);

/// Read, and optionally resolve the partitions a park would watch — in one
fn run_fetch(
    request: &FetchRequest,
    authz: &crate::acl::Authz,
    want_watching: bool,
) -> Result<FetchOutcome, HandlerError> {
    BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| {
            let store = crate::storage::open();
            let body = handlers::fetch::handle(request, &*store, authz)?;
            let watching = if want_watching {
                handlers::fetch::watched_partitions(request)?
            } else {
                Vec::new()
            };
            Ok((body, watching))
        })
    })
}

/// How long to hold a Fetch, clamped. Zero means answer immediately.
fn fetch_wait(max_wait_ms: i32) -> Duration {
    if max_wait_ms <= 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(max_wait_ms as u64).min(MAX_FETCH_WAIT)
    }
}

/// Evict members that stopped heartbeating, and cut join windows whose deadline passed.
fn expire_group_members(srv: &mut Server) {
    const EVERY_N_TICKS: u64 = 100; // ~500ms at the default 5ms tick
    if srv.tick % EVERY_N_TICKS != 0 {
        return;
    }
    let awaiting: Vec<(String, String)> = srv
        .parked
        .iter()
        .filter_map(|p| match &p.waiting {
            Waiting::Join {
                group_id,
                member_id,
            }
            | Waiting::Sync {
                group_id,
                member_id,
            } => Some((group_id.clone(), member_id.clone())),
            Waiting::Fetch { .. } => None,
        })
        .collect();

    let changed = BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| {
            crate::group::touch_parked(&awaiting)?;
            let swept = crate::group::sweep()?;
            let due = crate::group::groups_past_deadline()?;
            Ok((swept, due))
        })
    });
    match changed {
        Ok((swept, due)) => {
            srv.groups_changed.extend(swept);
            srv.groups_changed.extend(due);
        }
        Err(e) => log!("kafgres: group sweep failed: {e}"),
    }
}

/// Drop idempotent-producer state that is idle or over the retained-id ceiling.
fn expire_producer_state(srv: &mut Server) {
    const EVERY_N_TICKS: u64 = 12_000; // ~60s at the default 5ms tick
    const WHILE_DRAINING: u64 = 100; //  ~500ms, same cadence as the group sweep

    // A backlog is drained a batch at a time across many ticks, never in one statement.
    let every = if srv.producer_backlog {
        WHILE_DRAINING
    } else {
        EVERY_N_TICKS
    };
    if srv.tick % every != 0 {
        return;
    }

    let policy = crate::producer_retention();
    let swept = BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| crate::producer::sweep(policy).map_err(Into::into))
    });
    match swept {
        Ok(n) => {
            srv.producer_backlog = n >= crate::producer::SWEEP_BATCH as u64;
            if n > 0 {
                log!("kafgres: dropped state for {n} idle or surplus producer(s)");
            }
        }
        Err(e) => {
            // Do not keep retrying at the draining cadence against a statement that just
            srv.producer_backlog = false;
            log!("kafgres: producer sweep failed: {e}");
        }
    }
}

/// Raise every partition to the leader epoch this timeline implies.
fn raise_leader_epochs() -> bool {
    let result = BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| {
            // The timeline increments on `pg_promote`. Epoch 0 on a cluster that has
            let timeline: i32 = pgrx::Spi::get_one(
                "SELECT (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int)",
            )?
            .unwrap_or(1);
            let epoch = timeline - 1;

            let partitions = crate::meta::all_partitions()?;
            let mut store = crate::storage::open();
            let mut raised = 0usize;
            for (topic_id, partition) in partitions {
                // No per-partition error arm: a lock or statement timeout in here is a
                if crate::storage::LogStore::set_leader_epoch(
                    &mut *store, topic_id, partition, epoch,
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?
                {
                    raised += 1;
                }
            }
            Ok((epoch, raised))
        })
    });

    match result {
        Ok((epoch, 0)) => {
            log!("kafgres: leader epoch {epoch} already current for every partition");
            true
        }
        Ok((epoch, n)) => {
            log!("kafgres: promoted to leader epoch {epoch} for {n} partition(s)");
            true
        }
        Err(e) => {
            log!("kafgres: could not raise leader epochs, not serving yet: {e}");
            false
        }
    }
}

/// Reload the ACL table when the cached snapshot has aged out.
fn refresh_acls(srv: &mut Server) {
    if !srv.acls.is_stale() {
        return;
    }
    let enabled = crate::acls_enabled();
    let supers = crate::superusers();
    let loaded = BackgroundWorker::transaction(|| {
        crate::dbtx::contained(|| crate::acl::AclCache::load(enabled, &supers).map_err(Into::into))
    });
    match loaded {
        Ok(fresh) => srv.acls = fresh,
        // Keep the previous snapshot. Failing open would drop every rule the moment the
        Err(e) => {
            srv.acls.mark_attempted();
            log!("kafgres: acl reload failed, keeping the previous snapshot: {e}");
        }
    }
}

/// Rebuild the TLS configuration after a SIGHUP.
fn reload_tls(srv: &mut Server) {
    match crate::tls_setup() {
        Ok(Some(setup)) => {
            let was = srv.tls.as_ref().map(|t| t.client_auth);
            if was != Some(setup.client_auth) {
                log!(
                    "kafgres: TLS reloaded, client certificates {:?}",
                    setup.client_auth
                );
            }
            srv.tls = Some(setup);
        }
        Ok(None) => {
            if srv.tls.is_some() {
                log!("kafgres: TLS configuration removed; new connections are plaintext");
            }
            srv.tls = None;
        }
        Err(e) => log!("kafgres: TLS reload failed, keeping the previous configuration: {e}"),
    }
}

/// Close connections that have not authenticated within `AUTH_DEADLINE`.
fn drop_unauthenticated(srv: &mut Server) {
    const EVERY_N_TICKS: u64 = 200; // ~1s at the default 5ms tick
    if srv.tick % EVERY_N_TICKS != 0 || !crate::sasl_required() {
        return;
    }
    let now = Instant::now();
    // The same predicate the gate uses, or this reaps exactly the connections mTLS was
    let stale: Vec<i32> = srv
        .conns
        .iter()
        .filter(|(id, c)| {
            !authenticated(srv, **id)
                && !matches!(c.sasl, crate::sasl::SaslState::Authenticated { .. })
                && now.duration_since(c.opened_at) > AUTH_DEADLINE
        })
        .map(|(id, _)| *id)
        .collect();
    for id in stale {
        if let Some(c) = srv.conns.get(&id) {
            log!("kafgres: {} did not authenticate in time; closing", c.peer);
        }
        drop_conn(srv, id);
    }
}

fn expire_share_group_state(srv: &Server) {
    const EVERY_N_TICKS: u64 = 1_000; // ~5s at the default 5ms tick
    if srv.tick % EVERY_N_TICKS != 0 {
        return;
    }
    if let Err(e) = BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(crate::handlers::share_group::expire)
    }) {
        log!("kafgres: could not expire share group state: {e}");
    }
}

fn expire_consumer_group_members(srv: &Server) {
    const EVERY_N_TICKS: u64 = 1_000; // ~5s at the default 5ms tick
    if srv.tick % EVERY_N_TICKS != 0 {
        return;
    }
    if let Err(e) = BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(crate::handlers::consumer_group::expire_members)
    }) {
        log!("kafgres: could not expire consumer group members: {e}");
    }
}

fn enforce_retention(srv: &mut Server) {
    const EVERY_N_TICKS: u64 = 12_000; // ~60s at the default 5ms tick
    if srv.tick % EVERY_N_TICKS != 0 {
        return;
    }
    if let Err(e) = BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(crate::handlers::txn::expire_stale_transactions)
    }) {
        log!("kafgres: could not expire stale transactions: {e}");
    }

    let cursor = srv.retention_cursor;
    match BackgroundWorker::transaction(|| {
        crate::dbtx::guarded(|| crate::retention::sweep(cursor).map_err(Into::into))
    }) {
        Ok(batch) => srv.retention_cursor = batch.next,
        Err(e) => {
            srv.retention_cursor = cursor.saturating_add(1);
            log!("kafgres: retention sweep failed past topic {cursor}: {e}");
        }
    }
}

fn complete_parked(srv: &mut Server, _cfg: &ClusterConfig) {
    if srv.parked.is_empty() {
        srv.appended.clear();
        srv.groups_changed.clear();
        return;
    }

    let now = Instant::now();
    let appended = std::mem::take(&mut srv.appended);
    let groups_changed = std::mem::take(&mut srv.groups_changed);

    let pending = std::mem::take(&mut srv.parked);
    let mut still_parked = Vec::with_capacity(pending.len());

    for p in pending {
        if !srv.conns.contains_key(&p.conn_id) {
            continue;
        }
        let expired = now >= p.deadline;

        let encoded = match &p.waiting {
            Waiting::Fetch {
                request,
                watching,
                min_bytes,
            } => {
                let woken = watching.iter().any(|k| appended.contains(k));
                if !expired && !woken {
                    still_parked.push(p);
                    continue;
                }
                let authz = crate::acl::Authz {
                    acls: &srv.acls,
                    principal: principal_of(srv, p.conn_id),
                };
                let fetched = run_fetch(request, &authz, false);
                drop(authz);
                match fetched {
                    Ok((mut body, _)) => {
                        if !expired && handlers::fetch::records_bytes(&body) < *min_bytes {
                            still_parked.push(p);
                            continue;
                        }
                        let served = handlers::fetch::records_bytes(&body) as i64;
                        body.throttle_time_ms = charge(
                            srv,
                            crate::quota::Rate::Consumer,
                            p.conn_id,
                            &p.client_id,
                            served,
                        );
                        encode_parked(&p, &body)
                    }
                    Err(e) => {
                        log!("kafgres: parked fetch failed: {e}");
                        drop_conn(srv, p.conn_id);
                        continue;
                    }
                }
            }

            Waiting::Join {
                group_id,
                member_id,
            } => {
                if !expired && !groups_changed.contains(group_id) {
                    still_parked.push(p);
                    continue;
                }
                let group_id = group_id.clone();
                let member_id = member_id.clone();
                let ready = BackgroundWorker::transaction(|| {
                    crate::dbtx::guarded(|| {
                        let g = crate::group::load(&group_id)?;
                        let state = g.as_ref().map(|g| g.state);
                        if state == Some(crate::group::GroupState::PreparingRebalance)
                            && (expired || crate::group::join_window_closed(&group_id)?)
                        {
                            if crate::group::complete_join(&group_id)?.is_err() {
                                return Ok(Some(handlers::join_sync::error_join(
                                    kafgres_codec::ErrorCode::InconsistentGroupProtocol,
                                    member_id.clone(),
                                )));
                            }
                        }
                        let g = crate::group::load(&group_id)?;
                        match g.map(|g| g.state) {
                            Some(crate::group::GroupState::PreparingRebalance) | None => Ok(None),
                            _ => Ok(Some(handlers::join_sync::join_response(
                                &group_id,
                                &member_id,
                            )?)),
                        }
                    })
                });
                match ready {
                    Ok(Some(body)) => encode_parked(&p, &body),
                    Ok(None) => {
                        still_parked.push(p);
                        continue;
                    }
                    Err(e) => {
                        log!("kafgres: parked join failed: {e}");
                        drop_conn(srv, p.conn_id);
                        continue;
                    }
                }
            }

            Waiting::Sync {
                group_id,
                member_id,
            } => {
                if !expired && !groups_changed.contains(group_id) {
                    still_parked.push(p);
                    continue;
                }
                let group_id = group_id.clone();
                let member_id = member_id.clone();
                let ready = BackgroundWorker::transaction(|| {
                    crate::dbtx::guarded(|| {
                        handlers::join_sync::sync_response(&group_id, &member_id)
                    })
                });
                match ready {
                    Ok(Some(body)) => encode_parked(&p, &body),
                    Ok(None) if !expired => {
                        still_parked.push(p);
                        continue;
                    }
                    Ok(None) => encode_parked(
                        &p,
                        &handlers::join_sync::error_sync(
                            kafgres_codec::ErrorCode::RebalanceInProgress,
                        ),
                    ),
                    Err(e) => {
                        log!("kafgres: parked sync failed: {e}");
                        drop_conn(srv, p.conn_id);
                        continue;
                    }
                }
            }
        };

        match encoded {
            Ok(out) => {
                if let Some(c) = srv.conns.get_mut(&p.conn_id) {
                    c.complete(p.seq, out);
                }
            }
            Err(e) => {
                log!("kafgres: encoding a parked response failed: {e}");
                drop_conn(srv, p.conn_id);
            }
        }
    }

    srv.parked = still_parked;
}

fn encode_parked<T: kafgres_codec::Encodable>(
    p: &Parked,
    body: &T,
) -> Result<BytesMut, HandlerError> {
    let mut out = BytesMut::new();
    handlers::write_response(&mut out, p.api_key, p.api_version, p.correlation_id, body)?;
    Ok(out)
}

fn flush_all(srv: &mut Server) {
    let ids: Vec<i32> = srv.conns.keys().copied().collect();
    for id in ids {
        let ok = srv.conns.get_mut(&id).map(|c| c.flush()).unwrap_or(true);
        if !ok {
            drop_conn(srv, id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_wait_is_clamped_and_zero_means_now() {
        assert_eq!(fetch_wait(0), Duration::ZERO);
        assert_eq!(fetch_wait(-1), Duration::ZERO);
        assert_eq!(fetch_wait(500), Duration::from_millis(500));
        assert_eq!(fetch_wait(i32::MAX), MAX_FETCH_WAIT);
    }
}
