//! kafgres — a singleton Kafka broker embedded in Postgres.

use std::ffi::CString;
use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pub mod acl;
pub mod archive;
pub mod cdc;
pub mod decoding;
pub mod config;
mod dbtx;
pub mod group;
pub mod handlers;
mod init010;
mod init020;
mod init030;
mod init040;
mod init050;
mod init060;
mod init070;
mod init080;
mod init090;
mod init100;
mod init110;
mod init120;
mod init130;
mod init140;
pub mod quota;
pub mod meta;
pub mod produce_sql;
pub mod producer;
pub mod replication;
pub mod retention;
pub mod sasl;
pub mod tls;
pub mod server;
pub mod storage;

pgrx::pg_module_magic!();

/// Arbitrary, but it must stay stable: a changed key means two workers stop excluding
const SCHEMA_MIGRATION_LOCK_KEY: i64 = 0x7047_4B41_0000_0001u64 as i64;

static DATABASE_NAME: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static BROKER_PORT: GucSetting<i32> = GucSetting::<i32>::new(9092);
static BIND_HOST: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static ADVERTISED_HOST: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static ADVERTISED_PORT: GucSetting<i32> = GucSetting::<i32>::new(0);
static NODE_ID: GucSetting<i32> = GucSetting::<i32>::new(1);
static CLUSTER_ID: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static TICK_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(5);

static STORAGE_ENGINE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"segment"));

static SEGMENT_BYTES: GucSetting<i32> = GucSetting::<i32>::new(64 * 1024 * 1024);

static SEGMENT_LOCK_STRIPES: GucSetting<i32> = GucSetting::<i32>::new(16);

static REPLICATE_FROM: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

static ALLOW_TXN_PRODUCE: GucSetting<bool> = GucSetting::<bool>::new(true);

static MAX_REQUEST_BYTES: GucSetting<i32> = GucSetting::<i32>::new(32 * 1024 * 1024);

static ALLOW_ENGINE_MISMATCH: GucSetting<bool> = GucSetting::<bool>::new(false);

static AUTO_CREATE_TOPICS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// `0` stops draining but leaves the worker running: the slot then stops advancing and
static CDC_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(1000);

/// Bounds transactions, not changes: `upto_nchanges` is only consulted at commit
static CDC_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static CDC_SNAPSHOT_BATCH_ROWS: GucSetting<i32> = GucSetting::<i32>::new(1_000);
static SHARE_LOCK_DURATION_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);

static SEGMENT_ARCHIVE_COMMAND: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

static ARCHIVE_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(10_000);

static PRODUCER_ID_EXPIRATION_MS: GucSetting<i32> = GucSetting::<i32>::new(86_400_000);

static MAX_PRODUCER_IDS: GucSetting<i32> = GucSetting::<i32>::new(10_000);

static SEGMENT_OFFSETS: GucSetting<i32> = GucSetting::<i32>::new(1_000_000);

static SASL_REQUIRED: GucSetting<bool> = GucSetting::<bool>::new(false);

static ACLS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
static SUPERUSERS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

static TLS_CERT_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static TLS_KEY_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static TLS_CA_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static TLS_CLIENT_CERT_REQUIRED: GucSetting<bool> = GucSetting::<bool>::new(false);

fn cstr_guc(g: &GucSetting<Option<CString>>) -> Option<String> {
    g.get()
        .map(|c| c.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
}

pub fn database_guc() -> String {
    cstr_guc(&DATABASE_NAME).unwrap_or_else(|| "postgres".to_string())
}

pub fn broker_port_guc() -> u16 {
    BROKER_PORT.get() as u16
}

fn bind_host_guc() -> String {
    cstr_guc(&BIND_HOST).unwrap_or_else(|| "0.0.0.0".to_string())
}

fn advertised_host_guc() -> String {
    cstr_guc(&ADVERTISED_HOST).unwrap_or_else(|| "localhost".to_string())
}

fn advertised_port_guc() -> i32 {
    let p = ADVERTISED_PORT.get();
    if p > 0 {
        p
    } else {
        BROKER_PORT.get()
    }
}

fn cluster_id_guc() -> String {
    cstr_guc(&CLUSTER_ID).unwrap_or_else(|| "kafgres-cluster".to_string())
}

fn tick_interval() -> Duration {
    Duration::from_millis(TICK_INTERVAL_MS.get().max(1) as u64)
}

fn cdc_interval() -> Duration {
    match CDC_INTERVAL_MS.get() {
        0 => Duration::ZERO,
        ms => Duration::from_millis(ms as u64),
    }
}

fn cdc_batch_size() -> i32 {
    CDC_BATCH_SIZE.get().max(1)
}

pub fn cdc_snapshot_batch_rows() -> i32 {
    CDC_SNAPSHOT_BATCH_ROWS.get().max(1)
}

pub fn share_lock_duration_ms() -> i64 {
    SHARE_LOCK_DURATION_MS.get().max(1_000) as i64
}

pub fn segment_archive_command() -> String {
    SEGMENT_ARCHIVE_COMMAND
        .get()
        .map(|c| c.to_string_lossy().trim().to_string())
        .unwrap_or_default()
}

fn archive_interval() -> Duration {
    match ARCHIVE_INTERVAL_MS.get() {
        0 => Duration::ZERO,
        ms => Duration::from_millis(ms as u64),
    }
}

/// `sighup_received()` only reports that SIGHUP arrived — nothing in pgrx calls
pub fn reload_config() {
    unsafe {
        pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
    }
}

pub fn segment_offsets() -> i64 {
    SEGMENT_OFFSETS.get().max(1) as i64
}

pub fn tls_setup() -> Result<Option<crate::tls::TlsSetup>, crate::tls::TlsError> {
    crate::tls::build(
        cstr_guc(&TLS_CERT_FILE).as_deref(),
        cstr_guc(&TLS_KEY_FILE).as_deref(),
        cstr_guc(&TLS_CA_FILE).as_deref(),
        TLS_CLIENT_CERT_REQUIRED.get(),
    )
}

pub fn acls_enabled() -> bool {
    ACLS_ENABLED.get()
}

pub fn superusers() -> String {
    cstr_guc(&SUPERUSERS).unwrap_or_default()
}

pub fn cluster_config() -> handlers::metadata::ClusterConfig {
    handlers::metadata::ClusterConfig {
        node_id: NODE_ID.get(),
        host: advertised_host_guc(),
        port: advertised_port_guc(),
        cluster_id: cluster_id_guc(),
    }
}

pub fn allow_transactional_produce() -> bool {
    ALLOW_TXN_PRODUCE.get()
}

pub fn segment_lock_stripes() -> usize {
    SEGMENT_LOCK_STRIPES.get().max(1) as usize
}

pub fn replicate_from() -> Option<(String, i32)> {
    let raw = REPLICATE_FROM.get()?;
    let raw = raw.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    let (host, port) = raw.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

pub fn node_id() -> i32 {
    NODE_ID.get()
}

pub fn segment_bytes() -> u64 {
    SEGMENT_BYTES.get().max(1024) as u64
}

pub fn max_request_bytes() -> usize {
    MAX_REQUEST_BYTES.get().max(0) as usize
}

pub fn allow_engine_mismatch() -> bool {
    ALLOW_ENGINE_MISMATCH.get()
}

pub fn auto_create_topics() -> bool {
    AUTO_CREATE_TOPICS.get()
}

pub fn storage_engine_guc() -> String {
    STORAGE_ENGINE
        .get()
        .and_then(|c| c.to_str().ok().map(|s| s.trim().to_ascii_lowercase()))
        .unwrap_or_else(|| "segment".to_string())
}

pub fn sasl_required() -> bool {
    SASL_REQUIRED.get()
}

pub fn producer_retention() -> crate::producer::Retention {
    crate::producer::Retention {
        expiration_ms: PRODUCER_ID_EXPIRATION_MS.get() as i64,
        max_ids: MAX_PRODUCER_IDS.get() as i64,
    }
}

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"kafgres.database",
        c"Database the kafgres background worker connects to (default 'postgres'; requires BGW restart)",
        c"",
        &DATABASE_NAME,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.port",
        c"TCP port the Kafka listener binds",
        c"",
        &BROKER_PORT,
        1,
        65535,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.bind_host",
        c"Address the Kafka listener binds (default '0.0.0.0'; requires BGW restart)",
        c"",
        &BIND_HOST,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.advertised_host",
        c"Host clients are told to connect to, the advertised.listeners equivalent (default 'localhost')",
        c"",
        &ADVERTISED_HOST,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.advertised_port",
        c"Port clients are told to connect to; 0 means use kafgres.port",
        c"",
        &ADVERTISED_PORT,
        0,
        65535,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.node_id",
        c"Broker node id reported in Metadata",
        c"",
        &NODE_ID,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.cluster_id",
        c"Cluster id reported in Metadata (default 'kafgres-cluster')",
        c"",
        &CLUSTER_ID,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.tick_interval_ms",
        c"Broker event loop poll interval in milliseconds (1-1000, default 5)",
        c"",
        &TICK_INTERVAL_MS,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.segment_offsets",
        c"Offsets per log segment, the retention granularity (default 1000000). \
          Set before a partition has data: changing it later makes segment ranges overlap.",
        c"",
        &SEGMENT_OFFSETS,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.replicate_from",
        c"host:port of the leader to pull log from on a standby (segment engine only); empty disables",
        c"",
        &REPLICATE_FROM,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.max_request_bytes",
        c"Largest inbound request frame, as Kafka's socket.request.max.bytes. A bounded number of connections may exceed the 8 MiB free tier at a time",
        c"",
        &MAX_REQUEST_BYTES,
        1024 * 1024,
        100 * 1024 * 1024,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"kafgres.allow_engine_mismatch",
        c"Start even if a log written by the other storage engine is present. That log stays intact but invisible",
        c"",
        &ALLOW_ENGINE_MISMATCH,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"kafgres.auto_create_topics",
        c"Create a topic the first time a client produces to or fetches from it, as Kafka's auto.create.topics.enable does",
        c"",
        &AUTO_CREATE_TOPICS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"kafgres.allow_transactional_produce",
        c"Enable kafgres_produce(), the transactional SQL produce path (segment engine only)",
        c"",
        &ALLOW_TXN_PRODUCE,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"kafgres.segment_archive_command",
        c"Shell command shipping one rolled segment to an archive; %p is its path, %f its filename. Empty disables archiving. Setting it makes retention wait for the archive",
        c"",
        &SEGMENT_ARCHIVE_COMMAND,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.archive_interval_ms",
        c"How often the archiver ships sealed segments; 0 disables it",
        c"",
        &ARCHIVE_INTERVAL_MS,
        0,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.cdc_interval_ms",
        c"How often the CDC worker drains the logical replication slot; 0 disables draining",
        c"",
        &CDC_INTERVAL_MS,
        0,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.cdc_batch_size",
        c"Changes peeked from the CDC slot per drain",
        c"",
        &CDC_BATCH_SIZE,
        1,
        10_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.cdc_snapshot_batch_rows",
        c"Source rows read per CDC snapshot batch; each batch is one transaction",
        c"",
        &CDC_SNAPSHOT_BATCH_ROWS,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.share_record_lock_duration_ms",
        c"How long a share-group consumer holds an acquired record before it is offered again",
        c"",
        &SHARE_LOCK_DURATION_MS,
        1_000,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.segment_lock_stripes",
        c"Lock shards for segment engine append positions; 1 makes every partition share one lock. Narrowing also narrows capacity",
        c"",
        &SEGMENT_LOCK_STRIPES,
        1,
        16,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"kafgres.segment_bytes",
        c"Bytes a segment file reaches before rolling (segment engine)",
        c"",
        &SEGMENT_BYTES,
        1024,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"kafgres.storage_engine",
        c"Log storage engine: 'segment' (default) or 'table'. Startup only; does not migrate existing data",
        c"",
        &STORAGE_ENGINE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"kafgres.tls_cert_file",
        c"PEM server certificate chain. TLS is enabled when this and tls_key_file are both set (requires BGW restart)",
        c"",
        &TLS_CERT_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.tls_key_file",
        c"PEM private key for tls_cert_file (requires BGW restart)",
        c"",
        &TLS_KEY_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.tls_ca_file",
        c"PEM CA bundle that client certificates are verified against; enables mTLS",
        c"",
        &TLS_CA_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"kafgres.tls_client_cert_required",
        c"Refuse the TLS handshake unless the client presents a certificate valid against tls_ca_file",
        c"",
        &TLS_CLIENT_CERT_REQUIRED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"kafgres.acls_enabled",
        c"Enforce kafgres_acls. Off by default: with it on and no matching rule, the answer is refusal",
        c"",
        &ACLS_ENABLED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"kafgres.superusers",
        c"Semicolon-separated principals that bypass every ACL check, e.g. 'User:admin'",
        c"",
        &SUPERUSERS,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_bool_guc(
        c"kafgres.sasl_required",
        c"Require SASL/SCRAM-SHA-256 authentication against pg_authid roles (default off)",
        c"",
        &SASL_REQUIRED,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.producer_id_expiration_ms",
        c"Drop idempotent-producer state idle this long (default 24h, 0 disables)",
        c"",
        &PRODUCER_ID_EXPIRATION_MS,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"kafgres.max_producer_ids",
        c"Ceiling on retained producer ids; the least recently used are dropped first \
          (default 10000, 0 disables)",
        c"",
        &MAX_PRODUCER_IDS,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );

    // Registered unconditionally rather than only when `storage_engine=segment`: shared
    crate::storage::segment::init_shmem();

    // The broker starts at `RecoveryFinished` so a standby never serves partitions; the
    BackgroundWorkerBuilder::new("kafgres_follower")
        .set_function("kafgres_follower_worker_main")
        .set_library("kafgres")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .set_restart_time(Some(Duration::from_secs(10)))
        .set_argument(0i32.into_datum())
        .load();

    BackgroundWorkerBuilder::new("kafgres_cdc")
        .set_function("kafgres_cdc_worker_main")
        .set_library("kafgres")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(10)))
        .set_argument(0i32.into_datum())
        .load();

    BackgroundWorkerBuilder::new("kafgres_archiver")
        .set_function("kafgres_archiver_worker_main")
        .set_library("kafgres")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(15)))
        .set_argument(0i32.into_datum())
        .load();

    BackgroundWorkerBuilder::new("kafgres_broker")
        .set_function("kafgres_broker_worker_main")
        .set_library("kafgres")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .set_argument(0i32.into_datum())
        .load();
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn kafgres_broker_worker_main(arg: pg_sys::Datum) {
    let slot = i32::from_datum(arg, false).unwrap_or(0);
    let db_name = database_guc();

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

    log!(
        "kafgres: broker worker starting (slot {}), connected to '{}'",
        slot,
        db_name
    );

    // Outside `guarded` on purpose: guarded call sites substitute a fixed error for the
    if let Err(e) = storage::check_engine_name() {
        error!("kafgres: {e}");
    }

    BackgroundWorker::transaction(|| {
        ensure_tables_exist();
    });

    // Before the listener opens, so no client can observe the empty topics a mismatch produces.
    if let Err(e) = BackgroundWorker::transaction(storage::check_engine_data) {
        error!("kafgres: {e}");
    }

    let cfg = cluster_config();
    log!(
        "kafgres: advertising {}:{} as node {} in cluster '{}'",
        cfg.host,
        cfg.port,
        cfg.node_id,
        cfg.cluster_id
    );

    server::run(cfg, &bind_host_guc(), broker_port_guc(), tick_interval());
}

/// Own worker: a drain runs user-supplied SQL that must not stall the broker's loop.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn kafgres_cdc_worker_main(_arg: pg_sys::Datum) {
    let db_name = database_guc();
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

    let mut ready = false;
    let mut interval = cdc_interval();

    while BackgroundWorker::wait_latch(Some(if interval.is_zero() {
        Duration::from_secs(1)
    } else {
        interval
    })) {
        if BackgroundWorker::sighup_received() {
            reload_config();
            interval = cdc_interval();
        }
        if interval.is_zero() {
            continue;
        }

        if !ready {
            ready = BackgroundWorker::transaction(|| {
                Spi::get_one::<bool>("SELECT to_regclass('kafgres_cdc_mappings') IS NOT NULL")
                    .ok()
                    .flatten()
                    .unwrap_or(false)
            });
            if !ready {
                continue;
            }
            log!("kafgres: CDC worker ready");
        }

        // Not wrapped in `atomically`: an ephemeral slot is only cleaned up at top-level
        let slot = BackgroundWorker::transaction(cdc::ensure_slot);
        match slot {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => {
                log!("kafgres: CDC: {e}");
                continue;
            }
        }

        // Snapshots drain first: a snapshot row read alongside newer changes can land
        let snapshotting = BackgroundWorker::transaction(cdc::snapshot_outstanding);
        if snapshotting {
            match cdc::snapshot_worker() {
                Ok(_) => {}
                Err(e) => log!("kafgres: CDC snapshot: {e}"),
            }
            continue;
        }

        // `drain_worker` manages its own transaction per mapping, so no lock spans mappings.
        let drained = cdc::drain_worker(cdc_batch_size());
        match drained {
            Ok(n) if n > 0 => log!("kafgres: CDC produced {n} record(s)"),
            Ok(_) => {}
            Err(e) => log!("kafgres: CDC drain: {e}"),
        }
    }
}

/// Own worker: the archive command is an operator-supplied program of unbounded duration.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn kafgres_archiver_worker_main(_arg: pg_sys::Datum) {
    let db_name = database_guc();
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

    let mut ready = false;
    let mut interval = archive_interval();

    while BackgroundWorker::wait_latch(Some(if interval.is_zero() {
        Duration::from_secs(10)
    } else {
        interval
    })) {
        if BackgroundWorker::sighup_received() {
            reload_config();
            interval = archive_interval();
        }
        if interval.is_zero() || !archive::enabled() {
            continue;
        }

        if !ready {
            ready = BackgroundWorker::transaction(|| {
                Spi::get_one::<bool>("SELECT to_regclass('kafgres_segment_archive') IS NOT NULL")
                    .ok()
                    .flatten()
                    .unwrap_or(false)
            });
            if !ready {
                continue;
            }
            log!("kafgres: segment archiver ready");
        }

        let shipped = BackgroundWorker::transaction(|| {
            crate::dbtx::atomically(archive::archive_once, |caught| caught.to_string())
        });
        match shipped {
            Ok(n) if n > 0 => log!("kafgres: archived {n} segment(s)"),
            Ok(_) => {}
            Err(e) => log!("kafgres: archiver: {e}"),
        }
    }
}

/// `pub(crate)` for tests: a `#[pg_test]` backend has no worker to create the schema for it.
pub(crate) fn ensure_tables_exist() {
    // Serialise worker startups: two workers can race the same CREATE TABLE IF NOT EXISTS.
    let _ = Spi::run(&format!(
        "SELECT pg_advisory_xact_lock({SCHEMA_MIGRATION_LOCK_KEY})"
    ));
    init010::init_010();
    init020::init_020();
    init030::init_030();
    init040::init_040();
    init050::init_050();
    init060::init_060();
    init070::init_070();
    init080::init_080();
    init090::init_090();
    init100::init_100();
    init110::init_110();
    init120::init_120();
    init130::init_130();
    init140::init_140();
}

#[pg_extern]
fn kafgres_kafka_version() -> &'static str {
    include_str!("../../codec/KAFKA_VERSION").trim_end()
}

#[pg_extern]
fn kafgres_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Safe while the broker runs; under `guarded` with `NOWAIT` because both sweeps contend
#[pg_extern]
fn kafgres_expire_producers() -> i64 {
    let swept = crate::dbtx::guarded(|| crate::producer::sweep(producer_retention()).map_err(Into::into));
    match swept {
        Ok(n) => n as i64,
        Err(e) => error!("kafgres: producer sweep failed: {e}"),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn extension_loads_and_reports_its_pin() {
        assert_eq!(crate::kafgres_kafka_version(), "4.3.1");
    }

    #[pg_test]
    fn schema_tables_exist() {
        crate::init010::init_010();
        let n = Spi::get_one::<i64>(
            "SELECT count(*) FROM information_schema.tables
              WHERE table_name IN ('kafgres_topics','kafgres_partitions')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(n, 2);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    /// `_PG_init` registers workers and `PGC_POSTMASTER` GUCs, which a running server
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'kafgres'"]
    }
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn kafgres_follower_worker_main(_arg: pg_sys::Datum) {
    let db_name = database_guc();
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

    if !pg_sys::RecoveryInProgress() {
        return;
    }

    let Some((host, port)) = replicate_from() else {
        log!("kafgres: follower idle — kafgres.replicate_from is not set");
        return;
    };
    if storage_engine_guc() != "segment" {
        log!("kafgres: follower idle — the table engine's log replicates with the WAL");
        return;
    }
    log!("kafgres: follower starting, pulling log from {host}:{port}");

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(500))) {
        if BackgroundWorker::sighup_received() {
            reload_config();
            if replicate_from().is_none() {
                log!("kafgres: follower stopping — replicate_from was cleared");
                return;
            }
        }
        // Stop before the broker starts serving: never two writers on one partition.
        if !pg_sys::RecoveryInProgress() {
            log!("kafgres: follower stopping — this node was promoted, the broker takes over");
            return;
        }

        // A subtransaction (`contained`) needs an xid and a standby cannot assign one —
        let pulled = BackgroundWorker::transaction(|| {
            replication::replicate_once_from(&host, port)
        });
        match pulled {
            Ok(n) if n > 0 => log!("kafgres: follower applied {n} batch(es)"),
            Ok(_) => {}
            Err(e) => {
                log!("kafgres: follower: {e}");
            }
        }
    }
}
