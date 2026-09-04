use std::collections::HashMap;

use pgrx::prelude::*;

#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub partition: i32,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub topic_id: u32,
    pub name: String,
    pub uuid: [u8; 16],
    pub partitions: Vec<PartitionMeta>,
}

/// Take ACCESS SHARE on the metadata tables without waiting: a wait here would freeze
pub fn lock_for_read() -> Result<(), spi::Error> {
    Spi::run("LOCK TABLE kafgres_topics, kafgres_partitions IN ACCESS SHARE MODE NOWAIT")
}

/// Read every topic and its partitions in one ordered query, so a Metadata request is
pub fn load_topics(filter: Option<&[String]>) -> Result<Vec<TopicMeta>, spi::Error> {
    let mut out: Vec<TopicMeta> = Vec::new();

    Spi::connect(|client| {

        // A NULL filter means "every topic"; the array form keeps this to one plan.
        let sql = "SELECT t.topic_id, t.name, p.partition, p.leader_epoch, t.topic_uuid
                     FROM kafgres_topics t
                     LEFT JOIN kafgres_partitions p ON p.topic_id = t.topic_id
                    WHERE $1::text[] IS NULL OR t.name = ANY($1::text[])
                    ORDER BY t.name, p.partition";
        let names: Option<Vec<String>> = filter.map(|f| f.to_vec());
        let table = client.select(sql, None, &[names.into()])?;

        for row in table {
            let topic_id: Option<pg_sys::Oid> = row.get(1)?;
            let name: Option<String> = row.get(2)?;
            let (topic_id, name) = match (topic_id, name) {
                (Some(o), Some(n)) => (o.to_u32(), n),
                _ => continue,
            };
            if out.last().map(|t| t.name.as_str()) != Some(name.as_str()) {
                out.push(TopicMeta {
                    topic_id,
                    name: name.clone(),
                    uuid: to_uuid(row.get::<Vec<u8>>(5)?),
                    partitions: Vec::new(),
                });
            }
            // LEFT JOIN: a topic with no partition rows yields one all-NULL row.
            if let Some(partition) = row.get::<i32>(3)? {
                let leader_epoch = row.get::<i32>(4)?.unwrap_or(0);
                out.last_mut().unwrap().partitions.push(PartitionMeta {
                    partition,
                    leader_epoch,
                });
            }
        }
        Ok::<_, spi::Error>(())
    })?;

    Ok(out)
}

fn to_uuid(raw: Option<Vec<u8>>) -> [u8; 16] {
    let mut out = [0u8; 16];
    if let Some(v) = raw {
        if v.len() == 16 {
            out.copy_from_slice(&v);
        }
    }
    out
}

pub fn topic_by_uuid(uuid: &[u8; 16]) -> Result<Option<(u32, String)>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT topic_id::int, name FROM kafgres_topics WHERE topic_uuid = $1",
            Some(1),
            &[uuid.to_vec().into()],
        )?;
        for row in rows {
            if let (Some(id), Some(name)) = (row.get::<i32>(1)?, row.get::<String>(2)?) {
                return Ok(Some((id as u32, name)));
            }
        }
        Ok(None)
    })
}

#[derive(Debug, Clone)]
pub struct ResolvedTopic {
    pub topic_id: u32,
    pub name: String,
    pub uuid: [u8; 16],
    pub max_message_bytes: i64,
    pub compacted: bool,
}

/// Resolve a topic by name or uuid, whichever the request version carries, and return both so the response echoes the right one.
pub fn resolve_topic(name: &str, uuid: &[u8; 16]) -> Result<Option<ResolvedTopic>, spi::Error> {
    let sql = if name.is_empty() {
        "SELECT topic_id::int, name, topic_uuid,
                (config->>'max.message.bytes')::bigint,
                (config->>'cleanup.policy' = 'compact')
           FROM kafgres_topics WHERE topic_uuid = $1"
    } else {
        "SELECT topic_id::int, name, topic_uuid,
                (config->>'max.message.bytes')::bigint,
                (config->>'cleanup.policy' = 'compact')
           FROM kafgres_topics WHERE name = $2"
    };
    Spi::connect(|client| {
        let rows = client.select(sql, Some(1), &[uuid.to_vec().into(), name.into()])?;
        for row in rows {
            if let (Some(id), Some(n)) = (row.get::<i32>(1)?, row.get::<String>(2)?) {
                return Ok(Some(ResolvedTopic {
                    topic_id: id as u32,
                    name: n,
                    uuid: to_uuid(row.get::<Vec<u8>>(3)?),
                    max_message_bytes: row
                        .get::<i64>(4)?
                        .unwrap_or(crate::config::DEFAULT_MAX_MESSAGE_BYTES),
                    compacted: row.get::<bool>(5)?.unwrap_or(false),
                }));
            }
        }
        Ok(None)
    })
}

pub fn all_partitions() -> Result<Vec<(u32, i32)>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT topic_id::int, partition FROM kafgres_partitions
              ORDER BY topic_id, partition",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let (Some(t), Some(p)) = (row.get::<i32>(1)?, row.get::<i32>(2)?) {
                out.push((t as u32, p));
            }
        }
        Ok(out)
    })
}

pub fn topic_id_by_name(name: &str) -> Result<Option<u32>, spi::Error> {
    Ok(
        Spi::get_one_with_args::<i32>(
            // Scalar subquery: NULL when absent — a bare WHERE returns zero rows, which pgrx reports as an error.
            "SELECT (SELECT topic_id::int FROM kafgres_topics WHERE name = $1)",
            &[name.into()],
        )?
        .map(|v| v as u32),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicError {
    InvalidName(String),
    InvalidPartitions(i32),
    InvalidReplicationFactor(i16),
    AlreadyExists,
    UnknownTopic,
    InvalidConfig(String),
    PartitionCountReduced { have: i32, want: i32 },
    Internal(String),
}

impl TopicError {
    pub fn error_code(&self) -> kafgres_codec::ErrorCode {
        use kafgres_codec::ErrorCode as E;
        match self {
            TopicError::InvalidName(_) => E::InvalidTopicException,
            TopicError::InvalidPartitions(_) | TopicError::PartitionCountReduced { .. } => {
                E::InvalidPartitions
            }
            TopicError::InvalidReplicationFactor(_) => E::InvalidReplicationFactor,
            TopicError::AlreadyExists => E::TopicAlreadyExists,
            TopicError::UnknownTopic => E::UnknownTopicOrPartition,
            TopicError::InvalidConfig(_) => E::InvalidConfig,
            TopicError::Internal(_) => E::UnknownServerError,
        }
    }
}

impl std::fmt::Display for TopicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicError::InvalidName(why) => write!(f, "invalid topic name: {why}"),
            TopicError::InvalidPartitions(n) => write!(f, "invalid partition count {n}"),
            TopicError::InvalidReplicationFactor(n) => {
                write!(f, "replication factor {n}; this broker has one replica")
            }
            TopicError::AlreadyExists => write!(f, "topic already exists"),
            TopicError::UnknownTopic => write!(f, "unknown topic"),
            TopicError::InvalidConfig(m) => write!(f, "{m}"),
            TopicError::PartitionCountReduced { have, want } => {
                write!(f, "cannot reduce partitions from {have} to {want}")
            }
            TopicError::Internal(m) => write!(f, "internal: {m}"),
        }
    }
}

impl From<spi::Error> for TopicError {
    fn from(e: spi::Error) -> Self {
        TopicError::Internal(e.to_string())
    }
}

/// Ceiling on partitions in one topic: each is a `CREATE TABLE ... PARTITION OF` taking
pub const MAX_PARTITIONS: i32 = 10_000;

pub fn topic_name_by_id(topic_id: u32) -> Result<Option<String>, spi::Error> {
    Spi::get_one_with_args::<String>(
        "SELECT (SELECT name FROM kafgres_topics WHERE topic_id = $1::oid)",
        &[(topic_id as i32).into()],
    )
}

pub fn validate_topic_name(name: &str) -> Result<(), TopicError> {
    if name.is_empty() {
        return Err(TopicError::InvalidName("must not be empty".into()));
    }
    if name.len() > 249 {
        return Err(TopicError::InvalidName("longer than 249 characters".into()));
    }
    if name == "." || name == ".." {
        return Err(TopicError::InvalidName("'.' and '..' are reserved".into()));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(TopicError::InvalidName(
            "may contain only [a-zA-Z0-9._-]".into(),
        ));
    }
    Ok(())
}

/// One replica, always. -1 is the wire's "use the broker default", which is also 1.
pub fn validate_replication_factor(rf: i16) -> Result<(), TopicError> {
    if rf == -1 || rf == 1 {
        Ok(())
    } else {
        Err(TopicError::InvalidReplicationFactor(rf))
    }
}

pub struct CreatedTopic {
    pub topic_id: u32,
    pub uuid: [u8; 16],
    pub partitions: i32,
}

/// The library half of `CreateTopics` (API 19); a handler cannot use the SQL function because `error!` is a longjmp.
pub fn create_topic(
    name: &str,
    partitions: i32,
    config: &[(String, String)],
) -> Result<CreatedTopic, TopicError> {
    validate_topic_name(name)?;
    if partitions < 1 || partitions > MAX_PARTITIONS {
        return Err(TopicError::InvalidPartitions(partitions));
    }
    if topic_id_by_name(name)?.is_some() {
        return Err(TopicError::AlreadyExists);
    }

    let keys: Vec<String> = config.iter().map(|(k, _)| k.clone()).collect();
    let values: Vec<String> = config.iter().map(|(_, v)| v.clone()).collect();

    // From a sequence, never MAX()+1: a reused id inherits a dropped topic's dedup window and committed offsets.
    let created: Option<(i32, Vec<u8>)> = Spi::connect_mut(|client| {
        let rows = client.update(
            "INSERT INTO kafgres_topics (topic_id, name, num_partitions, topic_uuid, config)
             VALUES (nextval('kafgres_topic_id_seq')::oid, $1, $2,
                     decode(replace(gen_random_uuid()::text, '-', ''), 'hex'),
                     COALESCE(jsonb_object($3::text[], $4::text[]), '{}'::jsonb))
             RETURNING topic_id::int, topic_uuid",
            None,
            &[name.into(), partitions.into(), keys.into(), values.into()],
        )?;
        for row in rows {
            return Ok::<_, spi::Error>(Some((
                row.get::<i32>(1)?.unwrap_or(0),
                row.get::<Vec<u8>>(2)?.unwrap_or_default(),
            )));
        }
        Ok(None)
    })?;
    let (topic_id, uuid_bytes) =
        created.ok_or_else(|| TopicError::Internal("create returned no id".into()))?;

    let mut store = crate::storage::open();
    for p in 0..partitions {
        crate::storage::LogStore::create_partition(&mut *store, topic_id as u32, p, 0)
            .map_err(|e| TopicError::Internal(e.to_string()))?;
    }
    Ok(CreatedTopic {
        topic_id: topic_id as u32,
        uuid: to_uuid(Some(uuid_bytes)),
        partitions,
    })
}

pub fn partition_count(topic_id: u32) -> Result<i32, spi::Error> {
    Ok(Spi::get_one_with_args::<i32>(
        "SELECT (SELECT num_partitions FROM kafgres_topics WHERE topic_id = $1::oid)",
        &[(topic_id as i32).into()],
    )?
    .unwrap_or(0))
}

/// Add partitions to an existing topic; growing moves where a keyed producer's records land (`hash(key) % partitions`).
pub fn create_partitions(name: &str, total: i32) -> Result<(), TopicError> {
    let topic_id = topic_id_by_name(name)?.ok_or(TopicError::UnknownTopic)?;
    let have = Spi::get_one_with_args::<i32>(
        "SELECT (SELECT num_partitions FROM kafgres_topics WHERE topic_id = $1::oid)",
        &[(topic_id as i32).into()],
    )?
    .ok_or(TopicError::UnknownTopic)?;

    if total < have {
        return Err(TopicError::PartitionCountReduced { have, want: total });
    }
    if total == have || total > MAX_PARTITIONS {
        return Err(TopicError::InvalidPartitions(total));
    }

    let mut store = crate::storage::open();
    for p in have..total {
        crate::storage::LogStore::create_partition(&mut *store, topic_id, p, 0)
            .map_err(|e| TopicError::Internal(e.to_string()))?;
    }
    Spi::run_with_args(
        "UPDATE kafgres_topics SET num_partitions = $2 WHERE topic_id = $1::oid",
        &[(topic_id as i32).into(), total.into()],
    )?;
    Ok(())
}

pub fn delete_topic(name: &str) -> Result<bool, TopicError> {
    let topic_id = match topic_id_by_name(name)? {
        Some(id) => id,
        None => return Ok(false),
    };

    let mut store = crate::storage::open();
    let parts: Vec<i32> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT partition FROM kafgres_partitions WHERE topic_id = $1::oid",
            None,
            &[(topic_id as i32).into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(p) = row.get::<i32>(1)? {
                out.push(p);
            }
        }
        Ok::<_, spi::Error>(out)
    })?;

    for p in parts {
        crate::storage::LogStore::drop_partition(&mut *store, topic_id, p)
            .map_err(|e| TopicError::Internal(e.to_string()))?;
    }

    Spi::run_with_args(
        "DELETE FROM kafgres_producer_batches WHERE topic_id = $1::oid",
        &[(topic_id as i32).into()],
    )?;
    Spi::run_with_args(
        "DELETE FROM kafgres_offsets WHERE topic_id = $1::oid",
        &[(topic_id as i32).into()],
    )?;

    // A txn row outliving its topic wedges the producer: every `EndTxn` retry fails, and the expiry sweep takes the same path.
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_partitions WHERE topic_id = $1::oid",
        &[(topic_id as i32).into()],
    )?;
    Spi::run_with_args(
        "DELETE FROM kafgres_txn_offsets WHERE topic_id = $1::oid",
        &[(topic_id as i32).into()],
    )?;
    // Serialise against the archiver: otherwise the DELETE can miss a row it wrote but not committed, orphaning it on the dropped id.
    Spi::run_with_args(
        "SELECT pg_advisory_xact_lock($1)",
        &[crate::archive::ARCHIVE_LOCK_KEY.into()],
    )?;

    // The archived *files* are not ours to remove; the rows go because a reused topic id would inherit a false "archived" claim.
    let orphaned: i64 = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_segment_archive WHERE topic_id = $1::oid)",
        &[(topic_id as i32).into()],
    )?
    .unwrap_or(0);
    if orphaned > 0 {
        log!(
            "kafgres: dropping topic {name} orphans {orphaned} archived segment(s); the \
             files are still wherever kafgres.segment_archive_command put them and nothing \
             records them any more"
        );
    }
    Spi::run_with_args(
        "DELETE FROM kafgres_segment_archive WHERE topic_id = $1::oid",
        &[(topic_id as i32).into()],
    )?;
    Spi::run_with_args(
        "DELETE FROM kafgres_topics WHERE topic_id = $1::oid",
        &[(topic_id as i32).into()],
    )?;
    Ok(true)
}

/// Where each of a topic's partitions currently starts and ends. `offset_span` is a span
#[pg_extern]
fn kafgres_partition_offsets(
    topic: &str,
) -> TableIterator<
    'static,
    (
        name!(partition, Option<i32>),
        name!(log_start_offset, Option<i64>),
        name!(high_watermark, Option<i64>),
        name!(offset_span, Option<i64>),
        name!(leader_epoch, Option<i32>),
    ),
> {
    let topic_id = match topic_id_by_name(topic) {
        Ok(Some(id)) => id,
        Ok(None) => error!("kafgres: no such topic: {topic}"),
        Err(e) => error!("kafgres: {e}"),
    };
    let store = crate::storage::open();
    let parts: Vec<i32> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT partition FROM kafgres_partitions WHERE topic_id = $1::oid
              ORDER BY partition",
            None,
            &[(topic_id as i32).into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(p) = row.get::<i32>(1)? {
                out.push(p);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_else(|e| error!("kafgres: listing partitions of {topic}: {e}"));

    let mut out = Vec::new();
    for p in parts {
        let start = match store.log_start_offset(topic_id, p) {
            Ok(v) => v,
            Err(e) => error!("kafgres: partition {p}: {e}"),
        };
        let hwm = match store.high_watermark_if_tracked(topic_id, p) {
            Ok(v) => v,
            Err(e) => error!("kafgres: partition {p}: {e}"),
        };
        let span = match hwm {
            Some(h) if h >= start => Some(h - start),
            _ => None,
        };
        out.push((
            Some(p),
            Some(start),
            hwm,
            span,
            match crate::storage::LogStore::leader_epoch(&*store, topic_id, p) {
                Ok(v) => Some(v),
                Err(e) => error!("kafgres: partition {p} leader epoch: {e}"),
            },
        ));
    }
    TableIterator::new(out)
}

#[pg_extern]
fn kafgres_create_topic(name: &str, partitions: default!(i32, 1)) -> i32 {
    match create_topic(name, partitions, &[]) {
        Ok(t) => t.topic_id as i32,
        Err(e) => error!("kafgres: create topic failed: {e}"),
    }
}

#[pg_extern]
fn kafgres_drop_topic(name: &str) -> bool {
    match delete_topic(name) {
        Ok(dropped) => dropped,
        Err(e) => error!("kafgres: drop topic failed: {e}"),
    }
}

/// Every topic id in `ids`, with its wire uuid, in one query: pgrx plans each statement afresh, so per-id lookups stall the worker.
pub fn topic_uuids_by_ids(ids: &[u32]) -> Result<HashMap<u32, [u8; 16]>, spi::Error> {
    let mut out = HashMap::new();
    if ids.is_empty() {
        return Ok(out);
    }
    let as_int: Vec<i32> = ids.iter().map(|i| *i as i32).collect();
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT topic_id::int, topic_uuid FROM kafgres_topics
              WHERE topic_id = ANY($1::int[]::oid[])",
            None,
            &[as_int.into()],
        )?;
        for row in rows {
            if let Some(id) = row.get::<i32>(1)? {
                out.insert(id as u32, to_uuid(row.get::<Vec<u8>>(2)?));
            }
        }
        Ok::<_, spi::Error>(())
    })?;
    Ok(out)
}

pub fn topic_ids_by_uuids(uuids: &[[u8; 16]]) -> Result<HashMap<[u8; 16], u32>, spi::Error> {
    let mut out = HashMap::new();
    if uuids.is_empty() {
        return Ok(out);
    }
    let as_bytes: Vec<Vec<u8>> = uuids.iter().map(|u| u.to_vec()).collect();
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT topic_id::int, topic_uuid FROM kafgres_topics
              WHERE topic_uuid = ANY($1::bytea[])",
            None,
            &[as_bytes.into()],
        )?;
        for row in rows {
            if let Some(id) = row.get::<i32>(1)? {
                out.insert(to_uuid(row.get::<Vec<u8>>(2)?), id as u32);
            }
        }
        Ok::<_, spi::Error>(())
    })?;
    Ok(out)
}
