//! KIP-932 share groups: queue semantics. Every member reads every partition it subscribes

use std::collections::BTreeMap;

use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;

use super::HandlerError;

/// How long an acquired record stays out of circulation (Kafka's
pub fn lock_duration_ms() -> i64 {
    crate::share_lock_duration_ms()
}

/// Kafka's `group.share.delivery.attempts` default; past this a record is archived — a dead
pub const MAX_DELIVERY_ATTEMPTS: i32 = 5;

const HEARTBEAT_INTERVAL_MS: i32 = 5_000;

pub const SESSION_TIMEOUT_MS: i64 = 45_000;

const MAX_SUBSCRIBED_TOPICS: usize = 5_000;
const MAX_GROUP_SIZE: i64 = 1_000;
/// Records acquired in one ShareFetch. Kafka's `MaxRecords` is a client hint; this is the
const MAX_ACQUIRE_PER_FETCH: i32 = 5_000;

/// Kafka's `group.share.partition.max.record.locks`. Bounds `kafgres_share_inflight` to a
const MAX_RECORD_LOCKS: i64 = 200;

/// Rows one share partition may hold in `kafgres_share_inflight` before it stops handing out
const MAX_INFLIGHT_ROWS: i64 = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    Gap,
    Accept,
    Release,
    Reject,
    Renew,
}

impl Ack {
    pub fn from_wire(v: i8) -> Option<Ack> {
        Some(match v {
            0 => Ack::Gap,
            1 => Ack::Accept,
            2 => Ack::Release,
            3 => Ack::Reject,
            4 => Ack::Renew,
            _ => return None,
        })
    }
}

/// Ensure the group and this member exist, and refuse a group id already used by another
pub fn ensure_member(
    group: &str,
    member: &str,
    rack: Option<&str>,
    subscribed: Option<&[String]>,
) -> Result<bool, HandlerError> {
    let clash: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_groups WHERE group_id = $1)
              + (SELECT count(*) FROM kafgres_consumer_groups WHERE group_id = $1)",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if clash.unwrap_or(0) > 0 {
        return Err(HandlerError::Internal(format!(
            "{group} is already a consumer group; a share group cannot share the name"
        )));
    }

    Spi::run_with_args(
        "INSERT INTO kafgres_share_groups (group_id) VALUES ($1) ON CONFLICT DO NOTHING",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let existed: Option<bool> = Spi::get_one_with_args(
        "SELECT (SELECT true FROM kafgres_share_members WHERE group_id = $1 AND member_id = $2)",
        &[group.into(), member.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    Spi::run_with_args(
        "INSERT INTO kafgres_share_members (group_id, member_id, rack_id, subscribed, last_seen)
         VALUES ($1, $2, $3, COALESCE($4::text[], '{}'), now())
         ON CONFLICT (group_id, member_id) DO UPDATE SET
             rack_id = COALESCE($3, kafgres_share_members.rack_id),
             subscribed = COALESCE($4::text[], kafgres_share_members.subscribed),
             last_seen = now()",
        &[
            group.into(),
            member.into(),
            rack.into(),
            subscribed.map(|s| s.to_vec()).into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    if existed != Some(true) {
        Spi::run_with_args(
            "UPDATE kafgres_share_groups
                SET group_epoch = group_epoch + 1, state = 'Stable', updated_at = now()
              WHERE group_id = $1",
            &[group.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    Ok(existed == Some(true))
}

/// Every partition of every topic this member subscribes to. Not an assignment: members all
pub fn assignment(group: &str, member: &str) -> Result<Vec<(u32, i32)>, HandlerError> {
    let names: Vec<String> = Spi::get_one_with_args::<Vec<String>>(
        "SELECT (SELECT subscribed FROM kafgres_share_members
                  WHERE group_id = $1 AND member_id = $2)",
        &[group.into(), member.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?
    .unwrap_or_default();
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let loaded = crate::meta::load_topics(Some(&names))
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let mut out = Vec::new();
    for t in loaded {
        for p in &t.partitions {
            out.push((t.topic_id, p.partition));
        }
    }
    Ok(out)
}

pub fn start_offset(group: &str, topic: u32, partition: i32) -> Result<i64, HandlerError> {
    Spi::get_one_with_args::<i64>(
        "SELECT (SELECT start_offset FROM kafgres_share_offsets
                  WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3)",
        &[group.into(), (topic as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))
    .map(|v| v.unwrap_or(0))
}

/// Take up to `limit` records for this member from the share partition's offset. Records in
pub fn acquire(
    group: &str,
    member: &str,
    topic: u32,
    partition: i32,
    from: i64,
    upto: i64,
    limit: i32,
) -> Result<Vec<i64>, HandlerError> {
    let limit = limit.clamp(1, MAX_ACQUIRE_PER_FETCH);

    // Two counts bound different things: `held` is live locks (the parallelism knob); `rows`
    let counts: Option<(i64, i64)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT count(*) FILTER (WHERE state = 'acquired' AND acquired_until > now()),
                    count(*)
               FROM kafgres_share_inflight
              WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3",
            Some(1),
            &[group.into(), (topic as i32).into(), partition.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some((
                r.get::<i64>(1)?.unwrap_or(0),
                r.get::<i64>(2)?.unwrap_or(0),
            )));
        }
        Ok(None)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let (held, rows) = counts.unwrap_or((0, 0));
    if rows >= MAX_INFLIGHT_ROWS {
        log!(
            "kafgres: share partition {topic}-{partition} of group {group:?} is holding \
             {rows} unfinished records; its head is not advancing, so no more will be \
             handed out until it does"
        );
        return Ok(Vec::new());
    }
    let room = (MAX_RECORD_LOCKS - held).max(0);
    if room == 0 {
        return Ok(Vec::new());
    }
    let limit = limit.min(room as i32).max(1);

    let acquired: Vec<i64> = Spi::connect(|client| {
        let rows = client.select(
            "WITH candidate AS (
                 SELECT g.o AS record_offset
                   FROM generate_series($4::bigint, $5::bigint) AS g(o)
                  WHERE NOT EXISTS (
                        SELECT 1 FROM kafgres_share_inflight f
                         WHERE f.group_id = $1 AND f.topic_id = $2::oid
                           AND f.partition = $3 AND f.record_offset = g.o
                           AND (f.state <> 'acquired' OR f.acquired_until > now()))
                  ORDER BY g.o
                  LIMIT $7)
             INSERT INTO kafgres_share_inflight
                    (group_id, topic_id, partition, record_offset, state,
                     delivery_count, member_id, acquired_until)
             SELECT $1, $2::oid, $3, record_offset, 'acquired', 1, $6,
                    now() + make_interval(secs => $8::double precision / 1000)
               FROM candidate
             ON CONFLICT (group_id, topic_id, partition, record_offset) DO UPDATE SET
                    -- A record whose lock lapsed, or that was released: hand it out again
                    -- and count the attempt. Past the limit it is archived instead, which
                    -- is what keeps one bad record from cycling forever.
                    state = CASE WHEN kafgres_share_inflight.delivery_count + 1
                                       > $9 THEN 'archived' ELSE 'acquired' END,
                    delivery_count = kafgres_share_inflight.delivery_count + 1,
                    member_id = CASE WHEN kafgres_share_inflight.delivery_count + 1
                                          > $9 THEN NULL ELSE $6 END,
                    acquired_until = CASE WHEN kafgres_share_inflight.delivery_count + 1
                                               > $9 THEN NULL
                                          ELSE now() + make_interval(
                                                 secs => $8::double precision / 1000) END
             -- **Re-checked at conflict resolution, not only in the anti-join above.**
             -- The candidate scan is planned once and conflicts resolve per row, so a
             -- record that became live-locked in between was taken anyway — and the second
             -- writer was told it had acquired it. Reproduced with two concurrent callers
             -- of `kafgres_share_acquire`: both got offset 15 and both processed it. The
             -- dispatch loop is single-threaded so the wire path never raced; the SQL entry
             -- point is callable from any backend and does.
             WHERE kafgres_share_inflight.state = 'acquired'
               AND (kafgres_share_inflight.acquired_until IS NULL
                    OR kafgres_share_inflight.acquired_until <= now())
             RETURNING record_offset, state",
            None,
            &[
                group.into(),
                (topic as i32).into(),
                partition.into(),
                from.into(),
                upto.into(),
                member.into(),
                (limit as i64).into(),
                lock_duration_ms().into(),
                MAX_DELIVERY_ATTEMPTS.into(),
            ],
        )?;
        let mut out = Vec::new();
        for r in rows {
            // An archived record was not handed out; reporting it as acquired would hang the
            if r.get::<String>(2)?.as_deref() == Some("acquired") {
                if let Some(o) = r.get::<i64>(1)? {
                    out.push(o);
                }
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Acquiring can finish a record, so acquiring can move the offset: a record past its
    advance_start_offset(group, topic, partition)?;
    Ok(acquired)
}

/// Apply one acknowledgement range.
pub fn acknowledge(
    group: &str,
    member: &str,
    topic: u32,
    partition: i32,
    first: i64,
    last: i64,
    types: &[Ack],
) -> Result<(), HandlerError> {
    for (i, kind) in types.iter().enumerate() {
        let offset = first + i as i64;
        if offset > last {
            break;
        }
        match kind {
            // A gap means "there is no record at this offset" — a transaction control batch,
            Ack::Gap => {
                let _ = set_state(group, member, topic, partition, offset, "archived");
            }
            Ack::Accept => set_state(group, member, topic, partition, offset, "acked")?,
            Ack::Reject => set_state(group, member, topic, partition, offset, "archived")?,
            // Delivery count stays: forgetting a release's attempt is how a poison record
            Ack::Release => {
                Spi::run_with_args(
                    "UPDATE kafgres_share_inflight
                        SET state = 'acquired', member_id = NULL, acquired_until = NULL
                      WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3
                        AND record_offset = $4 AND member_id = $5",
                    &[
                        group.into(),
                        (topic as i32).into(),
                        partition.into(),
                        offset.into(),
                        member.into(),
                    ],
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
            // Extend the lock: a consumer still working says so rather than losing the
            Ack::Renew => {
                Spi::run_with_args(
                    "UPDATE kafgres_share_inflight
                        SET acquired_until = now() + make_interval(
                                secs => $6::double precision / 1000)
                      WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3
                        AND record_offset = $4 AND member_id = $5 AND state = 'acquired'",
                    &[
                        group.into(),
                        (topic as i32).into(),
                        partition.into(),
                        offset.into(),
                        member.into(),
                        lock_duration_ms().into(),
                    ],
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
        }
    }
    advance_start_offset(group, topic, partition)
}

/// An acknowledgement for a record this member no longer holds. The client must hear
#[derive(Debug)]
pub struct NotHeld;

fn set_state(
    group: &str,
    member: &str,
    topic: u32,
    partition: i32,
    offset: i64,
    state: &str,
) -> Result<(), HandlerError> {
    // `member_id = $5` so a consumer whose lock already lapsed cannot acknowledge a record
    let changed: Option<i64> = Spi::get_one_with_args(
        "WITH up AS (
             UPDATE kafgres_share_inflight
                SET state = $6, member_id = NULL, acquired_until = NULL
              WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3
                AND record_offset = $4 AND member_id = $5
             RETURNING 1)
         SELECT count(*) FROM up",
        &[
            group.into(),
            (topic as i32).into(),
            partition.into(),
            offset.into(),
            member.into(),
            state.into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if changed.unwrap_or(0) == 0 {
        return Err(HandlerError::Internal(NOT_HELD.to_string()));
    }
    Ok(())
}

/// Marker for "this member does not hold that record"; the handlers turn it into
pub const NOT_HELD: &str = "record is not held by this member";

/// Move the share partition past every finished record at its head, contiguously only —
fn advance_start_offset(group: &str, topic: u32, partition: i32) -> Result<(), HandlerError> {
    Spi::run_with_args(
        "WITH start AS (
             SELECT COALESCE((SELECT start_offset FROM kafgres_share_offsets
                               WHERE group_id = $1 AND topic_id = $2::oid
                                 AND partition = $3), 0) AS o)
         , finished AS (
             SELECT f.record_offset,
                    f.record_offset - row_number() OVER (ORDER BY f.record_offset) AS run
               FROM kafgres_share_inflight f, start
              WHERE f.group_id = $1 AND f.topic_id = $2::oid AND f.partition = $3
                AND f.state IN ('acked', 'archived')
                AND f.record_offset >= start.o)
         , head AS (
             SELECT max(record_offset) + 1 AS next
               FROM finished, start
              WHERE run = start.o - 1)
         INSERT INTO kafgres_share_offsets (group_id, topic_id, partition, start_offset)
         SELECT $1, $2::oid, $3, COALESCE((SELECT next FROM head), (SELECT o FROM start))
         ON CONFLICT (group_id, topic_id, partition) DO UPDATE
            SET start_offset = EXCLUDED.start_offset",
        &[group.into(), (topic as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Rows below the new start describe records nobody will be offered again; keeping them
    Spi::run_with_args(
        "DELETE FROM kafgres_share_inflight f
          USING kafgres_share_offsets o
          WHERE f.group_id = o.group_id AND f.topic_id = o.topic_id
            AND f.partition = o.partition
            AND f.group_id = $1 AND f.topic_id = $2::oid AND f.partition = $3
            AND f.record_offset < o.start_offset",
        &[group.into(), (topic as i32).into(), partition.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(())
}

/// Release acquisitions whose lock has lapsed, and expire silent members: a consumer that
pub fn expire() -> Result<usize, HandlerError> {
    const PER_SWEEP: i64 = 1_000;
    let released: Option<i64> = Spi::get_one_with_args(
        "WITH lapsed AS (
             UPDATE kafgres_share_inflight
                SET member_id = NULL, acquired_until = NULL
              WHERE ctid = ANY (ARRAY(
                    SELECT ctid FROM kafgres_share_inflight
                     WHERE state = 'acquired' AND acquired_until < now()
                     ORDER BY acquired_until LIMIT $1))
             RETURNING 1)
         SELECT count(*) FROM lapsed",
        &[PER_SWEEP.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    Spi::run_with_args(
        "DELETE FROM kafgres_share_members
          WHERE last_seen < now() - make_interval(secs => $1::double precision / 1000)",
        &[SESSION_TIMEOUT_MS.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(released.unwrap_or(0) as usize)
}

pub fn check_bounds(subscribed: Option<&[String]>) -> Result<(), HandlerError> {
    if let Some(names) = subscribed {
        if names.len() > MAX_SUBSCRIBED_TOPICS {
            return Err(HandlerError::TooLarge { what: "subscribed topics", n: names.len() });
        }
    }
    Ok(())
}

pub fn group_is_full(group: &str) -> Result<bool, HandlerError> {
    let n: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_share_members WHERE group_id = $1)",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(n.unwrap_or(0) >= MAX_GROUP_SIZE)
}

pub const fn heartbeat_interval_ms() -> i32 {
    HEARTBEAT_INTERVAL_MS
}

pub fn describe_members(group: &str) -> Result<Vec<(String, i32, Vec<String>)>, HandlerError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT member_id, member_epoch, subscribed FROM kafgres_share_members
              WHERE group_id = $1 ORDER BY member_id",
            None,
            &[group.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push((
                r.get::<String>(1)?.unwrap_or_default(),
                r.get::<i32>(2)?.unwrap_or(0),
                r.get::<Vec<String>>(3)?.unwrap_or_default(),
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

pub fn describe_group(group: &str) -> Result<Option<(i32, String)>, HandlerError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT group_epoch, state FROM kafgres_share_groups WHERE group_id = $1",
            Some(1),
            &[group.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some((
                r.get::<i32>(1)?.unwrap_or(0),
                r.get::<String>(2)?.unwrap_or_default(),
            )));
        }
        Ok(None)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

pub fn decode_acks(raw: &[i8]) -> Vec<Ack> {
    raw.iter()
        .map(|v| Ack::from_wire(*v).unwrap_or(Ack::Gap))
        .collect()
}

pub fn wrong_group_kind() -> ErrorCode {
    ErrorCode::GroupIdNotFound
}

/// SQL entry points for driving and inspecting the delivery model without a KIP-932 client,
#[pg_extern]
fn kafgres_share_acquire(
    group: &str,
    member: &str,
    topic: &str,
    partition: i32,
    limit: default!(i32, 100),
) -> Vec<i64> {
    let topic_id = crate::meta::topic_id_by_name(topic)
        .ok()
        .flatten()
        .unwrap_or_else(|| error!("kafgres: no such topic {topic:?}"));
    let store = crate::storage::open();
    let end = crate::storage::LogStore::high_watermark(&*store, topic_id, partition)
        .unwrap_or_else(|e| error!("kafgres: {e}"));
    let from = start_offset(group, topic_id, partition)
        .unwrap_or_else(|e| error!("kafgres: {e}"));
    if end <= from {
        return Vec::new();
    }
    acquire(group, member, topic_id, partition, from, end - 1, limit)
        .unwrap_or_else(|e| error!("kafgres: {e}"))
}

#[pg_extern]
fn kafgres_share_ack(
    group: &str,
    member: &str,
    topic: &str,
    partition: i32,
    first: i64,
    last: i64,
    kind: &str,
) -> bool {
    let topic_id = crate::meta::topic_id_by_name(topic)
        .ok()
        .flatten()
        .unwrap_or_else(|| error!("kafgres: no such topic {topic:?}"));
    let one = match kind {
        "accept" => Ack::Accept,
        "release" => Ack::Release,
        "reject" => Ack::Reject,
        "renew" => Ack::Renew,
        _ => error!("kafgres: kind must be accept, release, reject or renew"),
    };
    let n = (last - first + 1).max(0) as usize;
    acknowledge(group, member, topic_id, partition, first, last, &vec![one; n])
        .unwrap_or_else(|e| error!("kafgres: {e}"));
    true
}

#[pg_extern]
fn kafgres_share_state(
    group: &str,
    topic: &str,
) -> TableIterator<
    'static,
    (
        name!(partition, Option<i32>),
        name!(start_offset, Option<i64>),
        name!(record_offset, Option<i64>),
        name!(state, Option<String>),
        name!(delivery_count, Option<i32>),
        name!(member_id, Option<String>),
        name!(locked_for_ms, Option<i64>),
    ),
> {
    let topic_id = crate::meta::topic_id_by_name(topic)
        .ok()
        .flatten()
        .unwrap_or_else(|| error!("kafgres: no such topic {topic:?}"));
    let rows = Spi::connect(|client| {
        let got = client.select(
            "SELECT f.partition, o.start_offset, f.record_offset, f.state, f.delivery_count,
                    f.member_id,
                    CASE WHEN f.acquired_until IS NULL THEN NULL
                         ELSE (extract(epoch FROM f.acquired_until - now()) * 1000)::bigint END
               FROM kafgres_share_inflight f
               LEFT JOIN kafgres_share_offsets o
                      ON o.group_id = f.group_id AND o.topic_id = f.topic_id
                     AND o.partition = f.partition
              WHERE f.group_id = $1 AND f.topic_id = $2::oid
              ORDER BY f.partition, f.record_offset",
            None,
            &[group.into(), (topic_id as i32).into()],
        )?;
        let mut out = Vec::new();
        for r in got {
            out.push((
                r.get::<i32>(1)?,
                r.get::<i64>(2)?,
                r.get::<i64>(3)?,
                r.get::<String>(4)?,
                r.get::<i32>(5)?,
                r.get::<String>(6)?,
                r.get::<i64>(7)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| error!("kafgres: {e}"));
    TableIterator::new(rows)
}

#[pg_extern]
fn kafgres_share_expire() -> i64 {
    expire().unwrap_or_else(|e| error!("kafgres: {e}")) as i64
}

#[pg_extern]
fn kafgres_share_join(group: &str, member: &str, topics: &str) -> bool {
    let subs: Vec<String> = topics.split(',').map(|s| s.trim().to_string()).collect();
    ensure_member(group, member, None, Some(&subs))
        .unwrap_or_else(|e| error!("kafgres: {e}"));
    true
}

use kafgres_codec::generated::share_acknowledge_request::ShareAcknowledgeRequest;
use kafgres_codec::generated::share_acknowledge_response::{
    PartitionData as AckPartitionData, ShareAcknowledgeResponse, ShareAcknowledgeTopicResponse,
};
use kafgres_codec::generated::share_fetch_request::{
    FetchPartition as ShareFetchPartition, FetchTopic as ShareFetchTopic, ShareFetchRequest,
};
use kafgres_codec::generated::share_fetch_response::{
    AcquiredRecords, PartitionData as FetchPartitionData, ShareFetchResponse,
    ShareFetchableTopicResponse,
};
use kafgres_codec::generated::share_group_describe_request::ShareGroupDescribeRequest;
use kafgres_codec::generated::share_group_describe_response::{
    DescribedGroup, Member as DescMember, ShareGroupDescribeResponse,
};
use kafgres_codec::generated::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use kafgres_codec::generated::share_group_heartbeat_response::{
    Assignment, ShareGroupHeartbeatResponse, TopicPartitions,
};
use kafgres_codec::primitives::Uuid;

/// `76 ShareGroupHeartbeat`. No reconciliation: a member is told every partition it
pub fn heartbeat(
    req: &ShareGroupHeartbeatRequest,
    authz: &crate::acl::Authz,
) -> Result<ShareGroupHeartbeatResponse, HandlerError> {
    check_bounds(req.subscribed_topic_names.as_deref())?;

    if let Err(code) = authz.check(
        crate::acl::Operation::Read,
        crate::acl::ResourceType::Group,
        &req.group_id,
    ) {
        return Ok(err_hb(code, "not authorized to read this group"));
    }
    if let Some(names) = &req.subscribed_topic_names {
        for n in names {
            if authz
                .check(crate::acl::Operation::Read, crate::acl::ResourceType::Topic, n)
                .is_err()
            {
                return Ok(err_hb(
                    ErrorCode::TopicAuthorizationFailed,
                    "not authorized to read a subscribed topic",
                ));
            }
        }
    }

    let member_id = if req.member_id.is_empty() { uuid_like()? } else { req.member_id.clone() };

    // Leaving: no assignment to hand back, but acquired records go back into the pool now
    if req.member_epoch < 0 {
        release_all(&req.group_id, &member_id)?;
        Spi::run_with_args(
            "DELETE FROM kafgres_share_members WHERE group_id = $1 AND member_id = $2",
            &[req.group_id.as_str().into(), member_id.as_str().into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        return Ok(ShareGroupHeartbeatResponse {
            error_code: ErrorCode::None.code(),
            member_id: Some(member_id),
            member_epoch: req.member_epoch,
            ..Default::default()
        });
    }

    if group_is_full(&req.group_id)? {
        let known: Option<bool> = Spi::get_one_with_args(
            "SELECT (SELECT true FROM kafgres_share_members
                      WHERE group_id = $1 AND member_id = $2)",
            &[req.group_id.as_str().into(), member_id.as_str().into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        if known != Some(true) {
            return Ok(err_hb(ErrorCode::GroupMaxSizeReached, "group is at its member limit"));
        }
    }

    if let Err(e) = ensure_member(
        &req.group_id,
        &member_id,
        req.rack_id.as_deref(),
        req.subscribed_topic_names.as_deref(),
    ) {
        if let HandlerError::Internal(m) = &e {
            if m.contains("already a consumer group") {
                return Ok(err_hb(wrong_group_kind(), m));
            }
        }
        return Err(e);
    }

    let epoch: i32 = Spi::get_one_with_args(
        "SELECT (SELECT group_epoch FROM kafgres_share_groups WHERE group_id = $1)",
        &[req.group_id.as_str().into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?
    .unwrap_or(0);
    Spi::run_with_args(
        "UPDATE kafgres_share_members SET member_epoch = $3
          WHERE group_id = $1 AND member_id = $2",
        &[req.group_id.as_str().into(), member_id.as_str().into(), epoch.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let asg = assignment(&req.group_id, &member_id)?;
    let mut per_topic: BTreeMap<u32, Vec<i32>> = BTreeMap::new();
    for (t, p) in asg {
        per_topic.entry(t).or_default().push(p);
    }
    let uuids = crate::meta::topic_uuids_by_ids(&per_topic.keys().copied().collect::<Vec<_>>())
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let tps: Vec<TopicPartitions> = per_topic
        .into_iter()
        .filter_map(|(t, mut ps)| {
            let u = uuids.get(&t)?;
            ps.sort_unstable();
            Some(TopicPartitions { topic_id: Uuid(*u), partitions: ps, ..Default::default() })
        })
        .collect();

    Ok(ShareGroupHeartbeatResponse {
        error_code: ErrorCode::None.code(),
        member_id: Some(member_id),
        member_epoch: epoch,
        heartbeat_interval_ms: heartbeat_interval_ms(),
        assignment: Some(Assignment { topic_partitions: tps, ..Default::default() }),
        ..Default::default()
    })
}

fn err_hb(code: ErrorCode, msg: &str) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code: code.code(),
        error_message: Some(msg.to_string()),
        ..Default::default()
    }
}

/// Put everything a member holds back in the pool: only a crash should cost the lock
fn release_all(group: &str, member: &str) -> Result<(), HandlerError> {
    Spi::run_with_args(
        "UPDATE kafgres_share_inflight
            SET member_id = NULL, acquired_until = NULL
          WHERE group_id = $1 AND member_id = $2 AND state = 'acquired'",
        &[group.into(), member.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

fn uuid_like() -> Result<String, HandlerError> {
    let pair: Option<(i64, i64)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT (random() * 9e18)::bigint, (random() * 9e18)::bigint",
            Some(1),
            &[],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some((
                r.get::<i64>(1)?.unwrap_or(0),
                r.get::<i64>(2)?.unwrap_or(0),
            )));
        }
        Ok(None)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let (n, m) = pair.ok_or_else(|| HandlerError::Internal("could not generate a member id".into()))?;
    Ok(format!(
        "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
        (n >> 32) as u32, (n >> 16) as u16, (n & 0xfff) as u16,
        (m >> 48) as u16 & 0xfff, m & 0xffff_ffff_ffff
    ))
}

/// The code for an acknowledgement that did not apply: `INVALID_RECORD_STATE` when the
fn ack_error(e: &HandlerError) -> ErrorCode {
    match e {
        HandlerError::Internal(m) if m.contains(NOT_HELD) => ErrorCode::InvalidRecordState,
        _ => ErrorCode::UnknownServerError,
    }
}

/// A member swept for silence keeps fetching under an id with no row behind it;
fn member_is_known(group: &str, member: &str) -> Result<bool, HandlerError> {
    let n: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_share_members
                  WHERE group_id = $1 AND member_id = $2)",
        &[group.into(), member.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(n.unwrap_or(0) > 0)
}

/// The partitions an established share session covers: this member's whole assignment,
fn implied_session(group: &str, member: &str) -> Result<Vec<ShareFetchTopic>, HandlerError> {
    let mut per_topic: BTreeMap<u32, Vec<i32>> = BTreeMap::new();
    for (t, p) in assignment(group, member)? {
        per_topic.entry(t).or_default().push(p);
    }
    let uuids = crate::meta::topic_uuids_by_ids(&per_topic.keys().copied().collect::<Vec<_>>())
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(per_topic
        .into_iter()
        .filter_map(|(t, mut ps)| {
            let u = uuids.get(&t)?;
            ps.sort_unstable();
            Some(ShareFetchTopic {
                topic_id: Uuid(*u),
                partitions: ps
                    .into_iter()
                    .map(|p| ShareFetchPartition {
                        partition_index: p,
                        partition_max_bytes: i32::MAX,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
        })
        .collect())
}

/// `78 ShareFetch` — acquire records, and apply any acknowledgements riding along. Batches
pub fn share_fetch(
    req: &ShareFetchRequest,
    store: &dyn crate::storage::LogStore,
    authz: &crate::acl::Authz,
) -> Result<ShareFetchResponse, HandlerError> {
    let group = req.group_id.clone().unwrap_or_default();
    let member = req.member_id.clone().unwrap_or_default();
    if group.is_empty() || member.is_empty() {
        return Ok(ShareFetchResponse {
            error_code: ErrorCode::InvalidRequest.code(),
            error_message: Some("share fetch needs a group id and a member id".into()),
            ..Default::default()
        });
    }
    if let Err(code) = authz.check(
        crate::acl::Operation::Read,
        crate::acl::ResourceType::Group,
        &group,
    ) {
        return Ok(ShareFetchResponse {
            error_code: code.code(),
            error_message: Some("not authorized to read this group".into()),
            ..Default::default()
        });
    }
    super::check_admin_len("share fetch topics", req.topics.len())?;

    // A closing session releases what the member holds, for the same reason a leave does.
    if req.share_session_epoch < 0 {
        release_all(&group, &member)?;
        return Ok(ShareFetchResponse {
            error_code: ErrorCode::None.code(),
            acquisition_lock_timeout_ms: lock_duration_ms() as i32,
            ..Default::default()
        });
    }

    if !member_is_known(&group, &member)? {
        return Ok(ShareFetchResponse {
            error_code: ErrorCode::UnknownMemberId.code(),
            error_message: Some("unknown member; rejoin the share group".into()),
            ..Default::default()
        });
    }
    // Liveness: a member fetching is a member alive, whether or not its heartbeat lands.
    Spi::run_with_args(
        "UPDATE kafgres_share_members SET last_seen = now()
          WHERE group_id = $1 AND member_id = $2",
        &[group.as_str().into(), member.as_str().into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // A share session's incremental requests carry no topics, and that means "the same
    let implied: Vec<ShareFetchTopic>;
    let topics: &[ShareFetchTopic] = if req.topics.is_empty() && req.share_session_epoch > 0 {
        implied = implied_session(&group, &member)?;
        &implied
    } else {
        &req.topics
    };

    let mut budget = req.max_records.clamp(1, MAX_ACQUIRE_PER_FETCH);
    // A running byte total, not a per-partition cap: `budget` counts records, and without
    let mut bytes_left: usize = super::MAX_RESPONSE_BYTES
        .min(req.max_bytes.max(1) as usize)
        .saturating_sub(1024);
    let mut responses = Vec::with_capacity(topics.len());
    for t in topics {
        let resolved = crate::meta::topic_by_uuid(&t.topic_id.0)
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        let mut partitions = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let Some((topic_id, name)) = resolved.clone() else {
                partitions.push(fetch_error(p.partition_index, ErrorCode::UnknownTopicId));
                continue;
            };
            if authz
                .check(crate::acl::Operation::Read, crate::acl::ResourceType::Topic, &name)
                .is_err()
            {
                partitions.push(fetch_error(
                    p.partition_index,
                    ErrorCode::TopicAuthorizationFailed,
                ));
                continue;
            }

            // Acknowledgements first, then the fetch: progress must be applied before the
            let mut ack_code = ErrorCode::None;
            for b in &p.acknowledgement_batches {
                let acks = decode_acks(&b.acknowledge_types);
                if let Err(e) = acknowledge(
                    &group, &member, topic_id, p.partition_index,
                    b.first_offset, b.last_offset, &acks,
                ) {
                    ack_code = ack_error(&e);
                    if ack_code != ErrorCode::InvalidRecordState {
                        log!("kafgres: share ack {name}-{}: {e}", p.partition_index);
                    }
                }
            }

            let per_partition = bytes_left.min(if p.partition_max_bytes > 0 {
                p.partition_max_bytes as usize
            } else {
                bytes_left
            });
            let (records, acquired) = if budget == 0 || bytes_left == 0 {
                (Vec::new(), Vec::new())
            } else {
                match acquire_and_read(
                    store, &group, &member, topic_id, p.partition_index, budget, per_partition,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        log!("kafgres: share fetch {name}-{}: {e}", p.partition_index);
                        partitions.push(fetch_error(
                            p.partition_index,
                            ErrorCode::UnknownServerError,
                        ));
                        continue;
                    }
                }
            };
            budget -= acquired.iter().map(|a| a.count()).sum::<i32>().min(budget);
            bytes_left = bytes_left.saturating_sub(records.len());

            partitions.push(FetchPartitionData {
                partition_index: p.partition_index,
                error_code: ErrorCode::None.code(),
                acknowledge_error_code: ack_code.code(),
                records: kafgres_codec::prelude::Bytes::from(records),
                acquired_records: acquired,
                ..Default::default()
            });
        }
        responses.push(ShareFetchableTopicResponse {
            topic_id: t.topic_id,
            partitions,
            ..Default::default()
        });
    }

    Ok(ShareFetchResponse {
        error_code: ErrorCode::None.code(),
        acquisition_lock_timeout_ms: lock_duration_ms() as i32,
        responses,
        ..Default::default()
    })
}

trait Span {
    fn count(&self) -> i32;
}
impl Span for AcquiredRecords {
    fn count(&self) -> i32 {
        (self.last_offset - self.first_offset + 1).max(0) as i32
    }
}

fn fetch_error(partition: i32, code: ErrorCode) -> FetchPartitionData {
    FetchPartitionData {
        partition_index: partition,
        error_code: code.code(),
        ..Default::default()
    }
}

/// Read the byte slice, then acquire only the offsets it covers.
fn acquire_and_read(
    store: &dyn crate::storage::LogStore,
    group: &str,
    member: &str,
    topic: u32,
    partition: i32,
    limit: i32,
    byte_cap: usize,
) -> Result<(Vec<u8>, Vec<AcquiredRecords>), HandlerError> {
    let hwm = store
        .high_watermark(topic, partition)
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let from = start_offset(group, topic, partition)?;
    if hwm <= from {
        return Ok((Vec::new(), Vec::new()));
    }

    // Read first, then acquire only what the bytes cover: `read` is byte-capped, so
    let slice = store
        .read(
            topic,
            partition,
            from,
            byte_cap,
            crate::storage::IsolationLevel::ReadUncommitted,
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if slice.next_offset <= from {
        return Ok((Vec::new(), Vec::new()));
    }
    let offsets = acquire(group, member, topic, partition, from, slice.next_offset - 1, limit)?;
    if offsets.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let first = *offsets.first().unwrap();
    let last = *offsets.last().unwrap();

    // Contiguous runs, because that is what the protocol carries; a run of one is still
    let mut ranges: Vec<AcquiredRecords> = Vec::new();
    let counts = delivery_counts(group, topic, partition, first, last)?;
    for o in offsets {
        let dc = counts.get(&o).copied().unwrap_or(1) as i16;
        match ranges.last_mut() {
            Some(r) if r.last_offset + 1 == o && r.delivery_count == dc => r.last_offset = o,
            _ => ranges.push(AcquiredRecords {
                first_offset: o,
                last_offset: o,
                delivery_count: dc,
                ..Default::default()
            }),
        }
    }
    Ok((slice.bytes, ranges))
}

fn delivery_counts(
    group: &str,
    topic: u32,
    partition: i32,
    first: i64,
    last: i64,
) -> Result<BTreeMap<i64, i32>, HandlerError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT record_offset, delivery_count FROM kafgres_share_inflight
              WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3
                AND record_offset BETWEEN $4 AND $5",
            None,
            &[
                group.into(),
                (topic as i32).into(),
                partition.into(),
                first.into(),
                last.into(),
            ],
        )?;
        let mut out = BTreeMap::new();
        for r in rows {
            if let (Some(o), Some(c)) = (r.get::<i64>(1)?, r.get::<i32>(2)?) {
                out.insert(o, c);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

/// `79 ShareAcknowledge` — acknowledgements without a fetch.
pub fn share_acknowledge(
    req: &ShareAcknowledgeRequest,
    authz: &crate::acl::Authz,
) -> Result<ShareAcknowledgeResponse, HandlerError> {
    let group = req.group_id.clone().unwrap_or_default();
    let member = req.member_id.clone().unwrap_or_default();
    if let Err(code) = authz.check(
        crate::acl::Operation::Read,
        crate::acl::ResourceType::Group,
        &group,
    ) {
        return Ok(ShareAcknowledgeResponse {
            error_code: code.code(),
            error_message: Some("not authorized to read this group".into()),
            ..Default::default()
        });
    }
    super::check_admin_len("share acknowledge topics", req.topics.len())?;

    if req.share_session_epoch < 0 {
        release_all(&group, &member)?;
    }

    let mut responses = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let resolved = crate::meta::topic_by_uuid(&t.topic_id.0)
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        let mut partitions = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let code = match &resolved {
                None => ErrorCode::UnknownTopicId,
                Some((topic_id, _)) => {
                    let mut code = ErrorCode::None;
                    for b in &p.acknowledgement_batches {
                        let acks = decode_acks(&b.acknowledge_types);
                        if let Err(e) = acknowledge(
                            &group, &member, *topic_id, p.partition_index,
                            b.first_offset, b.last_offset, &acks,
                        ) {
                            code = ack_error(&e);
                            if code != ErrorCode::InvalidRecordState {
                                log!("kafgres: share acknowledge: {e}");
                            }
                        }
                    }
                    code
                }
            };
            partitions.push(AckPartitionData {
                partition_index: p.partition_index,
                error_code: code.code(),
                ..Default::default()
            });
        }
        responses.push(ShareAcknowledgeTopicResponse {
            topic_id: t.topic_id,
            partitions,
            ..Default::default()
        });
    }
    Ok(ShareAcknowledgeResponse {
        error_code: ErrorCode::None.code(),
        acquisition_lock_timeout_ms: lock_duration_ms() as i32,
        responses,
        ..Default::default()
    })
}

pub fn describe(
    req: &ShareGroupDescribeRequest,
    authz: &crate::acl::Authz,
) -> Result<ShareGroupDescribeResponse, HandlerError> {
    super::check_admin_len("described share groups", req.group_ids.len())?;
    let mut groups = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        if authz
            .check(crate::acl::Operation::Describe, crate::acl::ResourceType::Group, gid)
            .is_err()
        {
            groups.push(DescribedGroup {
                error_code: ErrorCode::GroupAuthorizationFailed.code(),
                group_id: gid.clone(),
                ..Default::default()
            });
            continue;
        }
        let Some((epoch, state)) = describe_group(gid)? else {
            groups.push(DescribedGroup {
                error_code: ErrorCode::GroupIdNotFound.code(),
                group_id: gid.clone(),
                ..Default::default()
            });
            continue;
        };
        let members: Vec<DescMember> = describe_members(gid)?
            .into_iter()
            .map(|(id, epoch, subs)| DescMember {
                member_id: id,
                member_epoch: epoch,
                subscribed_topic_names: subs,
                ..Default::default()
            })
            .collect();
        groups.push(DescribedGroup {
            error_code: ErrorCode::None.code(),
            group_id: gid.clone(),
            group_state: state,
            group_epoch: epoch,
            assignment_epoch: epoch,
            // Every member reads every partition it subscribes to, so there is no assignor
            assignor_name: "simple".to_string(),
            members,
            authorized_operations: i32::MIN,
            ..Default::default()
        });
    }
    Ok(ShareGroupDescribeResponse { groups, ..Default::default() })
}
