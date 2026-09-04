//! KIP-848 (`68 ConsumerGroupHeartbeat`, `69 ConsumerGroupDescribe`), assignment done

use std::collections::{BTreeMap, BTreeSet};

use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use kafgres_codec::generated::consumer_group_heartbeat_response::{
    Assignment, ConsumerGroupHeartbeatResponse, TopicPartitions as RespTopicPartitions,
};

use kafgres_codec::generated::consumer_group_describe_request::ConsumerGroupDescribeRequest;
use kafgres_codec::generated::consumer_group_describe_response::{
    Assignment as DescAssignment, ConsumerGroupDescribeResponse, DescribedGroup,
    Member as DescMember, TopicPartitions as DescTopicPartitions,
};

use super::HandlerError;

const HEARTBEAT_INTERVAL_MS: i32 = 5_000;

/// A member silent this long is gone and its partitions reassigned; Kafka derives this from
pub const SESSION_TIMEOUT_MS: i64 = 45_000;

const ASSIGNOR: &str = "uniform";

const MAX_SUBSCRIBED_TOPICS: usize = 5_000;
const MAX_OWNED_PARTITIONS: usize = 50_000;
/// Kafka's `group.consumer.max.size`; over it, `GROUP_MAX_SIZE_REACHED`.
const MAX_GROUP_SIZE: i64 = 1_000;
/// How long a target assignment may go without a recompute while the group epoch has not
const REASSIGN_INTERVAL_MS: i64 = 30_000;

const MAX_DESCRIBED_MEMBERS: usize = 1_000;

/// `topic_id:partition`, keyed on oid so a recreated topic cannot inherit stale assignments.
fn tp(topic: u32, partition: i32) -> String {
    format!("{topic}:{partition}")
}

fn parse_tp(s: &str) -> Option<(u32, i32)> {
    let (t, p) = s.split_once(':')?;
    Some((t.parse().ok()?, p.parse().ok()?))
}

/// Spread every subscribed partition over the members that want it (`uniform`), evenly and
fn assign(members: &BTreeMap<String, BTreeSet<String>>, partitions_of: &BTreeMap<String, Vec<i32>>)
    -> BTreeMap<String, BTreeSet<String>>
{
    let mut out: BTreeMap<String, BTreeSet<String>> =
        members.keys().map(|m| (m.clone(), BTreeSet::new())).collect();

    // Topic by topic, so a member's fair share of a busy topic is not diluted by quiet ones.
    let mut topics: Vec<&String> = partitions_of.keys().collect();
    topics.sort();
    for topic in topics {
        let interested: Vec<&String> = members
            .iter()
            .filter(|(_, subs)| subs.contains(topic))
            .map(|(m, _)| m)
            .collect();
        if interested.is_empty() {
            continue;
        }
        let Some(parts) = partitions_of.get(topic) else { continue };
        for (i, p) in parts.iter().enumerate() {
            let who = interested[i % interested.len()];
            if let Some(set) = out.get_mut(who) {
                set.insert(format!("{topic}:{p}"));
            }
        }
    }
    out
}

struct Member {
    member_id: String,
    epoch: i32,
    subscribed: Vec<String>,
    owned: Vec<String>,
    granted: Vec<String>,
    target: Vec<String>,
}

fn load_members(group: &str) -> Result<Vec<Member>, HandlerError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT member_id, member_epoch, subscribed, owned, granted, target
               FROM kafgres_consumer_group_members WHERE group_id = $1 ORDER BY member_id",
            None,
            &[group.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(Member {
                member_id: r.get::<String>(1)?.unwrap_or_default(),
                epoch: r.get::<i32>(2)?.unwrap_or(0),
                subscribed: r.get::<Vec<String>>(3)?.unwrap_or_default(),
                owned: r.get::<Vec<String>>(4)?.unwrap_or_default(),
                granted: r.get::<Vec<String>>(5)?.unwrap_or_default(),
                target: r.get::<Vec<String>>(6)?.unwrap_or_default(),
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

struct MemberRow {
    epoch: i32,
    subscribed: Vec<String>,
    /// Still holding a partition it was told to release, past its own `rebalance.timeout.ms`.
    revoke_expired: bool,
}

fn lookup_member(group: &str, member: &str) -> Result<Option<MemberRow>, HandlerError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT member_epoch, subscribed,
                    (revoking_since IS NOT NULL
                     AND revoking_since < now() - make_interval(
                             secs => GREATEST(rebalance_timeout_ms, 1000)::double precision / 1000))
               FROM kafgres_consumer_group_members
              WHERE group_id = $1 AND member_id = $2",
            None,
            &[group.into(), member.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some(MemberRow {
                epoch: r.get::<i32>(1)?.unwrap_or(0),
                subscribed: r.get::<Vec<String>>(2)?.unwrap_or_default(),
                revoke_expired: r.get::<bool>(3)?.unwrap_or(false),
            }));
        }
        Ok(None)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

/// Resolve every subscribed topic name to its oid and partition list, in one query through
fn subscribed_partitions(
    names: &BTreeSet<String>,
) -> Result<(BTreeMap<String, Vec<i32>>, BTreeMap<String, String>), HandlerError> {
    let mut parts = BTreeMap::new();
    let mut oid_of_name = BTreeMap::new();
    if names.is_empty() {
        return Ok((parts, oid_of_name));
    }
    let list: Vec<String> = names.iter().cloned().collect();
    let loaded = crate::meta::load_topics(Some(&list)).map_err(|e| HandlerError::Internal(e.to_string()))?;
    for t in loaded {
        // A subscription to a topic that does not exist is not an error; the member gets nothing.
        let oid = t.topic_id.to_string();
        parts.insert(oid.clone(), t.partitions.iter().map(|p| p.partition).collect());
        oid_of_name.insert(t.name, oid);
    }
    Ok((parts, oid_of_name))
}

/// `68 ConsumerGroupHeartbeat` — one RPC replacing JoinGroup/SyncGroup/Heartbeat; every call
pub fn heartbeat(
    req: &ConsumerGroupHeartbeatRequest,
    authz: &crate::acl::Authz,
) -> Result<ConsumerGroupHeartbeatResponse, HandlerError> {
    if let Some(names) = &req.subscribed_topic_names {
        if names.len() > MAX_SUBSCRIBED_TOPICS {
            return Err(HandlerError::TooLarge { what: "subscribed topics", n: names.len() });
        }
    }
    if let Some(tps) = &req.topic_partitions {
        let n: usize = tps.iter().map(|t| t.partitions.len()).sum();
        if n > MAX_OWNED_PARTITIONS {
            return Err(HandlerError::TooLarge { what: "owned partitions", n });
        }
    }

    if let Err(code) = authz.check(
        crate::acl::Operation::Read,
        crate::acl::ResourceType::Group,
        &req.group_id,
    ) {
        return Ok(err_hb(code, "not authorized to read this group"));
    }

    // A group id already used by another protocol is refused: two groups with one name, each
    let classic: Option<i64> = Spi::get_one_with_args(
        "SELECT (SELECT count(*) FROM kafgres_groups WHERE group_id = $1)
              + (SELECT count(*) FROM kafgres_share_groups WHERE group_id = $1)",
        &[req.group_id.as_str().into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    if classic.unwrap_or(0) > 0 {
        return Ok(err_hb(
            ErrorCode::GroupIdNotFound,
            "this group id is already in use by another group protocol; \
             set group.protocol=classic or use a different group.id",
        ));
    }

    if let Some(a) = req.server_assignor.as_deref() {
        if a != ASSIGNOR {
            return Ok(err_hb(
                ErrorCode::UnsupportedAssignor,
                "this broker implements the 'uniform' server assignor only",
            ));
        }
    }
    // Refused, not ignored: a regex silently treated as "subscribed to nothing" is a
    if req.subscribed_topic_regex.is_some() {
        return Ok(err_hb(
            ErrorCode::InvalidRequest,
            "regex subscriptions require ConsumerGroupHeartbeat v1, which this broker does not advertise",
        ));
    }

    // Leaving: `-1` is a plain leave, `-2` a static member leaving temporarily. Handled
    if req.member_epoch < 0 {
        if req.member_id.is_empty() {
            return Ok(err_hb(ErrorCode::UnknownMemberId, "leaving requires a member id"));
        }
        // -1 and -2 are the only defined leave epochs; anything below would delete a member
        if req.member_epoch < -2 {
            return Ok(err_hb(
                ErrorCode::InvalidRequest,
                "member epoch below -2 is not a defined leave",
            ));
        }
        let member_id = req.member_id.clone();
        if req.member_epoch == -2 {
            // Static membership: the assignment stays reserved and the group epoch does not
            Spi::run_with_args(
                "UPDATE kafgres_consumer_group_members SET static_departed = true
                  WHERE group_id = $1 AND member_id = $2",
                &[req.group_id.as_str().into(), member_id.as_str().into()],
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        } else {
            // Bump only if a member was actually removed: a consumer swept at the session
            let removed: Option<i64> = Spi::get_one_with_args(
                "WITH gone AS (DELETE FROM kafgres_consumer_group_members
                                WHERE group_id = $1 AND member_id = $2 RETURNING 1)
                 SELECT count(*) FROM gone",
                &[req.group_id.as_str().into(), member_id.as_str().into()],
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
            if removed.unwrap_or(0) > 0 {
                bump_epoch(&req.group_id)?;
            }
        }
        return Ok(ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None.code(),
            member_id: Some(member_id),
            member_epoch: req.member_epoch,
            heartbeat_interval_ms: 0,
            assignment: None,
            ..Default::default()
        });
    }

    let member_id = if req.member_id.is_empty() { uuid_like()? } else { req.member_id.clone() };

    // Every subscribed topic needs READ, and the failure must surface here: partitions
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

    Spi::run_with_args(
        "INSERT INTO kafgres_consumer_groups (group_id) VALUES ($1) ON CONFLICT DO NOTHING",
        &[req.group_id.as_str().into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    // Static membership: a `-2` leaver returning under a fresh member id gets the reserved
    if let Some(inst) = req.instance_id.as_deref() {
        let holder: Option<(String, bool)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT member_id, static_departed FROM kafgres_consumer_group_members
                  WHERE group_id = $1 AND instance_id = $2",
                Some(1),
                &[req.group_id.as_str().into(), inst.into()],
            )?;
            for r in rows {
                return Ok::<_, pgrx::spi::Error>(Some((
                    r.get::<String>(1)?.unwrap_or_default(),
                    r.get::<bool>(2)?.unwrap_or(false),
                )));
            }
            Ok(None)
        })
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        if let Some((holder_id, departed)) = holder {
            if holder_id != member_id {
                if !departed {
                    return Ok(err_hb(
                        ErrorCode::UnreleasedInstanceId,
                        "another member is already using this group.instance.id",
                    ));
                }
                // `revoking_since` is cleared with the adoption: it is wall-clock and the row
                Spi::run_with_args(
                    "UPDATE kafgres_consumer_group_members
                        SET member_id = $3, static_departed = false, last_seen = now(),
                            revoking_since = NULL
                      WHERE group_id = $1 AND member_id = $2",
                    &[req.group_id.as_str().into(), holder_id.as_str().into(), member_id.as_str().into()],
                )
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            }
        }
    }

    let existing = lookup_member(&req.group_id, &member_id)?;

    // Joining is epoch 0, not an absent member id: in this protocol the client generates its
    match &existing {
        None if req.member_epoch != 0 => {
            return Ok(err_hb(ErrorCode::UnknownMemberId, "unknown member; rejoin"))
        }
        Some(row) if req.member_epoch != 0 && req.member_epoch != row.epoch => {
            return Ok(err_hb(ErrorCode::FencedMemberEpoch, "member epoch is stale; rejoin"))
        }
        // Still holding a partition past its own rebalance timeout: the new owner waits
        Some(row) if row.revoke_expired => {
            Spi::run_with_args(
                "DELETE FROM kafgres_consumer_group_members WHERE group_id = $1 AND member_id = $2",
                &[req.group_id.as_str().into(), member_id.as_str().into()],
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
            bump_epoch(&req.group_id)?;
            return Ok(err_hb(
                ErrorCode::UnknownMemberId,
                "did not release a revoked partition within rebalance.timeout.ms; rejoin",
            ));
        }
        _ => {}
    }

    if existing.is_none() {
        let size: Option<i64> = Spi::get_one_with_args(
            "SELECT (SELECT count(*) FROM kafgres_consumer_group_members WHERE group_id = $1)",
            &[req.group_id.as_str().into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        if size.unwrap_or(0) >= MAX_GROUP_SIZE {
            return Ok(err_hb(ErrorCode::GroupMaxSizeReached, "group is at its member limit"));
        }
    }

    // A null `topic_partitions` means "unchanged", not "nothing".
    let owned: Option<Vec<String>> = match &req.topic_partitions {
        None => None,
        Some(tps) => {
            let uuids: Vec<[u8; 16]> = tps.iter().map(|t| t.topic_id.0).collect();
            let ids = crate::meta::topic_ids_by_uuids(&uuids)
                .map_err(|e| HandlerError::Internal(e.to_string()))?;
            let mut v = Vec::new();
            for t in tps {
                // An unresolvable uuid is a topic deleted under the member; drop it rather
                let Some(oid) = ids.get(&t.topic_id.0) else { continue };
                v.extend(t.partitions.iter().map(|p| tp(*oid, *p)));
            }
            Some(v)
        }
    };
    let subs: Option<Vec<String>> = req.subscribed_topic_names.clone();

    // Computed before the upsert overwrites the stored value; afterwards it would compare
    let subscription_changed = match (&subs, &existing) {
        (Some(new), Some(row)) => {
            let a: BTreeSet<&String> = new.iter().collect();
            let b: BTreeSet<&String> = row.subscribed.iter().collect();
            a != b
        }
        _ => false,
    };

    upsert_member(&req.group_id, &member_id, req, subs.as_deref(), owned.as_deref())?;
    if existing.is_none() || subscription_changed {
        bump_epoch(&req.group_id)?;
    }

    let prev_epoch = existing.as_ref().map(|r| r.epoch).unwrap_or(0);
    // `None` means "unchanged"; the difference decides whether `granted` may shrink.
    let acknowledged: Option<BTreeSet<String>> =
        owned.as_ref().map(|v| v.iter().cloned().collect());
    let (epoch, assignment) =
        reconcile(&req.group_id, &member_id, prev_epoch, acknowledged.as_ref())?;

    Ok(ConsumerGroupHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        error_message: None,
        member_id: Some(member_id),
        member_epoch: epoch,
        heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
        assignment,
        ..Default::default()
    })
}

fn err_hb(code: ErrorCode, msg: &str) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: code.code(),
        error_message: Some(msg.to_string()),
        member_id: None,
        member_epoch: 0,
        heartbeat_interval_ms: 0,
        assignment: None,
        ..Default::default()
    }
}

/// Recompute the target assignment only when the group epoch moved, then tell this member
fn reconcile(
    group: &str,
    me: &str,
    prev_epoch: i32,
    acknowledged: Option<&BTreeSet<String>>,
) -> Result<(i32, Option<Assignment>), HandlerError> {
    let epochs: Option<(i32, i32, bool)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT group_epoch, assignment_epoch,
                    updated_at < now() - make_interval(secs => $2::double precision / 1000)
               FROM kafgres_consumer_groups WHERE group_id = $1",
            Some(1),
            &[group.into(), REASSIGN_INTERVAL_MS.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some((
                r.get::<i32>(1)?.unwrap_or(0),
                r.get::<i32>(2)?.unwrap_or(0),
                r.get::<bool>(3)?.unwrap_or(true),
            )));
        }
        Ok(None)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let (group_epoch, assignment_epoch, stale) = epochs.unwrap_or((0, 0, true));

    let members = load_members(group)?;

    let target: BTreeMap<String, BTreeSet<String>> = if assignment_epoch == group_epoch && !stale {
        // Membership and subscriptions unchanged, so the stored target is still the answer.
        members
            .iter()
            .map(|m| (m.member_id.clone(), m.target.iter().cloned().collect()))
            .collect()
    } else {
        recompute_target(group, &members)?
    };

    // Everything still held by somebody other than its intended owner; `granted` is unioned
    let mut held_elsewhere: BTreeSet<String> = BTreeSet::new();
    for m in &members {
        let mine = target.get(&m.member_id);
        for p in m.owned.iter().chain(m.granted.iter()) {
            if mine.map(|t| !t.contains(p)).unwrap_or(true) {
                held_elsewhere.insert(p.clone());
            }
        }
    }

    let my_target = target.get(me).cloned().unwrap_or_default();
    let grantable: BTreeSet<String> = my_target
        .iter()
        .filter(|p| !held_elsewhere.contains(*p))
        .cloned()
        .collect();

    // What is written back to `granted` is not `grantable`: `granted` shrinks only when the
    let my_granted: BTreeSet<String> = members
        .iter()
        .find(|m| m.member_id == me)
        .map(|m| m.granted.iter().cloned().collect())
        .unwrap_or_default();
    let keep: BTreeSet<String> = match acknowledged {
        Some(owned) => my_granted.intersection(owned).cloned().collect(),
        None => my_granted,
    };
    let now_granted: BTreeSet<String> = keep.union(&grantable).cloned().collect();

    // Caught up only when what we may grant is the whole target; until then the member keeps
    let caught_up = grantable.len() == my_target.len();
    let epoch = if caught_up { group_epoch } else { prev_epoch };

    // Start the fence clock `heartbeat` checks while this member holds something it was told
    let still_revoking = now_granted.iter().any(|p| !my_target.contains(p));

    let granted_v: Vec<String> = now_granted.iter().cloned().collect();
    Spi::run_with_args(
        "UPDATE kafgres_consumer_group_members
            SET member_epoch = $3, granted = $4, last_seen = now(),
                revoking_since = CASE WHEN $5 THEN COALESCE(revoking_since, now()) ELSE NULL END
          WHERE group_id = $1 AND member_id = $2",
        &[group.into(), me.into(), epoch.into(), granted_v.into(), still_revoking.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    let mut per_topic: BTreeMap<u32, Vec<i32>> = BTreeMap::new();
    for s in &grantable {
        if let Some((t, p)) = parse_tp(s) {
            per_topic.entry(t).or_default().push(p);
        }
    }
    let tps = to_wire(&per_topic)?;

    Ok((
        epoch,
        Some(Assignment { topic_partitions: tps, ..Default::default() }),
    ))
}

/// Run the assignor and store the result. Only reached when the group epoch has moved.
fn recompute_target(
    group: &str,
    members: &[Member],
) -> Result<BTreeMap<String, BTreeSet<String>>, HandlerError> {
    let all_names: BTreeSet<String> = members.iter().flat_map(|m| m.subscribed.clone()).collect();
    let (partitions_of, oid_of_name) = subscribed_partitions(&all_names)?;

    let by_oid: BTreeMap<String, BTreeSet<String>> = members
        .iter()
        .map(|m| {
            let oids = m
                .subscribed
                .iter()
                .filter_map(|n| oid_of_name.get(n).cloned())
                .collect();
            (m.member_id.clone(), oids)
        })
        .collect();
    let target = assign(&by_oid, &partitions_of);

    // Written only where it changed: Postgres writes a new tuple version even when the value
    for m in members {
        let stored: BTreeSet<&String> = m.target.iter().collect();
        let Some(want) = target.get(&m.member_id) else { continue };
        if stored == want.iter().collect() {
            continue;
        }
        let v: Vec<String> = want.iter().cloned().collect();
        Spi::run_with_args(
            "UPDATE kafgres_consumer_group_members SET target = $3
              WHERE group_id = $1 AND member_id = $2",
            &[group.into(), m.member_id.as_str().into(), v.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }
    Spi::run_with_args(
        "UPDATE kafgres_consumer_groups SET assignment_epoch = group_epoch, updated_at = now(),
            state = CASE WHEN (SELECT count(*) FROM kafgres_consumer_group_members
                                WHERE group_id = $1) = 0 THEN 'Empty' ELSE 'Stable' END
          WHERE group_id = $1",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;

    Ok(target)
}

fn bump_epoch(group: &str) -> Result<(), HandlerError> {
    Spi::run_with_args(
        "UPDATE kafgres_consumer_groups SET group_epoch = group_epoch + 1, updated_at = now()
          WHERE group_id = $1",
        &[group.into()],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))
}

/// Insert or refresh the member. `COALESCE` on every optional column: `null` means
fn upsert_member(
    group: &str,
    member: &str,
    req: &ConsumerGroupHeartbeatRequest,
    subs: Option<&[String]>,
    owned: Option<&[String]>,
) -> Result<(), HandlerError> {
    Spi::run_with_args(
        "INSERT INTO kafgres_consumer_group_members
              (group_id, member_id, instance_id, rack_id, rebalance_timeout_ms,
               subscribed, owned, static_departed, last_seen)
         VALUES ($1, $2, $3, $4, COALESCE(NULLIF($5, -1), 300000),
                 COALESCE($6::text[], '{}'), COALESCE($7::text[], '{}'), false, now())
         ON CONFLICT (group_id, member_id) DO UPDATE SET
              instance_id = COALESCE($3, kafgres_consumer_group_members.instance_id),
              rack_id = COALESCE($4, kafgres_consumer_group_members.rack_id),
              rebalance_timeout_ms =
                  COALESCE(NULLIF($5, -1), kafgres_consumer_group_members.rebalance_timeout_ms),
              subscribed = COALESCE($6::text[], kafgres_consumer_group_members.subscribed),
              owned = COALESCE($7::text[], kafgres_consumer_group_members.owned),
              static_departed = false,
              last_seen = now()",
        &[
            group.into(),
            member.into(),
            req.instance_id.clone().into(),
            req.rack_id.clone().into(),
            req.rebalance_timeout_ms.into(),
            subs.map(|s| s.to_vec()).into(),
            owned.map(|o| o.to_vec()).into(),
        ],
    )
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    Ok(())
}

/// Members that stopped heartbeating; their partitions go back into the pool. Bounded per
pub fn expire_members() -> Result<usize, HandlerError> {
    const PER_SWEEP: i64 = 500;
    let gone: Vec<String> = Spi::connect(|client| {
        let rows = client.select(
            "DELETE FROM kafgres_consumer_group_members
              WHERE ctid = ANY (ARRAY(
                    SELECT ctid FROM kafgres_consumer_group_members
                     WHERE last_seen < now() - make_interval(secs => $1::double precision / 1000)
                     ORDER BY last_seen LIMIT $2))
             RETURNING group_id",
            None,
            &[SESSION_TIMEOUT_MS.into(), PER_SWEEP.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let Some(g) = r.get::<String>(1)? {
                out.push(g);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for g in gone {
        if seen.insert(g.clone()) {
            bump_epoch(&g)?;
            // A group that just lost its last member has no heartbeat coming to write
            Spi::run_with_args(
                "UPDATE kafgres_consumer_groups SET state = 'Empty'
                  WHERE group_id = $1
                    AND NOT EXISTS (SELECT 1 FROM kafgres_consumer_group_members
                                     WHERE group_id = $1)",
                &[g.as_str().into()],
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        }
    }
    Ok(seen.len())
}

/// Kafka's member ids are UUIDs. Only uniqueness matters; the format is what tooling expects.
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

/// Render `topic_id:partition` pairs for the wire, resolving every oid to the topic's real
fn to_wire(per_topic: &BTreeMap<u32, Vec<i32>>) -> Result<Vec<RespTopicPartitions>, HandlerError> {
    let ids: Vec<u32> = per_topic.keys().copied().collect();
    let uuids = crate::meta::topic_uuids_by_ids(&ids)
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    let mut out = Vec::with_capacity(per_topic.len());
    for (t, ps) in per_topic {
        // A topic dropped since the assignment was stored: omitted, not reported as the zero
        let Some(u) = uuids.get(t) else { continue };
        let mut ps = ps.clone();
        ps.sort_unstable();
        out.push(RespTopicPartitions {
            topic_id: kafgres_codec::primitives::Uuid(*u),
            partitions: ps,
            ..Default::default()
        });
    }
    Ok(out)
}

/// `69 ConsumerGroupDescribe` — what `kafka-consumer-groups.sh --describe` reads here;
pub fn describe(
    req: &ConsumerGroupDescribeRequest,
    authz: &crate::acl::Authz,
) -> Result<ConsumerGroupDescribeResponse, HandlerError> {
    super::check_admin_len("described groups", req.group_ids.len())?;
    // Bounded as it is assembled: group ids expand to every member of every group.
    let mut budget = MAX_DESCRIBED_MEMBERS;
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
        let found: Option<(i32, i32, String, String)> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT group_epoch, assignment_epoch, state, assignor
                   FROM kafgres_consumer_groups WHERE group_id = $1",
                Some(1),
                &[gid.as_str().into()],
            )?;
            for r in rows {
                return Ok::<_, pgrx::spi::Error>(Some((
                    r.get::<i32>(1)?.unwrap_or(0),
                    r.get::<i32>(2)?.unwrap_or(0),
                    r.get::<String>(3)?.unwrap_or_default(),
                    r.get::<String>(4)?.unwrap_or_default(),
                )));
            }
            Ok(None)
        })
        .map_err(|e| HandlerError::Internal(e.to_string()))?;

        let Some((gepoch, aepoch, state, assignor)) = found else {
            groups.push(DescribedGroup {
                error_code: ErrorCode::GroupIdNotFound.code(),
                group_id: gid.clone(),
                ..Default::default()
            });
            continue;
        };

        // Refused, not truncated, and before any queries run: each past-budget group would
        if budget == 0 {
            return Err(HandlerError::TooLarge {
                what: "described group members",
                n: MAX_DESCRIBED_MEMBERS,
            });
        }

        let members = load_members(gid)?;
        let mut oids: BTreeSet<u32> = BTreeSet::new();
        for m in &members {
            for s in m.owned.iter().chain(m.target.iter()) {
                if let Some((t, _)) = parse_tp(s) {
                    oids.insert(t);
                }
            }
        }
        let uuids = crate::meta::topic_uuids_by_ids(&oids.into_iter().collect::<Vec<_>>())
            .map_err(|e| HandlerError::Internal(e.to_string()))?;

        let mut described = Vec::new();
        for m in &members {
            if budget == 0 {
                return Err(HandlerError::TooLarge {
                    what: "described group members",
                    n: MAX_DESCRIBED_MEMBERS,
                });
            }
            budget -= 1;
            described.push(DescMember {
                member_id: m.member_id.clone(),
                member_epoch: m.epoch,
                subscribed_topic_names: m.subscribed.clone(),
                // What the member reports holding, not what it was granted: `--describe`
                assignment: to_assignment(&m.owned, &uuids),
                target_assignment: to_assignment(&m.target, &uuids),
                ..Default::default()
            });
        }

        groups.push(DescribedGroup {
            error_code: ErrorCode::None.code(),
            group_id: gid.clone(),
            group_state: state,
            group_epoch: gepoch,
            assignment_epoch: aepoch,
            assignor_name: assignor,
            members: described,
            authorized_operations: i32::MIN,
            ..Default::default()
        });
    }
    Ok(ConsumerGroupDescribeResponse {
        throttle_time_ms: 0,
        groups,
        ..Default::default()
    })
}

fn to_assignment(
    pairs: &[String],
    uuids: &std::collections::HashMap<u32, [u8; 16]>,
) -> DescAssignment {
    let mut per_topic: BTreeMap<u32, Vec<i32>> = BTreeMap::new();
    for s in pairs {
        if let Some((t, p)) = parse_tp(s) {
            per_topic.entry(t).or_default().push(p);
        }
    }
    DescAssignment {
        topic_partitions: per_topic
            .into_iter()
            .filter_map(|(t, mut ps)| {
                let u = uuids.get(&t)?;
                ps.sort_unstable();
                Some(DescTopicPartitions {
                    topic_id: kafgres_codec::primitives::Uuid(*u),
                    partitions: ps,
                    ..Default::default()
                })
            })
            .collect(),
        ..Default::default()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use kafgres_codec::generated::consumer_group_heartbeat_request::TopicPartitions as ReqTopicPartitions;

    /// A `#[pg_test]` body runs in a backend, not the worker, so the init chain runs here.
    fn schema() {
        crate::ensure_tables_exist();
    }

    fn allow_all() -> crate::acl::AclCache {
        crate::acl::AclCache::default()
    }

    fn hb(
        acls: &crate::acl::AclCache,
        group: &str,
        member: &str,
        epoch: i32,
        subs: Option<Vec<String>>,
        owned: Option<Vec<(kafgres_codec::primitives::Uuid, Vec<i32>)>>,
    ) -> ConsumerGroupHeartbeatResponse {
        let authz = crate::acl::Authz {
            acls,
            principal: crate::acl::Principal::user("postgres", "127.0.0.1"),
        };
        let req = ConsumerGroupHeartbeatRequest {
            group_id: group.to_string(),
            member_id: member.to_string(),
            member_epoch: epoch,
            rebalance_timeout_ms: 300_000,
            subscribed_topic_names: subs,
            topic_partitions: owned.map(|v| {
                v.into_iter()
                    .map(|(topic_id, partitions)| ReqTopicPartitions {
                        topic_id,
                        partitions,
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        };
        heartbeat(&req, &authz).expect("heartbeat failed")
    }

    fn granted_partitions(r: &ConsumerGroupHeartbeatResponse) -> Vec<i32> {
        let mut out: Vec<i32> = r
            .assignment
            .as_ref()
            .map(|a| a.topic_partitions.iter().flat_map(|t| t.partitions.clone()).collect())
            .unwrap_or_default();
        out.sort_unstable();
        out
    }

    fn make_topic(name: &str, partitions: i32) -> kafgres_codec::primitives::Uuid {
        let created = crate::meta::create_topic(name, partitions, &[]).expect("create topic");
        kafgres_codec::primitives::Uuid(created.uuid)
    }

    /// The module's core property, checked inside the grant/acknowledge window — converged
    #[pg_test]
    fn a_granted_partition_is_not_regranted_before_it_is_acknowledged() {
        schema();
        let uuid = make_topic("cg-grant-window", 2);
        let acls = allow_all();

        let a = hb(&acls, "g1", "member-a", 0, Some(vec!["cg-grant-window".into()]), Some(vec![]));
        assert_eq!(granted_partitions(&a), vec![0, 1], "A should get the whole topic");

        let b = hb(&acls, "g1", "member-b", 0, Some(vec!["cg-grant-window".into()]), Some(vec![]));
        assert_eq!(
            granted_partitions(&b),
            Vec::<i32>::new(),
            "B was handed a partition A holds but has not released yet"
        );

        let a2 = hb(&acls, "g1", "member-a", a.member_epoch,
                    Some(vec!["cg-grant-window".into()]), None);
        assert_eq!(granted_partitions(&a2), vec![0], "A should be revoked partition 1");

        // The regression this exists for: writing the smaller grant back to `granted` drops
        let b_mid = hb(&acls, "g1", "member-b", b.member_epoch,
                       Some(vec!["cg-grant-window".into()]), None);
        assert_eq!(
            granted_partitions(&b_mid),
            Vec::<i32>::new(),
            "B took a partition that A was told to drop but has never acknowledged dropping"
        );

        let _ = hb(&acls, "g1", "member-a", a2.member_epoch,
                   Some(vec!["cg-grant-window".into()]), Some(vec![(uuid, vec![0])]));

        let b2 = hb(&acls, "g1", "member-b", b_mid.member_epoch,
                    Some(vec!["cg-grant-window".into()]), Some(vec![]));
        assert_eq!(granted_partitions(&b2), vec![1], "B never received the released partition");
    }

    /// A member's epoch reaches the group's only once it fully holds its target; until then
    #[pg_test]
    fn a_member_mid_reconciliation_keeps_its_own_epoch() {
        schema();
        let uuid = make_topic("cg-epochs", 2);
        let acls = allow_all();

        let a = hb(&acls, "g2", "member-a", 0, Some(vec!["cg-epochs".into()]), Some(vec![]));
        assert!(a.member_epoch > 0, "a caught-up member takes the group epoch");
        let a = hb(&acls, "g2", "member-a", a.member_epoch,
                   Some(vec!["cg-epochs".into()]), Some(vec![(uuid, vec![0, 1])]));
        let settled = a.member_epoch;

        let b = hb(&acls, "g2", "member-b", 0, Some(vec!["cg-epochs".into()]), Some(vec![]));
        assert_eq!(b.member_epoch, 0, "an unreconciled member must not be advanced");

        let _ = hb(&acls, "g2", "member-c", 0, Some(vec!["cg-epochs".into()]), Some(vec![]));
        let b2 = hb(&acls, "g2", "member-b", b.member_epoch,
                    Some(vec!["cg-epochs".into()]), Some(vec![]));
        assert_eq!(b2.member_epoch, 0, "B was given a fabricated epoch");

        let a2 = hb(&acls, "g2", "member-a", settled,
                    Some(vec!["cg-epochs".into()]), Some(vec![(uuid, vec![0, 1])]));
        assert!(a2.member_epoch > settled, "a member that only sheds partitions is caught up");
    }

    /// A changed subscription must move the group epoch or the assignor never re-runs; the
    #[pg_test]
    fn subscribing_to_another_topic_reassigns() {
        schema();
        make_topic("cg-sub-one", 1);
        make_topic("cg-sub-two", 1);
        let acls = allow_all();

        let a = hb(&acls, "g3", "member-a", 0, Some(vec!["cg-sub-one".into()]), Some(vec![]));
        assert_eq!(a.assignment.as_ref().unwrap().topic_partitions.len(), 1);

        let a2 = hb(&acls, "g3", "member-a", a.member_epoch,
                    Some(vec!["cg-sub-one".into(), "cg-sub-two".into()]), None);
        assert_eq!(
            a2.assignment.as_ref().unwrap().topic_partitions.len(),
            2,
            "the second topic was never assigned; the subscription change did not re-run the assignor"
        );
    }

    /// A static member leaving with epoch -2 reserves its assignment without moving the
    #[pg_test]
    fn a_static_member_keeps_its_assignment_across_a_restart() {
        schema();
        let uuid = make_topic("cg-static", 2);
        let acls = allow_all();

        let authz = crate::acl::Authz {
            acls: &acls,
            principal: crate::acl::Principal::user("postgres", "127.0.0.1"),
        };
        let mut req = ConsumerGroupHeartbeatRequest {
            group_id: "g4".into(),
            member_id: "member-a".into(),
            member_epoch: 0,
            instance_id: Some("pod-0".into()),
            rebalance_timeout_ms: 300_000,
            subscribed_topic_names: Some(vec!["cg-static".into()]),
            topic_partitions: Some(vec![]),
            ..Default::default()
        };
        let first = heartbeat(&req, &authz).unwrap();
        assert_eq!(granted_partitions(&first), vec![0, 1]);
        let epoch_before: i32 = Spi::get_one_with_args(
            "SELECT (SELECT group_epoch FROM kafgres_consumer_groups WHERE group_id = $1)",
            &["g4".into()],
        )
        .unwrap()
        .unwrap();

        req.member_epoch = -2;
        let left = heartbeat(&req, &authz).unwrap();
        assert_eq!(left.error_code, 0);

        req.member_id = "member-a-restarted".into();
        req.member_epoch = 0;
        req.topic_partitions = Some(vec![(uuid, vec![0, 1])].into_iter()
            .map(|(topic_id, partitions)| ReqTopicPartitions { topic_id, partitions, ..Default::default() })
            .collect());
        let back = heartbeat(&req, &authz).unwrap();
        assert_eq!(granted_partitions(&back), vec![0, 1], "the reserved assignment was lost");

        let epoch_after: i32 = Spi::get_one_with_args(
            "SELECT (SELECT group_epoch FROM kafgres_consumer_groups WHERE group_id = $1)",
            &["g4".into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(epoch_after, epoch_before, "a static restart triggered a rebalance");

        // A *second* live process configured with the same instance id is a duplicate
        let clash = ConsumerGroupHeartbeatRequest {
            member_id: "member-b".into(),
            member_epoch: 0,
            ..req.clone()
        };
        let refused = heartbeat(&clash, &authz).unwrap();
        assert_eq!(
            refused.error_code,
            ErrorCode::UnreleasedInstanceId.code(),
            "two processes were allowed to share one group.instance.id"
        );
    }

    /// A classic protocol group id is refused with the code upstream uses, not
    #[pg_test]
    fn a_classic_group_id_is_refused_with_the_code_kafka_uses() {
        schema();
        Spi::run("INSERT INTO kafgres_groups (group_id, state) VALUES ('g5', 'Stable')").unwrap();
        let acls = allow_all();
        let r = hb(&acls, "g5", "member-a", 0, Some(vec![]), Some(vec![]));
        assert_eq!(r.error_code, ErrorCode::GroupIdNotFound.code());
    }
}
