//! Group coordinator: a state machine and a timer. The leader *client* computes the

use pgrx::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl GroupState {
    pub fn as_str(self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
        }
    }

    fn parse(s: &str) -> GroupState {
        match s {
            "PreparingRebalance" => GroupState::PreparingRebalance,
            "CompletingRebalance" => GroupState::CompletingRebalance,
            "Stable" => GroupState::Stable,
            _ => GroupState::Empty,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NoCommonProtocol;

#[derive(Debug, Clone)]
pub struct Group {
    pub group_id: String,
    pub generation: i32,
    pub state: GroupState,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader_member: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
    pub assignment: Option<Vec<u8>>,
    pub joined_generation: i32,
    pub protocols: Vec<String>,
}

const MAX_SESSION_TIMEOUT_MS: i32 = 300_000;
const MIN_SESSION_TIMEOUT_MS: i32 = 1_000;
const MAX_REBALANCE_TIMEOUT_MS: i32 = 300_000;

pub fn clamp_session_timeout(ms: i32) -> i32 {
    ms.clamp(MIN_SESSION_TIMEOUT_MS, MAX_SESSION_TIMEOUT_MS)
}

pub fn clamp_rebalance_timeout(ms: i32) -> i32 {
    if ms <= 0 {
        60_000
    } else {
        ms.min(MAX_REBALANCE_TIMEOUT_MS)
    }
}

pub fn new_member_id(client_id: &str) -> Result<String, spi::Error> {
    let uuid: String = Spi::get_one("SELECT gen_random_uuid()::text")?.unwrap_or_default();
    let prefix = if client_id.is_empty() {
        "consumer"
    } else {
        client_id
    };
    Ok(format!("{prefix}-{uuid}"))
}

/// NOWAIT read lock: `VACUUM FULL`/`pg_repack` on the bloat-prone members table take
pub fn lock_for_read() -> Result<(), spi::Error> {
    Spi::run(
        "LOCK TABLE kafgres_groups, kafgres_group_members, kafgres_offsets
           IN ACCESS SHARE MODE NOWAIT",
    )
}

/// Parked members send no heartbeats and the SQL sweep can't see them: without this a
pub fn touch_parked(entries: &[(String, String)]) -> Result<(), spi::Error> {
    for (group_id, member_id) in entries {
        touch_heartbeat(group_id, member_id)?;
    }
    Ok(())
}

pub fn load(group_id: &str) -> Result<Option<Group>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT generation, state, protocol_type, protocol_name, leader_member
               FROM kafgres_groups WHERE group_id = $1",
            Some(1),
            &[group_id.into()],
        )?;
        for row in rows {
            return Ok(Some(Group {
                group_id: group_id.to_string(),
                generation: row.get::<i32>(1)?.unwrap_or(0),
                state: GroupState::parse(&row.get::<String>(2)?.unwrap_or_default()),
                protocol_type: row.get::<String>(3)?,
                protocol_name: row.get::<String>(4)?,
                leader_member: row.get::<String>(5)?,
            }));
        }
        Ok(None)
    })
}

pub fn ensure(group_id: &str) -> Result<Group, spi::Error> {
    Spi::run_with_args(
        "INSERT INTO kafgres_groups (group_id, state) VALUES ($1, 'Empty')
         ON CONFLICT (group_id) DO NOTHING",
        &[group_id.into()],
    )?;
    Ok(load(group_id)?.expect("group exists after insert"))
}

pub fn members(group_id: &str) -> Result<Vec<Member>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT member_id, group_instance_id, metadata, assignment, joined_generation, protocols
               FROM kafgres_group_members WHERE group_id = $1 ORDER BY member_id",
            None,
            &[group_id.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(Member {
                member_id: row.get::<String>(1)?.unwrap_or_default(),
                group_instance_id: row.get::<String>(2)?,
                metadata: row.get::<Vec<u8>>(3)?.unwrap_or_default(),
                assignment: row.get::<Vec<u8>>(4)?,
                joined_generation: row.get::<i32>(5)?.unwrap_or(-1),
                protocols: row.get::<Vec<String>>(6)?.unwrap_or_default(),
            });
        }
        Ok(out)
    })
}

pub fn member(group_id: &str, member_id: &str) -> Result<Option<Member>, spi::Error> {
    Ok(members(group_id)?.into_iter().find(|m| m.member_id == member_id))
}

#[allow(clippy::too_many_arguments)]
pub fn protocol_type_conflicts(group_id: &str, protocol_type: &str) -> Result<bool, spi::Error> {
    let conflicts: bool = Spi::get_one_with_args(
        "SELECT COALESCE(
                  (SELECT protocol_type IS NOT NULL AND protocol_type <> '' AND protocol_type <> $2
                     FROM kafgres_groups WHERE group_id = $1),
                  false)",
        &[group_id.into(), protocol_type.into()],
    )?
    .unwrap_or(false);
    Ok(conflicts)
}

pub fn join(
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    client_id: &str,
    client_host: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_type: &str,
    protocols: &[String],
    metadata: &[u8],
) -> Result<(), spi::Error> {
    let g = ensure(group_id)?;

    Spi::run_with_args(
        "INSERT INTO kafgres_group_members
            (group_id, member_id, group_instance_id, client_id, client_host,
             session_timeout_ms, rebalance_timeout_ms, metadata, protocols,
             joined_generation, last_heartbeat)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
         ON CONFLICT (group_id, member_id) DO UPDATE SET
            group_instance_id = EXCLUDED.group_instance_id,
            client_id = EXCLUDED.client_id,
            client_host = EXCLUDED.client_host,
            session_timeout_ms = EXCLUDED.session_timeout_ms,
            rebalance_timeout_ms = EXCLUDED.rebalance_timeout_ms,
            metadata = EXCLUDED.metadata,
            protocols = EXCLUDED.protocols,
            joined_generation = EXCLUDED.joined_generation,
            last_heartbeat = now()",
        &[
            group_id.into(),
            member_id.into(),
            group_instance_id.into(),
            client_id.into(),
            client_host.into(),
            clamp_session_timeout(session_timeout_ms).into(),
            clamp_rebalance_timeout(rebalance_timeout_ms).into(),
            metadata.to_vec().into(),
            protocols.to_vec().into(),
            (g.generation + 1).into(),
        ],
    )?;

    if g.state != GroupState::PreparingRebalance {
        Spi::run_with_args(
            "UPDATE kafgres_groups
                SET state = 'PreparingRebalance',
                    protocol_type = COALESCE(NULLIF($2, ''), protocol_type),
                    rebalance_deadline = now() + ($3::int || ' milliseconds')::interval,
                    updated_at = now()
              WHERE group_id = $1",
            &[
                group_id.into(),
                protocol_type.into(),
                clamp_rebalance_timeout(rebalance_timeout_ms).into(),
            ],
        )?;
        Spi::run_with_args(
            "UPDATE kafgres_group_members SET assignment = NULL WHERE group_id = $1",
            &[group_id.into()],
        )?;
    }
    Ok(())
}

pub fn join_window_closed(group_id: &str) -> Result<bool, spi::Error> {
    let ready: bool = Spi::get_one_with_args(
        "SELECT COALESCE(
                  (SELECT count(*) = 0
                     FROM kafgres_group_members m
                     JOIN kafgres_groups g USING (group_id)
                    WHERE m.group_id = $1
                      AND m.joined_generation <= g.generation),
                  true)
             OR COALESCE((SELECT rebalance_deadline < now() FROM kafgres_groups WHERE group_id = $1), false)",
        &[group_id.into()],
    )?
    .unwrap_or(false);
    Ok(ready)
}

pub fn complete_join(group_id: &str) -> Result<Result<Group, NoCommonProtocol>, spi::Error> {
    Spi::run_with_args(
        "DELETE FROM kafgres_group_members m
          USING kafgres_groups g
          WHERE m.group_id = $1 AND g.group_id = m.group_id
            AND m.joined_generation <= g.generation",
        &[group_id.into()],
    )?;

    let remaining = members(group_id)?;
    if remaining.is_empty() {
        Spi::run_with_args(
            "UPDATE kafgres_groups
                SET state = 'Empty', leader_member = NULL, protocol_name = NULL,
                    generation = generation + 1, rebalance_deadline = NULL, updated_at = now()
              WHERE group_id = $1",
            &[group_id.into()],
        )?;
        return Ok(Ok(load(group_id)?.expect("group exists")));
    }

    // Completing with no common protocol hands the leader "" and crashes clients out of poll(), logged nowhere.
    let protocol = remaining
        .iter()
        .map(|m| m.protocols.clone())
        .reduce(|acc, p| acc.into_iter().filter(|x| p.contains(x)).collect())
        .and_then(|common| common.into_iter().next());
    if protocol.is_none() {
        return Ok(Err(NoCommonProtocol));
    }

    let leader = remaining[0].member_id.clone();

    Spi::run_with_args(
        "UPDATE kafgres_groups
            SET generation = generation + 1,
                state = 'CompletingRebalance',
                protocol_name = $2,
                leader_member = $3,
                rebalance_deadline = NULL,
                updated_at = now()
          WHERE group_id = $1",
        &[group_id.into(), protocol.into(), leader.into()],
    )?;
    Ok(Ok(load(group_id)?.expect("group exists")))
}

pub fn apply_assignments(
    group_id: &str,
    assignments: &[(String, Vec<u8>)],
) -> Result<(), spi::Error> {
    for (member_id, bytes) in assignments {
        Spi::run_with_args(
            "UPDATE kafgres_group_members SET assignment = $3
              WHERE group_id = $1 AND member_id = $2",
            &[group_id.into(), member_id.as_str().into(), bytes.clone().into()],
        )?;
    }
    // Empty, not null: null means "not yet delivered" and parks the member's SyncGroup until the deadline.
    Spi::run_with_args(
        "UPDATE kafgres_group_members SET assignment = ''::bytea
          WHERE group_id = $1 AND assignment IS NULL",
        &[group_id.into()],
    )?;
    Spi::run_with_args(
        "UPDATE kafgres_groups SET state = 'Stable', updated_at = now() WHERE group_id = $1",
        &[group_id.into()],
    )?;
    Ok(())
}

pub fn touch_heartbeat(group_id: &str, member_id: &str) -> Result<(), spi::Error> {
    Spi::run_with_args(
        "UPDATE kafgres_group_members SET last_heartbeat = now()
          WHERE group_id = $1 AND member_id = $2",
        &[group_id.into(), member_id.into()],
    )
}

pub fn leave(group_id: &str, member_id: &str) -> Result<bool, spi::Error> {
    let removed: i64 = Spi::get_one_with_args(
        "WITH d AS (DELETE FROM kafgres_group_members
                     WHERE group_id = $1 AND member_id = $2 RETURNING 1)
         SELECT count(*) FROM d",
        &[group_id.into(), member_id.into()],
    )?
    .unwrap_or(0);
    if removed > 0 {
        open_rebalance(group_id)?;
    }
    Ok(removed > 0)
}

pub fn open_rebalance(group_id: &str) -> Result<(), spi::Error> {
    // A rebalance is not pushed; members learn of one only at their next heartbeat, so
    Spi::run_with_args(
        "UPDATE kafgres_groups g
            SET state = CASE WHEN EXISTS (SELECT 1 FROM kafgres_group_members WHERE group_id = $1)
                             THEN 'PreparingRebalance' ELSE 'Empty' END,
                rebalance_deadline = now() + (
                    COALESCE((SELECT max(rebalance_timeout_ms) FROM kafgres_group_members
                               WHERE group_id = $1), 60000) || ' milliseconds')::interval,
                updated_at = now()
          WHERE g.group_id = $1",
        &[group_id.into()],
    )?;
    Spi::run_with_args(
        "UPDATE kafgres_group_members SET assignment = NULL WHERE group_id = $1",
        &[group_id.into()],
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
    NotEmpty,
}

impl DeleteOutcome {
    pub fn error_code(self) -> kafgres_codec::ErrorCode {
        use kafgres_codec::ErrorCode as E;
        match self {
            DeleteOutcome::Deleted => E::None,
            DeleteOutcome::NotFound => E::GroupIdNotFound,
            DeleteOutcome::NotEmpty => E::NonEmptyGroup,
        }
    }
}

pub fn delete(group_id: &str) -> Result<DeleteOutcome, spi::Error> {
    let (exists, _) = existence(group_id)?;
    if !exists {
        return Ok(DeleteOutcome::NotFound);
    }

    let members = Spi::get_one_with_args::<i64>(
        "SELECT (SELECT count(*) FROM kafgres_group_members WHERE group_id = $1)",
        &[group_id.into()],
    )?
    .unwrap_or(0);
    if members > 0 {
        return Ok(DeleteOutcome::NotEmpty);
    }

    // Offsets first: the other order leaves them orphaned if this fails partway, for a recreated group to inherit.
    Spi::run_with_args(
        "DELETE FROM kafgres_offsets WHERE group_id = $1",
        &[group_id.into()],
    )?;
    Spi::run_with_args(
        "DELETE FROM kafgres_groups WHERE group_id = $1",
        &[group_id.into()],
    )?;
    Ok(DeleteOutcome::Deleted)
}

pub fn existence(group_id: &str) -> Result<(bool, bool), spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            // One row of scalars, not `FROM kafgres_groups`: SPI raises on an empty result set
            "SELECT EXISTS(SELECT 1 FROM kafgres_groups WHERE group_id = $1)
                 OR EXISTS(SELECT 1 FROM kafgres_offsets WHERE group_id = $1),
                    EXISTS(SELECT 1 FROM kafgres_group_members WHERE group_id = $1)",
            Some(1),
            &[group_id.into()],
        )?;
        for r in rows {
            return Ok((
                r.get::<bool>(1)?.unwrap_or(false),
                r.get::<bool>(2)?.unwrap_or(false),
            ));
        }
        Ok((false, false))
    })
}

pub fn delete_offset(group_id: &str, topic_id: u32, partition: i32) -> Result<bool, spi::Error> {
    let n: Option<i64> = Spi::get_one_with_args(
        "WITH gone AS (
             DELETE FROM kafgres_offsets
              WHERE group_id = $1 AND topic_id = $2::oid AND partition = $3
          RETURNING 1
         ) SELECT count(*) FROM gone",
        &[group_id.into(), (topic_id as i32).into(), partition.into()],
    )?;
    Ok(n.unwrap_or(0) > 0)
}

/// Must run from the background worker: a dead member sends nothing, so no request path ever notices it.
pub fn sweep() -> Result<Vec<String>, spi::Error> {
    let mut changed = Vec::new();
    Spi::connect(|client| {
        let rows = client.select(
            "WITH dead AS (
                 DELETE FROM kafgres_group_members
                  WHERE last_heartbeat < now() - (session_timeout_ms || ' milliseconds')::interval
                RETURNING group_id, member_id)
             SELECT DISTINCT group_id FROM dead",
            None,
            &[],
        )?;
        for row in rows {
            if let Some(g) = row.get::<String>(1)? {
                changed.push(g);
            }
        }
        Ok::<_, spi::Error>(())
    })?;

    for g in &changed {
        pgrx::log!("kafgres: group '{g}' lost a member to session timeout; rebalancing");
        open_rebalance(g)?;
    }
    Ok(changed)
}

pub fn groups_past_deadline() -> Result<Vec<String>, spi::Error> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT group_id FROM kafgres_groups
              WHERE state = 'PreparingRebalance' AND rebalance_deadline < now()",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(g) = row.get::<String>(1)? {
                out.push(g);
            }
        }
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeouts_are_clamped() {
        assert_eq!(clamp_session_timeout(i32::MAX), MAX_SESSION_TIMEOUT_MS);
        assert_eq!(clamp_session_timeout(0), MIN_SESSION_TIMEOUT_MS);
        assert_eq!(clamp_session_timeout(45_000), 45_000);
        assert_eq!(clamp_rebalance_timeout(0), 60_000);
        assert_eq!(clamp_rebalance_timeout(i32::MAX), MAX_REBALANCE_TIMEOUT_MS);
    }

    #[test]
    fn state_names_round_trip() {
        for s in [
            GroupState::Empty,
            GroupState::PreparingRebalance,
            GroupState::CompletingRebalance,
            GroupState::Stable,
        ] {
            assert_eq!(GroupState::parse(s.as_str()), s);
        }
        assert_eq!(GroupState::parse("Dead"), GroupState::Empty);
    }
}
