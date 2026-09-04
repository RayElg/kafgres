use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_030() {
    // `state` uses Kafka's own names so log lines and `--describe` output line up with what an
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_groups (
            group_id           text PRIMARY KEY,
            generation         int  NOT NULL DEFAULT 0,
            state              text NOT NULL DEFAULT 'Empty',
            protocol_type      text,
            protocol_name      text,
            leader_member      text,
            /* When the join window closes and the generation is cut, even if some
               member never rejoined. Kafka's rebalance timeout. */
            rebalance_deadline timestamptz,
            updated_at         timestamptz NOT NULL DEFAULT now()
        )",
        "create groups table",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_group_members (
            group_id            text NOT NULL REFERENCES kafgres_groups(group_id) ON DELETE CASCADE,
            member_id           text NOT NULL,
            group_instance_id   text,
            client_id           text NOT NULL DEFAULT '',
            client_host         text NOT NULL DEFAULT '',
            session_timeout_ms  int  NOT NULL DEFAULT 45000,
            rebalance_timeout_ms int NOT NULL DEFAULT 300000,
            /* The protocol metadata the member offered, opaque to us. */
            metadata            bytea NOT NULL DEFAULT ''::bytea,
            /* Protocol names the member supports, so the group can pick a common one. */
            protocols           text[] NOT NULL DEFAULT '{}',
            /* The leader's assignment for this member. NULL until SyncGroup. */
            assignment          bytea,
            /* Generation this member has rejoined for; the join window closes when every
               member has caught up to the group's pending generation. */
            joined_generation   int NOT NULL DEFAULT -1,
            last_heartbeat      timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (group_id, member_id)
        )",
        "create group members table",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_offsets (
            group_id              text   NOT NULL,
            topic_id              oid    NOT NULL,
            partition             int    NOT NULL,
            committed_offset      bigint NOT NULL,
            committed_leader_epoch int   NOT NULL DEFAULT -1,
            metadata              text,
            commit_ts             timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (group_id, topic_id, partition)
        )",
        "create offsets table",
    );

    // The expiry sweep scans by heartbeat age on every tick; without this it is a sequential
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_group_members_heartbeat_idx
           ON kafgres_group_members (last_heartbeat)",
        "index member heartbeats",
    );

    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.3.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );
}
