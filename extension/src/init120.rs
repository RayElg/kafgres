//! Separate tables from the classic protocol: KIP-848 makes the *broker* the assignor, and a

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_120() {
    // The clock the whole protocol runs on: a member is caught up when its member epoch equals
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_consumer_groups (
            group_id         text PRIMARY KEY,
            group_epoch      int  NOT NULL DEFAULT 0,
            /* The epoch the current target assignment was computed for. Lags group_epoch
               only inside a single heartbeat, but storing it is what lets DescribeGroups
               report the two separately as Kafka does. */
            assignment_epoch int  NOT NULL DEFAULT 0,
            assignor         text NOT NULL DEFAULT 'uniform',
            state            text NOT NULL DEFAULT 'Empty',
            updated_at       timestamptz NOT NULL DEFAULT now()
         )",
        "create consumer groups table",
    );

    // A partition may not be given to its new owner until the old owner has confirmed it let
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_consumer_group_members (
            group_id         text NOT NULL REFERENCES kafgres_consumer_groups(group_id) ON DELETE CASCADE,
            member_id        text NOT NULL,
            member_epoch     int  NOT NULL DEFAULT 0,
            instance_id      text,
            rack_id          text,
            client_id        text NOT NULL DEFAULT '',
            client_host      text NOT NULL DEFAULT '',
            rebalance_timeout_ms int NOT NULL DEFAULT 300000,
            /* Topic names the member asked for. Regex subscriptions are a v1 field and are
               resolved to names when the heartbeat arrives, so this column is always the
               resolved set. */
            subscribed       text[] NOT NULL DEFAULT '{}',
            /* `topic_id:partition` pairs, as text, so a topic dropped and recreated under a
               new oid cannot have a stale assignment inherited by its replacement. */
            owned            text[] NOT NULL DEFAULT '{}',
            /* What the broker last *told* this member it may hold. Distinct from both of
               its neighbours, and the column it is easiest to leave out: `target` is where
               the assignor wants a partition to end up, `owned` is what the member has
               confirmed, and `granted` is the gap between them. Without it the broker
               believes nobody holds a partition between granting it and the grantee's next
               heartbeat — a window as long as the client's own `onPartitionsAssigned` — and
               hands the same partition to a second member inside it. */
            granted          text[] NOT NULL DEFAULT '{}',
            target           text[] NOT NULL DEFAULT '{}',
            /* Set when this member is still holding something it has been told to release,
               cleared when it lets go. A member that never lets go blocks the partition's
               new owner forever while looking perfectly healthy, so past its own
               `rebalance_timeout_ms` it is fenced. */
            revoking_since   timestamptz,
            /* The member left with epoch -2: a static member expected back. Its assignment
               stays reserved and the group epoch does not move, which is the entire point
               of static membership — a rolling restart costs zero rebalances rather than
               two per pod. The session timeout still reclaims it if it never returns. */
            static_departed  boolean NOT NULL DEFAULT false,
            last_seen        timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (group_id, member_id)
         )",
        "create consumer group members table",
    );

    // Partial: most members are not static, and indexing their NULLs would be most of the
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_cg_members_instance
             ON kafgres_consumer_group_members (group_id, instance_id)
             WHERE instance_id IS NOT NULL",
        "index consumer group members by instance id",
    );

    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_cg_members_last_seen
             ON kafgres_consumer_group_members (last_seen)",
        "index consumer group members by last seen",
    );
}
