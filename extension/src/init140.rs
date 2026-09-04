//! Share-group state stores only the exceptions to "everything at or above a share-partition's

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_140() {
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_share_groups (
            group_id    text PRIMARY KEY,
            group_epoch int  NOT NULL DEFAULT 0,
            state       text NOT NULL DEFAULT 'Empty',
            updated_at  timestamptz NOT NULL DEFAULT now()
         )",
        "create share groups table",
    );

    // No reconciliation: every member may read every partition it subscribes to, so there is
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_share_members (
            group_id     text NOT NULL REFERENCES kafgres_share_groups(group_id) ON DELETE CASCADE,
            member_id    text NOT NULL,
            member_epoch int  NOT NULL DEFAULT 0,
            rack_id      text,
            subscribed   text[] NOT NULL DEFAULT '{}',
            last_seen    timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (group_id, member_id)
         )",
        "create share members table",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_share_members_last_seen
             ON kafgres_share_members (last_seen)",
        "index share members by last seen",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_share_offsets (
            group_id     text NOT NULL,
            topic_id     oid  NOT NULL,
            partition    int  NOT NULL,
            start_offset bigint NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, topic_id, partition)
         )",
        "create share offsets table",
    );

    // `acquired_until` is what makes a queue survive a consumer that dies holding work;
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_share_inflight (
            group_id       text NOT NULL,
            topic_id       oid  NOT NULL,
            partition      int  NOT NULL,
            record_offset  bigint NOT NULL,
            state          text NOT NULL
                           CHECK (state IN ('acquired', 'acked', 'archived')),
            delivery_count int  NOT NULL DEFAULT 0,
            member_id      text,
            acquired_until timestamptz,
            PRIMARY KEY (group_id, topic_id, partition, record_offset)
         )",
        "create share inflight table",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_share_inflight_expiry
             ON kafgres_share_inflight (acquired_until)
             WHERE state = 'acquired'",
        "index share acquisitions by lock expiry",
    );
}
