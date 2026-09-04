//! `LIST (topic_id)` over `RANGE (partition, base_offset)`: retention reclaims a whole segment

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_020() {
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_log (
            topic_id       oid    NOT NULL,
            partition      int    NOT NULL,
            base_offset    bigint NOT NULL,
            last_offset    bigint NOT NULL,
            batch          bytea  NOT NULL,
            append_ts      bigint NOT NULL,
            max_timestamp  bigint NOT NULL,
            record_count   int    NOT NULL,
            producer_id    bigint,
            producer_epoch smallint,
            base_seq       int,
            leader_epoch   int    NOT NULL,
            is_txn         bool   NOT NULL DEFAULT false,
            is_control     bool   NOT NULL DEFAULT false,
            PRIMARY KEY (topic_id, partition, base_offset)
        ) PARTITION BY LIST (topic_id)",
        "create log table",
    );

    // Producer batches arrive already compressed; EXTENDED storage makes TOAST try pglz on
    run_ddl(
        "ALTER TABLE kafgres_log ALTER COLUMN batch SET STORAGE EXTERNAL",
        "set batch storage external",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_log_segments (
            topic_id     oid    NOT NULL,
            partition    int    NOT NULL,
            base_offset  bigint NOT NULL,
            end_offset   bigint NOT NULL,
            table_name   text   NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (topic_id, partition, base_offset)
        )",
        "create log segment index",
    );

    // Fetch v13+/Metadata v10+ address topics by id. Stored, not derived from the oid, so it
    run_ddl(
        "ALTER TABLE kafgres_topics
           ADD COLUMN IF NOT EXISTS topic_uuid bytea",
        "add topic uuid column",
    );
    run_ddl(
        "UPDATE kafgres_topics
            SET topic_uuid = decode(replace(gen_random_uuid()::text, '-', ''), 'hex')
          WHERE topic_uuid IS NULL",
        "backfill topic uuids",
    );
    run_ddl(
        "CREATE UNIQUE INDEX IF NOT EXISTS kafgres_topics_uuid_idx
           ON kafgres_topics (topic_uuid)",
        "index topic uuids",
    );

    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.2.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );
}
