use pgrx::spi::Spi;

/// Abort init on failure: a half-created schema a worker can then read is worse than refusing to start.
fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_010() {
    // `config` holds topic configs verbatim so CreateTopics/DescribeConfigs round-trip unknown keys.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_topics (
            topic_id       oid    PRIMARY KEY,
            name           text   NOT NULL UNIQUE,
            num_partitions int    NOT NULL,
            config         jsonb  NOT NULL DEFAULT '{}'::jsonb,
            created_at     timestamptz NOT NULL DEFAULT now()
        )",
        "create topics table",
    );

    // next_offset is the offset assignment point: a row lock here per append is what keeps
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_partitions (
            topic_id         oid    NOT NULL REFERENCES kafgres_topics(topic_id) ON DELETE CASCADE,
            partition        int    NOT NULL,
            next_offset      bigint NOT NULL DEFAULT 0,
            log_start_offset bigint NOT NULL DEFAULT 0,
            leader_epoch     int    NOT NULL DEFAULT 0,
            epoch_start_offset bigint NOT NULL DEFAULT 0,
            PRIMARY KEY (topic_id, partition)
        )",
        "create partitions table",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_schema_version (
            version    text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )",
        "create schema version table",
    );
    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.1.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );
}
