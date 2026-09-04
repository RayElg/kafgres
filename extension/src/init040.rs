use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_040() {
    // Unlike offsets, a producer id needs only uniqueness; a gap after a rollback costs nothing.
    run_ddl(
        "CREATE SEQUENCE IF NOT EXISTS kafgres_producer_id_seq AS bigint START 1",
        "create producer id sequence",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_producers (
            producer_id     bigint   PRIMARY KEY,
            producer_epoch  smallint NOT NULL DEFAULT 0,
            /* NULL for a plain idempotent producer. Set only by a transactional one,
               which is phase 9 — but the column exists now so the id allocation path
               does not change shape later. */
            transactional_id text UNIQUE,
            last_ts         timestamptz NOT NULL DEFAULT now()
        )",
        "create producers table",
    );

    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_producers_last_ts_idx
           ON kafgres_producers (last_ts)",
        "index producers by last use",
    );

    // Retained batches must be read in insertion order. Sequences restart at 0 on an epoch bump
    run_ddl(
        "CREATE SEQUENCE IF NOT EXISTS kafgres_producer_batch_seq AS bigint START 1",
        "create producer batch ordering sequence",
    );

    // One row per retained batch: a duplicate has to be answered with the *original* base
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_producer_batches (
            producer_id    bigint   NOT NULL,
            topic_id       oid      NOT NULL,
            partition      int      NOT NULL,
            producer_epoch smallint NOT NULL,
            first_seq      int      NOT NULL,
            last_seq       int      NOT NULL,
            base_offset    bigint   NOT NULL,
            added_seq      bigint   NOT NULL DEFAULT nextval('kafgres_producer_batch_seq'),
            appended_at    timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (producer_id, topic_id, partition, first_seq)
        )",
        "create producer batch window",
    );
    run_ddl(
        "ALTER TABLE kafgres_producer_batches
           ADD COLUMN IF NOT EXISTS added_seq bigint NOT NULL
               DEFAULT nextval('kafgres_producer_batch_seq')",
        "add producer batch ordering column",
    );

    // The primary key orders on first_seq, which is not insertion order.
    run_ddl(
        "DROP INDEX IF EXISTS kafgres_producer_batches_seq_idx",
        "drop superseded producer batch index",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_producer_batches_added_idx
           ON kafgres_producer_batches (producer_id, topic_id, partition, added_seq)",
        "index producer batch window",
    );

    // Topic ids must never be reused: producer windows and committed offsets are keyed by
    run_ddl(
        "CREATE SEQUENCE IF NOT EXISTS kafgres_topic_id_seq AS bigint START 1",
        "create topic id sequence",
    );
    run_ddl(
        "SELECT setval('kafgres_topic_id_seq',
                       GREATEST((SELECT COALESCE(MAX(topic_id::bigint), 0) FROM kafgres_topics),
                                (SELECT last_value FROM kafgres_topic_id_seq)))",
        "advance topic id sequence past existing topics",
    );

    run_ddl(
        "DELETE FROM kafgres_producer_batches b
          WHERE NOT EXISTS (SELECT 1 FROM kafgres_topics t WHERE t.topic_id = b.topic_id)",
        "clear orphaned producer windows",
    );
    run_ddl(
        "DELETE FROM kafgres_offsets o
          WHERE NOT EXISTS (SELECT 1 FROM kafgres_topics t WHERE t.topic_id = o.topic_id)",
        "clear orphaned committed offsets",
    );

    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.4.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );
}
