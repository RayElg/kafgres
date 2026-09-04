//! Kafka EOS (wire-protocol) transaction state in Postgres — not the same thing as `kafgres_produce()`'s transactionality.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_090() {
    // Keyed on producer_id, not transactional_id: fencing bumps the epoch at InitProducerId,
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_txns (
            producer_id     bigint  NOT NULL,
            producer_epoch  int     NOT NULL,
            transactional_id text   NOT NULL,
            state           text    NOT NULL DEFAULT 'ongoing',
            started_at      bigint  NOT NULL,
            PRIMARY KEY (producer_id)
         )",
        "create txns table",
    );

    // Set by an operator's `WriteTxnMarkers`; recorded, not fenced on first mark, so the remaining partitions can still be ended.
    run_ddl(
        "ALTER TABLE kafgres_txns
            ADD COLUMN IF NOT EXISTS forced_result boolean",
        "add txn forced_result column",
    );

    run_ddl(
        "ALTER TABLE kafgres_txns
            ADD COLUMN IF NOT EXISTS timeout_ms int NOT NULL DEFAULT 60000",
        "add transaction timeout",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_txn_partitions (
            producer_id bigint NOT NULL,
            topic_id    oid    NOT NULL,
            partition   int    NOT NULL,
            PRIMARY KEY (producer_id, topic_id, partition)
         )",
        "create txn partitions table",
    );

    // The open transaction's first offset — what makes the LSO answerable. Derived from the
    run_ddl(
        "ALTER TABLE kafgres_txn_partitions
            ADD COLUMN IF NOT EXISTS first_offset bigint NOT NULL DEFAULT -1",
        "add txn partition first offset",
    );

    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_txn_partitions_by_partition
             ON kafgres_txn_partitions (topic_id, partition)",
        "index txn partitions by partition",
    );

    // Aborted transactions by offset range — Kafka's `.txnindex`. Cannot be rebuilt from the
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_txn_aborted (
            topic_id     oid    NOT NULL,
            partition    int    NOT NULL,
            producer_id  bigint NOT NULL,
            first_offset bigint NOT NULL,
            last_offset  bigint NOT NULL,
            PRIMARY KEY (topic_id, partition, first_offset)
         )",
        "create aborted transaction index",
    );

    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_txn_aborted_range
             ON kafgres_txn_aborted (topic_id, partition, last_offset)",
        "index aborted transactions by range",
    );

    // Offsets the transaction consumed, held until it commits — the read-process-write half of `exactly_once_v2`.
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_txn_offsets (
            producer_id   bigint NOT NULL,
            group_id      text   NOT NULL,
            topic_id      oid    NOT NULL,
            partition     int    NOT NULL,
            committed_offset bigint NOT NULL,
            committed_leader_epoch int NOT NULL DEFAULT -1,
            metadata      text,
            PRIMARY KEY (producer_id, group_id, topic_id, partition)
         )",
        "create txn offsets table",
    );
}
