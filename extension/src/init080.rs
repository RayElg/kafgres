//! Commit markers are what the caller's transaction actually governs: the segment payload is

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_080() {
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_markers (
            topic_id    oid    NOT NULL,
            partition   int    NOT NULL,
            base_offset bigint NOT NULL,
            last_offset bigint NOT NULL,
            bytes       int    NOT NULL,
            PRIMARY KEY (topic_id, partition, base_offset)
         )",
        "create markers table",
    );

    // Retention and the LSO both ask for the lowest marker at or above X on a partition — a
}
