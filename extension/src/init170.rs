//! Leader epochs carry the timeline in their high half, but pg_upgrade and pg_resetwal
//! start a cluster over at timeline 1. `bias`, added to it, keeps the epochs' timeline
//! from going backwards.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_170() {
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_timeline (
            only_row      boolean PRIMARY KEY DEFAULT true CHECK (only_row),
            last_timeline int NOT NULL,
            bias          int NOT NULL
        )",
        "create the timeline table",
    );
}
