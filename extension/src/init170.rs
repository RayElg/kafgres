//! Leader epochs carry the timeline in their high half, but pg_upgrade and pg_resetwal
//! start a cluster over at timeline 1. `bias`, added to it, keeps the epochs' timeline
//! from going backwards.

use crate::ddl::run_ddl;

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
