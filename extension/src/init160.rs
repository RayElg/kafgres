//! A group forming from Empty cuts its generation around the first arrival; peers arrive
//! into an already-cut generation and pay a second round. `join_window_until` holds the
//! window open so simultaneous arrivals land in one generation, capped by the rebalance deadline.

use crate::ddl::run_ddl;

pub fn init_160() {
    run_ddl(
        "ALTER TABLE kafgres_groups
            ADD COLUMN IF NOT EXISTS join_window_until timestamptz",
        "add the initial-rebalance join window column",
    );
}
