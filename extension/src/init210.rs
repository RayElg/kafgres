//! One value per server start, folded into `kafgres_produce()` producer ids, and each
//! partition's window checkpoint.

use crate::ddl::run_ddl;

pub fn init_210() {
    run_ddl(
        "CREATE SEQUENCE IF NOT EXISTS kafgres_boot_seq AS bigint START 1",
        "create the boot sequence",
    );
    // The offset below which every idempotent batch's window row had committed. NULL on a
    // partition created before this column: nothing is known about it yet.
    run_ddl(
        "ALTER TABLE kafgres_partitions ADD COLUMN IF NOT EXISTS window_checkpoint bigint",
        "add the window checkpoint column",
    );
}
