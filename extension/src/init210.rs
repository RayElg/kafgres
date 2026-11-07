//! One value per server start, folded into `kafgres_produce()` producer ids.

use crate::ddl::run_ddl;

pub fn init_210() {
    run_ddl(
        "CREATE SEQUENCE IF NOT EXISTS kafgres_boot_seq AS bigint START 1",
        "create the boot sequence",
    );
}
