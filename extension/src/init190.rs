//! Each CDC mapping's delivered position: the last change it rendered, as its transaction's
//! commit end LSN and its place in that transaction. A replay held for another mapping
//! skips what this one already produced.

use crate::ddl::run_ddl;

pub fn init_190() {
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings
            ADD COLUMN IF NOT EXISTS delivered_lsn pg_lsn,
            ADD COLUMN IF NOT EXISTS delivered_seq bigint",
        "add the cdc delivered position columns",
    );
}
