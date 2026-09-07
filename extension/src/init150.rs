//! EndTxn v5 rotates the producer epoch as part of ending a transaction, so a retry of a
//! lost response would otherwise fence a producer whose transaction committed. The
//! coordinator remembers the retired (producer_id, epoch) and outcome, and answers a
//! retry carrying that pair, on an already-completed transaction, with NONE.
//!
//! The columns live on `kafgres_producers` because rotation is a producer property: an
//! epoch-overflowed producer id moves to a new row while its transaction row does not.

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_150() {
    run_ddl(
        "ALTER TABLE kafgres_producers
            ADD COLUMN IF NOT EXISTS retired_epoch smallint",
        "add the retired-epoch column for KIP-890 EndTxn retries",
    );
    run_ddl(
        "ALTER TABLE kafgres_producers
            ADD COLUMN IF NOT EXISTS retired_committed boolean",
        "add the retired-outcome column for KIP-890 EndTxn retries",
    );
}
