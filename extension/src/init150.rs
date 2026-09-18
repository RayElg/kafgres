//! EndTxn v5 rotates the producer epoch as part of ending a transaction; the coordinator
//! remembers the retired (producer_id, epoch) and outcome and answers a matching retry with
//! NONE. The columns live on `kafgres_producers` because an overflowed producer id moves rows,
//! and `retired_producer_id` is what still finds the row after such a move.

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
    run_ddl(
        "ALTER TABLE kafgres_producers
            ADD COLUMN IF NOT EXISTS retired_producer_id bigint",
        "add the retired-producer-id column for KIP-890 EndTxn retries",
    );
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_producers_retired_idx
            ON kafgres_producers (retired_producer_id, retired_epoch)
         WHERE retired_producer_id IS NOT NULL",
        "index producers by retired pair",
    );
}
