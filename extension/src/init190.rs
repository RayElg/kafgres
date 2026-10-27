//! The `__consumer_offsets` partitions a transaction has added (AddOffsetsToTxn, or
//! TxnOffsetCommit v5), by Kafka's group-to-partition hash; cleared when it ends.

use crate::ddl::run_ddl;

pub fn init_190() {
    run_ddl(
        "ALTER TABLE kafgres_txns
            ADD COLUMN IF NOT EXISTS offsets_partitions int[] NOT NULL DEFAULT '{}'",
        "add the transaction's offsets partitions",
    );
}
