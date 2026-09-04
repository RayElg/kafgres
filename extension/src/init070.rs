//! `OffsetForLeaderEpoch`'s answer has to survive retention: the consumer asking is usually one

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_070() {
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_leader_epochs (
            topic_id     oid    NOT NULL,
            partition    int    NOT NULL,
            leader_epoch int    NOT NULL,
            /* First offset written under this epoch. Kafka's `LeaderEpochFileCache`
               entry, and the only durable record of it. */
            start_offset bigint NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (topic_id, partition, leader_epoch)
        )",
        "create leader epoch history",
    );

    // kafgres_partitions knows the *current* epoch's start offset exactly, so insert it first
    run_ddl(
        "INSERT INTO kafgres_leader_epochs (topic_id, partition, leader_epoch, start_offset)
         SELECT topic_id, partition, leader_epoch, epoch_start_offset
           FROM kafgres_partitions
         ON CONFLICT DO NOTHING",
        "backfill the current leader epoch",
    );

    run_ddl(
        "INSERT INTO kafgres_leader_epochs (topic_id, partition, leader_epoch, start_offset)
         SELECT topic_id, partition, leader_epoch, MIN(base_offset)
           FROM kafgres_log
          GROUP BY topic_id, partition, leader_epoch
         ON CONFLICT DO NOTHING",
        "backfill leader epoch history from the log",
    );

    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.7.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );
}
