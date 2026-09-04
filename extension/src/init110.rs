//! Retention may not reclaim a segment until a row here records it as archived, or the archive

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_110() {
    // Rows outlive the segment: a restore needs the list of what the archive holds after the
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_segment_archive (
            topic_id    oid    NOT NULL,
            partition   int    NOT NULL,
            base_offset bigint NOT NULL,
            bytes       bigint NOT NULL,
            archived_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (topic_id, partition, base_offset)
         )",
        "create segment archive table",
    );
}
