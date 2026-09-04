//! `max_last_offset` is not an optimization: a batch straddling a range-partition boundary lands

use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_050() {
    run_ddl(
        "ALTER TABLE kafgres_log_segments
           ADD COLUMN IF NOT EXISTS max_last_offset bigint NOT NULL DEFAULT -1",
        "add segment max_last_offset",
    );
    run_ddl(
        "ALTER TABLE kafgres_log_segments
           ADD COLUMN IF NOT EXISTS max_append_ts bigint NOT NULL DEFAULT 0",
        "add segment max_append_ts",
    );
    run_ddl(
        "ALTER TABLE kafgres_log_segments
           ADD COLUMN IF NOT EXISTS bytes bigint NOT NULL DEFAULT 0",
        "add segment bytes",
    );

    // A segment left at the -1 default would look empty to retention and be dropped with its
    run_ddl(
        "UPDATE kafgres_log_segments s
            SET max_last_offset = agg.max_last,
                max_append_ts   = agg.max_ts,
                bytes           = agg.total
           FROM (SELECT l.topic_id, l.partition, sg.base_offset,
                        MAX(l.last_offset) AS max_last,
                        MAX(l.append_ts)   AS max_ts,
                        SUM(octet_length(l.batch))::bigint AS total
                   FROM kafgres_log_segments sg
                   JOIN kafgres_log l
                     ON l.topic_id = sg.topic_id AND l.partition = sg.partition
                    AND l.base_offset >= sg.base_offset AND l.base_offset < sg.end_offset
                  GROUP BY l.topic_id, l.partition, sg.base_offset) agg
          WHERE s.topic_id = agg.topic_id AND s.partition = agg.partition
            AND s.base_offset = agg.base_offset
            AND s.max_last_offset = -1",
        "backfill segment retention metadata",
    );

    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.5.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );
}
