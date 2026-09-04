use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_100() {
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_cdc_mappings (
            mapping_name text NOT NULL,
            source_table text NOT NULL,
            topic        text NOT NULL,
            -- SQL expressions, not templates. `key_expr` yields text (or NULL for
            -- round-robin partitioning), `value_expr` yields jsonb or text, `filter_expr`
            -- yields boolean. All three see `new`, `old` and `op`.
            key_expr     text,
            value_expr   text NOT NULL,
            filter_expr  text,
            enabled      boolean NOT NULL DEFAULT true,
            -- What to do when rendering a change raises: 'skip' or 'stall'.
            --
            -- The expressions are compiled when the mapping is defined, so a failure here
            -- is data-dependent — a cast that fails on one row, a subquery that returns
            -- two. The choice is therefore between losing one event and stopping the
            -- pipeline, and stopping is not the safe option it looks like: a stalled slot
            -- pins WAL, and pinned WAL fills the disk and takes Postgres down with it.
            -- So the default skips, and the change is written to kafgres_cdc_errors
            -- rather than dropped, which is what keeps 'skip' from being silent.
            on_error     text NOT NULL DEFAULT 'skip'
                         CHECK (on_error IN ('skip', 'stall')),
            PRIMARY KEY (mapping_name)
         )",
        "create cdc mappings table",
    );
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings
            ADD COLUMN IF NOT EXISTS on_error text NOT NULL DEFAULT 'skip'",
        "add cdc on_error column",
    );

    // Keyset pagination on the last primary key emitted, so a snapshot resumes across worker
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings
            ADD COLUMN IF NOT EXISTS snapshot text NOT NULL DEFAULT 'none'
                CHECK (snapshot IN ('none', 'pending', 'running', 'done')),
            ADD COLUMN IF NOT EXISTS snapshot_cursor jsonb,
            ADD COLUMN IF NOT EXISTS snapshot_rows bigint NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS snapshot_started_at timestamptz,
            ADD COLUMN IF NOT EXISTS snapshot_finished_at timestamptz",
        "add cdc snapshot columns",
    );

    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_cdc_errors (
            id           bigserial PRIMARY KEY,
            mapping_name text        NOT NULL,
            lsn          pg_lsn      NOT NULL,
            change       jsonb       NOT NULL,
            error        text        NOT NULL,
            failed_at    timestamptz NOT NULL DEFAULT now()
         )",
        "create cdc errors table",
    );

    // Several mappings may fan one table out to several topics, so indexed rather than unique.
    run_ddl(
        "CREATE INDEX IF NOT EXISTS kafgres_cdc_mappings_source
             ON kafgres_cdc_mappings (source_table)",
        "index cdc mappings by source",
    );
}
