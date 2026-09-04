//! CDC mappings: table → topic, defined in SQL. Enrichment subqueries run at render time

use pgrx::prelude::*;

/// One slot for every mapping: N slots would pin WAL for the slowest and decode changes N times.
pub const SLOT: &str = "kafgres_cdc";

const SLOT_CREATE_LOCK_KEY: i64 = 0x7047_4B41_0000_0002u64 as i64;

/// Net of batch framing and varints, so a record at the limit still forms a within-limit batch.
const MAX_RECORD_BYTES: usize =
    kafgres_codec::framing::DEFAULT_MAX_REQUEST_BYTES - kafgres_codec::records::RECORD_BATCH_OVERHEAD - 64;

/// The per-record cap does not bound a drain: every record renders before any single one looks large.
const MAX_DRAIN_BYTES: usize = 256 * 1024 * 1024;

pub const TRANSACTION_SOURCE: &str = "kafgres.transaction";

fn valid_source(source: &str) -> bool {
    !source.is_empty()
        && source.len() <= 128
        && source
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && !source.starts_with('.')
        && !source.ends_with('.')
        && source.matches('.').count() <= 1
}

#[pg_extern]
fn kafgres_add_mapping(
    mapping_name: &str,
    source_table: &str,
    topic: &str,
    value_expr: &str,
    key_expr: Option<&str>,
    filter_expr: Option<&str>,
) -> bool {
    if !valid_source(source_table) {
        error!("kafgres: {source_table:?} is not a plain schema-qualified table name");
    }
    if crate::meta::topic_id_by_name(topic).ok().flatten().is_none() {
        error!("kafgres: no such topic {topic:?}; create it before mapping to it");
    }

    render_check(source_table, value_expr, key_expr, filter_expr);

    Spi::run_with_args(
        "INSERT INTO kafgres_cdc_mappings
                (mapping_name, source_table, topic, key_expr, value_expr, filter_expr)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (mapping_name) DO UPDATE SET
            source_table = EXCLUDED.source_table,
            topic = EXCLUDED.topic,
            key_expr = EXCLUDED.key_expr,
            value_expr = EXCLUDED.value_expr,
            filter_expr = EXCLUDED.filter_expr",
        &[
            mapping_name.into(),
            source_table.into(),
            topic.into(),
            key_expr.into(),
            value_expr.into(),
            filter_expr.into(),
        ],
    )
    .unwrap_or_else(|e| error!("kafgres: {e}"));
    true
}

#[pg_extern]
fn kafgres_remove_mapping(mapping_name: &str) -> bool {
    Spi::run_with_args(
        "DELETE FROM kafgres_cdc_mappings WHERE mapping_name = $1",
        &[mapping_name.into()],
    )
    .unwrap_or_else(|e| error!("kafgres: {e}"));
    true
}

fn mapping_query(
    source: &str,
    value_expr: &str,
    key_expr: Option<&str>,
    filter_expr: Option<&str>,
    op: &str,
    predicate: &str,
) -> String {
    let key = key_expr.unwrap_or("NULL");
    let filter = filter_expr.unwrap_or("true");
    if source == TRANSACTION_SOURCE {
        return format!(
            "SELECT ({key})::text AS k, ({value_expr})::text AS v
               FROM (SELECT 1) AS _one,
                    LATERAL (SELECT 'T'::text) AS o(op),
                    LATERAL (SELECT '0/0'::pg_lsn) AS l(lsn),
                    LATERAL (SELECT NULL::bigint) AS x(xid),
                    LATERAL (SELECT NULL::timestamptz) AS c(commit_ts),
                    LATERAL (SELECT NULL::bigint) AS ec(event_count),
                    LATERAL (SELECT NULL::jsonb) AS dc(data_collections)
              WHERE ({filter}) AND ({predicate})"
        );
    }

    // Placeholders; the names must be in scope so the check matches what the renderer provides.
    format!(
        "SELECT ({key})::text AS k, ({value_expr})::text AS v
           FROM {source} AS new
           LEFT JOIN LATERAL (SELECT new.*) AS old ON true,
                LATERAL (SELECT '{op}'::text) AS o(op),
                LATERAL (SELECT '0/0'::pg_lsn) AS l(lsn),
                LATERAL (SELECT NULL::bigint) AS x(xid),
                LATERAL (SELECT NULL::timestamptz) AS c(commit_ts),
                LATERAL (SELECT NULL::bigint) AS ec(event_count),
                LATERAL (SELECT NULL::jsonb) AS dc(data_collections)
          WHERE ({filter}) AND ({predicate})"
    )
}

fn render_check(source: &str, value_expr: &str, key_expr: Option<&str>, filter_expr: Option<&str>) {
    let sql = mapping_query(source, value_expr, key_expr, filter_expr, "I", "false");
    Spi::connect(|client| client.select(&sql, Some(1), &[]).map(|_| ()))
        .unwrap_or_else(|e| error!("kafgres: {e}"));
}

#[pg_extern]
fn kafgres_preview_mapping(
    mapping_name: &str,
    predicate: &str,
) -> TableIterator<'static, (name!(key, Option<String>), name!(value, Option<String>))> {
    let row: Option<(String, String, Option<String>, Option<String>)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT source_table, value_expr, key_expr, filter_expr
               FROM kafgres_cdc_mappings WHERE mapping_name = $1",
            Some(1),
            &[mapping_name.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some((
                r.get::<String>(1)?.unwrap_or_default(),
                r.get::<String>(2)?.unwrap_or_default(),
                r.get::<String>(3)?,
                r.get::<String>(4)?,
            )));
        }
        Ok(None)
    })
    .unwrap_or_else(|e| error!("kafgres: {e}"));

    let Some((source, value_expr, key_expr, filter_expr)) = row else {
        error!("kafgres: no mapping named {mapping_name:?}");
    };

    let sql = mapping_query(
        &source,
        &value_expr,
        key_expr.as_deref(),
        filter_expr.as_deref(),
        "I",
        predicate,
    );
    let out: Vec<(Option<String>, Option<String>)> = Spi::connect(|client| {
        let rows = client.select(&sql, None, &[])?;
        let mut out = Vec::new();
        for r in rows {
            out.push((r.get::<String>(1)?, r.get::<String>(2)?));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| error!("kafgres: rendering {mapping_name:?}: {e}"));

    TableIterator::new(out)
}

/// Held in memory, not a temp table: a temp-table write is decoded as a transaction by the next peek.
struct TableChanges {
    schema: String,
    table: String,
    cols: String,
    changes: String,
}

fn render_query(
    columns: &[(String, String)],
    value_expr: &str,
    key_expr: Option<&str>,
    filter_expr: Option<&str>,
) -> String {
    let key = key_expr.unwrap_or("NULL");
    let filter = filter_expr.unwrap_or("true");

    // Project through the shape the change was captured under, not the table's current rowtype.
    let project = |src: &str| -> String {
        if columns.is_empty() {
            return "SELECT NULL::text AS __kafgres_no_columns".to_string();
        }
        let cols: Vec<String> = columns
            .iter()
            .map(|(name, ty)| format!("({src}->>{})::{ty} AS {}", sql_literal(name), quote_ident(name)))
            .collect();
        format!("SELECT {}", cols.join(", "))
    };

    format!(
        "SELECT ({key})::text AS k, ({value_expr})::text AS v
           FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS b(e, ord),
                -- `new` falls back to the before-image on DELETE, so the obvious mapping
                -- (`key_expr => new.id::text`) does not render a NULL key for every delete
                -- and send tombstones to partition 0. `op` distinguishes them.
                LATERAL (SELECT CASE WHEN b.e->'ch'->>'op' = 'D'
                                     THEN b.e->'ch'->'old' ELSE b.e->'ch'->'new' END) AS nj(j),
                LATERAL ({new_projection}) AS new,
                LATERAL (SELECT coalesce(nullif(b.e->'ch'->'old', 'null'::jsonb),
                                         '{{}}'::jsonb)) AS oj(j),
                LATERAL ({old_projection}) AS old,
                LATERAL (SELECT b.e->'ch'->>'op') AS o(op),
                -- The transaction's identity, which is what a Debezium-shaped envelope is
                -- mostly made of. `lsn` orders changes globally; `xid` groups the ones that
                -- shared a commit; `commit_ts` is when that commit happened.
                --
                -- All three are NULL on a snapshot row rather than faked, because a
                -- snapshot row belongs to no transaction and had no commit — `op` is `R`,
                -- and a mapping that wants a timestamp there should say `now()` itself
                -- rather than be handed one that looks like a commit time and is not.
                LATERAL (SELECT (b.e->>'lsn')::pg_lsn) AS l(lsn),
                LATERAL (SELECT (b.e->'ch'->>'xid')::bigint) AS x(xid),
                -- Postgres microseconds since 2000-01-01, which is what the plugin emits.
                -- NULL when the change carried no usable commit time (a snapshot row has
                -- none), rather than a bogus pre-epoch value.
                -- Transaction metadata: how many changes that commit carried, and how
                -- they were spread across tables. NULL on an ordinary change, which is a
                -- row *within* a transaction and cannot know either.
                LATERAL (SELECT (b.e->'ch'->>'events')::bigint) AS ec(event_count),
                LATERAL (SELECT b.e->'ch'->'tables') AS dc(data_collections),
                LATERAL (SELECT CASE WHEN (b.e->'ch'->>'ts')::bigint > 0
                                     THEN 'epoch'::timestamptz + '946684800 seconds'::interval
                                          + ((b.e->'ch'->>'ts')::bigint || ' microseconds')::interval
                                END) AS c(commit_ts)
          WHERE ({filter})
          ORDER BY b.ord",
        new_projection = project("nj.j"),
        old_projection = project("oj.j"),
    )
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn columns_of(cols_json: &str) -> Result<Vec<(String, String)>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT e->>0, e->>1 FROM jsonb_array_elements($1::jsonb) AS e",
            None,
            &[cols_json.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let (Some(name), Some(ty)) = (r.get::<String>(1)?, r.get::<String>(2)?) {
                out.push((name, ty));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())
}

/// Unlike pgrx's `Spi::get_one`, this assigns no xid: slot creation needs an unwritten transaction.
fn read_one<T: pgrx::datum::FromDatum + pgrx::datum::IntoDatum>(
    sql: &str,
    args: &[pgrx::datum::DatumWithOid<'_>],
) -> Result<Option<T>, String> {
    Spi::connect(|client| client.select(sql, Some(1), args)?.first().get_one::<T>())
        .map_err(|e| e.to_string())
}

fn slot_exists() -> bool {
    read_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        &[SLOT.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

/// Checked, not attempted: a failed creation leaves an undroppable half-built slot behind.
fn slot_preconditions() -> Result<(), String> {
    let wal_level = read_one::<String>("SELECT current_setting('wal_level')", &[])?
        .unwrap_or_default();
    if wal_level != "logical" {
        return Err(format!(
            "CDC needs wal_level = 'logical', not {wal_level:?}; it requires a restart"
        ));
    }

    if let Some(libs) = read_one::<String>(
        "SELECT current_setting('output_plugin_libraries', true)",
        &[],
    )? {
        if !libs.split(',').any(|l| l.trim() == "kafgres") {
            return Err(format!(
                "CDC needs 'kafgres' in output_plugin_libraries, which is {libs:?}"
            ));
        }
    }

    if read_one::<i64>("SELECT pg_current_xact_id_if_assigned()::text::bigint", &[])?.is_some() {
        return Err(
            "cannot create the CDC slot: this transaction has already been assigned an xid"
                .to_string(),
        );
    }
    Ok(())
}

fn enabled_mappings_exist() -> bool {
    read_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM kafgres_cdc_mappings WHERE enabled)",
        &[],
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

pub fn ensure_slot() -> Result<bool, String> {
    if slot_exists() {
        return Ok(true);
    }
    if !enabled_mappings_exist() {
        return Ok(false);
    }
    slot_preconditions()?;

    // Serialise, then re-check: the worker and the SQL entry point race. An advisory lock assigns no xid.
    Spi::connect(|client| {
        client
            .select(
                &format!("SELECT pg_advisory_xact_lock({SLOT_CREATE_LOCK_KEY})"),
                Some(1),
                &[],
            )
            .map(|_| ())
    })
    .map_err(|e| format!("could not lock for CDC slot creation: {e}"))?;
    if slot_exists() {
        return Ok(true);
    }

    // `client.select`, not `Spi::run`: the latter assigns the xid that disqualifies creation.
    Spi::connect(|client| {
        client
            .select(
                "SELECT pg_create_logical_replication_slot($1, 'kafgres')",
                Some(1),
                &[SLOT.into()],
            )
            .map(|_| ())
    })
    .map_err(|e| format!("could not create the CDC slot: {e}"))?;

    log!("kafgres: created logical replication slot {SLOT:?} for CDC");
    Ok(true)
}

/// Peek, not `get_changes`: advancing only after producing costs duplicates on a crash, not losses.
fn peek(max_changes: i32) -> Result<(Option<String>, Vec<TableChanges>), String> {
    Spi::connect(|client| {
        let rows = client.select(
            // Ordered by `WITH ORDINALITY`, not by LSN: one WAL record can carry several
            "WITH ch AS MATERIALIZED (
                SELECT s.lsn, s.ord, s.data::jsonb AS j
                  FROM pg_logical_slot_peek_changes($1, NULL, $2)
                       WITH ORDINALITY AS s(lsn, xid, data, ord)
            )
            , rows AS (
                SELECT lsn, ord, j,
                       j->>'schema' AS s, j->>'table' AS t, (j->'cols')::text AS cols,
                       -- Gaps-and-islands: the difference between a row's position among
                       -- its table's changes and its position among that table's changes
                       -- *of this shape* is constant within a contiguous run and changes
                       -- when the shape does. Grouping on it splits a table's changes at
                       -- each DDL while keeping each side in one piece.
                       --
                       -- Grouping on shape alone is not enough, and not only for ordering:
                       -- `ADD COLUMN x` then `DROP COLUMN x` returns the relation to a
                       -- shape it already had, so one group would hold two runs separated
                       -- in time and interleave them on the way out.
                       row_number() OVER (PARTITION BY j->>'schema', j->>'table'
                                              ORDER BY ord)
                     - row_number() OVER (PARTITION BY j->>'schema', j->>'table',
                                                       (j->'cols')::text ORDER BY ord)
                         AS run
                  FROM ch
                 WHERE j->>'op' IN ('I','U','D')
            )
            , grouped AS (
                SELECT s, t, cols,
                       jsonb_agg(jsonb_build_object('lsn', lsn::text, 'ch', j)
                                 ORDER BY ord)::text AS changes,
                       min(ord) AS first_ord
                  FROM rows
                 GROUP BY s, t, cols, run
            )
            -- Per-transaction summary, emitted as a group under the reserved source name so
            -- it travels the same path as a table's changes. Built from the changes rather
            -- than from the plugin's own begin/commit records: those carry the xid and
            -- nothing else, and the counts have to come from what was actually decoded.
            --
            -- A transaction is always returned whole — `upto_nchanges` is only consulted at
            -- commit boundaries — so a count taken within one drain is the transaction's
            -- real count and not a fragment of it.
            , per_collection AS (
                SELECT (j->>'xid')::bigint AS xid,
                       (j->>'schema') || '.' || (j->>'table') AS coll,
                       count(*) AS n, max(ord) AS last_ord,
                       max(lsn) AS lsn, max((j->>'ts')::bigint) AS ts
                  FROM ch WHERE j->>'op' IN ('I','U','D') AND j->>'xid' IS NOT NULL
                 GROUP BY 1, 2
            )
            , txns AS (
                SELECT max(last_ord) AS first_ord,
                       jsonb_agg(jsonb_build_object(
                           'lsn', lsn::text,
                           'ch', jsonb_build_object(
                                   'op', 'T', 'xid', xid, 'ts', ts,
                                   'events', events, 'tables', tables))
                         ORDER BY last_ord)::text AS changes
                  FROM (
                    SELECT xid, max(lsn) AS lsn, max(ts) AS ts,
                           sum(n) AS events,
                           jsonb_object_agg(coll, n) AS tables,
                           max(last_ord) AS last_ord
                      FROM per_collection GROUP BY xid
                  ) t
            )
            SELECT s, t, cols, changes, (SELECT max(lsn) FROM ch)::text
              FROM (
                SELECT s, t, cols, changes, first_ord FROM grouped
                UNION ALL
                -- Ordered after every table group by `first_ord`, so a consumer that reads
                -- both topics sees a transaction's changes before its summary. Kafka only
                -- orders within a partition and these are different topics, but emitting
                -- them the other way round would make the summary useless to anything that
                -- buffers until it arrives.
                SELECT 'kafgres'::text, 'transaction'::text, '[]'::text, changes, first_ord
                  FROM txns WHERE changes IS NOT NULL
                UNION ALL
                -- A batch of nothing but begin/commit markers still has to advance the
                -- slot. The `rows` filter drops those, so with no row changes at all there
                -- would be no row to carry the high-water mark and the slot would sit
                -- still while WAL accumulated — the thing `test_an_unmapped_table_still_
                -- lets_the_slot_advance` exists to catch. This sentinel carries it; the
                -- caller skips any row with no table.
                SELECT NULL::text, NULL::text, NULL::text, NULL::text, -1::bigint
                 WHERE NOT EXISTS (SELECT 1 FROM grouped)
              ) x
             -- **Invariant I1.** Groups are consumed in the order returned, so without this
             -- they arrive in hash-bucket order — measured: after a `DROP COLUMN` the
             -- post-DDL group hashes first, and two updates to one key either side of the
             -- DDL reach the partition backwards. Offsets stay dense, nothing errors, and a
             -- compacted topic settles on the older value. Exactly the hazard the
             -- `WITH ORDINALITY` note above closes, one level up.
             ORDER BY first_ord",
            None,
            &[SLOT.into(), max_changes.into()],
        )?;
        let mut high = None;
        let mut out = Vec::new();
        for r in rows {
            if high.is_none() {
                high = r.get::<String>(5)?;
            }
            match (
                r.get::<String>(1)?,
                r.get::<String>(2)?,
                r.get::<String>(3)?,
                r.get::<String>(4)?,
            ) {
                (Some(schema), Some(table), Some(cols), Some(changes)) => {
                    out.push(TableChanges { schema, table, cols, changes })
                }
                _ => continue,
            }
        }
        Ok::<_, pgrx::spi::Error>((high, out))
    })
    .map_err(|e| format!("peeking {SLOT:?}: {e}"))
}

/// Drain the slot once: peek, render, produce, advance. Returns records produced.
pub fn drain_once(max_changes: i32) -> Result<usize, String> {
    if !slot_exists() {
        return Ok(0);
    }

    let (high, batch) = peek(max_changes)?;
    let Some(high) = high else {
        return Ok(0);
    };

    let mappings = load_mappings()?;
    let mut produced = 0usize;
    for m in &mappings {
        for group in changes_for(&batch, m) {
            let columns = columns_of(&group.cols)?;
            let changes = group.changes.as_str();
            // Any error stops the drain before the advance: the changes were neither produced nor dead-lettered.
            produced += render_and_produce(m, &columns, changes)
                .map_err(|e| format!("mapping {:?} stopped the CDC slot: {e}", m.name))?;
        }
    }

    advance(&high)?;
    Ok(produced)
}

/// One transaction per mapping, so an append's partition lock never spans other mappings' renders.
pub fn drain_worker(max_changes: i32) -> Result<usize, String> {
    use pgrx::bgworkers::BackgroundWorker;

    let contained = |f: &dyn Fn() -> Result<(Option<String>, Vec<TableChanges>, Vec<Mapping>), String>| {
        BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
            crate::dbtx::atomically(f, |caught| caught.to_string())
        }))
    };

    let peeked = contained(&|| {
        if !slot_exists() {
            return Ok((None, Vec::new(), Vec::new()));
        }
        let (high, batch) = peek(max_changes)?;
        let mappings = load_mappings()?;
        Ok((high, batch, mappings))
    })?;
    let (high, batch, mappings) = peeked;
    let Some(high) = high else {
        return Ok(0);
    };

    let mut produced = 0usize;
    for m in &mappings {
        for group in changes_for(&batch, m) {
            let prepared = BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
                crate::dbtx::atomically(|| columns_of(&group.cols), |caught| caught.to_string())
            }))
            .map_err(|e| format!("mapping {:?}: reading the change shape: {e}", m.name))?;

            let changes = group.changes.as_str();
            let rendered = BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
                crate::dbtx::atomically(
                    || render_and_produce(m, &prepared, changes),
                    |caught| caught.to_string(),
                )
            }));
            produced += rendered
                .map_err(|e| format!("mapping {:?} stopped the CDC slot: {e}", m.name))?;
        }
    }

    BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
        crate::dbtx::atomically(|| advance(&high), |caught| caught.to_string())
    }))?;

    Ok(produced)
}

fn changes_for<'a>(batch: &'a [TableChanges], m: &Mapping) -> Vec<&'a TableChanges> {
    let (schema, table) = split_source(&m.source);
    batch
        .iter()
        .filter(|b| b.schema == schema && b.table == table)
        .collect()
}

fn advance(high: &str) -> Result<(), String> {
    Spi::run_with_args(
        "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
        &[SLOT.into(), high.into()],
    )
    .map_err(|e| format!("advancing {SLOT:?} to {high}: {e}"))
}

struct Mapping {
    name: String,
    source: String,
    topic: String,
    value_expr: String,
    key_expr: Option<String>,
    filter_expr: Option<String>,
    on_error: String,
}

fn load_mappings() -> Result<Vec<Mapping>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT mapping_name, source_table, topic, value_expr, key_expr, filter_expr,
                    on_error
               FROM kafgres_cdc_mappings WHERE enabled ORDER BY mapping_name",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(Mapping {
                name: r.get::<String>(1)?.unwrap_or_default(),
                source: r.get::<String>(2)?.unwrap_or_default(),
                topic: r.get::<String>(3)?.unwrap_or_default(),
                value_expr: r.get::<String>(4)?.unwrap_or_default(),
                key_expr: r.get::<String>(5)?,
                filter_expr: r.get::<String>(6)?,
                on_error: r.get::<String>(7)?.unwrap_or_else(|| "skip".into()),
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())
}

fn split_source(source: &str) -> (String, String) {
    match source.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), source.to_string()),
    }
}

fn render(
    m: &Mapping,
    columns: &[(String, String)],
    changes: &str,
) -> Result<Vec<(Option<String>, Option<String>)>, String> {
    let sql = render_query(
        columns,
        &m.value_expr,
        m.key_expr.as_deref(),
        m.filter_expr.as_deref(),
    );
    let owned = changes.to_string();

    // In a savepoint: uncontained, one bad row aborts the whole worker transaction.
    let rendered = crate::dbtx::atomically(
        || {
            Spi::run("SET LOCAL statement_timeout = '30s'").map_err(|e| e.to_string())?;
            Spi::connect(|client| {
                let rows = client.select(&sql, None, &[owned.clone().into()])?;
                let mut out = Vec::new();
                for r in rows {
                    out.push((r.get::<String>(1)?, r.get::<String>(2)?));
                }
                Ok::<_, pgrx::spi::Error>(out)
            })
            .map_err(|e| e.to_string())
        },
        |caught| caught.to_string(),
    )?;

    // Checked here, so an oversize record fails as a render and takes the per-change retry.
    let mut total = 0usize;
    for (key, value) in &rendered {
        let size = key.as_ref().map_or(0, |k| k.len()) + value.as_ref().map_or(0, |v| v.len());
        if size > MAX_RECORD_BYTES {
            return Err(format!(
                "rendered record is {size} bytes, over the {MAX_RECORD_BYTES}-byte limit"
            ));
        }
        total += size;
        if total > MAX_DRAIN_BYTES {
            return Err(format!(
                "this batch renders more than the {MAX_DRAIN_BYTES}-byte drain limit"
            ));
        }
    }
    Ok(rendered)
}

/// Render all, then append all: an append holds its partition lock until commit and must not span renders.
fn render_and_produce(
    m: &Mapping,
    columns: &[(String, String)],
    changes: &str,
) -> Result<usize, String> {
    let Some((target, rendered)) = render_phase(m, columns, changes)? else {
        return Ok(0);
    };

    if let Err(e) = Spi::run("SET LOCAL lock_timeout = '2s'") {
        return Err(e.to_string());
    }

    let mut n = 0;
    for (key, value) in rendered {
        if let Err(e) = produce_rendered(&target, key.as_deref(), value.as_deref()) {
            if n == 0 {
                return Err(e);
            }
            log!(
                "kafgres: WARNING: CDC mapping {:?} appended {n} of its rendered record(s)                  and then failed: {e}. The slot advances past this batch and every change                  in it goes to kafgres_cdc_errors — on the segment engine the {n} already                  appended cannot be withdrawn, so replaying the dead letters duplicates them.",
                m.name
            );
            for one in split_changes(changes)? {
                dead_letter(m, &one, &format!("partial append after {n} record(s): {e}"));
            }
            return Ok(n);
        }
        n += 1;
    }
    Ok(n)
}

/// Render without appending; `None` means the changes were dead-lettered and the drain carries on.
fn render_phase(
    m: &Mapping,
    columns: &[(String, String)],
    changes: &str,
) -> Result<Option<(Target, Vec<(Option<String>, Option<String>)>)>, String> {
    let target = match resolve_topic(&m.topic) {
        Ok(t) => t,
        Err(e) => {
            if m.on_error == "stall" {
                return Err(e);
            }
            log!("kafgres: CDC mapping {:?}: {e}", m.name);
            for one in split_changes(changes)? {
                dead_letter(m, &one, &e);
            }
            return Ok(None);
        }
    };

    let rendered = match render(m, columns, changes) {
        Ok(rendered) => rendered,
        // One set-based query cannot say which change raised; retry one at a time to divert only the offender.
        Err(batch_err) => {
            if m.on_error == "stall" {
                return Err(batch_err);
            }
            let mut out: Vec<(Option<String>, Option<String>)> = Vec::new();
            let mut held = 0usize;
            for one in split_changes(changes)? {
                match render(m, columns, &one) {
                    Ok(rows) => {
                        held += rows
                            .iter()
                            .map(|(k, v)| {
                                k.as_ref().map_or(0, |k| k.len())
                                    + v.as_ref().map_or(0, |v| v.len())
                            })
                            .sum::<usize>();
                        if held > MAX_DRAIN_BYTES {
                            return Err(format!(
                                "this batch renders more than the {MAX_DRAIN_BYTES}-byte \
                                 drain limit even one change at a time"
                            ));
                        }
                        out.extend(rows);
                    }
                    Err(e) => {
                        log!("kafgres: CDC mapping {:?}: {e}", m.name);
                        dead_letter(m, &one, &e);
                    }
                }
            }
            out
        }
    };

    Ok(Some((target, rendered)))
}

struct Target {
    topic: String,
    topic_id: u32,
    partitions: i32,
}

fn resolve_topic(topic: &str) -> Result<Target, String> {
    let topic_id: i32 = read_one(
        "SELECT (SELECT topic_id::int FROM kafgres_topics WHERE name = $1)",
        &[topic.into()],
    )?
    .ok_or_else(|| format!("no such topic {topic:?}"))?;

    let partitions: i32 = read_one(
        "SELECT (SELECT count(*)::int FROM kafgres_partitions WHERE topic_id = $1::oid)",
        &[topic_id.into()],
    )?
    .unwrap_or(0);
    if partitions <= 0 {
        return Err(format!("topic {topic:?} has no partitions"));
    }

    Ok(Target {
        topic: topic.to_string(),
        topic_id: topic_id as u32,
        partitions,
    })
}

fn split_changes(changes: &str) -> Result<Vec<String>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT jsonb_build_array(e)::text FROM jsonb_array_elements($1::jsonb) AS e",
            None,
            &[changes.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let Some(v) = r.get::<String>(1)? {
                out.push(v);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())
}

fn dead_letter(m: &Mapping, one: &str, err: &str) {
    let stored = Spi::run_with_args(
        "INSERT INTO kafgres_cdc_errors (mapping_name, lsn, change, error)
         SELECT $1, (e->>'lsn')::pg_lsn, e->'ch', $3
           FROM jsonb_array_elements($2::jsonb) AS e",
        &[m.name.clone().into(), one.into(), err.into()],
    );
    if let Err(e) = stored {
        log!("kafgres: could not dead-letter a change for mapping {:?}: {e}", m.name);
    }
}

/// Plain `append`, not the pending+marker path: there is no caller transaction to tie visibility to.
fn produce_rendered(target: &Target, key: Option<&str>, value: Option<&str>) -> Result<(), String> {
    use crate::storage::RawBatch;
    use kafgres_codec::records::{build_batch, NewRecord, RecordBatch};

    // Same murmur2 as `kafgres_produce()`, so a key lands on one partition on either write path.
    let partition = match key {
        Some(k) => (crate::produce_sql::murmur2(k.as_bytes()) & 0x7fff_ffff) % target.partitions,
        None => 0,
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let bytes = build_batch(&[NewRecord {
        key: key.map(|k| k.as_bytes().to_vec()),
        value: value.map(|v| v.as_bytes().to_vec()),
        timestamp: now,
    }]);
    let view =
        RecordBatch::new(bytes.clone()).map_err(|e| format!("built an invalid batch: {e:?}"))?;
    let raw = RawBatch {
        bytes: bytes.to_vec(),
        record_count: view.record_count(),
        last_offset_delta: view.last_offset_delta(),
        max_timestamp: view.max_timestamp(),
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        is_transactional: false,
        is_control: false,
    };

    let mut store = crate::storage::open();
    store
        .append(target.topic_id, partition, raw, None)
        .map_err(|e| format!("appending to {:?}/{partition}: {e}", target.topic))?;
    Ok(())
}

/// Call as the first statement of a transaction: slot creation is refused in one that has written.
#[pg_extern]
fn kafgres_cdc_create_slot() -> bool {
    match ensure_slot() {
        Ok(created) => created,
        Err(e) => error!("kafgres: {e}"),
    }
}

/// If an enabled mapping remains, the worker recreates the slot; changes in between are not replayed.
#[pg_extern]
fn kafgres_cdc_drop_slot() -> bool {
    if !slot_exists() {
        return false;
    }
    Spi::run_with_args("SELECT pg_drop_replication_slot($1)", &[SLOT.into()])
        .unwrap_or_else(|e| error!("kafgres: {e}"));
    true
}

/// Do not call in a transaction you might roll back: the slot advance is not transactional,
#[pg_extern]
fn kafgres_cdc_drain(max_changes: default!(i32, 10000)) -> i64 {
    match drain_once(max_changes) {
        Ok(n) => n as i64,
        Err(e) => error!("kafgres: {e}"),
    }
}

#[pg_extern]
fn kafgres_cdc_status() -> TableIterator<
    'static,
    (
        name!(slot, Option<String>),
        name!(active, Option<bool>),
        name!(confirmed_flush_lsn, Option<String>),
        name!(retained_bytes, Option<i64>),
        name!(mappings, Option<i64>),
        name!(dead_lettered, Option<i64>),
    ),
> {
    let row = Spi::connect(|client| {
        let rows = client.select(
            "SELECT s.slot_name::text, s.active, s.confirmed_flush_lsn::text,
                    CASE WHEN pg_is_in_recovery() THEN NULL
                         ELSE pg_wal_lsn_diff(pg_current_wal_lsn(),
                                              s.confirmed_flush_lsn)::bigint END,
                    (SELECT count(*) FROM kafgres_cdc_mappings WHERE enabled),
                    (SELECT count(*) FROM kafgres_cdc_errors)
               FROM pg_replication_slots s WHERE s.slot_name = $1",
            Some(1),
            &[SLOT.into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push((
                r.get::<String>(1)?,
                r.get::<bool>(2)?,
                r.get::<String>(3)?,
                r.get::<i64>(4)?,
                r.get::<i64>(5)?,
                r.get::<i64>(6)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| error!("kafgres: {e}"));
    TableIterator::new(row)
}

// Snapshot rows are synthesised into the plugin's shape (`op` of `R`) and rendered like live changes.
fn snapshot_batch_rows() -> i32 {
    crate::cdc_snapshot_batch_rows()
}

struct SnapshotState {
    name: String,
    source: String,
    cursor: Option<String>,
}

fn pending_snapshots() -> Result<Vec<SnapshotState>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT mapping_name, source_table, snapshot_cursor::text
               FROM kafgres_cdc_mappings
              WHERE enabled AND snapshot IN ('pending', 'running')
              ORDER BY mapping_name",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(SnapshotState {
                name: r.get::<String>(1)?.unwrap_or_default(),
                source: r.get::<String>(2)?.unwrap_or_default(),
                cursor: r.get::<String>(3)?,
            });
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())
}

fn primary_key_of(source: &str) -> Result<Vec<(String, String)>, String> {
    if source == TRANSACTION_SOURCE {
        return Err(format!(
            "{TRANSACTION_SOURCE} is a stream of commits, not a table — there is no prior \
             state to snapshot, and the transactions that already happened are not \
             reconstructable from anything this broker keeps"
        ));
    }
    let (schema, table) = split_source(source);
    let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
    let cols: Vec<(String, String)> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)
               FROM pg_index i
               JOIN pg_attribute a
                 ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
              WHERE i.indrelid = $1::regclass AND i.indisprimary
              ORDER BY array_position(i.indkey::int2[], a.attnum)",
            None,
            &[qualified.as_str().into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let (Some(n), Some(t)) = (r.get::<String>(1)?, r.get::<String>(2)?) {
                out.push((n, t));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())?;
    if cols.is_empty() {
        return Err(format!(
            "{source} has no primary key, so a snapshot has no stable order to resume from; \
             add one, or leave this mapping to stream changes only"
        ));
    }
    Ok(cols)
}

fn shape_of(source: &str) -> Result<Vec<(String, String)>, String> {
    let (schema, table) = split_source(source);
    let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT attname::text, format_type(atttypid, atttypmod)
               FROM pg_attribute
              WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped
              ORDER BY attnum",
            None,
            &[qualified.as_str().into()],
        )?;
        let mut out = Vec::new();
        for r in rows {
            if let (Some(n), Some(t)) = (r.get::<String>(1)?, r.get::<String>(2)?) {
                out.push((n, t));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())
}

/// Values are each column's text output, like the plugin emits; `to_jsonb(t)` would lose `numeric` digits.
fn snapshot_page(
    source: &str,
    pk: &[(String, String)],
    shape: &[(String, String)],
    cursor: Option<&str>,
    limit: i32,
) -> Result<(String, Option<String>, i64), String> {
    let (schema, table) = split_source(source);
    let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));

    let order: Vec<String> = pk.iter().map(|(n, _)| quote_ident(n)).collect();
    let order_asc = order.join(", ");
    let order_desc: Vec<String> = order.iter().map(|c| format!("{c} DESC")).collect();

    let after = match cursor {
        None => String::new(),
        Some(_) => {
            let lhs = order_asc.clone();
            let rhs: Vec<String> = pk
                .iter()
                .enumerate()
                .map(|(i, (_, ty))| format!("($1::jsonb ->> {i})::{ty}"))
                .collect();
            format!("WHERE ({lhs}) > ({})", rhs.join(", "))
        }
    };

    let text_pairs: Vec<String> = shape
        .iter()
        .map(|(n, _)| format!("{}, {}::text", sql_literal(n), quote_ident(n)))
        .collect();
    let pk_text: Vec<String> = pk.iter().map(|(n, _)| format!("{}::text", quote_ident(n))).collect();

    let sql = format!(
        "WITH page AS (SELECT * FROM {qualified} {after} ORDER BY {order_asc} LIMIT {limit})
         SELECT
           (SELECT coalesce(jsonb_agg(e ORDER BY ord), '[]'::jsonb)::text
              FROM (SELECT row_number() OVER (ORDER BY {order_asc}) AS ord,
                           jsonb_build_object(
                             'lsn', '0/0',
                             'ch', jsonb_build_object(
                                     'op', 'R',
                                     -- No transaction and no commit: a snapshot row is a
                                     -- read, not a change. Absent rather than zeroed, so a
                                     -- mapping that groups by `xid` groups nothing here
                                     -- instead of putting every snapshot row in one
                                     -- fictitious transaction 0.
                                     'xid', NULL,
                                     'ts', NULL,
                                     'new', jsonb_build_object({pairs}),
                                     'old', NULL)) AS e
                      FROM page) s),
           (SELECT jsonb_build_array({pk_text})::text
              FROM page ORDER BY {desc} LIMIT 1),
           (SELECT count(*) FROM page)",
        pairs = text_pairs.join(", "),
        pk_text = pk_text.join(", "),
        desc = order_desc.join(", "),
    );

    Spi::connect(|client| {
        let rows = if cursor.is_some() {
            client.select(&sql, None, &[cursor.into()])?
        } else {
            client.select(&sql, None, &[])?
        };
        for r in rows {
            return Ok::<_, pgrx::spi::Error>((
                r.get::<String>(1)?.unwrap_or_else(|| "[]".into()),
                r.get::<String>(2)?,
                r.get::<i64>(3)?.unwrap_or(0),
            ));
        }
        Ok(("[]".to_string(), None, 0))
    })
    .map_err(|e| e.to_string())
}

fn snapshot_step(st: &SnapshotState, limit: i32) -> Result<bool, String> {
    let m = load_mapping(&st.name)?
        .ok_or_else(|| format!("mapping {:?} disappeared mid-snapshot", st.name))?;
    let pk = primary_key_of(&st.source)?;
    let shape = shape_of(&st.source)?;
    let (changes, last, n) = snapshot_page(&st.source, &pk, &shape, st.cursor.as_deref(), limit)?;

    if n > 0 {
        render_and_produce(&m, &shape, &changes)?;
    }

    // Short page means end of table: a full page ending at the last row looks like one with more behind it.
    let done = n < limit as i64;
    Spi::run_with_args(
        "UPDATE kafgres_cdc_mappings
            SET snapshot = CASE WHEN $3 THEN 'done' ELSE 'running' END,
                snapshot_cursor = COALESCE($2::jsonb, snapshot_cursor),
                snapshot_rows = snapshot_rows + $4,
                snapshot_started_at = COALESCE(snapshot_started_at, now()),
                snapshot_finished_at = CASE WHEN $3 THEN now() ELSE NULL END
          WHERE mapping_name = $1",
        &[st.name.as_str().into(), last.into(), done.into(), n.into()],
    )
    .map_err(|e| e.to_string())?;
    Ok(done)
}

fn load_mapping(name: &str) -> Result<Option<Mapping>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT mapping_name, source_table, topic, value_expr, key_expr, filter_expr, on_error
               FROM kafgres_cdc_mappings WHERE mapping_name = $1",
            Some(1),
            &[name.into()],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>(Some(Mapping {
                name: r.get::<String>(1)?.unwrap_or_default(),
                source: r.get::<String>(2)?.unwrap_or_default(),
                topic: r.get::<String>(3)?.unwrap_or_default(),
                value_expr: r.get::<String>(4)?.unwrap_or_default(),
                key_expr: r.get::<String>(5)?,
                filter_expr: r.get::<String>(6)?,
                on_error: r.get::<String>(7)?.unwrap_or_else(|| "skip".into()),
            }));
        }
        Ok(None)
    })
    .map_err(|e| e.to_string())
}

/// The drain is held while a snapshot runs, so a change cannot land before that key's older snapshot row.
pub fn snapshot_outstanding() -> bool {
    Spi::get_one::<i64>(
        "SELECT (SELECT count(*) FROM kafgres_cdc_mappings
                  WHERE enabled AND snapshot IN ('pending', 'running'))",
    )
    .ok()
    .flatten()
    .unwrap_or(0)
        > 0
}

pub fn snapshot_worker() -> Result<usize, String> {
    use pgrx::bgworkers::BackgroundWorker;

    let pending = BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
        crate::dbtx::atomically(pending_snapshots, |caught| caught.to_string())
    }))?;
    let Some(st) = pending.into_iter().next() else {
        return Ok(0);
    };

    let limit = snapshot_batch_rows();
    let name = st.name.clone();
    let done = BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
        crate::dbtx::atomically(|| snapshot_step(&st, limit), |caught| caught.to_string())
    }));
    match done {
        Ok(true) => {
            log!("kafgres: CDC snapshot of mapping {name:?} complete");
            Ok(1)
        }
        Ok(false) => Ok(1),
        Err(e) => {
            log!("kafgres: CDC snapshot of mapping {name:?} paused: {e}");
            Err(e)
        }
    }
}

#[pg_extern]
fn kafgres_snapshot_mapping(mapping_name: &str) -> bool {
    let Some(m) = load_mapping(mapping_name).unwrap_or_else(|e| error!("kafgres: {e}")) else {
        error!("kafgres: no such mapping {mapping_name:?}");
    };
    primary_key_of(&m.source).unwrap_or_else(|e| error!("kafgres: {e}"));

    Spi::run_with_args(
        "UPDATE kafgres_cdc_mappings
            SET snapshot = 'pending', snapshot_cursor = NULL, snapshot_rows = 0,
                snapshot_started_at = NULL, snapshot_finished_at = NULL
          WHERE mapping_name = $1",
        &[mapping_name.into()],
    )
    .unwrap_or_else(|e| error!("kafgres: {e}"));
    true
}

#[pg_extern]
fn kafgres_cdc_snapshots() -> TableIterator<
    'static,
    (
        name!(mapping_name, Option<String>),
        name!(source_table, Option<String>),
        name!(state, Option<String>),
        name!(rows_emitted, Option<i64>),
        name!(cursor, Option<String>),
        name!(started_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(finished_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    let rows = Spi::connect(|client| {
        let rows = client.select(
            "SELECT mapping_name, source_table, snapshot, snapshot_rows,
                    snapshot_cursor::text, snapshot_started_at, snapshot_finished_at
               FROM kafgres_cdc_mappings
              WHERE snapshot <> 'none'
              ORDER BY mapping_name",
            None,
            &[],
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push((
                r.get::<String>(1)?,
                r.get::<String>(2)?,
                r.get::<String>(3)?,
                r.get::<i64>(4)?,
                r.get::<String>(5)?,
                r.get::<pgrx::datum::TimestampWithTimeZone>(6)?,
                r.get::<pgrx::datum::TimestampWithTimeZone>(7)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_else(|e| error!("kafgres: {e}"));
    TableIterator::new(rows)
}

#[pg_extern]
fn kafgres_cdc_snapshot(max_batches: default!(i32, 10_000)) -> i64 {
    let limit = snapshot_batch_rows();
    let mut total = 0i64;
    for _ in 0..max_batches.max(1) {
        let pending = pending_snapshots().unwrap_or_else(|e| error!("kafgres: {e}"));
        let Some(st) = pending.into_iter().next() else { break };
        let before = Spi::get_one_with_args::<i64>(
            "SELECT (SELECT snapshot_rows FROM kafgres_cdc_mappings WHERE mapping_name = $1)",
            &[st.name.as_str().into()],
        )
        .ok()
        .flatten()
        .unwrap_or(0);
        let done = snapshot_step(&st, limit).unwrap_or_else(|e| error!("kafgres: {e}"));
        let after = Spi::get_one_with_args::<i64>(
            "SELECT (SELECT snapshot_rows FROM kafgres_cdc_mappings WHERE mapping_name = $1)",
            &[st.name.as_str().into()],
        )
        .ok()
        .flatten()
        .unwrap_or(0);
        total += after - before;
        if done {
            break;
        }
    }
    total
}
