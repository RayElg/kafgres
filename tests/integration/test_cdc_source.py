"""The CDC change source: a logical decoding output plugin.

`test_cdc_mappings.py` covers mappings, the SQL that turns a change into a record. This
covers where the change comes from: `extension/src/decoding.rs`, a real output plugin
registered under the library's own name, drained by the `kafgres_cdc` worker.

The plugin exists rather than a parser for `test_decoding` because `test_decoding` is a
debugging aid with no format promise, and because the alternative to carrying the tuple out
of the WAL — re-`SELECT`ing the row by key — makes `new` current state rather than
as-of-the-change and cannot render a `DELETE` at all.

**These tests drive the drain by hand**: `kafgres.cdc_interval_ms = 0` stops the worker,
so "produce, drain, assert" is not racing a tick that already consumed the changes.
`test_the_worker_drains_on_its_own` is the one test that leaves it on.
"""

import json
import os
import subprocess
import time

import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

def psql(query, timeout=60):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-tAc", query],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )

def sql(query, timeout=60):
    return psql(query, timeout).stdout.strip()

def set_guc(name, value):
    sql(f"ALTER SYSTEM SET {name} = {value}")
    sql("SELECT pg_reload_conf()")

def consume(topic, timeout=25):
    """Every record on a topic, as (partition, key, value) — through a real client.

    kcat rather than a direct read of the log: the claim being tested is that CDC output is
    ordinary Kafka records, and only a client that never heard of Postgres can establish it.
    """
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "kafgres-clients",
         "kcat", "-b", "127.0.0.1:9092", "-C", "-t", topic, "-o", "beginning", "-e",
         "-f", "%p\t%k\t%s\n"],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    rows = []
    for line in out.stdout.splitlines():
        if line.startswith("%") or "\t" not in line:
            continue
        p, k, v = line.split("\t", 2)
        rows.append((int(p), k, v))
    return rows

def drain():
    out = psql("SELECT kafgres_cdc_drain()")
    assert out.returncode == 0, out.stderr[-500:]
    return int(out.stdout.strip() or 0)

@pytest.fixture(scope="module", autouse=True)
def hand_driven():
    """Stop the worker draining so the tests own the timing.

    A background drain makes "how many records did this change produce?" unanswerable:
    the answer depends on which side got there first.

    The slot is dropped first and the settle is deliberate: `pg_reload_conf()` returns as
    soon as the postmaster has re-read the file, and the worker only picks the new value up
    on its next wake — up to one whole interval later. Without the wait the first test in
    the module races a tick that was already in flight.
    """
    sql("SELECT kafgres_cdc_drop_slot()")
    set_guc("kafgres.cdc_interval_ms", "0")
    time.sleep(2)
    yield
    set_guc("kafgres.cdc_interval_ms", "1000")

@pytest.fixture(autouse=True)
def fixtures(hand_driven):
    sql("SELECT kafgres_cdc_drop_slot()")
    sql("DELETE FROM kafgres_cdc_mappings")
    sql("DELETE FROM kafgres_cdc_errors")
    sql("DROP TABLE IF EXISTS src_orders, src_customers CASCADE")
    sql("CREATE TABLE src_customers (id int primary key, tier text)")
    sql("CREATE TABLE src_orders (id int primary key, customer_id int, "
        "total numeric, status text, note text)")
    sql("INSERT INTO src_customers VALUES (1,'gold'),(2,'silver')")
    sql("SELECT kafgres_drop_topic('src-events')")
    sql("SELECT kafgres_create_topic('src-events', 3)")
    yield
    sql("SELECT kafgres_cdc_drop_slot()")
    sql("DELETE FROM kafgres_cdc_mappings")
    sql("DROP TABLE IF EXISTS src_orders, src_customers CASCADE")

def add_mapping(value_expr, key_expr="new.id::text", filter_expr=None,
                name="m1", source="public.src_orders", topic="src-events"):
    def lit(x):
        return "NULL" if x is None else f"$${x}$$"
    out = psql(
        f"SELECT kafgres_add_mapping('{name}', '{source}', '{topic}', "
        f"$${value_expr}$$, {lit(key_expr)}, {lit(filter_expr)})"
    )
    assert out.returncode == 0, out.stderr[-500:]

def start_slot():
    """Create the slot, which is what starts capturing changes.

    Before this point nothing is retained, which is the intended behaviour and not a
    limitation: a slot pins WAL, so one that exists before any mapping wants it is a disk
    that fills for no reason.
    """
    out = psql("SELECT kafgres_cdc_create_slot()")
    assert out.returncode == 0, out.stderr[-500:]
    assert out.stdout.strip() == "t", "the slot was not created"

def values(rows):
    return [json.loads(v) for _, _, v in rows]

def test_an_insert_reaches_the_topic_through_the_wal():
    add_mapping("jsonb_build_object('id', new.id, 'status', new.status, 'op', op)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x')")
    assert drain() == 1

    rows = consume("src-events")
    assert len(rows) == 1, rows
    assert rows[0][1] == "10", f"key came from the changed row: {rows}"
    assert values(rows)[0] == {"id": 10, "status": "placed", "op": "I"}

def test_a_numeric_survives_the_round_trip():
    """The plugin emits text; the mapping casts it back through the column's own type.

    This is the whole reason `decoding.rs` writes `"99.5"` rather than a JSON number. A
    JSON number is a double, and `numeric` is not — the round trip through one loses digits
    silently on values a test with small integers would never catch. The type comes from the
    change's own `cols`, typmod included, so the cast back is the column's real type as of
    that change rather than whatever the table says now.
    """
    add_mapping("jsonb_build_object('total', new.total)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,12345678901234567890.12345,'placed','x')")
    drain()

    rows = consume("src-events")
    assert len(rows) == 1, rows
    assert "12345678901234567890.12345" in rows[0][2], rows[0][2]

def test_a_delete_still_has_a_key():
    """`new` falls back to the before-image on DELETE.

    Without the fallback the obvious mapping — key on `new.id` — renders a NULL key for
    every delete, which sends tombstones to partition 0 and breaks compaction. Silently:
    nothing errors, the records are there, and only a consumer relying on per-key ordering
    ever finds out.
    """
    add_mapping("jsonb_build_object('id', new.id, 'op', op)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x')")
    sql("DELETE FROM src_orders WHERE id = 10")
    assert drain() == 2

    rows = consume("src-events")
    assert [k for _, k, _ in rows] == ["10", "10"], f"the delete lost its key: {rows}"
    assert [v["op"] for v in values(rows)] == ["I", "D"]
    assert len({p for p, _, _ in rows}) == 1, rows

def test_replica_identity_full_gives_an_exact_before_image():
    """`old` is as of the change, not current state.

    The default REPLICA IDENTITY carries only the key columns, so `old.status` is NULL —
    a property of the table, not something the plugin can supply. `FULL` carries the whole
    row, and that before-image comes out of the WAL rather than a re-read, so it is exact
    even though the row has since changed again.
    """
    sql("ALTER TABLE src_orders REPLICA IDENTITY FULL")
    add_mapping("jsonb_build_object('was', old.status, 'now', new.status, 'op', op)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x')")
    sql("UPDATE src_orders SET status = 'shipped' WHERE id = 10")
    sql("UPDATE src_orders SET status = 'delivered' WHERE id = 10")
    drain()

    vs = values(consume("src-events"))
    assert [v["op"] for v in vs] == ["I", "U", "U"], vs
    assert vs[1] == {"was": "placed", "now": "shipped", "op": "U"}, vs[1]
    assert vs[2] == {"was": "shipped", "now": "delivered", "op": "U"}, vs[2]

def test_changes_arrive_in_commit_order():
    add_mapping("jsonb_build_object('n', new.status)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,1,'s0','x')")
    for i in range(1, 12):
        sql(f"UPDATE src_orders SET status = 's{i}' WHERE id = 10")
    drain()

    rows = consume("src-events")
    assert [v["n"] for v in values(rows)] == [f"s{i}" for i in range(12)], rows
    assert len({p for p, _, _ in rows}) == 1

def test_the_brokers_own_tables_are_not_decoded():
    """The plugin drops `kafgres_*` relations, and that is a loop, not tidiness.

    On the table engine the log *is* a Postgres table, so every record CDC produces
    would come back through the slot as a change — decoded, matched against no mapping,
    discarded. Unbounded decoding work proportional to broker throughput, on the commit
    path.
    """
    add_mapping("jsonb_build_object('id', new.id)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x')")
    drain()
    baseline = len(consume("src-events"))

    produced = subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", "kafgres-clients",
         "kcat", "-b", "127.0.0.1:9092", "-P", "-t", "src-events", "-p", "0"],
        input="direct\n", capture_output=True, text=True, timeout=30, cwd=REPO,
    )
    assert produced.returncode == 0, produced.stderr[-300:]
    sql("SELECT kafgres_cdc_drain()")

    peeked = sql(
        "SELECT count(*) FROM pg_logical_slot_peek_changes('kafgres_cdc', NULL, NULL) "
        "WHERE data::jsonb->>'op' IN ('I','U','D')"
    )
    assert peeked == "0", f"broker-internal writes were decoded as changes: {peeked}"
    assert len(consume("src-events")) == baseline + 1, "CDC re-emitted the wire produce"

def test_a_bad_change_does_not_divert_the_good_ones_beside_it():
    """One bad row must not dead-letter the whole batch.

    A mapping renders as one set-based query per drain, so a raise names no row: without a
    per-change retry on the failure path, every good change in the same batch is diverted
    with the bad one — never reaching its topic, and silently, because the drain reports
    success and the records are sitting in a table nobody watches.
    """
    add_mapping("jsonb_build_object('n', new.note::int)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','1')")
    sql("INSERT INTO src_orders VALUES (11,1,99.5,'placed','oops')")
    sql("INSERT INTO src_orders VALUES (12,1,99.5,'placed','3')")
    assert drain() == 2, "the good changes were diverted along with the bad one"

    assert sql("SELECT count(*) FROM kafgres_cdc_errors") == "1"
    assert sql("SELECT change->'new'->>'id' FROM kafgres_cdc_errors") == "11"
    assert [v["n"] for v in values(consume("src-events"))] == [1, 3]

def test_a_render_failure_dead_letters_and_the_stream_continues():
    """`on_error = 'skip'` is not `drop`.

    The mapping compiles at definition time, so a failure here is data-dependent — a cast
    that fails on one row. Stalling on it is not the safe option it looks like: a stalled
    slot pins WAL, and pinned WAL fills the disk and takes Postgres down. So the default
    skips, and the change lands in `kafgres_cdc_errors` whole, where it can be inspected
    and replayed.
    """
    add_mapping("jsonb_build_object('n', new.note::int)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','oops')")
    drain()

    assert sql("SELECT count(*) FROM kafgres_cdc_errors") == "1", "the change was dropped"
    err = sql("SELECT error FROM kafgres_cdc_errors LIMIT 1")
    assert "invalid input syntax" in err, err
    stored = sql("SELECT change->>'op' FROM kafgres_cdc_errors LIMIT 1")
    assert stored == "I", stored

    sql("INSERT INTO src_orders VALUES (11,1,5,'placed','7')")
    assert drain() == 1
    assert [v["n"] for v in values(consume("src-events"))] == [7]

def test_on_error_stall_holds_the_slot_where_it_was():
    """The other half of the choice, for whoever would rather stop than skip.

    What makes it a real stall is that the slot is not advanced — so nothing is lost and
    the changes are still there once the mapping is fixed. The cost is that WAL accumulates
    until it is, which is why it is not the default.
    """
    add_mapping("jsonb_build_object('n', new.note::int)")
    sql("UPDATE kafgres_cdc_mappings SET on_error = 'stall'")
    start_slot()

    before = sql("SELECT confirmed_flush_lsn FROM pg_replication_slots "
                 "WHERE slot_name='kafgres_cdc'")
    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','oops')")

    out = psql("SELECT kafgres_cdc_drain()")
    assert out.returncode != 0, "a stalling mapping reported success"
    assert "stopped the CDC slot" in out.stderr, out.stderr[-400:]

    after = sql("SELECT confirmed_flush_lsn FROM pg_replication_slots "
                "WHERE slot_name='kafgres_cdc'")
    assert after == before, f"the slot advanced past an unrendered change: {before} -> {after}"

    sql("UPDATE src_orders SET note = '7' WHERE id = 10")
    sql("UPDATE kafgres_cdc_mappings SET on_error = 'skip'")
    assert drain() == 1, "the stalled changes were not replayed after the fix"
    assert [v["n"] for v in values(consume("src-events"))] == [7]

def test_an_oversize_rendered_record_is_refused_rather_than_appended():
    """A mapping is the one path that can put an unbounded record in the log.

    A wire produce is bounded by the frame that carries it. `value_expr => to_jsonb(new)`
    over a row with a large column is not, and the cost lands on the broker rather than
    here: Fetch must return the first batch whole even when it exceeds the consumer's
    `max.partition.fetch.bytes`, so the broker worker allocates it on read and again on
    encode.
    """
    add_mapping("jsonb_build_object('n', new.note)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,1,'placed', repeat('x', 9 * 1024 * 1024))")
    sql("INSERT INTO src_orders VALUES (11,1,1,'placed','small')")
    assert drain() == 1, "the small record beside the oversize one did not get through"

    assert sql("SELECT count(*) FROM kafgres_cdc_errors") == "1"
    err = sql("SELECT error FROM kafgres_cdc_errors LIMIT 1")
    assert "over the" in err and "byte limit" in err, err
    assert [v["n"] for v in values(consume("src-events"))] == ["small"]

def test_a_mapping_whose_topic_vanished_dead_letters_rather_than_losing_changes():
    """A produce failure must fail the same way a render failure does.

    Otherwise it skips the dead letter, the drain logs a line and advances the slot anyway,
    and those changes are gone — the exact silent loss `on_error = 'skip'` was written to
    avoid.
    """
    add_mapping("jsonb_build_object('id', new.id)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x')")
    sql("SELECT kafgres_drop_topic('src-events')")
    try:
        assert drain() == 0
        assert sql("SELECT count(*) FROM kafgres_cdc_errors") == "1", (
            "the change was lost rather than dead-lettered"
        )
        assert "no such topic" in sql("SELECT error FROM kafgres_cdc_errors LIMIT 1")
        assert sql(
            "SELECT count(*) FROM pg_logical_slot_peek_changes('kafgres_cdc', NULL, NULL) "
            "WHERE data::jsonb->>'op' IN ('I','U','D')"
        ) == "0", "the slot stalled on a permanent fault"
    finally:
        sql("SELECT kafgres_create_topic('src-events', 3)")

def test_an_unchanged_toasted_column_does_not_crash_the_decoder():
    """The WAL carries a TOAST pointer, not the value, for a column an UPDATE did not touch.

    Passing that pointer to the type's output function is a toast fetch under a historic
    snapshot. The plugin guards it and emits null instead, which is indistinguishable from a
    real NULL — a limitation pgoutput shares. What must not happen is the decoder returning
    the pointer's bytes or failing.

    This is the shape of bug no amount of INSERT/UPDATE/DELETE on short rows will produce.
    """
    add_mapping("jsonb_build_object('id', new.id, 'note_len', length(new.note))")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,1,'placed', "
        "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1,200)))")
    sql("UPDATE src_orders SET status = 'shipped' WHERE id = 10")
    assert drain() == 2

    vs = values(consume("src-events"))
    assert vs[0]["note_len"] == 6400, vs[0]
    assert vs[1]["note_len"] is None, f"a TOAST pointer was decoded as a value: {vs[1]}"

def test_a_filter_is_applied_to_changes():
    add_mapping("jsonb_build_object('id', new.id)", filter_expr="new.status = 'placed'")
    start_slot()

    sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x'),(11,2,5,'draft','x')")
    assert drain() == 1

    assert [v["id"] for v in values(consume("src-events"))] == [10]

def test_an_unmapped_table_still_lets_the_slot_advance():
    """Otherwise a busy unmapped table pins WAL forever while the drain reports nothing.

    The begin/commit markers and the changes of unmapped tables render no records, so a
    drain that advanced only past *rendered* changes would never move at all on a database
    whose traffic is mostly elsewhere — which is every database.
    """
    add_mapping("jsonb_build_object('id', new.id)")
    start_slot()

    sql("CREATE TABLE unmapped (id int)")
    try:
        before = sql("SELECT confirmed_flush_lsn FROM pg_replication_slots "
                     "WHERE slot_name='kafgres_cdc'")
        sql("INSERT INTO unmapped SELECT generate_series(1, 500)")
        assert drain() == 0
        after = sql("SELECT confirmed_flush_lsn FROM pg_replication_slots "
                    "WHERE slot_name='kafgres_cdc'")
        assert after != before, "the slot did not advance past unmapped traffic"
    finally:
        sql("DROP TABLE IF EXISTS unmapped")

def test_the_slot_is_not_created_until_a_mapping_wants_it():
    """A slot that exists before anything reads it is a disk that fills for no reason."""
    assert sql("SELECT count(*) FROM pg_replication_slots WHERE slot_name='kafgres_cdc'") == "0"
    out = psql("SELECT kafgres_cdc_create_slot()")
    assert out.returncode == 0, out.stderr[-400:]
    assert out.stdout.strip() == "f", "a slot was created with no mapping to feed"

def test_the_worker_drains_on_its_own():
    """The one test that leaves the worker's tick on, because it is what is under test."""
    add_mapping("jsonb_build_object('id', new.id)")
    start_slot()
    set_guc("kafgres.cdc_interval_ms", "200")
    try:
        sql("INSERT INTO src_orders VALUES (10,1,99.5,'placed','x')")
        deadline = time.time() + 20
        while time.time() < deadline:
            if len(consume("src-events")) == 1:
                break
            time.sleep(1)
        else:
            pytest.fail("the CDC worker never produced the change")
    finally:
        set_guc("kafgres.cdc_interval_ms", "0")

def test_a_drain_straddling_an_alter_renders_both_shapes(fixtures):
    """Each change renders through the shape it was captured with.

    Rendering through the table's rowtype *now* would render a change captured before an
    `ALTER TABLE` through a definition it never had; `cols` in the change is the same
    answer Debezium's event-carried schema gives.
    """
    add_mapping("jsonb_build_object('id', new.id, 'total', new.total)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (1,1,10.50,'placed','x')")
    sql("ALTER TABLE src_orders ADD COLUMN extra text")
    sql("INSERT INTO src_orders VALUES (2,1,20.75,'placed','x','new')")
    assert drain() == 2, "one drain must handle both shapes"

    vs = values(consume("src-events"))
    assert [v["total"] for v in vs] == [10.50, 20.75], vs

def test_a_column_dropped_after_a_change_still_renders_that_change(fixtures):
    """The strongest case for carrying the shape: a change captured while the column
    existed must still produce its value. Rendering from the current rowtype discards
    the value silently — a field quietly missing from records."""
    add_mapping("jsonb_build_object('id', new.id, 'status', new.status)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (1,1,1,'placed','x')")
    sql("ALTER TABLE src_orders DROP COLUMN status")
    sql("INSERT INTO src_orders (id, customer_id, total, note) VALUES (2,1,1,'x')")
    assert drain() == 1, "the change from before the drop should still render"

    vs = values(consume("src-events"))
    assert vs == [{"id": 1, "status": "placed"}], vs

    assert sql("SELECT count(*) FROM kafgres_cdc_errors") == "1"
    assert "new.status does not exist" in sql("SELECT error FROM kafgres_cdc_errors LIMIT 1")

def test_a_renamed_column_does_not_silently_become_null(fixtures):
    """A rename is a drop and an add as far as a projection is concerned."""
    add_mapping("jsonb_build_object('s', new.status)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (1,1,1,'placed','x')")
    sql("ALTER TABLE src_orders RENAME COLUMN status TO state")
    sql("INSERT INTO src_orders VALUES (2,1,1,'shipped','x')")
    drain()

    vs = values(consume("src-events"))
    assert vs == [{"s": "placed"}], f"the pre-rename change lost its value: {vs}"
    assert sql("SELECT count(*) FROM kafgres_cdc_errors") == "1", (
        "the post-rename change should fail rather than emit null"
    )

def test_changes_either_side_of_a_ddl_keep_their_order(fixtures):
    """Ordering across a group boundary.

    A drain straddling a DDL renders one group per shape, and the groups are appended in
    the order `peek` returns them. Grouped without an `ORDER BY`, Postgres hash-aggregates
    and returns them in bucket order — measured: after a `DROP COLUMN` the *post*-DDL group
    comes first, so two updates to one key either side of the DDL reach the partition
    backwards. Offsets stay dense, nothing errors, and a compacted topic settles on the
    older value.

    `ADD COLUMN` happens to hash in order, which is why the test above passes either way
    and this one uses `DROP`.
    """
    add_mapping("jsonb_build_object('total', new.total)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (1,1,100,'placed','x')")
    sql("ALTER TABLE src_orders DROP COLUMN note")
    sql("UPDATE src_orders SET total = 200 WHERE id = 1")
    assert drain() == 2

    rows = consume("src-events")
    assert [v["total"] for v in values(rows)] == [100, 200], (
        f"the two shapes were appended out of commit order: {values(rows)}"
    )
    assert len({p for p, _, _ in rows}) == 1, rows

def test_a_shape_that_recurs_does_not_interleave(fixtures):
    """`ADD COLUMN x` then `DROP COLUMN x` returns the relation to a shape it already had.

    Grouped by shape alone, the changes from before the add and after the drop share a
    group while the ones between sit in another — so one group carries two runs separated
    in time, and they interleave on the way out. Grouping by contiguous run keeps them
    apart.
    """
    add_mapping("jsonb_build_object('total', new.total)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (1,1,10,'placed','x')")
    sql("ALTER TABLE src_orders ADD COLUMN tmp text")
    sql("INSERT INTO src_orders VALUES (2,1,20,'placed','x','t')")
    sql("ALTER TABLE src_orders DROP COLUMN tmp")
    sql("INSERT INTO src_orders VALUES (3,1,30,'placed','x')")
    assert drain() == 3

    assert [v["total"] for v in values(consume("src-events"))] == [10, 20, 30], (
        "a recurring shape interleaved two runs that were separated in time"
    )

def test_a_jsonb_column_is_not_double_encoded(fixtures):
    """`new.payload` is the column's value, not a string containing it.

    The plugin emits every value as its type's text output, so a `jsonb` column arrives as a
    JSON *string*. `jsonb_populate_record` special-cases jsonb and assigned that string
    straight through, so a mapping saw `"{\\"a\\": 1}"` — the value wrapped in a second
    layer of encoding — and every consumer had to know to unwrap it. Casting the text back
    through the column's own type parses it, which is what the column actually held.
    """
    sql("ALTER TABLE src_orders ADD COLUMN payload jsonb")
    add_mapping("jsonb_build_object('p', new.payload)")
    start_slot()

    sql("INSERT INTO src_orders VALUES (1,1,1,'placed','x','{\"a\": 1}')")
    assert drain() == 1

    assert values(consume("src-events")) == [{"p": {"a": 1}}], values(consume("src-events"))
