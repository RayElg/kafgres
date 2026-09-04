"""The CDC initial snapshot.

A mapping that only tails the WAL can never tell a consumer about a row that has not
changed since the mapping was defined, so a topic bootstrapped from one starts empty and
stays wrong for every quiet key. The snapshot is rendered through the *same* path as a
live change, with `op = 'R'`: same expressions, same types, same key.
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
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "kafgres-clients",
         "kcat", "-b", "127.0.0.1:9092", "-C", "-t", topic, "-o", "beginning", "-e",
         "-f", "%k\t%s\n"],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    rows = []
    for line in out.stdout.splitlines():
        if "\t" not in line:
            continue
        k, v = line.split("\t", 1)
        rows.append((k, v))
    return rows

@pytest.fixture(scope="module", autouse=True)
def hand_driven():
    sql("SELECT kafgres_cdc_drop_slot()")
    set_guc("kafgres.cdc_interval_ms", "0")
    time.sleep(2)
    yield
    set_guc("kafgres.cdc_interval_ms", "1000")
    set_guc("kafgres.cdc_snapshot_batch_rows", "1000")

@pytest.fixture(autouse=True)
def fixtures(hand_driven):
    sql("SELECT kafgres_cdc_drop_slot()")
    sql("DELETE FROM kafgres_cdc_mappings")
    sql("DELETE FROM kafgres_cdc_errors")
    sql("DROP TABLE IF EXISTS snap_orders, snap_parts, snap_nokey, snap_other CASCADE")
    sql("SELECT kafgres_drop_topic('snap-events')")
    sql("SELECT kafgres_create_topic('snap-events', 1)")
    yield
    sql("SELECT kafgres_cdc_drop_slot()")
    sql("DELETE FROM kafgres_cdc_mappings")
    sql("DROP TABLE IF EXISTS snap_orders, snap_parts, snap_nokey, snap_other CASCADE")
    sql("SELECT kafgres_drop_topic('snap-events')")

def add_mapping(source, value_expr, key_expr="NULL", name="snap"):
    def lit(x):
        return "NULL" if x == "NULL" else f"$${x}$$"
    out = psql(
        f"SELECT kafgres_add_mapping('{name}', '{source}', 'snap-events', "
        f"$${value_expr}$$, {lit(key_expr)}, NULL)"
    )
    assert out.returncode == 0, out.stderr[-600:]

def snapshot(name="snap"):
    out = psql(f"SELECT kafgres_snapshot_mapping('{name}')")
    assert out.returncode == 0, out.stderr[-600:]
    out = psql("SELECT kafgres_cdc_snapshot()")
    assert out.returncode == 0, out.stderr[-600:]
    return int(out.stdout.strip() or 0)

def test_a_snapshot_emits_every_existing_row():
    """The point of the feature: rows that predate the mapping still reach the topic."""
    sql("CREATE TABLE snap_orders (id int primary key, total numeric, status text)")
    sql("INSERT INTO snap_orders VALUES (10,99.5,'placed'),(11,5,'draft'),(12,7,'placed')")
    add_mapping("public.snap_orders", "to_jsonb(new)", "new.id::text")

    assert snapshot() == 3
    got = {k: json.loads(v) for k, v in consume("snap-events")}
    assert set(got) == {"10", "11", "12"}, got
    assert got["10"]["total"] == 99.5, got["10"]
    assert got["10"]["status"] == "placed", got["10"]

def test_pagination_crosses_batches_without_skipping_or_repeating():
    """Keyset pagination over a key space with gaps: contiguous ids cannot distinguish
    a cursor that resumes at `last + 1` from one that resumes strictly after `last`,
    and both pass on 1..N."""
    set_guc("kafgres.cdc_snapshot_batch_rows", "2")
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    sql("INSERT INTO snap_orders VALUES (1,1),(2,2),(20,20),(99,99),(100,100)")
    add_mapping("public.snap_orders", "to_jsonb(new)", "new.id::text")

    assert snapshot() == 5
    keys = [k for k, _ in consume("snap-events")]
    assert sorted(int(k) for k in keys) == [1, 2, 20, 99, 100], keys
    assert len(keys) == len(set(keys)), f"a row was emitted twice: {keys}"

def test_a_composite_key_uses_a_row_comparison():
    """`(a, b) > (x, y)`, not `a > x AND b > y`: the two agree unless the first key
    column repeats, and with the naive conjunction resuming after (1,'b') drops
    (1,'c') *and* (2,'a')."""
    set_guc("kafgres.cdc_snapshot_batch_rows", "2")
    sql("CREATE TABLE snap_parts (a int, b text, v int, primary key (a, b))")
    sql("INSERT INTO snap_parts VALUES (1,'a',1),(1,'b',2),(1,'c',3),(2,'a',4),(2,'b',5)")
    add_mapping("public.snap_parts", "to_jsonb(new)", "new.a::text || '/' || new.b")

    assert snapshot() == 5
    keys = sorted(k for k, _ in consume("snap-events"))
    assert keys == ["1/a", "1/b", "1/c", "2/a", "2/b"], keys

def test_snapshot_rows_are_distinguishable_from_changes():
    """`op` is in scope for a mapping, and a snapshot row carries `R`: without it a
    consumer cannot tell a bootstrap read from a real insert."""
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    sql("INSERT INTO snap_orders VALUES (1,1)")
    add_mapping("public.snap_orders",
                "jsonb_build_object('op', op, 'id', new.id)", "new.id::text")
    assert snapshot() == 1
    assert json.loads(consume("snap-events")[0][1])["op"] == "R"

def test_a_snapshot_is_followed_by_the_stream():
    """Bootstrap then tail, the whole workflow.

    The drain is deliberately run *after* the snapshot: a change drained alongside one
    can reach the topic before the older snapshot row for the same key and be
    overwritten by it."""
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    sql("INSERT INTO snap_orders VALUES (1,10)")
    add_mapping("public.snap_orders",
                "jsonb_build_object('op', op, 'total', new.total)", "new.id::text")
    sql("SELECT kafgres_cdc_create_slot()")
    assert snapshot() == 1

    sql("UPDATE snap_orders SET total = 20 WHERE id = 1")
    sql("SELECT kafgres_cdc_drain()")

    got = [json.loads(v) for _, v in consume("snap-events")]
    assert [g["op"] for g in got] == ["R", "U"], got
    assert [float(g["total"]) for g in got] == [10.0, 20.0], got

def test_a_table_without_a_primary_key_is_refused_at_the_prompt():
    """Refused where the operator can see it, not in a worker log a tick later: a
    snapshot needs a stable total order, and `ctid` is not one — an UPDATE moves a
    row, so a snapshot ordered by it can read a row twice or never at all."""
    sql("CREATE TABLE snap_nokey (id int, total numeric)")
    sql("INSERT INTO snap_nokey VALUES (1,1)")
    add_mapping("public.snap_nokey", "to_jsonb(new)")

    out = psql("SELECT kafgres_snapshot_mapping('snap')")
    assert out.returncode != 0, out.stdout
    assert "primary key" in out.stderr, out.stderr[-400:]
    assert sql("SELECT count(*) FROM kafgres_cdc_snapshots()") == "0"

def test_progress_is_reportable_while_it_runs():
    """An operator mid-snapshot needs to know how far it has got and what it is
    costing: a snapshot holds the drain, and a held drain pins WAL."""
    set_guc("kafgres.cdc_snapshot_batch_rows", "2")
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    sql("INSERT INTO snap_orders SELECT g, g FROM generate_series(1,10) g")
    add_mapping("public.snap_orders", "to_jsonb(new)", "new.id::text")
    sql("SELECT kafgres_snapshot_mapping('snap')")

    assert sql("SELECT kafgres_cdc_snapshot(1)") == "2"
    state, rows = sql("SELECT state || ' ' || rows_emitted FROM kafgres_cdc_snapshots()").split()
    assert state == "running", state
    assert rows == "2", rows

    assert sql("SELECT kafgres_cdc_snapshot()") == "8"
    state, rows = sql("SELECT state || ' ' || rows_emitted FROM kafgres_cdc_snapshots()").split()
    assert state == "done", state
    assert rows == "10", rows

def test_the_worker_holds_the_drain_until_the_snapshot_finishes():
    """The ordering rule, tested against the worker.

    A snapshot row carries the state of a key when it was read, and a change drained
    beside it may be newer; interleaved, the topic goes stale for that key silently.

    Asserted positionally, not by timing: read the log once at the end and check the
    order. Batch size 1 over 60 rows spans many ticks; with one batch there is no
    window to interleave in.
    """
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    sql("INSERT INTO snap_orders SELECT g, g FROM generate_series(1,60) g")
    add_mapping("public.snap_orders",
                "jsonb_build_object('op', op, 'total', new.total)", "new.id::text")
    sql("SELECT kafgres_cdc_create_slot()")
    sql("SELECT kafgres_snapshot_mapping('snap')")

    sql("UPDATE snap_orders SET total = 999 WHERE id = 1")

    set_guc("kafgres.cdc_snapshot_batch_rows", "1")
    set_guc("kafgres.cdc_interval_ms", "50")
    try:
        deadline = time.time() + 180
        while time.time() < deadline:
            if sql("SELECT state FROM kafgres_cdc_snapshots()") == "done":
                break
            time.sleep(0.5)
        assert sql("SELECT state FROM kafgres_cdc_snapshots()") == "done", "snapshot never finished"
        assert sql("SELECT rows_emitted FROM kafgres_cdc_snapshots()") == "60"

        deadline = time.time() + 90
        ops = []
        while time.time() < deadline:
            ops = [json.loads(v)["op"] for _, v in consume("snap-events")]
            if "U" in ops:
                break
            time.sleep(0.5)
        assert "U" in ops, f"the change held behind the snapshot was never delivered: {ops}"

        first_change = ops.index("U")
        assert set(ops[:first_change]) == {"R"}, ops
        assert "R" not in ops[first_change:], ops
        assert ops.count("R") == 60, ops
    finally:
        set_guc("kafgres.cdc_interval_ms", "0")
        set_guc("kafgres.cdc_snapshot_batch_rows", "1000")
        time.sleep(1)

def test_a_change_carries_its_transactions_identity():
    """`lsn`, `xid` and `commit_ts` in scope — what a Debezium envelope is made of, and
    what a consumer needs to order across tables, deduplicate after a restart, and
    group the changes that shared a commit. Two rows written in **one** transaction
    must report the same `xid`; two rows written separately must not."""
    sql("DROP TABLE IF EXISTS snap_orders CASCADE")
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    add_mapping("public.snap_orders",
                "jsonb_build_object('op', op, 'lsn', lsn::text, 'xid', xid, "
                "'ts_ms', extract(epoch from commit_ts) * 1000)",
                "new.id::text")
    sql("SELECT kafgres_cdc_create_slot()")

    sql("BEGIN; INSERT INTO snap_orders VALUES (1,1); INSERT INTO snap_orders VALUES (2,2); COMMIT")
    sql("INSERT INTO snap_orders VALUES (3,3)")
    sql("SELECT kafgres_cdc_drain()")

    got = {k: json.loads(v) for k, v in consume("snap-events")}
    assert set(got) == {"1", "2", "3"}, got
    assert got["1"]["xid"] == got["2"]["xid"], (
        f"two changes from one commit report different transaction ids: {got}"
    )
    assert got["3"]["xid"] != got["1"]["xid"], (
        "a separate transaction reports the same id, so xid groups nothing"
    )
    assert got["1"]["lsn"] != got["3"]["lsn"], got
    assert got["1"]["ts_ms"] > 1_600_000_000_000, got["1"]
    assert got["1"]["ts_ms"] == got["2"]["ts_ms"], "one commit, two commit timestamps"

def test_a_snapshot_row_does_not_invent_a_transaction():
    """`xid` and `commit_ts` are NULL on a snapshot row, not zero and not `now()`: a
    snapshot row is a read, not a change, and zeroing them would put every row into
    one fictitious transaction 0."""
    sql("DROP TABLE IF EXISTS snap_orders CASCADE")
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    sql("INSERT INTO snap_orders VALUES (1,1),(2,2)")
    add_mapping("public.snap_orders",
                "jsonb_build_object('op', op, 'xid', xid, 'ts', commit_ts)", "new.id::text")
    assert snapshot() == 2
    for _, v in consume("snap-events"):
        row = json.loads(v)
        assert row["op"] == "R", row
        assert row["xid"] is None, f"a snapshot row claims a transaction id: {row}"
        assert row["ts"] is None, f"a snapshot row claims a commit time: {row}"

def test_a_mapping_naming_the_metadata_compiles_at_definition_time():
    """The compile check has to know the same names the renderer binds, or a mapping
    using `xid` is accepted and then fails on every change in a worker log."""
    sql("DROP TABLE IF EXISTS snap_orders CASCADE")
    sql("CREATE TABLE snap_orders (id int primary key, total numeric)")
    out = psql("SELECT kafgres_add_mapping('snap', 'public.snap_orders', 'snap-events', "
               "$$jsonb_build_object('x', xid, 't', commit_ts, 'l', lsn::text)$$, NULL, NULL)")
    assert out.returncode == 0, out.stderr[-500:]
    bad = psql("SELECT kafgres_add_mapping('snap2', 'public.snap_orders', 'snap-events', "
               "$$jsonb_build_object('n', no_such_binding)$$, NULL, NULL)")
    assert bad.returncode != 0, bad.stdout

def test_a_transaction_mapping_summarises_a_commit():
    """`kafgres.transaction` — the transaction topic. `xid` on every change lets a
    consumer group them but not know the group is complete; `event_count` and
    `data_collections` are what tell it when to stop waiting.

    A reserved pseudo-source rather than a new kind of mapping, so the summary travels
    the same drain path — same ordering, filtering, dead-lettering."""
    sql("DROP TABLE IF EXISTS snap_orders, snap_other CASCADE")
    sql("CREATE TABLE snap_orders (id int primary key, v int)")
    sql("CREATE TABLE snap_other (id int primary key, v int)")
    sql("SELECT kafgres_drop_topic('snap-tx')")
    sql("SELECT kafgres_create_topic('snap-tx', 1)")
    add_mapping("public.snap_orders", "jsonb_build_object('id', new.id)", "new.id::text")
    add_mapping("public.snap_other", "jsonb_build_object('id', new.id)", "new.id::text",
                name="snap_other")
    out = psql("SELECT kafgres_add_mapping('snap_tx', 'kafgres.transaction', 'snap-tx', "
               "$$jsonb_build_object('op', op, 'xid', xid, 'events', event_count, "
               "'collections', data_collections)$$, $$xid::text$$, NULL)")
    assert out.returncode == 0, out.stderr[-600:]
    sql("SELECT kafgres_cdc_create_slot()")

    sql("BEGIN; INSERT INTO snap_orders VALUES (1,1),(2,2); "
        "INSERT INTO snap_other VALUES (9,9); COMMIT")
    sql("INSERT INTO snap_orders VALUES (3,3)")
    sql("SELECT kafgres_cdc_drain()")

    got = [json.loads(v) for _, v in consume("snap-tx")]
    assert len(got) == 2, got
    multi, single = got

    assert multi["events"] == 3, multi
    assert multi["collections"] == {"public.snap_orders": 2, "public.snap_other": 1}, multi
    assert single["events"] == 1, single
    assert single["collections"] == {"public.snap_orders": 1}, single

    assert multi["xid"] != single["xid"], got
    assert all(g["op"] == "T" for g in got), got

def test_a_transaction_mapping_cannot_reach_for_a_column():
    """There is no row behind a commit summary: compiled against a shape with no `new`
    and no `old`, so `new.id` is an error the operator sees while typing."""
    sql("SELECT kafgres_drop_topic('snap-tx')")
    sql("SELECT kafgres_create_topic('snap-tx', 1)")
    bad = psql("SELECT kafgres_add_mapping('snap_tx', 'kafgres.transaction', 'snap-tx', "
               "$$jsonb_build_object('id', new.id)$$, NULL, NULL)")
    assert bad.returncode != 0, bad.stdout
    ok = psql("SELECT kafgres_add_mapping('snap_tx', 'kafgres.transaction', 'snap-tx', "
              "$$jsonb_build_object('n', event_count)$$, NULL, NULL)")
    assert ok.returncode == 0, ok.stderr[-500:]
    sql("SELECT kafgres_drop_topic('snap-tx')")

def test_a_transaction_stream_cannot_be_snapshotted():
    """A commit stream has no prior state; falling through to the primary-key lookup
    would fail with a `regclass` error about a relation that does not exist —
    technically correct and useless."""
    sql("SELECT kafgres_drop_topic('snap-tx')")
    sql("SELECT kafgres_create_topic('snap-tx', 1)")
    psql("SELECT kafgres_add_mapping('snap_tx', 'kafgres.transaction', 'snap-tx', "
         "$$jsonb_build_object('n', event_count)$$, NULL, NULL)")
    out = psql("SELECT kafgres_snapshot_mapping('snap_tx')")
    assert out.returncode != 0, out.stdout
    assert "stream of commits" in out.stderr, out.stderr[-400:]
    sql("SELECT kafgres_drop_topic('snap-tx')")
