"""CDC mappings defined in SQL.

A mapping runs *inside* the database, so it can join, filter with real predicates, and
produce `jsonb` that is typed and valid by construction. These tests cover mapping
definition and rendering; `test_cdc_source.py` covers the change source end to end.
"""

import os
import subprocess

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

@pytest.fixture(autouse=True)
def fixtures():
    sql("DROP TABLE IF EXISTS cdc_orders, cdc_customers CASCADE")
    sql("CREATE TABLE cdc_customers (id int primary key, tier text)")
    sql("CREATE TABLE cdc_orders (id int primary key, customer_id int, "
        "total numeric, status text)")
    sql("INSERT INTO cdc_customers VALUES (1,'gold'),(2,'silver')")
    sql("INSERT INTO cdc_orders VALUES (10,1,99.5,'placed'),(11,2,5,'draft')")
    sql("SELECT kafgres_drop_topic('cdc-events')")
    sql("SELECT kafgres_create_topic('cdc-events', 1)")
    sql("DELETE FROM kafgres_cdc_mappings")
    yield
    sql("DELETE FROM kafgres_cdc_mappings")
    sql("DROP TABLE IF EXISTS cdc_orders, cdc_customers CASCADE")

def add_mapping(value_expr, key_expr="NULL", filter_expr="NULL", name="m1"):
    def lit(x):
        return "NULL" if x == "NULL" else f"$${x}$$"
    return psql(
        f"SELECT kafgres_add_mapping('{name}', 'public.cdc_orders', 'cdc-events', "
        f"$${value_expr}$$, {lit(key_expr)}, {lit(filter_expr)})"
    )

def preview(name="m1", predicate="true"):
    out = psql(f"SELECT key, value FROM kafgres_preview_mapping('{name}', '{predicate}')")
    rows = []
    for line in out.stdout.strip().splitlines():
        if "|" in line:
            k, v = line.split("|", 1)
            rows.append((k.strip(), v.strip()))
    return rows

def test_a_mapping_can_join(fixtures):
    """Enrichment by join, in one place."""
    assert add_mapping(
        "jsonb_build_object('order_id', new.id, "
        "'customer', (SELECT jsonb_build_object('tier', c.tier) "
        "FROM cdc_customers c WHERE c.id = new.customer_id))",
        key_expr="new.id::text",
    ).returncode == 0

    rows = preview()
    assert len(rows) == 2, rows
    by_key = dict(rows)
    assert '"tier": "gold"' in by_key["10"], by_key
    assert '"tier": "silver"' in by_key["11"], by_key

def test_the_value_is_typed_not_stringified(fixtures):
    """A number stays a number."""
    add_mapping("jsonb_build_object('total', new.total)", key_expr="new.id::text")
    value = dict(preview())["10"]
    assert '"total": 99.5' in value, f"total was stringified: {value}"
    assert '"total": "99.5"' not in value

def test_preview_op_matches_what_the_drainer_emits(fixtures):
    """The preview binds the same `op` codes the output plugin emits (I/U/D),
    so a preview of an op-switching mapping means what production means."""
    add_mapping(
        "jsonb_build_object('op', op)",
        key_expr="new.id::text",
        filter_expr="op = 'I'",
    )
    rows = preview()
    assert len(rows) == 2, rows
    assert all('"op": "I"' in v for _, v in rows), rows

def test_a_filter_is_a_real_sql_predicate(fixtures):
    add_mapping(
        "jsonb_build_object('id', new.id)",
        key_expr="new.id::text",
        filter_expr="new.status = 'placed'",
    )
    rows = preview()
    assert [k for k, _ in rows] == ["10"], f"the filter did not exclude the draft: {rows}"

def test_a_mapping_that_does_not_compile_is_refused_at_definition(fixtures):
    """Rejected at definition, not per-change later: a broken mapping would otherwise
    either drop the change or stall the stream."""
    out = add_mapping("jsonb_build_object('x', new.no_such_column)")
    assert out.returncode != 0, "a mapping referencing a missing column was accepted"
    assert "no_such_column does not exist" in out.stderr, out.stderr[-400:]
    assert sql("SELECT count(*) FROM kafgres_cdc_mappings") == "0", (
        "the broken mapping was stored anyway"
    )

def test_a_mapping_to_a_nonexistent_topic_is_refused(fixtures):
    out = psql(
        "SELECT kafgres_add_mapping('m2', 'public.cdc_orders', 'no-such-topic', "
        "$$jsonb_build_object('id', new.id)$$, NULL, NULL)"
    )
    assert out.returncode != 0, "a mapping to a topic that does not exist was accepted"
    assert "no such topic" in out.stderr, out.stderr[-300:]

def test_a_source_that_is_not_a_plain_table_name_is_refused(fixtures):
    """The source is interpolated into generated SQL — it cannot be a bind parameter,
    because a table name is not a value — so it is validated rather than escaped."""
    out = psql(
        "SELECT kafgres_add_mapping('m3', 'orders; DROP TABLE cdc_customers', "
        "'cdc-events', $$jsonb_build_object('id', new.id)$$, NULL, NULL)"
    )
    assert out.returncode != 0, "an injected source table name was accepted"
    assert sql("SELECT count(*) FROM cdc_customers") == "2", "the injection ran"
