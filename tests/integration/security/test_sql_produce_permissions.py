"""Who may call what through SQL: grants and `kafgres_acls` for `kafgres_produce()`,
decided before anything reaches the log, and no administrative function for PUBLIC."""

import time

import pytest

from conftest import sql

ROLE = "sqlprod"
TOPIC = "sql-produce-perms"

# Executable by PUBLIC on purpose; everything else the extension defines is not.
PUBLIC_FUNCTIONS = {
    "kafgres_produce", "kafgres_version", "kafgres_kafka_version",
    "kafgres_partition_offsets", "kafgres_archive_status", "kafgres_cdc_status",
    "kafgres_cdc_snapshots", "kafgres_share_state",
}

def as_role(statement):
    """Run `statement` as the test role; returns (ok, output)."""
    try:
        return True, sql(f"SET ROLE {ROLE}; {statement}")
    except RuntimeError as e:
        return False, str(e)

def log_end():
    return int(sql(f"SELECT COALESCE(high_watermark, 0) FROM kafgres_partition_offsets('{TOPIC}')"))

def set_acls(on):
    sql(f"ALTER SYSTEM SET kafgres.acls_enabled = {'on' if on else 'off'}")
    sql("SELECT pg_reload_conf()")
    time.sleep(1.5)

@pytest.fixture
def producer_role():
    if sql("SHOW kafgres.storage_engine") != "segment":
        pytest.skip("kafgres_produce() is the segment engine's")
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")
    sql(f"SELECT kafgres_create_topic('{TOPIC}', 1)")
    sql(f"DROP OWNED BY {ROLE}; DROP ROLE {ROLE}" if sql(
        f"SELECT count(*) FROM pg_roles WHERE rolname = '{ROLE}'") == "1" else "SELECT 1")
    sql(f"CREATE ROLE {ROLE}")
    sql(f"GRANT SELECT ON kafgres_topics, kafgres_partitions TO {ROLE}")
    yield
    set_acls(False)
    sql(f"DELETE FROM kafgres_acls WHERE principal = 'User:{ROLE}'")
    sql(f"DROP OWNED BY {ROLE}; DROP ROLE {ROLE}")
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")

def test_the_administrative_functions_are_not_public():
    """Functions default to EXECUTE for PUBLIC; only the listed ones may keep it."""
    public = sql("""SELECT string_agg(p.proname, ',' ORDER BY p.proname)
                      FROM pg_proc p
                      JOIN pg_depend d ON d.objid = p.oid AND d.classid = 'pg_proc'::regclass
                      JOIN pg_extension e ON e.oid = d.refobjid AND d.deptype = 'e'
                     WHERE e.extname = 'kafgres'
                       AND has_function_privilege('public', p.oid, 'EXECUTE')""").split(",")
    assert set(public) == PUBLIC_FUNCTIONS, (
        f"executable by PUBLIC: {sorted(set(public) - PUBLIC_FUNCTIONS)}; "
        f"missing: {sorted(PUBLIC_FUNCTIONS - set(public))}"
    )

def test_a_refused_produce_writes_nothing_to_the_log(producer_role):
    """A role that may not insert a marker is refused before its record is appended, or
    read_uncommitted consumers would see it."""
    before = log_end()
    ok, out = as_role(f"SELECT kafgres_produce('{TOPIC}', 'k', 'should-not-land')")
    assert not ok, "a role without INSERT on kafgres_markers produced"
    assert "permission denied to produce" in out, out
    assert log_end() == before, "the refused record reached the log anyway"

def test_acls_govern_sql_producers_too(producer_role):
    """`User:<role>` needs WRITE on the topic, as a Kafka client would, and a refusal
    happens before the append."""
    sql(f"GRANT INSERT ON kafgres_markers TO {ROLE}")
    ok, out = as_role(f"SELECT kafgres_produce('{TOPIC}', 'k', 'acls-off')")
    assert ok, f"a fully granted role could not produce with ACLs off: {out}"

    set_acls(True)
    before = log_end()
    ok, out = as_role(f"SELECT kafgres_produce('{TOPIC}', 'k', 'no-rule')")
    assert not ok and "not allowed to WRITE" in out, (
        f"with ACLs on and no rule the answer must be refusal: {out}"
    )
    assert log_end() == before, "the refused record reached the log anyway"

    sql(f"SELECT kafgres_add_acl('User:{ROLE}', 'WRITE', 'TOPIC', '{TOPIC}')")
    ok, out = as_role(f"SELECT kafgres_produce('{TOPIC}', 'k', 'allowed')")
    assert ok, f"an ALLOW WRITE rule did not let the role produce: {out}"

    sql(f"SELECT kafgres_add_acl('User:{ROLE}', 'WRITE', 'TOPIC', '{TOPIC}', 'DENY')")
    time.sleep(1.5)
    ok, out = as_role(f"SELECT kafgres_produce('{TOPIC}', 'k', 'denied')")
    assert not ok and "not allowed to WRITE" in out, f"DENY did not beat ALLOW: {out}"

    # A Postgres superuser is not checked: it can rewrite kafgres_acls regardless.
    sql(f"SELECT kafgres_produce('{TOPIC}', 'k', 'superuser')")

def test_temporary_objects_cannot_stand_in_for_the_extensions_tables(producer_role):
    """`kafgres_produce()` reads `kafgres_acls` as a superuser. Under the caller's search
    path, where `pg_temp` comes first, a temporary view of that name would run the caller's
    code with superuser rights."""
    sql(f"GRANT INSERT ON kafgres_markers TO {ROLE}")
    set_acls(True)
    ok, out = as_role(
        "CREATE FUNCTION pg_temp.escalate() RETURNS SETOF kafgres_acls LANGUAGE plpgsql AS $$ "
        f"BEGIN EXECUTE 'ALTER ROLE {ROLE} SUPERUSER'; RETURN; END $$; "
        "CREATE TEMP VIEW kafgres_acls AS SELECT * FROM pg_temp.escalate(); "
        f"SELECT kafgres_produce('{TOPIC}', 'k', 'shadowed')")
    assert sql(f"SELECT rolsuper FROM pg_roles WHERE rolname = '{ROLE}'") == "f", (
        "a temporary view made the caller a superuser"
    )
    assert not ok and "not allowed to WRITE" in out, (
        f"the real, empty kafgres_acls should have refused the produce: {out}"
    )
