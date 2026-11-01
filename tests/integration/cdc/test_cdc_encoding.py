"""CDC on a LATIN1 database: a value reaches the topic as the same characters in UTF-8.

The broker serves one database, so this module points `kafgres.database` at a LATIN1 one,
restarts, and points it back afterwards. Table engine only: segment partition directories
are named by topic id, which is per database, so the two databases would share them.
"""

import json
import subprocess
import time

import pytest
from support.docker import REPO, client_network, compose

DB = "kafgres_latin1"


def run(q, db="postgres", timeout=60):
    return compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", db,
                   "-v", "ON_ERROR_STOP=1", "-tAc", q, timeout=timeout)


def ok(q, db="postgres"):
    out = run(q, db)
    assert out.returncode == 0, out.stderr[-500:]
    return out.stdout.strip()


def serve(db):
    """Restart with the broker on `db` and wait until it has created its tables there."""
    ok(f"ALTER SYSTEM SET kafgres.database = '{db}'")
    compose("restart", "postgres", timeout=300, check=True)
    deadline = time.time() + 120
    while time.time() < deadline:
        up = run("SELECT count(*) FROM pg_stat_activity "
                 f"WHERE backend_type = 'kafgres_broker' AND datname = '{db}'")
        tables = run("SELECT to_regclass('kafgres_topics') IS NOT NULL", db)
        if up.stdout.strip() == "1" and tables.stdout.strip() == "t":
            return
        time.sleep(2)
    raise AssertionError(f"the broker did not come up on {db}")


@pytest.fixture(scope="module")
def latin1():
    if ok("SHOW kafgres.storage_engine") != "table":
        pytest.skip("segment partition directories are named by per-database topic ids")
    run(f"DROP DATABASE IF EXISTS {DB}")
    ok(f"CREATE DATABASE {DB} ENCODING 'LATIN1' TEMPLATE template0 "
       "LC_COLLATE 'C' LC_CTYPE 'C'")
    ok("CREATE EXTENSION kafgres", DB)
    # The slot name is cluster-wide.
    ok("SELECT kafgres_cdc_drop_slot()")
    ok("ALTER SYSTEM SET kafgres.cdc_interval_ms = 0")
    try:
        serve(DB)
        yield
    finally:
        run("SELECT kafgres_cdc_drop_slot()", DB)
        run("ALTER SYSTEM SET kafgres.cdc_interval_ms = 1000")
        serve("postgres")
        run(f"DROP DATABASE IF EXISTS {DB}")


def consume(topic, timeout=25):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", client_network(), "kafgres-clients",
         "kcat", "-b", "127.0.0.1:9092", "-C", "-t", topic, "-o", "beginning", "-e",
         "-f", "%k\t%s\n"],
        capture_output=True, timeout=timeout, cwd=REPO,
    )
    return [tuple(line.split(b"\t", 1)) for line in out.stdout.splitlines()
            if b"\t" in line and not line.startswith(b"%")]


def test_a_latin1_value_reaches_the_topic_as_utf8(latin1):
    assert ok("SHOW server_encoding", DB) == "LATIN1"
    ok("CREATE TABLE enc_src (id int primary key, note text)", DB)
    ok("SELECT kafgres_create_topic('enc-events', 1)", DB)
    ok("SELECT kafgres_add_mapping('enc', 'public.enc_src', 'enc-events', "
       "$$jsonb_build_object('id', new.id, 'note', new.note)$$, $$new.note$$, NULL)", DB)
    assert ok("SELECT kafgres_cdc_create_slot()", DB) == "t"

    # chr(233) is é in LATIN1; spelled so, the statement is ASCII in any client encoding.
    ok("INSERT INTO enc_src VALUES (1, 'caf' || chr(233))", DB)
    assert ok("SELECT kafgres_cdc_drain()", DB) == "1"
    assert ok("SELECT count(*) FROM kafgres_cdc_errors", DB) == "0"

    rows = consume("enc-events")
    assert len(rows) == 1, rows
    key, value = rows[0]
    assert key == "café".encode(), key
    assert json.loads(value.decode()) == {"id": 1, "note": "café"}, value


def test_text_read_from_a_mapping_is_written_back_in_the_server_encoding(latin1):
    """Text read as UTF-8 goes back as LATIN1, or the position never lands on its row."""
    ok("CREATE TABLE enc_back (id int primary key, note text)", DB)
    ok("SELECT kafgres_create_topic('enc-back', 1)", DB)
    name = "'caf' || chr(233)"
    # Inserted directly: the SQL function refuses non-ASCII text on a non-UTF-8 database.
    ok(f"INSERT INTO kafgres_cdc_mappings (mapping_name, source_table, topic, key_expr, value_expr) "
       f"VALUES ({name}, 'public.enc_back', 'enc-back', $$new.id::text$$, "
       "$$jsonb_build_object('id', 1 / (new.id - 2))$$)", DB)
    assert ok("SELECT kafgres_cdc_create_slot()", DB) in ("t", "f")

    ok("INSERT INTO enc_back VALUES (1, 'a')", DB)
    ok("INSERT INTO enc_back VALUES (2, 'b')", DB)
    ok("SELECT kafgres_cdc_drain()", DB)

    assert ok(f"SELECT delivered_lsn IS NOT NULL FROM kafgres_cdc_mappings "
              f"WHERE mapping_name = {name}", DB) == "t", "the position was not recorded"
    assert ok(f"SELECT count(*) FROM kafgres_cdc_errors WHERE mapping_name = {name}", DB) == "1"


def test_a_non_ascii_expression_is_refused_rather_than_rendered_as_other_characters(latin1):
    """Its UTF-8 bytes would be read as LATIN1, so the value would come out as mojibake."""
    ok("CREATE TABLE enc_expr (id int primary key)", DB)
    ok("SELECT kafgres_create_topic('enc-expr', 1)", DB)
    # The literal é arrives as UTF-8 and is stored as LATIN1.
    ok("SET client_encoding = 'UTF8'; "
       "INSERT INTO kafgres_cdc_mappings (mapping_name, source_table, topic, key_expr, value_expr) "
       "VALUES ('expr', 'public.enc_expr', 'enc-expr', $$new.id::text$$, "
       "$$jsonb_build_object('id', new.id, 'tag', 'é')$$)", DB)
    assert ok("SELECT kafgres_cdc_create_slot()", DB) in ("t", "f")
    ok("INSERT INTO enc_expr VALUES (1)", DB)
    assert ok("SELECT kafgres_cdc_drain()", DB) == "0"
    assert consume("enc-expr") == []
    assert "not UTF-8" in ok("SELECT error FROM kafgres_cdc_errors WHERE mapping_name = 'expr'", DB)
