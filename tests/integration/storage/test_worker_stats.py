"""The broker's row changes must reach Postgres's statistics while it runs.

A background worker never runs `PostgresMain`'s idle loop, where an ordinary backend
flushes its pending table statistics, so it must flush them itself or they surface only
on exit. Autovacuum decides what to vacuum from those statistics.
"""

import struct
import time

import pytest

from conftest import sql
from support.recordbatch import parse_produce_v3, produce_v3, record_batch

PRODUCE = 0

# `dbtx::report_stats` flushes at most once per second while transactions run, and
# forces a flush at least every 10 s regardless; 30 s is slack for a loaded CI box.
FLUSH_DEADLINE_S = 30


@pytest.fixture
def topic():
    name = "stats-visible"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


def window_inserts():
    """Inserts into the idempotent-producer window as the stats system has been told.

    Each `sql` call is its own session, so this never reads a cached snapshot.
    """
    return int(sql(
        "SELECT coalesce((SELECT n_tup_ins FROM pg_stat_user_tables"
        " WHERE relname = 'kafgres_producer_batches'), 0)"
    ).strip())


def test_broker_table_changes_reach_the_stats_system_while_it_runs(topic, conn):
    before = window_inserts()

    batches = 20
    for seq in range(batches):
        batch = record_batch([f"s{seq}".encode()], producer_id=8800,
                             producer_epoch=0, base_sequence=seq)
        header = struct.pack(">hhi", PRODUCE, 3, 9000 + seq) + struct.pack(">h", 6) + b"pytest"
        frame = header + produce_v3(topic, 0, batch)
        conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
        _, err, _ = parse_produce_v3(conn.recv())
        assert err == 0, f"batch {seq} failed with {err}"

    deadline = time.time() + FLUSH_DEADLINE_S
    seen = window_inserts() - before
    while seen < batches and time.time() < deadline:
        time.sleep(0.5)
        seen = window_inserts() - before

    assert seen >= batches, (
        f"{batches} idempotent batches were acknowledged but the stats system saw {seen} "
        f"window inserts after {FLUSH_DEADLINE_S}s. If the broker is not flushing its "
        "table statistics, autovacuum cannot see what it churns and those tables bloat "
        "for as long as the worker runs."
    )
