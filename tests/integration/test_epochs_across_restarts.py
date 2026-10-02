"""Leader epochs across a crash and a timeline that goes backwards: SQL produce appends
only under the epoch the broker takes after a crash, and an upgraded cluster restarting
at timeline 1 does not take epochs the old cluster already used."""

import subprocess
import time

import pytest

from conftest import sql

TOPIC = "epochs-restart"


def compose(*args, timeout=300):
    return subprocess.run(["docker", "compose", *args], check=True, timeout=timeout,
                          capture_output=True, text=True).stdout


def retry_sql(query, deadline_s=120):
    deadline = time.time() + deadline_s
    while True:
        try:
            return sql(query)
        except RuntimeError:
            if time.time() > deadline:
                raise
            time.sleep(0.2)


def epoch_of(topic):
    return int(retry_sql(f"""SELECT p.leader_epoch FROM kafgres_partitions p
                               JOIN kafgres_topics t USING (topic_id)
                              WHERE t.name = '{topic}' AND p.partition = 0"""))


def wait_for_new_epoch(topic, before):
    deadline = time.time() + 120
    while time.time() < deadline:
        if epoch_of(topic) > before:
            return
        time.sleep(1)
    raise AssertionError("the broker did not take a new epoch after the restart")


@pytest.fixture
def topic():
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")
    sql(f"SELECT kafgres_create_topic('{TOPIC}', 1)")
    yield TOPIC
    retry_sql(f"SELECT kafgres_drop_topic('{TOPIC}')")


def test_sql_produce_after_a_crash_waits_for_the_new_epoch(topic):
    """A crash resets shared memory. A kafgres_produce() racing the broker's start must not
    append under the old epoch: the new one would then start past a batch stamped with the
    one before it."""
    if sql("SHOW kafgres.storage_engine") != "segment":
        pytest.skip("kafgres_produce() is the segment engine's")
    sql(f"SELECT kafgres_produce('{topic}', NULL, 'before')")
    before = epoch_of(topic)
    pid = sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'kafgres_broker'")
    assert pid, "no broker to kill"
    compose("exec", "-T", "postgres", "sh", "-c", f"kill -9 {pid}")

    base = int(retry_sql(f"SELECT kafgres_produce('{topic}', NULL, 'after')"))
    wait_for_new_epoch(topic, before)
    start = int(sql(f"""SELECT e.start_offset FROM kafgres_leader_epochs e
                         JOIN kafgres_topics t USING (topic_id)
                        WHERE t.name = '{topic}' AND e.partition = 0
                          AND e.leader_epoch = {epoch_of(topic)}"""))
    assert base >= start, f"produced at {base} before the new epoch's start {start}"


def test_a_timeline_going_backwards_does_not_reuse_epochs(topic):
    """pg_upgrade starts the new cluster at timeline 1. Simulated by recording a later
    timeline than the WAL's: the restart must take epochs above every one that timeline
    could have taken."""
    wal = int(sql("SELECT (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))"
                  "::bit(32)::int)"))
    bias = int(sql("SELECT bias FROM kafgres_timeline"))
    sql(f"UPDATE kafgres_timeline SET last_timeline = {wal + 3}")
    before = epoch_of(topic)
    try:
        compose("restart", "postgres")
        wait_for_new_epoch(topic, before)

        # The old cluster's epochs reach the high half wal + bias + 2; one past it is next.
        epoch = epoch_of(topic)
        assert epoch >> 16 == wal + bias + 3, f"epoch {epoch} does not clear the old timeline"
        row = sql("SELECT last_timeline || ',' || bias FROM kafgres_timeline")
        assert row == f"{wal},{bias + 4}", f"kafgres_timeline is {row}"
    finally:
        # Put the bias back, or later tests' new topics start at a higher timeline's epochs.
        retry_sql(f"UPDATE kafgres_timeline SET last_timeline = {wal}, bias = {bias}")


def test_a_first_start_after_an_upgrade_clears_the_old_timeline(topic):
    """With no kafgres_timeline row the start may be the first after pg_upgrade, the old
    cluster's epochs from a later timeline than the WAL's. It must take the one after."""
    wal = int(sql("SELECT (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))"
                  "::bit(32)::int)"))
    row = sql("SELECT last_timeline || ',' || bias FROM kafgres_timeline")
    # Above every epoch already taken, so this partition's decides the timeline.
    highest = int(sql("SELECT max(leader_epoch) FROM kafgres_partitions"))
    old = (max(highest >> 16, wal) + 1) * 65536 + 5
    sql("DELETE FROM kafgres_timeline")
    sql(f"""UPDATE kafgres_partitions SET leader_epoch = {old}
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')""")
    try:
        compose("restart", "postgres")
        wait_for_new_epoch(topic, old)
        epoch = epoch_of(topic)
        assert epoch >> 16 == (old >> 16) + 1, f"epoch {epoch} shares a timeline with the old {old}"
    finally:
        last, bias = row.split(",")
        retry_sql(f"UPDATE kafgres_timeline SET last_timeline = {last}, bias = {bias}")
