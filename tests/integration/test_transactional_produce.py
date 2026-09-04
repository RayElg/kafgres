"""Transactional produce: the property this project exists for.

A produce that happens **inside the same transaction as a business write**, which Kafka
structurally cannot offer because the commit decision and the record live in different
systems. No Kafka client can express that, so it is asserted here against a real
consumer, both ways round.

The segment engine only. The table engine refuses rather than doing it slowly: its
offset assignment is a row lock a SQL caller would hold for its own transaction's
lifetime, serialising the partition behind application logic.
"""

import os
import subprocess

import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def compose(*args, timeout=180, check=False):
    out = subprocess.run(["docker", "compose", *args], capture_output=True,
                         text=True, timeout=timeout, cwd=REPO)
    if check and out.returncode != 0:
        raise RuntimeError(f"docker compose {args}: {out.stderr.strip()}")
    return out

def psql(sql_text, timeout=60):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-tAc", sql_text],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    return out

def sql(query, timeout=60):
    return psql(query, timeout).stdout.strip()

def script(body, timeout=90):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-q"],
        input=body, capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )

class ConsumerStuck(AssertionError):
    """The consumer never reached the end of the partition."""

def consume(topic, isolation="read_committed", timeout=60):
    """Raises `ConsumerStuck` if the consumer cannot reach the end of the partition.

    A hang and an empty log are *different results* and must not collapse into one.
    Returning `{}` for a hang makes "a rolled-back produce is not consumable" pass on a
    broker that has wedged the consumer instead — the assertion would be satisfied by the
    worst possible behaviour.
    """
    try:
        return _consume(topic, isolation, timeout)
    except subprocess.TimeoutExpired:
        raise ConsumerStuck(
            f"consumer did not reach the end of {topic} in {timeout}s at "
            f"isolation.level={isolation}"
        ) from None

def _consume(topic, isolation, timeout):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-C", "-e", "-q", "-o", "beginning",
         "-X", f"isolation.level={isolation}", "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=timeout,
    )
    got = {}
    for line in out.stdout.splitlines():
        if "\t" in line:
            off, value = line.split("\t", 1)
            got[int(off)] = value
    return got

pytestmark = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "segment",
    reason="engine B only: ALTER SYSTEM SET kafgres.storage_engine='segment'",
)

def wait_ready(timeout_s=120):
    import time
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if sql("SELECT count(*) FROM pg_stat_activity "
               "WHERE backend_type='kafgres_broker'") == "1":
            return True
        time.sleep(2)
    return False

@pytest.fixture
def topic():
    name = "txn-produce"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    sql("CREATE TABLE IF NOT EXISTS txn_orders (id int primary key, total numeric)")
    sql("DELETE FROM txn_orders")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql("DROP TABLE IF EXISTS txn_orders")

def test_rollback_publishes_nothing(topic):
    """The half that matters: getting this wrong is worse than not having the feature.

    The application is told the write did not happen while a consumer acts on it anyway
    — the dual-write inconsistency the whole design exists to remove.

    Note what it does *not* assert — that the broker withheld the record. The record
    was delivered and the client dropped it using the aborted-transaction list, which
    is the only thing the protocol can express.
    """
    out = script(f"""
        BEGIN;
          INSERT INTO txn_orders VALUES (1, 42);
          SELECT kafgres_produce('{topic}', 'k1', '{{"id":1}}');
        ROLLBACK;
    """)
    assert out.returncode == 0, out.stderr

    assert sql("SELECT count(*) FROM txn_orders") == "0", "the business write rolled back"
    assert consume(topic, timeout=30) == {}, "a rolled-back produce must not be consumable"

def test_rollback_leaves_the_bytes_in_the_log(topic):
    """And they are visible to `read_uncommitted`, which is not a leak but the design.

    Kafka does exactly this: an aborted transactional produce leaves its data physically
    in the log, and the consumer is told to skip it rather than the broker rewriting
    history. Asserting it pins the behaviour — if these bytes ever *stopped* being there,
    it would mean something is rewriting the log, which breaks the append-only property
    every recovery path depends on.
    """
    script(f"""
        BEGIN;
          SELECT kafgres_produce('{topic}', 'k1', '{{"id":1}}');
        ROLLBACK;
    """)
    assert consume(topic, "read_uncommitted") == {0: '{"id":1}'}

def test_commit_publishes_atomically_with_the_row(topic):
    out = script(f"""
        BEGIN;
          INSERT INTO txn_orders VALUES (2, 99);
          SELECT kafgres_produce('{topic}', 'k2', '{{"id":2}}');
        COMMIT;
    """)
    assert out.returncode == 0, out.stderr

    assert sql("SELECT count(*) FROM txn_orders") == "1"
    got = consume(topic)
    assert list(got.values()) == ['{"id":2}'], got

def test_a_rolled_back_offset_is_a_permanent_hole(topic):
    """Offsets are never reused, exactly as an aborted Kafka transaction leaves a gap.

    Reusing the offset would be the tempting "tidy" behaviour and is wrong: a consumer
    that read the uncommitted record at that offset would later see a *different* record
    with the same offset, which no client expects and none can detect.
    """
    script(f"BEGIN; SELECT kafgres_produce('{topic}', 'a', 'first'); ROLLBACK;")
    script(f"BEGIN; SELECT kafgres_produce('{topic}', 'b', 'second'); COMMIT;")

    got = consume(topic)
    assert got.get(1) == "second", (
        f"the committed record must occupy offset 1, not reuse the rolled-back 0: {got}"
    )

def test_wire_produced_records_are_never_filtered(topic):
    """Ordinary traffic must be untouched by any of this.

    Kept even though no filter is active right now: it is the guard that a future
    orphan-skipping implementation must not break. Ordinary producers write no markers, so
    a filter keyed on "has no marker" would make every wire-produced record vanish under
    `read_committed` — a total outage for normal traffic caused by a feature nobody in
    that path is using.
    """
    subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-P"],
        input="plain\n", capture_output=True, text=True, timeout=60,
    )
    script(f"BEGIN; SELECT kafgres_produce('{topic}', 'k', 'txn'); COMMIT;")

    got = consume(topic)
    assert sorted(got.values()) == ["plain", "txn"], got

def test_an_in_flight_produce_is_not_visible_until_it_commits(topic):
    """The Last Stable Offset, from a consumer's point of view.

    A separate session holds a transaction open; the record must stay invisible while it
    does, then appear on commit. Without the LSO gate a `read_committed` consumer would
    see a record that might still roll back.
    """
    import time

    holder = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-q"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        text=True, cwd=REPO,
    )
    assert holder.stdin is not None
    holder.stdin.write(f"BEGIN;\nSELECT kafgres_produce('{topic}', 'k', 'inflight');\n")
    holder.stdin.flush()
    time.sleep(3)

    try:
        assert consume(topic) == {}, "an uncommitted produce must not be visible"
        assert consume(topic, "read_uncommitted") != {}, (
            "the bytes should already be in the log — if not, this test is passing "
            "because nothing was produced rather than because the LSO held it back"
        )
    finally:
        holder.stdin.write("COMMIT;\n")
        holder.stdin.flush()
        holder.stdin.close()
        holder.wait(timeout=60)

    got = consume(topic)
    assert list(got.values()) == ["inflight"], got

def test_an_uncommitted_produce_does_not_block_the_partition(topic):
    """A held-open produce must not stop *other* producers.

    The append lock is released before the marker is written, so an application holding a
    transaction open cannot wedge the partition — which was the whole objection to doing
    this on the table engine, where the offset-assignment row lock is held to commit.
    """
    import time

    holder = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-q"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        text=True, cwd=REPO,
    )
    assert holder.stdin is not None
    holder.stdin.write(f"BEGIN;\nSELECT kafgres_produce('{topic}', 'held', 'held');\n")
    holder.stdin.flush()
    time.sleep(2)
    try:
        wire = subprocess.run(
            ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
             "kcat", "-b", BROKER, "-t", topic, "-P"],
            input="not-blocked\n", capture_output=True, text=True, timeout=45,
        )
        assert wire.returncode == 0, (
            "a wire produce blocked behind an open SQL transaction: "
            f"{wire.stderr.strip()}"
        )
    finally:
        holder.stdin.write("ROLLBACK;\n")
        holder.stdin.flush()
        holder.stdin.close()
        holder.wait(timeout=60)

def test_a_marker_whose_payload_did_not_survive_is_dropped_loudly(topic):
    """The two-durability-domain seam.

    A marker row commits in Postgres; the payload it points at lives in a segment file.
    A crash between the two leaves a *committed* marker pointing past a truncated log —
    Postgres says the record exists and the log does not have it. Simulated here by
    truncating the segment while the broker is down, which is exactly the state a torn
    tail leaves behind.

    Two things must happen, and the second matters as much as the first. The marker is
    dropped, because keeping it gates the LSO behind an offset no record will ever
    occupy and stalls every read_committed consumer on the partition forever. And it is
    logged with the offsets, because a committed transaction was told its record existed
    and it does not — that is not something an operator should have to infer.
    """
    script(f"BEGIN; SELECT kafgres_produce('{topic}', 'k', 'will-be-lost'); COMMIT;")
    topic_id = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
    mine = f"SELECT count(*) FROM kafgres_markers WHERE topic_id = {topic_id}"
    assert sql(mine) == "1", "no marker was written for this topic"

    seg = f"/var/lib/postgresql/data/kafgres/{topic_id}/0/00000000000000000000.log"

    out = compose("exec", "-T", "postgres", "truncate", "-s", "0", seg)
    assert out.returncode == 0, f"could not truncate the segment: {out.stderr.strip()}"
    compose("restart", "postgres", check=True, timeout=300)
    assert wait_ready(), "broker did not come back"

    assert sql(mine) == "0", (
        "a marker pointing past the truncated log was kept; the partition's LSO is now "
        "gated behind an offset that will never be filled"
    )

    logs = subprocess.run(
        ["docker", "compose", "logs", "postgres"],
        capture_output=True, text=True, timeout=120, cwd=REPO,
    ).stdout
    assert "the log does not contain their records" in logs, (
        "the loss was not logged — an operator has no way to know a committed "
        "transaction's record is gone"
    )
    assert "restored from a segment archive" in logs, logs[-2000:]
