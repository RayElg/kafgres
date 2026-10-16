"""A read that races retention gets the records or `offset out of range`, never a skip.

A read that checked its offset against the log start before retention committed must not
return the first batch that survived. Callers accept gaps, which compaction makes legal, so
the deleted records would be skipped silently. A consumer is owed OFFSET_OUT_OF_RANGE.

One session runs retention and holds it uncommitted while the other reads. The table
engine's read waits for the commit and is out of range; the segment engine unlinks only at
commit, so its read is served.
"""

import subprocess
import threading
import time

import pytest

from conftest import BROKER_HOST, BROKER_PORT, PSQL, sql
from support.docker import CLIENTS

BROKER = f"{BROKER_HOST}:{BROKER_PORT}"
PAD = "p" * 500


def kcat(*args, stdin=None, timeout=120):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )


def fill(topic, key=None):
    """Three batches, of offsets 0-9, 10-19 and 20-24, each in its own segment.

    Each batch is about 5 KB, over `segment_bytes`, so the segment engine rolls before every
    batch after the first. The table engine's segments are `segment_offsets` wide. `key`
    names each record's key from its position in its batch.
    """
    for first, count in ((0, 10), (10, 10), (20, 5)):
        lines = "".join(
            (f"{key(i - first)}:" if key else "") + f"v{i}{PAD}\n"
            for i in range(first, first + count)
        )
        out = kcat("-t", topic, "-p", "0", "-P", *(["-K:"] if key else []), stdin=lines)
        assert out.returncode == 0, out.stderr
    assert sql(f"SELECT high_watermark FROM kafgres_partition_offsets('{topic}')") == "25"


@pytest.fixture
def topic(request):
    """An empty topic with small segments. `segment_offsets` is set before the topic
    exists: changing it under a topic with data overlaps the table engine's ranges."""
    sql("ALTER SYSTEM SET kafgres.segment_offsets = 10")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 4096")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    name = f"race-{request.node.name.replace('_', '-')[5:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")


class Session:
    """One psql backend, fed a statement at a time, so a transaction can stay open."""

    def __init__(self):
        self.proc = subprocess.Popen(
            PSQL[:-1] + ["-X", "-q", "-tA", "-v", "ON_ERROR_STOP=1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )

    def run(self, statement):
        self.proc.stdin.write(statement + "\n\\echo __done__\n")
        self.proc.stdin.flush()
        out = []
        for line in self.proc.stdout:
            if line.strip() == "__done__":
                return [l.strip() for l in out if l.strip()]
            out.append(line)
        raise AssertionError(f"psql exited: {''.join(out)}")

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=60)


def sql_reader(topic):
    """`kafgres_read()` from offset 5: the offsets it returned, or its error."""
    out = subprocess.run(
        PSQL + [f"SELECT record_offset FROM kafgres_read('{topic}', 0, 5)"],
        capture_output=True, text=True, timeout=60,
    )
    if out.returncode != 0:
        return out.stderr.strip()
    return [int(o) for o in out.stdout.split()]


def kafka_reader(topic):
    """A consumer fetching from offset 5 that may not reset: the offsets it got, or its error."""
    out = kcat("-C", "-t", topic, "-p", "0", "-o", "5", "-e", "-q",
               "-X", "auto.offset.reset=error", "-f", "%o\n", timeout=60)
    got = [int(o) for o in out.stdout.split()]
    return got or out.stderr.strip()


@pytest.mark.parametrize("reader", [sql_reader, kafka_reader], ids=["kafgres_read", "fetch"])
def test_a_read_racing_retention_is_never_a_skip(topic, reader):
    fill(topic)
    held = Session()
    try:
        held.run("BEGIN;")
        # Inside the transaction, so the broker's own retention cannot act on it first.
        held.run(f"""UPDATE kafgres_topics SET config = config || '{{"retention.ms":"1"}}'
                      WHERE name = '{topic}';""")
        time.sleep(0.1)
        held.run("SELECT kafgres_enforce_retention();")
        start = held.run(f"SELECT log_start_offset FROM kafgres_partition_offsets('{topic}');")
        assert start == ["20"], f"retention did not reach the third segment: {start}"

        got = {}
        thread = threading.Thread(target=lambda: got.update(result=reader(topic)))
        thread.start()
        # Long enough to check the log start, and on the table engine to wait on the DROP.
        thread.join(3)
        held.run("COMMIT;")
    finally:
        held.close()
    thread.join(60)

    result = got.get("result")
    served = result == list(range(5, 25))
    refused = isinstance(result, str) and "out of range" in result.lower()
    assert served or refused, (
        f"read from offset 5 while retention moved the log start to 20: {result!r}"
    )


@pytest.mark.parametrize("reader", [sql_reader, kafka_reader], ids=["kafgres_read", "fetch"])
def test_a_rolled_back_retention_leaves_every_record_readable(topic, reader):
    """The log start stays where it was, so the records below it must too: a consumer
    reset to the earliest offset would otherwise be told it is out of range forever."""
    fill(topic)
    retention = f"""UPDATE kafgres_topics SET config = config || '{{"retention.ms":"1"}}'
                     WHERE name = '{topic}'; SELECT kafgres_enforce_retention();"""
    for statements in (f"BEGIN; {retention} ROLLBACK;",
                       f"BEGIN; SAVEPOINT s; {retention} ROLLBACK TO SAVEPOINT s; COMMIT;"):
        time.sleep(0.1)
        sql(statements)
        assert sql(f"SELECT log_start_offset FROM kafgres_partition_offsets('{topic}')") == "0"
        assert reader(topic) == list(range(5, 25)), statements


@pytest.mark.parametrize("reader", [sql_reader, kafka_reader], ids=["kafgres_read", "fetch"])
def test_a_segment_compaction_emptied_is_a_gap_not_out_of_range(topic, reader):
    """Compaction also deletes a segment it empties, but leaves the log start where it was,
    so the records below are a gap to skip. Out of range here would send a consumer reset
    to the earliest offset straight back to the same error."""
    sql(f"""UPDATE kafgres_topics SET config = config ||
              '{{"cleanup.policy":"compact","min.compaction.lag.ms":"0"}}'
             WHERE name = '{topic}'""")
    fill(topic, key=lambda i: f"k{i}")  # The second batch supersedes all of the first.
    time.sleep(0.1)
    for _ in range(3):
        sql("SELECT kafgres_enforce_retention()")
    assert sql(f"SELECT log_start_offset FROM kafgres_partition_offsets('{topic}')") == "0"
    got = reader(topic)
    assert got == list(range(10, 25)), f"from offset 5, compacted below 10: {got!r}"
