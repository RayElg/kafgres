"""The `.timeindex`, and what retention leaves behind.

`offset_for_timestamp` answers `offsetsForTimes()` and `--reset-offsets --to-datetime`
by reading batch headers; the index narrows each segment's scan to the window the
answer can be in.

The index is sparse and lazily written, never fsynced, and written after the batch it
describes — so every failure mode is "the entry is missing or wrong", which costs a longer
scan. `test_the_answer_does_not_depend_on_the_time_index` is what holds that claim to
account: delete every index file and the answers must not move.
"""

import os
import socket
import struct
import subprocess
import time

import pytest

from recordbatch import parse_produce_v3, produce_v3, record_batch

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def compose(*args, timeout=180):
    return subprocess.run(["docker", "compose", *args], capture_output=True, text=True,
                          timeout=timeout, cwd=REPO)

def sql(query, timeout=60):
    return compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "postgres",
                   "-tAc", query, timeout=timeout).stdout.strip()

def sh(command, timeout=60):
    return compose("exec", "-T", "postgres", "sh", "-c", command, timeout=timeout)

def kcat(*args, stdin=None, timeout=120):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def offset_at(topic, when_ms):
    """`kcat -Q`, which is `offsetsForTimes` — the first offset at or after `when_ms`."""
    out = kcat("-Q", "-t", f"{topic}:0:{when_ms}")
    for line in out.stdout.splitlines():
        if "offset" in line:
            return int(line.rsplit(" ", 1)[1])
    raise AssertionError(f"no offset in {out.stdout!r} {out.stderr!r}")

def segment_files(topic, ext=None):
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    if not tid:
        return []
    out = sh(f"ls $PGDATA/kafgres/{tid}/0/ 2>/dev/null || true")
    names = [n for n in out.stdout.split() if n.strip()]
    return sorted(n for n in names if ext is None or n.endswith(ext))

pytestmark = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "segment",
    reason="engine B only: the table engine has no segment files to index",
)

@pytest.fixture
def rig(request):
    topic = f"p7ti-{request.node.name.replace('_', '-')[:32]}"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 4096")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    yield topic
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")

@pytest.fixture
def big_segments(request):
    """One segment big enough to hold several index entries.

    The opposite of `rig`: shrinking `segment_bytes` to the index interval puts exactly one
    entry per segment, always at position 0, which makes the seek a no-op.
    """
    topic = f"p7ti-{request.node.name.replace('_', '-')[:32]}"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 1048576")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    yield topic
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")

def fill(topic, waves=4, per_wave=30):
    """Several segments, in waves with a gap, so a timestamp lands *between* records.

    Separate invocations because a batch never spans two segments: one big produce can
    land as a single batch in a single segment and roll nothing, which would leave this
    testing a one-segment scan.
    """
    marks = []
    for w in range(waves):
        marks.append(int(time.time() * 1000))
        payload = "".join(f"{'z' * 200}-{w}-{i:03d}\n" for i in range(per_wave))
        assert kcat("-t", topic, "-P", stdin=payload).returncode == 0
        time.sleep(1.1)
    assert len(segment_files(topic, ".log")) >= 3, segment_files(topic, ".log")
    return marks

def test_a_time_index_is_written_beside_every_segment(rig):
    topic = rig
    fill(topic)
    logs = [n[: -len(".log")] for n in segment_files(topic, ".log")]
    times = [n[: -len(".timeindex")] for n in segment_files(topic, ".timeindex")]
    assert times == logs, f"segments {logs} but time indexes {times}"

def test_offsets_for_times_finds_the_first_record_at_or_after(rig):
    topic = rig
    marks = fill(topic)

    assert offset_at(topic, marks[0] - 60_000) == 0
    assert offset_at(topic, int(time.time() * 1000) + 60_000) == -1

    for w, mark in enumerate(marks):
        got = offset_at(topic, mark)
        assert 0 <= got <= w * 30, f"wave {w} at {mark} gave offset {got}"

def test_the_answer_does_not_depend_on_the_time_index(rig):
    """The index is an optimisation and nothing else.

    It is sparse, written lazily, never fsynced, and written *after* the batch it describes,
    so a crash can leave it short, torn or absent. Every one of those has to cost a longer
    scan rather than a different answer — which is only true if the log remains the thing
    actually consulted.
    """
    topic = rig
    marks = fill(topic)
    probes = [marks[0] - 60_000, marks[1], marks[-1], int(time.time() * 1000) + 60_000]
    before = [offset_at(topic, p) for p in probes]

    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    sh(f"find $PGDATA/kafgres/{tid}/0 -name '*.timeindex' -delete")
    assert segment_files(topic, ".timeindex") == [], "the index files are still there"

    after = [offset_at(topic, p) for p in probes]
    assert after == before, f"answers moved when the index went away: {before} -> {after}"

def test_retention_removes_the_sidecar_indexes(rig):
    """A reclaimed segment takes its `.index` and `.timeindex` with it.

    `segment_bases` enumerates `.log` files, so an orphaned index is invisible to every
    code path *and* permanent: offsets never repeat, so nothing reuses the name and
    reclaims it. It reports as "the disk is full", a long way from here.
    """
    topic = rig
    fill(topic)
    assert len(segment_files(topic, ".log")) >= 3

    assert subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "apache/kafka:4.1.0",
         "/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", BROKER,
         "--entity-type", "topics", "--entity-name", topic,
         "--alter", "--add-config", "retention.ms=1"],
        capture_output=True, text=True, timeout=180, cwd=REPO,
    ).returncode == 0
    time.sleep(2)
    sql("SELECT kafgres_enforce_retention()")

    logs = segment_files(topic, ".log")
    assert len(logs) == 1, f"expected only the active segment, got {logs}"
    bases = {n[: -len(".log")] for n in logs}
    for ext in (".index", ".timeindex"):
        orphans = {n[: -len(ext)] for n in segment_files(topic, ext)} - bases
        assert not orphans, f"reclaim left orphaned {ext} files: {sorted(orphans)}"

def test_a_backfill_producer_does_not_hide_records_from_a_lookup(big_segments):
    """The index entry must dominate the batches it claims to speak for.

    An entry `(ts, pos)` is read as "nothing before `pos` is at or above `ts`". The index is
    sparse, so recording the *indexed batch's own* timestamp is not enough: a backfill
    producer stamping old `CreateTime` values can put low timestamps on every indexed batch
    while live traffic sits unindexed between them, and the lookup skips records that
    qualify. Kafka keeps a running max over all appends for this reason, and so does this.

    **One segment, several index entries** — not the small segments the other tests use.
    With `segment_bytes` equal to the 4 KiB index interval, each segment gets exactly one
    entry and it sits at position 0, so the seek can never skip anything and the bug is
    unreachable. The entry count is asserted below so it cannot quietly regress to that.

    Driven through raw batches, because no client can set a record timestamp: kcat's `-T`
    is tee. The answer is checked against the same query with the index deleted, which
    forces the from-zero scan and is therefore the ground truth.
    """
    topic = big_segments
    old_ms = 1_600_000_000_000            # long ago
    new_ms = 1_800_000_000_000            # long hence

    sock = socket.create_connection(("127.0.0.1", 9092), timeout=20)
    try:
        for i in range(60):
            stamp = old_ms + i if i % 2 == 0 else new_ms + i
            batch = record_batch([f"{'q' * 300}-{i}".encode()], timestamp=stamp)
            header = struct.pack(">hhi", 0, 3, 900 + i) + struct.pack(">h", 6) + b"pytest"
            frame = header + produce_v3(topic, 0, batch)
            sock.sendall(struct.pack(">i", len(frame)) + frame)
            size = struct.unpack(">i", _read_exactly(sock, 4))[0]
            _, error, _ = parse_produce_v3(_read_exactly(sock, size))
            assert error == 0, f"produce {i} failed with error {error}"
    finally:
        sock.close()

    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    entries = int(sh(
        f"stat -c%s $PGDATA/kafgres/{tid}/0/*.timeindex 2>/dev/null | head -1 || echo 0"
    ).stdout.strip() or 0) // 12
    assert entries >= 2, (
        f"only {entries} index entries, so the seek starts at 0 and this test cannot fail"
    )

    target = (old_ms + new_ms) // 2
    with_index = offset_at(topic, target)

    sh(f"find $PGDATA/kafgres/{tid}/0 -name '*.timeindex' -delete")
    ground_truth = offset_at(topic, target)

    assert with_index == ground_truth, (
        f"the index skipped records a full scan finds: {with_index} vs {ground_truth}"
    )
    assert ground_truth == 1, f"expected offset 1, got {ground_truth}"

def _read_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return buf
