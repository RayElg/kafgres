"""A sealed segment that lost its tail is reported, never read as a shorter log, and an
index entry pointing at the wrong batch costs a scan rather than skipped records."""

import socket
import struct
import subprocess
import time

import pytest

from conftest import BROKER_HOST, BROKER_PORT, sql
from support.docker import CLIENTS, compose, sh
from support.recordbatch import parse_produce_v3, produce_v3, record_batch

pytestmark = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "segment",
    reason="engine B only: the table engine has no segment files",
)

BATCHES = 120


@pytest.fixture
def topic(request):
    """Several sealed segments of a few index entries each (one per 4 KiB)."""
    name = f"sealed-{request.node.name.replace('_', '-')[5:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 16384")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    produce_batches(name)
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")


def value(i):
    return f"{i:05d}-{'x' * 300}"


def produce_batches(topic):
    """One record per batch, so the index has batch boundaries to name."""
    sock = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=20)
    try:
        for i in range(BATCHES):
            header = struct.pack(">hhi", 0, 3, i) + struct.pack(">h", 6) + b"pytest"
            frame = header + produce_v3(topic, 0, record_batch([value(i).encode()]))
            sock.sendall(struct.pack(">i", len(frame)) + frame)
            size = struct.unpack(">i", read_exactly(sock, 4))[0]
            _, error, _ = parse_produce_v3(read_exactly(sock, size))
            assert error == 0, f"produce {i} failed with error {error}"
    finally:
        sock.close()


def read_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return buf


def first_sealed(topic):
    """The first segment's path stem and its index entries as (offset, position)."""
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
    d = f"$PGDATA/kafgres/{tid}/0"
    logs = sorted(n for n in sh(f"ls {d}").stdout.split() if n.endswith(".log"))
    assert len(logs) >= 3, logs
    stem = f"{d}/{logs[0][:-len('.log')]}"
    raw = bytes(int(b) for b in sh(f"od -An -tu1 -v {stem}.index").stdout.split())
    entries = [struct.unpack_from(">II", raw, i) for i in range(0, len(raw) - len(raw) % 8, 8)]
    base = int(logs[0][: -len(".log")])
    assert len(entries) >= 3, f"only {len(entries)} index entries in the first segment"
    return stem, [(base + rel, pos) for rel, pos in entries]


@pytest.mark.parametrize("past_boundary", [0, 30])
def test_a_sealed_segment_cut_short_is_an_error(topic, past_boundary):
    """Cut on a batch boundary, or a few bytes past one: either way a read reaches the end
    of the segment early and must say so rather than carry on in the next one."""
    stem, entries = first_sealed(topic)
    _, pos = entries[-1]
    sh(f"truncate -s {pos + past_boundary} {stem}.log")
    with pytest.raises(RuntimeError):
        sql(f"SELECT count(*) FROM kafgres_read('{topic}', 0)")


def wait_ready(timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if compose("exec", "-T", "postgres", "pg_isready", "-U", "postgres").returncode == 0:
            try:
                if sql("SELECT count(*) FROM pg_stat_activity "
                       "WHERE backend_type = 'kafgres_broker'") == "1":
                    return
            except RuntimeError:
                pass
        time.sleep(2)
    raise AssertionError("the server did not come back")


def test_an_index_entry_naming_a_later_batch_skips_nothing(topic):
    """Entry 1 keeps its offset but points at entry 2's batch, a valid batch further on.
    After a restart the broker seeks with it; reading from entry 1's offset must still
    return every record up to entry 2's."""
    stem, entries = first_sealed(topic)
    (start, _), (stop, later) = entries[1], entries[2]
    octal = "".join(f"\\{b:03o}" for b in struct.pack(">I", later))
    sh(f"printf '{octal}' | dd of={stem}.index bs=1 seek=12 conv=notrunc status=none")
    compose("restart", "postgres", timeout=180)
    wait_ready()

    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat",
         "-b", f"{BROKER_HOST}:{BROKER_PORT}", "-C", "-t", topic, "-p", "0",
         "-o", str(start), "-c", str(stop - start), "-e", "-f", "%o %s\\n"],
        capture_output=True, text=True, timeout=120,
    )
    got = [tuple(line.split(" ", 1)) for line in out.stdout.splitlines() if line]
    assert got == [(str(i), value(i)) for i in range(start, stop)], out.stderr[-500:]
