"""Leader epoch and failover correctness.

What this protects against only exists on a real **async** replica: a consumer whose
position is past the new leader's log end, reading records at offsets it has already
seen that are *not the records it saw*. Nothing errors. The consumer does not slow
down. It just returns different data.

So this builds the rig: a physical streaming standby, a deliberate divergence created
by cutting the wire so WAL never arrives, a promotion, and then the question — "where
did the epoch I was reading under end?"
"""

import os
import socket
import struct
import subprocess
import time

import pytest

from conftest import sql

pytestmark = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "table",
    reason="engine A only: builds divergence through WAL, which is not engine B's log",
)

CLIENTS = "kafgres-clients"
PRIMARY = "127.0.0.1:9092"
STANDBY_PORT = 9192
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

TOPIC = "p6-failover"

FENCED_LEADER_EPOCH = 74
UNKNOWN_LEADER_EPOCH = 75

def compose(*args, timeout=300, check=False):
    out = subprocess.run(
        ["docker", "compose", "--profile", "failover", *args],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    if check and out.returncode != 0:
        raise AssertionError(f"docker compose {' '.join(args)} failed:\n{out.stderr}")
    return out

def standby_container():
    """Ask compose, rather than assuming the project is named after this directory.

    The container and network names are `<project>-...`, and the project defaults to the
    checkout directory name. Hardcoding them means the rig silently addresses the wrong
    thing — or nothing — under any other checkout name, which is the same class of
    failure as the standby's stale image.
    """
    out = subprocess.run(["docker", "compose", "ps", "-aq", "standby"],
                         capture_output=True, text=True, timeout=60, cwd=REPO)
    cid = out.stdout.strip().splitlines()
    assert cid, "the standby container does not exist"
    return cid[0]

def standby_network():
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
         standby_container()],
        capture_output=True, text=True, timeout=60, cwd=REPO,
    )
    name = out.stdout.strip()
    assert name, "the standby is on no network"
    return name

def standby_sql(query, timeout=60):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "standby", "psql", "-U", "postgres",
         "-d", "postgres", "-tAc", query],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    return out.stdout.strip()

def kcat(port, *args, stdin=None, timeout=90):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", f"127.0.0.1:{port}", *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def next_offset(query_fn, topic=TOPIC):
    v = query_fn(
        f"""SELECT next_offset FROM kafgres_partitions
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip()
    return int(v) if v else -1

def offset_for_leader_epoch(port, topic, leader_epoch, current_leader_epoch=-1):
    """OffsetForLeaderEpoch v2 by hand — the newest version before flexible encoding.

    Hand-built because no CLI exposes this API: it is spoken between a consumer and a
    broker during recovery and never by a person.
    """
    sock = socket.create_connection(("127.0.0.1", port), timeout=20)
    try:
        body = struct.pack(">i", 1)
        body += struct.pack(">h", len(topic)) + topic.encode()
        body += struct.pack(">i", 1)
        body += struct.pack(">iii", 0, current_leader_epoch, leader_epoch)
        header = struct.pack(">hhi", 23, 2, 1) + struct.pack(">h", 6) + b"pytest"
        frame = header + body
        sock.sendall(struct.pack(">i", len(frame)) + frame)

        (size,) = struct.unpack(">i", read_exactly(sock, 4))
        resp = read_exactly(sock, size)
        pos = 8  # correlation + throttle
        (_topics,) = struct.unpack_from(">i", resp, pos)
        pos += 4
        (name_len,) = struct.unpack_from(">h", resp, pos)
        pos += 2 + name_len
        (_parts,) = struct.unpack_from(">i", resp, pos)
        pos += 4
        error, _partition, epoch, end = struct.unpack_from(">hiiq", resp, pos)
        return error, epoch, end
    finally:
        sock.close()

def read_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise AssertionError(f"peer closed after {len(buf)} of {n} bytes")
        buf += chunk
    return buf

def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            return True
    except OSError:
        return False

@pytest.fixture(scope="module")
def promoted_standby():
    """Build the divergence, then promote.

    Every step is load-bearing:
      1. a physical standby, streaming
      2. three records, replicated — the part both nodes agree on
      3. **cut the wire**, so the next records never reach the standby. Pausing replay is
         not enough: promotion applies whatever WAL has already arrived, so the standby
         would catch up and there would be nothing to diverge.
      4. four more records to the primary
      5. promote the standby, which has only the first three
    """
    subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "sh", "-c",
         "grep -q 'kafgres-failover-rig' /var/lib/postgresql/data/pg_hba.conf || "
         "printf '# kafgres-failover-rig\\nhost replication all all trust\\n"
         "host all all all trust\\n' >> /var/lib/postgresql/data/pg_hba.conf"],
        capture_output=True, text=True, timeout=60, cwd=REPO,
    )
    sql("SELECT pg_reload_conf()")

    compose("rm", "-sfv", "standby")

    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")
    sql(f"SELECT kafgres_create_topic('{TOPIC}', 1)")

    compose("up", "-d", "standby", check=True)
    deadline = time.time() + 120
    while time.time() < deadline:
        if standby_sql("SELECT pg_is_in_recovery()") == "t":
            break
        time.sleep(2)
    else:
        pytest.fail("standby never reached recovery")

    serving_while_replica = subprocess.run(
        ["docker", "compose", "exec", "-T", "standby", "sh", "-c",
         "grep -c ':23D8 .* 0A ' /proc/net/tcp || true"],
        capture_output=True, text=True, timeout=60, cwd=REPO,
    ).stdout.strip() not in ("", "0")

    kept = "kept-1\nkept-2\nkept-3\n"
    assert kcat(9092, "-t", TOPIC, "-P", "-X", "batch.num.messages=1",
                stdin=kept).returncode == 0
    deadline = time.time() + 60
    while time.time() < deadline and next_offset(standby_sql) < 3:
        time.sleep(1)
    assert next_offset(standby_sql) == 3, "standby never caught up to the agreed point"

    network = standby_network()
    cut = subprocess.run(["docker", "network", "disconnect", network, standby_container()],
                         capture_output=True, text=True, timeout=60)
    assert cut.returncode == 0, f"could not cut the wire: {cut.stderr}"

    lost = "lost-1\nlost-2\nlost-3\nlost-4\n"
    assert kcat(9092, "-t", TOPIC, "-P", "-X", "batch.num.messages=1",
                stdin=lost).returncode == 0
    time.sleep(2)

    subprocess.run(["docker", "compose", "restart", "postgres"],
                   capture_output=True, text=True, timeout=180, cwd=REPO)
    deadline = time.time() + 90
    while time.time() < deadline:
        if sql("SELECT 1").strip() == "1":
            break
        time.sleep(2)
    primary_epoch = sql(
        f"""SELECT leader_epoch FROM kafgres_partitions
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{TOPIC}')"""
    ).strip()

    standby_sql("SELECT pg_promote(wait => true, wait_seconds => 60)", timeout=120)
    deadline = time.time() + 60
    while time.time() < deadline:
        if standby_sql("SELECT pg_is_in_recovery()") == "f":
            break
        time.sleep(2)
    else:
        pytest.fail("standby never left recovery")

    reconnect = subprocess.run(
        ["docker", "network", "connect", network, standby_container()],
        capture_output=True, text=True, timeout=60,
    )
    assert reconnect.returncode == 0, reconnect.stderr
    deadline = time.time() + 60
    while time.time() < deadline and not port_open(STANDBY_PORT):
        time.sleep(2)

    fresh = "new-1\nnew-2\nnew-3\nnew-4\n"
    assert kcat(STANDBY_PORT, "-t", TOPIC, "-P", "-X", "batch.num.messages=1",
                stdin=fresh).returncode == 0
    deadline = time.time() + 30
    while time.time() < deadline and next_offset(standby_sql) < 7:
        time.sleep(1)

    yield {"serving_while_replica": serving_while_replica, "primary_epoch": primary_epoch}
    compose("rm", "-sfv", "standby")
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")

def test_the_standby_diverged(promoted_standby):
    """The premise. If the two nodes agree, nothing below proves anything.

    Both logs now end at 7 and disagree about offsets 3..6: the same offsets holding
    different records on the two nodes, at the same moment. That is the divergence a
    consumer cannot see for itself.
    """
    assert next_offset(sql) == 7, "the primary should hold all seven records"
    assert next_offset(standby_sql) == 7, "the new leader should have written past the split"

    on_new = kcat(STANDBY_PORT, "-t", TOPIC, "-C", "-o", "beginning", "-e", "-q").stdout.split()
    on_old = kcat(9092, "-t", TOPIC, "-C", "-o", "beginning", "-e", "-q").stdout.split()
    assert on_new == ["kept-1", "kept-2", "kept-3", "new-1", "new-2", "new-3", "new-4"], on_new
    assert on_old == ["kept-1", "kept-2", "kept-3", "lost-1", "lost-2", "lost-3", "lost-4"], on_old

def test_a_standby_runs_no_broker_until_promotion(promoted_standby):
    """The worker is registered `RecoveryFinished`, so a replica serves no Kafka traffic.
    A broker answering reads from a replica hands out data a failover can still take
    away — and the reader has no way to know.

    Observed while the standby was actually in recovery, with its port published: the
    fixture records it before promoting, because afterwards there is nothing left to
    look at.
    """
    assert not promoted_standby["serving_while_replica"], (
        "the broker was listening on the standby while it was still a replica"
    )

def test_a_restart_is_not_a_promotion(promoted_standby):
    """The primary restarted mid-divergence and its epoch did not move.

    An epoch derived from a local counter bumps here, and that is what makes it unsafe:
    the promoted standby then raises its own stale copy to the *same* number for
    different records, and a consumer holding it is told its position is current. The
    epoch is the Postgres timeline, which a diverged primary can never reach — so a
    restart, which diverges from nothing, must leave it alone.
    """
    assert promoted_standby["primary_epoch"] == "0", (
        f"the primary's epoch moved on a restart: {promoted_standby['primary_epoch']}"
    )

def test_promotion_bumps_the_leader_epoch(promoted_standby):
    """A promotion that does not bump the epoch leaves consumers with nothing to detect
    the change by — every record still claims the epoch of the leader that is gone."""
    history = standby_sql(
        f"""SELECT leader_epoch || ':' || start_offset FROM kafgres_leader_epochs
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{TOPIC}')
             ORDER BY leader_epoch"""
    ).split()
    assert history == ["0:0", "1:3"], (
        f"expected epoch 0 from offset 0 and epoch 1 from the divergence point, got {history}"
    )

def test_offset_for_leader_epoch_gives_the_truncation_point(promoted_standby):
    """The point of the whole rig, in one assertion.

    A consumer read to offset 7 under epoch 0. The new leader wrote its own records at
    3..6, so its log also ends at 7 — and those are different records. Asked where epoch
    0 ended, the broker must say **3**, and the consumer discards everything above it.

    Note what the log end is here: also 7. "Answer with the log end", which is what a
    broker with no epoch tracking effectively tells a consumer, gives 7 — and the
    consumer keeps four records that never existed on this leader. Answering
    `min(base_offset)` over rows carrying that epoch gives 0 and the consumer throws
    away everything. Only the recorded boundary is right.
    """
    assert next_offset(standby_sql) == 7, "the log end must differ from the boundary"
    error, epoch, end = offset_for_leader_epoch(STANDBY_PORT, TOPIC, leader_epoch=0)
    assert error == 0, f"error {error}"
    assert epoch == 0, f"expected the epoch asked about, got {epoch}"
    assert end == 3, f"expected epoch 0 to end at the divergence point, got {end}"

def test_the_current_epoch_ends_at_the_log_end(promoted_standby):
    """An epoch that has not ended has no start-of-next to report, so the answer is the
    log end. A consumer ahead of *that* truncates too, which is the same case."""
    error, epoch, end = offset_for_leader_epoch(STANDBY_PORT, TOPIC, leader_epoch=1)
    assert error == 0, f"error {error}"
    assert epoch == 1 and end == next_offset(standby_sql), (epoch, end)

def test_fencing_in_both_directions(promoted_standby):
    """`current_leader_epoch` says what the client believes. The two disagreements mean
    opposite things and a client recovers differently from each: behind us it is talking
    to a leader that has moved on and must refresh metadata; ahead of us it has reached a
    broker that has not caught up and must retry. Answering either with an offset would
    have it truncate against a leader it is not reading from."""
    error, _, _ = offset_for_leader_epoch(STANDBY_PORT, TOPIC, 0, current_leader_epoch=0)
    assert error == FENCED_LEADER_EPOCH, f"expected 74, got {error}"

    error, _, _ = offset_for_leader_epoch(STANDBY_PORT, TOPIC, 0, current_leader_epoch=9)
    assert error == UNKNOWN_LEADER_EPOCH, f"expected 75, got {error}"

def test_retention_cannot_erase_the_truncation_point(promoted_standby):
    """Why the history is its own table.

    An answer derived from `min(base_offset)` over `kafgres_log` is correct until
    retention reclaims the epoch's early segments and then quietly wrong: the recorded
    start creeps forward as they go, and once the epoch is fully reclaimed the answer
    becomes "no such epoch" — a consumer that should truncate is told nothing at all.
    Deleting the rows outright is harsher than retention, and the boundary has to
    survive it.
    """
    standby_sql(
        f"""DELETE FROM kafgres_log
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{TOPIC}')
               AND leader_epoch = 0"""
    )
    error, epoch, end = offset_for_leader_epoch(STANDBY_PORT, TOPIC, leader_epoch=0)
    assert (error, epoch, end) == (0, 0, 3), (
        f"the boundary did not survive losing epoch 0's log rows: {(error, epoch, end)}"
    )
