"""Request locks against the things that hold them: vacuum truncation, which yields only to a
waiter; VACUUM FULL and an operator's LOCK, which never yield; and a starved CPU, which makes
every lock phase slow. Each test tries to make a request fail, or the loop stall, rather than
to show one scenario working.

A failed request closes its connection, so the hammer counts disconnects and error codes. The
loop is single-threaded, so ApiVersions, which takes no table lock, measures how long the
loop was stalled."""

import os
import socket
import struct
import subprocess
import threading
import time

import pytest

from conftest import (API_VERSIONS, BROKER_HOST, BROKER_PORT, FETCH, METADATA, OFFSET_FETCH,
                      PRODUCE, Connection, sql)
from support.recordbatch import produce_v3, record_batch

TOPIC = "hunt-lockwait"
GROUP = "hunt-lockwait-group"
FILLER = "hunt-lockwait-filler"


def _str(s):
    b = s.encode()
    return struct.pack(">h", len(b)) + b


def _fetch_v11(topic, max_wait_ms):
    b = struct.pack(">iiii", -1, max_wait_ms, 1, 1048576) + struct.pack(">b", 0)
    b += struct.pack(">ii", 0, -1)
    b += struct.pack(">i", 1) + _str(topic) + struct.pack(">i", 1)
    b += struct.pack(">ii", 0, -1) + struct.pack(">qqi", 0, 0, 1048576)
    return b + struct.pack(">i", 0) + struct.pack(">h", 0)


def _fetch_error(resp):
    # correlation, throttle, error, session; then topic name, partition index, partition error
    top = struct.unpack_from(">h", resp, 8)[0]
    off = 14 + 4
    (n,) = struct.unpack_from(">h", resp, off)
    off += 2 + n + 4 + 4
    return top or struct.unpack_from(">h", resp, off)[0]


def _offset_fetch_v1(group, topic):
    return _str(group) + struct.pack(">i", 1) + _str(topic) + struct.pack(">ii", 1, 0)


def _offset_fetch_error(resp):
    off = 4 + 4
    (n,) = struct.unpack_from(">h", resp, off)
    off += 2 + n + 4 + 4 + 8
    (m,) = struct.unpack_from(">h", resp, off)
    return struct.unpack_from(">h", resp, off + 2 + max(m, 0))[0]


def _produce_error(resp):
    off = 4 + 4
    (n,) = struct.unpack_from(">h", resp, off)
    return struct.unpack_from(">h", resp, off + 2 + n + 4 + 4)[0]


# Each kind: (api, version, body, error-or-None from the response). Between them they take
# every lock group: Metadata the topic tables, OffsetFetch the group tables, Produce the
# producer and log tables, Fetch the log through the parked-fetch path.
KINDS = {
    "metadata": lambda: (METADATA, 1, struct.pack(">i", -1), lambda r: 0),
    "offset_fetch": lambda: (OFFSET_FETCH, 1, _offset_fetch_v1(GROUP, TOPIC), _offset_fetch_error),
    "produce": lambda: (PRODUCE, 3, produce_v3(TOPIC, 0, record_batch([b"v"])), _produce_error),
    "fetch": lambda: (FETCH, 11, _fetch_v11(TOPIC, 0), _fetch_error),
}


class Hammer:
    """One connection per kind, each sending its request back to back, plus an ApiVersions
    probe every 50 ms. Records every failure with its time, and the probe's worst latency."""

    def __init__(self, kinds=tuple(KINDS)):
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.failures, self.served, self.served_at, self.probe_worst = [], 0, [], 0.0
        self.threads = [threading.Thread(target=self._run, args=(k,)) for k in kinds]
        self.threads.append(threading.Thread(target=self._probe))

    def __enter__(self):
        for t in self.threads:
            t.start()
        time.sleep(0.5)
        return self

    def __exit__(self, *exc):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=30)

    def _fail(self, kind, what):
        with self.lock:
            self.failures.append((time.monotonic(), kind, what))

    def _run(self, kind):
        n = 0
        while not self.stop.is_set():
            api, version, body, error_of = KINDS[kind]()
            try:
                with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15) as s:
                    c = Connection(s)
                    while not self.stop.is_set():
                        n += 1
                        c.send(api, version, n, body=body)
                        resp = c.recv()
                        assert struct.unpack_from(">i", resp, 0)[0] == n, "correlation"
                        err = error_of(resp)
                        if err:
                            self._fail(kind, f"error code {err}")
                        else:
                            with self.lock:
                                self.served += 1
                                self.served_at.append(time.monotonic())
            except Exception as e:  # a failed request costs its connection: reconnect
                self._fail(kind, repr(e))
                time.sleep(0.05)

    def _probe(self):
        n = 0
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=30) as s:
            c = Connection(s)
            while not self.stop.is_set():
                n += 1
                t0 = time.monotonic()
                c.send(API_VERSIONS, 0, n)
                c.recv()
                with self.lock:
                    self.probe_worst = max(self.probe_worst, time.monotonic() - t0)
                time.sleep(0.05)

    def failures_between(self, start, end):
        with self.lock:
            return [f for f in self.failures if start <= f[0] <= end]

    def served_between(self, start, end):
        with self.lock:
            return [t for t in self.served_at if start <= t <= end]


def _container():
    return subprocess.run(["docker", "compose", "ps", "-q", "postgres"], capture_output=True,
                          text=True, check=True).stdout.strip()


@pytest.fixture
def cpus():
    """Starve the broker's container of CPU, as a busy CI runner does, and restore it."""
    cid = _container()
    before = int(subprocess.run(["docker", "inspect", "-f", "{{.HostConfig.NanoCpus}}", cid],
                                capture_output=True, text=True, check=True).stdout.strip())

    def limit(n):
        subprocess.run(["docker", "update", "--cpus", str(n), cid], check=True,
                       capture_output=True)
    yield limit
    # `--cpus 0` does not clear a limit, so restore to every host CPU.
    limit(before / 1e9 if before else os.cpu_count())


@pytest.fixture
def topic():
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")
    sql(f"SELECT kafgres_create_topic('{TOPIC}', 1)")
    yield TOPIC
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")


def broker_pid():
    return sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'kafgres_broker'")


def fill_and_empty_offsets(rows=300000):
    """A large empty tail on kafgres_offsets, which every group request locks."""
    sql(f"DELETE FROM kafgres_offsets WHERE group_id LIKE '{FILLER}-%'")
    sql(f"""INSERT INTO kafgres_offsets (group_id, topic_id, partition, committed_offset, metadata)
            SELECT '{FILLER}-' || g, 0, 0, 0, repeat('x', 500)
            FROM generate_series(1, {rows}) g""")
    sql(f"DELETE FROM kafgres_offsets WHERE group_id LIKE '{FILLER}-%'")


def fill_and_empty_producer_batches(rows=600000):
    """A large empty tail on kafgres_producer_batches, which every produce locks. Negative
    producer ids are never allocated."""
    sql("DELETE FROM kafgres_producer_batches WHERE producer_id < 0")
    sql(f"""INSERT INTO kafgres_producer_batches
              (producer_id, topic_id, partition, producer_epoch, first_seq, last_seq, base_offset)
            SELECT -g, 0, 0, 0, 0, 0, 0 FROM generate_series(1, {rows}) g""")
    sql("DELETE FROM kafgres_producer_batches WHERE producer_id < 0")


def size(table):
    return int(sql(f"SELECT pg_relation_size('{table}')"))


def vacuum_with_hammer(tables):
    before = {t: size(t) for t in tables}
    with Hammer() as h:
        t0 = time.monotonic()
        sql(f"VACUUM {', '.join(tables)}")
        t1 = time.monotonic()
        time.sleep(0.5)
    for t in tables:
        assert size(t) < before[t] // 2, f"vacuum truncated nothing on {t}: test is vacuous"
    return h, t0, t1


@pytest.mark.parametrize("starve", [None, 0.25], ids=["full-cpu", "quarter-cpu"])
def test_a_truncation_fails_no_request_even_on_a_starved_broker(topic, cpus, starve):
    """On a starved broker every lock phase is slow and vacuum's truncation holds its lock
    longer. Neither may cost a request."""
    fill_and_empty_offsets()
    if starve:
        cpus(starve)
    h, t0, t1 = vacuum_with_hammer(["kafgres_offsets"])
    assert not h.failures, f"{len(h.failures)} failed, first {h.failures[0][1:]} " \
                           f"({h.failures[0][0] - t0:+.2f}s into a {t1 - t0:.2f}s vacuum)"
    assert h.served > 0


def test_back_to_back_truncations_fail_no_request(topic, cpus):
    """Two tables truncated in one VACUUM on a half-starved broker: requests wait out both."""
    fill_and_empty_offsets()
    fill_and_empty_producer_batches()
    cpus(0.5)
    h, t0, t1 = vacuum_with_hammer(["kafgres_offsets", "kafgres_producer_batches"])
    assert not h.failures, f"{len(h.failures)} failed, first {h.failures[0][1:]} " \
                           f"({h.failures[0][0] - t0:+.2f}s into a {t1 - t0:.2f}s vacuum)"


def test_vacuum_full_does_not_stall_the_loop(topic):
    """VACUUM FULL holds ACCESS EXCLUSIVE for its whole rewrite and never yields. Requests
    behind it wait outside the lock manager: none fail, the loop does not stall, and they go
    through once the rewrite is done."""
    sql(f"DELETE FROM kafgres_offsets WHERE group_id LIKE '{FILLER}-%'")
    sql(f"""INSERT INTO kafgres_offsets (group_id, topic_id, partition, committed_offset, metadata)
            SELECT '{FILLER}-' || g, 0, 0, 0, repeat('x', 500)
            FROM generate_series(1, 600000) g""")
    pid = broker_pid()
    try:
        with Hammer() as h:
            t0 = time.monotonic()
            sql("VACUUM FULL kafgres_offsets")
            t1 = time.monotonic()
            time.sleep(1.5)
            t2 = time.monotonic()
    finally:
        sql(f"DELETE FROM kafgres_offsets WHERE group_id LIKE '{FILLER}-%'")
    assert t1 - t0 > 1.0, f"VACUUM FULL took {t1 - t0:.2f}s: too short to wedge anything"
    assert broker_pid() == pid, "the broker worker restarted"
    assert h.probe_worst < 1.0, f"the loop stalled {h.probe_worst:.2f}s behind VACUUM FULL"
    assert not h.failures, f"{len(h.failures)} failed, first {h.failures[0][1:]}"
    assert h.served_between(t1, t2), "nothing went through once the rewrite was done"


def test_a_held_lock_delays_requests_and_its_release_frees_them_at_once(topic):
    """An operator holds ACCESS EXCLUSIVE on a table every request locks, for less than the
    retry limit. Nothing fails, nothing needing the table is served while it is held, and
    everything waiting goes through on release."""
    pid = broker_pid()
    with Hammer() as h:
        with Holder() as held:
            locked = time.monotonic()
            time.sleep(3)
            released = held.release()
        time.sleep(1.5)
    assert broker_pid() == pid, "the broker worker restarted"
    assert not h.failures, f"{len(h.failures)} failed, first {h.failures[0][1:]}"
    assert not h.served_between(locked + 0.2, released - 0.2), "served past a held lock"
    assert h.served_between(released, released + 0.5), "nothing went through on release"
    assert h.probe_worst < 0.5, f"the loop stalled {h.probe_worst:.2f}s behind a held lock"


def test_a_parked_fetch_is_answered_across_a_truncation(topic):
    """Parked fetches complete from the loop, under the same locks, when a truncation can
    be holding them. Each must be answered, without an error and on its connection."""
    fill_and_empty_offsets()
    conns = [socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=30)
             for _ in range(8)]
    try:
        for i, s in enumerate(conns):
            Connection(s).send(FETCH, 11, 700 + i, body=_fetch_v11(TOPIC, 2500))
        with Hammer(kinds=("metadata", "offset_fetch")) as h:
            sql("VACUUM kafgres_offsets")
        errors = []
        for i, s in enumerate(conns):
            try:
                resp = Connection(s).recv()
                assert struct.unpack_from(">i", resp, 0)[0] == 700 + i
                if _fetch_error(resp):
                    errors.append(f"fetch {i}: error {_fetch_error(resp)}")
            except Exception as e:
                errors.append(f"fetch {i}: {e!r}")
    finally:
        for s in conns:
            s.close()
    assert not errors, errors
    assert not h.failures, f"{len(h.failures)} failed, first {h.failures[0][1:]}"


class Holder:
    """An operator's session holding ACCESS EXCLUSIVE on a table every request locks."""

    def __init__(self, table="kafgres_offsets"):
        self.table = table

    def __enter__(self):
        self.p = subprocess.Popen(
            ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            text=True)
        self.p.stdin.write(f"BEGIN;\nLOCK TABLE {self.table} IN ACCESS EXCLUSIVE MODE;\n")
        self.p.stdin.flush()
        time.sleep(1.0)
        return self

    def release(self):
        if self.p.poll() is None:
            self.p.stdin.write("ROLLBACK;\n\\q\n")
            self.p.stdin.flush()
            self.p.wait(timeout=30)
        return time.monotonic()

    def __exit__(self, *exc):
        self.release()


def _connect():
    return socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=30)


def _fetch_values(topic):
    """Every value in partition 0, in offset order."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "kafgres-clients", "kcat",
         "-b", f"{BROKER_HOST}:{BROKER_PORT}", "-t", topic, "-C", "-o", "beginning", "-e",
         "-f", "%s\n"], capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    return out.stdout.split()


def test_requests_behind_a_held_lock_run_in_the_order_sent(topic):
    """A connection pipelines produces, one with acks=0, and a Metadata while a lock is held.
    The first waits for the lock; the rest must not run ahead of it, or the log reads out of
    order, and the answers must come back in the order asked."""
    with _connect() as s:
        c = Connection(s)
        with Holder() as held:
            for corr, value, acks in [(1, b"a", 1), (2, b"b", 0), (3, b"c", 1)]:
                c.send(PRODUCE, 3, corr, body=produce_v3(TOPIC, 0, record_batch([value]), acks))
            c.send(METADATA, 1, 4, body=struct.pack(">i", -1))
            s.settimeout(1.0)
            with pytest.raises(socket.timeout):
                c.recv()  # nothing may be answered while the lock is held
            s.settimeout(30)
            held.release()
            got = [struct.unpack_from(">i", c.recv(), 0)[0] for _ in range(3)]
    assert got == [1, 3, 4], f"answered out of order: {got}"
    assert _fetch_values(TOPIC) == ["a", "b", "c"], "the log was written out of order"


def test_a_lock_held_past_the_retry_limit_fails_its_requests_and_nothing_else(topic):
    """A lock that never comes free: the request behind it must keep trying for the whole
    limit, not fail at once, then lose its connection; the loop must keep serving every
    other connection meanwhile, and serve normally once the lock goes."""
    pid = broker_pid()
    worst = 0.0
    with Holder() as held:
        s = _connect()
        Connection(s).send(METADATA, 1, 1, body=struct.pack(">i", -1))
        sent = time.monotonic()
        s.settimeout(0.1)
        closed_at = None
        with _connect() as p:
            probe = Connection(p)
            n = 0
            while time.monotonic() - sent < 13 and closed_at is None:
                n += 1
                t0 = time.monotonic()
                probe.send(API_VERSIONS, 0, n)
                probe.recv()
                worst = max(worst, time.monotonic() - t0)
                try:
                    if s.recv(1) == b"":
                        closed_at = time.monotonic() - sent
                except socket.timeout:
                    pass
                except ConnectionError:
                    closed_at = time.monotonic() - sent
        s.close()
        held.release()
    assert closed_at is not None, "the request behind a held lock was never given up on"
    assert closed_at > 8, f"gave up after {closed_at:.1f}s: it did not retry"
    assert worst < 0.5, f"the loop stalled {worst:.2f}s while a request retried"
    assert broker_pid() == pid, "the broker worker restarted"
    with _connect() as s:
        c = Connection(s)
        c.send(METADATA, 1, 9, body=struct.pack(">i", -1))
        assert struct.unpack_from(">i", c.recv(), 0)[0] == 9


def test_clients_that_leave_while_retrying_are_forgotten(topic):
    """Connections close while their requests wait for a lock. When it comes free nothing
    may be answered into a closed socket, and the worker must not trip over the leftovers."""
    pid = broker_pid()
    with Holder() as held:
        socks = [_connect() for _ in range(32)]
        for i, s in enumerate(socks):
            Connection(s).send(PRODUCE, 3, i, body=produce_v3(TOPIC, 0, record_batch([b"gone"])))
        time.sleep(0.5)
        for s in socks:
            s.close()
        time.sleep(0.5)
        held.release()
    time.sleep(0.5)
    assert broker_pid() == pid, "the broker worker restarted"
    assert "gone" not in _fetch_values(TOPIC), "a request ran after its client left"
    with _connect() as s:
        c = Connection(s)
        c.send(METADATA, 1, 9, body=struct.pack(">i", -1))
        assert struct.unpack_from(">i", c.recv(), 0)[0] == 9


def test_many_retrying_requests_do_not_slow_the_loop(topic):
    """Every waiting request is retried every pass. Hundreds of them, behind one held lock,
    must not turn each pass into hundreds of failed transactions that stall everyone."""
    with Holder() as held:
        socks = [_connect() for _ in range(300)]
        for i, s in enumerate(socks):
            Connection(s).send(METADATA, 1, i, body=struct.pack(">i", -1))
        time.sleep(0.5)
        worst, n = 0.0, 0
        with _connect() as p:
            probe = Connection(p)
            end = time.monotonic() + 3
            while time.monotonic() < end:
                n += 1
                t0 = time.monotonic()
                probe.send(API_VERSIONS, 0, n)
                probe.recv()
                worst = max(worst, time.monotonic() - t0)
                time.sleep(0.02)
        held.release()
        answered = 0
        for i, s in enumerate(socks):
            c = Connection(s)
            if struct.unpack_from(">i", c.recv(), 0)[0] == i:
                answered += 1
            s.close()
    assert answered == 300, f"{300 - answered} of the waiting requests were not answered"
    assert worst < 0.1, f"the loop took {worst * 1000:.0f} ms to answer ApiVersions " \
                        f"with 300 requests retrying"


def test_parked_fetches_past_their_deadline_behind_a_held_lock_do_not_slow_the_loop(topic):
    """Hundreds of consumers long-poll an empty topic, and their deadlines pass while a lock
    is held. Each must try again behind the lock without the loop making every pass a few
    hundred failed reads, and each must be answered once it is released."""
    socks = [_connect() for _ in range(300)]
    try:
        for i, s in enumerate(socks):
            Connection(s).send(FETCH, 11, i, body=_fetch_v11(TOPIC, 300))
        with Holder() as held:
            worst, n = 0.0, 0
            with _connect() as p:
                probe = Connection(p)
                end = time.monotonic() + 3
                while time.monotonic() < end:
                    n += 1
                    t0 = time.monotonic()
                    probe.send(API_VERSIONS, 0, n)
                    probe.recv()
                    worst = max(worst, time.monotonic() - t0)
                    time.sleep(0.02)
            held.release()
        errors = []
        for i, s in enumerate(socks):
            resp = Connection(s).recv()
            if struct.unpack_from(">i", resp, 0)[0] != i or _fetch_error(resp):
                errors.append(i)
    finally:
        for s in socks:
            s.close()
    assert not errors, f"{len(errors)} parked fetches were not answered cleanly"
    assert worst < 0.1, f"the loop took {worst * 1000:.0f} ms to answer ApiVersions " \
                        f"with 300 expired fetches behind a held lock"


def _restart_postgres():
    subprocess.run(["docker", "compose", "restart", "postgres"], check=True,
                   capture_output=True, timeout=180)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if broker_pid():
                return
        except RuntimeError:
            pass  # still starting
        time.sleep(0.5)
    raise AssertionError("the broker worker did not start")


@pytest.fixture
def prepared_transactions():
    """A prepared transaction keeps its locks across a restart: the one way to have a table
    lock held while the broker worker starts."""
    if sql("SHOW max_prepared_transactions") == "0":
        sql("ALTER SYSTEM SET max_prepared_transactions = 4")
        _restart_postgres()
    yield
    sql("ALTER SYSTEM RESET max_prepared_transactions")
    _restart_postgres()


def test_startup_reconciliation_waits_out_a_held_lock(topic, prepared_transactions):
    """A crash can leave a marker past the recovered log end; the worker drops it at start,
    before serving. If a table lock is held then, it must wait for the lock rather than give
    up: a stale marker makes the next batch at its offset read as committed."""
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{TOPIC}'")
    sql(f"INSERT INTO kafgres_markers VALUES ({tid}, 0, 5, 5, 100)")

    def markers():
        return int(sql(f"SELECT count(*) FROM kafgres_markers WHERE topic_id = {tid}"))

    gid = "hunt-lockwait-startup"
    try:
        sql(f"BEGIN; LOCK TABLE kafgres_offsets IN ACCESS EXCLUSIVE MODE; "
            f"PREPARE TRANSACTION '{gid}';")
        _restart_postgres()
        time.sleep(3)  # long enough to have tried reconciliation behind the lock
        assert markers() == 1, "reconciled through a held lock: test is vacuous"
        sql(f"ROLLBACK PREPARED '{gid}'")
        deadline = time.monotonic() + 10
        while markers() and time.monotonic() < deadline:
            time.sleep(0.5)
        assert markers() == 0, "the stale marker survived a start behind a held lock"
    finally:
        if sql(f"SELECT count(*) FROM pg_prepared_xacts WHERE gid = '{gid}'") != "0":
            sql(f"ROLLBACK PREPARED '{gid}'")
        sql(f"DELETE FROM kafgres_markers WHERE topic_id = {tid}")


def test_a_group_member_outlives_a_lock_held_past_its_session_timeout(topic):
    """A consumer's heartbeat waits behind a lock held longer than its session timeout. It
    must land before the member sweep judges the member silent, so the group keeps its
    generation instead of rebalancing a member that never stopped heartbeating."""
    from kafka import KafkaConsumer

    group = "hunt-lockwait-session"
    stop = threading.Event()
    errors = []

    def consume():
        try:
            c = KafkaConsumer(TOPIC, bootstrap_servers=f"{BROKER_HOST}:{BROKER_PORT}",
                              group_id=group, session_timeout_ms=6000,
                              heartbeat_interval_ms=1000, enable_auto_commit=False)
            while not stop.is_set():
                c.poll(timeout_ms=200)
            c.close(autocommit=False)
        except Exception as e:
            errors.append(repr(e))

    def member():
        return sql(f"SELECT g.generation || ' ' || m.member_id FROM kafgres_groups g "
                   f"JOIN kafgres_group_members m USING (group_id) "
                   f"WHERE g.group_id = '{group}' AND g.state = 'Stable'")

    t = threading.Thread(target=consume)
    t.start()
    try:
        deadline = time.monotonic() + 30
        while not member() and time.monotonic() < deadline:
            time.sleep(0.5)
        before = member()
        assert before, "the consumer never joined"
        with Holder() as held:
            time.sleep(8)
            held.release()
        time.sleep(4)
        after = member()
    finally:
        stop.set()
        t.join(timeout=30)
    assert not errors, errors
    assert after == before, f"the member was rebalanced: {before!r} -> {after!r}"


def test_parked_frames_count_against_the_inbound_cap(topic):
    """Every connection sends a large Produce while a lock is held, and each one parks. A
    parked frame stays allocated, so it must stay counted: past the 512 MiB inbound cap,
    further frames must wait unread rather than be taken off the socket and parked."""
    n, size = 120, 5 * 1024 * 1024
    cap = 512 * 1024 * 1024
    body = produce_v3(TOPIC, 0, record_batch([b"x" * size]))
    header = struct.pack(">hhi", PRODUCE, 3, 1) + struct.pack(">h", 6) + b"pytest"
    frame = struct.pack(">i", len(header) + len(body)) + header + body
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 1))
    answered = [0]
    lock = threading.Lock()

    def client():
        with _connect() as s:
            s.settimeout(120)
            s.sendall(frame)
            if struct.unpack_from(">i", Connection(s).recv(), 0)[0] == 1:
                with lock:
                    answered[0] += 1

    threads = [threading.Thread(target=client) for _ in range(n)]
    with Holder() as held:
        for t in threads:
            t.start()
        time.sleep(8)
        logs = subprocess.run(["docker", "compose", "logs", "--since", since, "postgres"],
                              capture_output=True, text=True, timeout=60).stdout
        parked = sum(1 for ln in logs.splitlines() if "api 0 v3" in ln and "retrying" in ln)
        held.release()
    for t in threads:
        t.join(timeout=150)
    assert parked > 1, "nothing parked: test is vacuous"
    assert parked * len(frame) <= cap + len(frame), \
        f"{parked} frames of {len(frame)} bytes parked, past the {cap}-byte inbound cap"
    assert answered[0] == n, f"{n - answered[0]} of {n} produces were never answered"
