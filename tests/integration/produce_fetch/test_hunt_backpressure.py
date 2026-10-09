"""Inbound budget at its edges: connections holding partial frames exhaust the shared
budget, and everyone must wait for it rather than be disconnected; a frame of exactly the
request limit must be served."""

import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import sql
from support.recordbatch import produce_v3, record_batch

HOST, PORT = "127.0.0.1", 9092
API_VERSIONS = 18


def produce_frame(topic, size, correlation):
    """A Produce v3 frame whose one batch carries `size` bytes of value: an answer of
    MESSAGE_TOO_LARGE is fine, the point is that there is an answer."""
    header = struct.pack(">hhi", 0, 3, correlation) + struct.pack(">h", 6) + b"pytest"
    body = produce_v3(topic, 0, record_batch([b"x" * size]))
    frame = header + body
    return struct.pack(">i", len(frame)) + frame


def read_frame(sock, timeout):
    sock.settimeout(timeout)
    head = b""
    while len(head) < 4:
        chunk = sock.recv(4 - len(head))
        if not chunk:
            raise ConnectionError("closed")
        head += chunk
    (n,) = struct.unpack(">i", head)
    body = b""
    while len(body) < n:
        chunk = sock.recv(min(1 << 20, n - len(body)))
        if not chunk:
            raise ConnectionError("closed mid-response")
        body += chunk
    return body


@pytest.fixture
def topic():
    name = "hunt-backpressure"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


def test_an_exhausted_budget_makes_clients_wait_not_disconnect(topic):
    """32 clients each send most of a 20 MiB frame and pause: together they want more than
    the 512 MiB budget. A client that does not fit must wait, its sends held back by TCP,
    rather than be disconnected; and every one, plus a small late client, is answered."""

    def holder(i):
        f = produce_frame(topic, 20 * 1024 * 1024, 9000 + i)
        with socket.create_connection((HOST, PORT), timeout=90) as s:
            s.sendall(f[: 16 * 1024 * 1024])
            time.sleep(3)
            s.sendall(f[16 * 1024 * 1024:])
            return struct.unpack_from(">i", read_frame(s, 90))[0]

    def late():
        time.sleep(2)
        request = struct.pack(">hhi", API_VERSIONS, 0, 9999) + struct.pack(">h", 6) + b"pytest"
        with socket.create_connection((HOST, PORT), timeout=90) as s:
            s.sendall(struct.pack(">i", len(request)) + request)
            return struct.unpack_from(">i", read_frame(s, 90))[0]

    with ThreadPoolExecutor(max_workers=33) as pool:
        holders = [pool.submit(holder, i) for i in range(32)]
        small = pool.submit(late)
        answers = [h.result() for h in holders]
        assert answers == [9000 + i for i in range(32)], answers
        assert small.result() == 9999, "the late client was not answered"


def test_a_frame_of_exactly_the_request_limit_is_served(topic):
    sql("ALTER SYSTEM SET kafgres.max_request_bytes = 1048576")
    sql("SELECT pg_reload_conf()")
    time.sleep(1.5)
    try:
        # A well-formed request whose records blob pads the frame to exactly the limit;
        # the blob is not a valid batch, and an error answer is as good as any.
        header = struct.pack(">hhi", 0, 3, 9100) + struct.pack(">h", 6) + b"pytest"
        pad = 1048576 - len(header + produce_v3(topic, 0, b""))
        body = header + produce_v3(topic, 0, b"\x00" * pad)
        assert len(body) == 1048576
        f = struct.pack(">i", len(body)) + body
        s = socket.create_connection((HOST, PORT), timeout=30)
        s.sendall(f)
        body = read_frame(s, 20)
        assert struct.unpack_from(">i", body)[0] == 9100
        s.close()
    finally:
        sql("ALTER SYSTEM RESET kafgres.max_request_bytes")
        sql("SELECT pg_reload_conf()")


def test_a_prefix_without_its_body_does_not_hold_the_budget(topic):
    """A frame's budget is claimed when its prefix arrives. A peer that declares a large
    frame and sends nothing more must be dropped, or a handful of them pin the budget."""
    s = socket.create_connection((HOST, PORT), timeout=60)
    s.sendall(struct.pack(">i", 20 * 1024 * 1024))
    s.settimeout(60)
    started = time.time()
    try:
        assert s.recv(1) == b"", "the broker sent something to a half-sent frame"
    except ConnectionResetError:
        pass
    assert time.time() - started < 50, "a stalled frame was held for too long"
    s.close()


def test_an_idle_connection_between_requests_is_kept(topic):
    """Silence between requests is not a stalled frame: a client idle longer than the stall
    timeout keeps its connection, as Kafka's minutes-long connections.max.idle.ms allows."""
    request = struct.pack(">hhi", API_VERSIONS, 0, 9201) + struct.pack(">h", 6) + b"pytest"
    with socket.create_connection((HOST, PORT), timeout=60) as s:
        s.sendall(struct.pack(">i", len(request)) + request)
        assert struct.unpack_from(">i", read_frame(s, 30))[0] == 9201
        time.sleep(35)
        again = struct.pack(">hhi", API_VERSIONS, 0, 9202) + struct.pack(">h", 6) + b"pytest"
        s.sendall(struct.pack(">i", len(again)) + again)
        assert struct.unpack_from(">i", read_frame(s, 30))[0] == 9202, "the idle client was dropped"


@pytest.mark.parametrize("length", [0, -1])
def test_an_empty_or_negative_frame_closes_only_its_connection(topic, length):
    """A frame with no body is complete at its prefix. It must be refused like any bad frame,
    not desynchronise the admission accounting and take the broker down."""
    broker = "SELECT pid FROM pg_stat_activity WHERE backend_type = 'kafgres_broker'"
    before = sql(broker)
    s = socket.create_connection((HOST, PORT), timeout=30)
    s.sendall(struct.pack(">i", length)[:2])
    time.sleep(0.3)
    s.sendall(struct.pack(">i", length)[2:])
    s.settimeout(30)
    try:
        assert s.recv(1) == b"", "the broker answered a frame with no request in it"
    except ConnectionResetError:
        pass
    s.close()
    request = struct.pack(">hhi", API_VERSIONS, 0, 9301) + struct.pack(">h", 6) + b"pytest"
    with socket.create_connection((HOST, PORT), timeout=30) as other:
        other.sendall(struct.pack(">i", len(request)) + request)
        assert struct.unpack_from(">i", read_frame(other, 30))[0] == 9301
    assert sql(broker) == before, "the broker worker restarted"
