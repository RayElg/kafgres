"""Produce, Fetch, ListOffsets.

The client round-trip tests are the acceptance criteria. The byte-level tests below
them are the ones no client will ever catch: a log that is not dense fails silently by
construction, because gaps in a Kafka log are legal and nothing errors on one.
"""

import concurrent.futures
import time
import struct
import subprocess

import pytest

from conftest import sql

CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def kcat(*args, stdin=None, timeout=120):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER, *args],
        input=stdin,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

@pytest.fixture
def topic(request):
    name = f"p2-{request.node.name.replace('_', '-')[:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def make(name, partitions=1):
    sql(f"SELECT kafgres_create_topic('{name}', {partitions})")

def test_produce_consume_roundtrip(topic):
    make(topic)
    out = kcat("-t", topic, "-P", stdin="alpha\nbeta\ngamma\n")
    assert out.returncode == 0, out.stderr

    out = kcat("-t", topic, "-C", "-o", "beginning", "-e")
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["alpha", "beta", "gamma"]

def test_roundtrip_survives_a_broker_restart(topic):
    """The log is durable, not in-memory."""
    make(topic)
    assert kcat("-t", topic, "-P", stdin="before\n").returncode == 0

    subprocess.run(["docker", "compose", "restart", "postgres"], capture_output=True, timeout=180)
    for _ in range(60):
        r = subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres", "pg_isready", "-U", "postgres"],
            capture_output=True,
        )
        if r.returncode == 0:
            break
    else:
        pytest.fail("broker did not come back")

    assert kcat("-t", topic, "-P", stdin="after\n").returncode == 0
    out = kcat("-t", topic, "-C", "-o", "beginning", "-e")
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["before", "after"]

def test_consume_from_beginning_reads_the_full_log(topic):
    make(topic)
    payload = "".join(f"m{i}\n" for i in range(200))
    assert kcat("-t", topic, "-P", stdin=payload).returncode == 0

    out = kcat("-t", topic, "-C", "-o", "beginning", "-e")
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == [f"m{i}" for i in range(200)]

def test_list_offsets_reports_earliest_and_latest(topic):
    make(topic)
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\nd\n").returncode == 0

    latest = kcat("-Q", "-t", f"{topic}:0:-1")
    earliest = kcat("-Q", "-t", f"{topic}:0:-2")
    assert "offset 4" in latest.stdout, latest.stdout
    assert "offset 0" in earliest.stdout, earliest.stdout

def test_multiple_partitions_each_start_at_zero(topic):
    """Offsets are per-partition. A scheme keyed on offset alone would collide."""
    make(topic, partitions=3)
    for p in range(3):
        assert kcat("-t", topic, "-P", "-p", str(p), stdin=f"part{p}\n").returncode == 0
    for p in range(3):
        out = kcat("-Q", "-t", f"{topic}:{p}:-1")
        assert "offset 1" in out.stdout, f"partition {p}: {out.stdout}"

def test_java_producer_then_librdkafka_consumer(topic):
    """Cross-client interop, which is the only thing that proves the wire format rather
    than our own encoder agreeing with our own decoder.

    Idempotence is disabled because `InitProducerId` is not under test here; a
    default-config Java producer still fails at construction. Produce v13 addresses
    topics by uuid, so this also covers the id path — echoing a zero uuid makes the
    Java client throw "Can't find batch created for topic id".
    """
    make(topic)
    out = subprocess.run(
        [
            "docker", "run", "--rm", "-i", "--network", "host", "apache/kafka:4.1.0",
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", BROKER, "--topic", topic,
            "--producer-property", "enable.idempotence=false",
            "--producer-property", "acks=1",
        ],
        input="java-a\njava-b\n",
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert "Exception" not in out.stderr, out.stderr

    back = kcat("-t", topic, "-C", "-o", "beginning", "-e")
    assert back.returncode == 0, back.stderr
    assert back.stdout.split() == ["java-a", "java-b"]

engine_a_storage = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "table",
    reason="engine A only: reads kafgres_log; I1/I2 have no cross-engine assertion yet",
)

@engine_a_storage
def test_i1_offsets_are_dense_under_concurrent_producers(topic):
    """Offsets must be dense under concurrent producers.

    A gap here causes an intermittent consumer stall much later, never a failure at the
    point of the bug: gaps are legal in Kafka (compaction, aborted transactions), so
    nothing in the protocol errors on one. Single-producer tests never reproduce it.
    """
    make(topic)
    writers, per_writer = 8, 25

    def produce(w):
        body = "".join(f"w{w}-{i}\n" for i in range(per_writer))
        return kcat("-t", topic, "-P", stdin=body).returncode

    with concurrent.futures.ThreadPoolExecutor(max_workers=writers) as pool:
        codes = list(pool.map(produce, range(writers)))
    assert set(codes) == {0}, "a producer failed"

    rows = sql(
        f"""SELECT base_offset, last_offset FROM kafgres_log
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')
             ORDER BY base_offset"""
    )
    spans = [tuple(map(int, line.split("|"))) for line in rows.splitlines() if line.strip()]
    assert spans, "nothing was written"

    expected = 0
    for base, last in spans:
        assert base == expected, f"offset gap: expected {base} to be {expected}"
        assert last >= base, f"batch at {base} ends before it starts ({last})"
        expected = last + 1

    total = writers * per_writer
    assert expected == total, f"expected {total} records, log ends at {expected}"

    headers = sql(
        f"""SELECT base_offset, ('x' || encode(substring(batch from 1 for 8), 'hex'))::bit(64)::bigint
             FROM kafgres_log
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')
             ORDER BY base_offset"""
    )
    for line in headers.splitlines():
        if not line.strip():
            continue
        col, stamped = (int(x) for x in line.split("|"))
        assert col == stamped, (
            f"row says base_offset={col} but the batch header says {stamped}; "
            "the offset was stamped outside the row lock"
        )

def test_i1_concurrent_producers_lose_no_records(topic):
    """Density asserted through LogStore, so it holds on both engines.

    The concurrent multi-writer check must run on the segment engine too, where
    producers serialise on a shared-memory shard lock across three independent writer
    processes (the broker worker, `kafgres_produce()` from any backend, and the CDC
    worker), rather than only where a Postgres row lock does the work.

    What this cannot see is a batch header disagreeing with its offset — that needs the
    stored bytes.
    """
    make(topic)
    writers, per_writer = 8, 25
    total = writers * per_writer

    def produce(w):
        body = "".join(f"w{w}-{i}\n" for i in range(per_writer))
        return kcat("-t", topic, "-P", stdin=body).returncode

    with concurrent.futures.ThreadPoolExecutor(max_workers=writers) as pool:
        codes = list(pool.map(produce, range(writers)))
    assert set(codes) == {0}, "a producer failed"

    hw = int(sql(f"""SELECT high_watermark FROM kafgres_partition_offsets('{topic}')
                      WHERE partition = 0"""))
    assert hw == total, f"high watermark {hw} disagrees with the {total} records produced"

    out = kcat("-t", topic, "-C", "-o", "beginning", "-e", timeout=180)
    assert out.returncode == 0, out.stderr
    got = out.stdout.split()
    assert len(got) == total, f"consumed {len(got)} of {total}"
    assert len(set(got)) == total, "duplicate records"

@engine_a_storage
def test_i2_stored_bytes_are_the_bytes_the_producer_sent(topic):
    """Byte-verbatim storage.

    Checked at the storage layer rather than end to end: a re-encode that happened to
    round-trip would still break idempotent-producer sequence tracking and cost the
    throughput the design depends on.
    """
    make(topic)
    assert kcat("-t", topic, "-P", stdin="verbatim\n").returncode == 0

    row = sql(
        f"""SELECT encode(batch, 'hex') FROM kafgres_log
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip()
    raw = bytes.fromhex(row)

    magic = raw[16]
    assert magic == 2, f"only RecordBatch v2 is stored, got magic {magic}"

    stored_crc = struct.unpack_from(">I", raw, 17)[0]
    assert stored_crc == crc32c(raw[21:]), "CRC does not match — the batch was re-encoded"

    assert struct.unpack_from(">q", raw, 0)[0] == 0, "baseOffset was not stamped"
    assert struct.unpack_from(">i", raw, 12)[0] == 0, "partitionLeaderEpoch was not stamped"

@engine_a_storage
def test_table_engine_rows_land_in_kafgres_log(topic):
    """The in-suite proof that the table engine really stored what produce sent.

    This used to be a CI assertion after the suites (`SELECT count(*) FROM
    kafgres_log != 0`), which failed vacuously: every test drops its own topic in
    teardown and a drop removes the rows, so a fully green run ends with an empty
    table. Here the count is scoped to this test's topic and runs before teardown,
    so a produce that silently went to the segment engine fails right here.
    """
    make(topic)
    assert kcat("-t", topic, "-P", stdin="stored\n").returncode == 0

    count = sql(
        f"""SELECT count(*) FROM kafgres_log
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    )
    assert count != "0", "produce stored nothing in kafgres_log"

def test_i8_a_huge_fetch_request_is_capped(topic):
    """The client's byte budget is a request, not an instruction: fetch.max.bytes
    defaults to 50MB and a client may raise it, and honouring that inside a Postgres
    backend is the OOM this cap exists to prevent."""
    make(topic)
    payload = "".join(f"{'x' * 500}-{i}\n" for i in range(500))
    assert kcat("-t", topic, "-P", stdin=payload).returncode == 0

    out = kcat(
        "-t", topic, "-C", "-o", "beginning", "-e",
        "-X", "fetch.max.bytes=200000000",
        "-X", "max.partition.fetch.bytes=100000000",
        timeout=180,
    )
    assert out.returncode == 0, out.stderr
    assert len(out.stdout.split()) == 500, "capping must not lose records"

def test_fetch_past_the_end_reports_offset_out_of_range(topic):
    """OFFSET_OUT_OF_RANGE is what drives auto.offset.reset. A wrong code here makes a
    consumer spin instead of resetting."""
    make(topic)
    assert kcat("-t", topic, "-P", stdin="one\n").returncode == 0

    out = kcat("-t", topic, "-C", "-o", "9999", "-e", "-X", "auto.offset.reset=error")
    combined = out.stdout + out.stderr
    assert "OFFSET_OUT_OF_RANGE" in combined.upper() or "OUT OF RANGE" in combined.upper(), combined

def crc32c(data):
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ (0x82F63B78 if c & 1 else 0)
        table.append(c)
    c = 0xFFFFFFFF
    for b in data:
        c = table[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF

def fetch_v11_body(topic, partition=0, offset=0, max_wait_ms=500, min_bytes=1):
    """A Fetch v11 request body. v11 is the newest non-flexible version, so the test
    asserts bytes without also having to encode compact types."""
    t = topic.encode()
    b = struct.pack(">iiii", -1, max_wait_ms, min_bytes, 1048576) + struct.pack(">b", 0)
    b += struct.pack(">ii", 0, -1)                       # session_id, session_epoch
    b += struct.pack(">i", 1) + struct.pack(">h", len(t)) + t
    b += struct.pack(">i", 1)
    b += struct.pack(">i", partition) + struct.pack(">i", -1)
    b += struct.pack(">q", offset) + struct.pack(">q", 0) + struct.pack(">i", 1048576)
    b += struct.pack(">i", 0)                            # forgotten_topics
    b += struct.pack(">h", 0)                            # rack_id
    return b

def test_fetch_with_nothing_to_send_is_held_not_answered_immediately(topic, conn):
    """Answering an empty fetch at once is legal but makes every idle consumer spin:
    librdkafka applies no backoff after a *successful* response, so it re-fetches
    instantly."""
    make(topic)
    from conftest import FETCH

    t0 = time.monotonic()
    conn.send(FETCH, 11, 4242, body=fetch_v11_body(topic, max_wait_ms=800))
    conn.recv()
    held = time.monotonic() - t0

    assert held >= 0.6, f"fetch returned after {held:.3f}s; it was not held"
    assert held < 3.0, f"fetch held {held:.3f}s, far past its own deadline"

def test_a_produce_wakes_a_parked_fetch_early(topic, conn):
    """The doorbell. Without it a parked fetch waits out its full deadline and every
    message costs up to fetch.max.wait.ms of latency — which would make parking a
    latency regression rather than an efficiency win."""
    make(topic)
    from conftest import FETCH

    conn.send(FETCH, 11, 555, body=fetch_v11_body(topic, max_wait_ms=5000))
    time.sleep(0.4)  # let it park

    t0 = time.monotonic()
    assert kcat("-t", topic, "-P", stdin="wakeup\n").returncode == 0
    resp = conn.recv()
    woke = time.monotonic() - t0

    assert woke < 2.5, f"took {woke:.3f}s — looks like it waited for the deadline"
    assert b"wakeup" in resp, "the response should carry the record that woke it"

def test_a_parked_fetch_does_not_let_later_responses_overtake_it(topic, conn):
    """Kafka guarantees responses arrive in request order per connection.

    A Metadata sent *behind* a parked Fetch must not be answered first. librdkafka pairs
    responses to requests positionally, so an out-of-order reply is read as the wrong
    response entirely — the Fetch's bytes parsed as Metadata — rather than merely an
    early one.
    """
    make(topic)
    from conftest import FETCH, METADATA

    conn.send(FETCH, 11, 1001, body=fetch_v11_body(topic, max_wait_ms=1500))
    conn.send(METADATA, 1, 1002, body=struct.pack(">i", 0))

    first = conn.recv()
    second = conn.recv()

    assert struct.unpack_from(">i", first, 0)[0] == 1001, "Metadata overtook a parked Fetch"
    assert struct.unpack_from(">i", second, 0)[0] == 1002

def test_max_wait_zero_is_answered_immediately(topic, conn):
    """A client that asks not to wait must not be parked."""
    make(topic)
    from conftest import FETCH

    t0 = time.monotonic()
    conn.send(FETCH, 11, 7777, body=fetch_v11_body(topic, max_wait_ms=0))
    conn.recv()
    assert time.monotonic() - t0 < 0.5, "max_wait_ms=0 must not park"

def test_closing_a_connection_with_a_parked_fetch_is_clean(topic):
    """A parked entry outliving its connection would complete into a closed socket, and
    connection ids are reused — so it could deliver one peer's data to another."""
    make(topic)
    import socket as _socket
    from conftest import BROKER_HOST, BROKER_PORT, FETCH, Connection

    for _ in range(5):
        s = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=5)
        c = Connection(s)
        c.send(FETCH, 11, 1, body=fetch_v11_body(topic, max_wait_ms=5000))
        time.sleep(0.2)
        s.close()  # walk away while it is parked

    time.sleep(0.5)
    out = kcat("-L")
    assert out.returncode == 0, out.stderr

def test_unsupported_listoffsets_sentinel_is_refused_not_guessed(topic, conn):
    """v7's MAX_TIMESTAMP (-3) and v9's LATEST_TIERED_OFFSET (-4) are sentinels, not
    timestamps. Treating them as timestamps runs `max_timestamp >= -3`, which every
    batch satisfies, and returns the *earliest* offset with error_code NONE — a
    confidently wrong answer. We do not advertise those versions, but a client that
    sends one must be told, not guessed at."""
    make(topic)
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\n").returncode == 0
    from conftest import LIST_OFFSETS

    t = topic.encode()
    body = struct.pack(">i", -1)
    body += struct.pack(">i", 1) + struct.pack(">h", len(t)) + t
    body += struct.pack(">i", 1) + struct.pack(">i", 0) + struct.pack(">q", -3)
    conn.send(LIST_OFFSETS, 1, 31337, body=body)
    resp = conn.recv()

    pos = 4 + 4                       # correlation + topics count
    tl = struct.unpack_from(">h", resp, pos)[0]
    pos += 2 + tl
    pos += 4                          # partitions count
    _idx, err = struct.unpack_from(">ih", resp, pos)
    pos += 6
    _ts, offset = struct.unpack_from(">qq", resp, pos)

    assert err != 0, f"sentinel -3 answered with error_code 0 and offset {offset}"
    assert offset == -1

def test_a_fetch_naming_absurdly_many_partitions_is_refused(topic, conn):
    """A partition entry is ~33 bytes, so one legal request can name a quarter of a
    million of them — each costing a read and, if the fetch parks, memory held until its
    deadline. The request has to be bounded."""
    make(topic)
    from conftest import FETCH

    n = 20000
    t = topic.encode()
    body = struct.pack(">iiii", -1, 0, 1, 1048576) + struct.pack(">b", 0)
    body += struct.pack(">ii", 0, -1)
    body += struct.pack(">i", 1) + struct.pack(">h", len(t)) + t
    body += struct.pack(">i", n)
    for _ in range(n):
        body += struct.pack(">i", 0) + struct.pack(">i", -1)
        body += struct.pack(">q", 0) + struct.pack(">q", 0) + struct.pack(">i", 1048576)
    body += struct.pack(">i", 0) + struct.pack(">h", 0)

    conn.send(FETCH, 11, 4141, body=body)
    assert conn.closed(), "an oversized partition list should be refused"

def test_a_held_table_lock_does_not_wedge_the_broker(topic):
    """A held table lock must not wedge the broker, end to end.

    `kafgres_create_topic` and `kafgres_drop_topic` issue CREATE TABLE PARTITION OF and
    DROP TABLE, so an ordinary user session can hold ACCESS EXCLUSIVE on tables every
    request path reads. With one worker and one loop, a request blocked in the lock
    manager stops accepts, reads, parked-fetch completion and flushes for *every*
    connection — and pg_stat_activity still shows a healthy worker, so it presents as a
    total outage with no broker-side error.

    The lock is held for `HOLD` seconds, well past the broker's 2 s lock_timeout. A
    second connection must be served before the lock is released; if the broker were
    wedged it could only answer once the holder let go, so the timing is the assertion.

    Bounding the block is only half of it. The timeout raises a Postgres ERROR, which in
    a background worker is a longjmp — if it escapes, the worker exits and every
    connection drops, turning an indefinite stall into a crash-restart loop. So the test
    also asserts the worker never died.
    """
    make(topic)
    from conftest import API_VERSIONS, BROKER_HOST, BROKER_PORT, METADATA, Connection
    import socket as _socket

    HOLD = 12.0  # far longer than any timeout, so a wedged broker cannot pass

    holder = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        text=True,
    )
    blocked_sock = None
    try:
        holder.stdin.write(
            "BEGIN;\n"
            "LOCK TABLE kafgres_topics, kafgres_partitions IN ACCESS EXCLUSIVE MODE;\n"
        )
        holder.stdin.flush()
        time.sleep(1.0)
        t_locked = time.monotonic()

        blocked_sock = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=30)
        Connection(blocked_sock).send(METADATA, 1, 8001, body=struct.pack(">i", -1))
        time.sleep(0.3)
        t_probe = time.monotonic()

        s = _socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=30)
        try:
            probe = Connection(s)
            probe.send(API_VERSIONS, 0, 9090)
            resp = probe.recv()
            served_at = time.monotonic() - t_probe
            assert struct.unpack_from(">i", resp, 0)[0] == 9090
            assert served_at < 1.0, (
                f"probe took {served_at:.2f}s while another request held a lock conflict; "
                "the loop was blocked, not merely busy"
            )
        finally:
            s.close()
    finally:
        if blocked_sock is not None:
            blocked_sock.close()
        try:
            holder.stdin.write("ROLLBACK;\n\\q\n")
            holder.stdin.flush()
        except Exception:
            pass
        holder.terminate()
        holder.wait(timeout=30)

    logs = subprocess.run(
        ["docker", "compose", "logs", "postgres"], capture_output=True, text=True, timeout=60
    ).stdout
    assert "query aborted" in logs.lower(), "no lock conflict occurred — test is vacuous"
    deaths = [
        ln for ln in logs.splitlines()
        if "kafgres_broker" in ln and ("exited" in ln or "terminated" in ln)
    ]
    assert not deaths, f"the worker died on the query error: {deaths[:2]}"

    time.sleep(1.0)
    out = kcat("-L")
    assert out.returncode == 0, out.stderr
