"""The idempotent producer.

The acceptance criterion is that a vanilla Java `KafkaProducer` with default
configuration works unmodified — which it could not without `InitProducerId`, because
`enable.idempotence` has defaulted true since Kafka 3.0 and the client fails at
*construction* without it.

The rest of these send batches by hand. Replaying a batch byte-for-byte is exactly what
deduplication has to survive, and no client library will do it on purpose.
"""

import struct
import subprocess

import pytest

from conftest import sql
from recordbatch import (
    parse_produce_v3,
    parse_produce_v3_many,
    produce_v3,
    produce_v3_many,
    record_batch,
)

CLIENTS = "kafgres-clients"
KAFKA = "apache/kafka:4.1.0"
BROKER = "127.0.0.1:9092"

PRODUCE = 0
INIT_PRODUCER_ID = 22
OUT_OF_ORDER_SEQUENCE_NUMBER = 45
INVALID_PRODUCER_EPOCH = 47
INVALID_RECORD = 87

def kcat(*args, stdin=None, timeout=180):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER, *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

@pytest.fixture
def topic(request):
    name = f"p4-{request.node.name.replace('_', '-')[:38]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def send_produce(conn, topic, batch, correlation):
    """Produce v3, whose request header is v1."""
    header = struct.pack(">hhi", PRODUCE, 3, correlation)
    header += struct.pack(">h", 6) + b"pytest"
    frame = header + produce_v3(topic, 0, batch)
    conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
    return parse_produce_v3(conn.recv())

def log_rows(topic):
    """Batches stored for the topic — deliberately batches, not records.

    The idempotent producer deduplicates *batches*, so "the batch was appended twice" is
    the assertion this whole file makes and a record count cannot express it: one batch
    stored twice and one batch with twice the records are the same number of records and
    different bugs, and `kafgres_partition_offsets` does not distinguish them. It stays
    on `kafgres_log`, which only the table engine has.
    """
    return int(sql(
        f"""SELECT count(*) FROM kafgres_log
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip())

engine_a_storage = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "table",
    reason="engine A only: asserts on batch counts in kafgres_log (see log_rows)",
)

def test_vanilla_java_producer_with_default_config(topic):
    """No `--producer-property` overrides: idempotence is on, which fails at
    construction without `InitProducerId`."""
    out = subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", BROKER, "--topic", topic],
        input="v1\nv2\nv3\n", capture_output=True, text=True, timeout=300,
    )
    assert "Exception" not in out.stderr, out.stderr
    assert "INIT_PRODUCER_ID" not in out.stderr, out.stderr

    back = kcat("-t", topic, "-C", "-o", "beginning", "-e")
    assert back.stdout.split() == ["v1", "v2", "v3"], back.stderr

    allocated = int(sql("SELECT count(*) FROM kafgres_producers").strip())
    assert allocated >= 1, "the client never called InitProducerId"

def test_idempotent_producer_end_to_end(topic):
    """Both Java tools, default configuration, no client library in the middle."""
    group = f"{topic}-g"
    out = subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", BROKER, "--topic", topic],
        input="j1\nj2\nj3\n", capture_output=True, text=True, timeout=300,
    )
    assert "Exception" not in out.stderr, out.stderr

    back = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", topic, "--from-beginning", "--timeout-ms", "20000",
         "--group", group],
        capture_output=True, text=True, timeout=300,
    )
    assert back.stdout.split() == ["j1", "j2", "j3"], back.stdout + back.stderr

def test_init_producer_id_allocates_distinct_ids(conn):
    """Every plain idempotent producer gets a fresh id at epoch 0. Handing two producers
    the same id would make one's sequences look like the other's retries, and the loser's
    records would be silently discarded as duplicates."""
    seen = set()
    for i in range(3):
        header = struct.pack(">hhi", INIT_PRODUCER_ID, 1, 900 + i)
        header += struct.pack(">h", 6) + b"pytest"
        body = struct.pack(">h", -1)            # transactional_id null
        body += struct.pack(">i", 60000)        # transaction_timeout_ms
        frame = header + body
        conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
        resp = conn.recv()

        correlation, throttle, error = struct.unpack_from(">iih", resp, 0)
        producer_id, epoch = struct.unpack_from(">qh", resp, 10)
        assert correlation == 900 + i
        assert error == 0, f"InitProducerId failed with {error}"
        assert epoch == 0, "a fresh idempotent producer starts at epoch 0"
        seen.add(producer_id)

    assert len(seen) == 3, f"ids must be distinct, got {seen}"

@engine_a_storage
def test_a_replayed_batch_is_not_appended_twice(topic, conn):
    """A producer that does not see its ack resends the identical batch. The broker must
    recognise it and answer with the offset it already assigned — appending again would
    duplicate the records, and nothing downstream would ever notice.
    """
    batch = record_batch([b"a", b"b", b"c"], producer_id=7000, producer_epoch=0, base_sequence=0)

    _, err1, off1 = send_produce(conn, topic, batch, 1001)
    assert err1 == 0, f"first append failed with {err1}"

    _, err2, off2 = send_produce(conn, topic, batch, 1002)
    assert err2 == 0, f"a retry must succeed, not error: {err2}"
    assert off2 == off1, f"a retry must report the original offset: {off1} then {off2}"

    assert log_rows(topic) == 1, "the batch was appended twice"
    back = kcat("-t", topic, "-C", "-o", "beginning", "-e")
    assert back.stdout.split() == ["a", "b", "c"], f"records duplicated: {back.stdout.split()}"

@engine_a_storage
def test_sequences_advance_normally(topic, conn):
    batch0 = record_batch([b"x"], producer_id=7100, producer_epoch=0, base_sequence=0)
    batch1 = record_batch([b"y"], producer_id=7100, producer_epoch=0, base_sequence=1)

    _, e0, o0 = send_produce(conn, topic, batch0, 2001)
    _, e1, o1 = send_produce(conn, topic, batch1, 2002)
    assert (e0, e1) == (0, 0)
    assert o1 == o0 + 1, f"offsets should advance: {o0} then {o1}"
    assert log_rows(topic) == 2

@engine_a_storage
def test_a_gap_in_sequence_is_refused(topic, conn):
    """OUT_OF_ORDER_SEQUENCE_NUMBER. Accepting the gap would leave a hole the producer
    believes it filled, and its own retry logic would never resend the missing batch."""
    ok = record_batch([b"x"], producer_id=7200, producer_epoch=0, base_sequence=0)
    gap = record_batch([b"z"], producer_id=7200, producer_epoch=0, base_sequence=5)

    _, e0, _ = send_produce(conn, topic, ok, 3001)
    assert e0 == 0
    _, e1, _ = send_produce(conn, topic, gap, 3002)
    assert e1 == OUT_OF_ORDER_SEQUENCE_NUMBER, f"expected 45, got {e1}"
    assert log_rows(topic) == 1, "an out-of-order batch must not be stored"

def test_the_retained_window_is_five_deep(topic, conn):
    """Upstream's NUM_BATCHES_TO_RETAIN. Five is not arbitrary — it is
    `max.in.flight.requests.per.connection`, which idempotence caps at 5, so it is
    exactly the number of batches that can be outstanding and therefore retried."""
    for seq in range(8):
        batch = record_batch([f"m{seq}".encode()], producer_id=7300,
                             producer_epoch=0, base_sequence=seq)
        _, err, _ = send_produce(conn, topic, batch, 4000 + seq)
        assert err == 0, f"seq {seq} failed with {err}"

    retained = int(sql(
        "SELECT count(*) FROM kafgres_producer_batches WHERE producer_id = 7300"
    ).strip())
    assert retained == 5, f"window should hold 5 batches, holds {retained}"

    lowest = int(sql(
        "SELECT min(first_seq) FROM kafgres_producer_batches WHERE producer_id = 7300"
    ).strip())
    assert lowest == 3, f"oldest retained sequence should be 3, got {lowest}"

@engine_a_storage
def test_a_retry_still_works_at_the_edge_of_the_window(topic, conn):
    """A retry of the oldest batch still in the window must be deduplicated. If the
    window were pruned too eagerly this would be re-appended instead."""
    offsets = []
    for seq in range(5):
        batch = record_batch([f"w{seq}".encode()], producer_id=7400,
                             producer_epoch=0, base_sequence=seq)
        _, err, off = send_produce(conn, topic, batch, 5000 + seq)
        assert err == 0
        offsets.append(off)

    oldest = record_batch([b"w0"], producer_id=7400, producer_epoch=0, base_sequence=0)
    _, err, off = send_produce(conn, topic, oldest, 5100)
    assert err == 0, f"a retry inside the window must succeed, got {err}"
    assert off == offsets[0], f"expected the original offset {offsets[0]}, got {off}"
    assert log_rows(topic) == 5, "a windowed retry was appended again"

@engine_a_storage
def test_a_producer_with_no_state_is_accepted_at_any_sequence(topic, conn):
    """Upstream stopped returning UNKNOWN_PRODUCER_ID here: "If there is no current
    producer epoch ... accept writes with any sequence number."

    Erroring instead makes the client reset producer state and re-send from its buffer,
    which for an idempotent producer reintroduces the duplicates the feature exists to
    prevent.
    """
    batch = record_batch([b"late"], producer_id=7500, producer_epoch=0, base_sequence=999)
    _, err, _ = send_produce(conn, topic, batch, 6001)
    assert err == 0, f"a producer we have no state for must be accepted, got {err}"
    assert log_rows(topic) == 1

@engine_a_storage
def test_dedup_survives_an_epoch_bump(topic, conn):
    """A client bumps its own epoch without asking the broker — `bumpIdempotentProducerEpoch`
    is client-side, triggered by any produce timeout — and resets its sequence to 0.

    The window is ordered by insertion, not by sequence, precisely so this works. Ordered
    by sequence, the new epoch's seq 0 sorts *below* the old epoch's retained 1..5, so the
    prune deletes the row it has just written and the batch's retry is appended a second
    time with no error anywhere.
    """
    pid = 7600
    for seq in range(6):                     # 6 > the 5-deep window, so it has pruned
        batch = record_batch([f"e0-{seq}".encode()], producer_id=pid,
                             producer_epoch=0, base_sequence=seq)
        _, err, _ = send_produce(conn, topic, batch, 8000 + seq)
        assert err == 0, f"epoch 0 seq {seq} failed with {err}"

    bumped = record_batch([b"e1-0"], producer_id=pid, producer_epoch=1, base_sequence=0)
    _, err, first = send_produce(conn, topic, bumped, 8100)
    assert err == 0, f"a new epoch starting at sequence 0 must be accepted, got {err}"
    assert log_rows(topic) == 7

    _, err, again = send_produce(conn, topic, bumped, 8101)
    assert err == 0, f"the retry must be deduplicated, not errored: {err}"
    assert again == first, f"expected the original offset {first}, got {again}"
    assert log_rows(topic) == 7, "the bumped-epoch batch was appended twice"

    nxt = record_batch([b"e1-1"], producer_id=pid, producer_epoch=1, base_sequence=1)
    _, err, _ = send_produce(conn, topic, nxt, 8102)
    assert err == 0, f"sequence 1 of the new epoch failed with {err}"
    assert log_rows(topic) == 8

@engine_a_storage
def test_dedup_survives_an_epoch_bump_that_collides_with_a_retained_sequence(topic, conn):
    """The same bump, but with the window not yet full, so the new epoch's sequence 0
    lands on the primary key of a retained row and takes the upsert path instead.

    The upsert has to make that row the *newest*, which means reassigning its insertion
    position and not just its epoch — otherwise the prune reads it as the stalest row in
    the window and the retry below is appended twice.
    """
    pid = 7950
    for seq in range(2):                     # 2 < the window depth, so nothing is pruned
        batch = record_batch([f"c{seq}".encode()], producer_id=pid,
                             producer_epoch=0, base_sequence=seq)
        _, err, _ = send_produce(conn, topic, batch, 8500 + seq)
        assert err == 0, f"epoch 0 seq {seq} failed with {err}"

    bumped = record_batch([b"c1-0"], producer_id=pid, producer_epoch=1, base_sequence=0)
    _, err, first = send_produce(conn, topic, bumped, 8510)
    assert err == 0, f"the bumped batch failed with {err}"

    _, err, again = send_produce(conn, topic, bumped, 8511)
    assert err == 0, f"the retry must be deduplicated, got {err}"
    assert again == first, f"expected the original offset {first}, got {again}"
    assert log_rows(topic) == 3, "the bumped-epoch batch was appended twice"

    rows = int(sql(
        f"SELECT count(*) FROM kafgres_producer_batches WHERE producer_id = {pid}"
    ).strip())
    assert rows == 2, f"expected 2 window rows after the collision, got {rows}"

@engine_a_storage
def test_an_older_epoch_is_fenced(topic, conn):
    """INVALID_PRODUCER_EPOCH, not OUT_OF_ORDER_SEQUENCE_NUMBER. The two ask the client
    for different things: 45 says "reset your sequence and resend", 47 says "you are
    fenced, stop". Telling a fenced writer to resend puts its records in the log."""
    pid = 7700
    for epoch in (0, 1):
        batch = record_batch([f"ep{epoch}".encode()], producer_id=pid,
                             producer_epoch=epoch, base_sequence=0)
        _, err, _ = send_produce(conn, topic, batch, 8200 + epoch)
        assert err == 0, f"epoch {epoch} failed with {err}"
    assert log_rows(topic) == 2

    stale = record_batch([b"zombie"], producer_id=pid, producer_epoch=0, base_sequence=1)
    _, err, _ = send_produce(conn, topic, stale, 8210)
    assert err == INVALID_PRODUCER_EPOCH, f"expected 47, got {err}"
    assert log_rows(topic) == 2, "a fenced producer's batch was appended"

    stale0 = record_batch([b"zombie0"], producer_id=pid, producer_epoch=0, base_sequence=0)
    _, err, _ = send_produce(conn, topic, stale0, 8211)
    assert err == INVALID_PRODUCER_EPOCH, f"expected 47 at sequence 0 too, got {err}"
    assert log_rows(topic) == 2, "a fenced producer's batch was appended at sequence 0"

@engine_a_storage
def test_a_partition_append_is_all_or_nothing(topic, conn):
    """One produce, two batches, the second out of order.

    The partition is reported as failed, so nothing it wrote may survive. Committing the
    first batch anyway is worse than either outcome: the client is told the partition
    failed and resends both, and the first one lands twice — under a retriable code, with
    no window entry to recognise it, so nothing downstream notices.
    """
    pid = 7800
    good = record_batch([b"first"], producer_id=pid, producer_epoch=0, base_sequence=0)
    gap = record_batch([b"second"], producer_id=pid, producer_epoch=0, base_sequence=9)

    _, err, _ = send_produce(conn, topic, good + gap, 8300)
    assert err == OUT_OF_ORDER_SEQUENCE_NUMBER, f"expected 45, got {err}"
    assert log_rows(topic) == 0, "the first batch of a failed partition append was kept"

    retained = int(sql(
        f"SELECT count(*) FROM kafgres_producer_batches WHERE producer_id = {pid}"
    ).strip())
    assert retained == 0, "the rolled-back append left a window entry behind"

    ok = record_batch([b"second"], producer_id=pid, producer_epoch=0, base_sequence=1)
    _, err, _ = send_produce(conn, topic, good + ok, 8301)
    assert err == 0, f"the corrected resend failed with {err}"
    assert log_rows(topic) == 2

def test_a_wide_produce_costs_one_subtransaction(conn):
    """The savepoint that makes a partition atomic must not be paid per partition.

    Postgres caches 64 subtransaction ids per backend in `PGPROC`. Past that the cache
    overflows and *every other backend's* snapshot has to consult the `pg_subtrans` SLRU
    on visibility checks — so a wide produce would tax the co-resident OLTP workload this
    broker exists to sit beside. A 64-partition topic is ordinary, and the Java producer
    packs every ready partition into one request, so a savepoint each reaches the cliff
    on a single normal append.

    Each writing subtransaction burns an xid, so xid consumption is the measurement.
    """
    name = "p4-wide"
    parts = 40
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', {parts})")
    try:
        def next_xid():
            return int(sql("SELECT pg_snapshot_xmax(pg_current_snapshot())").strip())

        before = next_xid()
        batches = [
            (p, record_batch([f"w{p}".encode()], producer_id=7900 + p,
                             producer_epoch=0, base_sequence=0))
            for p in range(parts)
        ]
        header = struct.pack(">hhi", PRODUCE, 3, 8400)
        header += struct.pack(">h", 6) + b"pytest"
        frame = header + produce_v3_many(name, batches)
        conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
        _, results = parse_produce_v3_many(conn.recv())

        assert len(results) == parts, f"expected {parts} partition responses"
        assert all(err == 0 for _, err, _ in results), results
        after = next_xid()

        assert after - before < 20, (
            f"a {parts}-partition produce consumed {after - before} xids; "
            "the per-partition savepoint is back"
        )
    finally:
        sql(f"SELECT kafgres_drop_topic('{name}')")

@engine_a_storage
def test_a_negative_last_offset_delta_cannot_rewind_the_partition(topic, conn):
    """Reachable from the wire by a hostile client.

    `lastOffsetDelta` is inside CRC coverage, so a batch declaring a negative one is
    well-formed: the checksum passes and the batch is exactly what was sent. The log's
    `next_offset` advances by `base + delta + 1`, so a negative delta either moves it
    *backwards* — the following append reuses offsets consumers have already read at —
    or drives `last_offset` outside the segment's range bound.

    Measured without the guard, this batch takes the second path and the range violation
    surfaces as `REQUEST_TIMED_OUT`: retriable, so the client resends a batch that can
    never succeed, forever, while the log looks healthy.
    """
    good = record_batch([b"a"], producer_id=-1, producer_epoch=-1, base_sequence=-1)
    _, err, first = send_produce(conn, topic, good, 9400)
    assert err == 0 and first == 0

    hostile = record_batch([b"b"], producer_id=-1, producer_epoch=-1, base_sequence=-1,
                           last_offset_delta=-5)
    _, err, _ = send_produce(conn, topic, hostile, 9401)
    assert err == INVALID_RECORD, f"expected 87, got {err}"
    assert log_rows(topic) == 1, "a batch with a negative extent was stored"

    _, err, second = send_produce(conn, topic, good, 9402)
    assert err == 0
    assert second == 1, f"next offset was rewound: expected 1, got {second}"

@pytest.fixture
def retention():
    """Drive `kafgres.producer_id_expiration_ms` / `max_producer_ids` from a test.

    Both defaults are deliberately untestable in a test suite — 24h and 10000 — so the
    only way to exercise the sweep is to move them.
    """
    def apply(expiration_ms=None, max_ids=None):
        if expiration_ms is not None:
            sql(f"ALTER SYSTEM SET kafgres.producer_id_expiration_ms = {expiration_ms}")
        if max_ids is not None:
            sql(f"ALTER SYSTEM SET kafgres.max_producer_ids = {max_ids}")
        sql("SELECT pg_reload_conf()")

    yield apply
    sql("ALTER SYSTEM RESET kafgres.producer_id_expiration_ms")
    sql("ALTER SYSTEM RESET kafgres.max_producer_ids")
    sql("SELECT pg_reload_conf()")

def init_producer_id(conn, correlation):
    header = struct.pack(">hhi", INIT_PRODUCER_ID, 1, correlation)
    header += struct.pack(">h", 6) + b"pytest"
    frame = header + struct.pack(">h", -1) + struct.pack(">i", 60000)
    conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
    resp = conn.recv()
    (error,) = struct.unpack_from(">h", resp, 8)
    assert error == 0, f"InitProducerId failed with {error}"
    (producer_id,) = struct.unpack_from(">q", resp, 10)
    return producer_id

def producer_rows():
    return int(sql("SELECT count(*) FROM kafgres_producers").strip())

def test_the_producer_id_ceiling_drops_the_least_recently_used(conn, retention):
    """Expiry alone bounds nothing.

    A client that allocates ids faster than the expiry window retires them grows the
    table without limit, and 24h is a long time at one id per request — which is exactly
    what a `KafkaProducer` per serverless invocation does. Upstream needed KIP-936's
    quota on top of `producer.id.expiration.ms` for the same reason.
    """
    sql("DELETE FROM kafgres_producers")
    ids = [init_producer_id(conn, 9000 + i) for i in range(5)]
    assert producer_rows() == 5

    for i, pid in enumerate(ids):
        sql(f"UPDATE kafgres_producers SET last_ts = now() - interval '600 seconds' "
            f"+ make_interval(secs => {i}) WHERE producer_id = {pid}")

    retention(max_ids=3)
    sql("SELECT kafgres_expire_producers()")
    assert producer_rows() == 3

    kept = {int(r) for r in sql(
        "SELECT producer_id FROM kafgres_producers"
    ).split()}
    assert kept == set(ids[2:]), f"expected {ids[2:]} kept, got {sorted(kept)}"

def test_the_ceiling_will_not_evict_a_recently_active_producer(conn, retention):
    """The ceiling has no timer of its own, so it needs a floor.

    Without one it evicts by rank alone, and under the churn that makes it fire at all
    the victim can be a producer that appended seconds ago. Its in-flight retry then
    finds an empty window, is accepted at any sequence, and the records land twice with
    error_code 0 — the exact duplication this phase exists to prevent. Upstream's KIP-936
    does not have this exposure because it throttles *allocation* rather than evicting
    live state; refusing a new id cannot duplicate anything.

    The trade is that the ceiling is soft: under heavy churn the table settles at
    `churn_rate x floor` rather than at `max_ids`. Bounded and small, against silent
    duplicates.
    """
    sql("DELETE FROM kafgres_producers")
    for i in range(5):
        init_producer_id(conn, 9050 + i)

    retention(max_ids=1)
    dropped = int(sql("SELECT kafgres_expire_producers()").strip())
    assert dropped == 0, f"the ceiling evicted {dropped} producer(s) still inside the floor"
    assert producer_rows() == 5, "a producer active seconds ago was evicted"

def test_idle_producer_state_expires(conn, topic, retention):
    """`producer.id.expiration.ms`. Nothing else ever removes a producer that stopped."""
    sql("DELETE FROM kafgres_producers")
    pid = init_producer_id(conn, 9100)
    batch = record_batch([b"before"], producer_id=pid, producer_epoch=0, base_sequence=0)
    _, err, _ = send_produce(conn, topic, batch, 9101)
    assert err == 0

    windowed = int(sql(
        f"SELECT count(*) FROM kafgres_producer_batches WHERE producer_id = {pid}"
    ).strip())
    assert windowed == 1, "the batch should be in the window before expiry"

    retention(expiration_ms=3_600_000)
    assert int(sql("SELECT kafgres_expire_producers()").strip()) == 0
    assert producer_rows() == 1, "a producer inside its expiry window was dropped"

    retention(expiration_ms=1)
    sql("SELECT kafgres_expire_producers()")
    assert producer_rows() == 0, "the idle producer was not expired"

    windowed = int(sql(
        f"SELECT count(*) FROM kafgres_producer_batches WHERE producer_id = {pid}"
    ).strip())
    assert windowed == 0, "the expired producer's window was left behind"

@engine_a_storage
def test_expiring_a_producer_does_not_break_it(conn, topic, retention):
    """Why dropping state is safe, and the property the whole policy rests on.

    A producer we hold no state for is accepted at any sequence — upstream's own
    post-retention behaviour. So the worst an eviction can do is fail to deduplicate a
    retry that has not been sent yet. If this ever became an error instead, the sweep
    would turn into a scheduled outage for every long-lived producer.
    """
    sql("DELETE FROM kafgres_producers")
    pid = init_producer_id(conn, 9200)
    for seq in range(2):
        batch = record_batch([f"s{seq}".encode()], producer_id=pid,
                             producer_epoch=0, base_sequence=seq)
        _, err, _ = send_produce(conn, topic, batch, 9201 + seq)
        assert err == 0

    retention(expiration_ms=1)
    sql("SELECT kafgres_expire_producers()")

    resumed = record_batch([b"s2"], producer_id=pid, producer_epoch=0, base_sequence=2)
    _, err, _ = send_produce(conn, topic, resumed, 9210)
    assert err == 0, f"a producer whose state was swept must still be accepted, got {err}"
    assert log_rows(topic) == 3

@engine_a_storage
def test_a_swept_producer_keeps_a_window_it_never_registered(conn, topic, retention):
    """A batch may carry a producer id this broker never issued — nothing in the
    protocol makes InitProducerId a precondition for setting the field. Collecting
    "window rows with no producer row" as orphans would delete such a producer's
    deduplication state, so the sweep only removes windows of producers it dropped.
    """
    sql("DELETE FROM kafgres_producers")
    unregistered = 9500
    batch = record_batch([b"u"], producer_id=unregistered, producer_epoch=0, base_sequence=0)
    _, err, first = send_produce(conn, topic, batch, 9300)
    assert err == 0

    victim = init_producer_id(conn, 9301)
    retention(expiration_ms=1)
    sql("SELECT kafgres_expire_producers()")

    assert int(sql(
        f"SELECT count(*) FROM kafgres_producers WHERE producer_id = {victim}"
    ).strip()) == 0, "the registered producer should have been swept"

    kept = int(sql(
        f"SELECT count(*) FROM kafgres_producer_batches WHERE producer_id = {unregistered}"
    ).strip())
    assert kept == 1, "the unregistered producer's window was swept as an orphan"

    _, err, again = send_produce(conn, topic, batch, 9302)
    assert err == 0 and again == first, f"dedup broken after sweep: {err}, {again} vs {first}"
    assert log_rows(topic) == 1

@engine_a_storage
def test_non_idempotent_producers_are_untouched(topic, conn):
    """A batch with no producer id has nothing to deduplicate against, and two identical
    ones are two distinct records — not a retry."""
    batch = record_batch([b"plain"], producer_id=-1, producer_epoch=-1, base_sequence=-1)
    _, e0, o0 = send_produce(conn, topic, batch, 7001)
    _, e1, o1 = send_produce(conn, topic, batch, 7002)
    assert (e0, e1) == (0, 0)
    assert o1 != o0, "identical non-idempotent batches are separate records"
    assert log_rows(topic) == 2

def test_an_oversized_batch_does_not_duplicate_its_neighbours(conn, topic):
    """An oversized batch must not duplicate its neighbours.

    The Java producer packs every ready partition for a broker into one request, so a
    single oversized batch decides what happens to the *others* in that request. Refusing
    it mid-append would abandon the shared pass and replay per partition — and on the
    segment engine an append is outside transaction control, so the savepoint rolls back
    the dedup window row while the records stay on disk and the replay writes them again.
    A consumer then reads its neighbour's records twice, with nothing reporting an error.

    Checking the size before the first append means no pass is abandoned for something
    knowable from the request alone. Counted through `kafgres_partition_offsets` rather
    than `kafgres_log`, so it runs on the engine where the bug lives.
    """
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 2)")
    small = record_batch([b"a", b"b", b"c"])
    huge = record_batch([b"x" * 3_000_000])

    header = struct.pack(">hhi", PRODUCE, 3, 8500)
    header += struct.pack(">h", 6) + b"pytest"
    frame = header + produce_v3_many(topic, [(0, small), (1, huge)])
    conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
    _, results = parse_produce_v3_many(conn.recv())
    by_partition = {index: (error, base) for index, error, base in results}

    assert by_partition[1][0] == 10, by_partition  # MESSAGE_TOO_LARGE
    assert by_partition[0][0] == 0, f"a healthy partition failed with its neighbour: {by_partition}"

    hw = int(sql(f"""SELECT high_watermark FROM kafgres_partition_offsets('{topic}')
                      WHERE partition = 0"""))
    assert hw == 3, f"the healthy partition's records landed twice: high watermark {hw}"
    assert int(sql(f"""SELECT coalesce(high_watermark, 0)
                         FROM kafgres_partition_offsets('{topic}')
                        WHERE partition = 1""")) == 0, "the oversized batch was stored"
