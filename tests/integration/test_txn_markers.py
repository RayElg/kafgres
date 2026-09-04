"""`27 WriteTxnMarkers` — the escape hatch for a hanging transaction.

A producer that dies between its first write and its `EndTxn` leaves the partition's last
stable offset below its records: every `read_committed` consumer stops advancing and
nothing reports it — the partition simply goes quiet. The expiry sweep reaches the same
state on its own, but only after the *producer's* configured transaction timeout, which
an operator watching a stalled consumer group cannot wait out.
"""

import os
import socket
import struct
import subprocess

import pytest

from conftest import (ADD_PARTITIONS_TO_TXN, BROKER_HOST, BROKER_PORT, END_TXN,
                      WRITE_TXN_MARKERS, Connection, read_compact_string, sql)

CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

@pytest.fixture
def topic(request):
    name = f"wtm-{request.node.name.replace('_', '-')[:34]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def run_txn_bg(topic, outcome="open"):
    p = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "sarama-conformance", BROKER, f"txn-{outcome}", topic],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    for _ in range(60):
        line = p.stdout.readline()
        if not line:
            break
        if line.startswith("OK "):
            return p
        if line.startswith("ERROR"):
            p.kill()
            raise AssertionError(line)
    p.kill()
    raise AssertionError("the scenario never opened its transaction")

def consume(topic, isolation="read_committed", timeout=60):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-C", "-e", "-q", "-o", "beginning",
         "-X", f"isolation.level={isolation}", "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=timeout,
    )
    return {int(l.split("\t", 1)[0]): l.split("\t", 1)[1]
            for l in out.stdout.splitlines() if "\t" in l}

def abort_transaction(topic, partition, producer_id, epoch, timeout=120):
    """`AdminClient.abortTransaction`, which is what sends WriteTxnMarkers.

    Through the real Java tool rather than a hand-built frame: the claim is that an
    unmodified toolchain can clear a hang.
    """
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "apache/kafka:4.1.0",
         "/opt/kafka/bin/kafka-transactions.sh", "--bootstrap-server", BROKER,
         "abort", "--topic", topic, "--partition", str(partition),
         "--producer-id", str(producer_id), "--producer-epoch", str(epoch),
         "--coordinator-epoch", "0"],
        capture_output=True, text=True, timeout=timeout,
    )

def hanging_producer():
    row = sql("SELECT producer_id || ' ' || producer_epoch FROM kafgres_producers "
              "WHERE transactional_id = 'kafgres-eos-open'")
    pid, epoch = row.split()
    return int(pid), int(epoch)

def test_aborting_a_hanging_transaction_releases_the_stalled_consumer(topic):
    """Asserted on what a consumer sees, not on the broker's own tables: the tables
    agreeing while the LSO stays put is exactly the failure mode this rules out."""
    p = run_txn_bg(topic)
    try:
        assert consume(topic) == {}, "the transaction was not actually holding the LSO"
        assert consume(topic, isolation="read_uncommitted") != {}, (
            "nothing was produced, so this test proves nothing about the LSO"
        )

        pid, epoch = hanging_producer()
        out = abort_transaction(topic, 0, pid, epoch)
        assert out.returncode == 0, out.stdout + out.stderr

        assert consume(topic) == {}
        assert sql("SELECT state FROM kafgres_txns WHERE producer_id = "
                   f"{pid}") == "aborted"
    finally:
        p.kill()
        p.wait(timeout=30)

def test_the_marker_is_visible_to_a_consumer_as_a_control_record(topic):
    """A marker is an offset, not a record: written as an ordinary record,
    `read_uncommitted` would show it as a message with an empty value, while the abort
    index and the LSO still look right."""
    p = run_txn_bg(topic)
    try:
        before = len(consume(topic, isolation="read_uncommitted"))
        pid, epoch = hanging_producer()
        assert abort_transaction(topic, 0, pid, epoch).returncode == 0
        after = consume(topic, isolation="read_uncommitted")
        assert len(after) == before, (
            f"the marker surfaced as a record: {before} -> {len(after)}"
        )
    finally:
        p.kill()
        p.wait(timeout=30)

def test_a_partition_not_in_the_transaction_is_refused(topic):
    """`INVALID_TXN_STATE`. Writing a marker there is not a harmless no-op: it appends
    a control batch that consumes an offset and tells consumers a transaction ended
    that never began on that partition.

    Aimed at a *different topic* while the transaction is ongoing — a repeat abort of
    the same partition is refused for the epoch bump, not the state.
    """
    other = f"{topic}-elsewhere"
    sql(f"SELECT kafgres_drop_topic('{other}')")
    sql(f"SELECT kafgres_create_topic('{other}', 1)")
    p = run_txn_bg(topic)
    try:
        pid, epoch = hanging_producer()
        out = abort_transaction(other, 0, pid, epoch)
        combined = out.stdout + out.stderr
        assert out.returncode != 0, combined
        assert "INVALID_TXN_STATE" in combined or "InvalidTxnState" in combined, combined
        assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}") == "ongoing"
    finally:
        p.kill()
        p.wait(timeout=30)
        sql(f"SELECT kafgres_drop_topic('{other}')")

def test_a_stale_producer_epoch_is_refused(topic):
    """An epoch behind the producer's current one names a transaction already
    superseded: accepting it would write a marker deciding the fate of records
    belonging to a *later* transaction than the one the operator described."""
    p = run_txn_bg(topic)
    try:
        pid, epoch = hanging_producer()
        out = abort_transaction(topic, 0, pid, epoch - 1 if epoch > 0 else 0)
        if epoch == 0:
            pytest.skip("producer is at epoch 0; there is no staler epoch to send")
        combined = out.stdout + out.stderr
        assert out.returncode != 0, combined
        assert "invalid producer epoch" in combined.lower(), combined
    finally:
        p.kill()
        p.wait(timeout=30)

def test_errors_are_reported_per_partition_rather_than_raised(topic):
    """One bad partition must not cost the operator the ones that worked.

    A raw frame, not the Java tool: `AdminClient.abortTransaction` resolves the topic
    through Metadata first and retries forever when it does not exist, so the
    per-partition path is unreachable through it."""
    p = run_txn_bg(topic)
    try:
        pid, epoch = hanging_producer()
        c = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            conn = Connection(c)
            conn.send(WRITE_TXN_MARKERS, 1, 77, write_txn_markers_body(
                pid, epoch, False,
                [("wtm-no-such-topic-at-all", [0]), (topic, [0])],
            ))
            body = conn.recv()
        finally:
            c.close()

        results = parse_marker_response(body)
        assert results == {
            ("wtm-no-such-topic-at-all", 0): 3,   # UNKNOWN_TOPIC_OR_PARTITION
            (topic, 0): 0,                        # and the real one still landed
        }, results

        assert consume(topic) == {}
        assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}") == "aborted"
    finally:
        p.kill()
        p.wait(timeout=30)

def write_txn_markers_body(producer_id, epoch, committed, topics):
    """One marker, flexible (v1) encoding."""
    body = uvarint(2)                                   # 1 marker
    body += struct.pack(">qh?", producer_id, epoch, committed)
    body += uvarint(len(topics) + 1)
    for name, partitions in topics:
        raw = name.encode()
        body += uvarint(len(raw) + 1) + raw
        body += uvarint(len(partitions) + 1)
        for p in partitions:
            body += struct.pack(">i", p)
        body += b"\x00"                                 # topic tagged fields
    body += struct.pack(">i", 0)                        # coordinator epoch
    body += b"\x00"                                     # marker tagged fields
    body += b"\x00"                                     # body tagged fields
    return body

def parse_marker_response(body):
    """{(topic, partition): error_code} from a v1 response."""
    pos = 4                       # correlation id
    pos += 1                      # response header tagged fields (flexible)
    out = {}
    n_markers = body[pos] - 1
    pos += 1
    for _ in range(n_markers):
        pos += 8                  # producer id
        n_topics = body[pos] - 1
        pos += 1
        for _ in range(n_topics):
            name, pos = read_compact_string(body, pos)
            n_parts = body[pos] - 1
            pos += 1
            for _ in range(n_parts):
                (part, code) = struct.unpack_from(">ih", body, pos)
                pos += 6
                pos += 1          # partition tagged fields
                out[(name, part)] = code
            pos += 1              # topic tagged fields
        pos += 1                  # marker tagged fields
    return out

def uvarint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | 0x80]) if n else bytes([b])
        if not n:
            return out

def test_dropping_a_topic_does_not_wedge_a_producer_mid_transaction():
    """A `kafgres_txn_partitions` row must not outlive its topic.

    `finish_transaction` writes one marker per registered partition, so a row naming a
    dropped topic makes every subsequent `EndTxn` — and every expiry sweep — try to
    append to a topic that is gone; the transactional id is unusable until someone
    deletes the row by hand.
    """
    name = "wtm-dropped-under-a-transaction"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    p = run_txn_bg(name)
    try:
        pid, _ = hanging_producer()
        assert sql("SELECT count(*) FROM kafgres_txn_partitions "
                   f"WHERE producer_id = {pid}") != "0", "nothing was registered"
    finally:
        p.kill()
        p.wait(timeout=30)

    sql(f"SELECT kafgres_drop_topic('{name}')")
    assert sql(f"SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = {pid}") == "0", (
        "the transaction still names a partition whose topic is gone; every EndTxn and "
        "every expiry sweep for this producer will now fail on an append to nothing"
    )

    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    try:
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", CLIENTS,
             "sarama-conformance", BROKER, "txn-commit", name],
            capture_output=True, text=True, timeout=180,
        )
        assert "OK commit" in out.stdout, out.stdout + out.stderr[-600:]
    finally:
        sql(f"SELECT kafgres_drop_topic('{name}')")

def test_a_forced_abort_is_not_half_undone_by_the_producers_own_commit():
    """One transaction cannot end as ABORT on one partition and COMMIT on another.

    Marking is per partition — the protocol's granularity — so an operator can abort
    partition 0 while the producer is merely slow. If nothing records that the outcome
    was taken out of the producer's hands, its own `EndTxn(commit)` finds the untouched
    partitions still registered, writes COMMIT markers there, and returns success: no
    error anywhere.

    Driven through SQL rather than a client: the interleaving needs the producer alive
    and unfenced between the two steps, which no client API exposes.
    """
    name = "wtm-atomic-across-partitions"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 2)")
    p = run_txn_bg(name)
    try:
        pid, epoch = hanging_producer()

        held = int(sql("SELECT partition FROM kafgres_txn_partitions "
                       f"WHERE producer_id = {pid} ORDER BY partition LIMIT 1"))
        other = 1 - held

        sql("INSERT INTO kafgres_txn_partitions (producer_id, topic_id, partition, first_offset) "
            f"SELECT {pid}, topic_id, {other}, -1 FROM kafgres_topics WHERE name = '{name}' "
            "ON CONFLICT DO NOTHING")

        assert abort_transaction(name, held, pid, epoch).returncode == 0
        assert sql(f"SELECT forced_result FROM kafgres_txns WHERE producer_id = {pid}") == "f"

        c = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            conn = Connection(c)
            conn.send(END_TXN, 2, 88, end_txn_body("kafgres-eos-open", pid, epoch, True))
            resp = conn.recv()
        finally:
            c.close()
        (code,) = struct.unpack_from(">h", resp, 8)   # correlation, throttle, error_code
        assert code == 47, f"EndTxn(commit) was not refused: error_code={code}"

        state = sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}")
        assert state == "aborted", (
            f"the producer's commit was honoured after an operator aborted the "
            f"transaction: state={state}"
        )
        assert sql(f"SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = {pid}") == "0"
    finally:
        p.kill()
        p.wait(timeout=30)
        sql(f"SELECT kafgres_drop_topic('{name}')")

def test_an_out_of_range_partition_is_not_registered_in_a_transaction():
    """`AddPartitionsToTxn` naming a partition the topic does not have.

    Registering it would leave a row no cleanup path can see, and `finish_transaction`'s
    marker append would fail on it, aborting the *entire* `expire_stale_transactions`
    sweep — one malformed request stops every stale transaction in the cluster from
    being cleaned up.

    Sent as a raw frame: no client library will construct an out-of-range partition.
    """
    name = "wtm-out-of-range-partition"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 2)")
    p = run_txn_bg(name)
    try:
        pid, _ = hanging_producer()
        before = int(sql(f"SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = {pid}"))
        c = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
        try:
            conn = Connection(c)
            conn.send(ADD_PARTITIONS_TO_TXN, 2, 91,
                      add_partitions_body("kafgres-eos-open", pid, 0, name, [0, 1, 99]))
            conn.recv()
        finally:
            c.close()
        after = int(sql(f"SELECT count(*) FROM kafgres_txn_partitions WHERE producer_id = {pid}"))
        assert after <= before + 2, (
            f"partition 99 of a 2-partition topic was registered: {before} -> {after}"
        )
        assert sql("SELECT count(*) FROM kafgres_txn_partitions "
                   f"WHERE producer_id = {pid} AND partition = 99") == "0"
    finally:
        p.kill()
        p.wait(timeout=30)
        sql(f"SELECT kafgres_drop_topic('{name}')")

def add_partitions_body(txn_id, producer_id, epoch, topic, partitions):
    """AddPartitionsToTxn v2 — plain encoding.

    Not v3: the schema enables flexible versions *at* v3, which the `V3AndBelowTopics`
    field names invite you to get wrong; a compact frame read as plain is garbage and
    the broker closes the connection.
    """
    def s(v):
        raw = v.encode()
        return struct.pack(">h", len(raw)) + raw
    body = s(txn_id) + struct.pack(">qh", producer_id, epoch)
    body += struct.pack(">i", 1) + s(topic)
    body += struct.pack(">i", len(partitions))
    for p in partitions:
        body += struct.pack(">i", p)
    return body

def end_txn_body(txn_id, producer_id, epoch, committed):
    """EndTxn v2 — plain encoding.

    Every transaction API in this family goes flexible at v3; the `V3AndBelow*` field
    names describe the request *shape*, not the encoding, and a plain-encoded v3 frame
    is read as garbage.
    """
    raw = txn_id.encode()
    return (struct.pack(">h", len(raw)) + raw
            + struct.pack(">qh?", producer_id, epoch, committed))
