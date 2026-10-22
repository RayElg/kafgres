"""Record fields as Kafka's `DefaultRecord.readFrom` reads them, on produce to a compacted
topic and in compaction. Raw frames only: no client writes these bytes.
"""

import struct
import time

import pytest

from conftest import sql
from support.recordbatch import _crc32c, _varint, parse_produce_v3, produce_v3

PRODUCE, FETCH = 0, 1
INVALID_RECORD, CORRUPT_MESSAGE = 87, 2
PAD = b"p" * 1500


def record(*fields):
    """One record: attributes, timestamp delta and offset delta 0, then `fields` verbatim."""
    body = b"\x00\x00\x00" + b"".join(fields)
    return _varint(len(body)) + body


def keyed(key: bytes, value: bytes):
    return record(_varint(len(key)), key, _varint(len(value)), value, _varint(0))


def batch(records: bytes, count=1, crc_ok=True):
    ts = 1_700_000_000_000
    covered = struct.pack(">hiqqqhii", 0, count - 1, ts, ts, -1, -1, -1, count) + records
    crc = _crc32c(covered) ^ (0 if crc_ok else 1)
    body = struct.pack(">ibI", -1, 2, crc) + covered
    return struct.pack(">qi", 0, len(body)) + body


def produce(conn, topic, wire, correlation=1):
    header = struct.pack(">hhi", PRODUCE, 3, correlation) + struct.pack(">h", 6) + b"pytest"
    frame = header + produce_v3(topic, 0, wire)
    conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
    _, err, base = parse_produce_v3(conn.recv())
    return err, base


def fetch_batches(conn, topic):
    """Fetch v4 from offset 0: every stored batch's bytes, keyed by base offset."""
    t = topic.encode()
    body = struct.pack(">iiiib", -1, 100, 1, 8 << 20, 0)
    body += struct.pack(">i", 1) + struct.pack(">h", len(t)) + t
    body += struct.pack(">i", 1) + struct.pack(">iqi", 0, 0, 8 << 20)
    conn.send(FETCH, 4, 77, body)
    resp = conn.recv()
    pos = 4 + 4                                           # correlation, throttle
    assert struct.unpack_from(">i", resp, pos)[0] == 1
    pos += 4
    (n,) = struct.unpack_from(">h", resp, pos)
    pos += 2 + n
    assert struct.unpack_from(">i", resp, pos)[0] == 1
    pos += 4
    _p, err = struct.unpack_from(">ih", resp, pos)
    assert err == 0, f"fetch error {err}"
    pos += 6 + 16
    (aborted,) = struct.unpack_from(">i", resp, pos)
    pos += 4 + max(aborted, 0) * 16
    (size,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    blob, out = resp[pos:pos + max(size, 0)], {}
    while len(blob) >= 12:
        base, length = struct.unpack_from(">qi", blob, 0)
        if len(blob) < 12 + length:
            break
        out[base] = blob[:12 + length]
        blob = blob[12 + length:]
    return out


def set_policy(topic, policy):
    sql(f"""UPDATE kafgres_topics SET config = COALESCE(config, '{{}}'::jsonb) || '{{"cleanup.policy": "{policy}"}}'
             WHERE name = '{topic}'""")


@pytest.fixture
def topic(request):
    name = f"rv-{request.node.name.replace('_', '-')[:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


ALIAS_MINUS_ONE = bytes([0x81, 0x80, 0x80, 0x80, 0x10])
ALIAS_ONE = bytes([0x82, 0x80, 0x80, 0x80, 0x10])
SIX_BYTES = bytes([0x81, 0x80, 0x80, 0x80, 0x80, 0x00])

REFUSED_ON_COMPACTED = {
    # Kafka reads both as a null key, which a compacted topic refuses.
    "five-byte alias of -1 key": record(ALIAS_MINUS_ONE, _varint(1), b"v", _varint(0)),
    "key length -2": record(_varint(-2), _varint(1), b"v", _varint(0)),
    # Structurally invalid: InvalidRecordException in DefaultRecord.readFrom.
    "six-byte key length": record(SIX_BYTES, _varint(1), b"v", _varint(0)),
    "trailing byte in the record": record(_varint(1), b"k", _varint(1), b"v", _varint(0), b"\xaa"),
    "negative header key length": record(_varint(1), b"k", _varint(1), b"v", _varint(1),
                                         _varint(-1), _varint(-1)),
}


@pytest.mark.parametrize("policy", ["compact", "compact,delete"])
@pytest.mark.parametrize("case", sorted(REFUSED_ON_COMPACTED))
def test_a_compacted_topic_refuses_what_kafka_refuses(topic, conn, policy, case):
    set_policy(topic, policy)
    err, _ = produce(conn, topic, batch(REFUSED_ON_COMPACTED[case]))
    assert err == INVALID_RECORD, f"{case} on cleanup.policy={policy}: error {err}"


def test_a_five_byte_alias_of_a_real_length_is_accepted(topic, conn):
    set_policy(topic, "compact")
    wire = batch(record(ALIAS_ONE, b"k", _varint(1), b"v", _varint(0)))
    assert produce(conn, topic, wire) == (0, 0)


def test_a_bad_crc_is_still_corrupt_message_on_a_compacted_topic(topic, conn):
    """Kafka checks the CRC before it reads a record."""
    set_policy(topic, "compact")
    wire = batch(REFUSED_ON_COMPACTED["trailing byte in the record"], crc_ok=False)
    assert produce(conn, topic, wire)[0] == CORRUPT_MESSAGE


@pytest.fixture
def small_segments():
    sql("ALTER SYSTEM SET kafgres.segment_offsets = 2")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 4096")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    yield
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")


# small_segments comes first: the table engine sizes a partition's segments when it is created.
def test_a_stored_batch_that_no_longer_decodes_does_not_wedge_compaction(
        small_segments, topic, conn):
    """A plain topic stores the batch without reading its records. Once the topic is
    compacted, the cleaner must still supersede around it and keep it verbatim."""
    bad = batch(record(_varint(1), b"b", _varint(len(PAD)), PAD, _varint(0), b"\xaa"))
    plan = [keyed(b"k", b"1" + PAD), None, keyed(b"k", b"2" + PAD), keyed(b"k", b"3" + PAD),
            keyed(b"z", b"1" + PAD), keyed(b"z", b"2" + PAD), keyed(b"y", b"1" + PAD)]
    for i, rec in enumerate(plan):
        assert produce(conn, topic, bad if rec is None else batch(rec), 100 + i) == (0, i)
        time.sleep(0.25)
    set_policy(topic, "compact")

    sql("SELECT kafgres_enforce_retention()")
    time.sleep(0.5)

    stored = fetch_batches(conn, topic)
    assert 0 not in stored, f"compaction stayed stuck behind the bad batch: {sorted(stored)}"
    assert 1 in stored, f"the undecodable batch was dropped: {sorted(stored)}"
    # Base offset and partitionLeaderEpoch are the broker's to stamp; the rest is verbatim.
    assert stored[1][16:] == bad[16:], "the undecodable batch was rewritten"
    assert 6 in stored, sorted(stored)
