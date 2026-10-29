"""Record fields as Kafka's `DefaultRecord.readFrom` reads them, on produce to a compacted
topic and in compaction. Raw frames only: no client writes these bytes.
"""

import struct
import subprocess
import time
import zlib

import pytest

from conftest import sql
from support.docker import REPO, compose
from support.recordbatch import _crc32c, _varint, parse_produce_v3, produce_v3

PRODUCE, FETCH = 0, 1
INVALID_RECORD, CORRUPT_MESSAGE = 87, 2
PAD = b"p" * 1500
GZIP = 1
MIB = 1 << 20


def record(*fields, delta=0):
    """One record: attributes and timestamp delta 0, offset `delta`, then `fields` verbatim."""
    body = b"\x00\x00" + _varint(delta) + b"".join(fields)
    return _varint(len(body)) + body


def keyed(key: bytes, value: bytes, delta=0):
    return record(_varint(len(key)), key, _varint(len(value)), value, _varint(0), delta=delta)


def batch(records: bytes, count=1, crc_ok=True, attributes=0):
    ts = 1_700_000_000_000
    covered = struct.pack(">hiqqqhii", attributes, count - 1, ts, ts, -1, -1, -1, count) + records
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


def gzipped(records):
    """`records` ([(key, value)]) as one gzip batch whose plain bytes pass 64 MiB."""
    plain = b"".join(
        record(_varint(-1), _varint(len(v)), v, _varint(0), delta=i) if k is None
        else keyed(k, v, delta=i)
        for i, (k, v) in enumerate(records))
    gz = zlib.compressobj(1, zlib.DEFLATED, 31)
    payload = gz.compress(plain) + gz.flush()
    assert len(plain) > 64 * MIB and len(payload) < MIB // 2
    return batch(payload, count=len(records), attributes=GZIP)


def big():
    """66 records of 1 MiB under distinct keys."""
    zeros = bytes(MIB)
    return [(f"v{i}".encode(), zeros) for i in range(66)]


@pytest.fixture
def no_sweeps():
    """The broker's own sweep would compact between a test's steps."""
    sql("ALTER SYSTEM SET kafgres.retention_check_interval_ms = 3600000")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    yield
    sql("ALTER SYSTEM RESET kafgres.retention_check_interval_ms")
    sql("SELECT pg_reload_conf()")


def set_raw_policy(topic, json_text):
    """`cleanup.policy` as a JSON string literal, escapes and all."""
    sql(f"""UPDATE kafgres_topics
               SET config = COALESCE(config, '{{}}'::jsonb) || '{{"cleanup.policy": {json_text}}}'
             WHERE name = '{topic}'""")


def server_log():
    return subprocess.run(["docker", "compose", "logs", "postgres"], capture_output=True,
                          text=True, timeout=120, cwd=REPO).stdout


@pytest.mark.parametrize("policy", [r'"compact\n"', r'"\tcompact"', r'"compact,delete\r\n"'])
def test_a_policy_kafka_trims_to_compact_refuses_null_keys(topic, conn, policy):
    """Kafka trims a list config with `String.trim()`. Produce must read the policy as the
    cleaner does, or null keys reach a log that is being compacted."""
    set_raw_policy(topic, policy)
    null_key = batch(record(_varint(-1), _varint(1), b"v", _varint(0)))
    assert produce(conn, topic, null_key)[0] == INVALID_RECORD


def test_a_null_key_with_a_bad_crc_is_corrupt_message(topic, conn):
    set_policy(topic, "compact")
    wire = batch(record(_varint(-1), _varint(1), b"v", _varint(0)), crc_ok=False)
    assert produce(conn, topic, wire)[0] == CORRUPT_MESSAGE


@pytest.mark.parametrize("policy", ["delete", "compact"])
def test_a_batch_past_64_mib_decompressed_is_accepted(topic, conn, policy):
    """Kafka validates a compressed batch's keys as it decompresses, with no cap."""
    set_policy(topic, policy)
    assert produce(conn, topic, gzipped(big())) == (0, 0)


def test_a_null_key_past_64_mib_decompressed_is_refused(topic, conn):
    set_policy(topic, "compact")
    assert produce(conn, topic, gzipped(big() + [(None, b"v")]))[0] == INVALID_RECORD


def test_a_batch_past_64_mib_decompressed_supersedes_older_values(
        small_segments, no_sweeps, topic, conn):
    set_policy(topic, "compact")
    large = gzipped([(b"k", b"new")] + big())
    plan = [batch(keyed(b"k", b"old" + PAD)), large] + [
        batch(keyed(k, b"1" + PAD)) for k in (b"z", b"y", b"x")]
    base = 0
    for i, wire in enumerate(plan):
        assert produce(conn, topic, wire, 200 + i) == (0, base)
        base += struct.unpack_from(">i", wire, 23)[0] + 1
        time.sleep(0.25)

    sql("SELECT kafgres_enforce_retention()")
    time.sleep(0.5)

    stored = fetch_batches(conn, topic)
    assert 0 not in stored, f"the large batch did not supersede k's older value: {sorted(stored)}"
    assert stored.get(1, b"")[16:] == large[16:], "the large batch was not kept verbatim"


def corrupt_one_byte(topic, conn, base, at_from_end, plan):
    """Flip a value byte of the stored batch at `base`, leaving it decodable. Returns what
    the cleaner should name: the segment file and position, or the offset."""
    if sql("SHOW kafgres.storage_engine") == "table":
        tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
        sql(f"""UPDATE kafgres_log SET batch = set_byte(batch, length(batch) - {at_from_end}, 113)
                 WHERE topic_id = {tid} AND partition = 0 AND base_offset = {base}""")
        return f"partition 0 failed: storage error: compaction: batch at offset {base}: CRC"
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
    folder = f"{sql('SHOW data_directory')}/kafgres/{tid}/0"
    out = compose("exec", "-T", "postgres", "ls", folder)
    segments = sorted(int(n[:-4]) for n in out.stdout.split() if n.endswith(".log"))
    first = max(s for s in segments if s <= base)
    at = sum(len(w) for o, w in plan if first <= o < base)
    path = f"{folder}/{first:020d}.log"
    seek = at + len(dict(plan)[base]) - at_from_end
    out = compose("exec", "-T", "postgres", "sh", "-c",
                  f"printf q | dd of={path} bs=1 seek={seek} conv=notrunc status=none")
    assert out.returncode == 0, out.stderr
    return f"/kafgres/{tid}/0/{first:020d}.log at {at}: CRC mismatch"


def test_a_batch_corrupted_on_disk_stops_compaction(small_segments, no_sweeps, topic, conn):
    """A CRC that no longer holds is corruption, not a batch that merely does not decode:
    the pass must stop rather than clean around it or rebuild it with a fresh CRC."""
    set_policy(topic, "compact")
    plan = [(i, batch(keyed(k, v + PAD))) for i, (k, v) in enumerate(
        [(b"k", b"1"), (b"k", b"2"), (b"k", b"3"), (b"z", b"1"), (b"z", b"2"), (b"y", b"1")])]
    for o, wire in plan:
        assert produce(conn, topic, wire, 300 + o) == (0, o)
        time.sleep(0.25)
    named = corrupt_one_byte(topic, conn, 1, 10, plan)

    sql("SELECT kafgres_enforce_retention()")
    time.sleep(0.5)

    stored = fetch_batches(conn, topic)
    assert 0 in stored and 1 in stored, f"compaction went past the corrupt batch: {sorted(stored)}"
    assert stored[1][-10:-9] == b"q", "the corrupt batch was rewritten"
    assert named in server_log(), f"the log does not say where the corruption is: {named}"


def test_an_undecodable_batch_is_decoded_and_logged_once(small_segments, no_sweeps, topic, conn):
    bad = batch(record(_varint(1), b"b", _varint(len(PAD)), PAD, _varint(0), b"\xaa"))
    first = [keyed(b"k", b"1" + PAD), None, keyed(b"k", b"2" + PAD), keyed(b"k", b"3" + PAD),
             keyed(b"z", b"1" + PAD), keyed(b"z", b"2" + PAD), keyed(b"y", b"1" + PAD)]
    next_offset = 0

    def produce_all(records):
        nonlocal next_offset
        for rec in records:
            wire = bad if rec is None else batch(rec)
            assert produce(conn, topic, wire, 400 + next_offset) == (0, next_offset)
            next_offset += 1
            time.sleep(0.05)

    produce_all(first)
    set_policy(topic, "compact")
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
    line = f"compaction keeps batch 1 of topic {tid} partition 0 verbatim"

    # One backend throughout: what the cleaner remembers is per process.
    session = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-X",
         "-q", "-v", "ON_ERROR_STOP=1"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=REPO,
    )

    def run(statement):
        session.stdin.write(statement + "\n\\echo __done__\n")
        session.stdin.flush()
        out = []
        for got in session.stdout:
            if got.strip() == "__done__":
                return out
            out.append(got)
        raise AssertionError(f"psql exited: {''.join(out)}")

    try:
        run("SELECT kafgres_enforce_retention();")
        assert 0 not in fetch_batches(conn, topic), "the first round compacted nothing"
        # Each later round needs as many dirty bytes as there are clean ones; `f` supersedes
        # itself so the clean part stays small.
        older_k = 3
        for round_ in (2, 3):
            newer_k = next_offset
            produce_all([keyed(b"k", b"%d" % round_ + PAD)]
                        + [keyed(b"f", b"%d" % i + PAD) for i in range(20)])
            run("SELECT kafgres_enforce_retention();")
            stored = fetch_batches(conn, topic)
            assert older_k not in stored, f"round {round_} compacted nothing: {sorted(stored)}"
            older_k = newer_k
    finally:
        session.stdin.close()
        session.wait(timeout=60)

    stored = fetch_batches(conn, topic)
    assert 1 in stored, f"the undecodable batch was dropped: {sorted(stored)}"
    assert server_log().count(line) == 1, "the undecodable batch was logged more than once"
