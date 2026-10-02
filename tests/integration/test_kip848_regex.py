"""KIP-848 regex subscriptions (`ConsumerGroupHeartbeat` v1), through raw heartbeats so the
assignment reads exactly, and once end to end through librdkafka."""

import struct
import subprocess
import uuid

import pytest

from conftest import sql

BROKER = "127.0.0.1:9092"
CLIENTS = "kafgres-clients"
CGH = 68
INVALID_REGULAR_EXPRESSION = 128


def uvarint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out += bytes([b | 0x80])
        else:
            return out + bytes([b])


def cstr(s):
    if s is None:
        return uvarint(0)
    b = s.encode()
    return uvarint(len(b) + 1) + b


def heartbeat(conn, group, member, epoch, names=None, regex=None, correlation=1):
    """One v1 heartbeat; returns (error, member_epoch, assigned topic uuids)."""
    body = cstr(group) + cstr(member) + struct.pack(">i", epoch) + cstr(None) + cstr(None)
    body += struct.pack(">i", 60000)
    if names is None:
        body += uvarint(0)
    else:
        body += uvarint(len(names) + 1) + b"".join(cstr(n) for n in names)
    body += cstr(regex) + cstr(None) + uvarint(0) + uvarint(0)
    header = struct.pack(">hhi", CGH, 1, correlation) + struct.pack(">h", 6) + b"pytest" + uvarint(0)
    frame = header + body
    conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
    resp = conn.recv()
    pos = 4  # correlation id
    pos += 1  # header tagged fields

    def read_uvarint():
        nonlocal pos
        shift = n = 0
        while True:
            b = resp[pos]
            pos += 1
            n |= (b & 0x7F) << shift
            if not b & 0x80:
                return n
            shift += 7

    def read_cstr():
        nonlocal pos
        n = read_uvarint()
        if n == 0:
            return None
        s = resp[pos:pos + n - 1].decode()
        pos += n - 1
        return s

    pos += 4  # throttle
    (err,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    message = read_cstr()
    read_cstr()  # member id
    (member_epoch,) = struct.unpack_from(">i", resp, pos)
    pos += 8  # epoch, heartbeat interval
    # None for a null ("unchanged") assignment, not "nothing".
    topics = None
    if pos < len(resp) and resp[pos] == 1:
        topics = set()
        pos += 1
        for _ in range(read_uvarint() - 1):
            topics.add(resp[pos:pos + 16])
            pos += 16
            n = read_uvarint() - 1
            pos += 4 * n
            read_uvarint()
    return err, member_epoch, topics, message


def uuid_of(name):
    hexed = sql(f"SELECT encode(topic_uuid, 'hex') FROM kafgres_topics WHERE name = '{name}'")
    return bytes.fromhex(hexed)


@pytest.fixture
def rx():
    run = uuid.uuid4().hex[:6]
    created = []

    def make(name, partitions=1):
        sql(f"SELECT kafgres_create_topic('{name}', {partitions})")
        created.append(name)
        return name

    yield run, make
    for n in created:
        sql(f"SELECT kafgres_drop_topic('{n}')")


def test_a_regex_matches_whole_names_and_nothing_else(conn, rx):
    run, make = rx
    a, b = make(f"rx-{run}-a", 2), make(f"rx-{run}-b")
    make(f"other-{run}")
    make(f"rx-{run}-a-but-longer-x")  # matches only if the anchor at the end is missing
    make(f"pre-rx-{run}-a")           # matches only if the anchor at the start is missing
    member = uuid.uuid4().hex
    err, _, topics, _ = heartbeat(conn, f"g-{run}", member, 0, regex=f"rx-{run}-(a|b)")
    assert err == 0
    assert topics == {uuid_of(a), uuid_of(b)}


def test_a_topic_created_later_is_assigned_on_a_later_heartbeat(conn, rx):
    run, make = rx
    a = make(f"rx-{run}-a")
    member, group = uuid.uuid4().hex, f"g-{run}"
    err, epoch, topics, _ = heartbeat(conn, group, member, 0, regex=f"rx-{run}-.*")
    assert (err, topics) == (0, {uuid_of(a)})
    late = make(f"rx-{run}-late")
    err, epoch2, topics, _ = heartbeat(conn, group, member, epoch, correlation=2)
    assert err == 0 and epoch2 > epoch, "the new match did not move the group epoch"
    assert topics == {uuid_of(a), uuid_of(late)}


def test_a_dropped_topic_and_a_cleared_pattern_are_unassigned(conn, rx):
    run, make = rx
    a, b = make(f"rx-{run}-a"), make(f"rx-{run}-b")
    member, group = uuid.uuid4().hex, f"g-{run}"
    _, epoch, topics, _ = heartbeat(conn, group, member, 0, regex=f"rx-{run}-.*")
    assert topics == {uuid_of(a), uuid_of(b)}
    gone = uuid_of(b)
    sql(f"SELECT kafgres_drop_topic('{b}')")
    _, epoch, topics, _ = heartbeat(conn, group, member, epoch, correlation=2)
    assert gone not in topics and uuid_of(a) in topics, topics
    # An empty pattern drops the regex, and there are no names.
    _, _, topics, _ = heartbeat(conn, group, member, epoch, regex="", correlation=3)
    assert topics == set(), f"the cleared pattern left {topics} (None: no new assignment sent)"


def test_leading_options_apply_to_the_whole_pattern(conn, rx):
    """`(?i)` is valid only first in Postgres, so it precedes the anchors."""
    run, make = rx
    upper = make(f"RX-{run}-Upper")
    member = uuid.uuid4().hex
    err, _, topics, message = heartbeat(conn, f"g-{run}", member, 0, regex=f"(?i)rx-{run}-upper")
    assert err == 0, message
    assert topics == {uuid_of(upper)}


def test_names_and_a_regex_together_are_unioned(conn, rx):
    run, make = rx
    a, other = make(f"rx-{run}-a"), make(f"other-{run}")
    member = uuid.uuid4().hex
    err, _, topics, _ = heartbeat(conn, f"g-{run}", member, 0, names=[other], regex=f"rx-{run}-.*")
    assert err == 0 and topics == {uuid_of(a), uuid_of(other)}


@pytest.mark.parametrize("pattern", ["rx-(unclosed", r"\p{L}+", "nothing)|(.*"])
def test_a_pattern_postgres_cannot_compile_is_refused(conn, pattern):
    """`\\p{L}` is RE2J only; `nothing)|(.*` would escape the anchors once wrapped."""
    err, _, topics, message = heartbeat(conn, f"g-bad-{uuid.uuid4().hex[:6]}", uuid.uuid4().hex, 0,
                                        regex=pattern)
    assert err == INVALID_REGULAR_EXPRESSION, (err, message)
    assert not topics


def test_librdkafka_consumes_by_regex_end_to_end(rx):
    """librdkafka asks for v1 only."""
    run, make = rx
    names = [make(f"rx-{run}-a", 2), make(f"rx-{run}-b"), make(f"other-{run}")]
    script = f"""
import time
from confluent_kafka import Consumer, Producer
p = Producer({{"bootstrap.servers": "{BROKER}"}})
for n in {names!r}:
    for i in range(50):
        p.produce(n, value=f"{{n}}:{{i}}")
p.flush(10)
c = Consumer({{"bootstrap.servers": "{BROKER}", "group.id": "g-e2e-{run}",
               "group.protocol": "consumer", "auto.offset.reset": "earliest"}})
c.subscribe(["^rx-{run}-.*"])
seen, end = {{}}, time.time() + 30
while time.time() < end and sum(seen.values()) < 100:
    m = c.poll(0.5)
    if m is not None and not m.error():
        seen[m.topic()] = seen.get(m.topic(), 0) + 1
c.close()
print(sorted(seen.items()))
"""
    out = subprocess.run(["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "python3", "-"],
                         input=script, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-800:]
    assert out.stdout.strip() == str([(names[0], 50), (names[1], 50)]), out.stdout
