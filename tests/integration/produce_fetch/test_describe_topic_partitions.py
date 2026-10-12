"""`75 DescribeTopicPartitions` — Metadata's topic half, paginated.

`Metadata` returns every partition of every topic named, in one response, which on a large
cluster is a response nobody can bound; here the client says how many partitions it will
accept and gets a cursor to continue from. Expectations were taken from Kafka 4.3.1 over
the wire, not from the schema.
"""

import socket
import struct
import subprocess

import pytest

from conftest import BROKER_HOST, BROKER_PORT, sql

KAFKA = "apache/kafka:4.1.0"
BROKER = "127.0.0.1:9092"
TOPICS = ["dtp-a", "dtp-b", "dtp-c"]

@pytest.fixture(autouse=True)
def topics():
    for t in TOPICS:
        sql(f"SELECT kafgres_drop_topic('{t}')")
        sql(f"SELECT kafgres_create_topic('{t}', 3)")
    yield
    for t in TOPICS:
        sql(f"SELECT kafgres_drop_topic('{t}')")

def uvarint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | 0x80]) if n else bytes([b])
        if not n:
            return out

def compact_str(s):
    raw = s.encode()
    return uvarint(len(raw) + 1) + raw

def describe(names, limit=2000, cursor=None):
    """One DescribeTopicPartitions v0 round trip, hand-framed.

    Raw rather than through `AdminClient`: an empty topic list and a cursor mid-topic
    are things no client library will construct on demand.
    """
    body = uvarint(len(names) + 1)
    for n in names:
        body += compact_str(n) + b"\x00"
    body += struct.pack(">i", limit)
    body += b"\xff" if cursor is None else (
        b"\x01" + compact_str(cursor[0]) + struct.pack(">i", cursor[1]) + b"\x00")
    body += b"\x00"
    header = struct.pack(">hhi", 75, 0, 1) + struct.pack(">h", 6) + b"pytest" + b"\x00"
    frame = header + body

    s = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=15)
    try:
        s.sendall(struct.pack(">i", len(frame)) + frame)
        size = struct.unpack(">i", s.recv(4))[0]
        buf = b""
        while len(buf) < size:
            buf += s.recv(size - len(buf))
    finally:
        s.close()
    return Reader(buf).response()

class Reader:
    def __init__(self, b):
        self.b, self.i = b, 0

    def i8(self):
        v = struct.unpack_from(">b", self.b, self.i)[0]; self.i += 1; return v

    def i16(self):
        v = struct.unpack_from(">h", self.b, self.i)[0]; self.i += 2; return v

    def i32(self):
        v = struct.unpack_from(">i", self.b, self.i)[0]; self.i += 4; return v

    def uv(self):
        r, sh = 0, 0
        while True:
            c = self.b[self.i]; self.i += 1
            r |= (c & 0x7F) << sh
            if not c & 0x80:
                return r
            sh += 7

    def cstr(self):
        n = self.uv()
        if n == 0:
            return None
        v = self.b[self.i:self.i + n - 1].decode(); self.i += n - 1; return v

    def tags(self):
        for _ in range(self.uv()):
            self.uv()
            self.i += self.uv()

    def array(self):
        """Returns None for a null array and a list for a present one — the distinction
        this file exists to police."""
        n = self.uv()
        if n == 0:
            return None
        return [self.i32() for _ in range(n - 1)]

    def response(self):
        self.i32(); self.tags(); self.i32()          # correlation, header tags, throttle
        topics = []
        for _ in range(self.uv() - 1):
            err = self.i16(); name = self.cstr()
            self.i += 16                              # topic id
            self.i8()                                 # is_internal
            parts = []
            for _ in range(self.uv() - 1):
                self.i16()
                idx = self.i32(); self.i32(); self.i32()
                arrays = [self.array() for _ in range(5)]
                self.tags()
                parts.append((idx, arrays))
            self.i32(); self.tags()
            topics.append({"name": name, "error": err, "partitions": parts})
        cursor = None if self.i8() < 0 else (self.cstr(), self.i32())
        return {"topics": topics, "cursor": cursor}

def names_and_partitions(resp):
    return [(t["name"], [p[0] for p in t["partitions"]]) for t in resp["topics"]]

def test_an_empty_topic_list_means_every_topic():
    """The trap: `DescribeLogDirs` treats an empty list as *none*. Here an empty list
    returns all topics, confirmed against Kafka 4.3.1."""
    got = dict(names_and_partitions(describe([])))
    for t in TOPICS:
        assert got.get(t) == [0, 1, 2], got

def test_the_limit_counts_partitions_and_splits_a_topic():
    """Not topics: the cursor points *into* a topic when the limit splits one."""
    resp = describe(TOPICS, limit=4)
    assert names_and_partitions(resp) == [("dtp-a", [0, 1, 2]), ("dtp-b", [0])], resp
    assert resp["cursor"] == ("dtp-b", 1), resp["cursor"]

def test_a_cursor_is_inclusive_of_the_partition_it_names():
    """Resuming at `(dtp-b, 1)` yields partition 1 again; an exclusive reading would
    lose one partition at every page boundary, invisibly."""
    resp = describe(TOPICS, limit=4, cursor=("dtp-b", 1))
    assert names_and_partitions(resp) == [("dtp-b", [1, 2]), ("dtp-c", [0, 1])], resp
    assert resp["cursor"] == ("dtp-c", 2), resp["cursor"]

    first = describe(TOPICS, limit=4)
    seen = [(t, p) for t, ps in names_and_partitions(first) for p in ps]
    seen += [(t, p) for t, ps in names_and_partitions(resp) for p in ps]
    assert seen == [("dtp-a", 0), ("dtp-a", 1), ("dtp-a", 2), ("dtp-b", 0),
                    ("dtp-b", 1), ("dtp-b", 2), ("dtp-c", 0), ("dtp-c", 1)], seen

def test_an_unknown_topic_is_an_entry_not_an_omission():
    """Omitting it leaves the client waiting for an answer about a topic it asked
    about."""
    resp = describe(["no-such-topic"])
    assert resp["topics"] == [{"name": "no-such-topic", "error": 3, "partitions": []}], resp

def test_the_leader_replica_fields_are_empty_lists_not_null():
    """`eligibleLeaderReplicas` and `lastKnownElr` are `nullableVersions: 0+`, so null
    is schema-legal — but `KafkaAdminClient` calls `.stream()` on them with no null
    check and `kafka-topics.sh --describe` dies with a `NullPointerException`."""
    resp = describe(["dtp-a"])
    for _, arrays in resp["topics"][0]["partitions"]:
        replicas, isr, elr, last_elr, offline = arrays
        assert replicas == [1] and isr == [1], arrays
        assert elr == [], f"eligible_leader_replicas is {elr!r}; the Java client NPEs on null"
        assert last_elr == [], f"last_known_elr is {last_elr!r}; same NPE"
        assert offline == [], arrays

def test_kafka_topics_describe_works_end_to_end():
    """The property all of the above is in service of: an unmodified tool prints a topic."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", BROKER,
         "--describe", "--topic", "dtp-b"],
        capture_output=True, text=True, timeout=180,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert "PartitionCount: 3" in out.stdout, out.stdout
    assert out.stdout.count("Leader: 1") == 3, out.stdout
    assert "Elr:" in out.stdout, out.stdout
