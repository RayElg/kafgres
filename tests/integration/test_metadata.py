"""ApiVersions and Metadata.

The byte-level tests cover the wire gotchas no client test can catch — a client and a
broker that are wrong in the same direction agree perfectly. The client tests at the
bottom are the ones that gate this module.
"""

import struct
import subprocess
import time

import pytest

from conftest import (
    ADVERTISED,
    ADVERTISED_HOST,
    API_VERSIONS,
    METADATA,
    read_compact_string,
    read_legacy_string,
    sql,
)

UNSUPPORTED_VERSION = 35
UNKNOWN_TOPIC_OR_PARTITION = 3
UNKNOWN_TOPIC_ID = 100

@pytest.fixture(scope="module", autouse=True)
def topics():
    sql("SELECT kafgres_drop_topic('it-orders')")
    sql("SELECT kafgres_drop_topic('it-events')")
    sql("SELECT kafgres_create_topic('it-orders', 3)")
    sql("SELECT kafgres_create_topic('it-events', 1)")
    yield
    sql("SELECT kafgres_drop_topic('it-orders')")
    sql("SELECT kafgres_drop_topic('it-events')")

def test_api_versions_v0_lists_what_we_serve(conn):
    conn.send(API_VERSIONS, 0, 111)
    resp = conn.recv()

    correlation, error, count = struct.unpack_from(">ihi", resp, 0)
    assert correlation == 111
    assert error == 0

    entries = {}
    pos = 10
    for _ in range(count):
        key, lo, hi = struct.unpack_from(">hhh", resp, pos)
        entries[key] = (lo, hi)
        pos += 6
    assert entries == ADVERTISED, "advertised set drifted from the handlers"

def test_api_versions_v3_response_header_is_v0(conn):
    """A v3 request is flexible, so the *request* header is v2 — but the response header
    must still be v0, carrying no tagged-field section. The client has to parse the
    response before it knows what version was negotiated. Get this wrong and clients
    connect and hang with no error at all.
    """
    conn.send(API_VERSIONS, 3, 222)
    resp = conn.recv()

    (correlation,) = struct.unpack_from(">i", resp, 0)
    assert correlation == 222

    (error,) = struct.unpack_from(">h", resp, 4)
    assert error == 0, "error_code is not at offset 4 — response header is not v0"

    assert resp[6] == len(ADVERTISED) + 1, "compact array length is count+1"

def test_api_versions_above_our_max_falls_back_to_v0(conn):
    """A newer client sends its highest ApiVersions version before it knows ours.

    Refusing would deadlock discovery, so the broker answers with a v0-encoded body
    carrying UNSUPPORTED_VERSION plus the real ranges, and the client retries.
    """
    conn.send(API_VERSIONS, 9, 333)
    resp = conn.recv()

    correlation, error = struct.unpack_from(">ih", resp, 0)
    assert correlation == 333
    assert error == UNSUPPORTED_VERSION

    (count,) = struct.unpack_from(">i", resp, 6)
    assert count == len(ADVERTISED)
    key, lo, hi = struct.unpack_from(">hhh", resp, 10)
    assert ADVERTISED.get(key) == (lo, hi)

def test_metadata_v13_reports_sql_created_topics(conn):
    body = b"\x00" + b"\x00" + b"\x00" + b"\x00"
    conn.send(METADATA, 13, 444, body=body)
    resp = conn.recv()

    (correlation,) = struct.unpack_from(">i", resp, 0)
    assert correlation == 444
    pos = 4
    assert resp[pos] == 0, "expected empty tagged fields in the response header"
    pos += 1

    (throttle,) = struct.unpack_from(">i", resp, pos)
    assert throttle == 0
    pos += 4

    broker_count = resp[pos] - 1
    pos += 1
    assert broker_count == 1, "singleton broker"
    (node_id,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    host, pos = read_compact_string(resp, pos)
    (port,) = struct.unpack_from(">i", resp, pos)
    assert node_id == 1
    assert host == ADVERTISED_HOST
    assert port == 9092

def test_metadata_unknown_topic_is_an_entry_not_an_omission(conn):
    """Omitting the topic leaves the client waiting for an answer that never comes.
    It must come back carrying UNKNOWN_TOPIC_OR_PARTITION.

    Auto-creation is turned off for this one: `allow_auto_topic_creation` does not exist
    before Metadata v4 and Kafka treats those versions as permitting it, so with the
    broker default (`on`) this v1 request *creates* the topic and there is no unknown
    topic left to report."""
    sql("ALTER SYSTEM SET kafgres.auto_create_topics = off")
    sql("SELECT pg_reload_conf()")
    for _ in range(20):
        time.sleep(0.5)
        if sql("SHOW kafgres.auto_create_topics") == "off":
            break
    try:
        _unknown_topic_is_an_entry(conn)
    finally:
        sql("ALTER SYSTEM RESET kafgres.auto_create_topics")
        sql("SELECT pg_reload_conf()")
        sql("SELECT kafgres_drop_topic('definitely-not-a-topic')")

def _unknown_topic_is_an_entry(conn):
    name = b"definitely-not-a-topic"
    body = struct.pack(">i", 1) + struct.pack(">h", len(name)) + name
    conn.send(METADATA, 1, 555, body=body)
    resp = conn.recv()

    correlation, broker_count = struct.unpack_from(">ii", resp, 0)
    assert correlation == 555
    pos = 8
    for _ in range(broker_count):
        pos += 4  # node_id
        _, pos = read_legacy_string(resp, pos)  # host
        pos += 4  # port
        _, pos = read_legacy_string(resp, pos)  # rack, nullable, v1+

    (controller_id,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert controller_id == 1

    (topic_count,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert topic_count == 1
    (error,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    got_name, pos = read_legacy_string(resp, pos)
    assert error == UNKNOWN_TOPIC_OR_PARTITION
    assert got_name == name.decode()

def test_metadata_v0_empty_array_means_all_topics(conn):
    """Upstream MetadataRequest.java: "In version 0, an empty topic list indicates
    request metadata for all topics." From v1 the same encoding means *no* topics, so
    treating the two alike answers "give me everything" with nothing."""
    conn.send(METADATA, 0, 666, body=struct.pack(">i", 0))
    resp = conn.recv()

    correlation, broker_count = struct.unpack_from(">ii", resp, 0)
    assert correlation == 666
    pos = 8
    for _ in range(broker_count):
        pos += 4  # node_id
        _, pos = read_legacy_string(resp, pos)  # host
        pos += 4  # port

    (topic_count,) = struct.unpack_from(">i", resp, pos)
    assert topic_count >= 2, "v0 empty array must return every topic"

def test_metadata_v12_topic_id_gets_unknown_topic_id(conn):
    """Topic-id addressing is live from v10 and we advertise up to 13. Dropping
    id-addressed entries returns zero topics with error_code 0, which reads to the
    client as 'no such cluster state' rather than 'I do not know that id'."""
    topic_id = bytes(range(16))
    body = b"\x02" + topic_id + b"\x00" + b"\x00" + b"\x00" + b"\x00" + b"\x00"
    conn.send(METADATA, 12, 888, body=body)
    resp = conn.recv()

    (correlation,) = struct.unpack_from(">i", resp, 0)
    assert correlation == 888
    pos = 5  # correlation + empty tagged section (response header v1)
    pos += 4  # throttle_time_ms

    broker_count = resp[pos] - 1
    pos += 1
    for _ in range(broker_count):
        pos += 4  # node_id
        _, pos = read_compact_string(resp, pos)  # host
        pos += 4  # port
        _, pos = read_compact_string(resp, pos)  # rack
        pos += 1  # tagged
    _, pos = read_compact_string(resp, pos)  # cluster_id
    pos += 4  # controller_id

    topic_count = resp[pos] - 1
    pos += 1
    assert topic_count == 1, "the id-addressed topic must get an entry"
    (error,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    name, pos = read_compact_string(resp, pos)
    echoed_id = resp[pos : pos + 16]

    assert error == UNKNOWN_TOPIC_ID
    assert name is None, "name must be null for a topic queried by id"
    assert echoed_id == topic_id, "the requested id must be echoed back"

def test_metadata_with_absurd_topic_count_is_refused(conn):
    """A 2-byte-per-entry topic list must not be able to inflate the response without
    bound. The cap has to be applied before assembly, since that is what allocates."""
    n = 50_000
    body = b""
    v = n + 1
    while v >= 0x80:
        body += bytes([(v & 0x7F) | 0x80])
        v >>= 7
    body += bytes([v])
    body += (b"\x01" + b"\x00") * n  # empty name + empty tagged, per entry
    body += b"\x00" + b"\x00" + b"\x00"
    conn.send(METADATA, 12, 999, body=body)
    assert conn.closed(), "an oversized topic list should be refused, not assembled"

def test_pipelined_requests_answer_in_order(conn):
    """Clients pipeline. Responses are matched by correlation id, but a broker that
    reorders them still breaks clients that assume FIFO per connection."""
    for i in range(5):
        conn.send(API_VERSIONS, 0, 1000 + i)
    for i in range(5):
        resp = conn.recv()
        (correlation,) = struct.unpack_from(">i", resp, 0)
        assert correlation == 1000 + i

def test_partial_frame_then_completion_is_handled(conn):
    """A frame split across TCP segments must not be misparsed."""
    header = struct.pack(">hhi", API_VERSIONS, 0, 777)
    header += struct.pack(">h", 4) + b"halb"
    conn.sock.sendall(struct.pack(">i", len(header)))
    conn.sock.sendall(header[:5])
    conn.sock.sendall(header[5:])
    resp = conn.recv()
    (correlation,) = struct.unpack_from(">i", resp, 0)
    assert correlation == 777

def docker_run(image, *args):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", image, *args],
        capture_output=True,
        text=True,
        timeout=180,
    )

def test_kcat_lists_the_cluster():
    """`kcat -b localhost:9092 -L` prints a one-broker cluster with the SQL topics."""
    out = docker_run("kafgres-clients", "kcat", "-b", "localhost:9092", "-L")
    assert out.returncode == 0, out.stderr
    assert "1 brokers:" in out.stdout
    assert f"broker 1 at {ADVERTISED_HOST}:9092 (controller)" in out.stdout
    assert 'topic "it-orders" with 3 partitions' in out.stdout
    assert 'topic "it-events" with 1 partitions' in out.stdout
    assert "partition 2, leader 1, replicas: 1, isrs: 1" in out.stdout

def test_java_admin_client_describe_cluster():
    """The Java AdminClient's describeCluster() succeeds."""
    out = docker_run(
        "apache/kafka:4.1.0",
        "/opt/kafka/bin/kafka-cluster.sh",
        "cluster-id",
        "--bootstrap-server",
        "localhost:9092",
    )
    assert out.returncode == 0, out.stderr
    assert "Cluster ID: kafgres-cluster" in out.stdout

def test_java_admin_client_sees_only_implemented_apis():
    """Over-advertising is the "works with one client, hangs with another" failure.
    The Java client must see exactly what we implement, and cleanly report the rest
    unsupported rather than negotiating something we cannot answer."""
    out = docker_run(
        "apache/kafka:4.1.0",
        "/opt/kafka/bin/kafka-broker-api-versions.sh",
        "--bootstrap-server",
        "localhost:9092",
    )
    assert out.returncode == 0, out.stderr
    assert "Metadata(3): 0 to 13" in out.stdout
    assert "ApiVersions(18): 0 to 4" in out.stdout
    assert "Produce(0): 0 to 13" in out.stdout
    assert "Fetch(1): 4 to 18" in out.stdout
    assert "ListOffsets(2): 1 to 6" in out.stdout
    assert "JoinGroup(11): 0 to 9" in out.stdout
    assert "InitProducerId(22): 0 to 5" in out.stdout
    assert "CreateTopics(19): 2 to 7" in out.stdout
    assert "DescribeConfigs(32): 1 to 4" in out.stdout
    assert "DescribeCluster(60): 0 to 2" in out.stdout
    assert "OffsetForLeaderEpoch(23): 2 to 4" in out.stdout
    assert "DescribeAcls(29): 1 to 3" in out.stdout
    assert "CreateAcls(30): 1 to 3" in out.stdout
    assert "DeleteAcls(31): 1 to 3" in out.stdout
    assert "TxnOffsetCommit(28): 0 to 3" in out.stdout
    assert "AddPartitionsToTxn(24): 0 to 3" in out.stdout
    assert "EndTxn(26): 0 to 3" in out.stdout
    assert "WriteTxnMarkers(27): 1 [usable: 1]" in out.stdout
    assert "DescribeTopicPartitions(75): 0 [usable: 0]" in out.stdout
    assert "AlterClientQuotas(49): 0 to 1" in out.stdout
    assert "AlterUserScramCredentials(51): 0 [usable: 0]" in out.stdout
    assert "ShareFetch(78): 1 [usable: 1]" in out.stdout
    assert "StreamsGroupHeartbeat(88): UNSUPPORTED" in out.stdout

def test_kafka_topics_sh_lists_topics():
    out = docker_run(
        "apache/kafka:4.1.0",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--list",
    )
    assert out.returncode == 0, out.stderr
    assert "it-orders" in out.stdout
    assert "it-events" in out.stdout
