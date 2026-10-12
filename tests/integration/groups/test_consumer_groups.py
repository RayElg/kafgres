"""Consumer groups.

Beyond the two rebalance round-trips, these cover the parts of the group protocol that
fail quietly: a member that is never evicted, a heartbeat that does not signal a
rebalance, an offset that is not durable.
"""

import struct
import subprocess
import time

import pytest

from conftest import OFFSET_DELETE, sql

CLIENTS = "kafgres-clients"
KAFKA = "apache/kafka:4.1.0"
BROKER = "127.0.0.1:9092"

SESSION_MS = 6000
HEARTBEAT_MS = 2000

def kcat(*args, stdin=None, timeout=180):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER, *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def kafka_tool(script, *args, timeout=240):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA, f"/opt/kafka/bin/{script}",
         "--bootstrap-server", BROKER, *args],
        capture_output=True, text=True, timeout=timeout,
    )

def start_consumer(name, group, topic):
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.run(
        ["docker", "run", "--rm", "-d", "--network", "host", "--name", name, CLIENTS,
         "kcat", "-b", BROKER, "-G", group, topic,
         "-X", f"session.timeout.ms={SESSION_MS}",
         "-X", f"heartbeat.interval.ms={HEARTBEAT_MS}"],
        capture_output=True, timeout=120,
    )

def stop_consumer(name, kill=False):
    subprocess.run(["docker", "kill" if kill else "stop", name], capture_output=True, timeout=120)
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)

def group_state(group):
    row = sql(
        f"""SELECT state || '|' || generation || '|' ||
                   (SELECT count(*) FROM kafgres_group_members m WHERE m.group_id = g.group_id)
              FROM kafgres_groups g WHERE group_id = '{group}'"""
    ).strip()
    if not row:
        return None, 0, 0
    state, gen, members = row.split("|")
    return state, int(gen), int(members)

def await_group(group, members=None, state=None, timeout=40):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = group_state(group)
        if (members is None or last[2] == members) and (state is None or last[0] == state):
            return last
        time.sleep(1)
    pytest.fail(f"group {group} never reached members={members} state={state}; last={last}")

def assignments(group):
    """partition -> consumer id, from kafka-consumer-groups.sh --describe."""
    out = kafka_tool("kafka-consumer-groups.sh", "--describe", "--group", group)
    result = {}
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 7 and parts[0] == group:
            result[int(parts[2])] = parts[6]
    return result

def offset_delete(conn, group, topic, partitions):
    """`47 OffsetDelete` v0 on the wire. Returns (top_level_error, {partition: error}).

    v0 is the only version the 4.x schema defines and it is not flexible, so every string
    is int16-prefixed and every array int32-counted — no compact encodings, no tagged
    fields.
    """
    body = struct.pack(">h", len(group)) + group.encode()
    body += struct.pack(">i", 1)
    body += struct.pack(">h", len(topic)) + topic.encode()
    body += struct.pack(">i", len(partitions))
    for p in partitions:
        body += struct.pack(">i", p)
    conn.send(OFFSET_DELETE, 0, 909, body)
    resp = conn.recv()

    pos = 4  # correlation id; v0 response header is v0, so no tagged fields
    (error,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    pos += 4  # throttle_time_ms
    (topic_count,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    results = {}
    for _ in range(topic_count):
        (nlen,) = struct.unpack_from(">h", resp, pos)
        pos += 2 + nlen
        (pcount,) = struct.unpack_from(">i", resp, pos)
        pos += 4
        for _ in range(pcount):
            index, code = struct.unpack_from(">ih", resp, pos)
            pos += 6
            results[index] = code
    return error, results

@pytest.fixture
def topic(request):
    name = f"p3-{request.node.name.replace('_', '-')[:38]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

@pytest.fixture
def group(request):
    name = f"g3-{request.node.name.replace('_', '-')[:38]}"
    yield name
    sql(f"DELETE FROM kafgres_offsets WHERE group_id = '{name}'")
    sql(f"DELETE FROM kafgres_groups WHERE group_id = '{name}'")

def make(name, partitions=1):
    sql(f"SELECT kafgres_create_topic('{name}', {partitions})")

def test_two_consumers_split_the_partitions(topic, group):
    make(topic, partitions=2)
    assert kcat("-t", topic, "-P", stdin="".join(f"m{i}\n" for i in range(20))).returncode == 0

    start_consumer("p3a", group, topic)
    try:
        await_group(group, members=1, state="Stable")
        start_consumer("p3b", group, topic)
        try:
            _, generation, _ = await_group(group, members=2, state="Stable")
            assert generation >= 2, "a second member must cut a new generation"

            owned = assignments(group)
            assert set(owned) == {0, 1}, f"both partitions should be assigned: {owned}"
            assert len(set(owned.values())) == 2, f"partitions must split across members: {owned}"
        finally:
            stop_consumer("p3b")
    finally:
        stop_consumer("p3a")

def test_killing_a_consumer_rebalances_to_the_survivor(topic, group):
    """Second half. `docker kill`, not `stop`: a member that gets no chance to send
    LeaveGroup is the case that can only be recovered by the session-timeout sweep, and
    the sweep only runs on the background tick — no request path will ever notice a
    consumer that has stopped sending."""
    make(topic, partitions=2)
    assert kcat("-t", topic, "-P", stdin="".join(f"m{i}\n" for i in range(20))).returncode == 0

    start_consumer("p3a", group, topic)
    start_consumer("p3b", group, topic)
    try:
        _, gen_before, _ = await_group(group, members=2, state="Stable")

        subprocess.run(["docker", "kill", "p3a"], capture_output=True, timeout=120)
        _, gen_after, _ = await_group(group, members=1, state="Stable", timeout=60)

        assert gen_after > gen_before, "eviction must cut a new generation"
        owned = assignments(group)
        assert set(owned) == {0, 1}, f"survivor should hold every partition: {owned}"
        assert len(set(owned.values())) == 1, f"one member should own both: {owned}"
    finally:
        stop_consumer("p3a")
        stop_consumer("p3b")

def test_describe_reports_offset_log_end_and_lag(topic, group):
    """`kafka-consumer-groups.sh --describe` shows correct current-offset,
    log-end-offset and lag for every partition."""
    make(topic, partitions=2)
    assert kcat("-t", topic, "-P", stdin="".join(f"m{i}\n" for i in range(10))).returncode == 0

    out = kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180)
    assert len(out.stdout.split()) == 10, out.stderr

    described = kafka_tool("kafka-consumer-groups.sh", "--describe", "--group", group)
    assert described.returncode == 0, described.stderr

    rows = [l.split() for l in described.stdout.splitlines() if l.split()[:1] == [group]]
    assert rows, described.stdout
    total = 0
    for r in rows:
        current, log_end, lag = r[3], r[4], r[5]
        assert lag == "0", f"consumed everything, lag should be 0: {r}"
        assert current == log_end, f"current offset should equal LEO: {r}"
        total += int(log_end)
    assert total == 10, f"log end offsets should sum to the 10 records produced: {rows}"

def test_java_console_consumer_works(topic, group):
    """kafka-console-consumer uses a consumer group, so it needs FindCoordinator."""
    make(topic)
    assert kcat("-t", topic, "-P", stdin="j1\nj2\nj3\n").returncode == 0

    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", topic, "--from-beginning", "--timeout-ms", "20000",
         "--group", group],
        capture_output=True, text=True, timeout=300,
    )
    assert "j1" in out.stdout and "j2" in out.stdout and "j3" in out.stdout, out.stdout + out.stderr

def test_list_groups(topic, group):
    make(topic)
    assert kcat("-t", topic, "-P", stdin="x\n").returncode == 0
    assert kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180).returncode == 0

    out = kafka_tool("kafka-consumer-groups.sh", "--list")
    assert out.returncode == 0, out.stderr
    assert group in out.stdout

def test_committed_offsets_survive_a_consumer_restart(topic, group):
    """The point of committing at all. A second run must resume, not replay — and
    `auto.offset.reset` must not be consulted, because there *is* a committed offset."""
    make(topic)
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\n").returncode == 0

    first = kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180)
    assert first.stdout.split() == ["a", "b", "c"], first.stderr

    assert kcat("-t", topic, "-P", stdin="d\ne\n").returncode == 0
    second = kcat("-G", group, topic, "-e", timeout=180)
    assert second.stdout.split() == ["d", "e"], (
        f"resumed from the wrong place: {second.stdout.split()}"
    )

def test_committed_offset_is_visible_in_sql(topic, group):
    """kafgres_offsets replaces __consumer_offsets. Clients never read that topic
    directly, so a plain table is enough — but it has to actually be written."""
    make(topic)
    assert kcat("-t", topic, "-P", stdin="1\n2\n3\n4\n").returncode == 0
    assert kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180).returncode == 0

    committed = sql(
        f"""SELECT sum(committed_offset) FROM kafgres_offsets WHERE group_id = '{group}'"""
    ).strip()
    assert committed == "4", f"expected the group to have committed offset 4, got {committed!r}"

def test_a_dead_member_is_evicted_by_the_background_sweep(topic, group):
    """A dead consumer sends nothing, so no request path will ever run to notice it;
    expiry has to live on the worker tick. Without the sweep its partitions stay
    assigned to a process that no longer exists."""
    make(topic)
    start_consumer("p3solo", group, topic)
    try:
        await_group(group, members=1, state="Stable")
        subprocess.run(["docker", "kill", "p3solo"], capture_output=True, timeout=120)

        deadline = time.time() + 45
        while time.time() < deadline:
            _, _, members = group_state(group)
            if members == 0:
                return
            time.sleep(1)
        pytest.fail("a dead member was never evicted; the sweep is not running")
    finally:
        stop_consumer("p3solo")

def test_heartbeat_signals_a_rebalance(topic, group, conn):
    """The heartbeat response *is* the rebalance signal. A member learns a rebalance
    started only by being told REBALANCE_IN_PROGRESS, and that is what makes it rejoin.
    Answering NONE leaves every member idle until its own session expires and the group
    never converges — with nothing logged."""
    make(topic, partitions=2)
    start_consumer("p3hb", group, topic)
    try:
        await_group(group, members=1, state="Stable")
        sql(
            f"""UPDATE kafgres_groups SET state='PreparingRebalance',
                       rebalance_deadline = now() + interval '30 seconds'
                 WHERE group_id='{group}'"""
        )
        await_group(group, state="Stable", timeout=40)
    finally:
        stop_consumer("p3hb")

def test_join_group_without_a_member_id_is_told_one(conn):
    """KIP-394. The broker issues an id and asks the client to retry with it, and
    deliberately does not register it yet — a client that dies in between must leave
    nothing behind. The retry then presents an id the coordinator has never seen, so
    rejecting unknown ids on JoinGroup loops the client forever."""
    from conftest import JOIN_GROUP

    group_id = b"kip394-group"
    proto_type = b"consumer"
    proto_name = b"range"
    body = struct.pack(">h", len(group_id)) + group_id
    body += struct.pack(">ii", 10000, 30000)
    body += struct.pack(">h", 0)                       # member_id ""
    body += struct.pack(">h", -1)                      # group_instance_id null
    body += struct.pack(">h", len(proto_type)) + proto_type
    body += struct.pack(">i", 1)
    body += struct.pack(">h", len(proto_name)) + proto_name
    body += struct.pack(">i", 4) + b"\x00\x01\x02\x03"  # opaque metadata

    conn.send(JOIN_GROUP, 5, 5150, body=body)
    resp = conn.recv()

    correlation, _throttle, error = struct.unpack_from(">iih", resp, 0)
    assert correlation == 5150
    assert error == 79, f"expected MEMBER_ID_REQUIRED (79), got {error}"

    pos = 10
    (generation,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert generation == -1
    for _ in range(2):  # protocol_name, leader
        (n,) = struct.unpack_from(">h", resp, pos)
        pos += 2 + max(n, 0)
    (n,) = struct.unpack_from(">h", resp, pos)
    pos += 2
    member_id = resp[pos : pos + n].decode()
    assert member_id, "the broker must supply a member id to retry with"

    sql("DELETE FROM kafgres_groups WHERE group_id = 'kip394-group'")

def test_three_members_rebalance_without_stripping_a_healthy_one(topic, group):
    """The two-member test cannot catch this.

    With one survivor, the join window closes because *every* remaining member has
    rejoined, so the rebalance deadline is never consulted. With three, a member that has
    not yet noticed the rebalance — it learns only at its next heartbeat, up to
    `heartbeat.interval.ms` later — is still present when the deadline fires, and a short
    deadline deletes it. The symptom is a healthy consumer silently losing its partitions
    to another member while it is still fetching them, then discovering it at its next
    heartbeat: a window where two members own the same partitions.
    """
    make(topic, partitions=6)
    assert kcat("-t", topic, "-P", stdin="".join(f"m{i}\n" for i in range(30))).returncode == 0

    for name in ("p3x", "p3y", "p3z"):
        start_consumer(name, group, topic)
    try:
        await_group(group, members=3, state="Stable", timeout=60)

        subprocess.run(["docker", "kill", "p3x"], capture_output=True, timeout=120)
        await_group(group, members=2, state="Stable", timeout=60)

        owned = assignments(group)
        assert set(owned) == set(range(6)), f"every partition must stay assigned: {owned}"
        holders = set(owned.values())
        assert len(holders) == 2, (
            f"both survivors should still hold partitions, got {len(holders)}: {owned}"
        )
    finally:
        for name in ("p3x", "p3y", "p3z"):
            stop_consumer(name)

def test_a_member_waiting_in_a_rebalance_is_not_evicted(topic, group, conn):
    """Upstream: "Members that are awaiting a rebalance automatically satisfy expected
    heartbeats." The Java consumer stops its heartbeat thread while a JoinGroup is
    outstanding, so a parked member sends nothing at all — and the sweep is pure SQL over
    last_heartbeat, which cannot see the pending map. Without an explicit refresh, any
    rebalance longer than session.timeout.ms evicts the very members it is waiting for
    and can never converge."""
    make(topic)
    sql(
        f"""INSERT INTO kafgres_groups (group_id, state, generation, rebalance_deadline)
            VALUES ('{group}', 'PreparingRebalance', 0, now() + interval '120 seconds')"""
    )
    sql(
        f"""INSERT INTO kafgres_group_members
              (group_id, member_id, session_timeout_ms, rebalance_timeout_ms,
               joined_generation, protocols, last_heartbeat)
            VALUES ('{group}', 'blocker', 300000, 120000, 0, '{{range}}', now())"""
    )

    from conftest import JOIN_GROUP

    gid = group.encode()
    body = struct.pack(">h", len(gid)) + gid
    body += struct.pack(">ii", 2000, 120000)         # 2s session timeout, long rebalance
    body += struct.pack(">h", len("parked")) + b"parked"
    body += struct.pack(">h", -1)
    body += struct.pack(">h", 8) + b"consumer"
    body += struct.pack(">i", 1)
    body += struct.pack(">h", 5) + b"range"
    body += struct.pack(">i", 0)
    conn.send(JOIN_GROUP, 5, 6161, body=body)

    time.sleep(8)
    alive = sql(
        f"""SELECT count(*) FROM kafgres_group_members
             WHERE group_id = '{group}' AND member_id = 'parked'"""
    ).strip()
    assert alive == "1", "a member parked in a rebalance was evicted for not heartbeating"

def test_oversized_commit_metadata_is_refused(topic, group, conn):
    """offset.metadata.max.bytes. Each commit is a row and one later OffsetFetch
    assembles every row into a single response buffer inside the backend, so unbounded
    metadata is an amplifier rather than a nuisance. Upstream's code is
    OFFSET_METADATA_TOO_LARGE; truncating instead would corrupt whatever the client
    stores there."""
    make(topic)
    from conftest import OFFSET_COMMIT

    gid = group.encode()
    tname = topic.encode()
    huge = b"z" * 5000
    body = struct.pack(">h", len(gid)) + gid
    body += struct.pack(">i", -1)
    body += struct.pack(">h", 0)
    body += struct.pack(">q", -1)
    body += struct.pack(">i", 1) + struct.pack(">h", len(tname)) + tname
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0) + struct.pack(">q", 5)
    body += struct.pack(">h", len(huge)) + huge

    conn.send(OFFSET_COMMIT, 2, 7171, body=body)
    resp = conn.recv()

    pos = 4
    (topic_count,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert topic_count == 1
    (n,) = struct.unpack_from(">h", resp, pos)
    pos += 2 + n
    (part_count,) = struct.unpack_from(">i", resp, pos)
    pos += 4
    assert part_count == 1
    _index, error = struct.unpack_from(">ih", resp, pos)
    assert error == 12, f"expected OFFSET_METADATA_TOO_LARGE (12), got {error}"

    stored = sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id = '{group}'").strip()
    assert stored == "0", "an over-long commit must not be stored"

def test_delete_offsets_makes_a_group_replay(topic, group):
    """`kafka-consumer-groups.sh --delete-offsets`, the reason API 47 exists.

    An operator wants a group to re-read a topic without destroying the group. Deleting
    the group would also do it, and would discard its offsets for every *other* topic it
    reads — so the coarse tool is not a substitute for this one.
    """
    make(topic)
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\n").returncode == 0
    assert kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180).returncode == 0
    assert sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id = '{group}'") == "1"

    out = kafka_tool("kafka-consumer-groups.sh", "--delete-offsets",
                     "--group", group, "--topic", topic)
    assert out.returncode == 0, out.stderr or out.stdout
    assert sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id = '{group}'") == "0", (
        "the committed offset survived --delete-offsets"
    )

    assert sql(f"SELECT count(*) FROM kafgres_groups WHERE group_id = '{group}'") == "1"
    replayed = kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180)
    assert replayed.stdout.split() == ["a", "b", "c"], (
        f"the group did not replay after its offsets were deleted: {replayed.stdout.split()}"
    )

def test_delete_offsets_is_refused_while_the_group_has_members(topic, group):
    """Deleting a live consumer's offsets moves its position under it.

    Kafka refuses for topics the group is subscribed to. This broker never parses
    subscription metadata — it forwards assignments and subscriptions untouched — so it
    cannot tell subscribed from unsubscribed and refuses while *any* member is present,
    conservative in the safe direction.
    """
    make(topic, partitions=2)
    assert kcat("-t", topic, "-P", stdin="a\nb\n").returncode == 0
    assert kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180).returncode == 0
    committed = sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id = '{group}'")
    assert committed != "0", "nothing was committed, so the test would prove nothing"

    start_consumer("live", group, topic)
    try:
        await_group(group, members=1, state="Stable")
        out = kafka_tool("kafka-consumer-groups.sh", "--delete-offsets",
                         "--group", group, "--topic", topic)
        combined = out.stdout + out.stderr
        assert "GroupSubscribedToTopicException" in combined, combined
        assert sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id = '{group}'") == committed, (
            "the refusal did not actually protect the live consumer's offsets"
        )
    finally:
        stop_consumer("live")

def test_delete_offsets_on_an_unknown_group_says_so(topic, group, conn):
    """A top-level error, not a per-partition one: a group that does not exist has no
    partitions to report against. Kafka answers the same way.

    Driven raw: given `--topic X` with no partition list the tool resolves the
    partitions via Metadata first, so an unknown topic fails there and OffsetDelete is
    never sent.
    """
    make(topic)
    error, _ = offset_delete(conn, f"{group}-nope", topic, [0])
    assert error == 69, f"expected GROUP_ID_NOT_FOUND (69), got {error}"

def test_delete_offsets_reaches_a_standalone_consumers_offsets(topic, group, conn):
    """A group that committed offsets but never joined must still be deletable.

    `assign()` + `commitSync()` — no subscription, no JoinGroup — is what Spark, Flink and
    every manual-offset integration does. Its offsets land in `kafgres_offsets` with no
    `kafgres_groups` row, so keying existence on that table reports GROUP_ID_NOT_FOUND for
    offsets `--describe` happily lists: permanently visible, permanently undeletable, and
    terminal in the Java client rather than retriable. Upstream cannot reach this because
    its coordinator materialises the group on the first commit.
    """
    make(topic)
    topic_id = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    sql(f"""INSERT INTO kafgres_offsets (group_id, topic_id, partition, committed_offset)
            VALUES ('{group}', {topic_id}, 0, 7)""")
    assert sql(f"SELECT count(*) FROM kafgres_groups WHERE group_id = '{group}'") == "0", (
        "the fixture has to leave no group row, or the test proves nothing"
    )

    error, per_partition = offset_delete(conn, group, topic, [0])
    assert error == 0, f"a standalone consumer's group was reported missing: {error}"
    assert per_partition == {0: 0}, per_partition
    assert sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id = '{group}'") == "0"

def test_delete_offsets_for_a_dropped_topic_reports_success(topic, group, conn):
    """Upstream has no topic catalog on this path and reports the operation done.

    Dropping a topic already cascades its offsets away, so the requested state is the
    actual state. Returning UNKNOWN_TOPIC_OR_PARTITION would be a *retriable* code for work
    that is complete, which any wrapper honouring retriable spins on until timeout.
    """
    make(topic)
    assert kcat("-t", topic, "-P", stdin="a\n").returncode == 0
    assert kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180).returncode == 0
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    assert sql(f"SELECT count(*) FROM kafgres_groups WHERE group_id = '{group}'") == "1"

    error, per_partition = offset_delete(conn, group, topic, [0])
    assert error == 0, error
    assert per_partition == {0: 0}, (
        f"a dropped topic reported {per_partition} rather than success"
    )
