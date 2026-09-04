"""The KIP-848 consumer group protocol: one RPC (`ConsumerGroupHeartbeat`) replaces
JoinGroup, SyncGroup and Heartbeat, and the *broker* computes the assignment rather
than an elected member. Driven by the real Java client throughout.
"""
import subprocess
import time

import pytest

from conftest import sql

KAFKA = "apache/kafka:4.3.1"
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def kafka(*args, timeout=180):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA, f"/opt/kafka/bin/{args[0]}",
         "--bootstrap-server", BROKER, *args[1:]],
        capture_output=True, text=True, timeout=timeout,
    )

def produce(topic, n):
    payload = "".join(f"m{i}\n" for i in range(n))
    subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-P"],
        input=payload, capture_output=True, text=True, timeout=120,
    )

def consume(topic, group, timeout_ms=25000, extra=(), timeout=120):
    """Only the records. The deprecation warning goes to *stdout*, not stderr, so an
    unfiltered line count reports it as a message."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", topic, "--group", group,
         "--consumer-property", "group.protocol=consumer",
         "--timeout-ms", str(timeout_ms), *extra],
        capture_output=True, text=True, timeout=timeout,
    )
    return [l for l in out.stdout.splitlines() if l.startswith("m") and l[1:].isdigit()]

@pytest.fixture
def topic(request):
    name = f"k848-{request.node.name.replace('_','-')[:30]}"
    grp = f"{name}-grp"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"DELETE FROM kafgres_consumer_groups WHERE group_id = '{grp}'")
    sql(f"DELETE FROM kafgres_offsets WHERE group_id = '{grp}'")
    sql(f"SELECT kafgres_create_topic('{name}', 4)")
    yield name, grp
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"DELETE FROM kafgres_consumer_groups WHERE group_id = '{grp}'")
    sql(f"DELETE FROM kafgres_offsets WHERE group_id = '{grp}'")

def test_a_consumer_on_the_new_protocol_reads_the_topic(topic):
    name, grp = topic
    produce(name, 40)
    got = consume(name, grp, extra=["--from-beginning", "--max-messages", "40"])
    assert len(got) == 40, f"read {len(got)} of 40"

def test_offsets_commit_and_are_honoured(topic):
    """A protocol that assigns partitions but loses positions is not usable."""
    name, grp = topic
    produce(name, 40)
    assert len(consume(name, grp, extra=["--from-beginning", "--max-messages", "40"])) == 40

    committed = sql(f"SELECT COALESCE(SUM(committed_offset),0) FROM kafgres_offsets "
                    f"WHERE group_id = '{grp}'")
    assert committed == "40", f"committed {committed} of 40"
    assert consume(name, grp, timeout_ms=12000) == [], "a committed group re-read records"

def test_two_consumers_never_hold_the_same_partition(topic):
    """Asserted against broker state rather than consumer output: output shows re-reads
    whenever a partition moves before its offset commit, which is not a protocol
    failure."""
    name, grp = topic
    produce(name, 400)
    procs = [
        subprocess.Popen(
            ["docker", "run", "--rm", "--network", "host", KAFKA,
             "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
             "--topic", name, "--from-beginning", "--group", grp,
             "--consumer-property", "group.protocol=consumer", "--timeout-ms", "20000"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(2)
    ]
    try:
        time.sleep(12)
        members = sql(f"SELECT count(*) FROM kafgres_consumer_group_members WHERE group_id='{grp}'")
        assert members == "2", f"expected 2 members, got {members}"
        for column in ("owned", "target"):
            clash = sql(
                f"SELECT count(*) FROM (SELECT unnest({column}) p, count(*) c "
                f"FROM kafgres_consumer_group_members WHERE group_id='{grp}' "
                f"GROUP BY 1 HAVING count(*) > 1) x")
            assert clash == "0", f"a partition is in two members' {column} sets"
        total = sql(f"SELECT COALESCE(SUM(cardinality(target)),0) "
                    f"FROM kafgres_consumer_group_members WHERE group_id='{grp}'")
        assert total == "4", f"the 4 partitions were not all assigned: {total}"
    finally:
        for p in procs:
            p.kill(); p.wait(timeout=30)

def test_the_group_is_visible_to_the_tools_an_operator_uses(topic):
    """An operator reads a group missing from `--list` as having disappeared."""
    name, grp = topic
    produce(name, 20)
    consume(name, grp, extra=["--from-beginning", "--max-messages", "20"])
    listed = kafka("kafka-consumer-groups.sh", "--list")
    assert grp in listed.stdout, f"group missing from --list: {listed.stdout}"
    described = kafka("kafka-consumer-groups.sh", "--describe", "--group", grp)
    assert name in described.stdout, described.stdout + described.stderr

def test_a_classic_group_id_is_refused_rather_than_forked(topic):
    """One group id cannot run both protocols: it would create two groups with one
    name, both committing into the same offsets table."""
    name, grp = topic
    produce(name, 10)
    classic = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", name, "--from-beginning", "--group", grp,
         "--consumer-property", "group.protocol=classic",
         "--timeout-ms", "12000", "--max-messages", "10"],
        capture_output=True, text=True, timeout=120)
    assert "m0" in classic.stdout, "the classic consumer did not run"

    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", name, "--from-beginning", "--group", grp,
         "--consumer-property", "group.protocol=consumer",
         "--timeout-ms", "12000"],
        capture_output=True, text=True, timeout=120)
    combined = out.stdout + out.stderr
    assert "GroupIdNotFound" in combined or "classic rebalance protocol" in combined, (
        f"the new protocol was allowed onto a classic group id: {combined[-400:]}"
    )
    assert "m0" not in out.stdout, (
        f"refused the group but consumed from it anyway: {out.stdout[-400:]}"
    )
