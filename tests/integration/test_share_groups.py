"""Share groups — KIP-932's queue semantics.

A consumer group gives each partition to one member and remembers one offset per partition.
A share group lets *every* member read the same partition and remembers the state of
individual records: acquired, accepted, released, rejected — a work queue. The wire
tests go through the real `KafkaShareConsumer`; the rest drive the delivery model from
SQL, where a share-group bug is not visible through a client.
"""

import os
import subprocess
import time

import pytest

from conftest import sql

KAFKA = "apache/kafka:4.1.0"
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

@pytest.fixture
def queue(request):
    name = f"sq-{request.node.name.replace('_', '-')[:34]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def set_lock_duration(ms):
    """`group.share.record.lock.duration.ms`, which is how long a dead consumer's work waits.

    Raised where every lock must hold for the whole run, lowered where one must lapse.
    """
    sql(f"ALTER SYSTEM SET kafgres.share_record_lock_duration_ms = {ms}")
    sql("SELECT pg_reload_conf()")
    time.sleep(1)

@pytest.fixture(autouse=True)
def default_lock():
    yield
    sql("ALTER SYSTEM RESET kafgres.share_record_lock_duration_ms")
    sql("SELECT pg_reload_conf()")

def produce(topic, n, prefix="job"):
    payload = "\n".join(f"{prefix}-{i}" for i in range(n)) + "\n"
    out = subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-P", "-t", topic],
        input=payload, capture_output=True, text=True, timeout=180,
    )
    assert out.returncode == 0, out.stderr

def share_consume(topic, group, idle_ms=8000, timeout=180):
    """One `kafka-console-share-consumer` run, returning the records it printed."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-share-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", topic, "--group", group, "--timeout-ms", str(idle_ms)],
        capture_output=True, text=True, timeout=timeout,
    )
    return [l for l in out.stdout.splitlines() if l.startswith("job-")]

def acquire(group, member, topic, limit=100):
    raw = sql(f"SELECT kafgres_share_acquire('{group}','{member}','{topic}',0,{limit})")
    inner = raw.strip("{}")
    return [int(x) for x in inner.split(",") if x]

def ack(group, member, topic, first, last, kind):
    sql(f"SELECT kafgres_share_ack('{group}','{member}','{topic}',0,{first},{last},'{kind}')")

def head(group, topic):
    v = sql(f"SELECT coalesce((SELECT start_offset FROM kafgres_share_offsets o "
            f"JOIN kafgres_topics t ON t.topic_id = o.topic_id "
            f"WHERE o.group_id = '{group}' AND t.name = '{topic}'), 0)")
    return int(v)

def state_of(group, topic, offset):
    return sql(f"SELECT coalesce((SELECT state || ':' || delivery_count "
               f"FROM kafgres_share_state('{group}','{topic}') "
               f"WHERE record_offset = {offset}), 'gone')")

def test_a_share_consumer_reads_the_queue(queue):
    """The headline: an unmodified `KafkaShareConsumer` drains a share group."""
    produce(queue, 20)
    got = share_consume(queue, "sg-basic")
    assert len(got) == 20, got[:5]
    assert set(got) == {f"job-{i}" for i in range(20)}

def test_two_consumers_split_one_partition_without_overlap(queue):
    """The property a consumer group cannot provide: two members read the *same*
    partition concurrently and each record goes to exactly one of them.

    The acquisition lock is raised for this test, not waited out: a lapsed lock is a
    legitimate redelivery — share groups are at-least-once — so "no overlap" is only a
    property while every lock holds."""
    set_lock_duration(600_000)
    produce(queue, 300)
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        a, b = [f.result() for f in
                [pool.submit(share_consume, queue, "sg-split"),
                 pool.submit(share_consume, queue, "sg-split")]]

    both = set(a) & set(b)
    assert both == set(), (
        f"{len(both)} records reached both consumers while every lock still held"
    )
    assert set(a) | set(b) == {f"job-{i}" for i in range(300)}, "records were lost"

def test_an_acquired_record_is_invisible_to_everyone_else(queue):
    """Two members, one partition, no overlap — at the level a client cannot show."""
    produce(queue, 10)
    sql(f"SELECT kafgres_share_join('m','worker-a','{queue}')")
    sql(f"SELECT kafgres_share_join('m','worker-b','{queue}')")
    a = acquire("m", "worker-a", queue, 4)
    b = acquire("m", "worker-b", queue, 4)
    assert a == [0, 1, 2, 3], a
    assert b == [4, 5, 6, 7], b
    assert set(a) & set(b) == set()

def test_the_head_advances_over_finished_work_and_stops_at_a_gap(queue):
    """`start_offset` moves over a *contiguous* prefix and no further: advancing past
    a hole would lose exactly the records inside it, with nothing to say so."""
    produce(queue, 10)
    sql(f"SELECT kafgres_share_join('m2','w','{queue}')")
    acquire("m2", "w", queue, 6)
    ack("m2", "w", queue, 0, 3, "accept")
    assert head("m2", queue) == 4, "the head did not advance over finished work"

    ack("m2", "w", queue, 5, 5, "accept")
    assert head("m2", queue) == 4, "the head advanced past an unfinished record"

    ack("m2", "w", queue, 4, 4, "accept")
    assert head("m2", queue) == 6, "the head did not resume once the gap closed"

def test_a_released_record_goes_back_to_the_pool_carrying_its_count(queue):
    """Release is a consumer saying "not me" — the record is offered again. The
    delivery count survives, because forgetting it is how a poison record becomes
    immortal."""
    produce(queue, 5)
    sql(f"SELECT kafgres_share_join('m3','w1','{queue}')")
    sql(f"SELECT kafgres_share_join('m3','w2','{queue}')")
    assert acquire("m3", "w1", queue, 1) == [0]
    ack("m3", "w1", queue, 0, 0, "release")
    assert acquire("m3", "w2", queue, 1) == [0], "a released record was not offered again"
    assert state_of("m3", queue, 0) == "acquired:2", state_of("m3", queue, 0)

def test_work_held_by_a_dead_consumer_is_redelivered(queue):
    """The property a queue lives or dies by: a consumer that stops answering while
    holding records must not take them with it. The acquisition lock lapsing is what
    turns a crash into a redelivery instead of lost work."""
    produce(queue, 5)
    sql(f"SELECT kafgres_share_join('m4','doomed','{queue}')")
    sql(f"SELECT kafgres_share_join('m4','survivor','{queue}')")
    assert acquire("m4", "doomed", queue, 2) == [0, 1]
    assert acquire("m4", "survivor", queue, 2) == [2, 3]

    sql("UPDATE kafgres_share_inflight SET acquired_until = now() - interval '1 second' "
        "WHERE member_id = 'doomed'")
    assert int(sql("SELECT kafgres_share_expire()")) >= 1
    assert acquire("m4", "survivor", queue, 2) == [0, 1], "the dead consumer's work was lost"
    assert state_of("m4", queue, 0) == "acquired:2", state_of("m4", queue, 0)

def test_a_poison_record_is_archived_and_stops_holding_the_queue(queue):
    """A record that fails every consumer must not cycle forever, *and* must not pin
    the head.

    Archiving happens on acquire — nobody ever acknowledged it — so the head has to
    advance there too, or the start offset stays on the poison record permanently and
    a restart replays from a record nobody can finish."""
    produce(queue, 5)
    sql(f"SELECT kafgres_share_join('m5','w','{queue}')")
    for _ in range(6):
        acquire("m5", "w", queue, 1)
        sql("UPDATE kafgres_share_inflight SET acquired_until = now() - interval '1s' "
            f"WHERE record_offset = 0")
        sql("SELECT kafgres_share_expire()")

    assert acquire("m5", "w", queue, 1) != [0], "a record past its delivery limit was re-offered"
    assert head("m5", queue) >= 1, "the poison record still pins the head"

def test_a_share_group_cannot_take_a_consumer_group_name(queue):
    """One name, one kind of group: a share group tracks records, a consumer group
    tracks an offset, so a name meaning both would show an operator two unrelated
    states through the same tooling."""
    produce(queue, 3)
    subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", queue, "--group", "clash", "--from-beginning",
         "--timeout-ms", "8000", "--max-messages", "3"],
        capture_output=True, text=True, timeout=120,
    )
    assert sql("SELECT count(*) FROM kafgres_groups WHERE group_id = 'clash'") == "1"
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-console-share-consumer.sh", "--bootstrap-server", BROKER,
         "--topic", queue, "--group", "clash", "--timeout-ms", "8000"],
        capture_output=True, text=True, timeout=120,
    )
    assert "job-" not in out.stdout, "a share group was allowed onto a consumer group id"
