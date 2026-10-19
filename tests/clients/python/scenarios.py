#!/usr/bin/env python3
"""kafka-python scenario runner for the conformance suite.

Pure Python, its own version negotiation, and — unlike librdkafka and Sarama — it
probes the broker's API versions on connect and *downgrades* rather than pinning, which
makes it the client most likely to exercise an older version of an API. One
machine-readable line per scenario, so the same binary can run against kafgres and a
reference Kafka and the two outputs diffed.
"""

import sys
import threading
import time
import zlib

from kafka import ConsumerRebalanceListener, KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import ConfigResource, ConfigResourceType, NewPartitions, NewTopic
from kafka.codec import has_gzip, has_lz4, has_snappy, has_zstd
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

def produce_consume(broker, topic):
    producer = KafkaProducer(bootstrap_servers=broker, acks=1, api_version_auto_timeout_ms=30000)
    offsets = []
    for value in (b"p1", b"p2", b"p3"):
        meta = producer.send(topic, value).get(timeout=30)
        offsets.append(meta.offset)
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30000,
        group_id=None,
    )
    values = [m.value.decode() for m in consumer]
    consumer.close()
    assert len(values) == 3, f"expected 3 records, got {len(values)}: {values}"
    print(f"OK offsets={','.join(str(o) for o in offsets)} values={','.join(values)}")

def group_consume(broker, topic):
    producer = KafkaProducer(bootstrap_servers=broker, acks=1)
    for value in (b"gp1", b"gp2"):
        producer.send(topic, value).get(timeout=30)
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=f"{topic}-kpy-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=45000,
    )
    values = sorted(m.value.decode() for m in consumer)
    assert len(values) == 2, f"expected 2 records, got {len(values)}: {values}"
    consumer.commit()
    consumer.close()
    print(f"OK values={','.join(values)}")

def metadata(broker, topic):
    consumer = KafkaConsumer(bootstrap_servers=broker)
    partitions = consumer.partitions_for_topic(topic)
    consumer.close()
    admin = KafkaAdminClient(bootstrap_servers=broker)
    cluster = admin.describe_cluster()
    admin.close()
    assert partitions, f"no partitions for {topic}; the topic was not reported"
    print(
        f"OK partitions={len(partitions or [])} "
        f"brokers={len(cluster.get('brokers', []))} "
        f"controller={'yes' if cluster.get('controller_id', -1) >= 0 else 'no'}"
    )

def unknown_topic(broker, topic):
    missing = topic + "-missing"
    consumer = KafkaConsumer(bootstrap_servers=broker)
    partitions = consumer.partitions_for_topic(missing)
    consumer.close()

    admin = KafkaAdminClient(bootstrap_servers=broker)
    described = admin.describe_topics([missing])
    admin.close()
    code = described[0]["error_code"] if described else "omitted"
    print(f"OK partitions={partitions} error_code={code}")


def err_name(e):
    """kafka-python maps broker error codes to exception classes."""
    return type(e).__name__


def attempt(fn):
    try:
        fn()
        return "ok"
    except Exception as e:  # noqa: BLE001 - the error class is the observable
        return err_name(e)


def producer(broker, **extra):
    return KafkaProducer(bootstrap_servers=broker, api_version_auto_timeout_ms=30000, **extra)


def consumer(broker, **extra):
    conf = dict(bootstrap_servers=broker, auto_offset_reset="earliest", enable_auto_commit=False,
                group_id=None, api_version_auto_timeout_ms=30000)
    conf.update(extra)
    return KafkaConsumer(**conf)


def admin(broker):
    return KafkaAdminClient(bootstrap_servers=broker, api_version_auto_timeout_ms=30000)


def send(p, topic, value, partition=0, **kw):
    return p.send(topic, value, partition=partition, **kw).get(timeout=30)


def drain(c, want, ms=30000):
    out, deadline = [], time.time() + ms / 1000
    while len(out) < want and time.time() < deadline:
        for recs in c.poll(timeout_ms=500).values():
            out.extend(recs)
    return out


def vals(rs):
    return ",".join("~" if r.value is None else r.value.decode() for r in rs)


def crc(parts):
    c = 0
    for p in parts:
        c = zlib.crc32(p, c)
    return format(c, "x")


def tp(topic, p=0):
    return TopicPartition(topic, p)


def read_from(broker, topic, offset, want, partition=0, **extra):
    c = consumer(broker, **extra)
    c.assign([tp(topic, partition)])
    c.seek(tp(topic, partition), offset)
    got = drain(c, want)
    c.close()
    return got


def records_shape(broker, topic):
    t0 = 1_700_000_000_000
    p = producer(broker)
    rows = [
        dict(key=b"k1", value=b"v1", timestamp_ms=t0, headers=[("a", b"1"), ("b", b"2")]),
        dict(key=None, value=b"v2", timestamp_ms=t0 + 1000),
        dict(key=b"k3", value=None, timestamp_ms=t0 + 2000),
        dict(key=b"", value=b"", timestamp_ms=t0 + 3000),
        dict(key=b"k5", value=b"v5", timestamp_ms=t0 + 4000,
             headers=[("h", b"x"), ("h", b"y"), ("n", b"")]),  # kafka-python refuses a null header value
    ]
    for r in rows:
        p.send(topic, partition=0, **r).get(timeout=30)
    p.close()
    out = []
    for m in read_from(broker, topic, 0, 5):
        hs = "".join(f"{k}={'~' if v is None else v.decode()};" for k, v in (m.headers or []))
        k = "~" if m.key is None else m.key.decode()
        v = "~" if m.value is None else m.value.decode()
        out.append(f"{k}:{v}@{m.timestamp}[{hs}]")
    print("OK records=" + " ".join(out))


def compression(broker, topic):
    codecs = ["none"]
    for name, has in (("gzip", has_gzip), ("snappy", has_snappy), ("lz4", has_lz4), ("zstd", has_zstd)):
        if has():
            codecs.append(name)
    out = []
    for codec in codecs:
        p = producer(broker, compression_type=None if codec == "none" else codec, linger_ms=100)
        sent, futs = [], []
        for i in range(20):
            v = (f"payload-{codec}-{i}-" * 40).encode()
            sent.append(v)
            futs.append(p.send(topic, v, partition=0))
        p.flush()
        first = min(f.get(timeout=30).offset for f in futs)
        p.close()
        got = [m.value for m in read_from(broker, topic, first, 20)]
        out.append(f"{codec}={len(got)}:{crc(got)}{'' if crc(got) == crc(sent) else ':MISMATCH'}")
    print("OK " + " ".join(out))


def large_records(broker, topic):
    big = bytes((i * 31) & 0xFF for i in range(900_000))
    sent = [big]
    p = producer(broker, linger_ms=50, max_request_size=4_000_000)
    futs = [p.send(topic, big, partition=0)]
    for i in range(200):
        v = bytes([i]) * 10_000
        sent.append(v)
        futs.append(p.send(topic, v, partition=0))
    p.flush()
    for f in futs:
        f.get(timeout=60)
    p.close()
    got = [m.value for m in read_from(broker, topic, 0, len(sent), max_partition_fetch_bytes=2_000_000)]
    print(f"OK count={len(got)} bytes={sum(map(len, got))} crc={crc(got)} same={crc(got) == crc(sent)}")


def record_too_large(broker, topic):
    p = producer(broker, max_request_size=8_388_608)
    error = "none"
    try:
        send(p, topic, bytes(2 * 1024 * 1024))
    except Exception as e:  # noqa: BLE001
        error = err_name(e)
    nxt = send(p, topic, b"after").offset
    p.close()
    print(f"OK error={error} next_offset={nxt}")


def multi_partition(broker, topic):
    p = producer(broker)
    offs = []
    for part in range(3):
        o = [send(p, topic, f"p{part}-{i}".encode(), part).offset for i in range(5)]
        offs.append(f"p{part}={','.join(map(str, o))}")
    p.close()
    c = consumer(broker)
    tps = [tp(topic, i) for i in range(3)]
    c.assign(tps)
    got = {}
    for m in drain(c, 15):
        got.setdefault(m.partition, []).append(m.value.decode())
    begin = {k.partition: v for k, v in c.beginning_offsets(tps).items()}
    end = {k.partition: v for k, v in c.end_offsets(tps).items()}
    parts = len(c.partitions_for_topic(topic))
    c.close()
    print(f"OK {','.join(offs)} read={dict(sorted(got.items()))} begin={dict(sorted(begin.items()))} "
          f"end={dict(sorted(end.items()))} partitions={parts}")


def offsets_by_time(broker, topic):
    t0 = 1_700_000_000_000
    p = producer(broker)
    for i in range(4):
        send(p, topic, f"t{i}".encode(), timestamp_ms=t0 + i * 1000)
    p.close()
    c = consumer(broker)
    out = []
    for at in (t0 - 1, t0, t0 + 1500, t0 + 3000, t0 + 5000):
        r = c.offsets_for_times({tp(topic): at})[tp(topic)]
        out.append(f"{at - t0}->{'null' if r is None else r.offset}")
    lo = c.beginning_offsets([tp(topic)])[tp(topic)]
    hi = c.end_offsets([tp(topic)])[tp(topic)]
    c.close()
    print(f"OK by_time={' '.join(out)} begin={lo} end={hi}")


def seek(broker, topic):
    p = producer(broker)
    for i in range(10):
        send(p, topic, f"r{i}".encode())
    c = consumer(broker)
    c.assign([tp(topic)])
    c.seek(tp(topic), 5)
    tail = vals(drain(c, 5))
    c.seek_to_end(tp(topic))
    end = c.position(tp(topic))
    send(p, topic, b"r10")
    live = vals(drain(c, 1))
    c.seek_to_beginning(tp(topic))
    begin = c.position(tp(topic))
    c.seek(tp(topic), 3)
    third = drain(c, 1)[0]
    c.close()
    p.close()
    print(f"OK tail={tail} end={end} live={live} begin={begin} third={third.offset}:{third.value.decode()}")


def long_poll(broker, topic):
    c = consumer(broker, fetch_max_wait_ms=1500)
    c.assign([tp(topic)])
    empty = len(drain(c, 1, 3000))
    p = producer(broker)
    started = time.time()
    send(p, topic, b"late")
    late = drain(c, 1, 20000)
    c.close()
    p.close()
    print(f"OK empty={empty} late={vals(late)} prompt={time.time() - started < 15}")


def offset_out_of_range(broker, topic):
    p = producer(broker)
    for i in range(3):
        send(p, topic, f"o{i}".encode())
    p.close()
    out = []
    for policy in ("none", "earliest", "latest"):
        c = consumer(broker, auto_offset_reset=policy)
        c.assign([tp(topic)])
        c.seek(tp(topic), 99)
        try:
            got = []
            deadline = time.time() + 10
            while not got and time.time() < deadline:
                for recs in c.poll(timeout_ms=500).values():
                    got.extend(recs)
                if policy == "latest" and c.position(tp(topic)) == 3:
                    break
            res = f"read={vals(got)},pos={c.position(tp(topic))}"
        except Exception as e:  # noqa: BLE001
            res = err_name(e)
        c.close()
        out.append(f"{policy}:{res}")
    print("OK " + " ".join(out))


def acks(broker, topic):
    out = []
    for a in (0, 1, "all"):
        p = producer(broker, acks=a)
        m = send(p, topic, f"acks-{a}".encode())
        out.append(f"acks{a}" + ("" if a == 0 else f"@{m.offset}"))
        p.close()
    print(f"OK {' '.join(out)} read={vals(read_from(broker, topic, 0, 3))}")


class Member:
    """poll() blocks through a rebalance that waits for every member, so one thread each."""

    def __init__(self, broker, topic, group, **extra):
        self.topic = topic
        self.got = []
        self.lock = threading.Lock()
        self.assigned = set()
        self.c = consumer(broker, group_id=group, session_timeout_ms=30000, heartbeat_interval_ms=3000,
                          **extra)
        member = self

        class Listener(ConsumerRebalanceListener):
            def on_partitions_revoked(self, revoked):
                with member.lock:
                    member.assigned -= {t.partition for t in revoked}

            def on_partitions_assigned(self, assigned):
                with member.lock:
                    member.assigned |= {t.partition for t in assigned}

        self.c.subscribe([topic], listener=Listener())
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            try:
                for recs in self.c.poll(timeout_ms=200).values():
                    with self.lock:
                        self.got.extend(r.value.decode() for r in recs)
            except Exception:  # noqa: BLE001 - a closing consumer fails its poll
                if self.stop.is_set():
                    return

    def owned(self):
        with self.lock:
            return set(self.assigned)

    def received(self):
        with self.lock:
            return list(self.got)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=30)
        self.c.close()


def settle(partitions, *members):
    stable, deadline = 0, time.time() + 90
    while stable < 15 and time.time() < deadline:
        time.sleep(0.2)
        sets = [m.owned() for m in members]
        union = set().union(*sets)
        ok = len(union) == partitions and sum(map(len, sets)) == partitions and all(sets)
        stable = stable + 1 if ok else 0
    assert stable >= 15, "assignment never settled"


def sizes(*members):
    return ",".join(map(str, sorted(len(m.owned()) for m in members)))


def produce_spread(broker, topic, n):
    p = producer(broker)
    for i in range(n):
        send(p, topic, f"m{i}".encode(), i % 3)
    p.close()


def consume_all(n, *members):
    deadline = time.time() + 60
    while sum(len(m.received()) for m in members) < n and time.time() < deadline:
        time.sleep(0.2)
    time.sleep(1.5)
    allv = [v for m in members for v in m.received()]
    return f"consumed={len(allv)} unique={len(set(allv))}"


def group_two_members(broker, topic):
    g = f"{topic}-g"
    a, b = Member(broker, topic, g), Member(broker, topic, g)
    try:
        settle(3, a, b)
        before = sizes(a, b)
        produce_spread(broker, topic, 30)
        print(f"OK sizes={before} {consume_all(30, a, b)}")
    finally:
        a.close()
        b.close()


def group_rebalance_leave(broker, topic):
    g = f"{topic}-g"
    a, b = Member(broker, topic, g), Member(broker, topic, g)
    try:
        settle(3, a, b)
        before = sizes(a, b)
        b.close()
        deadline = time.time() + 60
        while len(a.owned()) < 3 and time.time() < deadline:
            time.sleep(0.2)
        after = len(a.owned())
        produce_spread(broker, topic, 9)
        print(f"OK before={before} after_leave={after} {consume_all(9, a)}")
    finally:
        a.close()


def group_resume(broker, topic):
    g = f"{topic}-g"
    p = producer(broker)
    for i in range(5):
        send(p, topic, f"first-{i}".encode())
    c = consumer(broker, group_id=g)
    c.subscribe([topic])
    first = drain(c, 5, 60000)
    c.commit({tp(topic): OffsetAndMetadata(5, "meta-x")})
    c.close()
    for i in range(3):
        send(p, topic, f"second-{i}".encode())
    p.close()
    c = consumer(broker, group_id=g)
    stored = c.committed(tp(topic), metadata=True)
    c.subscribe([topic])
    second = drain(c, 3, 60000)
    c.close()
    print(f"OK first={len(first)} committed={stored.offset}/{stored.metadata} second={vals(second)}")


def delete_result(res):
    return ",".join(getattr(e, "__name__", str(e)) for _, e in res)


def group_admin(broker, topic):
    g = f"{topic}-g"
    p = producer(broker)
    for i in range(6):
        send(p, topic, f"ga{i}".encode())
    p.close()
    m = Member(broker, topic, g, enable_auto_commit=True, auto_commit_interval_ms=500)
    a = admin(broker)
    try:
        deadline = time.time() + 60
        while len(m.received()) < 6 and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(3)  # let the auto-commit land
        out = []
        listed = any(x[0] == g for x in a.list_consumer_groups())
        d = a.describe_consumer_groups([g])[0]
        out.append(f"listed={listed} state={d.state} members={len(d.members)}")
        offs = a.list_consumer_group_offsets(g)
        out.append("offsets=" + ",".join(f"{k.partition}:{v.offset}" for k, v in offs.items()))
        out.append("delete_while_active=" + delete_result(a.delete_consumer_groups([g])))
        m.close()
        for _ in range(40):
            if not a.describe_consumer_groups([g])[0].members:
                break
            time.sleep(0.25)
        res = a.delete_consumer_groups([g])
        out.append("delete=" + delete_result(res))
        out.append(f"gone={not any(x[0] == g for x in a.list_consumer_groups())}")
        print("OK " + " ".join(out))
    finally:
        try:
            m.close()
        except Exception:  # noqa: BLE001
            pass
        a.close()


def admin_topics(broker, topic):
    t, a, out = topic + "-a", admin(broker), []

    def create(spec):
        try:
            res = a.create_topics([spec])
            errors = [x[1] for x in res.topic_errors] if hasattr(res, "topic_errors") else []
            return "ok" if not errors or errors[0] == 0 else str(errors[0])
        except Exception as e:  # noqa: BLE001
            return err_name(e)

    out.append("create=" + create(NewTopic(t, 3, 1, topic_configs={"retention.ms": "3600000"})))
    out.append("again=" + create(NewTopic(t, 3, 1)))
    out.append("zero_partitions=" + create(NewTopic(t + "-z", 0, 1)))
    out.append("rf2=" + create(NewTopic(t + "-r", 1, 2)))
    out.append("bad_config=" + create(NewTopic(t + "-c", 1, 1, topic_configs={"no.such.config": "1"})))
    d = a.describe_topics([t])[0]
    pt = d["partitions"][0]
    out.append(f"partitions={len(d['partitions'])} rf={len(pt['replicas'])} isr={len(pt['isr'])} "
               f"internal={d['is_internal']}")
    out.append(f"listed={t in a.list_topics()}")
    cfg = a.describe_configs([ConfigResource(ConfigResourceType.TOPIC, t)])
    entries = {e[0]: e[1] for e in cfg[0].resources[0][4]}
    out.append(f"retention.ms={entries.get('retention.ms')}")
    out.append("grow=" + attempt(lambda: a.create_partitions({t: NewPartitions(5)})))
    out.append(f"grown={len(a.describe_topics([t])[0]['partitions'])}")
    out.append("shrink=" + attempt(lambda: a.create_partitions({t: NewPartitions(2)})))
    out.append("delete=" + attempt(lambda: a.delete_topics([t])))
    out.append("delete_again=" + attempt(lambda: a.delete_topics([t])))
    a.close()
    print("OK " + " ".join(out))


def admin_configs(broker, topic):
    a, out = admin(broker), []
    res = ConfigResource(ConfigResourceType.TOPIC, topic)

    def entries():
        cfg = a.describe_configs([res])[0].resources[0][4]
        return {e[0]: e[1] for e in cfg}

    cur = entries()
    for k in ("retention.ms", "cleanup.policy", "max.message.bytes", "segment.bytes",
              "min.compaction.lag.ms", "compression.type", "message.timestamp.type"):
        out.append(f"{k}={cur.get(k, 'absent')}")
    a.alter_configs([ConfigResource(ConfigResourceType.TOPIC, topic, {"retention.ms": "12345"})])
    out.append(f"after_set={entries().get('retention.ms')}")
    a.alter_configs([ConfigResource(ConfigResourceType.TOPIC, topic, {})])
    out.append(f"after_reset={entries().get('retention.ms')}")
    out.append("unknown_key=" + attempt(lambda: a.alter_configs(
        [ConfigResource(ConfigResourceType.TOPIC, topic, {"no.such.config": "1"})])))
    out.append("bad_value=" + attempt(lambda: a.alter_configs(
        [ConfigResource(ConfigResourceType.TOPIC, topic, {"retention.ms": "not-a-number"})])))
    a.close()
    print("OK " + " ".join(out))


def admin_cluster(broker, topic):
    a = admin(broker)
    d = a.describe_cluster()
    ids = [b["node_id"] for b in d["brokers"]]
    a.close()
    print(f"OK nodes={len(ids)} controller_listed={d.get('controller_id') in ids} "
          f"cluster_id={bool(d.get('cluster_id'))}")


def admin_offsets(broker, topic):
    t0 = 1_700_000_000_000
    p = producer(broker)
    for i, ts in enumerate((t0 + 2000, t0, t0 + 5000, t0 + 1000)):
        send(p, topic, f"x{i}".encode(), timestamp_ms=ts)
    p.close()
    c = consumer(broker)
    out = [f"earliest={c.beginning_offsets([tp(topic)])[tp(topic)]}",
           f"latest={c.end_offsets([tp(topic)])[tp(topic)]}"]
    for at in (1500, 99999):
        r = c.offsets_for_times({tp(topic): t0 + at})[tp(topic)]
        out.append(f"at_t0+{at}=" + ("null" if r is None else f"{r.offset}@{r.timestamp - t0}"))
    c.close()
    print("OK " + " ".join(out))


SCENARIOS = {
    "produce-consume": produce_consume,
    "group-consume": group_consume,
    "metadata": metadata,
    "unknown-topic": unknown_topic,
    "records-shape": records_shape,
    "compression": compression,
    "large-records": large_records,
    "record-too-large": record_too_large,
    "multi-partition": multi_partition,
    "offsets-by-time": offsets_by_time,
    "seek": seek,
    "long-poll": long_poll,
    "offset-out-of-range": offset_out_of_range,
    "acks": acks,
    "group-two-members": group_two_members,
    "group-resume": group_resume,
    "group-rebalance-leave": group_rebalance_leave,
    "group-admin": group_admin,
    "admin-topics": admin_topics,
    "admin-configs": admin_configs,
    "admin-cluster": admin_cluster,
    "admin-offsets": admin_offsets,
}

def main():
    if len(sys.argv) < 4:
        print("usage: kafka-python-conformance <broker> <scenario> <topic>")
        return 2
    broker, scenario, topic = sys.argv[1], sys.argv[2], sys.argv[3]
    fn = SCENARIOS.get(scenario)
    if fn is None:
        print(f"ERROR unknown scenario {scenario}")
        return 2
    try:
        fn(broker, topic)
    except (KafkaError, AssertionError, OSError) as e:
        print(f"ERROR {type(e).__name__}: {e}")
        return 1
    except Exception as e:  # noqa: BLE001 - a scenario bug must still print a result line
        print(f"ERROR {type(e).__name__}: {e}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
