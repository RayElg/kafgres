#!/usr/bin/env python3
"""librdkafka scenario runner (confluent-kafka) for the conformance suite.

It negotiates from ApiVersions and is strict about malformed optional fields.
"""

import sys
import time
import zlib

from confluent_kafka import (
    OFFSET_BEGINNING, OFFSET_END, Consumer, ConsumerGroupTopicPartitions, KafkaError,
    KafkaException, Producer, TopicCollection, TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewPartitions, NewTopic,
    OffsetSpec,
)

BROKER = None
TOPIC = None


def err_name(e):
    """The error name; message text differs between brokers."""
    if isinstance(e, KafkaException):
        return e.args[0].name()
    if isinstance(e, KafkaError):
        return e.name()
    return type(e).__name__


def base(**extra):
    # Auto-creation is off on the reference broker.
    conf = {"bootstrap.servers": BROKER, "client.id": "librdkafka-conformance",
            "allow.auto.create.topics": False}
    conf.update(extra)
    return conf


def producer(**extra):
    return Producer(base(**{"socket.timeout.ms": 30000, **extra}))


def consumer(**extra):
    conf = {"auto.offset.reset": "earliest", "enable.auto.commit": False,
            "group.id": f"{TOPIC}-anon"}
    conf.update(extra)
    return Consumer(base(**conf))


def admin():
    return AdminClient(base())


def produce_sync(p, topic, value, partition=0, **kw):
    box = []
    p.produce(topic, value, partition=partition, on_delivery=lambda e, m: box.append((e, m)), **kw)
    p.flush(30)
    assert box, "no delivery report"
    return box[0]


def produce_ok(p, topic, value, partition=0, **kw):
    e, m = produce_sync(p, topic, value, partition, **kw)
    if e is not None:
        raise KafkaException(e)
    return m


def drain(c, want, ms=30000):
    out = []
    deadline = time.time() + ms / 1000
    while len(out) < want and time.time() < deadline:
        m = c.poll(0.5)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(m.error())
        out.append(m)
    return out


def assign_all(c, partitions=(0,), offset=OFFSET_BEGINNING, topic=None):
    c.assign([TopicPartition(topic or TOPIC, p, offset) for p in partitions])


def vals(ms):
    return ",".join(m.value().decode() if m.value() is not None else "~" for m in ms)


def crc(parts):
    c = 0
    for p in parts:
        c = zlib.crc32(p, c)
    return format(c, "x")


def done(fut_map):
    out = {}
    for k, f in fut_map.items():
        try:
            out[k] = f.result(timeout=60)
        except Exception as e:  # noqa: BLE001 - the error name is the observable
            out[k] = err_name(e)
    return out


def attempt(fn):
    try:
        fn()
        return "ok"
    except Exception as e:  # noqa: BLE001
        return err_name(e)


# ---- data plane -----------------------------------------------------------------

def produce_consume():
    p = producer()
    offsets = [produce_ok(p, TOPIC, v).offset() for v in (b"l1", b"l2", b"l3")]
    c = consumer()
    assign_all(c)
    got = drain(c, 3)
    c.close()
    return f"offsets={','.join(map(str, offsets))} values={vals(got)}"


def records_shape():
    t0 = 1_700_000_000_000
    p = producer()
    rows = [
        dict(key=b"k1", value=b"v1", timestamp=t0, headers=[("a", b"1"), ("b", b"2")]),
        dict(key=None, value=b"v2", timestamp=t0 + 1000),
        dict(key=b"k3", value=None, timestamp=t0 + 2000),
        dict(key=b"", value=b"", timestamp=t0 + 3000),
        dict(key=b"k5", value=b"v5", timestamp=t0 + 4000,
             headers=[("h", b"x"), ("h", b"y"), ("n", None)]),
    ]
    for r in rows:
        produce_ok(p, TOPIC, r["value"], key=r["key"], timestamp=r["timestamp"],
                   headers=r.get("headers"))
    c = consumer()
    assign_all(c)
    out = []
    for m in drain(c, 5):
        hs = "".join(f"{k}={'~' if v is None else v.decode()};" for k, v in (m.headers() or []))
        k = "~" if m.key() is None else m.key().decode()
        v = "~" if m.value() is None else m.value().decode()
        out.append(f"{k}:{v}@{m.timestamp()[1]}/{m.timestamp()[0]}[{hs}]")
    c.close()
    return "records=" + " ".join(out)


def compression():
    out = []
    for codec in ("none", "gzip", "snappy", "lz4", "zstd"):
        p = producer(**{"compression.codec": codec, "linger.ms": 100})
        sent, first = [], None
        reports = []
        for i in range(20):
            v = (f"payload-{codec}-{i}-" * 40).encode()
            sent.append(v)
            p.produce(TOPIC, v, partition=0, on_delivery=lambda e, m: reports.append((e, m)))
        p.flush(30)
        assert len(reports) == 20 and all(e is None for e, _ in reports), reports[:1]
        first = min(m.offset() for _, m in reports)
        c = consumer()
        c.assign([TopicPartition(TOPIC, 0, first)])
        got = [m.value() for m in drain(c, 20)]
        c.close()
        out.append(f"{codec}={len(got)}:{crc(got)}{'' if crc(got) == crc(sent) else ':MISMATCH'}")
    return " ".join(out)


def large_records():
    big = bytes((i * 31) & 0xFF for i in range(900_000))
    sent = [big]
    p = producer(**{"linger.ms": 50, "message.max.bytes": 1_000_000})
    reports = []
    p.produce(TOPIC, big, partition=0, on_delivery=lambda e, m: reports.append(e))
    for i in range(200):
        v = bytes([i]) * 10_000
        sent.append(v)
        p.produce(TOPIC, v, partition=0, on_delivery=lambda e, m: reports.append(e))
    p.flush(60)
    assert len(reports) == 201 and not any(reports), reports[:1]
    c = consumer()
    assign_all(c)
    got = [m.value() for m in drain(c, len(sent), 60000)]
    c.close()
    return (f"count={len(got)} bytes={sum(map(len, got))} crc={crc(got)} "
            f"same={crc(got) == crc(sent)}")


def record_too_large():
    p = producer(**{"message.max.bytes": 8_388_608})
    e, _ = produce_sync(p, TOPIC, bytes(2 * 1024 * 1024))
    error = e.name() if e else "none"
    nxt = produce_ok(p, TOPIC, b"after").offset()
    return f"error={error} next_offset={nxt}"


def multi_partition():
    p = producer()
    offs = []
    for part in range(3):
        o = [produce_ok(p, TOPIC, f"p{part}-{i}".encode(), partition=part).offset() for i in range(5)]
        offs.append(f"p{part}={','.join(map(str, o))}")
    c = consumer()
    assign_all(c, (0, 1, 2))
    got = {}
    for m in drain(c, 15):
        got.setdefault(m.partition(), []).append(m.value().decode())
    marks = {i: c.get_watermark_offsets(TopicPartition(TOPIC, i), timeout=30) for i in range(3)}
    md = c.list_topics(TOPIC, timeout=30)
    c.close()
    return (",".join(offs) + f" read={dict(sorted(got.items()))} "
            f"begin={ {i: m[0] for i, m in marks.items()} } end={ {i: m[1] for i, m in marks.items()} } "
            f"partitions={len(md.topics[TOPIC].partitions)}")


def offsets_by_time():
    t0 = 1_700_000_000_000
    p = producer()
    for i in range(4):
        produce_ok(p, TOPIC, f"t{i}".encode(), timestamp=t0 + i * 1000)
    c = consumer()
    out = []
    for at in (t0 - 1, t0, t0 + 1500, t0 + 3000, t0 + 5000):
        r = c.offsets_for_times([TopicPartition(TOPIC, 0, at)], timeout=30)[0]
        # librdkafka answers "no such offset" with the high watermark sentinel -1.
        out.append(f"{at - t0}->{r.offset}")
    lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
    c.close()
    return f"by_time={' '.join(out)} begin={lo} end={hi}"


def seek():
    p = producer()
    for i in range(10):
        produce_ok(p, TOPIC, f"r{i}".encode())
    c = consumer()
    assign_all(c)
    c.seek(TopicPartition(TOPIC, 0, 5))
    tail = vals(drain(c, 5))
    c.seek(TopicPartition(TOPIC, 0, OFFSET_END))
    # END resolves asynchronously; a record produced before then lands behind the position.
    deadline = time.time() + 10
    while c.position([TopicPartition(TOPIC, 0)])[0].offset < 0 and time.time() < deadline:
        c.poll(0.2)
    produce_ok(p, TOPIC, b"r10")
    live = vals(drain(c, 1))
    c.seek(TopicPartition(TOPIC, 0, 3))
    third = drain(c, 1)[0]
    c.close()
    return f"tail={tail} live={live} third={third.offset()}:{third.value().decode()}"


def long_poll():
    c = consumer(**{"fetch.wait.max.ms": 1500})
    assign_all(c)
    empty = len(drain(c, 1, 3000))
    p = producer()
    started = time.time()
    p.produce(TOPIC, b"late", partition=0)
    p.flush(30)
    got = drain(c, 1, 20000)
    c.close()
    return f"empty={empty} late={vals(got)} prompt={time.time() - started < 15}"


def offset_out_of_range():
    p = producer()
    for i in range(3):
        produce_ok(p, TOPIC, f"o{i}".encode())
    out = []
    for policy in ("error", "earliest", "latest"):
        c = consumer(**{"auto.offset.reset": policy})
        c.assign([TopicPartition(TOPIC, 0, 99)])
        res = []
        deadline = time.time() + 10
        while time.time() < deadline:
            m = c.poll(0.5)
            if m is None:
                if policy == "latest":
                    break
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    continue
                res = [m.error().name()]
                break
            res.append(m.value().decode())
            if len(res) == 3:
                break
        c.close()
        out.append(f"{policy}:{'+'.join(res) or 'nothing'}")
    return " ".join(out)


def idempotent_produce():
    p = producer(**{"enable.idempotence": True, "acks": "all", "linger.ms": 5})
    reports = []
    for i in range(50):
        p.produce(TOPIC, f"i{i}".encode(), partition=0, on_delivery=lambda e, m: reports.append((e, m)))
    p.flush(60)
    assert len(reports) == 50 and all(e is None for e, _ in reports), reports[:1]
    offs = [m.offset() for _, m in reports]
    c = consumer()
    assign_all(c)
    n = len(drain(c, 50))
    c.close()
    return f"contiguous={offs == list(range(50))} first={offs[0]} last={offs[-1]} read={n}"


def acks():
    out = []
    for a in ("0", "1", "all"):
        p = producer(**{"acks": a, "enable.idempotence": False})
        m = produce_ok(p, TOPIC, f"acks-{a}".encode())
        out.append(f"acks{a}" + ("" if a == "0" else f"@{m.offset()}"))
    c = consumer()
    assign_all(c)
    return " ".join(out) + f" read={vals(drain(c, 3))}"


# ---- transactions ---------------------------------------------------------------

def txn_producer(tid):
    p = producer(**{"transactional.id": tid, "transaction.timeout.ms": 60000})
    p.init_transactions(30)
    return p


def state(isolation):
    c = consumer(**{"isolation.level": isolation})
    assign_all(c)
    lo, end = c.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
    got, pos = [], 0
    deadline = time.time() + 30
    while pos < end and time.time() < deadline:
        m = c.poll(0.5)
        if m is None:
            # A trailing control record yields no message; the position still advances.
            pos = c.position([TopicPartition(TOPIC, 0)])[0].offset
            continue
        if m.error():
            continue
        got.append(m.value().decode())
        pos = m.offset() + 1
    pos = max(pos, c.position([TopicPartition(TOPIC, 0)])[0].offset)
    c.close()
    return f"values={','.join(got)}"


def txn_read_committed():
    t = txn_producer(f"rd-rc-{TOPIC}")
    plain = producer()
    t.begin_transaction()
    for v in (b"a0", b"a1"):
        t.produce(TOPIC, v, partition=0)
    t.commit_transaction(30)
    t.begin_transaction()
    for v in (b"b0", b"b1"):
        t.produce(TOPIC, v, partition=0)
    # abort_transaction() purges unsent records, which would leave nothing aborted to skip.
    t.flush(30)
    t.abort_transaction(30)
    produce_ok(plain, TOPIC, b"n0")
    t.begin_transaction()
    t.produce(TOPIC, b"c0", partition=0)
    t.commit_transaction(30)
    t.begin_transaction()
    t.produce(TOPIC, b"o0", partition=0)
    t.flush(30)
    settled = state("read_committed")
    uncommitted = state("read_uncommitted")
    t.commit_transaction(30)
    return f"settled=[{settled}] open_uncommitted=[{uncommitted}] after_commit=[{state('read_committed')}]"


def committed_offset(c):
    """Polls: Kafka writes markers asynchronously, so offsets land after commit returns."""
    offset = -1001
    for _ in range(40):
        offset = c.committed([TopicPartition(TOPIC, 0)], timeout=30)[0].offset
        if offset >= 0:
            break
        time.sleep(0.25)
    return offset


def txn_consume_transform_produce():
    out_topic, group = f"{TOPIC}-out", f"{TOPIC}-g"
    a = admin()  # held: an AdminClient that is collected fails its requests with _DESTROY
    created = done(a.create_topics([NewTopic(out_topic, 1, 1)]))
    assert created[out_topic] is None, created
    plain = producer()
    for i in range(5):
        produce_ok(plain, TOPIC, f"in{i}".encode())
    c = consumer(**{"isolation.level": "read_committed", "group.id": group})
    assign_all(c)
    t = txn_producer(f"rd-cpt-{TOPIC}")
    batch = drain(c, 5)
    t.begin_transaction()
    for m in batch:
        t.produce(out_topic, m.value().upper(), partition=0)
    t.send_offsets_to_transaction([TopicPartition(TOPIC, 0, 5)], c.consumer_group_metadata(), 30)
    t.commit_transaction(30)
    after_commit = committed_offset(c)
    produce_ok(plain, TOPIC, b"in5")
    more = drain(c, 1)
    t.begin_transaction()
    t.produce(out_topic, b"SHOULD-NOT-APPEAR", partition=0)
    t.flush(30)
    t.send_offsets_to_transaction([TopicPartition(TOPIC, 0, 6)], c.consumer_group_metadata(), 30)
    t.abort_transaction(30)
    after_abort = committed_offset(c)
    c.close()
    r = consumer(**{"isolation.level": "read_committed"})
    r.assign([TopicPartition(out_topic, 0, OFFSET_BEGINNING)])
    lo, hi = r.get_watermark_offsets(TopicPartition(out_topic, 0), timeout=30)
    seen = []
    deadline = time.time() + 20
    while time.time() < deadline:
        m = r.poll(0.5)
        if m is not None and not m.error():
            seen.append(m)
        elif m is None and len(seen) >= 5:
            break
    r.close()
    return (f"processed={len(batch)} committed_after_commit={after_commit} "
            f"second_batch={len(more)} committed_after_abort={after_abort} output={vals(seen)}")


def txn_fenced():
    tid = f"rd-fence-{TOPIC}"
    first = txn_producer(tid)
    first.begin_transaction()
    first.produce(TOPIC, b"stale", partition=0)
    first.flush(30)
    second = txn_producer(tid)
    second.begin_transaction()
    second.produce(TOPIC, b"fresh", partition=0)
    second.flush(30)
    failed = "none"
    try:
        first.produce(TOPIC, b"stale-2", partition=0)
        first.commit_transaction(30)
    except KafkaException as e:
        failed = e.args[0].name()
    second.commit_transaction(30)
    return f"first_failed_with={failed} read_committed=[{state('read_committed')}]"


# ---- groups ---------------------------------------------------------------------

class Member:
    def __init__(self, group, **extra):
        self.revoked = []
        self.got = []
        self.errs = set()
        # The consumer protocol takes its timeouts from the broker and refuses this setting.
        timeouts = {} if extra.get("group.protocol") == "consumer" else {"session.timeout.ms": 30000}
        self.c = consumer(**{"group.id": group, **timeouts, **extra})
        self.c.subscribe([TOPIC], on_revoke=lambda c, ps: self.revoked.append(len(ps)))

    def owned(self):
        return {tp.partition for tp in self.c.assignment()}

    def poll(self, t=0.2):
        m = self.c.poll(t)
        if m is not None and not m.error():
            self.got.append(m.value().decode())
        elif m is not None and m.error().code() != KafkaError._PARTITION_EOF:
            self.errs.add(m.error().name())
        return m

    def error_names(self):
        return "+".join(sorted(self.errs)) or "none"


def settle(partitions, *members):
    stable, deadline = 0, time.time() + 90
    while stable < 15 and time.time() < deadline:
        for m in members:
            m.poll()
        sets = [m.owned() for m in members]
        union = set().union(*sets)
        ok = (len(union) == partitions and sum(map(len, sets)) == partitions
              and all(sets))
        stable = stable + 1 if ok else 0
    assert stable >= 15, "assignment never settled"


def sizes(*members):
    return ",".join(map(str, sorted(len(m.owned()) for m in members)))


def produce_spread(n):
    p = producer()
    for i in range(n):
        produce_ok(p, TOPIC, f"m{i}".encode(), partition=i % 3)


def consume_all(n, *members):
    deadline = time.time() + 60
    while sum(len(m.got) for m in members) < n and time.time() < deadline:
        for m in members:
            m.poll()
    for _ in range(5):
        for m in members:
            m.poll()
    allv = [v for m in members for v in m.got]
    return f"consumed={len(allv)} unique={len(set(allv))}"


def group_two_members():
    g = f"{TOPIC}-g"
    a, b = Member(g), Member(g)
    try:
        settle(3, a, b)
        before = sizes(a, b)
        produce_spread(30)
        return f"sizes={before} {consume_all(30, a, b)}"
    finally:
        a.c.close()
        b.c.close()


def group_rebalance_leave():
    g = f"{TOPIC}-g"
    a, b = Member(g), Member(g)
    members = [a, b]
    try:
        settle(3, a, b)
        before = sizes(a, b)
        b.c.close()
        members.remove(b)
        deadline = time.time() + 60
        while len(a.owned()) < 3 and time.time() < deadline:
            a.poll()
        after = len(a.owned())
        produce_spread(9)
        return f"before={before} after_leave={after} {consume_all(9, a)}"
    finally:
        for m in members:
            m.c.close()


def group_resume():
    g = f"{TOPIC}-g"
    p = producer()
    for i in range(5):
        produce_ok(p, TOPIC, f"first-{i}".encode())
    c = consumer(**{"group.id": g})
    c.subscribe([TOPIC])
    first = drain(c, 5, 60000)
    c.commit(offsets=[TopicPartition(TOPIC, 0, 5)], asynchronous=False)
    c.close()
    for i in range(3):
        produce_ok(p, TOPIC, f"second-{i}".encode())
    c = consumer(**{"group.id": g})
    stored = c.committed([TopicPartition(TOPIC, 0)], timeout=30)[0].offset
    c.subscribe([TOPIC])
    second = drain(c, 3, 60000)
    c.close()
    return f"first={len(first)} committed={stored} second={vals(second)}"


def group_static():
    g = f"{TOPIC}-g"
    a = Member(g, **{"group.instance.id": "inst-a"})
    b = Member(g, **{"group.instance.id": "inst-b"})
    b2 = usurper = None
    try:
        settle(3, a, b)
        b_had, a_had = b.owned(), a.owned()
        b.c.close()
        for _ in range(10):
            a.poll()
        a_stable = a.owned() == a_had
        b2 = Member(g, **{"group.instance.id": "inst-b"})
        deadline = time.time() + 60
        while not b2.owned() and time.time() < deadline:
            a.poll()
            b2.poll()
        same = b2.owned() == b_had
        a_after = a.owned() == a_had
        # Two live consumers cannot share an instance id: the one that was there first is fenced.
        usurper = Member(g, **{"group.instance.id": "inst-b"})
        fenced, until = "none", time.time() + 30
        while fenced == "none" and time.time() < until:
            usurper.poll()
            b2.poll()
            a.poll()
            fenced = b2.error_names()
        return (f"a_stable_while_b_away={a_stable} b_same_assignment={same} "
                f"a_stable_after={a_after} duplicate_instance={fenced}")
    finally:
        for m in (a, b2, usurper):
            if m is not None:
                try:
                    m.c.close()
                except Exception:  # noqa: BLE001 - a fenced consumer may fail to close cleanly
                    pass


def group_cooperative():
    g = f"{TOPIC}-g"
    a = Member(g, **{"partition.assignment.strategy": "cooperative-sticky"})
    deadline = time.time() + 60
    while len(a.owned()) < 3 and time.time() < deadline:
        a.poll()
    b = Member(g, **{"partition.assignment.strategy": "cooperative-sticky"})
    try:
        settle(3, a, b)
        sz = sizes(a, b)
        everything = max(a.revoked or [0]) == 3
        produce_spread(12)
        return f"sizes={sz} revoked_everything={everything} {consume_all(12, a, b)}"
    finally:
        b.c.close()
        a.c.close()


def group_kip848():
    g = f"{TOPIC}-g"
    a = Member(g, **{"group.protocol": "consumer"})
    b = Member(g, **{"group.protocol": "consumer"})
    try:
        settle(3, a, b)
        sz = sizes(a, b)
        produce_spread(12)
        consumed = consume_all(12, a, b)
        adm = admin()
        d = adm.describe_consumer_groups([g])[g].result(timeout=30)
        return f"sizes={sz} {consumed} type={d.type} state={d.state} members={len(d.members)}"
    finally:
        a.c.close()
        b.c.close()


def group_admin():
    g = f"{TOPIC}-g"
    p = producer()
    for i in range(6):
        produce_ok(p, TOPIC, f"ga{i}".encode())
    m = Member(g)
    a = admin()
    try:
        deadline = time.time() + 60
        while len(m.got) < 6 and time.time() < deadline:
            m.poll()
        m.c.commit(asynchronous=False)
        out = []
        listed = any(x.group_id == g for x in a.list_consumer_groups().result(timeout=30).valid)
        d = a.describe_consumer_groups([g])[g].result(timeout=30)
        assigned = len(d.members[0].assignment.topic_partitions) if d.members else 0
        out.append(f"listed={listed} state={d.state} members={len(d.members)} assigned={assigned}")
        offs = a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g)])[g].result(timeout=30)
        out.append("offsets=" + ",".join(f"{x.partition}:{x.offset}" for x in offs.topic_partitions))
        busy = attempt(lambda: a.alter_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(g, [TopicPartition(TOPIC, 0, 2)])])[g].result(timeout=30))
        out.append(f"alter_while_active={busy}")
        busy = attempt(lambda: a.delete_consumer_groups([g])[g].result(timeout=30))
        out.append(f"delete_while_active={busy}")
        m.c.close()
        for _ in range(40):
            if not a.describe_consumer_groups([g])[g].result(timeout=30).members:
                break
            time.sleep(0.25)
        a.alter_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(g, [TopicPartition(TOPIC, 0, 2)])])[g].result(timeout=30)
        offs = a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g)])[g].result(timeout=30)
        out.append(f"after_alter={offs.topic_partitions[0].offset}")
        a.delete_consumer_groups([g])[g].result(timeout=30)
        gone = not any(x.group_id == g for x in a.list_consumer_groups().result(timeout=30).valid)
        out.append(f"gone={gone}")
        return " ".join(out)
    finally:
        try:
            m.c.close()
        except Exception:  # noqa: BLE001
            pass


# ---- admin ----------------------------------------------------------------------

def admin_topics():
    t, a, out = f"{TOPIC}-a", admin(), []
    done(a.create_topics([NewTopic(t, 3, 1, config={"retention.ms": "3600000"})]))
    r = lambda spec: list(done(a.create_topics([spec])).values())[0]  # noqa: E731
    ok = lambda v: "ok" if v is None else v  # noqa: E731
    out.append(f"again={ok(r(NewTopic(t, 3, 1)))}")
    out.append(f"zero_partitions={ok(r(NewTopic(t + '-z', 0, 1)))}")
    out.append(f"rf2={ok(r(NewTopic(t + '-r', 1, 2)))}")
    out.append(f"bad_config={ok(r(NewTopic(t + '-c', 1, 1, config={'no.such.config': '1'})))}")
    out.append(f"bad_name={ok(r(NewTopic('bad name!', 1, 1)))}")
    d = a.describe_topics(TopicCollection([t]))[t].result(timeout=30)
    pt = d.partitions[0]
    out.append(f"partitions={len(d.partitions)} rf={len(pt.replicas)} isr={len(pt.isr)} "
               f"has_id={d.topic_id is not None}")
    out.append(f"listed={t in a.list_topics(timeout=30).topics}")
    cfg = list(a.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, t)]).values())[0].result(30)
    e = cfg["retention.ms"]
    out.append(f"retention.ms={e.value}/{e.source}")
    done(a.create_partitions([NewPartitions(t, 5)]))
    out.append(f"grown={len(a.describe_topics(TopicCollection([t]))[t].result(30).partitions)}")
    out.append(f"shrink={ok(list(done(a.create_partitions([NewPartitions(t, 2)])).values())[0])}")
    out.append(f"grow_missing={ok(list(done(a.create_partitions([NewPartitions(t + '-nope', 2)])).values())[0])}")
    done(a.delete_topics([t]))
    out.append(f"delete_again={ok(list(done(a.delete_topics([t])).values())[0])}")
    return " ".join(out)


def admin_configs():
    a, out = admin(), []
    res = ConfigResource(ConfigResource.Type.TOPIC, TOPIC)
    cfg = list(a.describe_configs([res]).values())[0].result(30)
    for k in ("retention.ms", "cleanup.policy", "max.message.bytes", "segment.bytes",
              "min.compaction.lag.ms", "compression.type", "message.timestamp.type"):
        e = cfg.get(k)
        out.append(f"{k}=" + ("absent" if e is None else f"{e.value}/{e.source}"))

    def alter(entries):
        r = ConfigResource(ConfigResource.Type.TOPIC, TOPIC, incremental_configs=entries)
        return list(done(a.incremental_alter_configs([r])).values())[0]

    alter([ConfigEntry("retention.ms", "12345", incremental_operation=AlterConfigOpType.SET)])
    e = list(a.describe_configs([res]).values())[0].result(30)["retention.ms"]
    out.append(f"after_set={e.value}/{e.source}")
    alter([ConfigEntry("retention.ms", None, incremental_operation=AlterConfigOpType.DELETE)])
    e = list(a.describe_configs([res]).values())[0].result(30)["retention.ms"]
    out.append(f"after_delete={e.value}/{e.source}")
    bad = alter([ConfigEntry("no.such.config", "1", incremental_operation=AlterConfigOpType.SET)])
    out.append(f"unknown_key={'ok' if bad is None else bad}")
    bad = alter([ConfigEntry("retention.ms", "not-a-number", incremental_operation=AlterConfigOpType.SET)])
    out.append(f"bad_value={'ok' if bad is None else bad}")
    missing = ConfigResource(ConfigResource.Type.TOPIC, TOPIC + "-missing")
    out.append("describe_missing=" + attempt(
        lambda: list(a.describe_configs([missing]).values())[0].result(30)))
    node = a.describe_cluster().result(30).nodes[0]
    b = list(a.describe_configs([ConfigResource(ConfigResource.Type.BROKER, str(node.id))]).values())[0].result(30)
    out.append(f"broker_config={len(b) > 0}")
    return " ".join(out)


def admin_cluster():
    a = admin()
    d = a.describe_cluster().result(30)
    out = [f"nodes={len(d.nodes)} controller_listed={d.controller is not None and d.controller.id in [n.id for n in d.nodes]}"
           f" cluster_id={bool(d.cluster_id)}"]
    from confluent_kafka import ElectionType
    res = a.elect_leaders(ElectionType.PREFERRED, [TopicPartition(TOPIC, 0)]).result(30)
    e = res.get(TopicPartition(TOPIC, 0))
    out.append(f"elect_preferred={'elected' if e is None else e.name()}")
    return " ".join(out)


def admin_delete_records():
    p = producer()
    for i in range(10):
        produce_ok(p, TOPIC, f"d{i}".encode())
    a, out = admin(), []
    low = list(a.delete_records([TopicPartition(TOPIC, 0, 4)]).values())[0].result(30).low_watermark
    out.append(f"low={low}")
    c = consumer()
    assign_all(c)
    out.append(f"begin={c.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)[0]}")
    out.append(f"read={vals(drain(c, 6))}")
    c.close()
    out.append("beyond_end=" + attempt(
        lambda: list(a.delete_records([TopicPartition(TOPIC, 0, 99)]).values())[0].result(30)))
    to_end = list(a.delete_records([TopicPartition(TOPIC, 0, OFFSET_END)]).values())[0].result(30)
    out.append(f"to_end={to_end.low_watermark}")
    return " ".join(out)


def admin_offsets():
    t0 = 1_700_000_000_000
    p = producer()
    for i, ts in enumerate((t0 + 2000, t0, t0 + 5000, t0 + 1000)):
        produce_ok(p, TOPIC, f"x{i}".encode(), timestamp=ts)
    a, out = admin(), []
    tp = TopicPartition(TOPIC, 0)
    specs = [("earliest", OffsetSpec.earliest()), ("latest", OffsetSpec.latest()),
             ("max_timestamp", OffsetSpec.max_timestamp()),
             ("at_t0+1500", OffsetSpec.for_timestamp(t0 + 1500)),
             ("at_far_future", OffsetSpec.for_timestamp(t0 + 99999))]
    for name, spec in specs:
        r = list(a.list_offsets({tp: spec}).values())[0].result(30)
        ts = r.timestamp
        suffix = ""
        if name.startswith("at_") or name == "max_timestamp":
            suffix = f"@{ts if ts < 0 else ts - t0}"
        out.append(f"{name}={r.offset}{suffix}")
    return " ".join(out)


# ---- metadata -------------------------------------------------------------------

def metadata():
    a = admin()
    md = a.list_topics(TOPIC, timeout=30)
    d = a.describe_cluster().result(30)
    return (f"partitions={len(md.topics[TOPIC].partitions)} brokers={len(d.nodes)} "
            f"controller={d.controller is not None} listed={TOPIC in a.list_topics(timeout=30).topics}")


def unknown_topic():
    a = admin()
    missing = TOPIC + "-missing"
    t = a.list_topics(missing, timeout=30).topics[missing]
    return f"partitions={len(t.partitions)} error={t.error.name() if t.error else 'none'}"


# ---- limits, scale, lifecycle ---------------------------------------------------

def make_topic(a, name, partitions=1, config=None):
    res = done(a.create_topics([NewTopic(name, partitions, 1, config=config or {})]))
    assert res[name] is None, res


def topic_recreate():
    a = admin()
    p = producer()
    for i in range(3):
        produce_ok(p, TOPIC, f"old{i}".encode())
    before = a.describe_topics(TopicCollection([TOPIC]))[TOPIC].result(30).topic_id
    done(a.delete_topics([TOPIC]))
    created = "TOPIC_ALREADY_EXISTS"
    for _ in range(60):
        if TOPIC not in a.list_topics(timeout=30).topics or a.list_topics(TOPIC, timeout=30).topics[TOPIC].error:
            res = done(a.create_topics([NewTopic(TOPIC, 1, 1)]))[TOPIC]
            created = "ok" if res is None else res
            if created != "TOPIC_ALREADY_EXISTS":
                break
        time.sleep(0.25)
    after = None
    for _ in range(40):
        try:
            after = a.describe_topics(TopicCollection([TOPIC]))[TOPIC].result(30).topic_id
            break
        except Exception:  # noqa: BLE001
            time.sleep(0.25)
    p2 = producer()
    o0 = produce_ok(p2, TOPIC, b"new0").offset()
    o1 = produce_ok(p2, TOPIC, b"new1").offset()
    c = consumer()
    assign_all(c)
    got = drain(c, 2)
    lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
    c.close()
    return (f"recreated={created} new_id={before != after} offsets={o0},{o1} read={vals(got)} "
            f"begin={lo} end={hi}")


def topic_limits():
    t, a = TOPIC + "-lim", admin()
    make_topic(a, t, 1, {"max.message.bytes": "2048"})
    cfg = list(a.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, t)]).values())[0].result(30)
    e = cfg["max.message.bytes"]
    p = producer(**{"message.max.bytes": 1_000_000})
    err, _ = produce_sync(p, t, bytes(4000))
    small = produce_ok(p, t, bytes(1000)).offset()
    return f"config={e.value}/{e.source} over_limit={err.name() if err else 'none'} under_limit_offset={small}"


def many_partitions():
    n = 50
    p = producer()
    for i in range(n):
        produce_ok(p, TOPIC, f"mp{i}".encode(), partition=i)
    a = admin()
    d = a.describe_topics(TopicCollection([TOPIC]))[TOPIC].result(30)
    ends = a.list_offsets({TopicPartition(TOPIC, i): OffsetSpec.latest() for i in range(n)})
    total = sum(f.result(30).offset for f in ends.values())
    c = consumer()
    assign_all(c, range(n))
    seen = {m.partition() for m in drain(c, n, 60000)}
    c.close()
    return f"described={len(d.partitions)} end_sum={total} read_partitions={len(seen)}"


def many_records():
    n = 20000
    p = producer(**{"linger.ms": 20, "batch.size": 65536, "queue.buffering.max.messages": 100000})
    sent, reports = [], []
    for i in range(n):
        v = (f"{i:08d}-" * 10).encode()
        sent.append(v)
        p.produce(TOPIC, v, partition=0, on_delivery=lambda e, m: reports.append(e))
        if i % 5000 == 0:
            p.poll(0)
    p.flush(120)
    assert len(reports) == n and not any(reports)
    c = consumer()
    assign_all(c)
    got = [m.value() for m in drain(c, n, 120000)]
    c.close()
    return f"count={len(got)} crc={crc(got)} same={crc(got) == crc(sent)}"


def seek_into_batch():
    t0 = 1_700_000_000_000
    p = producer(**{"linger.ms": 1000, "batch.size": 1048576})
    reports = []
    for i in range(100):
        p.produce(TOPIC, f"b{i}".encode(), partition=0, timestamp=t0 + i,
                  on_delivery=lambda e, m: reports.append((e, m.offset())))
    p.flush(60)
    contiguous = sorted(o for _, o in reports) == list(range(100))
    c = consumer()
    assign_all(c)
    c.seek(TopicPartition(TOPIC, 0, 37))
    first = drain(c, 1)[0]
    c.seek(TopicPartition(TOPIC, 0, 37))
    five = vals(drain(c, 5)[:5])
    c.seek(TopicPartition(TOPIC, 0, 99))
    last = vals(drain(c, 1))
    c.seek(TopicPartition(TOPIC, 0, 100))
    past = len(drain(c, 1, 1500))
    mid = c.offsets_for_times([TopicPartition(TOPIC, 0, t0 + 50)], timeout=30)[0].offset
    c.close()
    return (f"contiguous={contiguous} first={first.offset()} five={five} last={last} "
            f"past_end={past} by_time_mid={mid}")


def group_partition_growth():
    g = f"{TOPIC}-g"
    m = Member(g, **{"topic.metadata.refresh.interval.ms": 1000})
    try:
        deadline = time.time() + 60
        while not m.owned() and time.time() < deadline:
            m.poll()
        before = len(m.owned())
        a = admin()
        done(a.create_partitions([NewPartitions(TOPIC, 3)]))
        while len(m.owned()) < 3 and time.time() < deadline:
            m.poll()
        after = len(m.owned())
        produce_spread(9)
        return f"before={before} after={after} {consume_all(9, m)}"
    finally:
        m.c.close()


def group_pattern():
    g, prefix = f"{TOPIC}-g", f"{TOPIC}-p"
    a = admin()
    make_topic(a, prefix + "1")
    make_topic(a, prefix + "2")
    c = consumer(**{"group.id": g, "topic.metadata.refresh.interval.ms": 1000})
    c.subscribe(["^" + prefix.replace(".", "\\.") + "[0-9]$"])  # POSIX ERE: librdkafka has no \d
    deadline = time.time() + 60
    while len(c.assignment()) < 2 and time.time() < deadline:
        c.poll(0.2)
    before = len(c.assignment())
    make_topic(a, prefix + "3")
    while len(c.assignment()) < 3 and time.time() < deadline:
        c.poll(0.2)
    after = len(c.assignment())
    p = producer()
    for i in range(1, 4):
        produce_ok(p, prefix + str(i), f"pat{i}".encode())
    got = sorted(m.value().decode() for m in drain(c, 3, 60000))
    c.close()
    return f"before={before} after={after} read={','.join(got)}"


def compacted_null_key():
    t, a = TOPIC + "-c", admin()
    make_topic(a, t, 1, {"cleanup.policy": "compact"})
    p = producer()
    err, _ = produce_sync(p, t, b"v")
    keyed = produce_ok(p, t, b"v", key=b"k").offset()
    tomb = produce_ok(p, t, None, key=b"k").offset()
    return f"null_key={err.name() if err else 'none'} keyed_offset={keyed} tombstone_offset={tomb}"


SCENARIOS = {
    "produce-consume": produce_consume,
    "records-shape": records_shape,
    "compression": compression,
    "large-records": large_records,
    "record-too-large": record_too_large,
    "multi-partition": multi_partition,
    "offsets-by-time": offsets_by_time,
    "seek": seek,
    "long-poll": long_poll,
    "offset-out-of-range": offset_out_of_range,
    "idempotent-produce": idempotent_produce,
    "acks": acks,
    "txn-read-committed": txn_read_committed,
    "txn-consume-transform-produce": txn_consume_transform_produce,
    "txn-fenced": txn_fenced,
    "group-two-members": group_two_members,
    "group-resume": group_resume,
    "group-rebalance-leave": group_rebalance_leave,
    "group-static": group_static,
    "group-cooperative": group_cooperative,
    "group-kip848": group_kip848,
    "group-admin": group_admin,
    "admin-topics": admin_topics,
    "admin-configs": admin_configs,
    "admin-cluster": admin_cluster,
    "admin-delete-records": admin_delete_records,
    "admin-offsets": admin_offsets,
    "metadata": metadata,
    "unknown-topic": unknown_topic,
    "topic-recreate": topic_recreate,
    "topic-limits": topic_limits,
    "many-partitions": many_partitions,
    "many-records": many_records,
    "seek-into-batch": seek_into_batch,
    "group-partition-growth": group_partition_growth,
    "group-pattern": group_pattern,
    "compacted-null-key": compacted_null_key,
}


def main():
    global BROKER, TOPIC
    if len(sys.argv) < 4:
        print("usage: librdkafka-conformance <broker> <scenario> <topic>")
        return 2
    BROKER, scenario, TOPIC = sys.argv[1], sys.argv[2], sys.argv[3]
    fn = SCENARIOS.get(scenario)
    if fn is None:
        print(f"ERROR unknown scenario {scenario}")
        return 2
    try:
        print("OK " + fn())
    except (KafkaException, AssertionError, OSError) as e:
        print(f"ERROR {err_name(e)}: {e}")
        return 1
    except Exception as e:  # noqa: BLE001
        print(f"ERROR {type(e).__name__}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
