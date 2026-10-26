"""Zombies: a producer or consumer that was replaced but keeps sending.

RAY-7: a transactional Produce from a fenced epoch, or to a partition not added to the
transaction, is refused before it is written, or its successor's commit publishes it.

RAY-8: OffsetCommit and TxnOffsetCommit check the member, generation and group state, or
an evicted consumer's late commit rewinds its successor's offset.

RAY-11: the producer-id sweep spares a producer with an open transaction, or its EndTxn
fails with INVALID_PRODUCER_ID_MAPPING and the LSO stays pinned; and a transaction whose
producer row is already gone is still aborted.

Raw frames, since no client sends a stale epoch on purpose. Expected codes are
apache/kafka 4.3.1's answers to the same requests.
"""

import re
import socket
import struct
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import BROKER_HOST, BROKER_PORT, sql
from support.recordbatch import record_batch

NONE = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
ILLEGAL_GENERATION = 22
UNKNOWN_MEMBER_ID = 25
REBALANCE_IN_PROGRESS = 27
UNSUPPORTED_VERSION = 35
INVALID_PRODUCER_EPOCH = 47
PRODUCER_FENCED = 90
INVALID_TXN_STATE = 48
GROUP_ID_NOT_FOUND = 69
MEMBER_ID_REQUIRED = 79
FENCED_INSTANCE_ID = 82
STALE_MEMBER_EPOCH = 113
TRANSACTION_ABORTABLE = 120

# api key -> first flexible version
FLEX = {0: 9, 8: 8, 11: 6, 14: 4, 22: 2, 24: 3, 25: 3, 26: 3, 28: 3, 68: 0, 76: 0}


def uvarint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out


class Enc:
    def __init__(self, flex):
        self.flex, self.b = flex, b""

    def i8(self, v): self.b += struct.pack(">b", v); return self
    def i16(self, v): self.b += struct.pack(">h", v); return self
    def i32(self, v): self.b += struct.pack(">i", v); return self
    def i64(self, v): self.b += struct.pack(">q", v); return self

    def str(self, s):
        if s is None:
            self.b += uvarint(0) if self.flex else struct.pack(">h", -1)
            return self
        e = s.encode()
        self.b += (uvarint(len(e) + 1) if self.flex else struct.pack(">h", len(e))) + e
        return self

    def bytes(self, v):
        self.b += (uvarint(len(v) + 1) if self.flex else struct.pack(">i", len(v))) + v
        return self

    def arr(self, items, fn):
        if items is None:
            self.b += uvarint(0) if self.flex else struct.pack(">i", -1)
            return self
        self.b += uvarint(len(items) + 1) if self.flex else struct.pack(">i", len(items))
        for it in items:
            fn(self, it)
        return self

    def tags(self):
        if self.flex:
            self.b += uvarint(0)
        return self


class Dec:
    def __init__(self, b, flex):
        self.b, self.i, self.flex = b, 0, flex

    def i16(self): v = struct.unpack_from(">h", self.b, self.i)[0]; self.i += 2; return v
    def i32(self): v = struct.unpack_from(">i", self.b, self.i)[0]; self.i += 4; return v
    def i64(self): v = struct.unpack_from(">q", self.b, self.i)[0]; self.i += 8; return v

    def uv(self):
        n, s = 0, 0
        while True:
            c = self.b[self.i]; self.i += 1
            n |= (c & 0x7F) << s
            if not c & 0x80:
                return n
            s += 7

    def str(self):
        n = self.uv() - 1 if self.flex else self.i16()
        if n < 0:
            return None
        v = self.b[self.i:self.i + n].decode(); self.i += n
        return v

    def arrlen(self):
        return self.uv() - 1 if self.flex else self.i32()

    def tags(self):
        if self.flex:
            for _ in range(self.uv()):
                self.uv(); n = self.uv(); self.i += n


class Broker:
    def __init__(self):
        self.sock = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=60)
        self.corr = 0

    def close(self):
        self.sock.close()

    def send(self, key, ver, body):
        self.corr += 1
        h = struct.pack(">hhi", key, ver, self.corr) + struct.pack(">h", 6) + b"pytest"
        if ver >= FLEX[key]:
            h += uvarint(0)
        frame = h + body
        self.sock.sendall(struct.pack(">i", len(frame)) + frame)

    def recv(self, key, ver):
        n = struct.unpack(">i", self._exactly(4))[0]
        d = Dec(self._exactly(n), ver >= FLEX[key])
        d.i32()
        d.tags()
        return d

    def call(self, key, ver, body):
        self.send(key, ver, body)
        return self.recv(key, ver)

    def _exactly(self, n):
        b = b""
        while len(b) < n:
            c = self.sock.recv(n - len(b))
            if not c:
                raise EOFError
            b += c
        return b

    # ---- transactions ----

    def init_pid(self, txn):
        d = self.call(22, 1, Enc(False).str(txn).i32(60000).b)
        d.i32()
        err, pid, epoch = d.i16(), d.i64(), d.i16()
        assert err == NONE, f"InitProducerId failed with {err}"
        return pid, epoch

    def add_partitions(self, txn, pid, epoch, topic, parts=(0,)):
        e = Enc(False).str(txn).i64(pid).i16(epoch)
        e.arr([topic], lambda e, t: e.str(t).arr(list(parts), lambda e, p: e.i32(p)))
        d = self.call(24, 2, e.b)
        d.i32(); d.i32(); d.str(); d.i32(); d.i32()
        return d.i16()

    def add_offsets(self, txn, pid, epoch, group):
        d = self.call(25, 2, Enc(False).str(txn).i64(pid).i16(epoch).str(group).b)
        d.i32()
        return d.i16()

    def end_txn(self, txn, pid, epoch, commit):
        d = self.call(26, 2, Enc(False).str(txn).i64(pid).i16(epoch).i8(1 if commit else 0).b)
        d.i32()
        return d.i16()

    def produce(self, ver, txn, topic, part, records):
        """One partition; returns its (error_code, base_offset)."""
        flex = ver >= FLEX[0]
        e = Enc(flex).str(txn).i16(-1).i32(30000)

        def tp(e, t):
            e.str(t).arr([part], lambda e, p: e.i32(p).bytes(records).tags()).tags()
        e.arr([topic], tp).tags()
        d = self.call(0, ver, e.b)
        d.arrlen(); d.str(); d.arrlen()
        d.i32()
        return d.i16(), d.i64()

    def txn_offset_commit(self, txn, group, pid, epoch, gen, member, instance, topic, part, offset,
                          ver=3):
        e = Enc(True).str(txn).str(group).i64(pid).i16(epoch).i32(gen).str(member).str(instance)
        pp = lambda e, p: e.i32(p).i64(offset).i32(-1).str("").tags()
        e.arr([topic], lambda e, t: e.str(t).arr([part], pp).tags()).tags()
        d = self.call(28, ver, e.b)
        d.i32(); d.arrlen(); d.str(); d.arrlen(); d.i32()
        return d.i16()

    # ---- groups ----

    def offset_commit(self, ver, group, gen, member, instance, topic, part, offset):
        e = Enc(ver >= FLEX[8]).str(group).i32(gen).str(member)
        if ver >= 7:
            e.str(instance)
        if ver <= 4:
            e.i64(-1)

        def pp(e, p):
            e.i32(p).i64(offset)
            if ver >= 6:
                e.i32(-1)
            e.str("").tags()
        e.arr([topic], lambda e, t: e.str(t).arr([part], pp).tags()).tags()
        d = self.call(8, ver, e.b)
        if ver >= 3:
            d.i32()
        d.arrlen(); d.str(); d.arrlen(); d.i32()
        return d.i16()

    def join(self, group, member, instance=None, session_ms=30000):
        e = Enc(False).str(group).i32(session_ms).i32(30000).str(member).str(instance)
        e.str("consumer").arr(["range"], lambda e, n: e.str(n).bytes(b"\x00" * 10))
        d = self.call(11, 5, e.b)
        d.i32()
        err, gen = d.i16(), d.i32()
        d.str(); leader = d.str(); member = d.str()
        return err, gen, member, leader

    def join_full(self, group, instance=None, session_ms=30000):
        err, gen, member, leader = self.join(group, "", instance, session_ms)
        if err == MEMBER_ID_REQUIRED:
            err, gen, member, leader = self.join(group, member, instance, session_ms)
        assert err == NONE, f"JoinGroup failed with {err}"
        return gen, member, leader

    def sync(self, group, gen, member, instance=None, assignments=()):
        e = Enc(False).str(group).i32(gen).str(member).str(instance)
        e.arr(list(assignments), lambda e, m: e.str(m).bytes(b""))
        d = self.call(14, 3, e.b)
        d.i32()
        return d.i16()

    def cg_heartbeat(self, group, member, epoch, topics):
        e = Enc(True).str(group).str(member).i32(epoch).str(None).str(None).i32(30000)
        e.arr(topics, lambda e, t: e.str(t)).str(None).arr([] if epoch == 0 else None, None)
        d = self.call(68, 0, e.tags().b)
        d.i32()
        err = d.i16(); d.str(); mid = d.str(); ep = d.i32()
        return err, mid, ep

    def share_heartbeat(self, group, member, topics):
        e = Enc(True).str(group).str(member).i32(0).str(None).arr(topics, lambda e, t: e.str(t))
        d = self.call(76, 1, e.tags().b)
        d.i32()
        return d.i16()


def txn_batch(value, pid, epoch, seq):
    return record_batch([value], producer_id=pid, producer_epoch=epoch, base_sequence=seq,
                        transactional=True)


def high_watermark(topic, partition=0):
    # NULL on the segment engine until the partition's first append.
    return int(sql(f"SELECT COALESCE(high_watermark, 0) FROM kafgres_partition_offsets('{topic}') "
                   f"WHERE partition = {partition}"))


def read_committed(topic, timeout_ms=8000):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"{BROKER_HOST}:{BROKER_PORT}",
                      isolation_level="read_committed", enable_auto_commit=False,
                      consumer_timeout_ms=timeout_ms)
    try:
        tp = TopicPartition(topic, 0)
        c.assign([tp])
        c.seek_to_beginning(tp)
        return [m.value for m in c]
    finally:
        c.close()


@pytest.fixture
def topic(request):
    name = f"zf-{re.sub('[^a-z0-9]+', '-', request.node.name[5:])[:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 2)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


@pytest.fixture
def group(request):
    name = f"zf-{re.sub('[^a-z0-9]+', '-', request.node.name[5:])[:40]}-{uuid.uuid4().hex[:6]}"
    yield name
    for table in ("kafgres_offsets", "kafgres_group_members", "kafgres_groups", "kafgres_consumer_groups",
                  "kafgres_share_groups"):
        sql(f"DELETE FROM {table} WHERE group_id LIKE '{name}%'")


def txn_id(name):
    return f"{name}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------- RAY-7


@pytest.mark.parametrize("version,current_code", [
    (3, INVALID_TXN_STATE), (10, INVALID_TXN_STATE), (11, TRANSACTION_ABORTABLE), (12, NONE),
])
def test_a_transactional_produce_is_verified_before_it_is_written(topic, version, current_code):
    """The fenced epoch is refused at every version. The current epoch without
    AddPartitionsToTxn is refused before v12; from v12 the append adds the partition."""
    b = Broker()
    txn = txn_id(topic)
    try:
        pid, old = b.init_pid(txn)
        _, cur = b.init_pid(txn)
        before = high_watermark(topic)
        err, _ = b.produce(version, txn, topic, 0, txn_batch(b"zombie", pid, old, 0))
        assert err == INVALID_PRODUCER_EPOCH, f"fenced epoch at v{version}: {err}"
        assert high_watermark(topic) == before, "the fenced batch was appended"

        err, _ = b.produce(version, txn, topic, 0, txn_batch(b"current", pid, cur, 0))
        assert err == current_code, f"current epoch, no AddPartitions, v{version}: {err}"
        if current_code != NONE:
            assert high_watermark(topic) == before, "an unverified batch was appended"
        state = sql(f"SELECT state || ' ' || producer_epoch FROM kafgres_txns "
                    f"WHERE producer_id = {pid}")
        assert state == (f"ongoing {cur}" if current_code == NONE else f"empty {cur}"), state
    finally:
        b.end_txn(txn, pid, cur, False)
        b.close()


def test_a_fenced_producer_cannot_slip_a_record_into_its_successors_commit(topic):
    """The ticket's repro: read_committed must see only the successor's record."""
    txn = txn_id(topic)
    a, b = Broker(), Broker()
    try:
        pid, e0 = a.init_pid(txn)
        _, e1 = b.init_pid(txn)
        err, _ = a.produce(3, txn, topic, 0, txn_batch(b"ZOMBIE", pid, e0, 0))
        assert err == INVALID_PRODUCER_EPOCH, err
        assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}") != "ongoing"

        assert b.add_partitions(txn, pid, e1, topic) == NONE
        err, _ = b.produce(3, txn, topic, 0, txn_batch(b"B-rec", pid, e1, 0))
        assert err == NONE, err
        assert b.end_txn(txn, pid, e1, True) == NONE
        assert read_committed(topic) == [b"B-rec"]
    finally:
        a.close()
        b.close()


def test_a_producer_fenced_mid_transaction_is_refused_on_its_next_batch(topic):
    """Fenced after it added the partition and wrote, the case a real client reaches."""
    txn = txn_id(topic)
    b = Broker()
    try:
        pid, e0 = b.init_pid(txn)
        assert b.add_partitions(txn, pid, e0, topic) == NONE
        assert b.produce(3, txn, topic, 0, txn_batch(b"a", pid, e0, 0))[0] == NONE
        _, e1 = b.init_pid(txn)
        before = high_watermark(topic)
        for version in (3, 12):
            err, _ = b.produce(version, txn, topic, 0, txn_batch(b"b", pid, e0, 1))
            assert err == INVALID_PRODUCER_EPOCH, f"v{version}: {err}"
        assert high_watermark(topic) == before
        assert read_committed(topic) == []
    finally:
        b.close()


class Interleave:
    """A second session holding the producer's transaction and producer rows. `abort()`
    waits until the broker blocks behind them, then runs the expiry sweep and commits, so
    the transaction is aborted and fenced between a request's check and its write."""

    def __init__(self, pid):
        self.p = subprocess.Popen(
            ["docker", "compose", "exec", "-T", "postgres",
             "psql", "-U", "postgres", "-qtA", "-v", "ON_ERROR_STOP=1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self._run(f"BEGIN;\n"
                  f"SELECT 1 FROM kafgres_txns WHERE producer_id = {pid} FOR UPDATE;\n"
                  f"SELECT 1 FROM kafgres_producers WHERE producer_id = {pid} FOR UPDATE;\n")

    def _run(self, script):
        self.p.stdin.write(script + "\\echo MARK\n")
        self.p.stdin.flush()
        while (line := self.p.stdout.readline()) != "MARK\n":
            assert line, "the interleaving session failed"

    def abort(self):
        # Inside the broker's 2 s lock_timeout.
        self._run("""DO $$ BEGIN
            FOR i IN 1..300 LOOP
                PERFORM pg_stat_clear_snapshot();
                IF EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE backend_type = 'kafgres_broker' AND wait_event_type = 'Lock')
                THEN RETURN; END IF;
                PERFORM pg_sleep(0.005);
            END LOOP;
            RAISE EXCEPTION 'the broker never waited on the held rows';
        END $$;
        SELECT kafgres_expire_transactions();
        COMMIT;
        """)

    def close(self):
        if self.p.poll() is None:
            self.p.stdin.write("ROLLBACK;\n\\q\n")
            self.p.stdin.flush()
        self.p.wait(timeout=30)


@pytest.mark.parametrize("request_kind", ["add_partitions", "produce_v12", "produce_v3"])
def test_a_transaction_aborted_behind_a_waiting_request_stays_aborted(topic, request_kind):
    """The sweep aborts and fences while the request waits: AddPartitionsToTxn or a v12
    produce must not reopen the transaction, and a batch to an added partition must not
    land after the abort marker, where no marker closes it."""
    b = Broker()
    txn = txn_id(topic)
    with ThreadPoolExecutor(1) as pool:
        try:
            pid, e0 = open_txn(b, txn, topic, b"first")
            sql(f"UPDATE kafgres_txns SET timeout_ms = 1000, started_at = started_at - 60000 "
                f"WHERE producer_id = {pid}")
            # A stale `last_ts` is refreshed in phase 2, so an unlocked check would wait there,
            # between the check and the write.
            backdate(pid, 3600)
            before = (high_watermark(topic, 0), high_watermark(topic, 1))
            held = Interleave(pid)
            try:
                if request_kind == "add_partitions":
                    sent = pool.submit(b.add_partitions, txn, pid, e0, topic, (1,))
                    expected = PRODUCER_FENCED
                elif request_kind == "produce_v12":
                    sent = pool.submit(b.produce, 12, txn, topic, 1, txn_batch(b"z", pid, e0, 0))
                    expected = INVALID_PRODUCER_EPOCH
                else:
                    sent = pool.submit(b.produce, 3, txn, topic, 0, txn_batch(b"z", pid, e0, 1))
                    expected = INVALID_PRODUCER_EPOCH
                held.abort()
                answer = sent.result(timeout=30)
            finally:
                held.close()
            code = answer if request_kind == "add_partitions" else answer[0]
            assert code == expected, f"{request_kind}: {code}"
            assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}") == "aborted"
            # Partition 0 gained only the abort marker; partition 1 nothing.
            assert (high_watermark(topic, 0), high_watermark(topic, 1)) == (before[0] + 1, before[1])

            plain = record_batch([b"after"], producer_id=-1, producer_epoch=-1, base_sequence=-1)
            assert b.produce(3, None, topic, 0, plain)[0] == NONE
            assert read_committed(topic) == [b"after"]
        finally:
            b.close()


# ---------------------------------------------------------------- RAY-8


def test_commits_naming_no_live_member(topic, group):
    """Fabricated identities. A missing group with a generation is ILLEGAL_GENERATION
    before v9 and GROUP_ID_NOT_FOUND from v9; generation -1 is a simple (admin) commit."""
    b = Broker()
    try:
        assert b.offset_commit(2, group + "a", 99, "nobody", None, topic, 0, 2) == ILLEGAL_GENERATION
        assert b.offset_commit(7, group + "b", 99, "nobody", None, topic, 0, 2) == ILLEGAL_GENERATION
        assert b.offset_commit(9, group + "c", 99, "nobody", None, topic, 0, 2) == GROUP_ID_NOT_FOUND
        assert sql(f"SELECT count(*) FROM kafgres_offsets WHERE group_id LIKE '{group}%'") == "0"

        assert b.offset_commit(7, group + "d", -1, "", None, topic, 0, 2) == NONE
        assert b.offset_commit(7, group + "d", -1, "", None, topic, 7, 2) == UNKNOWN_TOPIC_OR_PARTITION
        assert b.offset_commit(7, group + "d", -1, "", None, topic, -1, 2) == UNKNOWN_TOPIC_OR_PARTITION
        # Kafka stores a negative offset as given; so does this.
        assert b.offset_commit(7, group + "d", -1, "", None, topic, 0, -5) == NONE
        # The group now exists (it has offsets) but has no such member.
        assert b.offset_commit(7, group + "d", 3, "nobody", None, topic, 0, 2) == UNKNOWN_MEMBER_ID
    finally:
        b.close()


def test_an_evicted_consumer_cannot_rewind_its_successors_offset(topic, group):
    """The ticket's part B: A is evicted, B commits 15 at a later generation, then A's
    late commit under its old member id and generation arrives."""
    a, b = Broker(), Broker()
    try:
        gen1, ma, _ = a.join_full(group, session_ms=2000)
        assert a.sync(group, gen1, ma, assignments=[ma]) == NONE
        assert a.offset_commit(7, group, gen1, ma, None, topic, 0, 5) == NONE

        deadline = time.time() + 60
        while sql(f"SELECT count(*) FROM kafgres_group_members WHERE group_id = '{group}' "
                  f"AND member_id = '{ma}'") != "0":
            assert time.time() < deadline, "A was never evicted"
            time.sleep(0.5)

        gen2, mb, _ = b.join_full(group)
        assert gen2 > gen1
        assert b.sync(group, gen2, mb, assignments=[mb]) == NONE
        assert b.offset_commit(7, group, gen2, mb, None, topic, 0, 15) == NONE

        assert a.offset_commit(7, group, gen1, ma, None, topic, 0, 1) == UNKNOWN_MEMBER_ID
        assert b.offset_commit(7, group, gen1, mb, None, topic, 0, 1) == ILLEGAL_GENERATION
        assert b.offset_commit(7, group, -1, "", None, topic, 0, 1) == UNKNOWN_MEMBER_ID
        assert b.offset_commit(7, group, gen2, mb, None, topic, 9, 1) == UNKNOWN_TOPIC_OR_PARTITION
        assert sql(f"SELECT committed_offset FROM kafgres_offsets WHERE group_id = '{group}' "
                   "AND partition = 0") == "15"
    finally:
        a.close()
        b.close()


def test_a_commit_between_join_and_sync_is_told_to_rejoin(topic, group):
    """CompletingRebalance: Kafka answers REBALANCE_IN_PROGRESS."""
    b = Broker()
    try:
        gen, m, _ = b.join_full(group)
        assert b.offset_commit(7, group, gen, m, None, topic, 0, 3) == REBALANCE_IN_PROGRESS
        assert b.sync(group, gen, m, assignments=[m]) == NONE
        assert b.offset_commit(7, group, gen, m, None, topic, 0, 3) == NONE
    finally:
        b.close()


def test_a_static_member_replaced_under_its_instance_id_is_fenced(topic, group):
    b = Broker()
    try:
        gen, m, _ = b.join_full(group, instance="inst-1")
        assert b.sync(group, gen, m, instance="inst-1", assignments=[m]) == NONE
        assert b.offset_commit(7, group, gen, m, "inst-1", topic, 0, 3) == NONE
        assert b.offset_commit(7, group, gen, "other", "inst-1", topic, 0, 3) == FENCED_INSTANCE_ID
        assert b.offset_commit(7, group, gen, m, "inst-2", topic, 0, 3) == UNKNOWN_MEMBER_ID
    finally:
        b.close()


def test_a_kip848_member_commits_only_at_its_own_epoch(topic, group):
    """STALE_MEMBER_EPOCH for an epoch ahead of the member's, or behind it on a partition
    it does not hold (KIP-1251); and only from OffsetCommit v9."""
    other = f"{topic}-x"
    sql(f"SELECT kafgres_drop_topic('{other}')")
    sql(f"SELECT kafgres_create_topic('{other}', 1)")
    b = Broker()
    try:
        err, mid, epoch = b.cg_heartbeat(group, uuid.uuid4().hex[:22], 0, [topic])
        assert err == NONE and epoch > 0, (err, epoch)
        assert b.offset_commit(9, group, epoch, mid, None, topic, 0, 7) == NONE
        assert b.offset_commit(9, group, epoch + 1, mid, None, topic, 0, 7) == STALE_MEMBER_EPOCH
        assert b.offset_commit(9, group, epoch, "bogus", None, topic, 0, 7) == UNKNOWN_MEMBER_ID
        assert b.offset_commit(9, group, -1, "", None, topic, 0, 7) == UNKNOWN_MEMBER_ID
        assert b.offset_commit(8, group, epoch, mid, None, topic, 0, 7) == UNSUPPORTED_VERSION

        # The member moves on an epoch while still holding partition 0 of `topic`.
        tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
        sql(f"UPDATE kafgres_consumer_group_members SET member_epoch = {epoch + 1}, "
            f"owned = ARRAY['{tid}:0'], granted = '{{}}' WHERE group_id = '{group}'")
        assert b.offset_commit(9, group, epoch, mid, None, topic, 0, 8) == NONE
        assert b.offset_commit(9, group, epoch, mid, None, topic, 1, 8) == STALE_MEMBER_EPOCH
        assert b.offset_commit(9, group, epoch, mid, None, other, 0, 8) == STALE_MEMBER_EPOCH
    finally:
        b.close()
        sql(f"SELECT kafgres_drop_topic('{other}')")


def test_offsets_cannot_be_committed_to_a_share_group(topic, group):
    b = Broker()
    try:
        assert b.share_heartbeat(group, uuid.uuid4().hex[:22], [topic]) == NONE
        assert b.offset_commit(9, group, -1, "", None, topic, 0, 1) == GROUP_ID_NOT_FOUND
        assert b.offset_commit(7, group, -1, "", None, topic, 0, 1) == GROUP_ID_NOT_FOUND
    finally:
        b.close()


def test_txn_offset_commit_fences_a_zombie_consumer(topic, group):
    """KIP-447: the member and generation in a transactional commit are checked."""
    c, t = Broker(), Broker()
    txn = txn_id(topic)
    try:
        gen, m, _ = c.join_full(group)
        assert c.sync(group, gen, m, assignments=[m]) == NONE
        pid, epoch = t.init_pid(txn)
        assert t.add_offsets(txn, pid, epoch, group) == NONE
        toc = lambda *a: t.txn_offset_commit(txn, group, pid, epoch, *a)
        assert toc(gen, m, None, topic, 0, 6) == NONE
        assert toc(gen + 1, m, None, topic, 0, 6) == ILLEGAL_GENERATION
        assert toc(gen, "bogus", None, topic, 0, 6) == UNKNOWN_MEMBER_ID
        assert toc(gen, m, "who", topic, 0, 6) == UNKNOWN_MEMBER_ID
        assert toc(-1, "", None, topic, 0, 6) == NONE, "pre-KIP-447 commits carry no member"
        assert toc(gen, m, None, topic, 9, 6) == UNKNOWN_TOPIC_OR_PARTITION
        assert toc(gen, m, None, f"{topic}-nope", 0, 6) == UNKNOWN_TOPIC_OR_PARTITION
        assert t.end_txn(txn, pid, epoch, False) == NONE

        txn2 = txn_id(topic)
        pid2, epoch2 = t.init_pid(txn2)
        assert t.add_offsets(txn2, pid2, epoch2, f"{group}-ghost") == NONE
        assert t.txn_offset_commit(txn2, f"{group}-ghost", pid2, epoch2, 5, "m", None,
                                   topic, 0, 1) == ILLEGAL_GENERATION
        assert t.end_txn(txn2, pid2, epoch2, False) == NONE
    finally:
        c.close()
        t.close()


def test_a_refused_v5_txn_offset_commit_still_begins_the_transaction(topic, group):
    """As in apache/kafka 4.3.1, which adds the offsets partition before checking the group:
    the refusal stages nothing, but the transaction is ongoing and a commit succeeds."""
    t = Broker()
    txn = txn_id(topic)
    try:
        pid, epoch = t.init_pid(txn)
        toc = lambda *a: t.txn_offset_commit(txn, f"{group}-ghost", pid, epoch, *a, ver=5)
        assert toc(5, "m", None, topic, 0, 1) == ILLEGAL_GENERATION
        assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}") == "ongoing"
        assert sql(f"SELECT count(*) FROM kafgres_txn_offsets WHERE producer_id = {pid}") == "0"
        assert t.end_txn(txn, pid, epoch, True) == NONE

        # Kafka refuses an absent topic before it adds the offsets partition.
        txn2 = txn_id(topic)
        pid2, epoch2 = t.init_pid(txn2)
        assert t.txn_offset_commit(txn2, f"{group}-ghost", pid2, epoch2, -1, "", None,
                                   f"{topic}-absent", 0, 1, ver=5) == UNKNOWN_TOPIC_OR_PARTITION
        assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid2}") == "empty"
    finally:
        t.close()


# ---------------------------------------------------------------- RAY-11


@pytest.fixture
def retention():
    """`kafgres.producer_id_expiration_ms` / `max_producer_ids`, reset afterwards."""
    def apply(expiration_ms=None, max_ids=None):
        if expiration_ms is not None:
            sql(f"ALTER SYSTEM SET kafgres.producer_id_expiration_ms = {expiration_ms}")
        if max_ids is not None:
            sql(f"ALTER SYSTEM SET kafgres.max_producer_ids = {max_ids}")
        sql("SELECT pg_reload_conf()")

    yield apply
    sql("ALTER SYSTEM RESET kafgres.producer_id_expiration_ms")
    sql("ALTER SYSTEM RESET kafgres.max_producer_ids")
    sql("SELECT pg_reload_conf()")


def backdate(pid, secs):
    sql(f"UPDATE kafgres_producers SET last_ts = now() - make_interval(secs => {secs}) "
        f"WHERE producer_id = {pid}")


def producer_exists(pid):
    return int(sql(f"SELECT count(*) FROM kafgres_producers WHERE producer_id = {pid}")) == 1


def open_txn(b, txn, topic, value):
    pid, epoch = b.init_pid(txn)
    assert b.add_partitions(txn, pid, epoch, topic) == NONE
    assert b.produce(3, txn, topic, 0, txn_batch(value, pid, epoch, 0))[0] == NONE
    return pid, epoch


def test_an_open_transaction_survives_the_expiry_sweep(topic, retention):
    """Nothing in a transaction refreshes `last_ts`, so the sweep must exempt it."""
    b = Broker()
    txn = txn_id(topic)
    try:
        pid, epoch = open_txn(b, txn, topic, b"held")
        idle, _ = b.init_pid(txn_id(topic))
        backdate(pid, 3600)
        backdate(idle, 3600)
        retention(expiration_ms=3000)
        sql("SELECT kafgres_expire_producers()")
        assert not producer_exists(idle), "the sweep did not run"
        assert producer_exists(pid), "the producer of an open transaction was swept"

        assert b.end_txn(txn, pid, epoch, True) == NONE
        assert read_committed(topic) == [b"held"]
    finally:
        b.close()


def test_an_open_transaction_survives_the_producer_id_ceiling(topic, retention):
    """The ceiling evicts past its 300 s floor, inside the 15 min a transaction may stay open."""
    b = Broker()
    txn = txn_id(topic)
    try:
        pid, epoch = open_txn(b, txn, topic, b"held")
        idle, _ = b.init_pid(txn_id(topic))
        b.init_pid(txn_id(topic))  # a fresh producer, so the cut falls at or above `pid`
        # Past the floor, well inside the default 24 h expiry: only the ceiling applies.
        backdate(pid, 3600)
        backdate(idle, 7200)
        retention(max_ids=1)
        sql("SELECT kafgres_expire_producers()")
        assert not producer_exists(idle), "the ceiling did not run"
        assert producer_exists(pid), "the ceiling evicted the producer of an open transaction"

        assert b.end_txn(txn, pid, epoch, True) == NONE
        assert read_committed(topic) == [b"held"]
    finally:
        b.close()


@pytest.mark.parametrize("by", ["timeout", "reinit"])
def test_a_transaction_whose_producer_is_gone_is_still_aborted(topic, by):
    """Both the timeout sweep and a new InitProducerId for its transactional id abort it."""
    b = Broker()
    txn = txn_id(topic)
    try:
        pid, _ = open_txn(b, txn, topic, b"orphan")
        sql(f"DELETE FROM kafgres_producers WHERE producer_id = {pid}")
        if by == "timeout":
            sql(f"UPDATE kafgres_txns SET timeout_ms = 1000, started_at = started_at - 60000 "
                f"WHERE producer_id = {pid}")
            sql("SELECT kafgres_expire_transactions()")
        else:
            new_pid, _ = b.init_pid(txn)
            assert new_pid != pid
        assert sql(f"SELECT state FROM kafgres_txns WHERE producer_id = {pid}") == "aborted"

        after = record_batch([b"after"], producer_id=-1, producer_epoch=-1, base_sequence=-1)
        assert b.produce(3, None, topic, 0, after)[0] == NONE
        assert read_committed(topic) == [b"after"], "the orphan is visible or the LSO is pinned"
    finally:
        b.close()
