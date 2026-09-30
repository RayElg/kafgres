"""Randomised Produce requests against Kafka's rules: several partitions per request,
partitions repeated (the last entry decides), plain and idempotent batches, exact resends,
sequence gaps and oversized batches. Afterwards the log must hold every acknowledged batch
exactly once at the offset it was answered with, and nothing refused or superseded.

Seeds come from HUNT_SEEDS (comma-separated) so a failure is replayable.
"""

import os
import random
import struct

import pytest
from kafka import KafkaConsumer, TopicPartition

from conftest import sql
from recordbatch import parse_produce_v3_many, produce_v3_many, record_batch

BROKER = "127.0.0.1:9092"
SEEDS = [int(s) for s in os.environ.get("HUNT_SEEDS", "11,23").split(",")]
REQUESTS = int(os.environ.get("HUNT_REQUESTS", "150"))
PARTS = 3
PRODUCERS = [7950, 7951]
MESSAGE_TOO_LARGE = 10
OUT_OF_ORDER_SEQUENCE_NUMBER = 45


@pytest.fixture
def hunt_topic(request):
    name = f"hunt-prod-{request.node.callspec.id}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', {PARTS})")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


def read_all(topic):
    c = KafkaConsumer(bootstrap_servers=BROKER, enable_auto_commit=False,
                      consumer_timeout_ms=4000, max_partition_fetch_bytes=4 * 1024 * 1024)
    tps = [TopicPartition(topic, p) for p in range(PARTS)]
    c.assign(tps)
    c.seek_to_beginning()
    ends = c.end_offsets(tps)
    got = {}
    for m in c:
        got[(m.partition, m.offset)] = m.value.decode()
        if all(c.position(tp) >= ends[tp] for tp in tps):
            break
    c.close()
    return got, {tp.partition: ends[tp] for tp in tps}


@pytest.mark.parametrize("seed", SEEDS)
def test_produce_agrees_with_kafkas_rules(conn, hunt_topic, seed):
    rng = random.Random(seed)
    topic = hunt_topic
    uid = 0
    next_seq = {}      # (pid, partition) -> next sequence the broker expects
    acked = {}         # (pid, partition) -> [(seq, batch bytes, value, offset)], newest last
    appended = {}      # value -> (partition, offset) for every batch that must be in the log
    refused = set()    # values that must never be in the log

    for r in range(REQUESTS):
        entries = []
        for _ in range(rng.randint(1, 4)):
            p = rng.randrange(PARTS)
            kinds = ["plain", "idem"]
            pid = rng.choice(PRODUCERS)
            if acked.get((pid, p)):
                kinds += ["resend", "gap"]
            kinds.append("huge")
            entries.append({"p": p, "kind": rng.choice(kinds), "pid": pid})
        last = {e["p"]: i for i, e in enumerate(entries)}

        for i, e in enumerate(entries):
            kept = last[e["p"]] == i
            uid += 1
            e["value"] = f"s{seed}-r{r}-u{uid}"
            pid, p = e["pid"], e["p"]
            if e["kind"] == "plain":
                e["batch"] = record_batch([e["value"].encode()])
            elif e["kind"] == "huge":
                e["batch"] = record_batch([("x" * 2_000_000).encode()])
            elif e["kind"] == "idem":
                e["seq"] = next_seq.get((pid, p), 0)
                e["batch"] = record_batch([e["value"].encode()], producer_id=pid,
                                          producer_epoch=0, base_sequence=e["seq"])
            elif e["kind"] == "resend":
                # Within the retained window of five.
                seq, batch, value, offset = rng.choice(acked[(pid, p)][-5:])
                e.update(seq=seq, batch=batch, value=value, original=offset)
            elif e["kind"] == "gap":
                e["seq"] = next_seq[(pid, p)] + 3
                e["batch"] = record_batch([e["value"].encode()], producer_id=pid,
                                          producer_epoch=0, base_sequence=e["seq"])
            e["kept"] = kept

        header = struct.pack(">hhi", 0, 3, 20000 + r) + struct.pack(">h", 6) + b"pytest"
        frame = header + produce_v3_many(topic, [(e["p"], e["batch"]) for e in entries])
        conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
        _, results = parse_produce_v3_many(conn.recv())
        assert len(results) == len(entries), f"request {r}: {len(results)} answers"

        answer_of = {}
        for e, (index, err, base) in zip(entries, results):
            assert index == e["p"], f"request {r}: answered partition {index} for {e['p']}"
            if e["kept"]:
                answer_of[e["p"]] = (err, base)
        for i, (e, (_, err, base)) in enumerate(zip(entries, results)):
            label = f"seed {seed} request {r} entry {i} ({e['kind']}, p{e['p']})"
            if not e["kept"]:
                assert (err, base) == answer_of[e["p"]], f"{label}: not echoing the last entry"
                if e["kind"] != "resend" and e["value"] not in appended:
                    refused.add(e["value"])
                continue
            pid, p = e["pid"], e["p"]
            if e["kind"] == "huge":
                assert err == MESSAGE_TOO_LARGE, f"{label}: got {err}"
            elif e["kind"] == "gap":
                assert err == OUT_OF_ORDER_SEQUENCE_NUMBER, f"{label}: got {err}"
                refused.add(e["value"])
            elif e["kind"] == "resend":
                assert (err, base) == (0, e["original"]), f"{label}: got {(err, base)}"
            else:
                assert err == 0, f"{label}: got {err}"
                appended[e["value"]] = (p, base)
                refused.discard(e["value"])
                if e["kind"] == "idem":
                    next_seq[(pid, p)] = e["seq"] + 1
                    acked.setdefault((pid, p), []).append((e["seq"], e["batch"], e["value"], base))

    got, ends = read_all(topic)
    for p in range(PARTS):
        present = sorted(o for q, o in got if q == p)
        assert present == list(range(ends[p])), f"partition {p}: offsets not dense"
    seen = {}
    for (p, o), value in got.items():
        assert value not in seen, f"{value} is in the log twice: {seen[value]} and {(p, o)}"
        seen[value] = (p, o)
    for value, where in appended.items():
        assert seen.get(value) == where, f"{value} acknowledged at {where}, found at {seen.get(value)}"
    for value in refused:
        assert value not in seen, f"refused or superseded {value} is in the log at {seen[value]}"


@pytest.mark.skipif(sql("SHOW kafgres.storage_engine") != "segment",
                    reason="segment engine only: its appends outlive a rollback")
def test_an_error_after_the_append_does_not_append_again(conn, hunt_topic_single):
    """An ERROR between the append and the stamp (a timeout, a cancel) unwinds the request
    savepoint but not the segment file. The redo must record the batch at the offset it
    landed at, not append it again. Injected with a trigger that fails once."""
    topic = hunt_topic_single
    sql("CREATE SEQUENCE IF NOT EXISTS hunt_fail_once")
    sql("SELECT setval('hunt_fail_once', 1, false)")
    sql("""CREATE OR REPLACE FUNCTION hunt_fail_stamp() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN
             IF NEW.producer_id = 7998 AND nextval('hunt_fail_once') = 1 THEN
               RAISE EXCEPTION 'injected';
             END IF;
             RETURN NEW;
           END $$""")
    sql("""CREATE TRIGGER hunt_fail_stamp BEFORE UPDATE ON kafgres_producer_batches
           FOR EACH ROW EXECUTE FUNCTION hunt_fail_stamp()""")
    try:
        batch = record_batch([b"once"], producer_id=7998, producer_epoch=0, base_sequence=0)
        header = struct.pack(">hhi", 0, 3, 30001) + struct.pack(">h", 6) + b"pytest"
        frame = header + produce_v3_many(topic, [(0, batch)])
        conn.sock.sendall(struct.pack(">i", len(frame)) + frame)
        _, [(_, err, base)] = parse_produce_v3_many(conn.recv())
        assert err == 0, f"the redo should succeed, got {err}"
        header = struct.pack(">hhi", 0, 3, 30002) + struct.pack(">h", 6) + b"pytest"
        conn.sock.sendall(struct.pack(">i", len(header + produce_v3_many(topic, [(0, batch)])))
                          + header + produce_v3_many(topic, [(0, batch)]))
        _, [(_, err2, base2)] = parse_produce_v3_many(conn.recv())
        assert (err2, base2) == (0, base), f"a resend answered {(err2, base2)}, not {(0, base)}"
        got, ends = read_all_single(topic)
        assert ends == 1, f"the batch was appended {ends} times"
    finally:
        sql("DROP TRIGGER IF EXISTS hunt_fail_stamp ON kafgres_producer_batches")
        sql("DROP FUNCTION IF EXISTS hunt_fail_stamp()")
        sql("DROP SEQUENCE IF EXISTS hunt_fail_once")


@pytest.fixture
def hunt_topic_single():
    name = "hunt-prod-error"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


def read_all_single(topic):
    c = KafkaConsumer(bootstrap_servers=BROKER, enable_auto_commit=False, consumer_timeout_ms=4000)
    tp = TopicPartition(topic, 0)
    c.assign([tp])
    c.seek_to_beginning(tp)
    end = c.end_offsets([tp])[tp]
    got = [m.value for m in c] if end else []
    c.close()
    return got, end
