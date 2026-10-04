"""Randomised compaction against a model: keyed values, tombstones, committed and aborted
Kafka transactions, SQL produces rolled back to a savepoint, and cleaning at random points,
some of it undone by a savepoint. After every cleaning, a read_committed scan from the
start must agree with what was acknowledged:

- every record read was acknowledged as committed, at the offset it was written at (I9);
- nothing aborted or rolled back is visible;
- each key's newest committed record is there, unless it is a tombstone old enough to go;
- a value is never readable once the tombstone after it has gone (I10).

Seeds come from HUNT_SEEDS (comma-separated) so a failure is replayable.
"""

import os
import random
import subprocess
import time

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from conftest import sql

BROKER = "127.0.0.1:9092"
KAFKA = "apache/kafka:4.1.0"
SEEDS = [int(s) for s in os.environ.get("HUNT_SEEDS", "11,23").split(",")]
STEPS = int(os.environ.get("HUNT_STEPS", "70"))
KEYS = [f"k{i}" for i in range(8)]
PAD = "p" * 700


def engine():
    return sql("SHOW kafgres.storage_engine")


@pytest.fixture
def hunt_topic(request):
    """Small segments so cleaning has sealed data to work on, and the broker's own sweep
    out of the way so only the test decides when cleaning runs."""
    name = f"hunt-cmp-{request.node.callspec.id}"
    for stmt in ("ALTER SYSTEM SET kafgres.segment_offsets = 4",
                 "ALTER SYSTEM SET kafgres.segment_bytes = 8192",
                 "ALTER SYSTEM SET kafgres.retention_check_interval_ms = 3600000",
                 "SELECT pg_reload_conf()"):
        sql(stmt)
    time.sleep(1)
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA, "/opt/kafka/bin/kafka-configs.sh",
         "--bootstrap-server", BROKER, "--entity-type", "topics", "--entity-name", name,
         "--alter", "--add-config", "cleanup.policy=compact,delete.retention.ms=0"],
        capture_output=True, text=True, timeout=300,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    for stmt in ("ALTER SYSTEM RESET kafgres.segment_offsets",
                 "ALTER SYSTEM RESET kafgres.segment_bytes",
                 "ALTER SYSTEM RESET kafgres.retention_check_interval_ms",
                 "SELECT pg_reload_conf()"):
        sql(stmt)


class Model:
    def __init__(self):
        self.committed = {}   # offset -> (key, value or None)
        self.hidden = set()   # offsets acknowledged but aborted or rolled back

    def latest(self):
        out = {}
        for off in sorted(self.committed):
            key, value = self.committed[off]
            out[key] = (off, value)
        return out


def read_all(topic):
    c = KafkaConsumer(bootstrap_servers=BROKER, isolation_level="read_committed",
                      enable_auto_commit=False, consumer_timeout_ms=4000)
    tp = TopicPartition(topic, 0)
    c.assign([tp])
    c.seek_to_beginning(tp)
    end = c.end_offsets([tp])[tp]
    out = []
    for m in c:
        out.append((m.offset, m.key.decode(), None if m.value is None else m.value.decode()))
        if c.position(tp) >= end:
            break
    c.close()
    return out


def check(model, read, label):
    offsets = [o for o, _, _ in read]
    assert offsets == sorted(set(offsets)), f"{label}: offsets not strictly increasing"
    for off, key, value in read:
        assert off not in model.hidden, f"{label}: aborted or rolled-back record visible at {off}"
        assert model.committed.get(off) == (key, value), (
            f"{label}: offset {off} reads {key}={value!r}, "
            f"acknowledged {model.committed.get(off)} (I9)"
        )
    present = {}
    for off, key, value in read:
        present.setdefault(key, []).append((off, value))
    for key, (off, value) in model.latest().items():
        seen = present.get(key, [])
        if value is not None:
            assert (off, value) in seen, f"{label}: newest value of {key} at {off} is gone"
        elif (off, None) not in seen:
            # The tombstone went: then nothing older of this key may remain (I10).
            assert not seen, f"{label}: tombstone of {key} gone while {seen} still readable"


@pytest.mark.parametrize("seed", SEEDS)
def test_compaction_agrees_with_the_model(hunt_topic, seed):
    rng = random.Random(seed)
    segment = engine() == "segment"
    topic = hunt_topic
    model = Model()
    plain = KafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=0)
    txn = KafkaProducer(bootstrap_servers=BROKER, acks="all",
                        transactional_id=f"hunt-{topic}", linger_ms=0)
    txn.init_transactions()

    def put(key, value):
        v = None if value is None else (value + PAD).encode()
        md = plain.send(topic, key=key.encode(), value=v).get(timeout=30)
        model.committed[md.offset] = (key, None if value is None else value + PAD)

    ops = ["put"] * 6 + ["tomb", "txn_commit", "txn_abort", "clean", "clean_undone"]
    if segment:
        ops += ["sql", "sql_rolled_back"]
    for step in range(STEPS):
        op = rng.choice(ops)
        key = rng.choice(KEYS)
        label = f"seed {seed} step {step} ({op})"
        if op == "put":
            put(key, f"{key}-{step}")
        elif op == "tomb":
            put(key, None)
        elif op in ("txn_commit", "txn_abort"):
            txn.begin_transaction()
            sent = []
            for i in range(rng.randint(1, 3)):
                k = rng.choice(KEYS)
                v = f"{k}-t{step}-{i}{PAD}"
                sent.append((txn.send(topic, key=k.encode(), value=v.encode()), k, v))
            txn.flush()
            done = [(f.get(timeout=30).offset, k, v) for f, k, v in sent]
            if op == "txn_commit":
                txn.commit_transaction()
                for off, k, v in done:
                    model.committed[off] = (k, v)
            else:
                txn.abort_transaction()
                model.hidden.update(off for off, _, _ in done)
        elif op == "sql":
            v = f"{key}-s{step}"
            off = int(sql(f"SELECT kafgres_produce('{topic}', '{key}', '{v}')"))
            model.committed[off] = (key, v)
        elif op == "sql_rolled_back":
            out = sql(f"BEGIN; SAVEPOINT s; SELECT kafgres_produce('{topic}', '{key}', 'gone'); "
                      "ROLLBACK TO SAVEPOINT s; COMMIT;")
            model.hidden.update(int(line) for line in out.split() if line.strip().isdigit())
        elif op == "clean":
            sql("SELECT kafgres_enforce_retention()")
            check(model, read_all(topic), label)
        elif op == "clean_undone":
            sql("BEGIN; SAVEPOINT s; SELECT kafgres_enforce_retention(); "
                "ROLLBACK TO SAVEPOINT s; COMMIT;")
            check(model, read_all(topic), label)

    sql("SELECT kafgres_enforce_retention()")
    final = read_all(topic)
    check(model, final, f"seed {seed} final")
    assert len(final) < len(model.committed), "nothing was cleaned, so the model proved nothing"
    plain.close()
    txn.close()
