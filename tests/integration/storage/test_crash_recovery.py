"""The segment engine under `kill -9`: everything acked is still there, nothing corrupt.

The table engine gets this from Postgres — a crash rolls back the transaction the append
lived in, and there is nothing to reconcile. The segment engine writes payload to files
outside transaction control, so it has to earn it: the tail is scanned forward, batch
CRCs are the boundary check, and anything past the first invalid boundary is truncated.

The test that matters is not "the broker restarts". It is **every offset the broker
acknowledged is readable afterwards, with the same bytes**. A producer that got an offset
back has told the application the record exists; losing it is silent data loss with a
committed decision behind it.

What is deliberately *not* asserted: that unacked records survive. `acks=1` on this engine
means "in the page cache", not "fsynced", exactly as it does in Kafka with its default
flush settings — so a record in flight at the moment of the kill may or may not be there,
and either is correct.
"""

import json
import os
import subprocess

import pytest
from support.docker import REPO, compose, query as sql, CLIENTS

BROKER = "127.0.0.1:9092"
TOPIC = "crash-rig"

def engine():
    return sql("SHOW kafgres.storage_engine")

pytestmark = pytest.mark.skipif(
    engine() != "segment",
    reason="engine B only: ALTER SYSTEM SET kafgres.storage_engine='segment'",
)

def wait_ready(timeout_s=120):
    import time
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if compose("exec", "-T", "postgres", "pg_isready", "-U", "postgres").returncode == 0:
            if sql("SELECT count(*) FROM pg_stat_activity "
                   "WHERE backend_type='kafgres_broker'") == "1":
                return True
        time.sleep(2)
    return False

PRODUCER = r"""
import json, sys
from kafka import KafkaProducer
topic = sys.argv[1]
p = KafkaProducer(bootstrap_servers="127.0.0.1:9092", acks=1, linger_ms=0)
acked = []
try:
    for i in range(10000):
        value = f"crash-{i:06d}".encode()
        md = p.send(topic, value).get(timeout=10)
        acked.append([md.offset, value.decode()])
        if i % 25 == 0:
            print(json.dumps(acked), flush=True)
            acked_flushed = True
except Exception:
    pass
print(json.dumps(acked), flush=True)
"""

def test_everything_acked_survives_kill_9():
    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")
    sql(f"SELECT kafgres_create_topic('{TOPIC}', 1)")

    proc = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "python3", "-c", PRODUCER, TOPIC],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    assert proc.stdout is not None

    import time
    acked = []
    deadline = time.time() + 25
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if len(parsed) > len(acked):
            acked = parsed
        if len(acked) >= 200:
            break

    assert len(acked) >= 50, f"producer did not get going: {len(acked)} acked"

    compose("kill", "-s", "SIGKILL", "postgres", check=True)
    proc.kill()
    compose("up", "-d", "postgres", check=True, timeout=300)
    assert wait_ready(), "broker did not come back after SIGKILL"

    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", TOPIC, "-C", "-e", "-q", "-o", "beginning",
         "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=120,
    )
    got = {}
    for line in out.stdout.splitlines():
        if "\t" not in line:
            continue
        off, value = line.split("\t", 1)
        got[int(off)] = value

    missing = [(o, v) for o, v in acked if o not in got]
    assert not missing, (
        f"{len(missing)} acked records lost across SIGKILL, first {missing[:3]}. "
        "An acked offset that does not read back is silent data loss."
    )
    wrong = [(o, v, got[o]) for o, v in acked if got[o] != v]
    assert not wrong, f"acked records came back with different bytes (I2): {wrong[:3]}"

    offsets = sorted(got)
    assert offsets == list(range(offsets[0], offsets[0] + len(offsets))), (
        "offsets are not dense after recovery; the tail scan resumed at the wrong place"
    )

    subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", TOPIC, "-P"],
        input="after-recovery\n", capture_output=True, text=True, timeout=60,
    )
    after = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", TOPIC, "-C", "-e", "-q", "-o", str(offsets[-1]),
         "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=120,
    )
    tail = [l.split("\t", 1) for l in after.stdout.splitlines() if "\t" in l]
    assert tail and tail[-1][1] == "after-recovery", (
        f"produce after recovery did not land at the tail: {tail[-3:]}"
    )
    assert int(tail[-1][0]) == offsets[-1] + 1, (
        f"expected the next offset to be {offsets[-1] + 1}, got {tail[-1][0]} — "
        "the counter was reconstructed at the wrong position"
    )

    sql(f"SELECT kafgres_drop_topic('{TOPIC}')")

def test_a_java_idempotent_producer_carries_on_after_sigkill():
    """A crash can lose the producer window's newest rows. The Java client must recover from
    the UNKNOWN_PRODUCER_ID answers, not retry until `delivery.timeout.ms` expires them."""
    import time
    topic = "crash-java"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 2)")
    perf = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", "apache/kafka:4.1.0",
         "/opt/kafka/bin/kafka-producer-perf-test.sh", "--topic", topic,
         "--num-records", "600000", "--record-size", "256", "--throughput", "30000",
         "--producer-props", f"bootstrap.servers={BROKER}", "acks=all",
         "enable.idempotence=true", "linger.ms=5", "delivery.timeout.ms=60000"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        time.sleep(8)
        compose("kill", "-s", "SIGKILL", "postgres", check=True)
        compose("up", "-d", "postgres", check=True, timeout=300)
        assert wait_ready(), "broker did not come back after SIGKILL"
        out, _ = perf.communicate(timeout=300)
    finally:
        perf.kill()
        sql(f"SELECT kafgres_drop_topic('{topic}')")
    assert "TimeoutException" not in out, (
        "records expired retrying; the producer never recovered:\n" + out[-1500:]
    )
    summary = [line for line in out.splitlines() if "records sent" in line][-1]
    assert summary.startswith("600000 records sent"), summary
