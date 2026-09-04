"""`kafgres.max_request_bytes` — Kafka's `socket.request.max.bytes`.

librdkafka aggregates a produce request across partitions until it passes
`message.max.bytes` per partition (1 MB default), so a stock producer writing to eight
partitions can exceed a lower cap; the Java client honours `max.request.size` strictly
and never trips it.
"""
import subprocess
import time

import pytest

from conftest import sql

CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"
RECORDS = 20000

def set_cap(value):
    sql(f"ALTER SYSTEM SET kafgres.max_request_bytes = {value}")
    sql("SELECT pg_reload_conf()")
    for _ in range(20):
        time.sleep(0.5)
        if sql("SHOW kafgres.max_request_bytes").replace("MB", "").strip():
            break
    time.sleep(2)

def produce_wide(topic, timeout=180):
    """kcat with its own defaults — the point is that a stock client must work."""
    payload = "".join(f"k{i % 2000}:{i}{'x' * 992}\n" for i in range(RECORDS))
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-P", "-K:", "-l", "/dev/stdin"],
        input=payload, capture_output=True, text=True, timeout=timeout,
    )

def landed(topic):
    v = sql(f"SELECT COALESCE(SUM(offset_span),0) FROM kafgres_partition_offsets('{topic}')")
    return int(v) if v.isdigit() else 0

@pytest.fixture
def topic(request):
    name = f"fcap-{request.node.name.replace('_','-')[:32]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 8)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql("ALTER SYSTEM RESET kafgres.max_request_bytes")
    sql("SELECT pg_reload_conf()")
    time.sleep(2)

def test_a_stock_librdkafka_producer_across_eight_partitions(topic):
    produce_wide(topic)
    time.sleep(2)
    got = landed(topic)
    assert got == RECORDS, (
        f"{got} of {RECORDS} landed — the broker closed the connection on a request a "
        f"stock client produced and Kafka accepts"
    )

@pytest.mark.parametrize("cap", [2 * 1024 * 1024, 4 * 1024 * 1024])
def test_the_cap_still_bounds_a_frame_when_lowered(topic, cap):
    """The ceiling has to still be a ceiling, at the value it is set to."""
    set_cap(cap)
    produce_wide(topic)
    time.sleep(2)
    got = landed(topic)
    assert got < RECORDS, (
        f"all {RECORDS} landed at a {cap // (1024 * 1024)} MiB cap; the frame ceiling is "
        f"no longer enforced"
    )
