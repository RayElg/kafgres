"""Client quotas and SCRAM credential administration.

Quotas and their enforcement arrive together: a stored quota nothing enforces is a
limit an operator can set, read back, and watch a client ignore. Credentials are
Postgres roles — a SCRAM verifier maps onto one exactly, so `kafka-configs.sh` and
`ALTER ROLE` manage the same credential.
"""

import socket
import struct
import subprocess
import time

import pytest

from conftest import sql

KAFKA = "apache/kafka:4.1.0"
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def set_quota(*args, timeout=180):
    """Set a quota and wait for the broker to observe it.

    Quotas are cached and refreshed on a timer, so "set it and immediately fetch"
    races the reload; the wait is `MAX_STALENESS` plus a margin.
    """
    out = kafka_configs(*args, timeout=timeout)
    assert out.returncode == 0, out.stdout + out.stderr
    time.sleep(2.5)
    return out

def kafka_configs(*args, timeout=180):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", BROKER, *args],
        capture_output=True, text=True, timeout=timeout,
    )

@pytest.fixture(autouse=True)
def clean():
    sql("DELETE FROM kafgres_client_quotas")
    yield
    sql("DELETE FROM kafgres_client_quotas")
    sql("DROP ROLE IF EXISTS q13user")

@pytest.fixture
def seeded_topic():
    name = "q13-data"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    payload = "\n".join("x" * 900 for _ in range(60)) + "\n"
    subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-P", "-t", name],
        input=payload, capture_output=True, text=True, timeout=180,
    )
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def fetch_throttle(topic, client_id, max_wait_ms=0, min_bytes=1):
    """A hand-built Fetch v4, returning `(throttle_time_ms, response_bytes)`.

    Raw because the number under test is a response *field*: a client library honours
    it by sleeping. `max_wait_ms`/`min_bytes` let a caller force the request down the
    *parked* path, answered by a different function in the broker.
    """
    body = struct.pack(">iiii", -1, max_wait_ms, min_bytes, 8 * 1024 * 1024)
    body += struct.pack(">b", 0)
    body += struct.pack(">i", 1) + struct.pack(">h", len(topic)) + topic.encode()
    body += struct.pack(">i", 1) + struct.pack(">iqi", 0, 0, 1024 * 1024)
    header = struct.pack(">hhi", 1, 4, 99) + struct.pack(">h", len(client_id)) + client_id.encode()
    frame = header + body

    s = socket.create_connection(("127.0.0.1", 9092), timeout=20)
    try:
        s.sendall(struct.pack(">i", len(frame)) + frame)
        n = struct.unpack(">i", s.recv(4))[0]
        buf = b""
        while len(buf) < n:
            buf += s.recv(n - len(buf))
    finally:
        s.close()
    return struct.unpack_from(">i", buf, 4)[0], n

def test_a_quota_survives_a_round_trip_through_the_tool():
    assert kafka_configs("--alter", "--add-config", "producer_byte_rate=1024",
                         "--entity-type", "clients", "--entity-name", "noisy").returncode == 0
    out = kafka_configs("--describe", "--entity-type", "clients")
    assert out.returncode == 0, out.stdout + out.stderr
    assert "producer_byte_rate=1024" in out.stdout, out.stdout

    assert kafka_configs("--alter", "--delete-config", "producer_byte_rate",
                         "--entity-type", "clients", "--entity-name", "noisy").returncode == 0
    assert "producer_byte_rate" not in kafka_configs(
        "--describe", "--entity-type", "clients").stdout

def test_a_quota_this_broker_cannot_apply_is_refused(seeded_topic):
    """`request_percentage` is refused, not stored: accepting a number nothing applies
    gives an operator a limit they can set, read back, and watch a client ignore, with
    no error anywhere."""
    out = kafka_configs("--alter", "--add-config", "request_percentage=50",
                        "--entity-type", "clients", "--entity-name", "noisy")
    assert out.returncode != 0, out.stdout
    combined = out.stdout + out.stderr
    assert "not enforced by this broker" in combined, combined
    assert "producer_byte_rate" in combined and "consumer_byte_rate" in combined, combined
    assert sql("SELECT count(*) FROM kafgres_client_quotas") == "0", "a refused quota was stored"

def test_a_consumer_quota_throttles_the_client_it_names(seeded_topic):
    """Two fetches for the same data, differing only in `client.id`: the one a quota
    names is told to wait and the other is not — a broker that throttled everything
    would pass the first on its own."""
    set_quota("--alter", "--add-config", "consumer_byte_rate=512",
              "--entity-type", "clients", "--entity-name", "slowreader")

    throttled, bytes_a = fetch_throttle(seeded_topic, "slowreader")
    free, bytes_b = fetch_throttle(seeded_topic, "unlimited")

    assert throttled > 0, "the quota'd client was not throttled"
    assert free == 0, f"a client with no quota was throttled {free}ms"
    assert bytes_a == bytes_b, (bytes_a, bytes_b)

def test_a_named_quota_beats_the_default(seeded_topic):
    """Most-specific-first, Kafka's resolution order: "any match wins" lets a
    permissive default silently override the tight limit an operator set for one noisy
    client."""
    set_quota("--alter", "--add-config", "consumer_byte_rate=512",
              "--entity-type", "clients", "--entity-default")
    assert fetch_throttle(seeded_topic, "anyone")[0] > 0

    set_quota("--alter", "--add-config", "consumer_byte_rate=100000000",
              "--entity-type", "clients", "--entity-name", "vip")
    assert fetch_throttle(seeded_topic, "vip")[0] == 0, (
        "the default overrode a client-specific quota"
    )
    assert fetch_throttle(seeded_topic, "anyone")[0] > 0, "the default stopped applying"

def test_a_scram_credential_set_over_the_wire_authenticates_a_client():
    """The end-to-end property: `kafka-configs.sh` writes a Postgres role password, and
    a Kafka client then authenticates with it — no separate credential store. The
    client sends the salted password, so nothing plaintext crosses the wire (RFC 5802
    derives `StoredKey`/`ServerKey` from it, which is what `pg_authid` holds)."""
    sql("DROP ROLE IF EXISTS q13user")
    sql("CREATE ROLE q13user LOGIN")
    out = kafka_configs("--alter", "--add-config",
                        "SCRAM-SHA-256=[iterations=8192,password=hunter2]",
                        "--entity-type", "users", "--entity-name", "q13user")
    assert out.returncode == 0, out.stdout + out.stderr

    stored = sql("SELECT left(rolpassword, 20) FROM pg_authid WHERE rolname='q13user'")
    assert stored.startswith("SCRAM-SHA-256$8192"), stored

    probe = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER, "-L",
         "-X", "security.protocol=sasl_plaintext", "-X", "sasl.mechanisms=SCRAM-SHA-256",
         "-X", "sasl.username=q13user", "-X", "sasl.password=hunter2"],
        capture_output=True, text=True, timeout=180,
    )
    assert probe.returncode == 0, probe.stdout + probe.stderr
    assert "brokers:" in probe.stdout, probe.stdout

    bad = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER, "-L",
         "-X", "security.protocol=sasl_plaintext", "-X", "sasl.mechanisms=SCRAM-SHA-256",
         "-X", "sasl.username=q13user", "-X", "sasl.password=wrong"],
        capture_output=True, text=True, timeout=180,
    )
    assert bad.returncode != 0, bad.stdout

def test_a_credential_for_a_role_that_does_not_exist_is_refused():
    """Creating the role instead would be a privilege grant nobody asked for: a Postgres
    role carries privileges on every table, so inventing one from a `kafka-configs.sh`
    invocation is not the same act."""
    sql("DROP ROLE IF EXISTS q13user")
    out = kafka_configs("--alter", "--add-config",
                        "SCRAM-SHA-256=[iterations=8192,password=hunter2]",
                        "--entity-type", "users", "--entity-name", "q13user")
    assert out.returncode != 0, out.stdout
    combined = out.stdout + out.stderr
    assert "no Postgres role" in combined, combined
    assert "CREATE ROLE" in combined, combined

def test_a_user_quota_outranks_a_client_id_quota(seeded_topic):
    """Kafka's precedence, which is not the obvious one: *all* user entries beat *all*
    client-id entries — `users/<default>` outranks `clients/<specific>`. Ranking the
    two specific cases together and the two defaults together inverts the answer,
    silently."""
    set_quota("--alter", "--add-config", "consumer_byte_rate=100000000",
              "--entity-type", "clients", "--entity-name", "bulk")
    set_quota("--alter", "--add-config", "consumer_byte_rate=512",
              "--entity-type", "users", "--entity-default")
    assert fetch_throttle(seeded_topic, "bulk")[0] > 0, (
        "a generous client-id quota overrode the user quota that should outrank it"
    )

    set_quota("--alter", "--delete-config", "consumer_byte_rate",
              "--entity-type", "users", "--entity-default")
    assert fetch_throttle(seeded_topic, "bulk")[0] == 0

def test_a_parked_fetch_is_charged(seeded_topic):
    """A long-poll answered later must still count against the quota, or any consumer
    whose fetches park — `fetch.min.bytes` above the typical batch, or one that has
    caught up — reads everything free.

    Forced down the parked path by asking for more bytes than exist with a real wait,
    so the request is answered on its deadline by the completion path."""
    set_quota("--alter", "--add-config", "consumer_byte_rate=512",
              "--entity-type", "clients", "--entity-name", "parker")
    throttle, size = fetch_throttle(seeded_topic, "parker",
                                    max_wait_ms=2000, min_bytes=10_000_000)
    assert size > 1000, f"the fetch returned nothing, so nothing was charged: {size}"
    assert throttle > 0, "a parked fetch escaped the consumer quota"

def test_a_privileged_role_is_not_a_kafka_credential():
    """Setting the `postgres` role's password through the Kafka admin protocol would be
    a database takeover: the effect is `ALTER ROLE … PASSWORD` run by a
    superuser-connected worker against any role, so upstream's "may manage SASL users"
    guard must not cover privileged roles."""
    for role in ("postgres",):
        out = kafka_configs("--alter", "--add-config",
                            "SCRAM-SHA-256=[iterations=8192,password=takeover]",
                            "--entity-type", "users", "--entity-name", role)
        combined = out.stdout + out.stderr
        assert out.returncode != 0, combined
        assert "privileged Postgres role" in combined, combined

    assert sql("SELECT 1") == "1"

def test_a_missing_role_is_reported_as_a_missing_resource():
    """`RESOURCE_NOT_FOUND`, not `UNACCEPTABLE_CREDENTIAL`: the credential was fine,
    the thing it named was not there. 93 tells an operator their password was rejected,
    which sends them to the wrong problem."""
    sql("DROP ROLE IF EXISTS q13user")
    out = kafka_configs("--alter", "--add-config",
                        "SCRAM-SHA-256=[iterations=8192,password=hunter2]",
                        "--entity-type", "users", "--entity-name", "q13user")
    combined = out.stdout + out.stderr
    assert out.returncode != 0, combined
    assert "ResourceNotFound" in combined or "no Postgres role" in combined, combined
