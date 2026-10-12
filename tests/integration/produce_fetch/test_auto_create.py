"""`kafgres.auto_create_topics` — Kafka's `auto.create.topics.enable`.

Kafka's own broker default is on, so refusing would be a divergence: producing to a
missing topic fails where upstream would create it. It is still a setting, because in a
Postgres extension "create a topic" means creating real tables or segment files.
"""
import struct
import subprocess
import time

import pytest

from conftest import sql

CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def produce(topic, value="v", timeout=90):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-P"],
        input=value, capture_output=True, text=True, timeout=timeout,
    )

def exists(topic):
    return sql(f"SELECT count(*) FROM kafgres_topics WHERE name = '{topic}'") != "0"

def set_auto_create(on):
    sql(f"ALTER SYSTEM SET kafgres.auto_create_topics = {'on' if on else 'off'}")
    sql("SELECT pg_reload_conf()")
    for _ in range(20):
        time.sleep(0.5)
        if sql("SHOW kafgres.auto_create_topics") == ("on" if on else "off"):
            return
    raise AssertionError("the setting never took")

@pytest.fixture
def clean():
    names = ["ac-producer", "ac-consumer", "ac-off"]
    for n in names:
        sql(f"SELECT kafgres_drop_topic('{n}')")
    yield names
    for n in names:
        sql(f"SELECT kafgres_drop_topic('{n}')")
    sql("ALTER SYSTEM RESET kafgres.auto_create_topics")
    sql("SELECT pg_reload_conf()")

def test_a_producer_creates_the_topic_it_writes_to(clean):
    set_auto_create(True)
    out = produce("ac-producer")
    assert out.returncode == 0, out.stderr[-400:]
    assert exists("ac-producer"), "the topic was not created"
    read = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         "-t", "ac-producer", "-C", "-e", "-q", "-o", "beginning"],
        capture_output=True, text=True, timeout=90,
    )
    assert read.stdout.strip() == "v", read.stdout

def test_a_consumer_that_opts_out_does_not_create_it(clean):
    """Both halves have to agree: the broker's setting *and* the client's flag."""
    set_auto_create(True)
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         "-t", "ac-consumer", "-C", "-e", "-q", "-o", "beginning",
         "-X", "allow.auto.create.topics=false"],
        capture_output=True, text=True, timeout=90,
    )
    assert not exists("ac-consumer"), (
        f"a consumer that opted out still created the topic: {out.stderr[-300:]}"
    )

def test_the_setting_off_restores_the_refusal(clean):
    set_auto_create(False)
    out = produce("ac-off")
    assert out.returncode != 0, "the produce succeeded with auto-create off"
    assert not exists("ac-off"), "the topic was created with auto-create off"

def test_a_denied_create_is_reported_as_denied_not_as_missing(clean):
    """A principal without CREATE must get a terminal error, not a retriable one.

    UNKNOWN_TOPIC_OR_PARTITION is retriable, so falling through to it makes the producer
    wait out `max.block.ms` and fail naming no ACL at all.
    """
    set_auto_create(True)
    sql("SET password_encryption='scram-sha-256'; DROP ROLE IF EXISTS acuser; "
        "CREATE ROLE acuser LOGIN PASSWORD 'pw-for-the-test'")
    sql("DELETE FROM kafgres_acls")
    sql("INSERT INTO kafgres_acls (principal, host, operation, permission, "
        "resource_type, resource_name, pattern_type) VALUES "
        "('User:acuser', '*', 'DESCRIBE', 'ALLOW', 'TOPIC', '*', 'LITERAL')")
    sql("ALTER SYSTEM SET kafgres.acls_enabled = on")
    sql("ALTER SYSTEM SET kafgres.sasl_required = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(2)
    try:
        out = subprocess.run(
            ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
             "kcat", "-b", BROKER, "-t", "ac-denied", "-P",
             "-X", "security.protocol=SASL_PLAINTEXT",
             "-X", "sasl.mechanisms=SCRAM-SHA-256",
             "-X", "sasl.username=acuser", "-X", "sasl.password=pw-for-the-test"],
            input="v", capture_output=True, text=True, timeout=120,
        )
        combined = out.stdout + out.stderr
        assert "Authorization" in combined or "authoriz" in combined.lower(), (
            f"a denial was not reported as one; the client saw: {combined[-500:]}"
        )
        assert not exists("ac-denied"), "the topic was created despite the denial"
    finally:
        sql("ALTER SYSTEM RESET kafgres.acls_enabled")
        sql("ALTER SYSTEM RESET kafgres.sasl_required")
        sql("SELECT pg_reload_conf()")
        sql("DELETE FROM kafgres_acls")
        sql("DROP ROLE IF EXISTS acuser")
        sql("SELECT kafgres_drop_topic('ac-denied')")
        time.sleep(2)

def metadata_naming(names):
    """One raw Metadata v1 request naming every topic.

    Raw, because `kcat -L -t a,b,c` names one topic called "a,b,c". v1 has no
    `allow_auto_topic_creation` field and Kafka treats pre-v4 as permitting it, so this
    is also the shape that most needs the cap.
    """
    import socket
    encoded = [n.encode() for n in names]
    sock = socket.create_connection((BROKER.split(":")[0], int(BROKER.split(":")[1])), 10)
    try:
        hdr = struct.pack(">hhi", 3, 1, 99) + struct.pack(">h", 5) + b"burst"
        body = struct.pack(">i", len(encoded)) + b"".join(
            struct.pack(">h", len(n)) + n for n in encoded
        )
        msg = hdr + body
        sock.sendall(struct.pack(">i", len(msg)) + msg)
        size = struct.unpack(">i", sock.recv(4))[0]
        got = b""
        while len(got) < size:
            got += sock.recv(size - len(got))
        return got
    finally:
        sock.close()

def test_one_request_cannot_create_an_unbounded_number_of_topics(clean):
    """Auto-creation runs DDL on the broker's single-threaded loop.

    `MAX_REQUESTED_TOPICS` bounds the *response* at 10,000 — far too loose to double as
    a bound on `CREATE TABLE` — and ten thousand fast statements never trip a
    per-statement `statement_timeout`, so the side effect is capped far lower and the
    overflow is answered with a retriable code.
    """
    set_auto_create(True)
    names = [f"ac-burst-{i}" for i in range(40)]
    for n in names:
        sql(f"SELECT kafgres_drop_topic('{n}')")
    try:
        metadata_naming(names)
        made = sum(1 for n in names if exists(n))
        assert made > 0, "no topics were created, so the cap was never exercised"
        assert made <= 16, f"one metadata request created {made} topics"
    finally:
        for n in names:
            sql(f"SELECT kafgres_drop_topic('{n}')")
