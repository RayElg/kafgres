"""Admin APIs a real client asks for.

These are conformance rather than capability: the data was already there in every case,
and what was missing was the RPC that hands it over. The distinction matters because the
failure is not an error a user can act on — a UI shows sizes as unknown, or a tab as
empty, and it reads as a broken broker rather than an unimplemented API.

Driven by the real Java tools where one exists, because a tool proves what a hand-built
frame cannot: that an unmodified client is satisfied by the answer.
"""

import json
import subprocess

import pytest

from conftest import sql

CLIENTS = "kafgres-clients"
KAFKA = "apache/kafka:4.1.0"
BROKER = "127.0.0.1:9092"

def kafka_tool(script, *args, timeout=300):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         f"/opt/kafka/bin/{script}", "--bootstrap-server", BROKER, *args],
        capture_output=True, text=True, timeout=timeout,
    )

def kcat(*args, stdin=None, timeout=180):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER, *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

@pytest.fixture
def topic(request):
    name = f"adm-{request.node.name.replace('_', '-')[:36]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def log_dirs(*args):
    """`kafka-log-dirs.sh --describe`, parsed. It prints a banner line then one JSON line."""
    out = kafka_tool("kafka-log-dirs.sh", "--describe", *args)
    assert out.returncode == 0, out.stdout + out.stderr
    line = next(ln for ln in out.stdout.splitlines() if ln.strip().startswith("{"))
    return json.loads(line)

def test_log_dirs_reports_a_size_that_tracks_the_data(topic):
    """The size has to be a real measurement, not a plausible constant.

    Redpanda Console calls DescribeLogDirs on every refresh; without it every topic and
    partition size showed as unknown and the broker logged "broker is too old" each time.
    A handler that returned zeros would silence the error and keep the column wrong, so
    this asserts the number *moves* with the data rather than merely existing.
    """
    before = log_dirs("--topic-list", topic)
    sizes = before["brokers"][0]["logDirs"][0]["partitions"]
    assert [p["partition"] for p in sizes] == [f"{topic}-0"], sizes
    assert sizes[0]["size"] == 0, "a fresh partition is not empty"

    payload = "".join(f"{'x' * 300}\n" for _ in range(50))
    assert kcat("-t", topic, "-P", stdin=payload).returncode == 0

    after = log_dirs("--topic-list", topic)["brokers"][0]["logDirs"][0]["partitions"][0]
    assert after["size"] > 10000, f"size did not track the data written: {after}"

def test_the_log_dir_is_a_path_that_exists(topic):
    """A UI shows this to an operator who may go looking for it.

    Kafka answers with `log.dirs`; there is no such setting here, and a plausible-looking
    invented path would be a lie an operator only discovers by `cd`-ing to it. Both
    engines' answers are truthful about where their log lives.
    """
    reported = log_dirs()["brokers"][0]["logDirs"][0]["logDir"]
    assert reported.startswith("/"), reported
    listed = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "test", "-d", reported],
        capture_output=True, text=True, timeout=60,
    )
    assert listed.returncode == 0, f"reported log dir does not exist: {reported}"

    engine = sql("SHOW kafgres.storage_engine")
    if engine == "segment":
        assert reported.endswith("/kafgres"), reported

def test_an_unfiltered_request_covers_every_topic(topic):
    """`null` topics means all of them, and an empty list means none.

    Getting that backwards is not a crash, it is a UI showing one broker's whole disk usage
    filed under a topic nobody selected.
    """
    other = f"{topic}-other"
    sql(f"SELECT kafgres_drop_topic('{other}')")
    sql(f"SELECT kafgres_create_topic('{other}', 1)")
    try:
        everything = log_dirs()["brokers"][0]["logDirs"][0]["partitions"]
        names = {p["partition"] for p in everything}
        assert {f"{topic}-0", f"{other}-0"} <= names, names

        just_one = log_dirs("--topic-list", topic)["brokers"][0]["logDirs"][0]["partitions"]
        assert [p["partition"] for p in just_one] == [f"{topic}-0"]
    finally:
        sql(f"SELECT kafgres_drop_topic('{other}')")

def legacy_alter(topic, pairs="", broker=BROKER):
    """Drive the pre-KIP-339 AlterConfigs. See legacy_alter_configs.py for why."""
    here = subprocess.run(["pwd"], capture_output=True, text=True).stdout.strip()
    script = f"{here}/tests/integration/legacy_alter_configs.py"
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "-v", f"{script}:/t.py",
         CLIENTS, "python3", "/t.py", broker, topic, pairs],
        capture_output=True, text=True, timeout=180,
    )

def stored_config(topic):
    return sql(f"SELECT config FROM kafgres_topics WHERE name = '{topic}'").strip()

def test_legacy_alter_configs_replaces_rather_than_merges(topic):
    """The two config APIs mean different things and this is the difference.

    `IncrementalAlterConfigs` applies operations and leaves everything unmentioned alone.
    `AlterConfigs` takes the entries as the *complete* desired state, so a config the
    request omits goes back to its default. Implementing this as a synonym for the
    incremental version is the more natural reading of the name and silently keeps
    overrides the client asked to drop.

    Checked against a real broker rather than against my reading of the protocol: Kafka
    4.1.0 given the same sequence also drops `retention.bytes`.
    """
    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config",
                      "retention.ms=60000,retention.bytes=1048576").returncode == 0
    assert "1048576" in stored_config(topic)

    out = legacy_alter(topic, "retention.ms=90000")
    assert "code=0" in out.stdout, out.stdout + out.stderr

    after = stored_config(topic)
    assert "90000" in after, after
    assert "1048576" not in after, f"a config the request omitted survived: {after}"

def test_legacy_alter_configs_still_refuses_a_bad_value(topic):
    """The new door gets the same validation as the old one.

    A second write path is a second chance to store a value nothing checked — which for
    retention.ms means a worker that silently falls back to seven days while the operator
    has a transcript saying otherwise.
    """
    out = legacy_alter(topic, "retention.ms=forever")
    assert "code=0" not in out.stdout, out.stdout
    assert "not a valid retention.ms" in out.stdout, out.stdout
    assert stored_config(topic) == "{}", stored_config(topic)

def test_legacy_alter_configs_validates_like_the_incremental_one(topic):
    """The old door gets the same validation as the new one.

    `cleanup.policy` became writable when compaction landed, so `delete` and `compact` are
    both real changes here rather than no-ops. What must still hold is that a value the
    broker cannot honour is refused through this door too — a second write path is a second
    chance to store something nothing enforces.
    """
    out = legacy_alter(topic, "cleanup.policy=delete")
    assert "code=0" in out.stdout, out.stdout + out.stderr

    bad = legacy_alter(topic, "cleanup.policy=nonsense")
    assert "code=0" not in bad.stdout, bad.stdout

    ro = legacy_alter(topic, "compression.type=zstd")
    assert "code=0" not in ro.stdout, ro.stdout

def txn_tool(*args, broker=BROKER):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-transactions.sh", "--bootstrap-server", broker, *args],
        capture_output=True, text=True, timeout=300,
    )

segment_only = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "segment",
    reason="engine B only: kafgres serves Kafka transactions on the segment engine",
)

def run_txn(topic, outcome="commit"):
    """Sarama, because kafka-python-ng has no transaction support at all."""
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "sarama-conformance", BROKER, f"txn-{outcome}", topic],
        capture_output=True, text=True, timeout=300,
    )

@segment_only
def test_a_completed_transaction_is_listed_in_kafkas_vocabulary(topic):
    """The state strings are a fixed set clients match on.

    kafgres stores 'ongoing'/'committed'/'aborted'; the wire wants
    Ongoing/CompleteCommit/CompleteAbort. A UI colours a row by that string, so our spelling
    would simply not be recognised.
    """
    sql("DELETE FROM kafgres_txns")
    assert run_txn(topic).stdout.strip().endswith("commit"), "the transaction did not run"

    out = txn_tool("list")
    assert out.returncode == 0, out.stdout + out.stderr
    assert "CompleteCommit" in out.stdout, out.stdout
    assert "committed" not in out.stdout, f"internal state name reached the wire: {out.stdout}"

    described = txn_tool("describe", "--transactional-id", "kafgres-eos-test")
    assert described.returncode == 0, described.stdout + described.stderr
    assert "CompleteCommit" in described.stdout, described.stdout

def test_an_unknown_transactional_id_gets_the_code_kafka_uses():
    """The number came off the wire rather than out of memory.

    A v0 DescribeTransactions for a made-up id against Kafka 4.1.0 answers 105
    (TRANSACTIONAL_ID_NOT_FOUND). The tool renders it as "the ID could not be found"; only
    the numeric code tells a client what to do, and a wrong one here would have the client
    retrying or giving up on something that means nothing of the sort.
    """
    out = txn_tool("describe", "--transactional-id", "no-such-transactional-id")
    assert out.returncode != 0
    combined = out.stdout + out.stderr
    assert "could not be found" in combined, combined

@segment_only
def test_an_idempotent_producer_is_not_reported_as_hanging(topic):
    """`current_txn_start_offset` is -1 when there is no open transaction.

    The field means "the offset this producer's *open* transaction began at". Reporting
    the last batch's base offset instead — the obvious-looking thing to put there —
    makes every ordinary idempotent producer look mid-transaction, so `find-hanging`
    reports hangs that do not exist. A diagnostic that invents the fault it exists to
    find is worse than one that is missing.
    """
    payload = "".join(f"m{i}\n" for i in range(20))
    assert kcat("-t", topic, "-P", stdin=payload).returncode == 0
    run_txn(topic)

    out = txn_tool("find-hanging", "--broker-id", "1")
    assert out.returncode == 0, out.stdout + out.stderr
    body = [ln for ln in out.stdout.splitlines() if ln.strip() and not ln.startswith("Topic")]
    assert body == [], f"an idempotent producer was reported as a hanging transaction: {body}"

def test_an_empty_partition_list_means_none_not_all():
    """The tempting reading is "no filter means everything". It is wrong.

    Kafka 4.1.0, asked about a named topic with an empty partition list, answers with zero
    topics and zero partitions — not with the topic's whole contents. Reading it the other
    way would make a UI show a broker's entire disk usage filed under a topic nobody
    selected. No Java tool can ask this, because `kafka-log-dirs.sh` always sends explicit
    partitions, so it goes through a raw frame.
    """
    name = "adm-empty-partition-list"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 3)")
    try:
        out = subprocess.run(
            ["python3", "tests/integration/probe_log_dirs.py", "127.0.0.1", "9092", name],
            capture_output=True, text=True, timeout=60,
        )
        assert out.returncode == 0, out.stdout + out.stderr
        assert "topics=0 partitions=0" in out.stdout, out.stdout
        assert "err=0" in out.stdout, out.stdout
    finally:
        sql(f"SELECT kafgres_drop_topic('{name}')")

def test_leader_election_reports_that_nothing_needed_doing(topic):
    """One broker means the preferred replica is always already the leader.

    Both shapes are pinned because they differ, and both were checked against the 4.3.1
    reference before being implemented: naming a partition returns `ELECTION_NOT_NEEDED`,
    which the tool renders as "Valid replica already elected"; asking for *all* partitions
    returns an empty result list, which it renders as nothing at all. Returning
    `ELECTION_NOT_NEEDED` for every partition in the cluster would also be "true" and
    would not match — the reference reports only partitions that needed an election.
    """
    out = kafka_tool("kafka-leader-election.sh", "--election-type", "preferred",
                     "--topic", topic, "--partition", "0")
    assert out.returncode == 0, out.stdout + out.stderr
    assert "already elected" in out.stdout, out.stdout + out.stderr

    every = kafka_tool("kafka-leader-election.sh", "--election-type", "preferred",
                       "--all-topic-partitions")
    assert every.returncode == 0, every.stdout + every.stderr
    assert "already elected" not in every.stdout, (
        f"an all-partitions election reported per-partition results; the reference "
        f"returns an empty list: {every.stdout}"
    )

def test_leader_election_on_a_topic_that_does_not_exist(topic):
    """Still an error, and the right one — the API existing must not make it agreeable."""
    out = kafka_tool("kafka-leader-election.sh", "--election-type", "preferred",
                     "--topic", f"{topic}-absent", "--partition", "0")
    assert "UnknownTopicOrPartition" in out.stdout + out.stderr, out.stdout + out.stderr

def test_leader_election_range_checks_the_partition(topic):
    """A partition index past the end is unknown, not healthy: the topic-level test
    above passes either way, which is why this one exists separately."""
    out = kafka_tool("kafka-leader-election.sh", "--election-type", "preferred",
                     "--topic", topic, "--partition", "5")
    combined = out.stdout + out.stderr
    assert "UnknownTopicOrPartition" in combined, combined
    assert "already elected" not in combined, (
        f"a partition that does not exist was reported as healthy: {combined}"
    )

def test_the_users_page_lists_postgres_roles_that_can_authenticate():
    """SASL identities here *are* Postgres roles, so this reports what really exists;
    without the API a console's users page raises `UnsupportedVersionException`, which
    reads as "this broker is too old" on a broker that does support SASL."""
    sql("SET password_encryption='scram-sha-256'; DROP ROLE IF EXISTS scramtest; "
        "CREATE ROLE scramtest LOGIN PASSWORD 'pw-for-the-test'")
    try:
        out = kafka_tool("kafka-configs.sh", "--entity-type", "users", "--describe")
        assert out.returncode == 0, out.stdout + out.stderr
        assert "scramtest" in out.stdout, out.stdout
        assert "SCRAM-SHA-256" in out.stdout, out.stdout

        sql("ALTER ROLE scramtest NOLOGIN")
        out = kafka_tool("kafka-configs.sh", "--entity-type", "users", "--describe")
        assert "scramtest" not in out.stdout, (
            f"a NOLOGIN role is still listed as a SCRAM credential: {out.stdout}"
        )
    finally:
        sql("DROP ROLE IF EXISTS scramtest")

def test_the_users_page_never_returns_a_verifier():
    """Mechanism and iteration count only — the stored key and salt stay in pg_authid.

    Kafka's API exposes exactly those two fields, and the reason to assert it is that the
    query this is built on selects `rolpassword` to parse the iteration count out of it.
    A refactor that passed the parsed struct straight through would leak the verifier to
    anyone with cluster DESCRIBE.
    """
    sql("SET password_encryption='scram-sha-256'; DROP ROLE IF EXISTS leaky; "
        "CREATE ROLE leaky LOGIN PASSWORD 'pw-for-the-test'")
    try:
        stored = sql("SELECT rolpassword FROM pg_authid WHERE rolname='leaky'")
        assert stored.startswith("SCRAM-SHA-256$"), stored
        secret = stored.split("$")[-1]
        out = kafka_tool("kafka-configs.sh", "--entity-type", "users", "--describe")
        assert secret not in out.stdout, "the SCRAM verifier reached the wire"
        assert "iterations=" in out.stdout, out.stdout
    finally:
        sql("DROP ROLE IF EXISTS leaky")
