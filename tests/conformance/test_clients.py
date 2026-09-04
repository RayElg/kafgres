"""The conformance gate: four client libraries, one scenario set, diffed against Kafka.

The thesis this rests on: the schema is not the specification, the clients are. Every
behavioural difference from a real broker is catalogued, and each is either fixed or
recorded as an accepted deviation; an uncatalogued diff is a bug nobody has found yet.

Two halves:

* **Client matrix.** Each client runs the same scenarios against kafgres. This half runs
  everywhere and needs nothing but the broker.
* **Reference diff.** The same scenarios run against `apache/kafka` and the results are
  compared. Behind the `conformance` profile, because it costs a second broker:
  `docker compose --profile conformance up -d kafka`. Skipped, not failed, when absent —
  a developer without it still gets the matrix.

The diff is on *observable* output, not bytes. Two brokers may legitimately differ in
timing, partition assignment order or error text; what must match is what a program
using the client would decide.
"""

import os
import subprocess

import pytest

CLIENTS_IMAGE = "kafgres-clients"
KAFKA_IMAGE = "apache/kafka:4.3.1"
KAFGRES = "127.0.0.1:9092"
REFERENCE = "127.0.0.1:9292"
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

SCENARIOS = ["metadata", "produce-consume", "group-consume", "unknown-topic"]

def sql(query, timeout=60):
    """Raises on failure. Returning "" for a failed psql makes every guard written
    against the result vacuous — `assert affected != "0"` passes on the empty string, so
    a setup step that never ran reads as a setup step that did."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-v", "ON_ERROR_STOP=1", "-d", "postgres", "-tAc", query],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    if out.returncode != 0:
        raise RuntimeError(f"psql failed ({out.returncode}) for {query!r}: {out.stderr.strip()}")
    return out.stdout.strip()

def reference_available():
    try:
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
             "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", REFERENCE, "--list"],
            capture_output=True, text=True, timeout=90,
        )
        return out.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False

needs_reference = pytest.mark.skipif(
    not reference_available(),
    reason="reference broker not running: docker compose --profile conformance up -d kafka",
)

def run_client(client, broker, scenario, topic, timeout=240):
    """One scenario, one client, one broker. Returns the single result line."""
    cmd = {
        "sarama": ["sarama-conformance"],
        "kafka-python": ["kafka-python-conformance"],
    }[client]
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS_IMAGE,
         *cmd, broker, scenario, topic],
        capture_output=True, text=True, timeout=timeout,
    )
    for line in reversed((out.stdout + out.stderr).splitlines()):
        if line.startswith(("OK ", "ERROR ")):
            return line.strip()
    return f"ERROR no result line (rc={out.returncode}): {out.stdout[-200:]} {out.stderr[-200:]}"

def make_topic_kafgres(name):
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")

def drop_topic_kafgres(name):
    sql(f"SELECT kafgres_drop_topic('{name}')")

def make_topic_reference(name):
    subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
         "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", REFERENCE,
         "--create", "--topic", name, "--partitions", "1", "--if-not-exists"],
        capture_output=True, text=True, timeout=120,
    )

def drop_topic_reference(name):
    subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
         "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", REFERENCE,
         "--delete", "--topic", name, "--if-exists"],
        capture_output=True, text=True, timeout=120,
    )

@pytest.mark.parametrize("client", ["sarama", "kafka-python"])
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_client_scenario_against_kafgres(client, scenario):
    """Every client, every scenario, against us.

    Sarama and kafka-python are the two implementations that share no code with
    librdkafka or the Java client; the other two are covered by the kcat- and
    tool-driven tests elsewhere in the suite.
    """
    topic = f"conf-{client}-{scenario}"
    make_topic_kafgres(topic)
    try:
        result = run_client(client, KAFGRES, scenario, topic)
        assert result.startswith("OK "), f"{client}/{scenario}: {result}"
    finally:
        drop_topic_kafgres(topic)

@needs_reference
@pytest.mark.parametrize("client", ["sarama", "kafka-python"])
@pytest.mark.parametrize("scenario", ["metadata", "produce-consume", "group-consume"])
def test_the_same_scenario_agrees_with_a_real_broker(client, scenario):
    """The half that makes this a conformance suite rather than a regression suite.

    Comparing our behaviour to our own expectations only proves we are self-consistent.
    The reference is what turns "the client did not complain" into "the client saw what
    it would have seen from Kafka".
    """
    topic = f"conf-diff-{client}-{scenario}"
    make_topic_kafgres(topic)
    make_topic_reference(topic)
    try:
        ours = run_client(client, KAFGRES, scenario, topic)
        theirs = run_client(client, REFERENCE, scenario, topic)
        assert ours.startswith("OK "), f"{client}/{scenario} failed against kafgres: {ours}"
        assert theirs.startswith("OK "), f"{client}/{scenario} failed against kafka: {theirs}"
        assert ours == theirs, (
            f"{client}/{scenario} differs from the reference broker.\n"
            f"  kafgres: {ours}\n"
            f"  kafka:   {theirs}\n"
            "If this is intended, record it in docs/conformance.md as an accepted "
            "deviation with a reason. An uncatalogued diff is a bug."
        )
    finally:
        drop_topic_kafgres(topic)
        drop_topic_reference(topic)

@needs_reference
def test_the_unknown_topic_answer_agrees(scenario="unknown-topic"):
    """Split out because the topic must *not* exist on either broker, and because it is
    the case a broker most easily gets subtly wrong — omitting the entry rather than
    reporting it, which reads to a client as a topic that might yet appear."""
    for client in ("sarama", "kafka-python"):
        ours = run_client(client, KAFGRES, scenario, "unused")
        theirs = run_client(client, REFERENCE, scenario, "unused")
        assert ours.startswith("OK "), f"{client} failed against kafgres: {ours}"
        assert theirs.startswith("OK "), f"{client} failed against kafka: {theirs}"
        assert ours == theirs, (
            f"{client}/{scenario} differs.\n  kafgres: {ours}\n  kafka:   {theirs}"
        )

@needs_reference
def test_the_advertised_api_surface_is_a_subset_of_kafkas():
    """Every API we advertise, Kafka advertises at an overlapping version range.

    A range wider than Kafka's is a version no client has ever exercised against us and
    no reference exists for — which is the same "works with one client, hangs with
    another" failure as advertising an API with no handler.
    """
    def surface(broker):
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
             "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", broker],
            capture_output=True, text=True, timeout=180,
        )
        assert out.returncode == 0, out.stderr
        found = {}
        for line in out.stdout.splitlines():
            line = line.strip().rstrip(",")
            if "(" not in line or "):" not in line:
                continue
            _name, rest = line.split("(", 1)
            key, rest = rest.split("):", 1)
            if "UNSUPPORTED" in rest:
                continue
            span = rest.split("[")[0].strip()
            if " to " in span:
                lo, hi = span.split(" to ")
            else:
                lo = hi = span
            found[int(key)] = (int(lo), int(hi.strip()))
        return found

    ours, theirs = surface(KAFGRES), surface(REFERENCE)
    assert len(ours) >= 26, f"parsed {len(ours)} APIs from kafgres; the parser is broken"
    assert len(theirs) >= 70, f"parsed {len(theirs)} APIs from kafka; the parser is broken"
    wider = {
        key: (rng, theirs.get(key))
        for key, rng in ours.items()
        if key not in theirs or rng[1] > theirs[key][1] or rng[0] < theirs[key][0]
    }
    assert not wider, (
        "we advertise versions the reference broker does not:\n"
        + "\n".join(f"  api {k}: kafgres {v[0]}, kafka {v[1]}" for k, v in sorted(wider.items()))
    )

def test_a_null_metadata_row_still_decodes(scenario="group-consume"):
    """The other half of the OffsetFetch metadata fix, which no scenario reaches.

    We sent null where Kafka sends an empty string, and Sarama's decoder rejects the
    response. Two code paths produce that field — the "no committed offset" default,
    which `group-consume` covers because a fresh group takes it, and a stored row whose
    `metadata` is SQL NULL, which nothing covers. Both runners commit with `""`, so the
    column is never NULL in a test.

    It can be NULL in the field: OffsetCommit's `metadata` is nullable on the wire. Left
    uncovered, a regression here reappears as "Sarama consumers hang against this one
    cluster".
    """
    topic = "conf-null-meta"
    make_topic_kafgres(topic)
    group = f"{topic}-sarama-group"
    try:
        first = run_client("sarama", KAFGRES, scenario, topic)
        assert first.startswith("OK "), first

        sql(f"UPDATE kafgres_offsets SET metadata = NULL WHERE group_id = '{group}'")
        affected = sql(
            f"SELECT count(*) FROM kafgres_offsets "
            f"WHERE group_id = '{group}' AND metadata IS NULL"
        )
        assert affected != "0", "no committed offsets to null out; the setup did not run"

        second = run_client("sarama", KAFGRES, scenario, topic)
        assert second.startswith("OK "), f"NULL metadata broke OffsetFetch: {second}"
    finally:
        drop_topic_kafgres(topic)
        sql(f"DELETE FROM kafgres_offsets WHERE group_id = '{group}'")
