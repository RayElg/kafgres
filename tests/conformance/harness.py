"""Shared plumbing for the per-client conformance files.

A scenario is one runner invocation, `<runner> <broker> <scenario> <topic>`, printing one
line that starts `OK ` or `ERROR `. The line carries counts, offsets, values and error names,
never timings or ids that differ between brokers. Without the reference broker, only the
kafgres half runs.
"""

import os
import subprocess
import time
from dataclasses import dataclass

import pytest

CLIENTS_IMAGE = "kafgres-clients"
KAFKA_IMAGE = "apache/kafka:4.3.1"
KAFGRES = "127.0.0.1:9092"
REFERENCE = "127.0.0.1:9292"
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

# Unique per run. The reference broker outlives the run and keeps group offsets for a
# topic name it has seen, so a fixed name would resume where the last run stopped.
RUN = int(time.time())

RUNNERS = {
    "sarama": ["sarama-conformance"],
    "franz-go": ["franz-conformance"],
    "kafka-python": ["kafka-python-conformance"],
    "librdkafka": ["librdkafka-conformance"],
    "java": ["java-conformance"],
}


@dataclass(frozen=True)
class Scenario:
    """Topics a scenario creates for itself are named `{topic}-<suffix>`."""

    name: str
    partitions: int = 1
    timeout: int = 300


PARTITIONS = {
    "multi-partition": 3,
    "group-two-members": 3,
    "group-rebalance-leave": 3,
    "group-static": 3,
    "group-cooperative": 3,
    "group-kip848": 3,
}


def scenarios(*names):
    return [Scenario(n, PARTITIONS.get(n, 1)) for n in names]


# Unfixed differences from Kafka, strict: an entry that now matches fails the run.
# Written up in docs/conformance.md under "Known gaps".
_STATIC_MEMBERSHIP = (
    "KIP-345 static membership is not enforced: group.instance.id is stored, but a second "
    "member joining with an id already in use does not fence the first (Kafka: FENCED_INSTANCE_ID)"
)
KNOWN_GAPS = {
    ("java", "group-static"): _STATIC_MEMBERSHIP,
    ("librdkafka", "group-static"): _STATIC_MEMBERSHIP,
    ("sarama", "group-static"): _STATIC_MEMBERSHIP,
    ("franz-go", "group-static"): _STATIC_MEMBERSHIP,
    ("java", "group-admin"): (
        "OffsetCommit does not validate the member: a commit with no member id and generation "
        "-1 is accepted while the group has members, where Kafka answers UNKNOWN_MEMBER_ID"
    ),
    ("sarama", "offsets-by-time"): (
        "Sarama's producer sends a batch header with max timestamp -1 and Kafka recomputes it "
        "on append; kafgres stores the header and CRC verbatim, so time lookups miss Sarama data"
    ),
    ("sarama", "admin-offsets"): "the same batch-header max timestamp as sarama/offsets-by-time",
}


def sql(query, timeout=60):
    """Raises on failure, so a guard like `assert affected != "0"` cannot pass on ""."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-v", "ON_ERROR_STOP=1", "-d", "postgres", "-tAc", query],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    if out.returncode != 0:
        raise RuntimeError(f"psql failed ({out.returncode}) for {query!r}: {out.stderr.strip()}")
    return out.stdout.strip()


def _kafka_tool(*args, timeout=120):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE, *args],
        capture_output=True, text=True, timeout=timeout,
    )


def reference_available():
    try:
        return _kafka_tool("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                           REFERENCE, "--list", timeout=90).returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


_reference = None


def has_reference():
    """Probed lazily once per session."""
    global _reference
    if _reference is None:
        _reference = reference_available()
    return _reference


def run_client(client, broker, scenario, topic, timeout=300):
    try:
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", CLIENTS_IMAGE,
             *RUNNERS[client], broker, scenario, topic],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return f"ERROR runner timed out after {timeout}s"
    for line in reversed((out.stdout + out.stderr).splitlines()):
        if line.startswith(("OK ", "ERROR ")):
            return line.strip()
    return f"ERROR no result line (rc={out.returncode}): {out.stdout[-200:]} {out.stderr[-200:]}"


def make_topic_kafgres(name, partitions=1):
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', {partitions})")


def drop_topic_kafgres(name):
    sql(f"SELECT kafgres_drop_topic('{name}')")


def make_topic_reference(name, partitions=1):
    out = _kafka_tool(
        "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", REFERENCE,
        "--create", "--topic", name, "--partitions", str(partitions), "--if-not-exists")
    assert out.returncode == 0, f"could not create {name} on the reference: {out.stderr}"


def drop_topic_reference(name):
    _kafka_tool("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", REFERENCE,
                "--delete", "--topic", name, "--if-exists")


def drop_prefixed(prefix, reference):
    for name in sql(f"SELECT name FROM kafgres_topics WHERE name LIKE '{prefix}%'").splitlines():
        drop_topic_kafgres(name)
    sql(f"DELETE FROM kafgres_offsets WHERE group_id LIKE '{prefix}%'")
    if reference:
        listing = _kafka_tool("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                              REFERENCE, "--list")
        for name in listing.stdout.split():
            if name.startswith(prefix):
                drop_topic_reference(name)
        groups = _kafka_tool("/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
                             REFERENCE, "--list")
        for name in groups.stdout.split():
            if name.startswith(prefix):
                _kafka_tool("/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
                            REFERENCE, "--delete", "--group", name)


def check_scenario(client, scenario):
    """A `KNOWN_GAPS` scenario xfails while it differs and fails once it matches."""
    gap = KNOWN_GAPS.get((client, scenario.name))
    if gap is None:
        return _check_scenario(client, scenario)
    if not has_reference():
        pytest.skip("a known gap is only meaningful against the reference broker")
    try:
        _check_scenario(client, scenario)
    except AssertionError:
        pytest.xfail(gap)
    pytest.fail(f"{client}/{scenario.name} now matches the reference; delete its KNOWN_GAPS "
                "entry and its paragraph in docs/conformance.md")


def _check_scenario(client, scenario):
    topic = f"conf-{client}-{scenario.name}-{RUN}"
    reference = has_reference()
    make_topic_kafgres(topic, scenario.partitions)
    if reference:
        make_topic_reference(topic, scenario.partitions)
    try:
        ours = run_client(client, KAFGRES, scenario.name, topic, scenario.timeout)
        assert ours.startswith("OK "), f"{client}/{scenario.name} against kafgres: {ours}"
        if not reference:
            return
        theirs = run_client(client, REFERENCE, scenario.name, topic, scenario.timeout)
        assert theirs.startswith("OK "), (
            f"{client}/{scenario.name} against the reference broker: {theirs}\n"
            "A scenario that fails on Kafka is a bad scenario, not a deviation.")
        assert ours == theirs, (
            f"{client}/{scenario.name} differs from the reference broker.\n"
            f"  kafgres: {ours}\n"
            f"  kafka:   {theirs}\n"
            "If this is intended, record it in docs/conformance.md as an accepted "
            "deviation with a reason. An uncatalogued diff is a bug.")
    finally:
        drop_prefixed(topic, reference)


def parametrize(client, scenarios):
    return pytest.mark.parametrize("scenario", scenarios, ids=lambda s: s.name)
