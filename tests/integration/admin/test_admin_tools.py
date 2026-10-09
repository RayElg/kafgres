"""Admin tooling and retention.

The acceptance criterion names the real Java tools, so these drive `kafka-topics.sh`,
`kafka-configs.sh` and `kafka-consumer-groups.sh` rather than hand-built frames. What
matters is that unmodified tooling behaves, and only a tool can prove that.

Retention is measured against `pg_total_relation_size`, never assumed — a partition drop
that leaves the table behind looks identical to a working one from the wire.
"""

import subprocess
import time

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

def topic_size(topic):
    """Bytes on disk for a topic's log segments, from Postgres rather than from us."""
    return int(sql(
        f"""SELECT COALESCE(SUM(pg_total_relation_size(table_name::regclass)), 0)
              FROM kafgres_log_segments
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip())

def segment_count(topic):
    return int(sql(
        f"""SELECT count(*) FROM kafgres_log_segments
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip())

engine_a_storage = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "table",
    reason="engine A only: asserts on kafgres_log_segments, which is engine A's storage",
)

@pytest.fixture
def topic(request):
    name = f"p5-{request.node.name.replace('_', '-')[:38]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

@pytest.fixture
def small_segments():
    """Shrink the segment size so retention has something to reclaim.

    Retention's granularity is the segment, so at the default no test can produce
    enough to roll one. Both settings, because the two engines roll on different ones —
    `segment_offsets` on the table engine, `segment_bytes` on the segment engine — and
    setting only one leaves the other unable to roll at all.
    """
    sql("ALTER SYSTEM SET kafgres.segment_offsets = 20")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 4096")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    yield
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")

def test_kafka_topics_sh_lifecycle(topic):
    """Driven by the tool that defines the RPCs."""
    out = kafka_tool("kafka-topics.sh", "--create", "--topic", topic, "--partitions", "3")
    assert out.returncode == 0, out.stderr
    assert "Created topic" in out.stdout, out.stdout

    listing = kafka_tool("kafka-topics.sh", "--list")
    assert topic in listing.stdout, listing.stdout

    described = kafka_tool("kafka-topics.sh", "--describe", "--topic", topic)
    assert "PartitionCount: 3" in described.stdout, described.stdout
    assert "TopicId: AAAAAAAAAAAAAAAAAAAAAA" not in described.stdout, described.stdout

    grown = kafka_tool("kafka-topics.sh", "--alter", "--topic", topic, "--partitions", "5")
    assert grown.returncode == 0, grown.stderr
    assert "PartitionCount: 5" in kafka_tool(
        "kafka-topics.sh", "--describe", "--topic", topic
    ).stdout

    deleted = kafka_tool("kafka-topics.sh", "--delete", "--topic", topic)
    assert deleted.returncode == 0, deleted.stderr
    assert topic not in kafka_tool("kafka-topics.sh", "--list").stdout

def test_a_topic_cannot_be_shrunk(topic):
    """Kafka cannot reduce a partition count, and neither can we. Silently accepting it
    would strand every record in the partitions that vanished."""
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "4").returncode == 0
    out = kafka_tool("kafka-topics.sh", "--alter", "--topic", topic, "--partitions", "2")
    assert out.returncode != 0
    assert "4" in (out.stdout + out.stderr), out.stdout + out.stderr
    assert "PartitionCount: 4" in kafka_tool(
        "kafka-topics.sh", "--describe", "--topic", topic
    ).stdout, "the failed shrink changed the topic anyway"

def test_a_duplicate_topic_is_refused(topic):
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    again = kafka_tool("kafka-topics.sh", "--create", "--topic", topic)
    assert again.returncode != 0
    assert "already exists" in (again.stdout + again.stderr).lower()

def test_an_invalid_topic_name_is_refused():
    out = kafka_tool("kafka-topics.sh", "--create", "--topic", "not a valid name!")
    assert out.returncode != 0
    combined = (out.stdout + out.stderr).lower()
    assert "invalid" in combined or "legal characters" in combined, combined

def test_kafka_configs_sh_round_trip(topic):
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0

    described = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                           "--entity-name", topic, "--describe", "--all")
    assert "retention.ms=604800000" in described.stdout, described.stdout

    altered = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                         "--entity-name", topic, "--alter",
                         "--add-config", "retention.ms=60000")
    assert altered.returncode == 0, altered.stderr

    back = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                      "--entity-name", topic, "--describe")
    assert "retention.ms=60000" in back.stdout, back.stdout

    removed = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                         "--entity-name", topic, "--alter",
                         "--delete-config", "retention.ms")
    assert removed.returncode == 0, removed.stderr
    assert "retention.ms" not in kafka_tool(
        "kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--describe"
    ).stdout

def test_a_config_we_do_not_implement_is_refused(topic):
    """Reporting success for something that never runs is worse than refusing it: the
    operator reads the value back from `--describe` and believes it took effect.

    `min.cleanable.dirty.ratio` is deliberately absent from the registry: it exists
    upstream to avoid re-reading a clean log, and this cleaner is already incremental,
    so advertising a ratio nothing consults would be the very lie under test here.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "min.cleanable.dirty.ratio=0.7")
    assert out.returncode != 0, "a config nothing consults was accepted"

    described = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                           "--entity-name", topic, "--describe", "--all")
    assert "cleanup.policy=delete" in described.stdout, described.stdout

def test_one_bad_entry_does_not_leave_the_others_applied(topic):
    """A resource is one unit: its entries all take or none do.

    IncrementalAlterConfigs returns a single error code for the whole resource, so an
    operator told "invalid" has no way to learn that the first entry was written anyway.
    Validation ran before applying, but it only checked *writability* while the apply path
    checked the value — so an entry with a bad value passed the gate and failed on apply,
    after its predecessor had already committed.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "retention.bytes=1048576,retention.ms=forever")
    assert out.returncode != 0
    assert "not a valid retention.ms" in (out.stdout + out.stderr), out.stdout + out.stderr

    assert sql(f"SELECT config FROM kafgres_topics WHERE name = '{topic}'").strip() == "{}", (
        "a rejected alter left one of its entries applied"
    )

    ok = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                    "--alter", "--add-config", "retention.bytes=1048576,retention.ms=60000")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    stored = sql(f"SELECT config FROM kafgres_topics WHERE name = '{topic}'")
    assert "1048576" in stored and "60000" in stored, stored

def test_asserting_the_policy_we_implement_is_not_a_change(topic):
    """`cleanup.policy=delete` must be accepted — it is the value the broker already has.

    Any UI with a create-topic form sends the whole config block back, defaults
    included: Redpanda Console does, and CreateTopics failed with "'cleanup.policy' is
    read-only on this broker" for a topic whose requested policy was the only one this
    broker supported. `cleanup.policy` is genuinely writable now that compaction
    exists; the read-only no-op rule still governs the three configs that remain
    read-only, covered by
    test_the_configs_a_real_deployment_sets_are_accepted_at_their_true_value.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--config", "cleanup.policy=delete").returncode == 0

    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "cleanup.policy=delete")
    assert out.returncode == 0, out.stdout + out.stderr

    described = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                           "--entity-name", topic, "--describe", "--all")
    assert "cleanup.policy=delete" in described.stdout, described.stdout

def test_max_message_bytes_is_enforced_and_configurable(topic):
    """Kafka answers MESSAGE_TOO_LARGE by default at ~1 MiB. A broker that accepts
    larger records silently breaks every topic later pointed at a real one."""
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    big = "x" * 3_000_000

    out = kcat("-t", topic, "-P", "-X", "message.max.bytes=10000000", stdin=big + "\n")
    assert out.returncode != 0, "a 3 MB record was accepted at the default limit"
    assert "too large" in (out.stdout + out.stderr).lower(), out.stdout + out.stderr

    assert kcat("-t", topic, "-P", stdin="small\n").returncode == 0

    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config", "max.message.bytes=5000000").returncode == 0
    ok = kcat("-t", topic, "-P", "-X", "message.max.bytes=10000000", stdin=big + "\n")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert int(sql(f"SELECT coalesce(sum(offset_span), 0) "
                   f"FROM kafgres_partition_offsets('{topic}')")) == 2

def test_the_configs_a_real_deployment_sets_are_accepted_at_their_true_value(topic):
    """The configs a real deployment sets are accepted at their true value.

    Every one of these creates a topic on real Kafka, so any script, Terraform
    definition or Streams/Connect internal topic carried over from a Kafka deployment
    fails at step one otherwise. They are accepted at the value kafgres actually
    implements and refused at any other: accepting `compression.type=zstd` would mean
    re-compressing, and `message.timestamp.type=LogAppendTime` would mean rewriting
    timestamps — both re-encode the batch, which byte-verbatim storage forbids.
    `min.insync.replicas` above 1 is a durability promise this broker cannot make.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    for value in ["compression.type=producer", "min.insync.replicas=1",
                  "message.timestamp.type=CreateTime"]:
        out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name",
                         topic, "--alter", "--add-config", value)
        assert out.returncode == 0, f"{value} was refused: {out.stdout + out.stderr}"

    for value, why in [("compression.type=zstd", "re-compression"),
                       ("min.insync.replicas=2", "a durability promise"),
                       ("message.timestamp.type=LogAppendTime", "timestamp rewriting")]:
        out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name",
                         topic, "--alter", "--add-config", value)
        assert out.returncode != 0, f"{value} was accepted, promising {why}"
        assert "read-only" in (out.stdout + out.stderr), out.stdout + out.stderr

    assert sql(f"SELECT config FROM kafgres_topics WHERE name = '{topic}'").strip() == "{}"

def test_describe_configs_reports_only_what_is_enforced(topic):
    """Every config in the response has to be one the broker honours at the value it
    reports. An advertised config that nothing reads is a lie the operator cannot detect.

    Two kinds here, and both satisfy that. `retention.ms`, `retention.bytes` and
    `max.message.bytes` are settable and enforced. `cleanup.policy`, `compression.type`,
    `min.insync.replicas` and `message.timestamp.type` report a single true value and
    refuse any other — `delete`, `producer`, `1`, `CreateTime` are what this broker
    actually does, so reporting them is honest and changing them is refused.

    Pinned as an exact set on purpose: adding a config here must be a deliberate act, and
    the failure mode of getting it wrong — reporting something nothing enforces — is
    invisible from the wire.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                     "--entity-name", topic, "--describe", "--all")
    names = {
        line.strip().split("=", 1)[0]
        for line in out.stdout.splitlines()
        if "=" in line and "sensitive" in line
    }
    assert names == {
        "retention.ms",
        "retention.bytes",
        "cleanup.policy",
        "max.message.bytes",
        "min.compaction.lag.ms",
        "delete.retention.ms",
        "segment.ms",
        "segment.bytes",
        "compression.type",
        "min.insync.replicas",
        "message.timestamp.type",
    }, names

def fill(topic, count=150):
    """Produce one batch per record, so segments actually roll.

    Batching is the thing that makes this subtle: a segment holds a range of *base*
    offsets, so one batch of 150 records occupies one segment no matter how large it is.
    """
    payload = "\n".join(f"pad-pad-pad-pad-pad-pad-{i}" for i in range(count)) + "\n"
    out = kcat("-t", topic, "-P", "-X", "batch.num.messages=1", "-X", "linger.ms=0",
               stdin=payload)
    assert out.returncode == 0, out.stderr

@engine_a_storage
def test_retention_reclaims_disk(topic, small_segments):
    """Measured rather than assumed: enforcement is a partition `DROP`, so the space
    must actually come back. A `DELETE`-based implementation passes every wire-level
    check here and leaves the bloat behind, which is why this asserts on
    `pg_total_relation_size`."""
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0
    fill(topic)

    before_segments = segment_count(topic)
    before_bytes = topic_size(topic)
    assert before_segments > 1, f"expected several segments, got {before_segments}"
    assert before_bytes > 0

    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config", "retention.ms=1000").returncode == 0
    time.sleep(2)
    sql("SELECT kafgres_enforce_retention()")

    after_segments = segment_count(topic)
    after_bytes = topic_size(topic)
    assert after_segments < before_segments, "no segment was dropped"
    assert after_bytes < before_bytes, (
        f"disk not reclaimed: {before_bytes} -> {after_bytes} bytes"
    )

@engine_a_storage
def test_retention_never_drops_the_active_segment(topic, small_segments):
    """The segment `next_offset` falls inside is the range partition an in-flight append
    is targeting. Dropping it is a `DROP TABLE` racing an `INSERT`, and the records the
    producer just wrote go with it.

    The segment size is 20, so filling to 70 leaves `next_offset` inside `[60, 80)` —
    partially filled, and therefore live. Under a 1 ms retention every record in it is
    already past its deadline, so nothing but the active-segment rule protects it.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0
    fill(topic, 70)

    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config", "retention.ms=1").returncode == 0
    time.sleep(1)
    sql("SELECT kafgres_enforce_retention()")

    assert segment_count(topic) == 1, (
        f"expected only the active segment to survive, got {segment_count(topic)}"
    )
    start = int(sql(
        f"""SELECT log_start_offset FROM kafgres_partitions
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip())
    assert start == 60, f"expected the live segment's base offset, got {start}"

    back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert back.returncode == 0, back.stderr
    assert len(back.stdout.splitlines()) == 10, back.stdout

def test_a_consumer_sees_the_truncated_start(topic, small_segments):
    """`-o beginning` must resolve to the new log start, not to 0. Answering 0 sends the
    consumer to an offset that no longer exists, and it gets OFFSET_OUT_OF_RANGE on a
    topic that is perfectly healthy."""
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0
    fill(topic)

    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config", "retention.ms=1000").returncode == 0
    time.sleep(2)
    sql("SELECT kafgres_enforce_retention()")

    start = int(sql(
        f"""SELECT log_start_offset FROM kafgres_partitions
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip())
    assert start > 0, "retention did not advance the log start offset"

    back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert back.returncode == 0, back.stderr
    read = len(back.stdout.splitlines())
    assert read == 150 - start, f"expected {150 - start} surviving records, read {read}"

def test_delete_records_advances_the_low_watermark(topic):
    """`21 DeleteRecords`, which is retention driven by the client rather than the clock."""
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\n").returncode == 0

    import json
    import os
    import tempfile

    spec = {"partitions": [{"topic": topic, "partition": 0, "offset": 2}], "version": 1}
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "offsets.json")
        with open(path, "w") as f:
            json.dump(spec, f)
        os.chmod(d, 0o755)
        os.chmod(path, 0o644)
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", "-v", f"{d}:/w", KAFKA,
             "/opt/kafka/bin/kafka-delete-records.sh", "--bootstrap-server", BROKER,
             "--offset-json-file", "/w/offsets.json"],
            capture_output=True, text=True, timeout=300,
        )
    assert out.returncode == 0, out.stderr
    assert "low_watermark: 2" in out.stdout, out.stdout

    back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert back.stdout.split() == ["c"], back.stdout

def test_retention_does_not_drop_records_a_spanning_batch_left_above_its_segment(
    topic, small_segments
):
    """A segment holds offsets above its own `end_offset`, and retention must know it.

    A range partition is keyed on `base_offset` alone, so a batch straddling a boundary
    lands *whole* in the lower segment. Dropping that segment on `end_offset <= cutoff`
    destroys the records that spilled over — committed, acked, and inside
    `[log_start_offset, high_watermark)`. A consumer reading across the hole gets no
    error at all, because gaps are legal in Kafka, so the loss is silent.

    Every other retention test here produces one record per batch, which makes spanning
    impossible and hides this entirely. This one batches on purpose.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0

    for _ in range(6):
        chunk = "\n".join(f"m{i}" for i in range(15)) + "\n"
        assert kcat("-t", topic, "-P", "-X", "batch.num.messages=15",
                    "-X", "linger.ms=200", stdin=chunk).returncode == 0

    total = int(sql(
        f"SELECT coalesce(sum(offset_span), 0) FROM kafgres_partition_offsets('{topic}')"
    ).strip())
    assert total == 90, f"expected 90 records, stored {total}"

    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config", "retention.ms=1000").returncode == 0
    time.sleep(2)
    sql("SELECT kafgres_enforce_retention()")

    start = int(sql(
        f"""SELECT log_start_offset FROM kafgres_partitions
             WHERE topic_id = (SELECT topic_id FROM kafgres_topics WHERE name = '{topic}')"""
    ).strip())

    back = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert back.returncode == 0, back.stderr
    read = len(back.stdout.splitlines())
    assert read == 90 - start, (
        f"log start is {start} of 90, so {90 - start} records should survive; read {read}"
    )

def test_a_fetch_inside_a_batch_returns_the_containing_batch(topic):
    """A batch holds many records under one base offset, so an offset the client asks for
    routinely lands *inside* one — a consumer resuming after committing part of a batch,
    or a `log_start_offset` that DeleteRecords put there. Matching only batches whose
    base offset is at or past the request returns nothing at all, and the consumer waits
    at that offset forever: no records, no error, and the high watermark never reached.
    Kafka returns the whole containing batch and lets the client drop what it has seen.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0
    assert kcat("-t", topic, "-P", stdin="r0\nr1\nr2\nr3\n").returncode == 0
    if sql("SHOW kafgres.storage_engine") == "table":
        assert int(sql(
            f"""SELECT count(*) FROM kafgres_log
                 WHERE topic_id = (SELECT topic_id FROM kafgres_topics
                                    WHERE name = '{topic}')"""
        ).strip()) == 1, "this test needs the four records in one batch"

    back = kcat("-t", topic, "-C", "-o", "2", "-e", "-q")
    assert back.returncode == 0, back.stderr
    assert back.stdout.split() == ["r2", "r3"], back.stdout

def test_a_caught_up_consumer_is_not_redelivered_the_last_batch(topic):
    """The other half of the containing-batch rule, and the way to get it wrong.

    Matching "the newest batch at or below the requested offset" without also requiring
    that the batch *reaches* that offset means a consumer sitting at the high watermark
    matches the last batch and receives it again on every poll — forever, never
    advancing, with no error. It reads as a consumer that cannot finish.
    """
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic,
                      "--partitions", "1").returncode == 0
    assert kcat("-t", topic, "-P", stdin="a\nb\nc\n").returncode == 0

    back = kcat("-t", topic, "-C", "-o", "end", "-e", "-q")
    assert back.returncode == 0, back.stderr
    assert back.stdout.strip() == "", f"redelivered at the high watermark: {back.stdout!r}"

    whole = kcat("-t", topic, "-C", "-o", "beginning", "-e", "-q")
    assert whole.stdout.split() == ["a", "b", "c"], whole.stdout

def test_delete_groups(topic):
    """`42 DeleteGroups`. A live group must be refused rather than deleted out from
    under its consumers."""
    group = f"{topic}-g"
    assert kafka_tool("kafka-topics.sh", "--create", "--topic", topic).returncode == 0
    assert kcat("-t", topic, "-P", stdin="x\n").returncode == 0
    assert kcat("-G", group, topic, "-e", "-o", "beginning", timeout=180).returncode == 0

    listed = kafka_tool("kafka-consumer-groups.sh", "--list")
    assert group in listed.stdout, listed.stdout

    deleted = kafka_tool("kafka-consumer-groups.sh", "--delete", "--group", group)
    assert deleted.returncode == 0, deleted.stdout + deleted.stderr
    assert group not in kafka_tool("kafka-consumer-groups.sh", "--list").stdout

def test_deleting_an_unknown_group_reports_not_found():
    out = kafka_tool("kafka-consumer-groups.sh", "--delete", "--group", "no-such-group-p5")
    combined = out.stdout + out.stderr
    assert "does not exist" in combined or "GroupIdNotFound" in combined, combined

def test_describe_cluster(topic):
    """`60 DescribeCluster`, which `kafka-cluster.sh` uses and AdminClient prefers over
    Metadata when it is advertised."""
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA,
         "/opt/kafka/bin/kafka-cluster.sh", "cluster-id", "--bootstrap-server", BROKER],
        capture_output=True, text=True, timeout=300,
    )
    assert out.returncode == 0, out.stderr
    assert "kafgres-cluster" in out.stdout, out.stdout
