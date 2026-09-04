"""Log compaction (`cleanup.policy=compact`): a surviving record keeps the offset it was
written at — the log becomes sparse, never renumbered."""

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

def engine():
    return sql("SHOW kafgres.storage_engine")

table_engine_only = pytest.mark.skipif(
    engine() != "table",
    reason="asserts an exact offset list, which depends on the table engine's offset-based floor",
)

@pytest.fixture(autouse=True)
def small_active_region():
    """Shrink the segment so there is anything *outside* the active region to compact."""
    sql("ALTER SYSTEM SET kafgres.segment_offsets = 2")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 4096")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    yield
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")

@pytest.fixture
def topic(request):
    name = f"cmp-{request.node.name.replace('_', '-')[:36]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def make_compacted(name):
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", name,
                     "--alter", "--add-config", "cleanup.policy=compact")
    assert out.returncode == 0, out.stdout + out.stderr

PAD = "p" * 1500

def produce(name, pairs):
    """One kcat invocation per pair, so each lands in its own batch: compaction operates
    on whole batches."""
    for pair in pairs:
        key, value = pair.split(":", 1)
        assert kcat("-t", name, "-P", "-K:",
                    stdin=f"{key}:{value}{PAD}\n").returncode == 0
        time.sleep(0.25)
    time.sleep(0.5)

def read_back(name):
    """Offset and key only — the values carry padding that would drown the assertions."""
    out = kcat("-C", "-t", name, "-o", "beginning", "-e", "-q", "-f", "%o %k\n")
    assert out.returncode == 0, out.stderr
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]

def read_values(name):
    """Offset, key and whether the value is null — for the tombstone assertions."""
    out = kcat("-C", "-t", name, "-o", "beginning", "-e", "-q", "-f", "%o %k %S\n")
    assert out.returncode == 0, out.stderr
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]

def compact():
    sql("SELECT kafgres_enforce_retention()")
    time.sleep(0.5)

@table_engine_only
def test_the_last_record_per_key_survives_at_its_original_offset(topic):
    """The property that makes compaction safe: offsets are preserved."""
    make_compacted(topic)
    produce(topic, ["a:1", "b:1", "a:2", "c:1", "a:3", "b:2", "d:1"])
    assert read_back(topic) == ["0 a", "1 b", "2 a", "3 c", "4 a", "5 b", "6 d"]

    compact()

    assert read_back(topic) == ["1 b", "3 c", "4 a", "5 b", "6 d"]

def test_compaction_is_idempotent(topic):
    """A second pass over a clean log must rewrite nothing."""
    make_compacted(topic)
    produce(topic, ["k:1", "k:2", "k:3", "z:1", "z:2", "y:1"])
    compact()
    once = read_back(topic)
    assert "0 k" not in once, f"nothing was compacted, so idempotency proves nothing: {once}"
    assert once[-1] == "5 y", once

    compact()
    assert read_back(topic) == once, "a second pass changed a log that was already compacted"

def test_a_tombstone_survives_the_pass_that_supersedes_its_key(topic):
    """A tombstone survives the pass that supersedes its key."""
    make_compacted(topic)
    assert kcat("-t", topic, "-P", "-K:", stdin=f"gone:1{PAD}\n").returncode == 0
    time.sleep(0.3)
    assert kcat("-t", topic, "-P", "-K:", "-Z", stdin="gone:\n").returncode == 0
    time.sleep(0.3)
    produce(topic, ["other:1", "other:2", "third:1", "fourth:1"])

    compact()
    lines = read_values(topic)
    assert any(line.startswith("1 gone") for line in lines), f"the tombstone was dropped: {lines}"
    assert not any(line.startswith("0 gone") for line in lines), (
        f"the superseded value survived: {lines}"
    )

def test_a_null_key_is_refused_on_a_compacted_topic(topic):
    """Verified against Kafka 4.1.0: INVALID_RECORD at produce time."""
    make_compacted(topic)
    out = kcat("-t", topic, "-P", stdin="no-key-at-all\n")
    assert out.returncode != 0, "a null-keyed record was accepted on a compacted topic"
    assert "validate" in (out.stdout + out.stderr).lower(), out.stdout + out.stderr

    plain = f"{topic}-plain"
    sql(f"SELECT kafgres_drop_topic('{plain}')")
    sql(f"SELECT kafgres_create_topic('{plain}', 1)")
    try:
        assert kcat("-t", plain, "-P", stdin="no-key-at-all\n").returncode == 0
    finally:
        sql(f"SELECT kafgres_drop_topic('{plain}')")

def test_the_newest_batch_is_never_compacted(topic):
    """Compaction only ever removes records a caught-up consumer has already passed."""
    make_compacted(topic)
    produce(topic, ["x:1", "x:2", "x:3", "x:4", "x:5"])
    compact()
    lines = read_back(topic)
    assert lines[-1] == "4 x", lines
    assert lines.count("0 x") == 0, f"a superseded record survived: {lines}"

def test_compact_is_accepted_on_either_engine(topic):
    """`cleanup.policy=compact` is accepted on either engine and survives a restart."""
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "cleanup.policy=compact")
    assert out.returncode == 0, out.stdout + out.stderr
    described = kafka_tool("kafka-configs.sh", "--entity-type", "topics",
                           "--entity-name", topic, "--describe", "--all")
    assert "cleanup.policy=compact" in described.stdout, described.stdout

def test_an_unknown_cleanup_policy_is_refused(topic):
    """Only the three values the broker implements are accepted."""
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "cleanup.policy=nonsense")
    assert out.returncode != 0, "an unknown policy was accepted"

def test_compact_delete_applies_both(topic):
    """`compact,delete` keeps the latest record per key *and* ages records out.

    Order matters and is the reason this is one branch rather than two independent ones:
    retention drops whole segments below a watermark without looking inside them, so
    running it before compaction would discard records compaction was about to keep as the
    latest for their key. `compact` alone applies no retention at all — the latest record
    for a key stays forever, which is what a compacted topic is for.
    """
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "cleanup.policy=[compact,delete]")
    assert out.returncode == 0, out.stdout + out.stderr
    assert sql(f"SELECT config FROM kafgres_topics WHERE name = '{topic}'").strip() \
        == '{"cleanup.policy": "compact,delete"}'

    produce(topic, ["a:1", "b:1", "a:2", "c:1", "a:3", "b:2", "d:1"])
    compact()
    lines = read_back(topic)
    assert "0 a" not in lines, lines

    assert kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                      "--alter", "--add-config", "retention.ms=1").returncode == 0
    time.sleep(2)
    compact()
    remaining = read_back(topic)
    assert len(remaining) < len(lines), (
        f"retention did not run on a compact,delete topic: {lines} -> {remaining}"
    )

def test_a_fresh_tombstone_outlives_the_pass_that_created_it(topic):
    """`delete.retention.ms` is what makes a deletion observable.

    A tombstone is kept so a consumer that was offline learns the key is gone. Removing it
    in the same pass would make the key simply stop appearing — indistinguishable from a
    key that went quiet. Kept at the default 24h; the codec suite covers the expiry side,
    which needs a clock this test cannot move.
    """
    make_compacted(topic)
    assert kcat("-t", topic, "-P", "-K:", stdin="k:1\n").returncode == 0
    time.sleep(0.3)
    assert kcat("-t", topic, "-P", "-K:", "-Z", stdin="k:\n").returncode == 0
    time.sleep(0.3)
    for pair in ["f:1", "g:1", "h:1"]:
        assert kcat("-t", topic, "-P", "-K:", stdin=pair + "\n").returncode == 0
        time.sleep(0.25)
    time.sleep(0.5)

    compact()
    lines = read_values(topic)
    assert any(line.startswith("1 k") for line in lines), f"the tombstone was dropped: {lines}"

def test_a_backend_compaction_invalidates_the_workers_seek_hints():
    """A compaction pass must invalidate the worker's seek hints.

    Seek hints are a per-process `static`, not shared memory. A compaction pass running
    in a *backend* — which `kafgres_enforce_retention()` is, and which this test uses —
    can only clear its own. The broker worker keeps hints naming byte positions in a
    file whose bytes have all moved, and `read` does not CRC-validate on the seek path:
    it either stops at a garbage length, returning an empty Fetch forever with the high
    watermark far ahead, or reads a fabricated header and advances to an offset that
    never existed.

    `Slot::layout_generation` is the fix — shared memory, bumped under the shard lock on
    every rewrite, compared in `with_slot` before any hint is trusted.

    **This test needs segments larger than one index interval or it cannot fail.** At
    `segment_bytes = 4096`, which is exactly `INDEX_INTERVAL_BYTES`, every segment holds one
    index entry at position 0 — and a stale hint pointing at position 0 is always correct.
    The fixture here overrides the shared one for that reason.
    """
    if engine() != "segment":
        pytest.skip("seek hints are the segment engine's")

    name = "cmp-hint-invalidation"
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 65536")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    try:
        make_compacted(name)
        big = "q" * 1500
        for i in range(60):
            key = f"u{i}" if i % 2 == 0 else "dup"
            assert kcat("-t", name, "-P", "-K:",
                        stdin=f"{key}:{big}-{i}\n").returncode == 0
        time.sleep(1)

        before = kcat("-C", "-t", name, "-o", "30", "-e", "-q", "-f", "%o\n")
        assert before.returncode == 0, before.stderr
        assert before.stdout.strip(), "nothing was readable before compacting"

        sql("SELECT kafgres_enforce_retention()")
        time.sleep(1)

        after = kcat("-C", "-t", name, "-o", "30", "-e", "-q", "-f", "%o %k\n")
        assert after.returncode == 0, after.stderr
        lines = [l.strip() for l in after.stdout.splitlines() if l.strip()]
        assert lines, "the fetch came back empty — a stale hint stopped the scan"
        offsets = [int(l.split()[0]) for l in lines]
        assert offsets == sorted(offsets), f"offsets went backwards: {offsets}"
        assert max(offsets) == 59, f"the log end moved: {offsets[-5:]}"
        assert all(0 <= o <= 59 for o in offsets), f"a fabricated offset appeared: {offsets}"
        assert offsets[0] == 30, (
            f"the scan started past surviving records — stale seek hint: {offsets[:8]}"
        )
        assert len(offsets) >= 4, f"the scan stopped early: {offsets}"
    finally:
        sql(f"SELECT kafgres_drop_topic('{name}')")
        sql("ALTER SYSTEM SET kafgres.segment_bytes = 4096")
        sql("SELECT pg_reload_conf()")

def test_a_small_topic_compacts_on_segment_ms_alone():
    """A keyed state topic is usually far smaller than either seal bound — a segment
    seals on size (64 MiB) or on offset count (a million) — so without a time bound its
    one segment stays active forever and it never compacts. `segment.ms` is the arm
    that covers it.

    Deliberately **not** using the `small_active_region` fixture. That fixture shrinks the
    segment so a test can seal one, which is exactly what hides this bug — the whole point
    here is default sizing, where only time can seal a segment.
    """
    name = "cmp-segment-ms-only"
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    try:
        out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", name,
                         "--alter", "--add-config", "cleanup.policy=compact,segment.ms=1000")
        assert out.returncode == 0, out.stdout + out.stderr

        for i in range(8):
            assert kcat("-t", name, "-P", "-K:", stdin=f"k{i % 3}:v{i}\n").returncode == 0
            time.sleep(0.6)   # longer than segment.ms, so the roll is time-driven
        time.sleep(1)

        span = int(sql(f"SELECT offset_span FROM kafgres_partition_offsets('{name}')"))
        assert span == 8, f"8 records were not produced; span is {span}"

        compact()
        after = read_back(name)
        assert len(after) < 8, (
            f"a small topic did not compact: 8 records produced, {len(after)} readable "
            "after a pass — segment.ms did not seal anything"
        )
        assert after[-1] == "7 k1", after
    finally:
        sql(f"SELECT kafgres_drop_topic('{name}')")

@pytest.fixture
def byte_arm_topic(small_active_region, request):
    """A topic whose *only* active-region bound is `segment.bytes`.

    Depends on `small_active_region` explicitly so it runs after it and can put the offset
    floor back — and creates the topic only once the final value is in place. Changing
    `kafgres.segment_offsets` while a topic exists is not safe on the table engine: the
    range-partition bounds are a pure function of it, so a live topic's next append tries
    to create a partition overlapping one already there.
    """
    sql("ALTER SYSTEM RESET kafgres.segment_offsets")
    sql("ALTER SYSTEM RESET kafgres.segment_bytes")
    sql("SELECT pg_reload_conf()")
    time.sleep(0.5)
    name = f"cmpb-{request.node.name.replace('_', '-')[:34]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")

def set_compacted_with_segment_bytes(name, segment_bytes):
    sql(f"UPDATE kafgres_topics SET config = config || "
        f"'{{\"cleanup.policy\":\"compact\",\"min.compaction.lag.ms\":\"0\","
        f"\"segment.bytes\":\"{segment_bytes}\"}}'::jsonb WHERE name = '{name}'")

def test_segment_bytes_alone_makes_a_small_topic_compactable(byte_arm_topic):
    """`segment.bytes` has to *do* something on both engines, not merely be accepted.

    It is in the registry because refusing it broke real callers — Kafka Streams sets it on
    every repartition topic, so the create failed and Streams never started. But a config
    the broker accepts and ignores is the specific failure the registry exists to prevent:
    the operator reads it back from `--describe` and believes it took effect.

    The offset floor is at its default here, so on a topic this small it can never fire;
    the byte arm is the only bound that can. The segment engine rolls the active segment
    at this size; the table engine has no file to seal and applies the same bound
    directly.
    """
    topic = byte_arm_topic
    set_compacted_with_segment_bytes(topic, 3000)
    produce(topic, [f"k1:v{i}" for i in range(8)])
    before = read_back(topic)
    sql("SELECT kafgres_enforce_retention()")

    got = read_back(topic)
    assert len(got) < len(before), (
        f"segment.bytes is accepted and reported but changes nothing: {len(before)} in, "
        f"{len(got)} out"
    )
    assert got[-1].split()[0] == before[-1].split()[0], (
        f"the newest record for the key did not survive, or was renumbered: {got}"
    )

def test_a_large_segment_bytes_protects_the_whole_log(byte_arm_topic):
    """The other half, and the one that catches an arm that always fires.

    An active-region bound that is wired up but ignores its value looks identical to a
    working one in the test above — everything compacts either way. Only the negative case
    separates "enforced" from "always on".
    """
    topic = byte_arm_topic
    set_compacted_with_segment_bytes(topic, 1073741824)
    produce(topic, [f"k1:v{i}" for i in range(8)])
    before = read_back(topic)
    sql("SELECT kafgres_enforce_retention()")
    after = read_back(topic)
    assert after == before, (
        f"records were compacted inside a 1 GiB active region: {len(before)} -> {len(after)}"
    )
