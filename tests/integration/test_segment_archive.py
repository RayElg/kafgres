"""Retention-aware segment archiving.

The segment engine keeps the log outside Postgres, which quietly breaks the backup
story: `pg_basebackup` still seeds a replica, because segments live under
`$PGDATA/kafgres/`, but retention *unlinks* rolled segments. An archive taken
independently develops holes exactly where retention got there first, and nothing
reports it.

So the property under test is not "segments get copied". It is **retention does not outrun
the archive**: a segment the archive has no record of is still on disk, and
`log_start_offset` has not moved past it.

The archive command is `cp` to a directory inside the container. Nothing here needs an
object store — the design deliberately delegates that to the operator's own tooling, the
way Postgres delegates WAL archiving to `archive_command`, so `cp` exercises the same path
`aws s3 cp` would.
"""

import os
import subprocess
import time

import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CLIENTS = "kafgres-clients"
KAFKA = "apache/kafka:4.1.0"
BROKER = "127.0.0.1:9092"
ARCHIVE_DIR = "/tmp/kafgres-archive"

def compose(*args, timeout=180):
    return subprocess.run(
        ["docker", "compose", *args],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )

def sql(query, timeout=60):
    return compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "postgres",
                   "-tAc", query, timeout=timeout).stdout.strip()

def sh(command, timeout=60):
    """Run a shell command inside the broker container."""
    return compose("exec", "-T", "postgres", "sh", "-c", command, timeout=timeout)

def kafka_tool(script, *args, timeout=240):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", KAFKA, f"/opt/kafka/bin/{script}",
         "--bootstrap-server", BROKER, *args],
        capture_output=True, text=True, timeout=timeout,
    )

def expire(topic):
    """Set a retention window short enough that everything below the active segment is
    eligible, through the tool that owns the setting."""
    out = kafka_tool("kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                     "--alter", "--add-config", "retention.ms=1")
    assert out.returncode == 0, out.stderr
    time.sleep(2)

def kcat(*args, stdin=None, timeout=180):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )

def set_guc(name, value):
    sql(f"ALTER SYSTEM SET {name} = {value}")
    sql("SELECT pg_reload_conf()")

def reset_guc(name):
    sql(f"ALTER SYSTEM RESET {name}")
    sql("SELECT pg_reload_conf()")

def segments_on_disk(topic):
    """Segment filenames for a topic's partition 0, from inside the container."""
    topic_id = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    if not topic_id:
        return []
    out = sh(f"ls $PGDATA/kafgres/{topic_id}/0/ 2>/dev/null | grep '\\.log$' || true")
    return sorted(l for l in out.stdout.split() if l.endswith(".log"))

def archived_names():
    out = sh(f"ls {ARCHIVE_DIR} 2>/dev/null || true")
    return sorted(l for l in out.stdout.split() if l.endswith(".log"))

def topic_id(topic):
    return sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()

def archive_name(topic, segment_file):
    """What the archive stores a segment under.

    Not the segment's filename. Segments are named by base offset alone, so every
    partition's first one is `00000000000000000000.log` — `cp %p DIR/%f` with bare
    filenames would have one partition overwrite another's, both succeed, and both
    originals then get unlinked. `%f` carries topic and partition for that reason.
    """
    return f"{topic_id(topic)}-0-{segment_file}"

def archived_for(topic):
    """Segment filenames the archive holds *for this topic*.

    Scoped deliberately. The archiver walks every partition and the archive directory is
    flat, so comparing the whole directory against one topic's segments makes the test
    depend on what other topics happen to exist — which is how a leftover topic from an
    unrelated run turned this into a failure that looked like a broken archiver.
    """
    rows = sql(f"""SELECT topic_id || '-' || partition || '-'
                          || lpad(base_offset::text, 20, '0') || '.log'
                     FROM kafgres_segment_archive a
                     JOIN kafgres_topics t USING (topic_id)
                    WHERE t.name = '{topic}' ORDER BY base_offset""")
    return sorted(r.strip() for r in rows.splitlines() if r.strip())

pytestmark = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "segment",
    reason="engine B only: the table engine's log is in Postgres and pg_basebackup covers it",
)

@pytest.fixture(scope="module", autouse=True)
def hand_driven():
    """Stop the worker archiving so the tests own the timing.

    Otherwise the worker ships segments on its own tick before the test's explicit
    `kafgres_archive_segments()` runs, and the call reports 0. The settle is because
    `pg_reload_conf()` returns as soon as the postmaster re-reads the file, while the
    worker only picks the value up on its next wake.
    """
    set_guc("kafgres.archive_interval_ms", "0")
    time.sleep(2)
    yield
    reset_guc("kafgres.archive_interval_ms")

@pytest.fixture
def rig(request):
    topic = f"p8a-{request.node.name.replace('_', '-')[:34]}"
    sh(f"rm -rf {ARCHIVE_DIR} && mkdir -p {ARCHIVE_DIR} && chmod 0777 {ARCHIVE_DIR}")
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql("DELETE FROM kafgres_segment_archive")
    set_guc("kafgres.segment_bytes", "4096")
    time.sleep(0.5)
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    yield topic
    reset_guc("kafgres.segment_archive_command")
    reset_guc("kafgres.segment_bytes")
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql("DELETE FROM kafgres_segment_archive")
    sh(f"rm -rf {ARCHIVE_DIR}")

def fill(topic, chunks=5, per_chunk=40):
    """Roll several segments, deterministically.

    One `kcat -P` run is one flush and often one batch, and **a batch never spans two
    segments** — so 400 records in a single invocation can land as one 80 KiB batch in one
    4 KiB segment and roll nothing at all. How many segments appeared then depended on how
    kcat happened to batch, which made this suite pass or fail by timing. Separate
    invocations force separate batches, each larger than `segment_bytes`, so each rolls.
    """
    for c in range(chunks):
        payload = "".join(f"{'x' * 200}-{c}-{i:04d}\n" for i in range(per_chunk))
        assert kcat("-t", topic, "-P", stdin=payload).returncode == 0
    all_segments = segments_on_disk(topic)
    assert len(all_segments) >= 2, (
        f"expected the log to roll at least once; segments: {all_segments}"
    )
    return all_segments

def test_rolled_segments_reach_the_archive(rig):
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")

    shipped = int(sql("SELECT kafgres_archive_segments()"))
    assert shipped > 0, "nothing was archived"

    on_disk = segments_on_disk(topic)
    expected = [archive_name(topic, n) for n in on_disk[:-1]]
    assert archived_for(topic) == expected, (
        f"archive {archived_for(topic)} does not match the sealed segments {expected}"
    )
    for name in expected:
        assert name in archived_names(), f"{name} is recorded but not in the archive"
    tid = topic_id(topic)
    assert all(n.startswith(f"{tid}-0-") for n in archived_for(topic)), archived_for(topic)

def test_retention_does_not_reclaim_an_unarchived_segment(rig):
    """The property this feature exists for.

    A command that always fails stands in for a slow or broken archive. The segment files
    must survive — destroying data the archive never received is the silent failure this
    gate exists to prevent.

    **`log_start_offset` still advances, and that is correct.** The gate is on the unlink,
    not on the offset: retention's promise is that those records stop being readable, and
    DeleteRecords shares this path and promises exactly that — holding the watermark back
    would make it return success having made nothing unreadable. Nothing is hidden from the
    archive either, because the archiver enumerates segments from disk rather than from the
    start offset, which the last assertion here checks by shipping them afterwards.
    """
    topic = rig
    before = fill(topic)
    set_guc("kafgres.segment_archive_command", "'false'")

    assert int(sql("SELECT kafgres_archive_segments()")) == 0, "a failing command archived"

    expire(topic)
    sql("SELECT kafgres_enforce_retention()")

    assert segments_on_disk(topic) == before, (
        "retention destroyed segments the archive had not taken"
    )

    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")
    assert len(archived_for(topic)) == len(before) - 1, (
        "segments whose records were already expired could no longer be archived"
    )

def test_retention_reclaims_once_the_archive_has_them(rig):
    """The other half: the gate must open, not merely hold."""
    topic = rig
    before = fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    assert int(sql("SELECT kafgres_archive_segments()")) > 0

    expire(topic)
    sql("SELECT kafgres_enforce_retention()")

    after = segments_on_disk(topic)
    assert len(after) < len(before), (
        f"retention reclaimed nothing even though the archive holds the segments: {after}"
    )
    expected = [archive_name(topic, n) for n in before[:-1]]
    for name in expected:
        assert name in archived_names(), f"{name} was reclaimed but is not in the archive"
    assert archived_for(topic) == expected

def test_the_gate_is_off_when_no_command_is_configured(rig):
    """Archiving is opt-in, and an installation that never sets a command must not have
    its retention quietly stop working."""
    topic = rig
    before = fill(topic)
    assert sql("SHOW kafgres.segment_archive_command") == ""

    expire(topic)
    sql("SELECT kafgres_enforce_retention()")
    assert len(segments_on_disk(topic)) < len(before), (
        "retention stalled with no archive command set"
    )

def test_the_worker_archives_on_its_own(rig):
    """The one test that leaves the worker's tick on, because it is what is under test.

    Everything else drives `kafgres_archive_segments()` by hand; without this, a worker
    that never fired would look identical to one that did.
    """
    topic = rig
    sealed = fill(topic)[:-1]
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    set_guc("kafgres.archive_interval_ms", "500")
    try:
        deadline = time.time() + 40
        while time.time() < deadline:
            if archived_for(topic) == [archive_name(topic, n) for n in sealed]:
                break
            time.sleep(1)
        else:
            pytest.fail(
                f"the archiver worker never shipped them: {archived_for(topic)} vs {sealed}"
            )
    finally:
        set_guc("kafgres.archive_interval_ms", "0")
        time.sleep(2)

def test_status_reports_the_backlog(rig):
    """A failing archive stops reclamation, so the backlog has to be visible before the
    disk fills rather than after."""
    topic = rig

    def status():
        row = sql("SELECT enabled::text || '|' || archived_segments || '|' || "
                  "pending_segments FROM kafgres_archive_status()")
        enabled, archived, pending = row.split("|")
        return enabled, int(archived), int(pending), row

    _, _, baseline, _ = status()
    fill(topic)
    set_guc("kafgres.segment_archive_command", "'false'")
    sql("SELECT kafgres_archive_segments()")

    enabled, archived, pending, row = status()
    assert enabled == "true", row
    assert archived == 0
    expected = len(segments_on_disk(topic)) - 1
    assert pending - baseline == expected, (
        f"backlog {pending} (was {baseline}), sealed {expected}: {row}"
    )
    assert expected >= 1, "the log did not roll, so there was no backlog to report"

def test_two_topics_do_not_overwrite_each_others_segments(rig):
    """Segment filenames are not unique; archive names have to be.

    Every partition's first segment is `00000000000000000000.log`. With `%f` as the bare
    filename, `cp %p DIR/%f` has topic B's first segment overwrite topic A's — both exit 0,
    both get rows, and retention then unlinks both originals on the strength of one file
    that is only ever the second topic's. One log gone, the table saying it was archived.

    Postgres's own `%f` is globally unique, which is why borrowing the shape works there and
    needs this here.
    """
    first = rig
    second = f"{first}-b"
    sql(f"SELECT kafgres_drop_topic('{second}')")
    sql(f"SELECT kafgres_create_topic('{second}', 1)")
    try:
        fill(first)
        fill(second)
        set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
        assert int(sql("SELECT kafgres_archive_segments()")) > 0

        a, b = archived_for(first), archived_for(second)
        assert a and b, (a, b)
        assert not set(a) & set(b), f"the two topics share archive names: {set(a) & set(b)}"
        assert len(archived_names()) >= len(a) + len(b), (
            f"{len(a)} + {len(b)} archived but only {len(archived_names())} files exist"
        )
    finally:
        sql(f"SELECT kafgres_drop_topic('{second}')")

def restore_from_archive(topic):
    """Put the archive's segments back where the broker looks for them.

    A rename, not a copy: the archive stores `<topic>-<partition>-<base>.log`, because
    segment filenames are the base offset alone and every partition's first segment would
    collide in a flat archive. Getting this wrong is the most likely way to restore
    nothing and believe otherwise.
    """
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    sh(f"find $PGDATA/kafgres/{tid}/0 -type f -delete")
    sh(f'for f in {ARCHIVE_DIR}/{tid}-0-*.log; do '
       f'cp "$f" "$PGDATA/kafgres/{tid}/0/$(basename "$f" | sed "s/^{tid}-0-//")"; done')
    sh(f"chown -R postgres:postgres $PGDATA/kafgres/{tid}/0")
    return tid

def test_a_restored_node_reconciles_its_tail(rig):
    """The whole point of archiving: the records come back, and the gap is reported.

    The archive holds only *sealed* segments — the active one is still being written and is
    never shipped — so a restored log ends earlier than the metadata the basebackup carried.
    That is the routine case, not the disaster case, and the reconciliation has to handle it
    without anyone intervening.
    """
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    assert int(sql("SELECT kafgres_archive_segments()")) > 0

    sql(f"BEGIN; SELECT kafgres_produce('{topic}', 'k', 'v'); COMMIT")
    assert sql(f"""SELECT count(*) FROM kafgres_markers m JOIN kafgres_topics t
                     USING (topic_id) WHERE t.name = '{topic}'""") == "1"

    archived_count = len(archived_for(topic))
    restore_from_archive(topic)
    compose("restart", "postgres", timeout=300)
    time.sleep(16)

    assert len(segments_on_disk(topic)) == archived_count
    out = kcat("-C", "-t", topic, "-o", "beginning", "-e", "-q")
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip(), "the restored segments served nothing"

    assert sql(f"""SELECT count(*) FROM kafgres_markers m JOIN kafgres_topics t
                     USING (topic_id) WHERE t.name = '{topic}'""") == "0", (
        "a marker outlived the records it points at"
    )

def test_restore_check_reports_what_the_two_halves_disagree_about(rig):
    """An operator needs the facts *before* trusting the node, not from a consumer later.

    `reconcile_markers` repairs the marker half at startup and says so, but by then the
    decision — restore more, or accept the loss — has already been made for them.
    """
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")
    sql(f"BEGIN; SELECT kafgres_produce('{topic}', 'k', 'v'); COMMIT")

    restore_from_archive(topic)

    findings = sql("SELECT finding FROM kafgres_restore_check()")
    assert "markers past the log end" in findings, findings

    compose("restart", "postgres", timeout=300)
    time.sleep(16)
    assert "markers past the log end" not in sql(
        "SELECT coalesce(string_agg(finding, ','), '') FROM kafgres_restore_check()"
    )

def test_a_missing_segment_is_reported_rather_than_read_as_a_gap(rig):
    """A partial restore must not look like retention having done its job."""
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")

    tid = restore_from_archive(topic)
    oldest = sorted(segments_on_disk(topic))[0]
    sh(f"rm -f $PGDATA/kafgres/{tid}/0/{oldest}")

    findings = sql("SELECT finding FROM kafgres_restore_check()")
    assert "archived segment not restored" in findings, findings

def test_a_restore_the_server_cannot_read_is_reported(rig):
    """The most likely restore mistake must not make the check go quiet.

    `cp` gives the destination the source's mode, and segments are 0600 — so a restore run
    as root produces a directory full of files owned by root that the server cannot open.
    Every other check in `kafgres_restore_check()` needs to read the log to answer, so the
    partition that is actually broken is exactly the one that would report nothing.
    """
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")

    tid = restore_from_archive(topic)
    sh(f"chown -R root:root $PGDATA/kafgres/{tid}/0")

    rows = sql("SELECT finding || ': ' || detail FROM kafgres_restore_check()")
    assert "log unreadable" in rows, rows
    assert "0600" in rows and "root" in rows, rows

    sh(f"chown -R postgres:postgres $PGDATA/kafgres/{tid}/0")
    assert "log unreadable" not in sql(
        "SELECT coalesce(string_agg(finding, ','), '') FROM kafgres_restore_check()"
    )

def epoch_of(topic):
    return int(sql(f"""SELECT leader_epoch FROM kafgres_partitions p
                         JOIN kafgres_topics t USING (topic_id)
                        WHERE t.name = '{topic}' AND p.partition = 0"""))

def test_a_rewound_log_is_reported(rig):
    """A restore that lands less log than the node had is a divergence.

    Only sealed segments are archived, so the restored log ends short — the routine case,
    not the disaster case. New produces then re-issue the offsets that were lost. Nothing
    about this is a promotion, so the timeline does not move and `raise_leader_epochs`
    leaves the epoch alone; the re-issued offsets get stamped with the same epoch the lost
    records carried.

    A consumer holding `(epoch, offset)` from before the restore then asks
    `OffsetForLeaderEpoch`, is told its epoch is still current, and resumes at its old
    position reading *different records under the same coordinates*, with no error raised
    anywhere.

    **This test pins the report, not a repair.** The broker deliberately does not mint an
    epoch for a rewind: epochs are timeline-derived, so a locally computed `current + 1`
    consumes the number the next promotion needs and silently disarms the real failover
    bump. What is asserted here is that the condition is *visible* rather than silent.
    """
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")

    out = kafka_tool("kafka-consumer-groups.sh", "--group", "p8a-restore-group",
                     "--topic", f"{topic}:0", "--reset-offsets", "--to-latest", "--execute")
    assert out.returncode == 0, out.stderr
    committed = int(sql(f"""SELECT committed_offset FROM kafgres_offsets o
                             JOIN kafgres_topics t USING (topic_id)
                            WHERE t.name = '{topic}' AND o.partition = 0"""))
    assert committed > 0, "the group committed nothing, so the rewind has no durable trace"

    before = epoch_of(topic)
    restore_from_archive(topic)
    assert "log rewound" in sql("SELECT finding FROM kafgres_restore_check()")

    compose("restart", "postgres", timeout=300)
    time.sleep(16)

    detail = sql("SELECT detail FROM kafgres_restore_check() WHERE finding = 'log rewound'")
    assert "re-issue the offsets" in detail, detail
    assert epoch_of(topic) == before, (
        "something raised the leader epoch on a rewind; if that was deliberate, check it "
        "against raise_leader_epochs' timeline arithmetic before keeping it"
    )

def test_an_ordinary_restart_does_not_raise_the_epoch(rig):
    """The control for the test above, and the one that matters more.

    Nothing may raise the leader epoch on an ordinary restart. If anything did, the epoch
    would climb every time the broker came back, every consumer would be told to truncate
    for no reason, and — because epochs are timeline-derived — it would consume the numbers
    real promotions need. An archived log on an intact node is the case most likely to trip
    a rewind check into thinking otherwise: the
    archive holds real rows, and the active segment it does *not* hold is exactly the range
    a naive check would read as missing.
    """
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")
    assert archived_for(topic), "nothing was archived, so this proves nothing"

    before = epoch_of(topic)
    compose("restart", "postgres", timeout=300)
    time.sleep(16)

    assert epoch_of(topic) == before, "an ordinary restart raised the leader epoch"
    assert sql("SELECT coalesce(string_agg(finding, ','), '') "
               "FROM kafgres_restore_check()") == ""

def test_dropping_a_topic_reaps_its_archive_rows(rig):
    """Every other per-topic table is cleaned on drop; this one was not.

    Topic ids are reused (`storage/pmeta.rs` says so explicitly), and a reused id inherits
    these rows as a claim that segments it never wrote are already archived. Two things then
    go wrong at once: `archived_bases` lets retention unlink the new topic's segments on the
    strength of the old topic's backup, and the rewind check reads them as a log that ended
    further than it does.
    """
    topic = rig
    fill(topic)
    set_guc("kafgres.segment_archive_command", f"'cp %p {ARCHIVE_DIR}/%f'")
    sql("SELECT kafgres_archive_segments()")

    tid = topic_id(topic)
    rows = f"SELECT count(*) FROM kafgres_segment_archive WHERE topic_id = {tid}"
    assert int(sql(rows)) > 0, "nothing was archived, so this proves nothing"

    sql(f"SELECT kafgres_drop_topic('{topic}')")
    assert sql(rows) == "0", "a dropped topic left its archive rows behind"
    sql(f"SELECT kafgres_create_topic('{topic}', 1)")
    assert sql("SELECT count(*) FROM kafgres_segment_archive WHERE topic_id = "
               f"{topic_id(topic)}") == "0"

def test_a_rewind_finding_names_the_evidence_it_rests_on(rig):
    """The three rewind signals are not equally trustworthy, so the finding says which fired.

    `OffsetCommit` is not validated against the log end — not here and not in Kafka — so a
    client can commit an offset it never read, and a group pre-seeded from another cluster
    before a backfill does exactly that. That commit then reads as a rewind on a perfectly
    healthy partition, permanently, until the log grows past it. A marker or an archive row
    cannot be wrong in that way: they mean this node wrote records at those offsets.

    The committed offset is written here in SQL rather than over the wire because
    `kafka-consumer-groups.sh` clamps out-of-range offsets *client-side* — which is itself
    the point: the broker never checks, so the tool has to.
    """
    topic = rig
    fill(topic)
    tid = topic_id(topic)
    sql(f"""INSERT INTO kafgres_offsets
              (group_id, topic_id, partition, committed_offset, committed_leader_epoch)
            VALUES ('preseeded-from-elsewhere', {tid}, 0, 1000000, -1)""")

    detail = sql("SELECT detail FROM kafgres_restore_check() WHERE finding = 'log rewound'")
    assert detail, "the bogus commit did not produce a finding, so this test proves nothing"
    assert "preseeded-from-elsewhere" in detail, detail
    assert "commit marker" not in detail, detail
    assert "the archive holds" not in detail, detail

    sql(f"DELETE FROM kafgres_offsets WHERE topic_id = {tid}")
    assert sql("SELECT coalesce(string_agg(finding, ','), '') "
               "FROM kafgres_restore_check()") == ""

def test_a_rewind_finding_counts_rather_than_lists_a_long_history(rig):
    """Naming the evidence must not mean aggregating unbounded text.

    `kafgres_segment_archive` rows are never reclaimed, so they accumulate over a
    partition's whole lifetime — and the predicate is `base_offset >= log_end`, so on the
    case this check exists for (a partition directory that was not restored at all, where
    the log end is 0) it matches every row the partition has ever had. Across many
    partitions that is a backend OOM during a restore, which is exactly when the operator
    needs the answer and the database can least afford to lose a process.
    """
    topic = rig
    fill(topic)
    tid = topic_id(topic)
    sql(f"""INSERT INTO kafgres_segment_archive (topic_id, partition, base_offset, bytes)
            SELECT {tid}, 0, g * 1000000, 1
              FROM generate_series(1, 500) g
            ON CONFLICT DO NOTHING""")

    detail = sql("SELECT detail FROM kafgres_restore_check() WHERE finding = 'log rewound'")
    assert detail, "the seeded rows did not produce a finding, so this proves nothing"
    assert "500 segment(s)" in detail, detail
    assert "and 490 more" in detail, detail
    assert detail.count("000000") <= 12, f"too many offsets listed: {detail}"
