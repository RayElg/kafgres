"""Kafka's own transactions (EOS).

Not the same thing as `kafgres_produce()`, and the distinction matters:

* `kafgres_produce()` puts a record in the *caller's Postgres transaction*. That is what
  this project exists for and what Kafka structurally cannot do.
* These are the wire protocol's transactions — `transactional.id`, `AddPartitionsToTxn`,
  `EndTxn` — which let a *client* write atomically across partitions. Kafka Streams
  `exactly_once_v2` needs them and nothing else here provides them.

Driven by a real client rather than hand-built frames: the clients are the
specification, and a hand-rolled request proves we can talk to ourselves.
"""

import os
import subprocess

import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"

def sql(query, timeout=60):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres",
         "-d", "postgres", "-tAc", query],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    return out.stdout.strip()

def run_txn(topic, outcome, timeout=180):
    """Sarama, because kafka-python-ng has no transaction support — it rejects
    `transactional_id` as an unrecognised config. A protocol feature can only be tested by
    a client that speaks it."""
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "sarama-conformance", BROKER, f"txn-{outcome}", topic],
        capture_output=True, text=True, timeout=timeout,
    )

def consume(topic, isolation="read_committed", timeout=60):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-C", "-e", "-q", "-o", "beginning",
         "-X", f"isolation.level={isolation}", "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=timeout,
    )
    got = {}
    for line in out.stdout.splitlines():
        if "\t" in line:
            off, value = line.split("\t", 1)
            got[int(off)] = value
    return got

def engine():
    return sql("SHOW kafgres.storage_engine")

@pytest.fixture
def topic():
    name = "txn-eos"
    clear_txn_state()
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")
    clear_txn_state()

def clear_txn_state():
    sql("DELETE FROM kafgres_txn_partitions")
    sql("DELETE FROM kafgres_txn_offsets")
    sql("DELETE FROM kafgres_txn_aborted")
    sql("DELETE FROM kafgres_txns")

def test_init_transactions_is_accepted(topic):
    """The construction-time gate.

    A transactional producer calls `InitProducerId` with its `transactional.id` and then
    `AddPartitionsToTxn` before its first send. Either being unsupported fails the
    producer before any record moves, which is why this is asserted separately from the
    outcome tests — a failure here means none of the rest is even reachable.
    """
    out = run_txn(topic, "commit")
    assert "OK commit" in out.stdout, f"transactional producer failed: {out.stderr[-800:]}"

def test_a_committed_transaction_is_visible(topic):
    out = run_txn(topic, "commit")
    assert "OK commit" in out.stdout, out.stderr[-800:]

    got = consume(topic)
    assert sorted(got.values()) == ["commit-0", "commit-1", "commit-2"], got

def test_an_aborted_transaction_is_not_visible(topic):
    """The half that matters, and the one the broker cannot fake.

    An aborted transaction's records are physically in the log — Kafka does not rewrite
    history — and the *client* drops them using the abort marker plus the aborted-
    transaction list in the Fetch response. So this asserts on what a `read_committed`
    consumer sees, not on what the broker stored.
    """
    out = run_txn(topic, "abort")
    assert "OK abort" in out.stdout, out.stderr[-800:]

    assert consume(topic) == {}, (
        "an aborted transaction's records reached a read_committed consumer"
    )
    assert consume(topic, "read_uncommitted") != {}, (
        "the records are gone entirely; the broker rewrote the log instead of marking it"
    )

def test_a_marker_reaches_the_log(topic):
    """`EndTxn` must write a control record, not merely update coordinator state.

    A consumer learns a transaction's outcome by reading the partition. Recording it only
    in the coordinator's tables would leave the Last Stable Offset stuck behind the
    transaction forever, and every `read_committed` consumer of that partition would
    silently stop seeing new records.
    """
    run_txn(topic, "commit")
    offsets = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-Q", "-t", f"{topic}:0:-1"],
        capture_output=True, text=True, timeout=60,
    ).stdout
    assert offsets.strip(), "could not read the log end"

    assert sql("SELECT count(*) FROM kafgres_txns WHERE state = 'committed'") != "0", (
        "the coordinator did not record the outcome"
    )
    assert sql("SELECT count(*) FROM kafgres_txn_partitions") == "0", (
        "the transaction's partition list outlived it; a retry would write markers nowhere"
    )

def test_read_uncommitted_sees_what_read_committed_hides(topic):
    """The two isolation levels must actually differ, on both engines: if the aborted
    list is empty and the LSO is just the high watermark, a `read_committed` consumer
    sees the aborted records and this test's two reads come back identical."""
    run_txn(topic, "abort")
    committed = consume(topic, isolation="read_committed")
    uncommitted = consume(topic, isolation="read_uncommitted")
    assert committed == {}, f"read_committed saw aborted records: {committed}"
    assert uncommitted, (
        "read_uncommitted saw nothing; the records were never written, so this test "
        "would pass even with a broken aborted list"
    )

def test_offsets_committed_in_a_transaction_become_visible_with_it(topic):
    """The read-process-write loop, which is what `exactly_once_v2` actually needs.

    A transaction that writes output and commits the input offsets it consumed must do
    both or neither. Asserted on `kafgres_offsets` — the table a rejoining consumer reads
    — rather than on the staging table, because staging is an implementation detail and
    what matters is what the next consumer of that group sees.
    """
    out = run_txn(topic, "offsets-commit")
    assert "OK offsets-commit" in out.stdout, out.stderr[-800:]

    committed = sql("SELECT committed_offset FROM kafgres_offsets WHERE group_id = 'eos-group'")
    assert committed == "42", f"the transaction's offsets did not become visible: {committed!r}"
    assert sql("SELECT count(*) FROM kafgres_txn_offsets") == "0", (
        "staged offsets outlived the transaction"
    )

def test_offsets_staged_by_an_aborted_transaction_never_become_visible(topic):
    """The half that would silently lose data.

    An offset committed by an aborted transaction tells the group it already processed
    input that produced no output. The next consumer skips it, and nothing anywhere
    reports an error — a read-process-write pipeline quietly drops records.
    """
    sql("DELETE FROM kafgres_offsets WHERE group_id = 'eos-group'")
    out = run_txn(topic, "offsets-abort")
    assert "OK offsets-abort" in out.stdout, out.stderr[-800:]

    assert sql("SELECT count(*) FROM kafgres_offsets WHERE group_id = 'eos-group'") == "0", (
        "an aborted transaction's offsets were committed; the group will skip input it "
        "never produced output for"
    )
    assert sql("SELECT count(*) FROM kafgres_txn_offsets") == "0", "staged offsets leaked"

def run_txn_bg(topic, outcome):
    """Start a scenario that holds its transaction open and return once it says so.

    The other tests observe the log only after the outcome is decided, which is the one
    state where the LSO does not matter.
    """
    p = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "sarama-conformance", BROKER, f"txn-{outcome}", topic],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    for _ in range(60):
        line = p.stdout.readline()
        if not line:
            break
        if line.startswith("OK "):
            return p
        if line.startswith("ERROR"):
            p.kill()
            raise AssertionError(f"{outcome}: {line.strip()}")
    p.kill()
    raise AssertionError(f"{outcome} never reported ready")

def test_an_in_flight_transaction_holds_the_last_stable_offset(topic):
    """Records must not reach a `read_committed` consumer before the outcome exists.

    Delivering them early cannot be walked back — the consumer has already acted on
    them when the abort arrives. This is what the LSO is for, and it is the one
    transaction state the rest of this file never enters.
    """
    p = run_txn_bg(topic, "open")
    try:
        got = consume(topic, isolation="read_committed")
        assert got == {}, f"read_committed saw an undecided transaction's records: {got}"
        assert consume(topic, isolation="read_uncommitted"), (
            "read_uncommitted saw nothing either, so the records were never written and "
            "this test would pass against a broker with no LSO at all"
        )
    finally:
        p.kill()
        p.wait(timeout=30)

def test_a_second_transaction_does_not_drag_the_lso_back(topic):
    """The LSO must name the *open* transaction, not the producer's first one ever.

    Derived from the log — `MIN(base_offset)` over a producer's transactional batches —
    it spans every transaction that producer ever ran, so opening a second one pulls the
    LSO back below records that are already committed and already delivered. Consumers
    then stall for as long as the producer keeps transacting, which for a Streams
    application with the default commit interval is permanently. Nothing errors.
    """
    p = run_txn_bg(topic, "second")
    try:
        got = consume(topic, isolation="read_committed")
        assert set(got.values()) == {"first-0", "first-1", "first-2"}, (
            f"expected the committed transaction's records and nothing else, got {got}"
        )
    finally:
        p.kill()
        p.wait(timeout=30)

def test_an_aborted_transaction_larger_than_one_fetch_is_still_dropped(topic):
    """The aborted list cannot be derived from the batches a single response happens to
    hold: past the fetch cap, a transaction's first batch and its marker land in
    different responses, so a list built from what one response holds comes back empty
    and a `read_committed` consumer is handed aborted records as committed.

    The trailing committed record is what keeps this test honest: asserting it *arrives*
    under the same cap distinguishes a working consumer from an absent one.
    """
    out = run_txn(topic, "abort-big")
    assert "OK abort-big" in out.stdout, out.stderr[-800:]
    produce_plain(topic, "after")

    got = consume_capped(topic)
    assert set(got.values()) == {"after"}, (
        f"expected only the committed record under a split fetch, got {sorted(got.values())}"
    )

def produce_plain(topic, value):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", "-i", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-P"],
        input=value, capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr[-400:]

def consume_capped(topic, timeout=60):
    """Consume with a fetch cap small enough to split the transaction across responses.

    1000 is librdkafka's floor for `message.max.bytes`, and `fetch.max.bytes` must be at
    least that — which is why the cap is this value and not something smaller.
    """
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", BROKER, "-t", topic, "-C", "-e", "-q", "-o", "beginning",
         "-X", "isolation.level=read_committed",
         "-X", "message.max.bytes=1000",
         "-X", "fetch.max.bytes=1000",
         "-X", "max.partition.fetch.bytes=1000",
         "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=timeout,
    )
    got = {}
    for line in out.stdout.splitlines():
        if "\t" in line:
            off, value = line.split("\t", 1)
            got[int(off)] = value
    return got

def test_an_abandoned_transaction_is_expired_rather_than_blocking_the_partition(topic):
    """A producer that dies mid-transaction must not stop consumption forever.

    The LSO is held at the first record of the oldest open transaction, so a producer
    SIGKILLed between its first send and its commit takes every `read_committed`
    consumer of every partition it touched down with it — and the symptom is silence,
    which is what a caught-up consumer looks like too. Kafka's coordinator aborts on
    `transaction.timeout.ms`; so must this broker.
    """
    p = run_txn_bg(topic, "open")
    p.kill()
    p.wait(timeout=30)

    assert consume(topic, isolation="read_committed") == {}, (
        "the abandoned transaction's records were visible before it expired"
    )

    sql("UPDATE kafgres_txns SET started_at = started_at - timeout_ms - 1000 "
        "WHERE state = 'ongoing'")
    assert sql("SELECT kafgres_expire_transactions()") != "0", "nothing was expired"

    assert sql("SELECT count(*) FROM kafgres_txns WHERE state = 'ongoing'") == "0"
    assert consume(topic, isolation="read_committed") == {}, (
        "an expired transaction's records became visible; expiry must abort, not commit"
    )
    produce_plain(topic, "after-expiry")
    got = consume(topic, isolation="read_committed")
    assert set(got.values()) == {"after-expiry"}, (
        f"the partition is still stalled behind the expired transaction: {got}"
    )

def test_a_new_transaction_starts_its_own_clock(topic):
    """The expiry deadline must date from when *this* transaction began.

    A producer reuses one `kafgres_txns` row, so an upsert that leaves `started_at` alone
    makes every transaction after the first inherit the first one's start time — and the
    expiry sweep then aborts a perfectly live transaction. The producer sees its commit
    fail for no reason it can observe, and only after it has been running long enough for
    the difference to matter, which is not when anyone is watching.

    The scenario waits four seconds between its two transactions; a two-second timeout is
    therefore expired under the stale clock and live under the correct one.
    """
    p = run_txn_bg(topic, "second")
    try:
        assert sql("SELECT count(*) FROM kafgres_txns WHERE state = 'ongoing'") == "1"
        sql("UPDATE kafgres_txns SET timeout_ms = 2000 WHERE state = 'ongoing'")
        assert sql("SELECT kafgres_expire_transactions()") == "0", (
            "a live transaction was expired; its deadline is being measured from an "
            "earlier transaction"
        )
        assert sql("SELECT count(*) FROM kafgres_txns WHERE state = 'ongoing'") == "1"
    finally:
        p.kill()
        p.wait(timeout=30)

def test_expiry_fences_the_producer_it_gave_up_on(topic):
    """Expiring a transaction without fencing its producer is worse than not expiring it.

    The producer is not necessarily dead — it may be slow — and nothing tells it we gave
    up. Unfenced, it keeps producing into a transaction the broker already aborted: those
    later batches get no `first_offset`, so nothing holds the LSO for them and nothing
    puts them in the abort index; its eventual `EndTxn(commit)` finds no partitions,
    writes no markers, and returns success.

    So the assertion is that the epoch moved, which is what makes the producer's next
    request fail instead of silently landing in the wrong transaction.
    """
    p = run_txn_bg(topic, "open")
    try:
        before = int(sql("SELECT producer_epoch FROM kafgres_producers "
                         "WHERE transactional_id = 'kafgres-eos-open'"))
        sql("UPDATE kafgres_txns SET started_at = started_at - timeout_ms - 1000 "
            "WHERE state = 'ongoing'")
        assert sql("SELECT kafgres_expire_transactions()") == "1"

        after = int(sql("SELECT producer_epoch FROM kafgres_producers "
                        "WHERE transactional_id = 'kafgres-eos-open'"))
        assert after > before, (
            f"expiry did not fence the producer (epoch {before} -> {after}); it can still "
            "write into the transaction that was just aborted"
        )
    finally:
        p.kill()
        p.wait(timeout=30)

def test_a_new_transaction_does_not_inherit_a_stale_first_offset(topic):
    """Leftover partition rows must not survive into the next transaction.

    The segment engine's marker append writes to a file outside Postgres transaction
    control, so an `EndTxn` that fails partway rolls back the row deletions while the
    marker stays on disk. `kafgres_txn_partitions` — `first_offset` included — is then
    still there when the producer's next transaction starts, and `ON CONFLICT DO NOTHING`
    carries the *previous* transaction's first offset into it. That holds the LSO below
    records already committed and served, and puts them inside the abort range the new
    transaction writes if it rolls back: committed records dropped by every consumer.

    The leftover is injected directly rather than by engineering a partial failure. What
    matters is that a row with a stale `first_offset` is present when the next transaction
    begins, not how it got there.
    """
    out = run_txn(topic, "commit")
    assert "OK commit" in out.stdout, out.stderr[-800:]
    producer = sql("SELECT producer_id FROM kafgres_producers "
                   "WHERE transactional_id = 'kafgres-eos-test'")
    topic_oid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
    sql(f"INSERT INTO kafgres_txn_partitions (producer_id, topic_id, partition, first_offset) "
        f"VALUES ({producer}, {topic_oid}, 0, 0) "
        f"ON CONFLICT (producer_id, topic_id, partition) DO UPDATE SET first_offset = 0")

    out = run_txn(topic, "abort")
    assert "OK abort" in out.stdout, out.stderr[-800:]

    first = sql("SELECT COALESCE(MIN(first_offset), -1) FROM kafgres_txn_aborted")
    assert first != "0", (
        "the aborted range starts at the stale first offset, so the previous "
        "transaction's committed records are inside it and consumers will drop them"
    )
    got = consume(topic, isolation="read_committed")
    assert set(got.values()) == {"commit-0", "commit-1", "commit-2"}, got
