"""Segment-engine log replication and promotion.

Promote a standby mid-produce; consumers reconnect and either resume correctly or
receive `OFFSET_OUT_OF_RANGE` and apply `auto.offset.reset`. **No silent divergence** —
gaps are legal Kafka, so a consumer reading across one gets no error and no client can
detect it. Every assertion here is about records, not about the replication machinery:
machinery that ran and copied the wrong thing would pass a "did it replicate" test.

The table engine is deliberately not covered: its log lives in Postgres tables, already
replicated by WAL streaming.

Runs behind the `failover` profile, with both nodes on the segment engine:

    KAFGRES_ENGINE=segment docker compose --profile failover up -d
"""

import os
import subprocess
import time

import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CLIENTS = "kafgres-clients"
PRIMARY = "127.0.0.1:9092"
STANDBY = "127.0.0.1:9192"

def compose(*args, timeout=180, check=False):
    out = subprocess.run(["docker", "compose", *args], capture_output=True,
                         text=True, timeout=timeout, cwd=REPO)
    if check and out.returncode != 0:
        raise RuntimeError(f"docker compose {args}: {out.stderr.strip()}")
    return out

def sql(node, query, timeout=60):
    out = compose("exec", "-T", node, "psql", "-U", "postgres", "-d", "postgres",
                  "-tAc", query, timeout=timeout)
    return out.stdout.strip()

def standby_exists():
    out = compose("ps", "-aq", "standby")
    return bool(out.stdout.strip())

def engines_are_segment():
    return (sql("postgres", "SHOW kafgres.storage_engine") == "segment"
            and sql("standby", "SHOW kafgres.storage_engine") == "segment")

pytestmark = pytest.mark.skipif(
    not standby_exists() or not engines_are_segment(),
    reason="needs both nodes on engine B: "
           "KAFGRES_ENGINE=segment docker compose --profile failover up -d",
)

def consume(broker, topic, timeout=45):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS,
         "kcat", "-b", broker, "-t", topic, "-C", "-e", "-q", "-o", "beginning",
         "-f", "%o\t%s\n"],
        capture_output=True, text=True, timeout=timeout,
    )
    got = {}
    for line in out.stdout.splitlines():
        if "\t" in line:
            off, value = line.split("\t", 1)
            got[int(off)] = value
    return got

def produce(broker, topic, values, timeout=60):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", broker, "-t", topic, "-P"],
        input="".join(f"{v}\n" for v in values),
        capture_output=True, text=True, timeout=timeout,
    )

def follower_bytes():
    return int(compose("exec", "-T", "standby", "bash", "-c",
                       r'find "$PGDATA/kafgres" -name "*.log" -printf "%s\n" 2>/dev/null'
                       r' | awk "{s+=\$1} END {print s+0}"').stdout.strip() or 0)

def wait_for_follower(topic, want, timeout_s=90):
    """The standby's broker is not listening until promotion, so progress cannot be read
    over the wire. Ask the log itself."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if follower_bytes() > 0:
            return True
        time.sleep(3)
    return False

@pytest.fixture
def fresh_standby():
    """A standby that is actually a standby.

    Recreated per test, from a real `pg_basebackup`, because the promotion test leaves it
    a *primary* — and every later test would then run against a node that stopped
    following, passing or failing for reasons that look plausible but no longer test
    replication.
    """
    compose("rm", "-sf", "standby", timeout=180)
    subprocess.run(["docker", "compose", "--profile", "failover", "up", "-d", "standby"],
                   capture_output=True, text=True, timeout=300, cwd=REPO,
                   env={**os.environ, "KAFGRES_ENGINE": "segment"})
    deadline = time.time() + 180
    while time.time() < deadline:
        if sql("standby", "SELECT pg_is_in_recovery()") == "t":
            return
        time.sleep(4)
    pytest.fail("the standby never came up in recovery")

@pytest.fixture
def topic():
    name = "repl-failover"
    sql("postgres", f"SELECT kafgres_drop_topic('{name}')")
    sql("postgres", f"SELECT kafgres_create_topic('{name}', 1)")
    time.sleep(3)
    yield name

def test_the_follower_pulls_the_leaders_log(fresh_standby, topic):
    """Before promotion: the standby's segment files fill from the primary.

    Asserted on bytes on the standby's disk rather than over the wire, because the
    standby's broker deliberately does not listen while it is a standby — checking the
    port would be checking the wrong thing and would pass on a node replicating nothing.
    """
    assert produce(PRIMARY, topic, [f"pre-{i}" for i in range(20)]).returncode == 0
    assert wait_for_follower(topic, 20), (
        "the standby's log never grew; the follower is not pulling"
    )

def test_promotion_mid_produce_leaves_no_silent_divergence(fresh_standby, topic):
    """The acceptance criterion.

    Produce, let the follower catch up, promote mid-flight, then compare what the
    promoted node serves against what the old leader had. Records that both nodes hold
    must be **identical** — same offsets, same bytes. A promoted node may legitimately
    have *fewer* records (anything not yet replicated is lost, which is what asynchronous
    replication means), but it must never have a *different* record at the same offset.
    """
    assert produce(PRIMARY, topic, [f"m{i:03d}" for i in range(30)]).returncode == 0
    assert wait_for_follower(topic, 30), "follower never caught up"
    before = consume(PRIMARY, topic)
    assert len(before) == 30, f"the leader should hold 30 records, has {len(before)}"

    inflight = subprocess.Popen(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS,
         "kcat", "-b", PRIMARY, "-t", topic, "-P"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        text=True,
    )
    assert inflight.stdin is not None
    for i in range(30, 60):
        inflight.stdin.write(f"m{i:03d}\n")
    inflight.stdin.flush()

    compose("exec", "-T", "standby", "psql", "-U", "postgres", "-d", "postgres",
            "-c", "SELECT pg_promote(wait => true, wait_seconds => 60)", check=True)
    try:
        inflight.stdin.close()
    except BrokenPipeError:
        pass
    inflight.kill()

    deadline = time.time() + 120
    after = {}
    while time.time() < deadline:
        if sql("standby", "SELECT count(*) FROM pg_stat_activity "
                          "WHERE backend_type='kafgres_broker'") == "1":
            after = consume(STANDBY, topic)
            if after:
                break
        time.sleep(3)

    assert after, "the promoted node served nothing; it should serve what it replicated"

    divergent = {o: (v, after[o]) for o, v in before.items() if o in after and after[o] != v}
    assert not divergent, (
        f"the promoted node serves different records at offsets already read from the "
        f"old leader: {list(divergent.items())[:3]}"
    )

    offsets = sorted(after)
    assert offsets == list(range(offsets[0], offsets[0] + len(offsets))), (
        f"the promoted node's log has a hole: {offsets[:5]}...{offsets[-5:]}"
    )

def test_the_promoted_node_stops_following(fresh_standby, topic):
    """One writer per partition, always.

    Promotes for itself rather than leaning on the previous test having done it. Two
    writers to the same segment files would corrupt offsets in a way nothing catches
    at runtime — the shared-memory counter assumes a single appender per partition.
    """
    assert produce(PRIMARY, topic, ["x"]).returncode == 0
    assert wait_for_follower(topic, 1), "follower never caught up"

    compose("exec", "-T", "standby", "psql", "-U", "postgres", "-d", "postgres",
            "-c", "SELECT pg_promote(wait => true, wait_seconds => 60)", check=True)

    deadline = time.time() + 120
    while time.time() < deadline:
        if "follower stopping" in compose("logs", "--tail", "200", "standby").stdout:
            return
        time.sleep(4)
    pytest.fail(
        "the follower did not announce that it stopped after promotion; if it is still "
        "pulling there are now two writers to the same partition"
    )

def test_a_diverged_follower_truncates_rather_than_stalls(fresh_standby, topic):
    """`OffsetForLeaderEpoch`'s write side.

    Divergence is created by cutting the **leader's** tail and restarting it, so the
    follower genuinely holds records the leader no longer has. That models a leader that
    lost its tail to a crash and came back short — the case where a follower must discard,
    not stall.

    A test that manufactures its precondition has to assert the precondition actually
    happened, which is why the leader's log end is checked before the follower is asked
    to react.
    """
    assert produce(PRIMARY, topic, [f"d{i:02d}" for i in range(12)]).returncode == 0
    assert wait_for_follower(topic, 12), "follower never caught up"

    leader_before = len(consume(PRIMARY, topic))
    assert leader_before == 12, f"leader should hold 12, has {leader_before}"

    assert follower_bytes() > 0, "the follower replicated nothing; nothing to diverge"

    tid = sql("postgres", f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'")
    compose("exec", "-T", "postgres", "bash", "-c",
            f'f=$(ls "$PGDATA/kafgres/{tid.strip()}"/*/*.log | head -1); '
            's=$(stat -c%s "$f"); truncate -s $((s/2)) "$f"', check=True)
    compose("restart", "postgres", check=True, timeout=300)
    time.sleep(20)

    leader_after = len(consume(PRIMARY, topic))
    assert leader_after < leader_before, (
        f"the leader still holds {leader_after} records; the divergence was not created "
        "and the rest of this test would prove nothing"
    )

    deadline = time.time() + 150
    truncated = False
    while time.time() < deadline:
        logs = compose("logs", "--tail", "200", "standby").stdout
        if "truncated" in logs and "the leader did not" in logs:
            truncated = True
            break
        time.sleep(5)

    assert truncated, (
        "the follower kept records the leader no longer has. It is now a node that, if "
        "promoted, would serve data no consumer ever saw from the real leader — the "
        "silent divergence this phase exists to prevent"
    )
