"""Switching storage engines must not hide a log.

`kafgres.storage_engine` is a postmaster GUC, so changing it is a restart. The old log
is then intact and invisible: a consumer sees an empty topic, which is exactly what
being caught up looks like. These tests restart the container, so they are few.
"""
import subprocess
import time

import pytest

BROKER = "127.0.0.1:9092"
CLIENTS = "kafgres-clients"

def sql(q, timeout=60):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-tAc", q],
        capture_output=True, text=True, timeout=timeout,
    )
    return out.stdout.strip()

def set_engine(engine, mismatch_ok=False):
    sql(f"ALTER SYSTEM SET kafgres.storage_engine='{engine}'")
    sql(f"ALTER SYSTEM SET kafgres.allow_engine_mismatch={'on' if mismatch_ok else 'off'}")
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate"],
                   capture_output=True, timeout=300)
    for _ in range(40):
        if sql("SELECT 1") == "1":
            return
        time.sleep(2)
    raise AssertionError("postgres did not come back")

def broker_reachable(timeout=25):
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER, "-L"],
        capture_output=True, text=True, timeout=timeout,
    )
    return out.returncode == 0

def wipe_both_logs():
    """Clear both engines' logs, with the guard disabled so the broker can start at all.

    These tests deliberately leave a log the running engine cannot see.
    """
    sql("ALTER SYSTEM SET kafgres.allow_engine_mismatch=on")
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate"],
                   capture_output=True, timeout=300)
    for _ in range(40):
        if sql("SELECT 1") == "1":
            break
        time.sleep(2)
    sql("DELETE FROM kafgres_log")
    subprocess.run(["docker", "compose", "exec", "-T", "postgres", "bash", "-c",
                    'rm -rf "$PGDATA"/kafgres/*'], capture_output=True, timeout=120)

@pytest.fixture
def restore_engine():
    before = sql("SHOW kafgres.storage_engine")
    wipe_both_logs()
    set_engine(before)
    yield
    wipe_both_logs()
    set_engine(before)

def test_the_broker_refuses_to_serve_a_log_the_engine_cannot_see(restore_engine):
    """Refusing to listen is unmistakable; serving empty topics is unfalsifiable."""
    set_engine("table")
    sql("SELECT kafgres_drop_topic('cliff')")
    sql("SELECT kafgres_create_topic('cliff', 1)")
    subprocess.run(["docker", "run", "--rm", "--network", "host", "-i", CLIENTS,
                    "kcat", "-b", BROKER, "-t", "cliff", "-P"],
                   input="precious", capture_output=True, text=True, timeout=60)
    assert sql("SELECT count(*) FROM kafgres_log") != "0", "nothing was written to compare against"

    set_engine("segment")
    assert not broker_reachable(), (
        "the broker served requests while the table engine's log was invisible to it; "
        "every topic in it reads as empty with no error"
    )
    assert sql("SELECT count(*) FROM kafgres_log") != "0", "the guard destroyed the log it exists to protect"

    set_engine("table")
    assert broker_reachable()
    out = subprocess.run(
        ["docker", "run", "--rm", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         "-t", "cliff", "-C", "-e", "-q", "-o", "beginning"],
        capture_output=True, text=True, timeout=60,
    )
    assert "precious" in out.stdout, out.stdout

def test_the_operator_can_override_and_strand_the_log(restore_engine):
    """`allow_engine_mismatch` is a decision, not a workaround, so it has to actually work."""
    set_engine("table")
    sql("SELECT kafgres_drop_topic('cliff')")
    sql("SELECT kafgres_create_topic('cliff', 1)")
    subprocess.run(["docker", "run", "--rm", "--network", "host", "-i", CLIENTS,
                    "kcat", "-b", BROKER, "-t", "cliff", "-P"],
                   input="expendable", capture_output=True, text=True, timeout=60)

    set_engine("segment", mismatch_ok=True)
    assert broker_reachable(), "the override did not let the broker start"

def broker_error():
    out = subprocess.run(["docker", "compose", "logs", "--tail", "400", "postgres"],
                         capture_output=True, text=True, timeout=120)
    lines = [l for l in out.stdout.splitlines() if "kafgres.storage_engine is" in l]
    return lines[-1] if lines else ""

def test_a_log_under_both_engines_is_described_as_such(restore_engine):
    """The advice has to be right in the state the escape hatch creates."""
    set_engine("table")
    sql("SELECT kafgres_create_topic('both-a', 1)")
    subprocess.run(["docker", "run", "--rm", "--network", "host", "-i", CLIENTS,
                    "kcat", "-b", BROKER, "-t", "both-a", "-P"],
                   input="a", capture_output=True, text=True, timeout=60)

    set_engine("segment", mismatch_ok=True)
    assert broker_reachable(), "the override did not let the broker start"
    sql("SELECT kafgres_create_topic('both-b', 1)")
    subprocess.run(["docker", "run", "--rm", "--network", "host", "-i", CLIENTS,
                    "kcat", "-b", BROKER, "-t", "both-b", "-P"],
                   input="b", capture_output=True, text=True, timeout=60)

    set_engine("segment")
    assert not broker_reachable()
    msg = broker_error()
    assert "both" in msg, f"the message does not say both engines hold a log: {msg}"
    assert "no migration between engines" in msg, (
        f"the message does not tell the operator what to actually do: {msg}"
    )
