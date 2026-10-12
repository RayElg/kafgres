"""The broker lets go of dropped partitions' files and refuses clients when out of descriptors."""

import os
import socket
import struct
import subprocess
import time

import pytest

from conftest import sql
from support.docker import REPO

HOST, PORT = "127.0.0.1", 9092
API_VERSIONS = 18

pytestmark = pytest.mark.skipif(
    sql("SHOW kafgres.storage_engine") != "segment",
    reason="engine B only: the table engine holds no segment files",
)


def in_container(command):
    return subprocess.run(["docker", "compose", "exec", "-T", "-u", "postgres", "postgres", "sh", "-c", command],
                          capture_output=True, text=True, timeout=60, cwd=REPO).stdout.strip()


def cpu_ticks(pid):
    fields = in_container(f"cat /proc/{pid}/stat").rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def broker_pid():
    return sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'kafgres_broker'").strip()


def api_versions_ok(timeout=10):
    request = struct.pack(">hhi", API_VERSIONS, 0, 7) + struct.pack(">h", 6) + b"pytest"
    try:
        with socket.create_connection((HOST, PORT), timeout=timeout) as s:
            s.sendall(struct.pack(">i", len(request)) + request)
            head = s.recv(4)
            return len(head) == 4
    except OSError:
        return False


@pytest.fixture
def fast_retention():
    sql("ALTER SYSTEM SET kafgres.retention_check_interval_ms = 1000")
    sql("SELECT pg_reload_conf()")
    time.sleep(1.5)
    yield
    sql("ALTER SYSTEM RESET kafgres.retention_check_interval_ms")
    sql("SELECT pg_reload_conf()")


def test_a_topic_dropped_from_sql_does_not_stay_open_in_the_broker(fast_retention):
    """A topic dropped from SQL is closed in the broker too."""
    topic = "fds-dropped"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    sql(f"SELECT kafgres_create_topic('{topic}', 8)")
    out = subprocess.run(["docker", "run", "--rm", "-i", "--network", "host", "kafgres-clients", "kcat",
                          "-b", f"{HOST}:{PORT}", "-P", "-t", topic],
                         input="".join(f"m{i}\n" for i in range(800)), capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    tid = sql(f"SELECT topic_id FROM kafgres_topics WHERE name = '{topic}'").strip()
    pid = broker_pid()
    held = in_container(f"ls -l /proc/{pid}/fd | grep -c '/kafgres/{tid}/' || true")
    assert int(held) > 0, "the broker held none of the topic's files, so this proves nothing"
    sql(f"SELECT kafgres_drop_topic('{topic}')")
    deadline = time.time() + 30
    while time.time() < deadline:
        if in_container(f"ls -l /proc/{pid}/fd | grep -c '/kafgres/{tid}/' || true") == "0":
            return
        time.sleep(1)
    raise AssertionError(in_container(f"ls -l /proc/{pid}/fd | grep '/kafgres/{tid}/'"))


def test_running_out_of_descriptors_refuses_clients_without_spinning():
    """Under a lowered limit accept fails with EMFILE: the broker refuses without spinning,
    logs once, and accepts again once descriptors free up."""
    pid = broker_pid()
    original = in_container(f"prlimit --pid {pid} --nofile --output SOFT,HARD --noheadings").split()
    open_now = int(in_container(f"ls /proc/{pid}/fd | wc -l"))
    in_container(f"prlimit --pid {pid} --nofile={open_now + 20}:{original[1]}")
    lowered = in_container(f"prlimit --pid {pid} --nofile --output SOFT --noheadings")
    assert lowered == str(open_now + 20), f"prlimit did not take: {lowered}"
    held = []
    try:
        for _ in range(60):
            try:
                held.append(socket.create_connection((HOST, PORT), timeout=2))
            except OSError:
                pass
        time.sleep(3)
        ticks = cpu_ticks(pid)
        time.sleep(5)
        busy = (cpu_ticks(pid) - ticks) / 5
        assert busy < 50, f"the broker spent {busy:.0f} clock ticks per second while out of descriptors"
        logs = subprocess.run(["docker", "compose", "logs", "--since", "30s", "postgres"],
                              capture_output=True, text=True, cwd=REPO).stdout
        refusals = logs.count("out of file descriptors")
        assert 1 <= refusals <= 3, f"{refusals} refusal log lines: none means no EMFILE was hit"
        assert "accept error" not in logs, "EMFILE fell through to the generic accept error"
    finally:
        for s in held:
            s.close()
        in_container(f"prlimit --pid {pid} --nofile={original[0]}:{original[1]}")
    time.sleep(2)
    assert broker_pid() == pid, "the broker restarted while out of descriptors"
    assert api_versions_ok(), "the broker did not accept again once descriptors were free"
