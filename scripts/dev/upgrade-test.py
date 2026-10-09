#!/usr/bin/env python3
"""Upgrade a populated release install to the current build and check nothing moved.

    python3 scripts/dev/upgrade-test.py [--engine segment|table] [--from 0.3.0] [--keep]

Seeds a volume under the tagged release, restarts it on the current build, and checks the
same records, group offsets and timestamp lookups before and after
`ALTER EXTENSION kafgres UPDATE`. Runs on ports 15432 and 19092, beside the dev stack.
Needs Docker and the `kafgres-clients` image.
"""
import argparse
import hashlib
import os
import subprocess
import sys
import tarfile
import tempfile
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CONTAINER = "kafgres-upgrade-test"
VOLUME = "kafgres-upgrade-test"
PG_PORT, KAFKA_PORT = 15432, 19092
BROKER = f"127.0.0.1:{KAFKA_PORT}"
CLIENTS = "kafgres-clients"
KAFKA_TOOLS = "apache/kafka:4.1.0"


def run(*cmd, check=True, timeout=1800, stdin=None, cwd=None):
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, input=stdin,
                         cwd=cwd)
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:6])}...: {out.stderr.strip()[-2000:]}")
    return out


def step(msg):
    print(f"== {msg}", flush=True)


def sql(query, check=True):
    out = run("docker", "exec", CONTAINER, "psql", "-U", "postgres", "-tAc", query, check=check)
    return out.stdout.strip()


def kcat(*args, stdin=None, check=True):
    return run("docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b",
               BROKER, *args, stdin=stdin, check=check, timeout=600)


def kafka_tool(script, *args):
    return run("docker", "run", "--rm", "--network", "host", KAFKA_TOOLS,
               f"/opt/kafka/bin/{script}", "--bootstrap-server", BROKER, *args, timeout=600)


def build_images(old):
    step(f"building kafgres-postgres:{old} from its tag")
    with tempfile.TemporaryDirectory() as tmp:
        archive = os.path.join(tmp, "src.tar")
        run("git", "archive", "--format=tar", "-o", archive, old, cwd=REPO)
        src = os.path.join(tmp, "src")
        with tarfile.open(archive) as t:
            t.extractall(src)
        run("docker", "build", "-q", "-f", "docker/Dockerfile", "-t",
            f"kafgres-postgres:{old}", ".", cwd=src)
    step("building kafgres-postgres:upgrade-new from this tree")
    run("docker", "build", "-q", "-f", "docker/Dockerfile", "-t", "kafgres-postgres:upgrade-new",
        ".", cwd=REPO)


def start(image):
    run("docker", "rm", "-f", CONTAINER, check=False)
    run("docker", "run", "-d", "--name", CONTAINER, "-e", "POSTGRES_PASSWORD=postgres",
        "-p", f"127.0.0.1:{PG_PORT}:5432", "-p", f"127.0.0.1:{KAFKA_PORT}:9092",
        "-v", f"{VOLUME}:/var/lib/postgresql/data", image)
    wait_ready()


def wait_ready():
    deadline = time.time() + 180
    while time.time() < deadline:
        n = sql("SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'kafgres_broker'",
                check=False)
        if n == "1" and kcat("-L", check=False).returncode == 0:
            time.sleep(2)
            return
        time.sleep(2)
    raise RuntimeError("the broker never came up:\n" + logs()[-3000:])


def restart():
    run("docker", "restart", CONTAINER)
    wait_ready()


def logs():
    out = run("docker", "logs", CONTAINER, check=False)
    return out.stdout + out.stderr


def read_topic(topic, isolation="read_committed"):
    """(partition, offset, key, digest of value) for every record, in order."""
    out = kcat("-C", "-t", topic, "-o", "beginning", "-e", "-q",
               "-X", f"isolation.level={isolation}", "-f", "%p %o %k %s\n")
    rows = []
    for line in out.stdout.splitlines():
        parts = line.split(" ", 3)
        if len(parts) == 4:
            p, o, k, v = parts
            rows.append((int(p), int(o), k, hashlib.sha1(v.encode()).hexdigest()[:12]))
    return sorted(rows)


def offset_for_time(topic, partition, when_ms):
    out = kcat("-Q", "-t", f"{topic}:{partition}:{when_ms}")
    for line in out.stdout.splitlines():
        if "offset" in line:
            return int(line.rsplit(" ", 1)[1])
    raise RuntimeError(f"no offset in {out.stdout!r}")


def timestamps(topic, partition):
    out = kcat("-C", "-t", f"{topic}", "-p", str(partition), "-o", "beginning", "-e", "-q",
               "-f", "%T\n")
    return [int(t) for t in out.stdout.split() if t.strip().isdigit()]


def seed(engine):
    step(f"seeding under the old image, {engine} engine")
    sql(f"ALTER SYSTEM SET kafgres.storage_engine = '{engine}'")
    sql(f"ALTER SYSTEM SET kafgres.advertised_port = {KAFKA_PORT}")
    sql("ALTER SYSTEM SET kafgres.segment_bytes = 65536")
    restart()

    sql("SELECT kafgres_create_topic('up-plain', 3)")
    sql("SELECT kafgres_create_topic('up-compact', 1)")
    sql("""UPDATE kafgres_topics SET config = COALESCE(config, '{}'::jsonb)
             || '{"cleanup.policy": "compact", "segment.bytes": "65536"}'
            WHERE name = 'up-compact'""")
    pad = "p" * 200
    for wave in range(3):
        lines = "".join(f"k{i % 97}:w{wave}-{i}-{pad}\n" for i in range(1000))
        kcat("-t", "up-plain", "-P", "-K:", "-X", "enable.idempotence=true", stdin=lines)
        time.sleep(1.2)
    for wave in range(4):
        lines = "".join(f"c{i % 50}:w{wave}-{i}-{pad}\n" for i in range(500))
        # One record per batch: on the table engine a batch larger than `segment.bytes`
        # keeps the whole log in the active region.
        kcat("-t", "up-compact", "-P", "-K:", "-X", "batch.num.messages=1",
             "-X", "linger.ms=0", stdin=lines)
    kafka_tool("kafka-consumer-groups.sh", "--group", "up-group", "--topic", "up-plain",
               "--reset-offsets", "--to-offset", "400", "--execute")

    if engine == "segment":
        sql("SELECT kafgres_create_topic('up-sql', 1)")
        for i in range(5):
            sql(f"SELECT kafgres_produce('up-sql', 'k{i}', 'committed-{i}')")
        sql("BEGIN; SELECT kafgres_produce('up-sql', 'kx', 'rolled-back'); ROLLBACK")


def snapshot(engine):
    topics = ["up-plain", "up-compact"] + (["up-sql"] if engine == "segment" else [])
    ts = timestamps("up-plain", 0)
    probe = ts[len(ts) // 2]
    return {
        "records": {t: read_topic(t) for t in topics},
        "uncommitted": read_topic("up-sql", "read_uncommitted") if engine == "segment" else [],
        "groups": sql("""SELECT string_agg(partition || ':' || committed_offset, ','
                                           ORDER BY partition)
                           FROM kafgres_offsets WHERE group_id = 'up-group'"""),
        "lookup": offset_for_time("up-plain", 0, probe),
        "probe": probe,
    }


def compare(before, after, when):
    for topic, rows in before["records"].items():
        got = after["records"][topic]
        assert got == rows, f"{when}: {topic} changed: {len(rows)} records before, {len(got)} after"
    assert after["uncommitted"] == before["uncommitted"], f"{when}: read_uncommitted view changed"
    assert after["groups"] == before["groups"], (
        f"{when}: group offsets {before['groups']} became {after['groups']}")
    assert after["lookup"] == before["lookup"], (
        f"{when}: offsetsForTimes({before['probe']}) was {before['lookup']}, "
        f"now {after['lookup']}")


def epoch():
    return int(sql("""SELECT max(leader_epoch) FROM kafgres_partitions p
                        JOIN kafgres_topics t USING (topic_id) WHERE t.name = 'up-plain'"""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=["segment", "table"], default="segment")
    ap.add_argument("--from", dest="old", default="0.3.0")
    ap.add_argument("--keep", action="store_true", help="leave the container and volume")
    ap.add_argument("--no-build", action="store_true")
    args = ap.parse_args()

    if not args.no_build:
        build_images(args.old)
    run("docker", "rm", "-f", CONTAINER, check=False)
    run("docker", "volume", "rm", VOLUME, check=False)
    try:
        start(f"kafgres-postgres:{args.old}")
        old_version = sql("SELECT extversion FROM pg_extension WHERE extname = 'kafgres'")
        assert old_version == args.old, f"the old image installed {old_version}"
        seed(args.engine)
        before = snapshot(args.engine)
        old_epoch = epoch()
        print(f"   {sum(len(r) for r in before['records'].values())} records, "
              f"groups {before['groups']}, epoch {old_epoch}")

        step("restarting the same volume on the new image, extension not yet updated")
        run("docker", "stop", CONTAINER)
        start("kafgres-postgres:upgrade-new")
        assert sql("SELECT extversion FROM pg_extension WHERE extname = 'kafgres'") == args.old
        compare(before, snapshot(args.engine), "new binary, old extension")
        assert epoch() > old_epoch, "the restart on the new build took no new leader epoch"
        kcat("-t", "up-plain", "-P", "-K:", "-X", "enable.idempotence=true",
             stdin="k1:after-upgrade\n")
        if args.engine == "segment":
            sql("SELECT kafgres_produce('up-sql', 'k9', 'after-upgrade')")
            if args.old < "0.3.0":
                assert "pre-0.3.0 .timeindex" in logs(), "the time index was not migrated"

        step("ALTER EXTENSION kafgres UPDATE")
        sql("ALTER EXTENSION kafgres UPDATE")
        new_version = sql("SELECT extversion FROM pg_extension WHERE extname = 'kafgres'")
        assert new_version != args.old, f"still {new_version}"
        assert sql("SELECT has_function_privilege('public', 'kafgres_drop_topic(text)', "
                   "'EXECUTE')") == "f", "the update left the admin functions public"
        restart()
        after = snapshot(args.engine)
        for topic in ("up-plain", "up-sql"):
            if topic in before["records"]:
                missing = set(before["records"][topic]) - set(after["records"][topic])
                assert not missing, f"{topic} lost {len(missing)} records after the update"
        assert after["groups"] == before["groups"]
        # kafgres_read() arrives with 0.4.0, not executable by PUBLIC, and reads the old log.
        assert sql("SELECT has_function_privilege('public', 'kafgres_read(text,int,bigint,bigint,"
                   "timestamptz,timestamptz,text,bigint)', 'EXECUTE')") == "f", \
            "the update left kafgres_read() public"
        read = sql("SELECT count(*) FROM kafgres_read('up-plain')")
        assert int(read) >= len(before["records"]["up-plain"]), \
            f"kafgres_read() saw {read} of {len(before['records']['up-plain'])} records"
        assert after["lookup"] == before["lookup"]

        step(f"compacting what {args.old} wrote")
        sql("SELECT kafgres_enforce_retention()")
        compacted = read_topic("up-compact")
        latest = {k: o for _, o, k, _ in compacted}
        assert len(latest) == 50, f"a key was lost to compaction: {len(latest)} keys"
        assert all(o >= 1500 for o in latest.values()), "a superseded value outlived its key's last"
        assert len(compacted) < len(before["records"]["up-compact"]), "nothing was compacted"
        print(f"   up-compact: {len(before['records']['up-compact'])} -> {len(compacted)} records")

        bad = [l for l in logs().splitlines()
               if ("PANIC" in l or "FATAL" in l or "ERROR" in l) and "kafgres" in l]
        assert not bad, "errors in the server log:\n" + "\n".join(bad[-20:])
        step(f"upgrade from {args.old} on the {args.engine} engine: OK")
    except Exception:
        print(logs()[-4000:], file=sys.stderr)
        raise
    finally:
        if not args.keep:
            run("docker", "rm", "-f", CONTAINER, check=False)
            run("docker", "volume", "rm", VOLUME, check=False)


if __name__ == "__main__":
    main()
