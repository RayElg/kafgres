"""`kafgres_read()`: a topic's records as rows.

The reference for what a row should hold is a real consumer reading the same partition,
so most of these compare against kcat rather than against what this file produced.
"""

import subprocess
import time

import pytest

from conftest import BROKER_HOST, BROKER_PORT, sql
from support.docker import CLIENTS

BROKER = f"{BROKER_HOST}:{BROKER_PORT}"
ROLE = "sqlreader"


def kcat(*args, stdin=None, timeout=120):
    return subprocess.run(
        ["docker", "run", "--rm", "-i", "--network", "host", CLIENTS, "kcat", "-b", BROKER,
         *args],
        input=stdin, capture_output=True, text=True, timeout=timeout,
    )


def produce(topic, lines, *extra):
    out = kcat("-t", topic, "-P", *extra, stdin="".join(f"{l}\n" for l in lines))
    assert out.returncode == 0, out.stderr


def consumed(topic, isolation="read_committed", partition=0):
    """`{offset: value}` as a Kafka consumer sees the partition."""
    out = kcat("-t", topic, "-p", str(partition), "-C", "-e", "-q", "-o", "beginning",
               "-X", f"isolation.level={isolation}", "-f", "%o\t%s\n")
    got = {}
    for line in out.stdout.splitlines():
        if "\t" in line:
            off, value = line.split("\t", 1)
            got[int(off)] = value
    return got


def rows(query):
    """Tab-separated rows of `query`."""
    out = sql(f"COPY ({query}) TO STDOUT")
    return [line.split("\t") for line in out.splitlines() if line]


def read(topic, *args, isolation=None):
    """`{offset: value}` from `kafgres_read()`, the values decoded as UTF-8."""
    named = list(args) + ([f"isolation => '{isolation}'"] if isolation else [])
    call = ", ".join([f"'{topic}'"] + named)
    return {int(o): v for o, v in rows(
        f"SELECT record_offset, convert_from(value, 'UTF8') FROM kafgres_read({call})")}


def offset_at(topic, when_ms):
    """`kcat -Q`, which is `offsetsForTimes`."""
    out = kcat("-Q", "-t", f"{topic}:0:{when_ms}")
    for line in out.stdout.splitlines():
        if "offset" in line:
            return int(line.rsplit(" ", 1)[1])
    raise AssertionError(f"no offset in {out.stdout!r} {out.stderr!r}")


def ts(ms):
    return f"to_timestamp({ms} / 1000.0)"


@pytest.fixture
def topic(request):
    name = f"sqlread-{request.node.name.replace('_', '-')[5:40]}"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 1)")
    yield name
    sql(f"SELECT kafgres_drop_topic('{name}')")


def test_rows_match_what_a_consumer_reads(topic):
    produce(topic, [f"value-{i}" for i in range(50)])
    produce(topic, [f"lz4-{i}" for i in range(50)], "-z", "lz4")
    expected = consumed(topic)
    assert len(expected) == 100
    assert read(topic) == expected


def test_keys_headers_and_metadata_ride_along(topic):
    produce(topic, ["k1:v1", "k2:v2"], "-K", ":", "-H", "trace=abc", "-H", "empty=")
    got = rows(f"""SELECT record_offset, convert_from(key, 'UTF8'), convert_from(value, 'UTF8'),
                          header_keys::text, timestamp_type, leader_epoch IS NOT NULL,
                          timestamp > now() - interval '5 minutes', is_transactional
                     FROM kafgres_read('{topic}')""")
    assert got == [
        ["0", "k1", "v1", "{trace,empty}", "CreateTime", "t", "t", "f"],
        ["1", "k2", "v2", "{trace,empty}", "CreateTime", "t", "t", "f"],
    ]
    values = sql(f"""SELECT string_agg(coalesce(convert_from(v, 'UTF8'), 'NULL'), ',')
                       FROM kafgres_read('{topic}', 0, to_offset => 1),
                            unnest(header_values) v""")
    assert values == "abc,"


def test_offset_bounds_are_half_open_and_clamped(topic):
    produce(topic, [str(i) for i in range(20)])
    assert sorted(read(topic, "0", "from_offset => 5", "to_offset => 8")) == [5, 6, 7]
    assert sorted(read(topic, "0", "from_offset => -10", "to_offset => 2")) == [0, 1]
    assert sorted(read(topic, "0", "from_offset => 18", "to_offset => 1000")) == [18, 19]
    assert read(topic, "0", "from_offset => 8", "to_offset => 8") == {}
    assert read(topic, "0", "from_offset => 9", "to_offset => 3") == {}


def test_time_bounds_follow_offsets_for_times(topic):
    """The time index decides where a time range starts and ends, as offsetsForTimes does."""
    marks = []
    for wave in range(4):
        marks.append(int(time.time() * 1000))
        produce(topic, [f"w{wave}-{i}" for i in range(10)])
        time.sleep(1.1)
    start, end = offset_at(topic, marks[1]), offset_at(topic, marks[3])
    assert (start, end) == (10, 30), (start, end)
    got = read(topic, "0", f"from_time => {ts(marks[1])}", f"to_time => {ts(marks[3])}")
    assert sorted(got) == list(range(start, end))
    # Time and offset bounds intersect.
    got = read(topic, "0", f"from_time => {ts(marks[1])}", "to_offset => 15")
    assert sorted(got) == list(range(10, 15))
    # Past the last record, nothing.
    assert read(topic, "0", f"from_time => {ts(int(time.time() * 1000) + 60_000)}") == {}


def test_it_composes_with_ordinary_sql(topic):
    produce(topic, [f'{{"n": {i}, "kind": "{"even" if i % 2 == 0 else "odd"}"}}'
                    for i in range(30)])
    got = sql(f"""SELECT string_agg((convert_from(value, 'UTF8')::jsonb ->> 'n'), ','
                                    ORDER BY record_offset)
                    FROM kafgres_read('{topic}', 0, from_offset => 10, to_offset => 20)
                   WHERE convert_from(value, 'UTF8')::jsonb ->> 'kind' = 'odd'""")
    assert got == "11,13,15,17,19"
    # LATERAL over the partitions report, and a LIMIT on top.
    got = sql(f"""SELECT count(*) FROM kafgres_partition_offsets('{topic}') p,
                         LATERAL kafgres_read('{topic}', p.partition,
                                              from_offset => p.high_watermark - 5) r""")
    assert got == "5"
    assert sql(f"SELECT count(*) FROM (SELECT * FROM kafgres_read('{topic}') LIMIT 3) s") == "3"
    assert sql(f"SELECT count(*) FROM kafgres_read('{topic}', max_records => 4)") == "4"


def test_every_partition_when_none_is_named():
    name = "sqlread-partitions"
    sql(f"SELECT kafgres_drop_topic('{name}')")
    sql(f"SELECT kafgres_create_topic('{name}', 3)")
    try:
        for p in range(3):
            produce(name, [f"p{p}-{i}" for i in range(p + 2)], "-p", str(p))
        got = rows(f"SELECT partition, count(*) FROM kafgres_read('{name}') GROUP BY 1 ORDER BY 1")
        assert got == [["0", "2"], ["1", "3"], ["2", "4"]]
        with pytest.raises(RuntimeError, match="has no partition 3"):
            sql(f"SELECT * FROM kafgres_read('{name}', 3)")
    finally:
        sql(f"SELECT kafgres_drop_topic('{name}')")


def test_aborted_kafka_transactions_are_filtered_like_a_consumer_would(topic):
    """The aborted list and the abort marker, applied as the Java consumer applies them."""
    for outcome in ("abort", "commit", "abort"):
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", CLIENTS,
             "sarama-conformance", BROKER, f"txn-{outcome}", topic],
            capture_output=True, text=True, timeout=180,
        )
        assert f"OK {outcome}" in out.stdout, out.stderr[-800:]
    for isolation in ("read_committed", "read_uncommitted"):
        assert read(topic, isolation=isolation) == consumed(topic, isolation), isolation
    assert len(read(topic)) < len(read(topic, isolation="read_uncommitted"))


@pytest.mark.skipif(sql("SHOW kafgres.storage_engine") != "segment",
                    reason="kafgres_produce() is the segment engine's")
def test_a_rolled_back_sql_produce_is_read_only_uncommitted(topic):
    sql(f"BEGIN; SELECT kafgres_produce('{topic}', NULL, 'kept'); COMMIT")
    sql(f"BEGIN; SELECT kafgres_produce('{topic}', NULL, 'rolled-back'); ROLLBACK")
    sql(f"BEGIN; SELECT kafgres_produce('{topic}', NULL, 'kept-too'); COMMIT")
    assert list(read(topic).values()) == ["kept", "kept-too"]
    assert list(read(topic, isolation="read_uncommitted").values()) == [
        "kept", "rolled-back", "kept-too"]
    got = rows(f"SELECT is_transactional, producer_id IS NOT NULL FROM kafgres_read('{topic}')")
    assert got == [["t", "t"], ["t", "t"]]


def test_bad_arguments_are_refused(topic):
    with pytest.raises(RuntimeError, match="isolation must be"):
        sql(f"SELECT * FROM kafgres_read('{topic}', isolation => 'snapshot')")
    with pytest.raises(RuntimeError, match="max_records must not be negative"):
        sql(f"SELECT * FROM kafgres_read('{topic}', max_records => -1)")
    with pytest.raises(RuntimeError, match="no such topic"):
        sql("SELECT * FROM kafgres_read('sqlread-no-such-topic')")


@pytest.fixture
def reader_role(topic):
    if sql(f"SELECT count(*) FROM pg_roles WHERE rolname = '{ROLE}'") == "1":
        sql(f"DROP OWNED BY {ROLE}; DROP ROLE {ROLE}")
    sql(f"CREATE ROLE {ROLE}")
    yield topic
    sql("ALTER SYSTEM SET kafgres.acls_enabled = off")
    sql("SELECT pg_reload_conf()")
    sql(f"DELETE FROM kafgres_acls WHERE principal = 'User:{ROLE}'")
    sql(f"DROP OWNED BY {ROLE}; DROP ROLE {ROLE}")


def as_role(statement):
    try:
        return True, sql(f"SET ROLE {ROLE}; {statement}")
    except RuntimeError as e:
        return False, str(e)


def test_reading_takes_a_grant_and_then_the_acls(reader_role):
    topic = reader_role
    produce(topic, ["secret"])
    query = f"SELECT convert_from(value, 'UTF8') FROM kafgres_read('{topic}')"

    ok, out = as_role(query)
    assert not ok and "permission denied for function kafgres_read" in out, out

    # EXECUTE alone is enough: the tables behind the log are not granted.
    sql(f"GRANT EXECUTE ON FUNCTION kafgres_read TO {ROLE}")
    ok, out = as_role(query)
    assert ok and out.endswith("secret"), out

    sql("ALTER SYSTEM SET kafgres.acls_enabled = on")
    sql("SELECT pg_reload_conf()")
    time.sleep(1.5)
    ok, out = as_role(query)
    assert not ok and "not allowed to READ" in out, out

    sql(f"SELECT kafgres_add_acl('User:{ROLE}', 'READ', 'TOPIC', '{topic}')")
    time.sleep(1.5)
    ok, out = as_role(query)
    assert ok and out.endswith("secret"), out

    # The read runs elevated, but nothing after it does.
    ok, out = as_role(f"SELECT count(*) FROM kafgres_read('{topic}'), "
                      f"LATERAL (SELECT current_user::text AS who) u WHERE u.who = '{ROLE}'")
    assert ok and out.endswith("1"), out


def test_a_scan_does_not_hold_what_it_read_until_the_transaction_ends(topic):
    """A scan frees each batch as it goes, not at transaction end."""
    produce(topic, ["x" * 4000] * 5000)  # 20 MB, toasted on the table engine
    got = sql(f"""BEGIN;
                  SELECT count(*) FROM kafgres_read('{topic}');
                  SELECT sum(total_bytes) FROM pg_backend_memory_contexts
                   WHERE name = 'TopTransactionContext';
                  COMMIT;""")
    # psql prints each statement's result: BEGIN, the count, the bytes held, COMMIT.
    held = int(got.splitlines()[2])
    assert held < 4 * 1024 * 1024, f"{held} bytes still held after scanning 20 MB"
