"""kafgres_read(): a bad deploy that double-charged customers, investigated in SQL off the log.

Yesterday's payment events sit on a Kafka topic, written by a real idempotent producer, next
to the `orders` table in the same Postgres. Build 2.4.1 went out at 14:00 with a retry bug
that charged some customers twice, and was rolled back at 14:20. Each step of the
investigation is one SQL statement over the topic, checked against what the generator planted.

  pip install -r demo/read/requirements.txt
  python3 demo/read/incident.py             # 200,000 orders
  SCALE=0.05 python3 demo/read/incident.py  # 10,000 orders

Needs the compose stack up (`docker compose up -d`). See demo/read/README.md.
"""

import datetime
import decimal
import json
import os
import random
import sys
import time
import uuid

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOT = os.environ.get("BOOT", "127.0.0.1:9092")
# UTC, so the hours SQL buckets by are the ones the checks name.
DSN = ("host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres "
       "options='-c TimeZone=UTC'")
SCALE = float(os.environ.get("SCALE", "1"))
N_ORDERS = int(200_000 * SCALE)
RESULTS = []
rng = random.Random(20261014)
UTC = datetime.timezone.utc


# --- plumbing ------------------------------------------------------------------------------

def say(*a):
    print(*a, flush=True)


def step(title):
    say("\n" + "=" * 92 + f"\n  {title}\n" + "=" * 92)


def check(name, ok, detail, quiet=False):
    RESULTS.append((name, ok, detail))
    if not quiet:
        say(f"  {'PASS' if ok else 'FAIL'}  {detail}")


def sql(statement, show=True, max_rows=30):
    """Run one statement, print it and its rows, return (rows, seconds)."""
    with psycopg.connect(DSN, autocommit=True) as c:
        t0 = time.perf_counter()
        cur = c.execute(statement)
        rows = cur.fetchall() if cur.description else []
        took = time.perf_counter() - t0
        cols = [d.name for d in cur.description] if cur.description else []
    if show:
        say("\n" + "\n".join("  " + l for l in statement.strip("\n").splitlines()))
        say("")
        if cols:
            table(cols, rows[:max_rows])
            if len(rows) > max_rows:
                say(f"  ... {len(rows) - max_rows} more rows")
        say(f"  ({len(rows)} rows, {took * 1000:.0f} ms)" if cols else f"  ({took * 1000:.0f} ms)")
    return rows, took


def table(cols, rows):
    cells = [[str(v) if c.endswith("_id") else fmt(v) for c, v in zip(cols, r)] for r in rows]
    width = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(cols)]
    say("  " + "  ".join(c.ljust(w) for c, w in zip(cols, width)).rstrip())
    say("  " + "  ".join("-" * w for w in width))
    for r in cells:
        say("  " + "  ".join(v.ljust(w) for v, w in zip(r, width)).rstrip())


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, datetime.datetime):
        return v.astimezone(UTC).strftime("%H:%M:%S")
    if isinstance(v, decimal.Decimal):
        return f"{v:,.2f}" if v != v.to_integral_value() else f"{int(v):,}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def make_topic(name, partitions):
    sql(f"SELECT kafgres_drop_topic('{name}')", show=False)
    admin = AdminClient({"bootstrap.servers": BOOT})
    for f in admin.create_topics([NewTopic(name, partitions, 1)]).values():
        f.result(30)
    time.sleep(0.5)


def consume_all(name):
    """What a librdkafka consumer reads from the beginning: {(partition, offset): (key, value)}."""
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"audit-{uuid.uuid4()}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    parts = list(c.list_topics(name, timeout=10).topics[name].partitions)
    c.assign([TopicPartition(name, p, 0) for p in parts])
    out, last = {}, time.time()
    while time.time() - last < 4:
        for m in c.consume(10_000, 0.5):
            if not m.error():
                out[(m.partition(), m.offset())] = (m.key(), m.value())
                last = time.time()
    c.close()
    return out


# --- the day being investigated ------------------------------------------------------------

class Day:
    """Yesterday's orders and charges, with 2.4.1 live from 14:00 to 14:20."""

    def __init__(self):
        today = datetime.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        self.date = today - datetime.timedelta(days=1)
        self.start = int(self.date.timestamp() * 1000)
        self.deploy = self.start + 14 * 3600 * 1000
        self.rollback = self.deploy + 20 * 60 * 1000
        step_ms = 23.5 * 3600 * 1000 / N_ORDERS
        self.orders = []                                  # (id, customer, cents, placed ms)
        for i in range(1, N_ORDERS + 1):
            self.orders.append((i, f"cust-{rng.randint(1, max(1, N_ORDERS // 4)):05d}",
                                rng.randint(199, 49_999), self.start + int(i * step_ms)))

        # Each order is charged 0.2 to 4 s after it is placed. 2.4.1's retry bug charges every
        # 7th order it handles a second time, 0.8 to 2.5 s later.
        self.charges = []                                 # (ms, order, cents, version)
        self.double = {}                                  # order -> cents charged again
        handled = 0
        for oid, _, cents, placed in self.orders:
            at = placed + rng.randint(200, 4_000)
            version = self.version(at)
            self.charges.append((at, oid, cents, version))
            if version == "2.4.1":
                handled += 1
                again = at + rng.randint(800, 2_500)
                if handled % 7 == 0 and again < self.rollback:
                    self.charges.append((again, oid, cents, version))
                    self.double[oid] = cents
        self.charges.sort()

    def version(self, ms):
        return "2.4.1" if self.deploy <= ms < self.rollback else "2.4.0"

    def at(self, hhmm):
        """Yesterday at HH:MM, as a timestamptz literal."""
        h, m = map(int, hhmm.split(":"))
        return (self.date + datetime.timedelta(hours=h, minutes=m)).strftime("%Y-%m-%d %H:%M+00")


SETUP_SQL = """
CREATE FUNCTION payments(from_time timestamptz DEFAULT NULL, to_time timestamptz DEFAULT NULL)
RETURNS TABLE (charged_at timestamptz, order_id bigint, amount_cents bigint,
               version text, trace_id text, charge_no bigint)
LANGUAGE sql AS $$
  SELECT r.timestamp, p.order_id, p.amount_cents,
         convert_from(r.header_values[array_position(r.header_keys, 'service-version')], 'UTF8'),
         convert_from(r.header_values[array_position(r.header_keys, 'trace-id')], 'UTF8'),
         row_number() OVER (PARTITION BY p.order_id ORDER BY r.partition, r.record_offset)
    FROM kafgres_read('payments', from_time => from_time, to_time => to_time) r,
         jsonb_to_record(convert_from(r.value, 'UTF8')::jsonb)
           AS p(order_id bigint, amount_cents bigint)
$$;"""


def load(day):
    step("Setup: yesterday's orders in Postgres, and its charges on the 'payments' topic")
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS orders, refunds_due")
        c.execute("DROP FUNCTION IF EXISTS payments")
        c.execute("""CREATE TABLE orders (id bigint PRIMARY KEY, customer text NOT NULL,
                                          amount_cents bigint NOT NULL, placed_at timestamptz NOT NULL)""")
        with c.cursor().copy("COPY orders FROM STDIN") as cp:
            for oid, customer, cents, ms in day.orders:
                cp.write_row((oid, customer, cents, datetime.datetime.fromtimestamp(ms / 1000, UTC)))
        c.execute("ANALYZE orders")
    make_topic("payments", 6)

    p = Producer({"bootstrap.servers": BOOT, "enable.idempotence": True, "linger.ms": 5,
                  "batch.size": 262_144, "compression.type": "lz4"})
    for at, oid, cents, version in day.charges:
        value = json.dumps({"order_id": oid, "amount_cents": cents, "currency": "USD",
                            "psp_ref": uuid.UUID(int=rng.getrandbits(128)).hex})
        headers = [("trace-id", uuid.UUID(int=rng.getrandbits(128)).hex[:16].encode()),
                   ("service-version", version.encode())]
        while True:
            try:
                p.produce("payments", key=str(oid), value=value, headers=headers, timestamp=at)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
    p.flush(60)
    say(f"  {len(day.orders):,} orders, {len(day.charges):,} charge events over the day "
        f"(6 partitions, idempotent producer, lz4)")
    say("  Each event: JSON value {order_id, amount_cents, ...}, headers trace-id and service-version.")
    say("\n  One helper, created once, decodes the topic into typed columns. It is a function rather")
    say("  than a view so a time range reaches kafgres_read() and bounds what it reads:")
    sql(SETUP_SQL)


# --- the investigation ---------------------------------------------------------------------

def how_bad(day):
    step("1. Support: \"customers say they were charged twice.\" How bad is it?")
    rows, _ = sql("""
SELECT count(*)                  AS customers_charged_twice,
       sum(amount_cents) / 100.0 AS overcharged_usd
  FROM payments()
 WHERE charge_no > 1;""")
    want = (len(day.double), decimal.Decimal(sum(day.double.values())) / 100)
    check("how-bad", (rows[0][0], rows[0][1]) == want,
          f"{want[0]:,} customers, ${want[1]:,.2f}: exactly the repeat charges that were planted")


def when(day):
    step("2. When did it happen?")
    rows, took = sql("""
SELECT to_char(date_trunc('hour', charged_at), 'HH24:MI') AS hour,
       count(*)                                         AS charges,
       count(*) FILTER (WHERE charge_no > 1)            AS repeat_charges,
       repeat('#', (count(*) FILTER (WHERE charge_no > 1) / 10)::int) AS " "
  FROM payments()
 GROUP BY 1 ORDER BY 1;""")
    spiked = [r for r in rows if r[2] > 0]
    check("when", [r[0] for r in spiked] == ["14:00"] and spiked[0][2] == len(day.double),
          f"every repeat charge is in the 14:00 hour; the whole day read in {took * 1000:.0f} ms")
    day.full_ms = took


def what_changed(day):
    step("3. What changed at 14:00? Zoom to 13:55-14:25, by minute")
    say("  Only the window is read: the time range goes to kafgres_read(), which finds its start")
    say("  and end offsets through the time index, as Kafka's offsetsForTimes does.")
    rows, took = sql(f"""
SELECT to_char(date_trunc('minute', charged_at), 'HH24:MI') AS minute,
       version,
       count(*)                                           AS charges,
       count(*) FILTER (WHERE charge_no > 1)              AS repeat_charges
  FROM payments('{day.at("13:55")}', '{day.at("14:25")}')
 GROUP BY 1, 2 ORDER BY 1, 2;""", max_rows=40)
    good = all(((r[1] == "2.4.1") == ("14:00" <= r[0] < "14:20"))
               and ((r[3] > 0) == (r[1] == "2.4.1")) for r in rows)
    span = [r[0] for r in rows if r[1] == "2.4.1"]
    in_window = sum(r[2] for r in rows)
    check("what-changed", good and span[0] == "14:00" and span[-1] == "14:19",
          f"2.4.1 ran 14:00 to 14:19 and is the only build that charged twice; read "
          f"{in_window:,} of {len(day.charges):,} events in {took * 1000:.0f} ms")


def refunds(day):
    step("4. Who do we refund?")
    sql(f"""
CREATE TABLE refunds_due AS
SELECT o.id AS order_id, o.customer, p.amount_cents, p.trace_id AS duplicate_trace_id
  FROM payments('{day.at("14:00")}', '{day.at("14:20")}') p
  JOIN orders o ON o.id = p.order_id
 WHERE p.charge_no > 1;""")
    sql("SELECT * FROM refunds_due ORDER BY order_id LIMIT 5;")
    got = dict(sql("SELECT order_id, amount_cents FROM refunds_due", show=False)[0])
    check("refunds", got == day.double,
          f"{len(got):,} refunds, one per double-charged order, each for the repeated amount")


def consistent(day):
    """Not part of the story: SQL must see the log a Kafka consumer sees."""
    consumed = consume_all("payments")
    rows, _ = sql("SELECT partition, record_offset, key, value FROM kafgres_read('payments')",
                  show=False)
    from_sql = {(p, o): (bytes(k), bytes(v)) for p, o, k, v in rows}
    check("consistent", consumed == from_sql and len(from_sql) == len(day.charges),
          f"kafgres_read() returned the same {len(from_sql):,} records, byte for byte, as a "
          f"librdkafka consumer", quiet=True)


def main():
    version = sql("SELECT kafgres_version()", show=False)[0][0][0]
    engine = sql("SHOW kafgres.storage_engine", show=False)[0][0][0]
    say(f"kafgres {version}, {engine} engine")
    day = Day()
    load(day)
    for f in (how_bad, when, what_changed, refunds):
        f(day)
    step("Checks")
    consistent(day)
    for name, ok, detail in RESULTS:
        say(f"  {'PASS' if ok else 'FAIL'}  {name:<13} {detail}")
    sys.exit(0 if all(ok for _, ok, _ in RESULTS) else 1)


if __name__ == "__main__":
    main()
