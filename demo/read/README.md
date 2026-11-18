# Demo: a bad deploy, investigated in SQL straight off the Kafka log

Yesterday at 14:00, build 2.4.1 of a payments service went out with a retry bug that charged
some customers twice. It was rolled back at 14:20. The charges are events on a Kafka topic;
the orders are rows in a table in the same Postgres. Every step of the investigation below
is one SQL statement over the topic, using [`kafgres_read()`](../../docs/reading.md).

| Normally | Here |
|---|---|
| A consumer app to copy the topic into a table before anyone can query it | `FROM kafgres_read('payments')` |
| A stream processor or ETL job to join events with the database | an ordinary `JOIN orders` |
| Consuming a whole day of events to look at twenty minutes of them | `from_time` / `to_time`, resolved like `offsetsForTimes` |

## Running it

```sh
docker compose up -d
pip install -r demo/read/requirements.txt
python3 demo/read/incident.py                 # 200,000 orders, about 30 s
SCALE=0.05 python3 demo/read/incident.py      # 10,000 orders
```

The script produces a day of charges with a real idempotent Kafka producer (librdkafka,
lz4), with `trace-id` and `service-version` headers, and plants the double charges itself.
Each step prints its SQL and the rows it returned, then checks the answer against what was
planted, so the demo is also a test: it exits non-zero if any check fails.

The outputs below are from one run with 200,000 orders on the segment engine (the table
engine gives the same answers). Dates are yesterday's, whenever it runs.

## The helper

The topic holds JSON values and two headers. One function, created once, turns it into typed
columns. It is a function rather than a view so that a time range is passed to
`kafgres_read()`, which then reads only that range:

```sql
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
$$;
```

`charge_no` numbers each order's charges in log order, so a repeat charge is `charge_no > 1`.

## 1. Support: "customers say they were charged twice." How bad is it?

```sql
SELECT count(*)                  AS customers_charged_twice,
       sum(amount_cents) / 100.0 AS overcharged_usd
  FROM payments()
 WHERE charge_no > 1;
```

```
customers_charged_twice  overcharged_usd
-----------------------  ---------------
405                      100,546.59
(1 rows, 595 ms)
```

## 2. When did it happen?

```sql
SELECT to_char(date_trunc('hour', charged_at), 'HH24:MI') AS hour,
       count(*)                                         AS charges,
       count(*) FILTER (WHERE charge_no > 1)            AS repeat_charges,
       repeat('#', (count(*) FILTER (WHERE charge_no > 1) / 10)::int) AS " "
  FROM payments()
 GROUP BY 1 ORDER BY 1;
```

```
hour   charges  repeat_charges
-----  -------  --------------  ----------------------------------------
...
12:00  8,510    0
13:00  8,510    0
14:00  8,916    405             ########################################
15:00  8,512    0
16:00  8,511    0
...
(24 rows, 627 ms)
```

Every other hour of the day has no repeat charges.

## 3. What changed at 14:00?

Zoom to 13:55 to 14:25, by minute. Only that half hour is read: `kafgres_read()` finds its
start and end offsets through the time index, the way Kafka's `offsetsForTimes` does.

```sql
SELECT to_char(date_trunc('minute', charged_at), 'HH24:MI') AS minute,
       version,
       count(*)                              AS charges,
       count(*) FILTER (WHERE charge_no > 1) AS repeat_charges
  FROM payments('2026-10-13 13:55+00', '2026-10-13 14:25+00')
 GROUP BY 1, 2 ORDER BY 1, 2;
```

```
minute  version  charges  repeat_charges
------  -------  -------  --------------
13:58   2.4.0    141      0
13:59   2.4.0    142      0
14:00   2.4.1    162      19
14:01   2.4.1    163      20
...     2.4.1             19 to 21 every minute
14:19   2.4.1    162      21
14:20   2.4.0    140      0
14:21   2.4.0    144      0
(30 rows, 18 ms)
```

2.4.1 shows up at 14:00, is the only build that charged twice, and is gone at 14:20. The
query read 4,657 of the day's 200,405 events, in 18 ms against 627 ms for the whole day.

## 4. Who do we refund?

```sql
CREATE TABLE refunds_due AS
SELECT o.id AS order_id, o.customer, p.amount_cents, p.trace_id AS duplicate_trace_id
  FROM payments('2026-10-13 14:00+00', '2026-10-13 14:20+00') p
  JOIN orders o ON o.id = p.order_id
 WHERE p.charge_no > 1;
```

```
order_id  customer    amount_cents  duplicate_trace_id
--------  ----------  ------------  ------------------
119149    cust-22690  7,405         50b21eb4b6b37fb8
119156    cust-02105  37,047        54d3b77b1c1ac855
119163    cust-32574  25,241        070db68829e09a22
...
(405 rows, built in 15 ms)
```

## Checks

```
PASS  how-bad       405 customers, $100,546.59: exactly the repeat charges that were planted
PASS  when          every repeat charge is in the 14:00 hour; the whole day read in 627 ms
PASS  what-changed  2.4.1 ran 14:00 to 14:19 and is the only build that charged twice; read 4,657 of 200,405 events in 18 ms
PASS  refunds       405 refunds, one per double-charged order, each for the repeated amount
PASS  consistent    kafgres_read() returned the same 200,405 records, byte for byte, as a librdkafka consumer
```

The last check is not part of the story: it compares every record `kafgres_read()` returned
with what a librdkafka consumer reads from the same topic.
