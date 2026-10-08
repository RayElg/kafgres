# Reading records with SQL

`kafgres_read()` returns a topic's records as rows, so any SQL can filter, join and
aggregate them. It is the read-side counterpart of `kafgres_produce()`.

```sql
SELECT record_offset, convert_from(value, 'UTF8')::jsonb AS event
  FROM kafgres_read('order-events', 0, from_offset => 1000, to_offset => 2000)
 WHERE convert_from(value, 'UTF8')::jsonb ->> 'op' = 'D';
```

## Arguments

Only `topic` is required, and every bound is optional; the bounds that are given intersect.

| argument | default | meaning |
|---|---|---|
| `topic` | | the topic to read |
| `partition` | NULL | one partition; NULL reads every partition, in partition order |
| `from_offset` | log start | first offset, inclusive |
| `to_offset` | log end | last offset, exclusive |
| `from_time` | | `timestamptz`: start at the first record stamped at or after it |
| `to_time` | | `timestamptz`: end before the first record stamped at or after it |
| `isolation` | `'read_committed'` | or `'read_uncommitted'` |
| `max_records` | NULL | stop after this many rows |

Offsets are clamped to what the partition holds, so `from_offset => 0` on a partition
whose retention has moved its start is not an error. The log end is the high watermark,
or under `read_committed` the last stable offset. The range is fixed when the call starts,
and records produced while it runs are not returned.

The time bounds are looked up in the time index the same way `offsetsForTimes` (ListOffsets)
looks them up, and that lookup is what turns them into an offset range. A record inside that
range is returned even if its own timestamp falls outside the bounds, which a producer
setting its own `CreateTime` out of order can cause. Add a `WHERE timestamp >= ...` clause to
drop such records.

Kafka offsets are per partition, so offset bounds given with `partition => NULL` apply to
every partition alike. Time bounds are the ones that mean the same thing across partitions.

## Columns

| column | type | |
|---|---|---|
| `partition` | `int` | |
| `record_offset` | `bigint` | |
| `timestamp` | `timestamptz` | NULL for a record written without one |
| `timestamp_type` | `text` | `CreateTime` or `LogAppendTime` |
| `key` | `bytea` | NULL for a null key |
| `value` | `bytea` | NULL for a tombstone |
| `header_keys` | `text[]` | in record order |
| `header_values` | `bytea[]` | in the same order; an element is NULL for a null value |
| `leader_epoch` | `int` | the partition leader epoch the batch was written under |
| `producer_id` | `bigint` | NULL outside idempotent and transactional producers |
| `producer_epoch` | `smallint` | |
| `sequence` | `int` | the record's own sequence number, NULL without one |
| `is_transactional` | `boolean` | |

A record declaring more than 65,536 headers ends the read with an error rather than being
decoded.

`key` and `value` are bytes, as Kafka stores them. Use `convert_from(value, 'UTF8')` for
text, and add `::jsonb` for JSON. To pair the headers up, unnest both arrays together:

```sql
SELECT r.record_offset, h.k, convert_from(h.v, 'UTF8')
  FROM kafgres_read('order-events', 0) r,
       unnest(r.header_keys, r.header_values) AS h(k, v);
```

## Composing

`kafgres_read()` is an ordinary set-returning function. `LATERAL` reads a range from every
partition, here the last 100 records of each:

```sql
SELECT r.*
  FROM kafgres_partition_offsets('order-events') p,
       LATERAL kafgres_read('order-events', p.partition,
                            from_offset => p.high_watermark - 100) r;
```

A function in `FROM` runs to completion before the query above it consumes its rows, so a
`LIMIT` over the call does not shorten the scan. Bound the range itself, or pass
`max_records`.

A long scan is a long statement. While it runs, Postgres cannot prune the rows idempotent
producers update for every batch, and their produce slows. Bounded ranges keep scans short.

## What `read_committed` hides

Under `read_committed`, the default, `kafgres_read()` returns what a `read_committed` Kafka
consumer would receive. That excludes records from aborted Kafka transactions and from
rolled-back `kafgres_produce()` calls, and nothing past the last stable offset is returned.
`read_uncommitted` returns them too, as it does for a consumer. Control records
(transaction markers) are never returned.

## Who may read

`kafgres_read()` is not executable by `PUBLIC`. Granting `EXECUTE` is how a role is allowed
to read:

```sql
GRANT EXECUTE ON FUNCTION kafgres_read TO reporting;
```

With `kafgres.acls_enabled` on, the role also needs `READ` on the topic in `kafgres_acls`,
as `User:<role>`, the same principal `kafgres_produce()` checks for `WRITE`. A Postgres
superuser is not checked against `kafgres_acls`.

```sql
SELECT kafgres_add_acl('User:reporting', 'READ', 'TOPIC', 'order-events');
```

No grant on the tables behind the log is needed. They differ between storage engines, so
once both checks pass, `kafgres_read()` reads the log as the bootstrap superuser. Only the
read itself runs that way. Each row is handed back to the query as the calling role, so the
rest of the query runs as the caller.
