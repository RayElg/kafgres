# Producing records

There are three ways to get a record into a topic. They are not interchangeable, and
choosing between them is the main design decision when wiring an application to
kafgres.

| Path | Who writes it | Event shape | Commit-path cost |
|---|---|---|---|
| Wire Produce (API key 0) | a Kafka client | whatever the client sends | none |
| `kafgres_produce()` | application SQL, in its own transaction | application-authored | one marker row |
| CDC mapping | derived from the WAL | defined by a SQL mapping | none |

## `kafgres_produce()`

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total) VALUES (...);
  SELECT kafgres_produce('order-events', 'OrderCreated',
                         jsonb_build_object(...)::text);
COMMIT;
```

`kafgres_produce(topic text, key text, value text)` returns `bigint`, the offset the
record was assigned. `key` and `value` are `text`, so a `jsonb` payload is passed as
`jsonb_build_object(...)::text`; either may be `NULL`.

The record is visible to consumers exactly when the transaction commits. If the
transaction rolls back, nothing is consumable. This is the only produce path that can
promise the event reflects what the transaction saw, because it runs inside the
transaction.

Mechanically, `kafgres_produce()` appends the payload to the log and writes a commit
marker row inside the caller's transaction. Visibility to `read_committed` consumers is
computed from committed markers. Three properties of that mechanism are worth knowing:

- A rolled-back produce leaves its bytes in the log, as Kafka's own aborted transactions
  do. The Fetch response carries an `aborted_transactions` list, and `read_committed`
  clients discard matching records and advance past them. The broker does not withhold
  the batches: Fetch has no way to tell a client to advance past a withheld batch, and a
  client that re-requests the same offset forever looks like a hung consumer.
- Each transaction produces under its own producer id, taken from the transaction's xid.
  Kafka clients drop everything from a producer id at or after an aborted first offset,
  so a shared producer id would let one rollback discard later committed records.
- The last stable offset gates records from transactions that are still in flight, so
  `read_committed` consumers never see records that may yet commit or abort.

The feature is available on the segment engine and controlled by
`kafgres.allow_transactional_produce`. On the table engine it is not supported: offset assignment there
takes a row lock on the partition, and a SQL caller would hold it for the lifetime of
its own transaction, serializing the partition and queuing wire producers behind
application logic. The segment engine assigns offsets from shared memory, so a SQL
caller holds no lock that other producers need.

## CDC mappings

A mapping copies changes from a table to a topic through the WAL. You register it once:

The examples assume a `public.orders (id int, customer_id int, total numeric, status
text)` table and a `customers (id int, name text, tier text)` lookup table.

```sql
SELECT kafgres_create_topic('order-events', 3);

SELECT kafgres_add_mapping(
    'orders-cdc',       -- mapping name
    'public.orders',    -- source table
    'order-events',     -- topic
    value_expr  => $$ jsonb_build_object(
                        'order_id', new.id,
                        'total',    new.total,
                        'op',       op,
                        'customer', (SELECT jsonb_build_object('name', c.name, 'tier', c.tier)
                                       FROM customers c WHERE c.id = new.customer_id)) $$,
    key_expr    => $$ new.id::text $$,
    filter_expr => $$ new.status = 'placed' AND old.status IS DISTINCT FROM 'placed' $$
);
```

A background worker drains a logical replication slot fed by kafgres's own output
plugin, renders each change through its mappings, and produces the result. Existing rows
can be backfilled into the same topic with `kafgres_snapshot_mapping`, which applies the
same expressions to the table's current contents, so a backfilled record and a streamed
one are identical by construction.

Two settings are required: `wal_level = logical`, and `kafgres` in
`output_plugin_libraries`. Both are set in the Docker image. When one is missing, the
broker logs a line naming the setting.

`kafgres_preview_mapping(mapping, predicate)` renders a mapping over the source table's
current rows before you enable it. The second argument is a SQL predicate spliced into
the mapping's `WHERE` clause to select the rows to render, e.g.
`kafgres_preview_mapping('orders', 'new.id = 10')` — not a key or an op. Columns must be
qualified (`new.id`, not `id`), because `old` is in scope too. `op` binds as `I`,
matching what the drain emits. `kafgres_cdc_status()` reports the slot's position and
the WAL it is retaining.

### The mapping is a SQL expression

The mapping's `key`, `value` and `filter` are SQL expressions with `new`, `old` and `op`
bound, not string templates. That matters because string-built JSON loses types and is
injection-prone, and because SQL can do things a template over one row cannot:

- **Enrichment by join.** The `customers` lookup runs in the database, at decode time.
  No separate stream-processing job and no duplicated reference data. Debezium's
  equivalent is a downstream stream-join or a custom SMT that opens its own connection;
  those read current state exactly as this does, so the advantage here is locality, not
  correctness.
- **Real predicates.** `filter` is a SQL boolean, subqueries included.
- **Types.** `jsonb_build_object` produces typed JSON, so a numeric column stays a
  number. No schema registry is needed for a JSON topic.
- **Transition conditions.** `old` makes "only when status becomes placed"
  expressible, which is a domain event rather than a row diff.
- **Masking and redaction** are ordinary expressions.
- **The outbox pattern is one line.** A mapping over an outbox table is
  `value => new.payload, key => new.key, topic => new.topic`.

### What a mapping has in scope

| name | value |
|---|---|
| `new` | the changed row, typed through the shape the change carried |
| `old` | its before-image, subject to the table's `REPLICA IDENTITY` |
| `op` | `I`, `U`, `D`, or `R` for a snapshot row |
| `lsn` | the change's WAL position, `pg_lsn` |
| `xid` | the transaction that committed it, `bigint` |
| `commit_ts` | when that transaction committed, `timestamptz` |
| `event_count` | changes in this commit; only on a `kafgres.transaction` mapping |
| `data_collections` | `{"schema.table": n}` counts for this commit; same restriction |

`xid` and `commit_ts` are the same for every change from one commit, which is what lets
a consumer group those changes together. On a snapshot row they are NULL rather than
zeroed: a snapshot row is a read, not a change, and a shared fictitious transaction id
across a backfill is a grouping a downstream consumer would act on.

On a change, `commit_ts` is always present: it rides in the transaction's commit WAL
record, which the plugin reads directly. `track_commit_timestamp` is irrelevant to it —
that setting backs the `pg_xact_commit_timestamp()` SLRU, which kafgres does not read.
Only a snapshot row, above, has none.

The values in `new` and `old` are exact as of the change; the WAL carries the new tuple
and `REPLICA IDENTITY FULL` carries the before-image.

### The one caveat: joins read current state

An enrichment query runs when the change is rendered, after commit, and sees the tables
as they are now, not as they were when the change happened. If a customer's tier changes
between the order insert and the decode, the event carries the new tier against the old
order. Postgres cannot evaluate a query as of an arbitrary past LSN.

This is acceptable, and useful, for slowly-changing reference data: product names, tier
labels, tenant metadata. It is wrong when the event must reflect the transaction's own
snapshot. That case is what `kafgres_produce()` is for, and the two paths are
complementary for exactly this reason.

Delivery is at-least-once. The drain peeks changes from the slot, produces them, then
advances the slot, so a crash between steps replays the changes instead of losing them.
Unmapped write traffic never accumulates: the slot advances past every change it peeked,
mapped or not.

### Render failures: `on_error`

Per mapping, `on_error` decides what happens when rendering fails:

- `'skip'` (default) writes the change to `kafgres_cdc_errors` and continues.
- `'stall'` holds the slot where it is, so nothing is lost and the changes replay once
  the mapping is fixed. Stalling pins WAL, and pinned WAL fills the disk, so it is the
  choice for pipelines where every event matters, not the safe default.

`on_error` governs permanent failures, such as a bad expression or a missing topic.
Transient errors (deadlock, lock timeout) stop the drain and replay whatever `on_error`
says. A partially appended batch on the segment engine is dead-lettered instead of
replayed, since replaying would append the records a second time.

## Transaction summaries

A mapping whose source is the reserved name `kafgres.transaction` receives one record
per commit rather than one per row:

```sql
SELECT kafgres_add_mapping('txns', 'kafgres.transaction', 'my-transactions',
  $$jsonb_build_object('xid', xid, 'ts', commit_ts,
                       'event_count', event_count,
                       'data_collections', data_collections)$$,
  $$xid::text$$, NULL);
```

`xid` on each change lets a consumer group the changes of one commit; the summary tells
it the group is complete. This is the information Debezium's transaction topic carries.

There is no `BEGIN` event: the drain only sees committed transactions. A consumer that
buffers until the summary arrives has the same guarantee without one. `new` and `old`
are not in scope, and a mapping that reaches for a column is refused when it is defined.
A transaction stream cannot be snapshotted, since there is no prior state to read.

## Choosing

- An existing application you will not modify: use a CDC mapping.
- Row diffs are what consumers want (replication, search indexing, cache invalidation):
  use a CDC mapping.
- A domain event whose payload is a contract, or an event that must reflect exactly what
  the transaction saw: use `kafgres_produce()`.
- The producer is not this database: use the wire protocol, like any Kafka client.