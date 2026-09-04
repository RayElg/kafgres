# kafgres

A PostgreSQL extension that embeds a Kafka broker. Unmodified Kafka clients (librdkafka,
the Java client, Sarama, kafka-python, `kcat`, the `kafka-*.sh` tooling) connect to
Postgres on port 9092 and cannot tell the difference. Topics, partitions, offsets,
consumer groups, and the log itself live in the database.

The goal is transactional coupling between the database and the event stream:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total) VALUES (...);
  SELECT kafgres_produce('order-events', 'OrderCreated',
                         jsonb_build_object(...)::text);
COMMIT;
```

Both commit or neither does. There is no outbox table, no Debezium, no Connect cluster,
and no dual-write inconsistency window. See [docs/producing.md](docs/producing.md) for
the produce paths and when to use each.

## Status

The following all work today:

- Unmodified clients produce, consume, and rebalance: librdkafka and `kcat`, the Java
  client and its console tools, Sarama, and kafka-python.
- A default-config Java `KafkaProducer` with idempotence on (the default since Kafka
  3.0) works with no overrides.
- Admin APIs: `kafka-topics.sh`, `kafka-configs.sh`, `kafka-consumer-groups.sh`,
  `kafka-acls.sh`, transactions, share groups, and the KIP-848 consumer protocol.
- Retention reclaims disk, and `cleanup.policy=compact` topics are compacted on both
  storage engines.
- Clients authenticate with SCRAM-SHA-256 against Postgres roles or an mTLS certificate,
  with ACLs in a SQL-managed table.
- Consumers truncate rather than read divergent data when an async replica is promoted,
  verified against a real physical standby.
- The conformance suite drives four real clients against kafgres and a reference Kafka
  and diffs the observable results; it runs in CI on every commit, and every intended
  difference is catalogued in [docs/conformance.md](docs/conformance.md).
- The segment engine (the default) passes the same conformance suite, survives `kill -9`
  with every acknowledged record intact, and replicates its log to a standby out of
  band. Measured on the benchmark harness hardware, it produced about 1.5x the table
  engine's throughput with a lower p99, at about 1% degradation of co-resident pgbench
  against the table engine's 15%.
- `kafgres_produce()` commits atomically with a business write.
- CDC: a table's changes reach a topic through a logical decoding output plugin shipped
  with the extension, with the mapping written in SQL. See
  [docs/producing.md](docs/producing.md).

**If you run the segment engine, set `kafgres.segment_archive_command`.** The segment
engine's log lives in files, so `pg_basebackup` seeds a replica but is not a backup:
retention unlinks rolled segments that an archive would still want. The setting takes a
shell command with `%p`/`%f`, exactly as Postgres's own `archive_command` does, and
retention refuses to reclaim a segment the archive has not taken. As with WAL archiving,
a failing command stops reclamation and the disk grows, so watch
`kafgres_archive_status()`. The table engine needs none of this; its log is in Postgres
tables and `pg_basebackup` already covers it.

## Try it

`docker compose up -d` starts a Postgres preloaded with `kafgres` and creates the
extension on first start (`CREATE EXTENSION kafgres` runs from the image's init
scripts). On a cluster you administer yourself, add `kafgres` to
`shared_preload_libraries` and run `CREATE EXTENSION kafgres;`.

`scripts/kcat-demo.sh` then points `kcat` at it and runs the commands you would run
against any Kafka broker. It needs the broker up; with no local `kcat` it builds the
small test-client image on first use:

```
docker compose up -d
bash scripts/kcat-demo.sh
```

```
kafgres: a Kafka broker inside PostgreSQL

There is no broker process. Port 9092 is served by a Postgres background worker:

$ psql -tAc "SELECT 'PostgreSQL ' || current_setting('server_version')"
  PostgreSQL 16.14 (Debian 16.14-1.pgdg12+1)

$ psql -tAc "SELECT backend_type FROM pg_stat_activity
                   WHERE backend_type = 'kafgres_broker'"
  kafgres_broker

Topics are created in SQL, because a topic is a row:

$ psql -tAc "SELECT kafgres_create_topic('payments', 3)"
  121

Everything from here is plain kcat, pointed at 127.0.0.1:9092.

$ kcat -b 127.0.0.1:9092 -L -t payments
  Metadata for payments (from broker 1: 127.0.0.1:9092/1):
   1 brokers:
    broker 1 at 127.0.0.1:9092 (controller)
   1 topics:
    topic "payments" with 3 partitions:
      partition 0, leader 1, replicas: 1, isrs: 1
      partition 1, leader 1, replicas: 1, isrs: 1
      partition 2, leader 1, replicas: 1, isrs: 1

Produce three keyed records. The client hashes the key to a partition, so this
also checks that the broker serves the partition the client chose:

$ printf 'erin:{"amt":120}\nken:{"amt":80}\nalice:{"amt":210}\n' \
    | kcat -b 127.0.0.1:9092 -t payments -K: -P

Read them back with partition, offset and key:

$ kcat -b 127.0.0.1:9092 -t payments -C -e -q -o beginning -f '%p:%o  %k -> %s\n'
  0:0  erin -> {"amt":120}
  1:0  ken -> {"amt":80}
  2:0  alice -> {"amt":210}

Consumer groups work. The second run starts from the committed offset and
finds nothing left:

$ kcat -b 127.0.0.1:9092 -G payments-demo payments -e -q -o beginning -c 3 -f '%p:%o  %k -> %s\n'
  1:0  ken -> {"amt":80}
  0:0  erin -> {"amt":120}
  2:0  alice -> {"amt":210}

$ kcat -b 127.0.0.1:9092 -G payments-demo payments -e -q -f 'unexpected: %p:%o\n'
  (no output)

The log is part of the same database as the application tables that produced
them — here are its offsets, straight from SQL:

$ psql -c "SELECT partition, COALESCE(high_watermark, 0) AS log_end_offset,
                log_start_offset FROM kafgres_partition_offsets('payments')"
   partition | log_end_offset | log_start_offset
  -----------+----------------+------------------
           0 |              1 |                0
           1 |              1 |                0
           2 |              1 |                0
```

The script uses a local `kcat` if you have one and the test client image otherwise.
Nothing in it is kafgres-aware: every `kcat` line is one you would run against a real
broker.

## CDC without Debezium

A table's changes reach a topic without Kafka Connect, a second JVM, or a network hop.
The mapping is SQL, not a template, so it can join, filter with a real predicate, and
build `jsonb` that is typed by construction:

```sql
CREATE TABLE customers (id int PRIMARY KEY, name text, tier text);
CREATE TABLE orders (
    id          int PRIMARY KEY,
    customer_id int REFERENCES customers,
    total       numeric,
    status      text
);
INSERT INTO customers VALUES (1, 'Acme', 'gold');

SELECT kafgres_create_topic('order-events', 3);

SELECT kafgres_add_mapping(
    'orders', 'public.orders', 'order-events',
    value_expr  => $$ jsonb_build_object(
                        'order_id', new.id,
                        'total',    new.total,
                        'op',       op,
                        'customer', (SELECT jsonb_build_object('tier', c.tier)
                                       FROM customers c WHERE c.id = new.customer_id)) $$,
    key_expr    => $$ new.id::text $$,
    filter_expr => $$ new.status = 'placed' $$);
```

A background worker drains a logical replication slot fed by kafgres's own output plugin,
renders each mapping, and produces:

```
$ psql -c "INSERT INTO orders VALUES (10, 1, 99.50, 'placed')"
$ kcat -b 127.0.0.1:9092 -t order-events -C -e -q -o beginning -f '%k -> %s\n'
  10 -> {"op": "I", "total": 99.50, "customer": {"tier": "gold"}, "order_id": 10}
```

`old` is in scope too, and comes out of the WAL rather than a re-read, so on a table with
`REPLICA IDENTITY FULL` the before-image is exact as of the change. See
[docs/producing.md](docs/producing.md) for the full mapping interface.

One caveat: an enrichment subquery runs when the change is rendered, so it reads current
state rather than the state the transaction saw. That is the same trade a Debezium SMT
lookup or a downstream stream-join makes. When an event must reflect exactly what the
transaction saw, use `kafgres_produce()`, which runs inside the transaction.

## Parity demo

`scripts/parity-demo.sh` runs the same commands against kafgres and against a real
`apache/kafka` broker and diffs the output. It needs both images and nothing else:

```
docker compose build && docker build -t kafgres-clients tests/clients
bash scripts/parity-demo.sh
```

```
kafgres parity demo
Left:  kafgres: a Kafka broker inside PostgreSQL, on 127.0.0.1:9092
Right: apache/kafka: a reference Kafka broker, on 127.0.0.1:9292
Every check runs the identical command against both and diffs the output.

Admin tooling (the Java client, unmodified)
  ✓ kafka-topics.sh --describe                     identical
  ✓ kafka-topics.sh --list                         identical
  ✓ kafka-configs.sh --describe                    identical
  ✓ kafka-get-offsets.sh                           identical

Produce and consume (librdkafka / kcat)
  ✓ kcat -P then -C, 6 keyed records over 3 partitions identical
  ✓ kcat -L metadata                               identical

Independent clients (no shared code with librdkafka or Java)
  ✓ Sarama (Go) - produce, consume, offsets        identical
  ✓ Sarama (Go) - consumer group, commit           identical
  ✓ kafka-python - produce, consume, offsets       identical
  ✓ kafka-python - consumer group, commit          identical
  ✓ unknown topic returns the same error code      identical

Consumer groups
  ✓ kafka-consumer-groups.sh --describe (lag, offsets) identical
  ✓ kafka-consumer-groups.sh --list                identical

The difference
Everything above matched. This does not:

  $ psql -c "SELECT partition, COALESCE(high_watermark, 0) AS log_end_offset,
                log_start_offset FROM kafgres_partition_offsets('parity-demo')"
   partition | log_end_offset | log_start_offset
  -----------+----------------+------------------
           0 |              2 |                0
           1 |              1 |                0
           2 |              3 |                0

13/13 checks identical  ✓
```

A check that fails prints the diff and the script exits non-zero. Two differences are
normalized away, both catalogued in [docs/conformance.md](docs/conformance.md):
`min.insync.replicas`, which the broker does not report because it does not honour it,
and the ELR columns, which the tool renders as `N/A` against kafgres because it falls
back to `Metadata`. Anything else that differs is treated as a bug, and the script shows
it rather than absorbing it.

The demo is a summary; `tests/conformance/` is the gate. It runs the same four clients
in seventeen tests, in CI on every commit.

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/architecture.md](docs/architecture.md) | The design: one broker per instance, how durability maps onto Postgres, both storage engines, transactional produce, segment replication. |
| [docs/producing.md](docs/producing.md) | The three produce paths, CDC mappings, and how to choose between them. |
| [docs/conformance.md](docs/conformance.md) | The client matrix, how the suite runs, and the catalogue of client-visible differences from Kafka. |
| [docs/configuration.md](docs/configuration.md) | Every `kafgres.*` setting: scope, default, and what it controls. |


## Prior art

The Kafka protocol has been reimplemented several times over non-Kafka logs: Redpanda
(C++), WarpStream (S3), AutoMQ (S3-backed fork), Kafka-on-Pulsar. kafgres differs by
using Postgres as the log and by producing inside the same transaction as the
application write.

## Layout

```
codec/          kafgres-codec, the wire protocol. No pgrx, no Postgres, unit-testable alone.
  schemas/      Kafka message schemas, vendored at a pinned tag (codec/VENDORING.md)
  implemented.toml   which APIs are served, and at which versions
  src/generated/     emitted by codec-gen and checked in; do not edit
codec-gen/      the generator. `cargo run -p kafgres-codec-gen`
extension/      the pgrx extension. Its own workspace on purpose.
docs/           architecture.md, producing.md, conformance.md
```

## Build

```bash
cargo test                    # codec + generator. No Postgres needed.
docker compose build          # the only way to verify the extension links and loads
docker compose up -d

docker build -t kafgres-clients tests/clients
pip install -r requirements.txt   # pytest + kafka-python for the test suites
pytest tests/integration/         # real clients against a running broker
```

Try it:

```sql
SELECT kafgres_create_topic('orders', 3);
```

```console
$ kcat -b localhost:9092 -L
 1 brokers:
  broker 1 at localhost:9092 (controller)
  topic "orders" with 3 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
```

`cargo check` does not exercise the pgrx/Postgres link step and passes on code that
cannot load. Use `docker compose build`.

## License

Released under the Elastic License 2.0; see [LICENSE](LICENSE). The Kafka message
schemas vendored under `codec/schemas/` are Apache License 2.0, upstream Apache Kafka;
see `codec/VENDORING.md`.