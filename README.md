# kafgres

kafgres is a PostgreSQL extension that runs a Kafka broker inside Postgres. Kafka clients
(librdkafka, the Java client, Sarama, kafka-python, `kcat`, the `kafka-*.sh` tools)
connect to Postgres on port 9092 as they would to any broker.

This makes it possible to write to a table and publish an event in one transaction, and
to connect to the database using regular Kafka clients:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total) VALUES (...);
  SELECT kafgres_produce('order-events', 'OrderCreated',
                         jsonb_build_object(...)::text);
COMMIT;
```

Either both happen or neither does, so there is no outbox table, relay or dual-write
window. [docs/producing.md](docs/producing.md) describes the produce paths and when to
use each.

## Quick start

### Docker

Each [release](https://github.com/RayElg/kafgres/releases/latest) includes a prebuilt
image for PostgreSQL 13 to 18, on amd64 and arm64. The image has the extension
installed, `wal_level = logical` set, and `CREATE EXTENSION kafgres` run on first start.

```bash
curl -LO https://github.com/RayElg/kafgres/releases/latest/download/kafgres-postgres-pg16-amd64-image.tar.gz
docker load -i kafgres-postgres-pg16-amd64-image.tar.gz
docker run -d --name kafgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -p 9092:9092 kafgres-postgres:amd64
```

The loaded image is tagged with its architecture. On ARM hosts, use `arm64` in both the
file name and the tag. Use `pg13` to `pg18` in the file name for other Postgres versions.

### Pigsty packages

[Pigsty](https://pigsty.io) packages kafgres for PostgreSQL 14 to 18 on Debian, Ubuntu
and EL 8 to 10 ([package list](https://ext.pgsty.com/e/kafgres)). Use this to add kafgres
to a Postgres you already run.

```bash
curl -fsSL https://repo.pigsty.io/pig | bash   # installs the pig CLI
pig repo add pgsql -u                          # adds the Pigsty and PGDG apt/yum repos
pig ext install kafgres -v 16                  # -v is the PostgreSQL major version
```

The package installs files only. Add the following to `postgresql.conf`, restart
Postgres, and create the extension:

```
shared_preload_libraries = 'kafgres'
wal_level = logical                    # needed for CDC mappings; optional otherwise
kafgres.advertised_host = '<host that Kafka clients can reach>'
```

```sql
CREATE EXTENSION kafgres;
```

On Debian, installing the Postgres package creates a cluster of its own, on port 5433 if
5432 is taken. `pg_lsclusters` shows which port it uses. If you use `pig` to install
Postgres itself, or run the full Pigsty distribution, list `kafgres` among the cluster's
extensions instead.

### Extension tarball

Each release also has a tarball per Postgres major and architecture, containing the
shared library and extension files, for installing into any Postgres you manage yourself.
The steps are in the release notes.

### 5 second post-install demo

```bash
psql -h 127.0.0.1 -U postgres -tAc "SELECT kafgres_create_topic('hello', 1)"
echo 'first message' | kcat -b 127.0.0.1:9092 -t hello -P
kcat -b 127.0.0.1:9092 -t hello -C -e -q
```

This needs `psql` and [`kcat`](https://github.com/edenhill/kcat) on the host. With the
Docker image, `docker exec kafgres psql -U postgres -tAc "…"` replaces the `psql` line.
The last command prints `first message`. [`demo/kcat-demo.sh`](demo/kcat-demo.sh) goes
further: it creates a topic, produces keyed records, reads them back, uses a consumer
group, and queries the log's offsets from SQL. It uses a local `kcat` if there is one and
otherwise builds a small client image.

## CDC

A table's changes can be published to a topic without Kafka Connect or Debezium. A
mapping is written in SQL, so it can join other tables, filter with a predicate, and build
`jsonb`:

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

A background worker reads a logical replication slot using kafgres's own output plugin,
renders each mapping and produces the result:

```
$ psql -c "INSERT INTO orders VALUES (10, 1, 99.50, 'placed')"
$ kcat -b 127.0.0.1:9092 -t order-events -C -e -q -o beginning -f '%k -> %s\n'
  10 -> {"op": "I", "total": 99.50, "customer": {"tier": "gold"}, "order_id": 10}
```

`old` is also available in expressions. It comes from the WAL, so on a table with
`REPLICA IDENTITY FULL` it is the exact before-image. [docs/producing.md](docs/producing.md)
describes the full mapping interface.

An enrichment subquery runs when the change is rendered, so it sees current data, not the
data the original transaction saw. Debezium lookups and downstream stream joins behave the
same way. If an event must reflect exactly what the transaction saw, use
`kafgres_produce()`, which runs inside the transaction.

## What works

- **Clients.** Produce, consume and rebalance work with librdkafka and `kcat`, the Java
  client and its console tools, Sarama, franz-go and kafka-python. A Java
  `KafkaProducer` with default settings, including idempotence, needs no overrides.
- **Admin.** `kafka-topics.sh`, `kafka-configs.sh`, `kafka-consumer-groups.sh` and
  `kafka-acls.sh` work, as do transactions, share groups and the KIP-848 consumer
  protocol.
- **Retention and compaction.** Retention reclaims disk, and `cleanup.policy=compact`
  topics are compacted.
- **Authentication.** Clients can authenticate with SCRAM-SHA-256 against Postgres roles
  or with an mTLS certificate. ACLs are stored in a table managed with SQL.
- **Failover.** When an asynchronous replica is promoted, consumers are truncated to the
  new log end instead of reading data that diverged. This is tested against a physical
  standby.
- **Transactional produce.** `kafgres_produce()` commits atomically with the surrounding
  transaction.

### Conformance

The conformance suite runs five clients (Java, librdkafka, Sarama, franz-go and
kafka-python) through about 180 scenarios against both kafgres and a reference Kafka
broker, and compares what each client observes. It runs in CI on every pull request.
Every intended difference is listed in [docs/conformance.md](docs/conformance.md).

`demo/parity-demo.sh` is a short version: it runs the same commands against kafgres and
an `apache/kafka` container and diffs the output. It needs both images:

```bash
docker compose build && docker build -t kafgres-clients tests/clients
bash demo/parity-demo.sh
```

## Upgrading

Install the new build, restart Postgres, then run `ALTER EXTENSION kafgres UPDATE;` in the
database the broker uses. The broker keeps serving after the restart, before the update
runs.

- **From 0.3.0:** transaction markers written before 0.3.1 have no
  `EndTransactionMarker` value, which Sarama cannot decode. They stay until
  retention removes their segments. A Sarama consumer can start after the last such
  marker, or the topic can be recreated.
- **From 0.2.0:** the update also changes who may call the administrative functions (see
  [docs/producing.md](docs/producing.md#who-may-produce)) and fixes
  `kafgres_produce()`'s search path. On the segment engine, the first start removes the
  log's old-format `.timeindex` files. Timestamp lookups then scan the segments written
  before the upgrade, and segments written afterwards are indexed.

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/architecture.md](docs/architecture.md) | Design: one broker per instance, durability in Postgres terms, both storage engines, transactional produce, segment replication. |
| [docs/producing.md](docs/producing.md) | The three produce paths, CDC mappings, and how to choose between them. |
| [docs/conformance.md](docs/conformance.md) | Client matrix, how the suite runs, and the known differences from Kafka. |
| [docs/configuration.md](docs/configuration.md) | Every `kafgres.*` setting: scope, default and meaning. |

## Building from source

```bash
cargo test                    # codec and generator; no Postgres needed
docker compose build          # builds and links the extension
docker compose up -d

docker build -t kafgres-clients tests/clients
pip install -r requirements.txt
pytest tests/integration/     # real clients against a running broker
```

Use `docker compose build` rather than `cargo check`: `cargo check` skips the pgrx link
step and passes on code that cannot load. The image builds against PostgreSQL 16 by
default. Set `PG_MAJOR` (13 to 18) to build another major, as in
`PG_MAJOR=18 docker compose build`. CI builds and tests every major from 13 to 18.

Repository layout:

```
codec/          kafgres-codec, the wire protocol; no pgrx or Postgres dependency
  schemas/      Kafka message schemas, vendored at a pinned tag (codec/VENDORING.md)
  implemented.toml   which APIs are served, and at which versions
  src/generated/     generated by codec-gen and checked in; do not edit
codec-gen/      the generator: `cargo run -p kafgres-codec-gen`
extension/      the pgrx extension (its own workspace)
docs/           documentation
```

## License

kafgres is released under the Elastic License 2.0; see [LICENSE](LICENSE). The Kafka
message schemas vendored under `codec/schemas/` are Apache License 2.0, from upstream
Apache Kafka; see `codec/VENDORING.md`.
