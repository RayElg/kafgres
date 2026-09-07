# Conformance

A response can satisfy every schema constraint and still hang a client. This suite
drives real Kafka clients against kafgres and, optionally, against a real Kafka broker,
and compares what the clients observe. The clients, not the protocol schema, are the
specification: librdkafka and the Java client disagree about edge cases, and both are
correct in the sense that matters, which is that users run them.

## Running it

```bash
docker compose build
docker compose up -d
docker build -t kafgres-clients tests/clients
pip install -r requirements.txt

pytest tests/conformance/                       # matrix only; reference diffs skip

docker compose --profile conformance up -d kafka
pytest tests/conformance/                       # matrix + reference diff
```

Two halves. The **matrix** runs each client's scenarios against kafgres and needs
nothing but the broker. The **reference diff** runs the identical scenarios against
`apache/kafka:4.3.1` and compares the observable results. The reference half is
profile-gated because it needs a second broker, and it skips rather than fails when the
broker is absent. CI runs both halves.

`codec/KAFKA_VERSION` pins the message schemas the codec is generated from. The
reference image tag is matched to that version manually, in `docker-compose.yml`,
`scripts/parity-demo.sh`, and `tests/conformance/test_clients.py`. An unpinned older
broker turns version skew into false deviations: 4.1.0 serves OffsetCommit and
OffsetFetch only up to v9, so a correct v10 advertisement looks like over-advertising.

## What is compared

Observable output, not bytes. Two brokers may legitimately differ in timing, partition
assignment order, or error text; what must match is what a program using the client
would decide. Each scenario runner prints one machine-readable line, and the line is the
comparison.

## Clients in the matrix

| Client | Where it runs | Why |
|---|---|---|
| librdkafka / `kcat` | integration suites throughout | Most widely deployed. |
| Java (`kafka-*.sh`) | integration suites, plus an API-surface test here | Defines correct behaviour in practice; exercises admin APIs no library touches. |
| Sarama (Go) | `tests/clients/sarama/` | Independent implementation sharing no code with the other three. It pins its API version, so it breaks when kafgres advertises something it does not implement. |
| kafka-python | `tests/clients/python/` | Probes and downgrades rather than pinning, so it exercises older API versions. |

The Java tooling additionally gets
`test_the_advertised_api_surface_is_a_subset_of_kafkas`, which asserts every advertised
version range stays inside Kafka's own.

## Advertised API surface

kafgres advertises 53 of the 77 API keys `kafka-broker-api-versions.sh` knows. The
served set and version ranges are declared in `codec/implemented.toml`, and the same
declaration generates both the dispatch table and the ApiVersions payload, so what is
advertised and what is implemented cannot drift.

### APIs not implemented

| Family | API keys | Reason |
|---|---|---|
| Streams groups (KIP-1071) | 88, 89 | Out of scope. |
| Share-group coordinator state | 83 to 87, 90 to 92 | These are how a broker talks to the share coordinator, and here that is the same process. The client-facing four (76 to 79) are served. |
| Delegation tokens | 38 to 41 | Out of scope. |
| Raft voters | 55, 64, 80, 81 | No Raft. `46 ListPartitionReassignments` answers "none in progress", which is true for a single broker. |
| `UpdateFeatures`, `ListConfigResources`, telemetry | 57, 74, 71, 72 | Out of scope. Telemetry (KIP-714) is probed by 4.x clients, and declining it is handled. |
| Partition reassignment | 45 | One broker whose replication is Postgres's. Accepting an instruction that cannot be carried out would be worse than refusing it. |
| `AlterReplicaLogDirs` | 34 | One log directory. |

### Served with differences

- **Transactions (24 to 28, including `27 WriteTxnMarkers`).** Kafka's own
  `transactional.id` protocol works. Aborted transactions are decided from an index
  written at `EndTxn` and queried by offset range, rather than by scanning a fetch
  response, because a transaction's batches and its abort marker can span more than one
  response.
- **ACL administration (29 to 31).** The rules live in `kafgres_acls`; these are the
  RPCs `kafka-acls.sh` uses to manage them.
- **Admin APIs (33, 35, 46, 48, 61, 65, 66).** The tier a UI reaches for.
- **Share groups (76 to 79).** Every member reads every partition, and the broker
  tracks the state of individual records rather than one offset, as verified against the
  real `KafkaShareConsumer`. Fair distribution is not guaranteed, by Kafka or here: a
  consumer that drains faster takes more. `ShareFetch` does not park, so an idle share
  consumer polls rather than long-polls.
- **`49 AlterClientQuotas`, `51 AlterUserScramCredentials`, `75 DescribeTopicPartitions`.**
  `throttle_time_ms` is reported, not enforced by muting: a client that ignores it is
  not slowed.
- **KIP-848 consumer groups (68, 69).** Server-side assignment, with the classic
  protocol still available.
- **`ListOffsets` tops out at v6** (Kafka serves 1 to 11). Version 7 introduces a
  `MAX_TIMESTAMP` sentinel and version 9 `LATEST_TIERED_OFFSET`, neither of which is
  implemented, and advertising them would invite a query the broker can only answer
  wrongly. Clients negotiate down.

### Configuration reporting

- `kafka-topics.sh --describe` reports the topic config keys the broker actually
  implements: `retention.ms`, `retention.bytes`, `cleanup.policy`, and `segment.bytes`.
  Kafka reports `min.insync.replicas=1`; kafgres reports nothing there, because it does
  not honour the setting (replication is Postgres's), and reporting an unimplemented
  setting invites clients to act on it.
- `__consumer_offsets` does not exist in Metadata or in `kafka-topics.sh --list`.
  Consumer group offsets live in `kafgres_offsets`, and every group API answers from
  there, so `kafka-consumer-groups.sh` and admin-protocol UIs see everything they
  expect. Tools that consume `__consumer_offsets` directly to compute lag read nothing;
  a synthetic topic that lists in Metadata but yields no records was considered and
  rejected, because a tool would conclude the groups have committed nothing rather than
  that the topic is not consumable.

### Behavioural differences

- **`OffsetDelete` refuses for any topic while the group has members.** Kafka refuses
  only for topics the group is subscribed to. kafgres never parses a member's
  subscription metadata, so it cannot tell the two cases apart, and refusing is the
  conservative direction: deleting the offsets of a topic a live consumer reads moves
  that consumer's position at its next restart. The error code is the one Kafka defines,
  which `kafka-consumer-groups.sh` renders as `GroupSubscribedToTopicException` either
  way.
- **Leader epochs are not consecutive.** Kafka increments the epoch by one per
  election; kafgres uses the Postgres timeline id, so it jumps. The protocol requires
  monotonicity, not consecutiveness, and a client that assumed `+1` was already broken
  against real Kafka.
- **Frame and message limits.** `kafgres.max_request_bytes` (SIGHUP-reloadable, default
  32 MiB, range 1 to 100 MiB) bounds a produce request, and Kafka allows up to 100 MB.
  A stock librdkafka producer aggregates a request across partitions until it exceeds
  the per-partition `message.max.bytes`, so multi-partition producers reach the frame
  cap even when no single message is large; the limit is what keeps the aggregate
  per-connection buffering inside a fixed memory budget inside a Postgres backend.
  `max.message.bytes` has an upper bound for the same reason on the fetch side: Fetch
  must return the first batch whole, so an oversized batch could be produced but never
  fetched, which stalls the partition silently.

## Debezium parity

For a consumer written against Debezium, mappings cover most of what it provides. What
such a consumer will notice:

- **The envelope.** `lsn`, `xid` and `commit_ts` are in a mapping's scope, so a
  Debezium-shaped event can be built:

  ```sql
  jsonb_build_object('op', op, 'lsn', lsn::text, 'xid', xid,
                     'ts_ms', extract(epoch from commit_ts) * 1000,
                     'after', to_jsonb(new), 'before', to_jsonb(old))
  ```

  Changes from one commit report the same `xid` and the same `commit_ts`, and changes
  from separate transactions do not. On a snapshot row `lsn` is zeroed (`0/0`) while
  `xid` and `commit_ts` are NULL, so a backfill cannot be mistaken for one shared
  transaction. Still absent
  from the `source` block: the snapshot flag (`op` is `R`, which carries the
  information), and the database and connector names, which a mapping can write itself.
- **Transaction boundaries.** A mapping naming the reserved source `kafgres.transaction`
  receives one summary per commit with `event_count` and `data_collections`, the
  per-table counts Debezium's transaction topic carries. There is no `BEGIN` event; see
  [producing.md](producing.md).
- **No ad-hoc or incremental snapshot.** `kafgres_snapshot_mapping` re-snapshots a whole
  mapping and holds the drain for the duration. Debezium's signal table can snapshot a
  subset of a table or a newly added table without stopping the stream.
- **No heartbeat topic needed.** The drain advances the slot past every change it
  peeked, mapped or not, so an idle mapped table never pins WAL while the database is
  busy elsewhere. There is nothing to configure.

## What this suite does not cover

Stated so the suite is not read as broader than it is.

- **Throughput and latency.** Performance is measured separately and is not part of
  this suite.
- **Compression codecs end to end.** The integration suite asserts record-bytes
  round-tripping per codec; the conformance scenarios do not vary compression.
- **TLS and SASL under the matrix.** Covered by
  `tests/integration/test_tls.py` and the neighbouring auth and ACL suites,
  against librdkafka and the Java tooling. The Sarama and kafka-python runners speak
  PLAINTEXT.
- **Failover.** `tests/integration/test_failover.py`, behind the `failover` profile, needs
  a real physical standby.
- **Anything a scenario does not do.** The scenario count is a floor, not a ceiling.
  When a client reports a bug, the fix is a new scenario here first.

## Adding a deviation

If a reference-diff test fails and the difference is intended, it does not get a
normalizer in the test. It gets an entry above, with the reason. The test asserting
exact equality is what keeps this document honest; weakening the assertion to make a
diff go away removes the only mechanism that does that.