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

kafgres serves all 75 API keys the reference broker serves.
`kafka-broker-api-versions.sh` knows 77; the two neither broker advertises are `71
GetTelemetrySubscriptions` and `72 PushTelemetry`, which a stock 4.x broker does not offer
without a metrics reporter configured. The served set and version ranges are declared in
`codec/implemented.toml`, and the same declaration generates both the dispatch table and
the ApiVersions payload, so what is advertised and what is implemented cannot drift.

A client's version probe reads the set of advertised keys, not their ranges. franz-go,
which Redpanda Console, `kcat` and the Go ecosystem use, treats any missing key as an
old broker however current the served ranges are, so a cluster that omits a key is
reported as pre-1.0.

### Feature flags

`ApiVersions` also reports the cluster features kafgres implements. Clients pick
protocol behaviour from these rather than from a version range: the Java client uses
KIP-890 epoch rotation only when `transaction.version` is finalized at 2, and reads a
missing feature as off.

| Feature | Finalized | Meaning here |
|---|---|---|
| `transaction.version` | 2 | KIP-890. Configurable through `kafgres.transaction_version`; 2 is what a stock 4.x cluster finalizes. |
| `group.version` | 1 | The KIP-848 consumer protocol is served. |
| `share.version` | 1 | KIP-932 share groups are served. |

`kraft.version`, `metadata.version` and `eligible.leader.replicas.version` are absent
because there is no Raft log, no metadata image and no ELR, and `streams.version` because
KIP-1071 is not served.

### Keys served for probe completeness

Each key below answers what Kafka answers when the feature is unavailable or the request
cannot apply. None returns a success the broker cannot back.

| API keys | Answer |
|---|---|
| `34 AlterReplicaLogDirs` | One log directory: a move to it is a no-op success, any other path is `LOG_DIR_NOT_FOUND`. |
| `38` to `41` delegation tokens | `DELEGATION_TOKEN_AUTH_DISABLED`. kafgres has no `delegation.token.secret.key`; principals are Postgres roles or mTLS certificates. |
| `45 AlterPartitionReassignments` | Replication is Postgres's, so a partition has one replica. A cancellation returns `NO_REASSIGNMENT_IN_PROGRESS` and an assignment `INVALID_REPLICA_ASSIGNMENT`. `46` answers that no reassignment is in progress. |
| `55 DescribeQuorum`, `80`/`81` Raft voters | There is no metadata quorum; replication and failover are Postgres's, so there is no Raft log to inspect. |
| `57 UpdateFeatures` | The served protocol is fixed at build time, so the answer is `FEATURE_UPDATE_FAILED`: per feature at v0/v1, top level at v2, where the per-feature array does not exist. |
| `64 UnregisterBroker` | The cluster is one broker and it is this process. |
| `83` to `87` share-coordinator persister | The share coordinator is this same process, so the peer these RPCs expect does not exist. Gated on `CLUSTER_ACTION` first, as Kafka gates it. |
| `88`/`89` streams groups | `UNSUPPORTED_VERSION`, what a broker without `streams` among its rebalance protocols returns. |

Three keys return live state instead of a fixed answer. `74 ListConfigResources` lists
the broker and every topic, matching what `DescribeConfigs` serves; it serves v1 only,
because v0 of that key was `ListClientMetricsResources`, which lists different
resources. `90`/`91`/`92` are the share-group offset APIs `kafka-share-groups.sh` drives,
and they read and write the same table `ShareFetch` takes a group's starting position
from, so a reset moves where the next acquire begins.

### Version ranges narrower than Kafka's

- **`27 WriteTxnMarkers` serves v1, Kafka v1 to v2.** v2 adds a `TransactionVersion` field
  to an inter-broker RPC; there is no peer broker to send it.
- **`68 ConsumerGroupHeartbeat` serves v0, Kafka v0 to v1.** v1 adds
  `SubscribedTopicRegex`, which requires resolving a pattern against the topic list on
  every heartbeat and re-resolving it when topics appear. Advertising it without that
  would silently match nothing.

### Served with differences

- **Transactions (24 to 28, including `27 WriteTxnMarkers`).** Kafka's own
  `transactional.id` protocol works. Aborted transactions are decided from an index
  written at `EndTxn` and queried by offset range, rather than by scanning a fetch
  response, because a transaction's batches and its abort marker can span more than one
  response.
- **ACL administration (29 to 31).** The rules live in `kafgres_acls`; these are the
  RPCs `kafka-acls.sh` uses to manage them.
- **Admin APIs (33, 35, 46, 48, 61, 65, 66).** The tier a UI reaches for.
- **Share groups (76 to 79, and 90 to 92).** Every member reads every partition, and the
  broker tracks the state of individual records rather than one offset, as verified
  against the real `KafkaShareConsumer`. Fair distribution is not guaranteed, by Kafka or
  here: a consumer that drains faster takes more. `ShareFetch` does not park, so an idle
  share consumer polls rather than long-polls.
  `ShareFetch` and `ShareAcknowledge` serve v2: KIP-1222 renew acknowledgements extend a
  record's lock instead of completing it, and KIP-1206 `ShareAcquireMode` is honoured as
  record-limit because acquisition never exceeds `MaxRecords`. A reset through `91` or a
  delete through `92` also clears the per-record state; otherwise the acquire scan would
  skip records whose state still reads done.
- **`49 AlterClientQuotas`, `51 AlterUserScramCredentials`, `75 DescribeTopicPartitions`.**
  `throttle_time_ms` is reported, not enforced by muting: a client that ignores it is
  not slowed.
- **KIP-848 consumer groups (68, 69).** Server-side assignment, with the classic
  protocol still available.
- **`ListOffsets` serves the full 1 to 11.** v7's `MAX_TIMESTAMP` (KIP-734) returns the
  offset of the record with the greatest timestamp, decoded from the winning batch rather
  than taken from its base offset; the two differ when a producer stamps timestamps out
  of order, which is the case the sentinel exists for. The log is entirely local, so
  `EARLIEST_LOCAL` is the log start, and `LATEST_TIERED` and `EARLIEST_PENDING_UPLOAD`
  have no offset to report. Each sentinel is refused below the version that introduced
  it, because a sentinel is a negative number in a field that otherwise carries a
  timestamp.

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
- **`ListTransactions`'s id pattern is a POSIX regular expression, not RE2.** Postgres
  evaluates the filter, so the dialect is Postgres's. The pattern is anchored to the
  whole id, matching Kafka, but a Perl-style class such as `\p{L}+`, which Kafka's RE2J
  compiles, is refused here with `INVALID_REGULAR_EXPRESSION`. Plain patterns,
  alternation and character classes behave the same on both. Evaluating the pattern in
  the database keeps a filtered listing from materialising every open transaction first.
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