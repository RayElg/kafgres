# Configuration

Every setting is a Postgres GUC on the `kafgres.` prefix. Read one with `SHOW`, set one
with `ALTER SYSTEM SET ...` or `ALTER ROLE/DATABASE ... SET ...`, and apply
reloadable settings with `pg_reload_conf()` or SIGHUP.

Settings marked "restart" are Postmaster context: they need a Postgres restart. Settings
marked "reload" apply on SIGHUP, but most of them are only read by the broker worker at
its next event-loop pass; a few (noted in the description) need the worker itself to
restart, which a `SELECT pg_terminate_backend(pid)` of the broker backends or an instance
restart does.

## Broker and identity

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.database` | reload | `postgres` | Database the kafgres background workers (broker, CDC, archiver, follower) connect to. Changing it requires an instance restart. |
| `kafgres.port` | reload | `9092` | TCP port the Kafka listener binds. Changing it requires a broker worker restart. |
| `kafgres.bind_host` | reload | `0.0.0.0` | Address the Kafka listener binds. Changing it requires a broker worker restart. |
| `kafgres.advertised_host` | reload | `localhost` | Host clients are told to connect to, the `advertised.listeners` equivalent. |
| `kafgres.advertised_port` | reload | `0` | Port clients are told to connect to; `0` means use `kafgres.port`. |
| `kafgres.node_id` | reload | `1` | Broker node id reported in Metadata. |
| `kafgres.cluster_id` | reload | `kafgres-cluster` | Cluster id reported in Metadata. |
| `kafgres.tick_interval_ms` | reload | `5` | Broker event loop poll interval in milliseconds (1 to 1000). Changing it requires a broker worker restart. |
| `kafgres.auto_create_topics` | reload | on | Create a topic the first time a client produces to or fetches from it, as Kafka's `auto.create.topics.enable` does. |
| `kafgres.storage_engine` | restart | `segment` | Log storage engine: `segment` (default) or `table`. Does not migrate existing data. |
| `kafgres.allow_engine_mismatch` | restart | off | Start even if a log written by the other storage engine is present. That log stays intact but invisible. |

## Segment engine

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.segment_bytes` | reload | `64 MiB` | Bytes a segment file reaches before rolling. |
| `kafgres.segment_offsets` | reload | `1000000` | Offsets per log segment on the table engine, which is its retention granularity; the segment engine does not read it. Set before a partition has data: changing it later makes segment ranges overlap. |
| `kafgres.segment_lock_stripes` | restart | `16` | Lock shards for segment-engine append positions; `1` makes every partition share one lock. Narrowing also narrows capacity. |
| `kafgres.log_directory` | restart | empty | Directory holding the segment log. Empty means `$PGDATA/kafgres`; a relative path is under `$PGDATA`. For improved co-residency performance, put it on a device other than the WAL's: with both on one device the database's commit flush queues behind the log's writeback. |
| `kafgres.segment_archive_command` | reload | empty | Shell command shipping one rolled segment to an archive; `%p` is its path, `%f` its filename. Empty disables archiving. Setting it makes retention wait for the archive. |
| `kafgres.archive_interval_ms` | reload | `10000` | How often the archiver ships sealed segments; `0` disables it. |
| `kafgres.replicate_from` | reload | empty | `host:port` of the leader to pull log from on a standby; empty disables it. The follower reads it when it starts: a reload can clear it, but setting it on a standby that started without it takes an instance restart, and changing the address takes a restart of the follower worker. |
| `kafgres.allow_transactional_produce` | reload | on | Enable `kafgres_produce()`, the transactional SQL produce path. |

## Retention and compaction

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.retention_check_interval_ms` | reload | `60000` | How often the broker applies retention and compaction, as Kafka's `log.retention.check.interval.ms`. |
| `kafgres.compaction_pass_bytes` | reload | `32 MiB` | Bytes compaction reads in the broker per retention check, across all partitions; a larger plan resumes at the next check. With the defaults the broker cleans up to about 0.5 MiB/s in total. Raising it lengthens the pause each check takes from serving clients. |

## CDC

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.cdc_interval_ms` | reload | `1000` | How often the CDC worker drains the logical replication slot; `0` disables draining. |
| `kafgres.cdc_batch_size` | reload | `10000` | Changes peeked from the CDC slot per drain. |
| `kafgres.cdc_snapshot_batch_rows` | reload | `1000` | Source rows read per CDC snapshot batch; each batch is one transaction. |

## TLS, authentication and ACLs

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.tls_cert_file` | reload | empty | PEM server certificate chain. TLS is enabled when this and `kafgres.tls_key_file` are both set; a reload applies a change to new connections. |
| `kafgres.tls_key_file` | reload | empty | PEM private key for `kafgres.tls_cert_file`. |
| `kafgres.tls_ca_file` | reload | empty | PEM CA bundle that client certificates are verified against; enables mTLS. |
| `kafgres.tls_client_cert_required` | reload | off | Refuse the TLS handshake unless the client presents a certificate valid against `kafgres.tls_ca_file`. |
| `kafgres.sasl_required` | reload | off | Require SASL/SCRAM-SHA-256 authentication against `pg_authid` roles. |
| `kafgres.acls_enabled` | reload | off | Enforce the `kafgres_acls` table, for Kafka clients and for `kafgres_produce()` callers (as `User:<role>`). Off by default: with it on and no matching rule, the answer is refusal. |
| `kafgres.superusers` | reload | empty | Semicolon-separated principals that bypass every ACL check, for example `User:admin`. |

## Producers and limits

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.max_request_bytes` | reload | `32 MiB` | Largest inbound request frame, as Kafka's `socket.request.max.bytes`. A bounded number of connections may exceed the 8 MiB free tier at a time. |
| `kafgres.transaction_version` | reload | `2` | Kafka's `transaction.version` feature level, reported in `ApiVersions` and matching what a stock 4.x cluster finalizes. At `2`, `EndTxn` v5 hands the producer a fresh epoch with the result, so its next transaction needs no `InitProducerId` (KIP-890). Set `1` to keep the pre-KIP-890 behaviour. |
| `kafgres.fsync_before_ack` | reload | `on` | Make record bytes durable before the produce response leaves the broker (segment engine). One fsync per produce pass, not per request, so a pipelining client shares one flush across everything in flight. Off, the log is fsynced only when a segment rolls, so `acks=all` returns while the records are in the page cache: they survive `kill -9` but not a power cut. Wire produce only: `kafgres_produce()` always fsyncs before its transaction commits. No effect on the table engine. |
| `kafgres.relaxed_produce_commit` | reload | `off` | Let a wire-protocol produce commit without waiting for its WAL flush (segment engine, non-transactional only). On saves one device barrier per request. The cost: a crash can lose the idempotent-producer window's newest entries. An idempotent producer resending its in-flight batches is then refused as unknown: librdkafka fails the producer, and the Java client starts a new epoch and can write those batches twice. Never applies to transactional produce, to `kafgres_produce()`, or to the table engine. |
| `kafgres.producer_id_expiration_ms` | reload | `86400000` (24 h) | Drop idempotent-producer state idle this long; `0` disables expiration. |
| `kafgres.max_producer_ids` | reload | `10000` | Ceiling on retained producer ids; the least recently used are dropped first, `0` disables. |
| `kafgres.share_record_lock_duration_ms` | reload | `30000` | How long a share-group consumer holds an acquired record before it is offered again. |

Both durability settings are strict by default. Relaxing both (`fsync_before_ack = off`,
`relaxed_produce_commit = on`) raises produce throughput. Measured on an i9-13900 with
power-loss-protected NVMe, segment engine, `acks=all`, idempotent Java producers:

| Workload | Relaxed | Strict |
|---|---|---|
| 1 KiB records, 256 KiB batches, 1 producer | 635 MB/s | 558 MB/s |
| 1 KiB records, 256 KiB batches, 4 producers | 806 MB/s | 605 MB/s |
| 256 KiB records, 1 MiB batches, 1 producer | 728 MB/s | 708 MB/s |
| 256 KiB records, 1 MiB batches, 4 producers | 937 MB/s | 849 MB/s |

Small records from many producers pay the most. `kafgres_produce()` is always strict.

## Consumer groups

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.group_initial_rebalance_delay_ms` | reload | `500` | How long a forming consumer group holds its join window open before cutting a generation. A cold group would otherwise close it as soon as every member it knows about has rejoined — one member when the group is empty — so its peers arrive into an already-cut generation and pay another round. Holding the window open batches simultaneous arrivals into a single round; each new member extends it, capped by the group's rebalance timeout. `0` disables the wait. |