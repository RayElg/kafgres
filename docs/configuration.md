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
| `kafgres.database` | reload | `postgres` | Database the broker background worker connects to. Changing it requires a broker worker restart. |
| `kafgres.port` | reload | `9092` | TCP port the Kafka listener binds. Changing it requires a broker worker restart. |
| `kafgres.bind_host` | reload | `0.0.0.0` | Address the Kafka listener binds. Changing it requires a broker worker restart. |
| `kafgres.advertised_host` | reload | `localhost` | Host clients are told to connect to, the `advertised.listeners` equivalent. |
| `kafgres.advertised_port` | reload | `0` | Port clients are told to connect to; `0` means use `kafgres.port`. |
| `kafgres.node_id` | reload | `1` | Broker node id reported in Metadata. |
| `kafgres.cluster_id` | reload | `kafgres-cluster` | Cluster id reported in Metadata. |
| `kafgres.tick_interval_ms` | reload | `5` | Broker event loop poll interval in milliseconds (1 to 1000). |
| `kafgres.auto_create_topics` | reload | on | Create a topic the first time a client produces to or fetches from it, as Kafka's `auto.create.topics.enable` does. |
| `kafgres.storage_engine` | restart | `segment` | Log storage engine: `segment` (default) or `table`. Does not migrate existing data. |
| `kafgres.allow_engine_mismatch` | restart | off | Start even if a log written by the other storage engine is present. That log stays intact but invisible. |

## Segment engine

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.segment_bytes` | reload | `64 MiB` | Bytes a segment file reaches before rolling. |
| `kafgres.segment_offsets` | reload | `1000000` | Offsets per log segment, which is the retention granularity. Set before a partition has data: changing it later makes segment ranges overlap. |
| `kafgres.segment_lock_stripes` | restart | `16` | Lock shards for segment-engine append positions; `1` makes every partition share one lock. Narrowing also narrows capacity. |
| `kafgres.segment_archive_command` | reload | empty | Shell command shipping one rolled segment to an archive; `%p` is its path, `%f` its filename. Empty disables archiving. Setting it makes retention wait for the archive. |
| `kafgres.archive_interval_ms` | reload | `10000` | How often the archiver ships sealed segments; `0` disables it. |
| `kafgres.replicate_from` | reload | empty | `host:port` of the leader to pull log from on a standby; empty disables it. |
| `kafgres.allow_transactional_produce` | reload | on | Enable `kafgres_produce()`, the transactional SQL produce path. |

## CDC

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.cdc_interval_ms` | reload | `1000` | How often the CDC worker drains the logical replication slot; `0` disables draining. |
| `kafgres.cdc_batch_size` | reload | `10000` | Changes peeked from the CDC slot per drain. |
| `kafgres.cdc_snapshot_batch_rows` | reload | `1000` | Source rows read per CDC snapshot batch; each batch is one transaction. |

## TLS, authentication and ACLs

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.tls_cert_file` | reload | empty | PEM server certificate chain. TLS is enabled when this and `kafgres.tls_key_file` are both set; changing them requires a broker worker restart. |
| `kafgres.tls_key_file` | reload | empty | PEM private key for `kafgres.tls_cert_file`. |
| `kafgres.tls_ca_file` | reload | empty | PEM CA bundle that client certificates are verified against; enables mTLS. |
| `kafgres.tls_client_cert_required` | reload | off | Refuse the TLS handshake unless the client presents a certificate valid against `kafgres.tls_ca_file`. |
| `kafgres.sasl_required` | reload | off | Require SASL/SCRAM-SHA-256 authentication against `pg_authid` roles. |
| `kafgres.acls_enabled` | reload | off | Enforce the `kafgres_acls` table. Off by default: with it on and no matching rule, the answer is refusal. |
| `kafgres.superusers` | reload | empty | Semicolon-separated principals that bypass every ACL check, for example `User:admin`. |

## Producers and limits

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.max_request_bytes` | reload | `32 MiB` | Largest inbound request frame, as Kafka's `socket.request.max.bytes`. A bounded number of connections may exceed the 8 MiB free tier at a time. |
| `kafgres.transaction_version` | reload | `2` | Kafka's `transaction.version` feature level, reported in `ApiVersions` and matching what a stock 4.x cluster finalizes. At `2`, `EndTxn` v5 hands the producer a fresh epoch with the result, so its next transaction needs no `InitProducerId` (KIP-890). Set `1` to keep the pre-KIP-890 behaviour. |
| `kafgres.fsync_before_ack` | reload | `off` | Make record bytes durable before the produce response leaves the broker (segment engine). Without it the log is fsynced only when a segment rolls, so `acks=all` returns while the records are still in the page cache: they survive `kill -9` but not a power cut. The fsync is per produce pass, not per request, so a pipelining client amortises one flush across everything in flight. No effect on the table engine, whose records are Postgres rows made durable by the commit. |
| `kafgres.relaxed_produce_commit` | reload | `on` | Let a wire-protocol produce commit without waiting for its WAL flush (segment engine, non-transactional only). Acknowledged records already live in the page cache rather than on the platter, so a synchronous WAL flush of the metadata about them buys durability the records themselves do not have, at one device barrier per request; with it on, that metadata rides the WAL writer's next flush. The cost: after an OS or power failure the idempotent-producer window may be missing its newest entries, so a retried in-flight batch can land twice — at-least-once instead of exactly-once across an unclean shutdown. Never applies to transactional produce, to `kafgres_produce()`, or to the table engine. |
| `kafgres.producer_id_expiration_ms` | reload | `86400000` (24 h) | Drop idempotent-producer state idle this long; `0` disables expiration. |
| `kafgres.max_producer_ids` | reload | `10000` | Ceiling on retained producer ids; the least recently used are dropped first, `0` disables. |
| `kafgres.share_record_lock_duration_ms` | reload | `30000` | How long a share-group consumer holds an acquired record before it is offered again. |

## Consumer groups

| Setting | Reload | Default | Description |
|---|---|---|---|
| `kafgres.group_initial_rebalance_delay_ms` | reload | `500` | How long a forming consumer group holds its join window open before cutting a generation. A cold group would otherwise close it as soon as every member it knows about has rejoined — one member when the group is empty — so its peers arrive into an already-cut generation and pay another round. Holding the window open batches simultaneous arrivals into a single round; each new member extends it, capped by the group's rebalance timeout. `0` disables the wait. |