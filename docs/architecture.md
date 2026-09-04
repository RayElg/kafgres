# Architecture

kafgres embeds a Kafka broker in a PostgreSQL instance. One Postgres instance runs one
broker: a single background worker that speaks the Kafka wire protocol on port 9092,
with companion workers for the CDC drain, segment archiving, and standby replication.
Kafka clients see a one-node cluster. Every partition reports `leader=node`,
`replicas=[node]`, and `isr=[node]`.

Because the cluster is a single node, kafgres does not need most of what makes Kafka
complex: no controller, no quorum, no ISR tracking, no replica fetchers, no leader
election, no partition reassignment. `min.insync.replicas` is 1, and from a client's
point of view `acks=all` behaves like `acks=1`.

Durability and availability come from Postgres instead. The mapping is direct:

| Kafka concept | Postgres equivalent |
|---|---|
| `acks` | `synchronous_commit` |
| `min.insync.replicas` | `synchronous_standby_names` |

A produce with `acks=all` is durable once the transaction's commit has reached the
standbys `synchronous_commit` asks for. There is no separate replication mechanism to
configure or run.

## Primary-only operation

The broker workers run on the primary only. They are registered with
`BgWorkerStartTime::RecoveryFinished`, so a standby never starts them and a promotion
does. Redirecting clients at failover is the job of the usual HA tooling (VIP, HAProxy,
Patroni). Kafka clients retry metadata after a connection loss, so an endpoint flip plus
client-side `retries` covers it.

## Failover and leader epochs

Failing over to an asynchronous standby can lose committed offsets. A consumer holding
offset 5000 can reconnect to a primary whose log ends at 4800 and, without protection,
read divergent data.

Every partition therefore persists a `leader_epoch`. The broker raises it to the current
Postgres timeline id minus one at every start — which is the new timeline's value after a
promotion — stamps it into each batch's `partitionLeaderEpoch` field, and implements
`OffsetForLeaderEpoch` (API key 23). Clients use it to detect the divergence and truncate
back to the last offset they can trust.

The epoch value is the Postgres timeline id minus one. A freshly initialised cluster
sits on timeline 1, so its partitions carry epoch 0 from the broker's first start,
before any promotion; the first promotion moves the cluster to timeline 2, and the next
raise produces epoch 1, and so on. The protocol requires leader epochs to increase, not
to increase by exactly one, and the timeline ordering is correct across promotions in a
way that computing `old + 1` from replicated state may not be.

## Storage engines

The log has two storage engines, selected with `kafgres.storage_engine`. The setting is
read at startup and does not migrate existing data. The full setting reference is
[configuration.md](configuration.md).

| | `table` | `segment` (default) |
|---|---|---|
| Where the log lives | rows in `kafgres_log`, one row per record batch | segment files under `$PGDATA/kafgres` |
| Replication to a standby | WAL streaming, no extra configuration | `kafgres.replicate_from`, which pulls segments out of band |
| Transactional SQL produce | not supported | supported |
| Relative throughput | baseline | about 1.5x produce throughput, and much less load on co-resident OLTP |

The table engine writes every batch as rows. For a 1 MB batch that is roughly 525 TOAST
chunks with their index entries, WAL for all of it, a dead tuple per batch for autovacuum
to clean, and a row lock held per append. The segment engine replaces this with an append
to a file and an in-memory counter, so the broker no longer contends with user queries on
the WAL insert lock.

The engines do not read each other's logs. Switching the GUC leaves the old log in place,
invisible to the new engine.

## Transactional SQL produce: markers

`kafgres_produce()` appends the payload to the segment file and writes a small commit
marker row (about 40 bytes) inside the caller's transaction. The record becomes visible
to consumers when that transaction commits. If the transaction rolls back, the bytes stay
orphaned in the segment and `read_committed` consumers skip them.

That is the same arrangement Kafka uses for its own transactional produce: aborted
records remain physically in the log, and consumers skip them using the aborted
transaction information in the Fetch response. Postgres transaction abort maps onto Kafka
transaction abort directly.

The cost of transactionality is paid only when it is used: a non-transactional produce
never touches Postgres tables, while a transactional one adds a marker row to the
caller's transaction.

## Segment log replication

A standby runs a background worker that pulls segments from the primary over a TCP
connection (`kafgres.replicate_from`). Leadership follows Postgres: a worker serves requests only when the instance
is a primary. There is no election, no fencing, and no split-brain logic of its own. If
the HA stack cannot promote two primaries, kafgres cannot end up with two leaders.

The log stream and the WAL advance independently, so after a failover their tails can
disagree. The log is the source of truth for the high watermark; Postgres keeps only the
slowly-changing metadata (topic configs, group offsets, producer state, transaction
state). A committed consumer offset that ends up ahead of the log tail after a failover
surfaces as `OFFSET_OUT_OF_RANGE`, a condition Kafka clients already handle through
`auto.offset.reset`.

## Process topology

```
postmaster
+-- kafgres_broker     one worker serves every client connection on 9092; its tick
|                      also runs retention, compaction, and membership/quota expiry
+-- kafgres_cdc        drains the logical replication slot: renders mappings, produces
+-- kafgres_archiver   runs kafgres.segment_archive_command for rolled segments
+-- kafgres_follower   on a standby, applies the segment log streamed from the primary
```

The broker is one background worker, not a pool. It owns the listener and makes a
non-blocking read pass over every connection per tick, flushing completed responses
afterwards; a long-poll Fetch that cannot be satisfied yet is parked and completed when
the produce that fills it lands in this same process, or at its deadline. Group
coordination, ACLs and quota accounting run inline in the request path, each request in
its own short transaction. One broker per instance is the design — HA is Postgres HA —
and a second broker process is not available to load-balance onto.

## Metadata schema

```sql
kafgres_topics(topic_id, name, num_partitions, config jsonb, created_at)
kafgres_partitions(topic_id, partition, next_offset, log_start_offset,
                   leader_epoch, epoch_start_offset,
                   PRIMARY KEY (topic_id, partition))
kafgres_groups(group_id, generation, protocol_type, protocol, leader_member, state)
kafgres_group_members(group_id, member_id, client_id, host, metadata bytea,
                      assignment bytea, session_timeout_ms, last_heartbeat)
kafgres_offsets(group_id, topic_id, partition, committed_offset, leader_epoch,
                metadata, commit_ts)
kafgres_producers(producer_id, epoch, last_seq jsonb, last_ts)
kafgres_txns(txn_id, producer_id, epoch, state, partitions, started_at)
kafgres_acls(principal, resource_type, resource_name, pattern_type, operation, permission)
```

`next_offset` is the table engine's append position and is maintained only there: the
segment engine assigns offsets from shared memory, so the column reads 0 on the default
engine. Queries that read offsets from `kafgres_partitions` report an empty log against
a log that is not. The engine-independent interface is

```sql
SELECT * FROM kafgres_partition_offsets('order-events');
-- partition | log_start_offset | high_watermark | offset_span | leader_epoch
```

`high_watermark` is the log end offset; `offset_span` is `high_watermark −
log_start_offset`, the number of retained records. `high_watermark` is NULL when the
partition is not currently tracked in shared memory — in practice, a partition that has
never been written to — which renders as 0, so write `COALESCE(high_watermark, 0)` in
monitoring queries.

`kafgres_offsets` replaces `__consumer_offsets`. Clients read group offsets through the
`OffsetCommit` and `OffsetFetch` RPCs rather than the topic, so a plain table is
sufficient, and no synthetic `__consumer_offsets` topic appears in Metadata. See
[conformance.md](conformance.md) for what tools that read `__consumer_offsets` directly
will and will not see.

Schema changes are applied by versioned migration modules (`init010.rs`,
`init020.rs`, and so on), one per schema version, each running `CREATE TABLE IF NOT
EXISTS` statements and failing extension startup on error.

## Scope

Out of scope by design:

- **Kafka Connect, Kafka Streams, Schema Registry.** These are client-side systems that
  run outside the database.
- **Partition reassignment and multi-node clustering.** One broker per Postgres
  instance; HA is Postgres HA.
- **Delegation tokens.**

Quotas are implemented and enforced for `producer_byte_rate` and `consumer_byte_rate`;
responses report throttle times, but clients that ignore them are not muted.
`DescribeLogDirs` answers with the instance's single log directory. The client-visible
differences around both are catalogued in [conformance.md](conformance.md).

`cleanup.policy=compact` is accepted on both engines, and the retention sweep runs
compaction on both: each pass keeps the latest record per key, working through sealed
segments and resuming where the last pass stopped.