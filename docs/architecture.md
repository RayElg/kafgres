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

Durability and availability come from Postgres instead. On the table engine the mapping
is direct:

| Kafka concept | Postgres equivalent |
|---|---|
| `acks` | `synchronous_commit` |
| `min.insync.replicas` | `synchronous_standby_names` |

There, a produce with `acks=all` is durable once the transaction's commit has reached
the standbys `synchronous_commit` asks for, and there is no separate replication
mechanism to configure or run.

The segment engine keeps the log in files, outside the WAL. By default `acks=all`
returns once the records are fsynced (`kafgres.fsync_before_ack`). With that off it
returns once they are in the page cache, as Kafka does with its default flush settings:
they survive a crash of Postgres but not of the host. A standby's copy is pulled
out of band (see below) and is always asynchronous, since `synchronous_standby_names`
does not cover files outside the WAL; a failover can lose the newest records, and the
leader epochs described next make that visible to consumers rather than silent.

## Primary-only operation

The broker, CDC and archiver workers run on the primary only. They are registered with
`BgWorkerStartTime::RecoveryFinished`, so a standby never starts them and a promotion
does; the follower is the one worker a standby runs. Redirecting clients at failover is the job of the usual HA tooling (VIP, HAProxy,
Patroni). Kafka clients retry metadata after a connection loss, so an endpoint flip plus
client-side `retries` covers it.

## Failover and leader epochs

Failing over to an asynchronous standby can lose committed offsets. A consumer holding
offset 5000 can reconnect to a primary whose log ends at 4800 and, without protection,
read divergent data.

A restart without a failover can do the same on one node: records acknowledged before a
power cut may have been only in page cache, and new produces then reuse their offsets.

Every partition therefore persists a `leader_epoch`, and the broker takes a new one at every
start, as a Kafka leader does when it comes back. The new epoch begins at the recovered log
end, is stamped into each batch's `partitionLeaderEpoch` field, and is answered by
`OffsetForLeaderEpoch` (API key 23). A client that read past the recovered log end is told
to truncate back to it.

The epoch is `(timeline - 1) * 65536 + starts`: the Postgres timeline in the high 16 bits,
a count of broker starts in the low 16. A promotion moves the cluster to the next timeline,
so the new leader takes that timeline's first epoch (65536 for timeline 2), which a
diverged old primary restarting on its own timeline can never reach. pg_upgrade and
`pg_resetwal` start the WAL over at timeline 1; kafgres records the last timeline it saw
and, when the WAL's is lower, carries on from the one after it.

`kafgres_produce()`, `kafgres_cdc_drain()`, `kafgres_cdc_snapshot()`,
`kafgres_expire_transactions()` and the CDC worker append only once the broker has taken its
epoch after a start, so nothing lands under the previous epoch past the recovered log end.

## Storage engines

The log has two storage engines, selected with `kafgres.storage_engine`. The setting is
read at startup and does not migrate existing data. The full setting reference is
[configuration.md](configuration.md).

| | `table` | `segment` (default) |
|---|---|---|
| Where the log lives | rows in `kafgres_log`, one row per record batch | segment files under `kafgres.log_directory`, `$PGDATA/kafgres` by default |
| An acknowledged produce is | committed, per `synchronous_commit` | fsynced; in the page cache with `kafgres.fsync_before_ack` off |
| Replication to a standby | WAL streaming, no extra configuration | `kafgres.replicate_from`, which pulls segments out of band, asynchronously |
| Transactional SQL produce | not supported | supported |
| Relative throughput | baseline | about 2.4x produce throughput on the hardware measured with both durability settings relaxed; the strict defaults cost 3 to 25% (806 MB/s relaxed against 605 MB/s strict, 1 KiB records from 4 producers; table in [configuration.md](configuration.md)) |

The table engine writes every batch as rows. For a 1 MB batch that is roughly 525 TOAST
chunks with their index entries, WAL for all of it, a dead tuple per batch for autovacuum
to clean, and a row lock held per append. The segment engine replaces this with an append
to a file and an in-memory counter, so it writes far less WAL per batch.

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

The cost of transactionality is paid only when it is used: a plain produce never touches
Postgres tables (an idempotent one records its sequence window there), while a
transactional one adds a marker row to the caller's transaction.

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

The broker is one background worker, not a pool. It owns the listener, waits on every
socket at once with the tick as the longest wait, and makes a non-blocking read pass over
every connection each time it wakes, flushing completed responses afterwards; a long-poll Fetch that cannot be satisfied yet is parked and completed when
the produce that fills it lands in this same process, or at its deadline. Group
coordination, ACLs and quota accounting run inline in the request path, each request in
its own short transaction. One broker per instance is the design — HA is Postgres HA —
and a second broker process is not available to load-balance onto.

## Metadata schema

The tables an operator is most likely to query; others hold share-group, quota, CDC and
archive state.

```sql
kafgres_topics(topic_id, name, num_partitions, config jsonb, created_at, topic_uuid)
kafgres_partitions(topic_id, partition, next_offset, log_start_offset,
                   leader_epoch, epoch_start_offset,
                   PRIMARY KEY (topic_id, partition))
kafgres_leader_epochs(topic_id, partition, leader_epoch, start_offset, created_at)
kafgres_groups(group_id, generation, protocol_type, protocol_name, leader_member, state)
kafgres_group_members(group_id, member_id, client_id, client_host, metadata bytea,
                      assignment bytea, session_timeout_ms, last_heartbeat)
kafgres_offsets(group_id, topic_id, partition, committed_offset, committed_leader_epoch,
                metadata, commit_ts)
kafgres_producers(producer_id, producer_epoch, transactional_id, last_ts)
kafgres_producer_batches(producer_id, topic_id, partition, first_seq, last_seq, base_offset)
kafgres_txns(producer_id, producer_epoch, transactional_id, state, started_at)
kafgres_txn_partitions(producer_id, topic_id, partition, first_offset)
kafgres_txn_aborted(topic_id, partition, producer_id, first_offset, last_offset)
kafgres_markers(topic_id, partition, base_offset, last_offset, bytes)
kafgres_acls(acl_id, principal, host, operation, permission, resource_type,
             resource_name, pattern_type, created_at)
```

`kafgres_txn_aborted` is the index of aborted Kafka transactions that Fetch answers
`read_committed` consumers from; `kafgres_markers` holds one row per committed
`kafgres_produce()` batch.

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
never been written to, which stands for 0, so write `COALESCE(high_watermark, 0)` in
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

`cleanup.policy=compact` is accepted on both engines, and the retention sweep compacts on
both as Kafka's cleaner does. A cleaning round maps the latest offset of every key in the
part of the log no earlier round has cleaned, then rewrites the sealed part of the log
below that point, keeping each key's latest record at its original offset. The active
segment, anything newer than `min.compaction.lag.ms`, and anything at or above the last
stable offset are left alone. Records of aborted transactions are removed, and a tombstone
stays for `delete.retention.ms` after the round that cleaned it. A round starts once the
uncleaned part is at least half of what could be cleaned. The broker reads at most
`kafgres.compaction_pass_bytes` (32 MiB by default) across all partitions every
`kafgres.retention_check_interval_ms`, so a large log never stalls its connections.