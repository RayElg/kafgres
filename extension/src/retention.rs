//! Time- and size-based log retention, run from the background worker: retention is clock-driven and no request path will notice a segment aged out.

use pgrx::prelude::*;

/// Topics swept per call: a sweep freezes every connection for its duration, so the bound is broker-chosen, not topic count.
pub const SWEEP_TOPICS: i64 = 16;

/// Batches, not segments: a batch that drops anything writes and so takes an xid, and past
const MAX_WRITING_BATCHES: u64 = 63;

/// Lock bound, not a hard ceiling: each drop holds `ACCESS EXCLUSIVE` until the caller commits.
const MAX_SWEEP_DROPS: u64 = 512;

/// Worker and manual sweeps would lock-upgrade-deadlock on the same log leaf; the advisory lock makes the second decline.
const RETENTION_LOCK_KEY: i64 = 0x7047_4B41_0000_0005u64 as i64;

fn claim() -> Result<bool, spi::Error> {
    Ok(
        Spi::get_one_with_args::<bool>(
            "SELECT pg_try_advisory_xact_lock($1)",
            &[RETENTION_LOCK_KEY.into()],
        )?
        .unwrap_or(false),
    )
}

/// Sweeps the next `SWEEP_TOPICS` topics after `cursor`; the cursor keeps the bound fair rather than always hitting the same first topics.
pub fn sweep(cursor: u32) -> Result<Batch, spi::Error> {
    if !claim()? {
        // Not an error and not worth logging: the other caller is doing this work.
        return Ok(Batch::nothing(cursor));
    }

    let topics: Vec<u32> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT topic_id::int FROM kafgres_topics
              WHERE topic_id::int > $1 ORDER BY topic_id LIMIT $2",
            None,
            &[(cursor as i32).into(), SWEEP_TOPICS.into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(id) = row.get::<i32>(1)? {
                out.push(id as u32);
            }
        }
        Ok::<_, spi::Error>(out)
    })?;

    if topics.is_empty() {
        return Ok(Batch::nothing(0));
    }

    let mut store = crate::storage::open();
    let mut last = cursor;
    let mut reclaimed = 0;
    for topic_id in &topics {
        // A compacted topic is compacted *instead of* retained — running both would apply a window the operator did not ask for.
        let policy_kind = crate::config::cleanup_policy(*topic_id);
        if policy_kind.as_ref().map(|p| p.compacts).unwrap_or(false) {
            // Deliberately not counted in `reclaimed`: that counter is segments bounding the sweep; compaction counts records.
            if let Err(e) = compact_topic(&mut *store, *topic_id) {
                pgrx::log!("kafgres: compaction on topic {topic_id} failed: {e}");
            }
            // Compaction first: retention drops whole segments without looking inside them, discarding records compaction must keep.
            if policy_kind.map(|p| p.deletes).unwrap_or(false) {
                let policy = crate::config::retention_policy(*topic_id)?;
                match store.enforce_retention(*topic_id, &policy) {
                    Ok(n) => reclaimed += n,
                    Err(e) => {
                        pgrx::log!("kafgres: retention on compacted topic {topic_id} failed: {e}")
                    }
                }
            }
            last = *topic_id;
            continue;
        }
        let policy = crate::config::retention_policy(*topic_id)?;
        match store.enforce_retention(*topic_id, &policy) {
            Ok(n) => reclaimed += n,
            Err(e) => pgrx::log!("kafgres: retention on topic {topic_id} failed: {e}"),
        }
        last = *topic_id;
    }

    Ok(Batch { next: last, examined: topics.len() as u64, reclaimed })
}

fn compact_topic(store: &mut dyn crate::storage::LogStore, topic_id: u32) -> Result<u64, spi::Error> {
    let partitions: Vec<i32> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT partition FROM kafgres_partitions WHERE topic_id = $1::oid ORDER BY partition",
            None,
            &[(topic_id as i32).into()],
        )?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(p) = row.get::<i32>(1)? {
                out.push(p);
            }
        }
        Ok::<_, spi::Error>(out)
    })?;

    let mut removed = 0u64;
    for partition in partitions {
        match store.compact(topic_id, partition) {
            Ok(n) => removed += n,
            Err(crate::storage::StoreError::NotImplemented(_)) => {
                // No retention fallback: the operator asked for compaction, which keeps the latest record per key forever.
                pgrx::log!(
                    "kafgres: WARNING: topic {topic_id} has cleanup.policy=compact but \
                     kafgres.storage_engine cannot compact, so it is being neither compacted \
                     nor retained and will grow without bound. Either restart on the table \
                     engine or set cleanup.policy=delete."
                );
                return Ok(0);
            }
            Err(e) => pgrx::log!(
                "kafgres: compaction on topic {topic_id} partition {partition} failed: {e}"
            ),
        }
    }
    if removed > 0 {
        pgrx::log!("kafgres: compacted topic {topic_id}, {removed} superseded record(s) removed");
    }
    Ok(removed)
}

pub struct Batch {
    pub next: u32,
    pub examined: u64,
    pub reclaimed: u64,
}

impl Batch {
    fn nothing(next: u32) -> Self {
        Batch { next, examined: 0, reclaimed: 0 }
    }
}

/// Loops because a single pass only ever reaches the lowest `SWEEP_TOPICS` topics; bounded by drops and writing batches, not topics.
#[pg_extern]
fn kafgres_enforce_retention() -> i64 {
    let mut examined = 0i64;
    let mut reclaimed = 0u64;
    let mut writing_batches = 0u64;
    let mut cursor = 0u32;
    loop {
        let batch = match crate::dbtx::guarded(|| sweep(cursor).map_err(Into::into)) {
            Ok(b) => b,
            Err(e) => error!("kafgres: retention sweep failed: {e}"),
        };
        examined += batch.examined as i64;
        reclaimed += batch.reclaimed;
        if batch.reclaimed > 0 {
            writing_batches += 1;
        }
        if batch.examined == 0 {
            break;
        }
        if writing_batches >= MAX_WRITING_BATCHES || reclaimed >= MAX_SWEEP_DROPS {
            pgrx::log!(
                "kafgres: retention stopped after {reclaimed} segments across {examined} \
                 topics; call again to continue"
            );
            break;
        }
        // Should never fire — `sweep` returns a cursor ahead of the one given — but the failure mode is an infinite loop under `ACCESS EXCLUSIVE` locks.
        if batch.next <= cursor {
            break;
        }
        cursor = batch.next;
    }
    examined
}

#[pg_extern]
fn kafgres_expire_transactions() -> i64 {
    match crate::dbtx::guarded(crate::handlers::txn::expire_stale_transactions) {
        Ok(n) => n as i64,
        Err(e) => error!("kafgres: expiring stale transactions failed: {e}"),
    }
}
