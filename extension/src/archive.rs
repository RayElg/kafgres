//! Retention-aware segment archiving via an operator-supplied command, mirroring Postgres's

use std::collections::HashSet;

use pgrx::prelude::*;

const MAX_PER_TICK: usize = 64;

/// Serialises archivers: otherwise two can copy the same unarchived segment to one
pub const ARCHIVE_LOCK_KEY: i64 = 0x7047_4B41_0000_0006u64 as i64;

const MAX_STATUS_PARTITIONS: usize = 10_000;

/// Round-robin start, so a busy partition cannot starve the rest: a scan that always
static CURSOR: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Expand Postgres's `%p` (path) and `%f` (filename) as `archive_command` does. `%%` is
fn expand(command: &str, path: &str, filename: &str) -> String {
    let mut out = String::with_capacity(command.len() + path.len());
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('p') => out.push_str(path),
            Some('f') => out.push_str(filename),
            Some('%') => out.push('%'),
            // Anything else passes through: a stray `%` should look wrong in the log, not vanish.
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

pub fn archived_bases(
    topic: u32,
    partition: i32,
    from_base: i64,
    to_base: i64,
) -> Result<HashSet<i64>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT base_offset FROM kafgres_segment_archive
              WHERE topic_id = $1::oid AND partition = $2
                AND base_offset >= $3 AND base_offset <= $4",
            None,
            &[
                (topic as i32).into(),
                partition.into(),
                from_base.into(),
                to_base.into(),
            ],
        )?;
        let mut out = HashSet::new();
        for r in rows {
            if let Some(b) = r.get::<i64>(1)? {
                out.insert(b);
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .map_err(|e| e.to_string())
}

/// Whether archiving is on. Empty command means off, and retention is then ungated.
pub fn enabled() -> bool {
    !crate::segment_archive_command().is_empty()
}

/// Forget archived rows so the log's corrected bytes get re-shipped and retention may unlink
pub fn forget_segment(topic: u32, partition: i32, base_offset: i64) -> Result<(), String> {
    Spi::run_with_args(
        "DELETE FROM kafgres_segment_archive
          WHERE topic_id = $1::oid AND partition = $2 AND base_offset = $3",
        &[(topic as i32).into(), partition.into(), base_offset.into()],
    )
    .map_err(|e| format!("clearing archive row: {e}"))
}

pub fn forget_from(topic: u32, partition: i32, offset: i64) -> Result<(), String> {
    Spi::run_with_args(
        "DELETE FROM kafgres_segment_archive
          WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3",
        &[(topic as i32).into(), partition.into(), offset.into()],
    )
    .map_err(|e| e.to_string())
}

pub fn archive_once() -> Result<usize, String> {
    let command = crate::segment_archive_command();
    if command.is_empty() {
        return Ok(0);
    }
    if crate::storage_engine_guc() != "segment" {
        return Err(
            "kafgres.segment_archive_command is set but the storage engine is 'table'; \
             the table engine's log is in Postgres and is already covered by pg_basebackup"
                .to_string(),
        );
    }

    // Non-blocking: waiting would queue a backend behind a worker's object-store round trip.
    let got_lock: bool = Spi::get_one_with_args(
        "SELECT pg_try_advisory_xact_lock($1)",
        &[ARCHIVE_LOCK_KEY.into()],
    )
    .map_err(|e| e.to_string())?
    .unwrap_or(false);
    if !got_lock {
        return Ok(0);
    }

    let partitions = crate::meta::all_partitions().map_err(|e| e.to_string())?;
    if partitions.is_empty() {
        return Ok(0);
    }
    let mut shipped = 0usize;

    let start = CURSOR.load(std::sync::atomic::Ordering::Relaxed) % partitions.len();
    let mut visited = 0usize;
    for offset in 0..partitions.len() {
        let (topic, partition) = partitions[(start + offset) % partitions.len()];
        visited = offset + 1;
        if shipped >= MAX_PER_TICK {
            break;
        }
        let rolled = crate::storage::segment::rolled_segments(topic, partition)
            .map_err(|e| e.to_string())?;
        let (lo, hi) = match (rolled.first(), rolled.last()) {
            (Some(f), Some(l)) => (f.base, l.base),
            _ => continue,
        };
        let done = archived_bases(topic, partition, lo, hi)?;

        for seg in rolled {
            if shipped >= MAX_PER_TICK {
                break;
            }
            if done.contains(&seg.base) {
                continue;
            }
            let rendered = expand(&command, &seg.path, &seg.name);

            // `status`, not `output`: `output` reads the child's stdout/stderr to EOF with no
            let status = std::process::Command::new("sh")
                .arg("-c")
                .arg(&rendered)
                .status()
                .map_err(|e| format!("could not run the archive command: {e}"))?;

            if !status.success() {
                log!(
                    "kafgres: archive command failed for {} (exit {:?}); its own output is \
                     above this line",
                    seg.path,
                    status.code()
                );
                continue;
            }

            // Recorded only after the command succeeds, or retention unlinks a copy that never happened.
            Spi::run_with_args(
                "INSERT INTO kafgres_segment_archive (topic_id, partition, base_offset, bytes)
                 VALUES ($1::oid, $2, $3, $4)
                 ON CONFLICT (topic_id, partition, base_offset) DO NOTHING",
                &[
                    (topic as i32).into(),
                    partition.into(),
                    seg.base.into(),
                    (seg.bytes as i64).into(),
                ],
            )
            .map_err(|e| format!("recording archived segment {}: {e}", seg.path))?;
            shipped += 1;
        }
    }

    CURSOR.store(start + visited, std::sync::atomic::Ordering::Relaxed);
    Ok(shipped)
}

#[pg_extern]
fn kafgres_archive_segments() -> i64 {
    // Superuser-only: this is the one place in the extension that forks a shell.
    if !unsafe { pgrx::pg_sys::superuser() } {
        error!("kafgres: kafgres_archive_segments() is superuser-only; it runs kafgres.segment_archive_command");
    }
    match archive_once() {
        Ok(n) => n as i64,
        Err(e) => error!("kafgres: {e}"),
    }
}
const EVIDENCE_SAMPLE: i64 = 10;

const REWOUND_SQL: &str = "
    SELECT (SELECT string_agg(group_id, ', ')
              FROM (SELECT DISTINCT group_id FROM kafgres_offsets
                     WHERE topic_id = $1::oid AND partition = $2
                       AND committed_offset > $3
                     ORDER BY group_id LIMIT $4) g),
           (SELECT count(*)::bigint
              FROM kafgres_markers
             WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3),
           (SELECT count(*)::bigint
              FROM kafgres_segment_archive
             WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3),
           (SELECT string_agg(base_offset::text, ', ' ORDER BY base_offset)
              FROM (SELECT base_offset FROM kafgres_segment_archive
                     WHERE topic_id = $1::oid AND partition = $2
                       AND base_offset >= $3
                     ORDER BY base_offset LIMIT $4) b)";

/// Read-only report of what a restored node disagrees with itself about.
#[pg_extern]
fn kafgres_restore_check() -> TableIterator<
    'static,
    (
        name!(topic, Option<String>),
        name!(partition, Option<i32>),
        name!(finding, Option<String>),
        name!(detail, Option<String>),
    ),
> {
    // Superuser: the findings name absolute $PGDATA paths.
    if !unsafe { pgrx::pg_sys::superuser() } {
        error!("kafgres: kafgres_restore_check() is superuser-only; it reports $PGDATA paths");
    }

    let mut out: Vec<(Option<String>, Option<i32>, Option<String>, Option<String>)> = Vec::new();

    if crate::storage_engine_guc() != "segment" {
        return TableIterator::new(out);
    }

    let mut partitions = crate::meta::all_partitions().unwrap_or_default();
    let truncated = partitions.len() > MAX_STATUS_PARTITIONS;
    partitions.truncate(MAX_STATUS_PARTITIONS);
    let store = crate::storage::open();

    for (topic, partition) in partitions {
        // `SELECT (SELECT ...)`: always one row, so a vanished topic yields NULL instead of raising.
        let name: String = Spi::get_one_with_args::<String>(
            "SELECT (SELECT name FROM kafgres_topics WHERE topic_id = $1::oid)",
            &[(topic as i32).into()],
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| topic.to_string());
        // From the bytes, not the shared-memory counter: a restore swaps files under a live broker.
        let log_end = match crate::storage::segment::on_disk_log_end(topic, partition) {
            Ok(v) => v,
            Err(e) => {
                out.push((
                    Some(name.clone()),
                    Some(partition),
                    Some("log unreadable".to_string()),
                    Some(format!(
                        "{e:?}. Nothing else about this partition could be checked. Segments \
                         must be owned by the server's own user and mode 0600; a restore run \
                         as root leaves them owned by root and the server cannot open them."
                    )),
                ));
                continue;
            }
        };

        let orphaned: i64 = Spi::get_one_with_args(
            "SELECT (SELECT count(*) FROM kafgres_markers
                      WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3)",
            &[(topic as i32).into(), partition.into(), log_end.into()],
        )
        .ok()
        .flatten()
        .unwrap_or(0);
        if orphaned > 0 {
            out.push((
                Some(name.clone()),
                Some(partition),
                Some("markers past the log end".to_string()),
                Some(format!(
                    "{orphaned} committed transaction(s) at or above offset {log_end} have no \
                     records. The broker drops these at startup and says so; restoring a later \
                     segment set is the only way to keep them."
                )),
            ));
        }

        let bad_epoch: Option<String> = Spi::get_one_with_args(
            "SELECT (SELECT string_agg(leader_epoch || ' at ' || start_offset, ', ')
                       FROM kafgres_leader_epochs
                      WHERE topic_id = $1::oid AND partition = $2 AND start_offset > $3)",
            &[(topic as i32).into(), partition.into(), log_end.into()],
        )
        .ok()
        .flatten();
        if let Some(detail) = bad_epoch {
            out.push((
                Some(name.clone()),
                Some(partition),
                Some("epoch boundary past the log end".to_string()),
                Some(format!(
                    "epoch(s) {detail}, but the log ends at {log_end}. OffsetForLeaderEpoch \
                     will name a truncation point that does not exist."
                )),
            ));
        }

        // Its own finding, not part of the missing-segment check below: that check filters to
        let evidence: Result<Vec<String>, pgrx::spi::Error> = Spi::connect(|client| {
            let rows = client.select(
                REWOUND_SQL,
                None,
                &[
                    (topic as i32).into(),
                    partition.into(),
                    log_end.into(),
                    EVIDENCE_SAMPLE.into(),
                ],
            )?;
            let mut v = Vec::new();
            for r in rows {
                if let Some(groups) = r.get::<String>(1)? {
                    v.push(format!("consumer group(s) {groups} committed past it"));
                }
                let markers = r.get::<i64>(2)?.unwrap_or(0);
                if markers > 0 {
                    v.push(format!("{markers} commit marker(s) at or above it"));
                }
                let archived = r.get::<i64>(3)?.unwrap_or(0);
                if archived > 0 {
                    let sample = r.get::<String>(4)?.unwrap_or_default();
                    let more = if archived > EVIDENCE_SAMPLE {
                        format!(" and {} more", archived - EVIDENCE_SAMPLE)
                    } else {
                        String::new()
                    };
                    v.push(format!(
                        "the archive holds {archived} segment(s) at or above it ({sample}{more})"
                    ));
                }
            }
            Ok(v)
        });
        let evidence = match evidence {
            Ok(v) => v,
            Err(e) => {
                out.push((
                    Some(name.clone()),
                    Some(partition),
                    Some("rewind check failed".to_string()),
                    Some(format!(
                        "{e}. Whether this partition rewound is unknown — treat that as \
                         unanswered rather than as a clean result."
                    )),
                ));
                Vec::new()
            }
        };
        if !evidence.is_empty() {
            out.push((
                Some(name.clone()),
                Some(partition),
                Some("log rewound".to_string()),
                Some(format!(
                    "the log ends at {log_end}, but {} — this node is running on less log \
                     than it had, and new produces will re-issue the offsets above {log_end} \
                     carrying the same leader epoch as the records they replace. Consumers \
                     holding a position above {log_end} will read the new records as their \
                     own, with no error. The broker does not raise the epoch for you — doing \
                     that from local state would consume the number the next promotion \
                     needs. Either restore the remaining segments, or treat this node as a \
                     new leader over a divergent log. Note also that segments re-rolled at \
                     offsets the archive already holds will not be shipped over them, so \
                     point the archive somewhere new before serving.",
                    evidence.join("; ")
                )),
            ));
        }

        let start = store.log_start_offset(topic, partition).unwrap_or(0);
        // All segments on disk, not just sealed: after a restore the newest restored segment is active.
        let present: std::collections::HashSet<i64> =
            crate::storage::segment::segment_bases_on_disk(topic, partition)
                .unwrap_or_default()
                .into_iter()
                .collect();
        let recorded: Vec<i64> = Spi::connect(|client| {
            let rows = client.select(
                "SELECT base_offset FROM kafgres_segment_archive
                  WHERE topic_id = $1::oid AND partition = $2 AND base_offset >= $3
                  ORDER BY base_offset",
                None,
                &[(topic as i32).into(), partition.into(), start.into()],
            )?;
            let mut v = Vec::new();
            for r in rows {
                if let Some(b) = r.get::<i64>(1)? {
                    v.push(b);
                }
            }
            Ok::<_, pgrx::spi::Error>(v)
        })
        .unwrap_or_default();
        let missing: Vec<String> = recorded
            .into_iter()
            .filter(|b| !present.contains(b) && *b < log_end)
            .map(|b| b.to_string())
            .collect();
        if !missing.is_empty() {
            out.push((
                Some(name.clone()),
                Some(partition),
                Some("archived segment not restored".to_string()),
                Some(format!(
                    "the archive holds segment(s) at offset {} that this disk does not. \
                     Restore them before serving, or those offsets read as a gap.",
                    missing.join(", ")
                )),
            ));
        }
    }

    if truncated {
        out.push((
            None,
            None,
            Some("not all partitions checked".to_string()),
            Some(format!(
                "stopped after {MAX_STATUS_PARTITIONS} partitions. Anything below that is \
                 unexamined, so an empty result for the rest means nothing was looked at."
            )),
        ));
    }

    TableIterator::new(out)
}

/// `pending` is also what retention is blocked behind; a growing backlog means the command fails.
#[pg_extern]
fn kafgres_archive_status() -> TableIterator<
    'static,
    (
        name!(enabled, Option<bool>),
        name!(archived_segments, Option<i64>),
        name!(archived_bytes, Option<i64>),
        name!(pending_segments, Option<i64>),
        name!(oldest_pending, Option<String>),
    ),
> {
    if !unsafe { pgrx::pg_sys::superuser() } {
        error!("kafgres: kafgres_archive_status() is superuser-only; it reports $PGDATA paths");
    }
    let on = enabled();
    let (archived, bytes): (i64, i64) = Spi::connect(|client| {
        let rows = client.select(
            "SELECT count(*)::bigint, coalesce(sum(bytes), 0)::bigint
               FROM kafgres_segment_archive",
            Some(1),
            &[],
        )?;
        for r in rows {
            return Ok::<_, pgrx::spi::Error>((
                r.get::<i64>(1)?.unwrap_or(0),
                r.get::<i64>(2)?.unwrap_or(0),
            ));
        }
        Ok((0, 0))
    })
    .unwrap_or_else(|e| error!("kafgres: {e}"));

    // Counted from the filesystem: "pending" is exactly the on-disk segments with no row yet.
    let mut pending = 0i64;
    let mut oldest: Option<String> = None;
    if on && crate::storage_engine_guc() == "segment" {
        let partitions = crate::meta::all_partitions().unwrap_or_default();
        for (topic, partition) in partitions.into_iter().take(MAX_STATUS_PARTITIONS) {
            let rolled =
                crate::storage::segment::rolled_segments(topic, partition).unwrap_or_default();
            let (lo, hi) = match (rolled.first(), rolled.last()) {
                (Some(f), Some(l)) => (f.base, l.base),
                _ => continue,
            };
            let done = archived_bases(topic, partition, lo, hi).unwrap_or_default();
            for seg in rolled {
                if !done.contains(&seg.base) {
                    pending += 1;
                    if oldest.is_none() {
                        oldest = Some(seg.path.clone());
                    }
                }
            }
        }
    }

    TableIterator::once((Some(on), Some(archived), Some(bytes), Some(pending), oldest))
}
