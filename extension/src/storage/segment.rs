//! Segment-file log storage: the only module permitted to do file I/O.

use std::collections::HashMap;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use pgrx::prelude::*;

use kafgres_codec::records::{self, RecordBatch};

use super::pmeta;
use super::{
    FetchSlice, IsolationLevel, LogStore, RawBatch, RetentionPolicy, StoreError, StoreResult,
    TopicId, TxnContext,
};

const LOG_DIR: &str = "kafgres";

/// Kafka's own filename convention: base offset, zero-padded to 20 digits.
const OFFSET_DIGITS: usize = 20;

/// Whether any log exists on disk. Errors propagate: the negative answer is what lets the broker start.
pub fn log_presence() -> Result<Option<String>, String> {
    Ok(has_log_on_disk()?.then(|| format!("segment files under {}", data_path(&PathBuf::from(LOG_DIR)).display())))
}

fn has_log_on_disk() -> Result<bool, String> {
    fn any_segment(dir: &Path, depth: usize) -> Result<bool, String> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(format!("cannot read {}: {e}", dir.display())),
        };
        for entry in entries {
            let entry = entry.map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
            let path = entry.path();
            let meta = entry
                .metadata()
                .map_err(|e| format!("cannot stat {}: {e}", path.display()))?;
            if meta.is_dir() {
                if depth > 0 && any_segment(&path, depth - 1)? {
                    return Ok(true);
                }
            } else if meta.len() > 0 && path.extension().is_some_and(|x| x == "log") {
                return Ok(true);
            }
        }
        Ok(false)
    }
    any_segment(&data_path(&PathBuf::from(LOG_DIR)), 2)
}

fn partition_dir(topic: TopicId, partition: i32) -> PathBuf {
    PathBuf::from(LOG_DIR)
        .join(topic.to_string())
        .join(partition.to_string())
}

fn segment_path(topic: TopicId, partition: i32, base_offset: i64, ext: &str) -> PathBuf {
    partition_dir(topic, partition).join(format!("{base_offset:0OFFSET_DIGITS$}.{ext}"))
}

fn base_offset_of(name: &str, ext: &str) -> Option<i64> {
    let stem = name.strip_suffix(&format!(".{ext}"))?;
    if stem.len() != OFFSET_DIGITS || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

/// Bytes an active segment reaches before rolling, from `kafgres.segment_bytes`.
/// TODO: make this a per-topic config alongside `retention.bytes`.
fn segment_bytes() -> u64 {
    crate::segment_bytes()
}

/// A partition's append position, in **shared memory**: more than one process appends,
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Slot {
    topic: u32,
    partition: i32,
    /// The next offset to assign; dense because appends serialise on the shard lock.
    next_offset: i64,
    active_base: i64,
    active_bytes: u64,
    pending_count: i32,
    /// Lowest offset written by an uncommitted transaction, or `-1`; the LSO derives from it.
    pending_from: i64,
    active_since_ms: i64,
    /// Cached because the roll decision runs under the shard lock, where SPI is not allowed.
    segment_ms: i64,
    segment_bytes: i64,
    /// Bumped when a segment's byte layout changes: per-process seek hints name byte positions and must drop.
    layout_generation: u64,
    /// Where the next compaction pass starts; wraps so boundary-crossing supersessions are caught.
    compact_cursor: i64,
    /// Cached here because epoch and `next_offset` must be decided in one critical section, and
    leader_epoch: i32,
    /// Max timestamp over **every** batch: an index entry `(ts, pos)` claims nothing before `pos`
    max_timestamp_so_far: i64,
}

impl Default for Slot {
    fn default() -> Self {
        Slot {
            topic: 0,
            partition: 0,
            next_offset: 0,
            active_base: 0,
            active_bytes: 0,
            pending_count: 0,
            pending_from: -1,
            active_since_ms: 0,
            segment_bytes: 0,
            segment_ms: 604_800_000,
            layout_generation: 0,
            compact_cursor: 0,
            leader_epoch: -1,
            max_timestamp_so_far: i64::MIN,
        }
    }
}

impl Slot {
    /// The slot is shared memory and never rolls back; a `slot > seed` bump whose transaction has
    fn epoch_for_append(&mut self, seed: i32) -> StoreResult<i32> {
        if self.leader_epoch < 0 || seed > self.leader_epoch {
            self.leader_epoch = seed;
        }
        if self.leader_epoch > seed {
            return Err(StoreError::LeaderEpochUnsettled);
        }
        Ok(self.leader_epoch)
    }
}

unsafe impl pgrx::PGRXSharedMemory for Slot {}

/// Separate statics: `PgLwLock` couples the lock to the data it guards.
pub const LOCK_SHARDS: usize = 16;

pub const SLOTS_PER_SHARD: usize = 256;

pub const MAX_TRACKED_PARTITIONS: usize = LOCK_SHARDS * SLOTS_PER_SHARD;

macro_rules! shards {
    ($($name:ident => $lit:literal),* $(,)?) => {
        $(pub static $name: pgrx::PgLwLock<[Slot; SLOTS_PER_SHARD]> =
            unsafe { pgrx::PgLwLock::new($lit) };)*
        pub static SHARDS: [&pgrx::PgLwLock<[Slot; SLOTS_PER_SHARD]>; LOCK_SHARDS] =
            [$(&$name),*];
        pub fn init_shmem() {
            $(pgrx::pg_shmem_init!($name = [Slot::default(); SLOTS_PER_SHARD]);)*
        }
    };
}

shards! {
    COUNTERS_00 => c"kafgres_seg_00", COUNTERS_01 => c"kafgres_seg_01",
    COUNTERS_02 => c"kafgres_seg_02", COUNTERS_03 => c"kafgres_seg_03",
    COUNTERS_04 => c"kafgres_seg_04", COUNTERS_05 => c"kafgres_seg_05",
    COUNTERS_06 => c"kafgres_seg_06", COUNTERS_07 => c"kafgres_seg_07",
    COUNTERS_08 => c"kafgres_seg_08", COUNTERS_09 => c"kafgres_seg_09",
    COUNTERS_10 => c"kafgres_seg_10", COUNTERS_11 => c"kafgres_seg_11",
    COUNTERS_12 => c"kafgres_seg_12", COUNTERS_13 => c"kafgres_seg_13",
    COUNTERS_14 => c"kafgres_seg_14", COUNTERS_15 => c"kafgres_seg_15",
}

/// Hash of `(topic, partition)`, used for both shard choice and slot probing.
fn partition_hash(topic: TopicId, partition: i32) -> u64 {
    let mut h = (topic as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (partition as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    h ^= h >> 29;
    h
}

/// Capacity is `stripes * SLOTS_PER_SHARD`: placement and locking are one decision.
fn shard_of(topic: TopicId, partition: i32) -> usize {
    let stripes = crate::segment_lock_stripes().clamp(1, LOCK_SHARDS);
    (partition_hash(topic, partition) as usize) % stripes
}

/// Sparse seek hints, per process — deliberately not shared; a hint only moves the scan start.
type PartitionHints = (u64, HashMap<i64, Vec<(i64, u64)>>);
static HINTS: Mutex<Option<HashMap<(TopicId, i32), PartitionHints>>> = Mutex::new(None);

/// Bytes appended to the active segment between writeback hints; sized so the dirty backlog never approaches a segment.
const WRITEBACK_BYTES: u64 = 4 * 1024 * 1024;

/// The active segment's open descriptors, per partition, for this process.
/// `PathNameOpenFilePerm` hands back a virtual descriptor, so holding it costs no real fd
/// and it survives transaction end, which is what makes caching safe. Process-local.
struct ActiveSeg {
    generation: u64,
    base: i64,
    log: Vfd,
    /// Opened lazily: failing an append over an index file would reject a record the log can hold.
    index: Option<Vfd>,
    timeindex: Option<Vfd>,
    /// First byte of the log not yet handed to `FileWriteback`.
    writeback_from: u64,
}

static ACTIVE: Mutex<Option<HashMap<(TopicId, i32), ActiveSeg>>> = Mutex::new(None);

/// Drop this process's cached descriptors wherever the files underneath them can go.
/// Append one entry to a lazily-opened index file; failures are logged and swallowed, since
/// these files are hints and the log write this entry describes has already succeeded.
fn append_index_entry(slot: &mut Option<Vfd>, path: &Path, entry: &[u8], what: &str) {
    if slot.is_none() {
        match Vfd::open(path, true) {
            Ok(v) => *slot = Some(v),
            Err(e) => {
                log!("kafgres: could not open {what} file (harmless, costs a scan): {e}");
                return;
            }
        }
    }
    let Some(vfd) = slot.as_mut() else { return };
    let at = match vfd.size() {
        Ok(n) => n,
        Err(e) => {
            log!("kafgres: could not size {what} file (harmless, costs a scan): {e}");
            return;
        }
    };
    if let Err(e) = vfd.write_all_at(entry, at) {
        log!("kafgres: could not write {what} entry (harmless, costs a scan): {e}");
    }
}

fn evict_active(topic: TopicId, partition: i32) {
    if let Some(map) = ACTIVE.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
        map.remove(&(topic, partition));
    }
}

/// Run `f` against the open descriptors for `base`, opening them if stale. Lock order is
/// SHARD -> HINTS -> ACTIVE, and this is the only place ACTIVE is taken while another is held.
fn with_active<T>(
    topic: TopicId,
    partition: i32,
    base: i64,
    generation: u64,
    f: impl FnOnce(&mut ActiveSeg) -> StoreResult<T>,
) -> StoreResult<T> {
    let mut guard = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    let stale = match map.get(&(topic, partition)) {
        Some(a) => a.base != base || a.generation != generation,
        None => true,
    };
    if stale {
        map.remove(&(topic, partition));
        // Only here, not per append: walking the directory per write is a syscall per component.
        ensure_dir(&data_path(&partition_dir(topic, partition)))?;
        let log = Vfd::open(&data_path(&segment_path(topic, partition, base, "log")), true)?;
        // Whatever is on disk was written earlier; starting at zero would hand a whole
        // recovered segment to writeback on first append.
        let writeback_from = log.size()?;
        map.insert(
            (topic, partition),
            ActiveSeg {
                generation,
                base,
                log,
                index: None,
                timeindex: None,
                writeback_from,
            },
        );
    }
    let seg = map
        .get_mut(&(topic, partition))
        .expect("inserted directly above when absent");
    f(seg)
}

/// Recovers from disk if no process has touched the slot since postmaster start; holds the shard lock.
fn slot_for(slots: &mut [Slot; SLOTS_PER_SHARD], topic: TopicId, partition: i32)
    -> StoreResult<usize>
{
    // Linear probe from the hash start: a full scan per append is cache traffic.
    let start = (partition_hash(topic, partition) as usize) % SLOTS_PER_SHARD;

    let mut free: Option<usize> = None;
    for probe in 0..SLOTS_PER_SHARD {
        let i = (start + probe) % SLOTS_PER_SHARD;
        if slots[i].topic == topic && slots[i].partition == partition {
            return Ok(i);
        }
        if slots[i].topic == 0 {
            free = Some(i);
            break;
        }
    }

    let i = free.ok_or_else(|| {
        StoreError::Io(format!(
            "kafgres: lock shard full at {SLOTS_PER_SHARD} actively written partitions; \
             raise SLOTS_PER_SHARD (or kafgres.segment_lock_stripes, which narrows \
             capacity as well as concurrency) and restart"
        ))
    })?;

    let recovered = SegmentStore::recover(topic, partition)?;
    slots[i] = Slot {
        topic,
        partition,
        next_offset: recovered.next_offset,
        active_base: recovered.active_base,
        active_bytes: recovered.active_bytes,
        pending_count: 0,
        pending_from: -1,
        active_since_ms: 0,
        segment_ms: 604_800_000,
        segment_bytes: 0,
        // A fresh generation makes every process drop seek hints for the freed-and-recreated partition.
        layout_generation: 0,
        compact_cursor: 0,
        leader_epoch: -1,
        // From the recovery just scanned: `i64::MIN` would claim an empty-timestamp segment.
        max_timestamp_so_far: recovered.max_timestamp,
    };
    HINTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert((topic, partition), (0, recovered.index));
    Ok(i)
}

struct Recovered {
    next_offset: i64,
    active_base: i64,
    active_bytes: u64,
    index: HashMap<i64, Vec<(i64, u64)>>,
    max_timestamp: i64,
}

/// A file opened through Postgres's VFD layer — never raw `open()`: `max_files_per_process`
struct Vfd {
    file: pgrx::pg_sys::File,
    path: PathBuf,
}

impl Vfd {
    fn open(path: &Path, create: bool) -> StoreResult<Vfd> {
        let c = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| StoreError::Io(format!("path is not a C string: {}", path.display())))?;
        let mut flags = libc::O_RDWR;
        if create {
            flags |= libc::O_CREAT;
        }
        // 0600: the log is as sensitive as the heap, and lives beside it.
        let file = unsafe { pgrx::pg_sys::PathNameOpenFilePerm(c.as_ptr(), flags, 0o600) };
        if file < 0 {
            return Err(StoreError::Io(format!(
                "cannot open {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )));
        }
        Ok(Vfd { file, path: path.to_path_buf() })
    }

    fn size(&self) -> StoreResult<u64> {
        let n = unsafe { pgrx::pg_sys::FileSize(self.file) };
        if n < 0 {
            return Err(self.io_err("FileSize"));
        }
        Ok(n as u64)
    }

    fn write_all_at(&mut self, mut buf: &[u8], mut offset: u64) -> StoreResult<()> {
        while !buf.is_empty() {
            let n = unsafe {
                pgrx::pg_sys::FileWrite(
                    self.file,
                    buf.as_ptr() as *const core::ffi::c_void,
                    buf.len(),
                    offset as pgrx::pg_sys::off_t,
                    pgrx::pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_WRITE as u32,
                )
            };
            if n <= 0 {
                return Err(self.io_err("FileWrite"));
            }
            buf = &buf[n as usize..];
            offset += n as u64;
        }
        Ok(())
    }

    /// A short read means end of file — a normal condition for a tail scan.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> StoreResult<usize> {
        let mut total = 0;
        while total < buf.len() {
            let n = unsafe {
                pgrx::pg_sys::FileRead(
                    self.file,
                    buf[total..].as_mut_ptr() as *mut core::ffi::c_void,
                    buf.len() - total,
                    (offset + total as u64) as pgrx::pg_sys::off_t,
                    pgrx::pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_READ as u32,
                )
            };
            if n < 0 {
                return Err(self.io_err("FileRead"));
            }
            if n == 0 {
                break;
            }
            total += n as usize;
        }
        Ok(total)
    }

    fn truncate(&self, len: u64) -> StoreResult<()> {
        let rc = unsafe {
            pgrx::pg_sys::FileTruncate(
                self.file,
                len as pgrx::pg_sys::off_t,
                pgrx::pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_WRITE as u32,
            )
        };
        if rc < 0 {
            return Err(self.io_err("FileTruncate"));
        }
        Ok(())
    }

    /// PANIC on fsync failure, deliberately — do not soften into a retry: Linux drops the dirty
    fn sync(&self) {
        let rc = unsafe {
            pgrx::pg_sys::FileSync(
                self.file,
                pgrx::pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_SYNC as u32,
            )
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            pgrx::ereport!(
                pgrx::PgLogLevel::PANIC,
                pgrx::PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
                format!("kafgres: fsync failed on {}: {err}", self.path.display()),
                "Retrying fsync is not safe: the kernel may have already discarded the \
                 dirty pages, so a second call can report success having written nothing."
            );
        }
    }

    /// Hand a written range to the kernel for writeback without waiting for it.
    /// `FileWriteback` makes no durability promise; the `sync` above is still the only thing
    /// that does. It keeps the dirty-page backlog small so `sync` at roll finds little to write.
    fn writeback(&self, offset: u64, nbytes: u64) {
        if nbytes == 0 {
            return;
        }
        unsafe {
            pgrx::pg_sys::FileWriteback(
                self.file,
                offset as pgrx::pg_sys::off_t,
                nbytes as pgrx::pg_sys::off_t,
                pgrx::pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_FLUSH as u32,
            )
        };
    }

    fn io_err(&self, what: &str) -> StoreError {
        StoreError::Io(format!(
            "{what} on {}: {}",
            self.path.display(),
            std::io::Error::last_os_error()
        ))
    }
}

impl Drop for Vfd {
    fn drop(&mut self) {
        // Never fsyncs: durability is the caller's explicit `sync`, and a silent flush here would make the policy untestable.
        unsafe { pgrx::pg_sys::FileClose(self.file) };
    }
}

fn ensure_dir(path: &Path) -> StoreResult<()> {
    let mut acc = PathBuf::new();
    for part in path.components() {
        acc.push(part);
        let c = CString::new(acc.as_os_str().as_encoded_bytes())
            .map_err(|_| StoreError::Io(format!("path is not a C string: {}", acc.display())))?;
        let rc = unsafe { pgrx::pg_sys::MakePGDirectory(c.as_ptr()) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(StoreError::Io(format!(
                    "cannot create {}: {err}",
                    acc.display()
                )));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct SegmentStore {
    _private: (),
}

impl SegmentStore {
    pub fn new() -> Self {
        SegmentStore { _private: () }
    }
}

const INDEX_INTERVAL_BYTES: u64 = 4096;

const TIME_INDEX_ENTRY: usize = 12;

const MAX_SEGMENT_UNLINKS: usize = 32;

#[derive(Clone)]
struct SegInfo {
    base: i64,
    bytes: u64,
    /// Retention judges age on it; restores without `-Ft`/`cp -p` reset mtimes.
    mtime_ms: i64,
}

pub struct RolledSegment {
    pub base: i64,
    pub bytes: u64,
    pub path: String,
    /// The name the archive should store it under, for `%f` — **not the filename**: segments
    pub name: String,
}

/// The offset the log on disk actually ends at, ignoring the shared-memory counter — for
pub fn on_disk_log_end(topic: TopicId, partition: i32) -> StoreResult<i64> {
    let mut bases = SegmentStore::segment_bases(topic, partition)?;
    bases.sort_unstable();
    match bases.last() {
        None => Ok(0),
        Some(base) => {
            SegmentStore::read_segment(topic, partition, *base).map(|(_, _, next, _)| next)
        }
    }
}

/// Every segment base on disk, sealed and active alike — after a restore the newest segment
pub fn segment_bases_on_disk(topic: TopicId, partition: i32) -> StoreResult<Vec<i64>> {
    SegmentStore::segment_bases(topic, partition)
}

/// Sealed segments for a partition — archiving a file that is still growing would record a
pub fn rolled_segments(topic: TopicId, partition: i32) -> StoreResult<Vec<RolledSegment>> {
    let infos = SegmentStore::segment_infos(topic, partition)?;
    Ok(infos
        .iter()
        .take(infos.len().saturating_sub(1))
        .map(|i| RolledSegment {
            base: i.base,
            bytes: i.bytes,
            path: data_path(&segment_path(topic, partition, i.base, "log"))
                .to_string_lossy()
                .into_owned(),
            name: format!("{topic}-{partition}-{:0OFFSET_DIGITS$}.log", i.base),
        })
        .collect())
}

impl SegmentStore {
    fn segment_infos(topic: TopicId, partition: i32) -> StoreResult<Vec<SegInfo>> {
        let mut out = Vec::new();
        for base in Self::segment_bases(topic, partition)? {
            let path = data_path(&segment_path(topic, partition, base, "log"));
            let meta = match std::fs::metadata(&path) {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StoreError::Io(format!("stat {}: {e}", path.display()))),
            };
            let mtime_ms = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            out.push(SegInfo { base, bytes: meta.len(), mtime_ms });
        }
        Ok(out)
    }

    /// The offset below which segments may be reclaimed; the next segment's base is an exact
    fn retention_cutoff(infos: &[SegInfo], policy: &RetentionPolicy) -> i64 {
        // The active segment is never expendable, so only the boundaries between sealed
        if infos.len() < 2 {
            return 0;
        }
        let mut cutoff = 0i64;

        if let Some(ms) = policy.retention_ms {
            let horizon = now_millis().saturating_sub(ms);
            for i in 0..infos.len() - 1 {
                if infos[i].mtime_ms <= horizon {
                    cutoff = cutoff.max(infos[i + 1].base);
                } else {
                    break; // Ordered by offset, so the rest are newer.
                }
            }
        }

        if let Some(budget) = policy.retention_bytes {
            // The whole partition, active segment included: excluding the live end would let a topic
            let live: i64 = infos.iter().map(|s| s.bytes as i64).sum();
            let mut over = live - budget;
            for i in 0..infos.len() - 1 {
                if over <= 0 {
                    break;
                }
                over -= infos[i].bytes as i64;
                cutoff = cutoff.max(infos[i + 1].base);
            }
        }

        cutoff
    }
}

const MAX_COMPACT_SEGMENTS: usize = 16;
const MAX_COMPACT_BYTES: usize = 32 * 1024 * 1024;

impl SegmentStore {
    /// Verify nothing moved, then replace the segment. Runs under the shard lock; the re-stat
    fn swap_segment(
        topic: TopicId,
        partition: i32,
        info: &SegInfo,
        contents: &[u8],
    ) -> StoreResult<bool> {
        let final_path = data_path(&segment_path(topic, partition, info.base, "log"));
        let tmp_path = data_path(&segment_path(topic, partition, info.base, "log.compacting"));

        if !contents.is_empty() {
            let mut vfd = Vfd::open(&tmp_path, true)?;
            vfd.truncate(0)?;
            vfd.write_all_at(contents, 0)?;
            vfd.sync();
        }

        let swapped = Self::with_slot(topic, partition, |st, hints| {
            let current = match std::fs::metadata(&final_path) {
                Ok(m) => m,
                // Reclaimed while we were rebuilding it; that is the better outcome.
                Err(_) => return Ok(false),
            };
            let mtime = current
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if current.len() != info.bytes || mtime != info.mtime_ms {
                return Ok(false);
            }
            // Never the active segment: a roll since the window was chosen would make this one active,
            if st.active_base == info.base {
                return Ok(false);
            }

            if contents.is_empty() {
                let _ = std::fs::remove_file(&final_path);
            } else {
                let original = std::time::UNIX_EPOCH
                    + std::time::Duration::from_millis(info.mtime_ms.max(0) as u64);
                if let Ok(f) = std::fs::File::options().write(true).open(&tmp_path) {
                    let _ = f.set_times(std::fs::FileTimes::new().set_modified(original));
                }
                std::fs::rename(&tmp_path, &final_path).map_err(|e| {
                    StoreError::Io(format!("swapping {}: {e}", final_path.display()))
                })?;
                // fsync the directory after the rename, as Postgres's `durable_rename` does: the rename
                if let Some(dir) = final_path.parent() {
                    if let Ok(d) = std::fs::File::open(dir) {
                        let _ = d.sync_all();
                    }
                }
            }

            for ext in ["index", "timeindex"] {
                let _ = std::fs::remove_file(data_path(&segment_path(
                    topic, partition, info.base, ext,
                )));
            }
            hints.remove(&info.base);
            // And the cross-process half: every other backend drops its hints for this
            st.layout_generation = st.layout_generation.wrapping_add(1);
            Ok(true)
        })?;

        if !swapped {
            let _ = std::fs::remove_file(&tmp_path);
        } else {
            // The archived row vouches for pre-compaction bytes; drop it so the archiver re-ships.
            if let Err(e) = crate::archive::forget_segment(topic, partition, info.base) {
                pgrx::log!("kafgres: could not clear the archive row after compaction: {e}");
            }
        }
        Ok(swapped)
    }
}

static ROLL_BOUNDS: Mutex<Option<HashMap<TopicId, (i64, i64, i64)>>> = Mutex::new(None);
const ROLL_BOUNDS_TTL: i64 = 30_000;

fn roll_bounds_cached(topic: TopicId) -> (i64, i64) {
    let now = now_millis();
    let mut guard = ROLL_BOUNDS.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    if let Some((ms, bytes, read_at)) = map.get(&topic) {
        if now - *read_at < ROLL_BOUNDS_TTL {
            return (*ms, *bytes);
        }
    }
    let ms = crate::config::segment_ms(topic);
    let bytes = crate::config::segment_bytes(topic);
    map.insert(topic, (ms, bytes, now));
    (ms, bytes)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// One on-disk index entry: relative offset and file position, big-endian. Relative to the
const INDEX_ENTRY: usize = 8;

impl SegmentStore {
    /// Where to start scanning for the first batch at or after `timestamp`: the last indexed
    fn time_index_seek(
        topic: TopicId,
        partition: i32,
        base: i64,
        data_end: u64,
        timestamp: i64,
    ) -> u64 {
        let path = data_path(&segment_path(topic, partition, base, "timeindex"));
        let vfd = match Vfd::open(&path, false) {
            Ok(v) => v,
            Err(_) => return 0,
        };
        let size = vfd.size().unwrap_or(0);
        let mut start = 0u64;
        let mut buf = [0u8; TIME_INDEX_ENTRY];
        let mut at = 0u64;
        while at + TIME_INDEX_ENTRY as u64 <= size {
            if vfd.read_at(&mut buf, at).unwrap_or(0) != TIME_INDEX_ENTRY {
                break;
            }
            let ts = i64::from_be_bytes(buf[..8].try_into().expect("8 bytes"));
            let pos = u32::from_be_bytes(buf[8..].try_into().expect("4 bytes")) as u64;
            if pos >= data_end {
                break;
            }
            if ts >= timestamp {
                break;
            }
            start = pos;
            at += TIME_INDEX_ENTRY as u64;
        }
        start
    }

    fn truncate_time_index(topic: TopicId, partition: i32, base: i64, data_end: u64) {
        let path = data_path(&segment_path(topic, partition, base, "timeindex"));
        let Ok(mut vfd) = Vfd::open(&path, false) else {
            return;
        };
        let size = vfd.size().unwrap_or(0);
        let mut buf = [0u8; TIME_INDEX_ENTRY];
        let mut at = 0u64;
        while at + TIME_INDEX_ENTRY as u64 <= size {
            if vfd.read_at(&mut buf, at).unwrap_or(0) != TIME_INDEX_ENTRY {
                break;
            }
            let pos = u32::from_be_bytes(buf[8..].try_into().expect("4 bytes")) as u64;
            if pos >= data_end {
                break;
            }
            at += TIME_INDEX_ENTRY as u64;
        }
        if at < size {
            let _ = vfd.truncate(at);
        }
    }

    fn read_index(
        topic: TopicId,
        partition: i32,
        base: i64,
        data_end: u64,
    ) -> Vec<(i64, u64)> {
        let path = data_path(&segment_path(topic, partition, base, "index"));
        let vfd = match Vfd::open(&path, false) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };
        let size = vfd.size().unwrap_or(0);
        let mut out = Vec::new();
        let mut buf = [0u8; INDEX_ENTRY];
        let mut at = 0u64;
        while at + INDEX_ENTRY as u64 <= size {
            if vfd.read_at(&mut buf, at).unwrap_or(0) != INDEX_ENTRY {
                break;
            }
            let rel = u32::from_be_bytes(buf[..4].try_into().expect("4 bytes")) as i64;
            let pos = u32::from_be_bytes(buf[4..].try_into().expect("4 bytes")) as u64;
            if pos >= data_end {
                break;
            }
            out.push((base + rel, pos));
            at += INDEX_ENTRY as u64;
        }
        out
    }
}

impl SegmentStore {
    fn segment_bases(topic: TopicId, partition: i32) -> StoreResult<Vec<i64>> {
        let dir = partition_dir(topic, partition);
        let entries = match std::fs::read_dir(data_path(&dir)) {
            Ok(e) => e,
            // Never appended to: an empty log rather than an error.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(StoreError::Io(format!("read_dir {}: {e}", dir.display()))),
        };
        let mut bases: Vec<i64> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| base_offset_of(&e.file_name().to_string_lossy(), "log"))
            .collect();
        bases.sort_unstable();
        Ok(bases)
    }

    /// Scan a segment, validating each batch header, and return its index plus the first byte
    fn read_segment(
        topic: TopicId,
        partition: i32,
        base: i64,
    ) -> StoreResult<(Vec<(i64, u64)>, u64, i64, i64)> {
        let path = data_path(&segment_path(topic, partition, base, "log"));
        let vfd = Vfd::open(&path, false)?;
        let size = vfd.size()?;

        let mut max_timestamp = i64::MIN;
        let mut index = Vec::new();
        let mut pos = 0u64;
        let mut next_offset = base;
        let mut header = [0u8; records::RECORD_BATCH_OVERHEAD];

        while pos + header.len() as u64 <= size {
            if vfd.read_at(&mut header, pos)? != header.len() {
                break;
            }
            // `length` counts the bytes *after* itself, per the Kafka batch header.
            let length = i32::from_be_bytes(
                header[records::LENGTH_OFFSET..records::LENGTH_OFFSET + 4]
                    .try_into()
                    .expect("4 bytes"),
            );
            if length <= 0 {
                break;
            }
            let total = records::LENGTH_OFFSET as u64 + 4 + length as u64;
            if pos + total > size {
                // A torn tail: the batch was not fully written before the crash.
                break;
            }

            let mut whole = vec![0u8; total as usize];
            if vfd.read_at(&mut whole, pos)? != whole.len() {
                break;
            }
            // The CRC is the boundary check. An invalid one means everything from here
            let validated =
                match RecordBatch::validated(kafgres_codec::bytes::Bytes::from(whole.clone())) {
                    Ok(v) => v,
                    Err(_) => break,
                };
            let batch_base = i64::from_be_bytes(
                whole[records::BASE_OFFSET_OFFSET..records::BASE_OFFSET_OFFSET + 8]
                    .try_into()
                    .expect("8 bytes"),
            );
            let last_delta = i32::from_be_bytes(
                whole[records::LAST_OFFSET_DELTA_OFFSET..records::LAST_OFFSET_DELTA_OFFSET + 4]
                    .try_into()
                    .expect("4 bytes"),
            );
            drop(validated);

            let indexable = match index.last() {
                None => true,
                Some((_, last_pos)) => pos.saturating_sub(*last_pos) >= INDEX_INTERVAL_BYTES,
            };
            if indexable {
                index.push((batch_base, pos));
            }
            if let Some(ts) = max_timestamp_of(&whole) {
                max_timestamp = max_timestamp.max(ts);
            }
            next_offset = batch_base + last_delta as i64 + 1;
            pos += total;
        }

        Ok((index, pos, next_offset, max_timestamp))
    }

    /// `read_segment`, plus the repair that makes recovery idempotent. **Callers must hold the
    fn scan_segment(
        topic: TopicId,
        partition: i32,
        base: i64,
    ) -> StoreResult<(Vec<(i64, u64)>, u64, i64, i64)> {
        let (index, pos, next_offset, max_timestamp) = Self::read_segment(topic, partition, base)?;
        let path = data_path(&segment_path(topic, partition, base, "log"));
        let vfd = Vfd::open(&path, false)?;
        if pos < vfd.size()? {
            // Truncate rather than leave the partial batch; the next append would write past garbage.
            log!(
                "kafgres: truncating {} at {pos}: tail is not a complete batch",
                path.display()
            );
            vfd.truncate(pos)?;
        }
        Ok((index, pos, next_offset, max_timestamp))
    }

    fn recover(topic: TopicId, partition: i32) -> StoreResult<Recovered> {
        let bases = Self::segment_bases(topic, partition)?;
        let mut index = HashMap::new();
        let mut next_offset = 0i64;
        let mut active_base = 0i64;
        let mut active_bytes = 0u64;
        let mut max_timestamp = i64::MIN;

        // Only the active segment is scanned: rolled segments are immutable and indexed.
        for (i, base) in bases.iter().enumerate() {
            let last = i == bases.len() - 1;
            if !last {
                let path = data_path(&segment_path(topic, partition, *base, "log"));
                let size = Vfd::open(&path, false).and_then(|v| v.size()).unwrap_or(0);
                index.insert(*base, Self::read_index(topic, partition, *base, size));
                continue;
            }
            let (idx, end, next, max_ts) = Self::scan_segment(topic, partition, *base)?;
            index.insert(*base, idx);
            active_base = *base;
            active_bytes = end;
            next_offset = next;
            max_timestamp = max_ts;

            // Drop `.timeindex` entries the recovered log does not reach: a stale position then sits
            Self::truncate_time_index(topic, partition, *base, end);
        }
        Ok(Recovered { next_offset, active_base, active_bytes, index, max_timestamp })
    }

    /// Place already-stamped bytes: roll if needed, write, index. Does not stamp or advance the
    fn write_raw(
        topic: TopicId,
        partition: i32,
        st: &mut Slot,
        hints: &mut HashMap<i64, Vec<(i64, u64)>>,
        bytes: &[u8],
        base_offset: i64,
    ) -> StoreResult<()> {
        let generation = st.layout_generation;
        let aged_out = st.active_since_ms > 0
            && now_millis() - st.active_since_ms >= st.segment_ms
            && st.active_bytes > 0;
        let roll_at = if st.segment_bytes > 0 {
            st.segment_bytes as u64
        } else {
            segment_bytes()
        };
        if st.active_bytes > 0 && (aged_out || st.active_bytes + bytes.len() as u64 > roll_at) {
            // Flush what the pacing below has not already started, then fsync the segment we
            // are leaving; it now finds most of the file already on the device.
            let closing_base = st.active_base;
            let closing_bytes = st.active_bytes;
            with_active(topic, partition, closing_base, generation, |a| {
                a.log
                    .writeback(a.writeback_from, closing_bytes.saturating_sub(a.writeback_from));
                a.log.sync();
                Ok(())
            })?;
            evict_active(topic, partition);
            st.active_base = base_offset;
            st.active_bytes = 0;
            // A new segment is empty: carrying the previous maximum forward would poison its
            // first time-index entry.
            st.max_timestamp_so_far = i64::MIN;
            st.active_since_ms = now_millis();
            hints.insert(base_offset, Vec::new());
        }
        if st.active_since_ms == 0 {
            st.active_since_ms = now_millis();
        }

        let pos = st.active_bytes;
        let active_base = st.active_base;
        let max_timestamp_so_far = st.max_timestamp_so_far;

        let entries = hints.entry(active_base).or_default();
        let indexable = match entries.last() {
            None => true,
            Some((_, last_pos)) => pos.saturating_sub(*last_pos) >= INDEX_INTERVAL_BYTES,
        };
        if indexable {
            entries.push((base_offset, pos));
        }

        with_active(topic, partition, active_base, generation, |a| {
            a.log.write_all_at(bytes, pos)?;
            let end = pos + bytes.len() as u64;
            if end.saturating_sub(a.writeback_from) >= WRITEBACK_BYTES {
                a.log.writeback(a.writeback_from, end - a.writeback_from);
                a.writeback_from = end;
            }

            if indexable {
                // Both index writes stay non-fatal: a missing entry costs a scan. The write
                // position is re-read from the file because the worker and a user backend
                // appending in different processes would otherwise number entries independently.
                let mut entry = [0u8; INDEX_ENTRY];
                entry[..4].copy_from_slice(&((base_offset - active_base) as u32).to_be_bytes());
                entry[4..].copy_from_slice(&(pos as u32).to_be_bytes());
                append_index_entry(
                    &mut a.index,
                    &data_path(&segment_path(topic, partition, active_base, "index")),
                    &entry,
                    "index",
                );

                // Pairs the max over everything *before* this batch with this batch's position,
                // so a time lookup lands at or before the first record that can match.
                let mut tentry = [0u8; TIME_INDEX_ENTRY];
                tentry[..8].copy_from_slice(&max_timestamp_so_far.to_be_bytes());
                tentry[8..].copy_from_slice(&(pos as u32).to_be_bytes());
                append_index_entry(
                    &mut a.timeindex,
                    &data_path(&segment_path(topic, partition, active_base, "timeindex")),
                    &tentry,
                    "time index",
                );
            }
            Ok(())
        })?;

        // Every batch, not only indexed ones — the entries above claim to dominate them. From the
        // batch header, so a compressed batch costs nothing to read here.
        if let Some(ts) = max_timestamp_of(bytes) {
            st.max_timestamp_so_far = st.max_timestamp_so_far.max(ts);
        }
        st.active_bytes += bytes.len() as u64;
        Ok(())
    }

    fn write_batch(
        topic: TopicId,
        partition: i32,
        st: &mut Slot,
        hints: &mut HashMap<i64, Vec<(i64, u64)>>,
        batch: RawBatch,
        epoch: i32,
        base_offset: i64,
    ) -> StoreResult<()> {
        let last_offset = base_offset + batch.last_offset_delta as i64;

        // Validated and stamped, never re-encoded: the CRC fields stay outside the stamp.
        let stamped = RecordBatch::validated(kafgres_codec::bytes::Bytes::from(batch.bytes))
            .map_err(|_| StoreError::CorruptBatch)?
            .stamp(base_offset, epoch);
        let bytes = stamped.into_bytes();

        Self::write_raw(topic, partition, st, hints, &bytes, base_offset)?;
        st.next_offset = last_offset + 1;
        Ok(())
    }

    fn reclaim(&mut self, topic: TopicId, partition: i32, offset: i64) -> StoreResult<u64> {
        let start = pmeta::log_start_offset(topic, partition)?;
        let target = offset.max(start);

        let infos = Self::segment_infos(topic, partition)?;

        // The archive gates the unlink, and only the unlink: a failing command stops reclaiming
        let archived = if crate::archive::enabled() {
            let lowest = infos.first().map(|i| i.base).unwrap_or(0);
            Some(
                crate::archive::archived_bases(topic, partition, lowest, target)
                    .map_err(StoreError::Io)?,
            )
        } else {
            None
        };

        let mut unlinked = 0u64;
        for i in 0..infos.len().saturating_sub(1) {
            if unlinked as usize >= MAX_SEGMENT_UNLINKS {
                break;
            }
            if infos[i + 1].base > target {
                break; // Ordered, so nothing later qualifies either.
            }
            if let Some(done) = &archived {
                if !done.contains(&infos[i].base) {
                    break;
                }
            }
            let path = data_path(&segment_path(topic, partition, infos[i].base, "log"));
            match std::fs::remove_file(&path) {
                Ok(()) => unlinked += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StoreError::Io(format!("unlink {}: {e}", path.display()))),
            }
            for ext in ["index", "timeindex"] {
                let _ = std::fs::remove_file(data_path(&segment_path(
                    topic,
                    partition,
                    infos[i].base,
                    ext,
                )));
            }
            if let Some(map) = HINTS.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                if let Some((_, hint)) = map.get_mut(&(topic, partition)) {
                    hint.remove(&infos[i].base);
                }
            }
            evict_active(topic, partition);
        }

        pmeta::advance_log_start(topic, partition, target)?;
        Ok(unlinked)
    }

    /// Run `f` against the partition's shared append position and this process's seek hints,
    fn with_slot<T>(
        topic: TopicId,
        partition: i32,
        f: impl FnOnce(&mut Slot, &mut HashMap<i64, Vec<(i64, u64)>>) -> StoreResult<T>,
    ) -> StoreResult<T> {
        let mut slots = SHARDS[shard_of(topic, partition)].exclusive();
        let i = slot_for(&mut slots, topic, partition)?;
        let generation = slots[i].layout_generation;
        let mut hints = HINTS.lock().unwrap_or_else(|e| e.into_inner());
        let map = hints.get_or_insert_with(HashMap::new);
        let entry = map
            .entry((topic, partition))
            .or_insert_with(|| (generation, HashMap::new()));
        // The cross-process check, where hints are handed out: a rewrite in any process bumps the
        if entry.0 != generation {
            entry.1.clear();
            entry.0 = generation;
        }
        f(&mut slots[i], &mut entry.1)
    }

}

fn max_timestamp_of(bytes: &[u8]) -> Option<i64> {
    if bytes.len() < records::RECORD_BATCH_OVERHEAD {
        return None;
    }
    Some(i64::from_be_bytes(
        bytes[records::MAX_TIMESTAMP_OFFSET..records::MAX_TIMESTAMP_OFFSET + 8]
            .try_into()
            .ok()?,
    ))
}

fn data_path(relative: &Path) -> PathBuf {
    PathBuf::from(data_directory()).join(relative)
}

fn data_directory() -> String {
    unsafe {
        std::ffi::CStr::from_ptr(pgrx::pg_sys::DataDir)
            .to_string_lossy()
            .into_owned()
    }
}

const PHASE_7: &str = "SegmentStore (phase 7)";

impl LogStore for SegmentStore {
    fn append(
        &mut self,
        topic: TopicId,
        partition: i32,
        batch: RawBatch,
        _txn: Option<&TxnContext>,
    ) -> StoreResult<i64> {
        // The epoch is taken from the slot under the same lock that hands out the offset, so the
        let seed = self.leader_epoch(topic, partition)?;
        // Read before the lock (SPI is not allowed under it) and cached: a roll threshold need not
        let (seg_ms, seg_bytes) = roll_bounds_cached(topic);
        Self::with_slot(topic, partition, |st, hints| {
            st.segment_ms = seg_ms;
            st.segment_bytes = seg_bytes;
            let epoch = st.epoch_for_append(seed)?;
            let base_offset = st.next_offset;
            Self::write_batch(topic, partition, st, hints, batch, epoch, base_offset)?;
            Ok(base_offset)
        })
    }

    /// Whole batches only, byte-capped before allocation, and always at least one so a consumer
    fn read(
        &self,
        topic: TopicId,
        partition: i32,
        offset: i64,
        max_bytes: usize,
        isolation: IsolationLevel,
    ) -> StoreResult<FetchSlice> {
        let log_start = self.log_start_offset(topic, partition)?;
        let high_watermark = self.high_watermark(topic, partition)?;
        let lso = self.last_stable_offset_impl(topic, partition)?;
        // A `read_committed` consumer must not pass the LSO: past it lies a transaction
        let ceiling = match isolation {
            IsolationLevel::ReadCommitted => lso,
            IsolationLevel::ReadUncommitted => high_watermark,
        };

        if offset < log_start || offset > high_watermark {
            return Err(StoreError::OffsetOutOfRange);
        }

        let mut out: Vec<u8> = Vec::new();
        let mut next = offset;
        let mut aborted: Vec<super::AbortedTxn> = Vec::new();

        if offset < ceiling {
            // Enumerate from disk, not the hint map: hints are per-process, so a partition appended by
            // TODO: cache the per-Fetch `read_dir`; the list changes only on roll/reclaim.
            let bases_on_disk = Self::segment_bases(topic, partition)?;
            let committed = match isolation {
                IsolationLevel::ReadCommitted => {
                    Some(pmeta::committed_markers(topic, partition, offset, ceiling)?)
                }
                IsolationLevel::ReadUncommitted => None,
            };
            // Producer ids Kafka handed out, so a transactional batch from one of them is
            let kafka_producers = if committed.is_some() {
                pmeta::known_producer_ids()?
            } else {
                std::collections::HashSet::new()
            };
            Self::with_slot(topic, partition, |st, hints| {
                let mut bases = bases_on_disk;
                bases.sort_unstable();
                let mut reclaimed: Vec<i64> = Vec::new();

                for (bi, base) in bases.iter().enumerate() {
                    if let Some(next_base) = bases.get(bi + 1) {
                        if *next_base <= offset {
                            continue;
                        }
                    }

                    let path = data_path(&segment_path(topic, partition, *base, "log"));
                    let vfd = match Vfd::open(&path, false) {
                        Ok(v) => v,
                        // Reclaimed underneath us: retention runs from user backends too, and a missing file is
                        Err(_) if bases.get(bi + 1).is_some_and(|nb| *nb <= log_start) => {
                            reclaimed.push(*base);
                            continue;
                        }
                        Err(e) => return Err(e),
                    };

                    let data_end = if *base == st.active_base {
                        st.active_bytes
                    } else {
                        vfd.size()?
                    };

                    let mut pos = hints
                        .get(base)
                        .and_then(|entries| {
                            entries.iter().rev().find(|(b, _)| *b <= next).map(|(_, p)| *p)
                        })
                        .unwrap_or(0);

                    let mut header = [0u8; records::RECORD_BATCH_OVERHEAD];
                    while pos + header.len() as u64 <= data_end {
                        if vfd.read_at(&mut header, pos)? != header.len() {
                            break;
                        }
                        let length = i32::from_be_bytes(
                            header[records::LENGTH_OFFSET..records::LENGTH_OFFSET + 4]
                                .try_into()
                                .expect("4 bytes"),
                        );
                        if length <= 0 {
                            break;
                        }
                        let total = records::LENGTH_OFFSET as u64 + 4 + length as u64;
                        if pos + total > data_end {
                            break;
                        }
                        let batch_base = i64::from_be_bytes(
                            header[records::BASE_OFFSET_OFFSET..records::BASE_OFFSET_OFFSET + 8]
                                .try_into()
                                .expect("8 bytes"),
                        );
                        let last_delta = i32::from_be_bytes(
                            header[records::LAST_OFFSET_DELTA_OFFSET
                                ..records::LAST_OFFSET_DELTA_OFFSET + 4]
                                .try_into()
                                .expect("4 bytes"),
                        );
                        let batch_last = batch_base + last_delta as i64;

                        // Wholly below the request; the batch containing `offset` is kept whole (the consumer
                        if batch_last < offset {
                            pos += total;
                            continue;
                        }
                        // Stop at the ceiling rather than truncating the batch: batches
                        if batch_base >= ceiling {
                            break;
                        }

                        // A marker-backed batch with no committed marker is an orphan: a rolled-back
                        if let Some(committed) = &committed {
                            let attributes = i16::from_be_bytes(
                                header[records::ATTRIBUTES_OFFSET
                                    ..records::ATTRIBUTES_OFFSET + 2]
                                    .try_into()
                                    .expect("2 bytes"),
                            );
                            let producer_id = i64::from_be_bytes(
                                header[records::PRODUCER_ID_OFFSET
                                    ..records::PRODUCER_ID_OFFSET + 8]
                                    .try_into()
                                    .expect("8 bytes"),
                            );
                            let is_txn = attributes & records::TRANSACTIONAL_FLAG != 0;
                            let is_control =
                                attributes & records::CONTROL_BATCH_FLAG != 0;

                            if is_txn
                                && !is_control
                                && !kafka_producers.contains(&producer_id)
                                && !committed.contains(&batch_base)
                            {
                                aborted.push(super::AbortedTxn {
                                    producer_id,
                                    first_offset: batch_base,
                                });
                            }
                        }

                        if !out.is_empty() && out.len() + total as usize > max_bytes {
                            for b in reclaimed {
                                hints.remove(&b);
                            }
                            return Ok(());
                        }

                        let mut buf = vec![0u8; total as usize];
                        if vfd.read_at(&mut buf, pos)? != buf.len() {
                            break;
                        }
                        out.extend_from_slice(&buf);
                        next = batch_last + 1;
                        pos += total;

                        if out.len() >= max_bytes {
                            for b in reclaimed {
                                hints.remove(&b);
                            }
                            return Ok(());
                        }
                    }
                }
                for b in reclaimed {
                    hints.remove(&b);
                }
                Ok(())
            })?;

            // Kafka's aborts, scoped to what this response returned: bounding by the ceiling rather
            if matches!(isolation, IsolationLevel::ReadCommitted) {
                aborted.extend(pmeta::aborted_txns(topic, partition, offset, next.max(offset + 1))?);
            }
        }

        Ok(FetchSlice {
            bytes: out,
            next_offset: next,
            high_watermark,
            log_start_offset: log_start,
            last_stable_offset: lso,
            aborted,
        })
    }

    fn offset_for_timestamp(
        &self,
        topic: TopicId,
        partition: i32,
        timestamp: i64,
    ) -> StoreResult<Option<i64>> {
        // -1 latest, -2 earliest: the sentinels ListOffsets uses.
        match timestamp {
            -1 => return self.high_watermark(topic, partition).map(Some),
            -2 => return self.log_start_offset(topic, partition).map(Some),
            _ => {}
        }

        // The earliest offset whose timestamp is at or after `timestamp` — what `offsetsForTimes`
        let bases_on_disk = Self::segment_bases(topic, partition)?;
        Self::with_slot(topic, partition, |st, _hints| {
            let mut bases = bases_on_disk;
            bases.sort_unstable();
            for base in bases {
                let path = data_path(&segment_path(topic, partition, base, "log"));
                let vfd = match Vfd::open(&path, false) {
                    Ok(v) => v,
                    Err(_) => continue, // reclaimed while we looked
                };
                let data_end = if base == st.active_base {
                    st.active_bytes
                } else {
                    vfd.size()?
                };
                // Start where the time index says the answer cannot be behind us.
                let mut pos =
                    Self::time_index_seek(topic, partition, base, data_end, timestamp);
                let mut header = [0u8; records::RECORD_BATCH_OVERHEAD];
                while pos + header.len() as u64 <= data_end {
                    if vfd.read_at(&mut header, pos)? != header.len() {
                        break;
                    }
                    let length = i32::from_be_bytes(
                        header[records::LENGTH_OFFSET..records::LENGTH_OFFSET + 4]
                            .try_into()
                            .expect("4 bytes"),
                    );
                    if length <= 0 {
                        break;
                    }
                    let max_ts = i64::from_be_bytes(
                        header[records::MAX_TIMESTAMP_OFFSET..records::MAX_TIMESTAMP_OFFSET + 8]
                            .try_into()
                            .expect("8 bytes"),
                    );
                    if max_ts >= timestamp {
                        let batch_base = i64::from_be_bytes(
                            header[records::BASE_OFFSET_OFFSET..records::BASE_OFFSET_OFFSET + 8]
                                .try_into()
                                .expect("8 bytes"),
                        );
                        return Ok(Some(batch_base));
                    }
                    pos += records::LENGTH_OFFSET as u64 + 4 + length as u64;
                }
            }
            // Nothing at or after it. `None` is "no offset found", which is what a client
            Ok(None)
        })
    }

    fn max_timestamp_offset(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<(i64, i64)>> {
        // The greatest timestamp can sit in any batch, so walk headers and read only the winner.
        let bases_on_disk = Self::segment_bases(topic, partition)?;
        Self::with_slot(topic, partition, |st, _hints| {
            let mut bases = bases_on_disk;
            bases.sort_unstable();

            // Pass one: find the winning batch from headers alone, without reading bodies.
            struct Winner {
                max_ts: i64,
                seg_base: i64,
                pos: u64,
                len: u64,
            }
            let mut best: Option<Winner> = None;

            for base in bases {
                let path = data_path(&segment_path(topic, partition, base, "log"));
                let vfd = match Vfd::open(&path, false) {
                    Ok(v) => v,
                    Err(_) => continue, // reclaimed while we looked
                };
                let data_end = if base == st.active_base {
                    st.active_bytes
                } else {
                    vfd.size()?
                };
                let mut header = [0u8; records::RECORD_BATCH_OVERHEAD];
                let mut pos = 0u64;
                while pos + header.len() as u64 <= data_end {
                    if vfd.read_at(&mut header, pos)? != header.len() {
                        break;
                    }
                    let length = i32::from_be_bytes(
                        header[records::LENGTH_OFFSET..records::LENGTH_OFFSET + 4]
                            .try_into()
                            .expect("4 bytes"),
                    );
                    if length <= 0 {
                        break;
                    }
                    let total = records::LENGTH_OFFSET as u64 + 4 + length as u64;
                    // `total` comes from the file and pass two allocates exactly this size; a
                    // torn page yields a garbled length, so requiring the batch to fit the
                    // committed region bounds the allocation.
                    if pos + total > data_end {
                        break;
                    }
                    let max_ts = i64::from_be_bytes(
                        header[records::MAX_TIMESTAMP_OFFSET..records::MAX_TIMESTAMP_OFFSET + 8]
                            .try_into()
                            .expect("8 bytes"),
                    );
                    // Strictly greater: a tie keeps the earlier batch, matching Kafka.
                    let better = match &best {
                        Some(w) => max_ts > w.max_ts,
                        None => true,
                    };
                    if better {
                        best = Some(Winner {
                            max_ts,
                            seg_base: base,
                            pos,
                            len: total,
                        });
                    }
                    pos += total;
                }
            }

            // Pass two: read just that batch and find which record carried the timestamp.
            let Some(w) = best else { return Ok(None) };
            let path = data_path(&segment_path(topic, partition, w.seg_base, "log"));

            // Pass one proved the partition is not empty, so `Ok(None)` here would report an
            // empty log; only an unlinked file is benign, the winner having been reclaimed.
            let vfd = match Vfd::open(&path, false) {
                Ok(v) => v,
                Err(_) if !path.exists() => return Ok(None),
                Err(e) => return Err(e),
            };
            let mut body = vec![0u8; w.len as usize];
            if vfd.read_at(&mut body, w.pos)? != body.len() {
                return Err(StoreError::Io(format!(
                    "short read of the max-timestamp batch at {}+{} in {}",
                    w.pos,
                    w.len,
                    path.display()
                )));
            }
            Ok(super::offset_of_max_timestamp(kafgres_codec::bytes::Bytes::from(body)))
        })
    }

    fn high_watermark(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        Self::with_slot(topic, partition, |st, _| Ok(st.next_offset))
    }

    fn last_stable_offset_if_tracked(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<i64>> {
        let slots = SHARDS[shard_of(topic, partition)].share();
        let start = (partition_hash(topic, partition) as usize) % SLOTS_PER_SHARD;
        for probe in 0..SLOTS_PER_SHARD {
            let i = (start + probe) % SLOTS_PER_SHARD;
            if slots[i].topic == topic && slots[i].partition == partition {
                // `pending_from` is the first offset an uncommitted transaction wrote, or
                let st = &slots[i];
                return Ok(Some(if st.pending_from >= 0 {
                    st.pending_from
                } else {
                    st.next_offset
                }));
            }
            if slots[i].topic == 0 {
                break;
            }
        }
        Ok(None)
    }

    fn high_watermark_if_tracked(
        &self,
        topic: TopicId,
        partition: i32,
    ) -> StoreResult<Option<i64>> {
        let slots = SHARDS[shard_of(topic, partition)].share();
        let start = (partition_hash(topic, partition) as usize) % SLOTS_PER_SHARD;
        for probe in 0..SLOTS_PER_SHARD {
            let i = (start + probe) % SLOTS_PER_SHARD;
            if slots[i].topic == topic && slots[i].partition == partition {
                return Ok(Some(slots[i].next_offset));
            }
            if slots[i].topic == 0 {
                break;
            }
        }
        Ok(None)
    }

    fn log_start_offset(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        // Metadata, so Postgres holds it in both engines — retention moves it, and a
        pmeta::log_start_offset(topic, partition)
    }

    fn partition_bytes(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        let infos = match Self::segment_infos(topic, partition) {
            Ok(v) => v,
            Err(StoreError::Io(_)) => return Ok(0),
            Err(e) => return Err(e),
        };
        Ok(infos.iter().map(|i| i.bytes as i64).sum())
    }

    fn log_dir(&self) -> String {
        data_path(&PathBuf::from(LOG_DIR)).to_string_lossy().into_owned()
    }

    /// `unlink`, never a record-by-record delete: whole sealed segments only, never the active
    fn truncate_below(&mut self, topic: TopicId, partition: i32, offset: i64) -> StoreResult<()> {
        self.reclaim(topic, partition, offset).map(|_| ())
    }

    /// One compaction pass over a partition's **sealed** segments.
    fn compact(&mut self, topic: TopicId, partition: i32) -> StoreResult<u64> {
        use kafgres_codec::compaction::{rebuild_batch, survivors_until, KeptRecord};
        use kafgres_codec::records::{BatchIter, RecordBatch};

        let limits = crate::config::compaction_limits(topic);
        let now = now_millis();
        let tombstone_cutoff = now - limits.delete_retention_ms;
        let lag_cutoff = now - limits.min_compaction_lag_ms;

        let infos = Self::segment_infos(topic, partition)?;
        let sealed: Vec<SegInfo> = infos[..infos.len().saturating_sub(1)].to_vec();
        if sealed.is_empty() {
            return Ok(0);
        }

        // Resume where the last pass stopped, wrapping. Without this a bounded pass re-reads
        let cursor = Self::with_slot(topic, partition, |st, _| Ok(st.compact_cursor))?;
        let start = sealed.iter().position(|i| i.base >= cursor).unwrap_or(0);

        let mut window: Vec<SegInfo> = Vec::new();
        let mut bytes = 0usize;
        for info in sealed.iter().cycle().skip(start).take(sealed.len()) {
            // Checked *before* the segment is added: checking after would allow one whole `segment_bytes`
            if !window.is_empty()
                && (bytes + info.bytes as usize > MAX_COMPACT_BYTES
                    || window.len() >= MAX_COMPACT_SEGMENTS)
            {
                break;
            }
            if info.mtime_ms > lag_cutoff {
                break;
            }
            bytes += info.bytes as usize;
            window.push(info.clone());
        }
        if window.is_empty() {
            return Ok(0);
        }

        let mut loaded: Vec<(SegInfo, Vec<RecordBatch>)> = Vec::new();
        for info in &window {
            let path = data_path(&segment_path(topic, partition, info.base, "log"));
            let vfd = match Vfd::open(&path, false) {
                Ok(v) => v,
                // Reclaimed between the listing and here. Not an error: it is gone, which
                Err(_) => continue,
            };
            let size = vfd.size()? as usize;
            if size > MAX_COMPACT_BYTES {
                continue;
            }
            let mut buf = vec![0u8; size];
            if size > 0 && vfd.read_at(&mut buf, 0)? != size {
                continue;
            }
            let blob = kafgres_codec::bytes::Bytes::from(buf);
            let mut batches = Vec::new();
            for item in BatchIter::new(blob) {
                match item {
                    Ok(view) => batches.push(view),
                    Err(e) => {
                        pgrx::log!("kafgres: compaction skipping {}: {e}", path.display());
                        batches.clear();
                        break;
                    }
                }
            }
            if !batches.is_empty() {
                loaded.push((info.clone(), batches));
            }
        }
        if loaded.is_empty() {
            return Ok(0);
        }

        let all: Vec<RecordBatch> = loaded
            .iter()
            .flat_map(|(_, b)| b.iter().cloned())
            .collect();
        let keep = survivors_until(&all, tombstone_cutoff)
            .map_err(|e| StoreError::Io(format!("compaction survivors: {e}")))?;

        let mut removed = 0u64;
        let mut last_base = window[window.len() - 1].base;
        for (info, batches) in &loaded {
            let mut rebuilt: Vec<u8> = Vec::new();
            let mut changed = false;
            for view in batches {
                if view.is_control() {
                    rebuilt.extend_from_slice(view.as_bytes());
                    continue;
                }
                let base = view.base_offset();
                let mut kept = Vec::new();
                let mut total = 0usize;
                for record in view
                    .records_decompressed()
                    .map_err(|e| StoreError::Io(format!("compaction records: {e}")))?
                {
                    let record =
                        record.map_err(|e| StoreError::Io(format!("compaction record: {e}")))?;
                    total += 1;
                    let offset = base + record.offset_delta as i64;
                    if keep.keeps(offset) {
                        kept.push(KeptRecord {
                            offset,
                            timestamp: view.base_timestamp() + record.timestamp_delta,
                            key: record.key,
                            value: record.value,
                            headers: record.headers,
                            attributes: record.attributes,
                        });
                    }
                }
                if kept.len() == total {
                    rebuilt.extend_from_slice(view.as_bytes());
                    continue;
                }
                changed = true;
                removed += (total - kept.len()) as u64;
                if let Some(bytes) = rebuild_batch(view, &kept) {
                    rebuilt.extend_from_slice(&bytes);
                }
            }
            if !changed {
                continue;
            }
            if !Self::swap_segment(topic, partition, info, &rebuilt)? {
                // Something moved underneath us. The next pass picks it up.
                removed = removed.saturating_sub(1);
            }
        }

        last_base = last_base.saturating_add(1);
        Self::with_slot(topic, partition, |st, _| {
            // Wrap when the window reached the end, so the next pass starts over and
            st.compact_cursor = if window[window.len() - 1].base >= sealed[sealed.len() - 1].base {
                0
            } else {
                last_base
            };
            Ok(())
        })?;

        Ok(removed)
    }

    fn enforce_retention(
        &mut self,
        topic: TopicId,
        policy: &RetentionPolicy,
    ) -> StoreResult<u64> {
        if policy.retention_ms.is_none() && policy.retention_bytes.is_none() {
            return Ok(0);
        }
        let mut dropped = 0;
        for partition in pmeta::partitions(topic)? {
            let infos = Self::segment_infos(topic, partition)?;
            let cutoff = Self::retention_cutoff(&infos, policy);
            dropped += self.reclaim(topic, partition, cutoff)?;
        }
        Ok(dropped)
    }

    fn create_partition(&mut self, topic: TopicId, partition: i32, epoch: i32) -> StoreResult<()> {
        pmeta::create_partition(topic, partition, epoch)?;
        ensure_dir(&data_path(&partition_dir(topic, partition)))
    }

    fn drop_partition(&mut self, topic: TopicId, partition: i32) -> StoreResult<()> {
        pmeta::drop_partition(topic, partition)?;
        let dir = data_path(&partition_dir(topic, partition));
        if let Err(e) = std::fs::remove_dir_all(&dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(StoreError::Io(format!("remove {}: {e}", dir.display())));
            }
        }
        // And the topic directory once its last partition is gone; `remove_dir` failing with
        let _ = std::fs::remove_dir(data_path(&PathBuf::from(LOG_DIR).join(topic.to_string())));

        // Free the shared slot, or the partition keeps its append position across a
        {
            let mut slots = SHARDS[shard_of(topic, partition)].exclusive();
            for slot in slots.iter_mut() {
                if slot.topic == topic && slot.partition == partition {
                    *slot = Slot::default();
                }
            }
        }
        if let Some(map) = HINTS.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            map.remove(&(topic, partition));
        }
        evict_active(topic, partition);
        Ok(())
    }

    fn sync_partition(&mut self, topic: TopicId, partition: i32) -> StoreResult<()> {
        // The slot is read under the shard lock and released before the fsync: holding it
        // across a device flush would stall every other appender, including a
        // `kafgres_produce()` in a business transaction.
        let (base, generation, bytes) = Self::with_slot(topic, partition, |st, _| {
            Ok((st.active_base, st.layout_generation, st.active_bytes))
        })?;
        with_active(topic, partition, base, generation, |a| {
            // Hand the tail to writeback first, as the roll path does.
            a.log
                .writeback(a.writeback_from, bytes.saturating_sub(a.writeback_from));
            a.log.sync();
            a.writeback_from = bytes;
            Ok(())
        })
    }

    fn leader_epoch(&self, topic: TopicId, partition: i32) -> StoreResult<i32> {
        pmeta::leader_epoch(topic, partition)
    }

    fn set_leader_epoch(
        &mut self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<bool> {
        if epoch <= self.leader_epoch(topic, partition)? {
            return Ok(false);
        }
        // One critical section: the offset the epoch starts at and the epoch itself are decided
        let start = Self::with_slot(topic, partition, |st, _| {
            st.leader_epoch = epoch;
            Ok(st.next_offset)
        })?;

        // After the slot, and the order is forced: on an abort the slot stays ahead of committed
        pmeta::record_epoch(topic, partition, epoch, start)?;
        Ok(true)
    }

    fn epoch_end_offset(
        &self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<super::EpochEnd> {
        pmeta::epoch_end_offset(topic, partition, epoch, || {
            self.high_watermark(topic, partition)
        })
    }

    fn epoch_start_offset(
        &self,
        topic: TopicId,
        partition: i32,
        epoch: i32,
    ) -> StoreResult<Option<i64>> {
        pmeta::epoch_start_offset(topic, partition, epoch)
    }

    fn append_pending(
        &mut self,
        topic: TopicId,
        partition: i32,
        batch: RawBatch,
    ) -> StoreResult<(i64, i64)> {
        self.append_pending_impl(topic, partition, batch)
    }

    /// The only place committed records are deliberately destroyed: a leader's
    fn truncate_to(&mut self, topic: TopicId, partition: i32, offset: i64) -> StoreResult<i64> {
        let bases = Self::segment_bases(topic, partition)?;
        let removed = Self::with_slot(topic, partition, |st, hints| {
            if offset >= st.next_offset {
                return Ok(0); // Nothing above it; not divergence.
            }
            let removed = st.next_offset - offset;

            // Whether any segment survived far enough to be cut in place.
            let mut truncated_in_place = false;

            for base in bases.iter().rev() {
                if *base >= offset {
                    let p = data_path(&segment_path(topic, partition, *base, "log"));
                    let _ = std::fs::remove_file(&p);
                    let _ = std::fs::remove_file(
                        data_path(&segment_path(topic, partition, *base, "index")),
                    );
                    let _ = std::fs::remove_file(
                        data_path(&segment_path(topic, partition, *base, "timeindex")),
                    );
                    hints.remove(base);
                    continue;
                }

                let path = data_path(&segment_path(topic, partition, *base, "log"));
                let vfd = Vfd::open(&path, false)?;
                let size = vfd.size()?;
                let mut pos = 0u64;
                let mut cut = size;
                let mut header = [0u8; records::RECORD_BATCH_OVERHEAD];
                while pos + header.len() as u64 <= size {
                    if vfd.read_at(&mut header, pos)? != header.len() {
                        break;
                    }
                    let length = i32::from_be_bytes(
                        header[records::LENGTH_OFFSET..records::LENGTH_OFFSET + 4]
                            .try_into()
                            .expect("4 bytes"),
                    );
                    if length <= 0 {
                        break;
                    }
                    let batch_base = i64::from_be_bytes(
                        header[records::BASE_OFFSET_OFFSET..records::BASE_OFFSET_OFFSET + 8]
                            .try_into()
                            .expect("8 bytes"),
                    );
                    if batch_base >= offset {
                        cut = pos;
                        break;
                    }
                    pos += records::LENGTH_OFFSET as u64 + 4 + length as u64;
                }
                vfd.truncate(cut)?;
                // The retained prefix's max, recomputed: a stale value would poison the next index entry's
                st.max_timestamp_so_far = i64::MIN;
                let mut scan = 0u64;
                let mut hdr = [0u8; records::RECORD_BATCH_OVERHEAD];
                while scan + hdr.len() as u64 <= cut {
                    if vfd.read_at(&mut hdr, scan)? != hdr.len() {
                        break;
                    }
                    let len = i32::from_be_bytes(
                        hdr[records::LENGTH_OFFSET..records::LENGTH_OFFSET + 4]
                            .try_into()
                            .expect("4 bytes"),
                    );
                    if len <= 0 {
                        break;
                    }
                    if let Some(ts) = max_timestamp_of(&hdr) {
                        st.max_timestamp_so_far = st.max_timestamp_so_far.max(ts);
                    }
                    scan += records::LENGTH_OFFSET as u64 + 4 + len as u64;
                }
                // Both indexes may name positions beyond the cut; the segment refills with different
                let _ = std::fs::remove_file(
                    data_path(&segment_path(topic, partition, *base, "index")),
                );
                let _ = std::fs::remove_file(
                    data_path(&segment_path(topic, partition, *base, "timeindex")),
                );
                hints.remove(base);
                st.active_base = *base;
                st.active_bytes = cut;
                // A truncation is a byte-layout change, so it takes the same generation bump a
                // compaction rewrite takes: other processes hold hints past the cut, and this
                // process holds open descriptors for the index files removed just above.
                st.layout_generation = st.layout_generation.wrapping_add(1);
                truncated_in_place = true;
                break;
            }

            if !truncated_in_place {
                // Every base was at or above the cut, so the loop unlinked all of them. Without
                // this the next append writes into a zero-filled prefix the first scan stops
                // on, or into the unlinked inode this process still holds open.
                st.active_base = offset;
                st.active_bytes = 0;
                st.max_timestamp_so_far = i64::MIN;
                st.layout_generation = st.layout_generation.wrapping_add(1);
            }

            st.next_offset = offset;
            log!(
                "kafgres: truncated {topic}/{partition} to offset {offset}, discarding \
                 {removed} record slot(s) this node held and the leader did not"
            );
            Ok(removed)
        })?;

        // The archive's record is now false: a row saying base N was archived vouches for bytes
        crate::archive::forget_from(topic, partition, offset).map_err(StoreError::Io)?;
        Ok(removed)
    }

    fn append_replicated(
        &mut self,
        topic: TopicId,
        partition: i32,
        bytes: &[u8],
        expected_base: i64,
    ) -> StoreResult<i64> {
        let view = RecordBatch::validated(kafgres_codec::bytes::Bytes::from(bytes.to_vec()))
            .map_err(|_| StoreError::CorruptBatch)?;
        let batch_base = view.base_offset();
        let last_offset = view.last_offset();
        let epoch = view.partition_leader_epoch();
        drop(view);

        Self::with_slot(topic, partition, |st, hints| {
            if st.next_offset != expected_base {
                return Err(StoreError::Io(format!(
                    "replication position moved: caller expected log end {expected_base}, \
                     partition is at {}",
                    st.next_offset
                )));
            }
            if batch_base != st.next_offset {
                return Err(StoreError::Io(format!(
                    "replicated batch starts at {batch_base} but the log ends at {}: a gap \
                     or overlap, not something to write through",
                    st.next_offset
                )));
            }

            Self::write_raw(topic, partition, st, hints, bytes, batch_base)?;
            st.next_offset = last_offset + 1;
            let _ = epoch;
            Ok(batch_base)
        })
    }

    fn last_stable_offset(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        self.last_stable_offset_impl(topic, partition)
    }
}

impl SegmentStore {
    /// Append `batch` and reserve its offsets for an uncommitted transaction: until the caller's
    fn append_pending_impl(
        &mut self,
        topic: TopicId,
        partition: i32,
        batch: RawBatch,
    ) -> StoreResult<(i64, i64)> {
        let seed = self.leader_epoch(topic, partition)?;
        let (seg_ms, seg_bytes) = roll_bounds_cached(topic);
        Self::with_slot(topic, partition, |st, hints| {
            st.segment_ms = seg_ms;
            st.segment_bytes = seg_bytes;
            let epoch = st.epoch_for_append(seed)?;
            let base_offset = st.next_offset;
            let last_offset = base_offset + batch.last_offset_delta as i64;
            Self::write_batch(topic, partition, st, hints, batch, epoch, base_offset)?;

            // Conservative on purpose: the low-water mark does not rise as earlier transactions commit
            if st.pending_count == 0 {
                st.pending_from = base_offset;
            }
            st.pending_count += 1;
            Ok((base_offset, last_offset))
        })
    }

    /// Release one uncommitted reservation, on both commit and abort: missing it on the abort
    pub fn release_pending(topic: TopicId, partition: i32) {
        let mut slots = SHARDS[shard_of(topic, partition)].exclusive();
        for slot in slots.iter_mut() {
            if slot.topic == topic && slot.partition == partition {
                slot.pending_count = (slot.pending_count - 1).max(0);
                if slot.pending_count == 0 {
                    slot.pending_from = -1;
                }
                return;
            }
        }
    }

    /// The Last Stable Offset: the first offset a `read_committed` consumer must not pass.
    fn last_stable_offset_impl(&self, topic: TopicId, partition: i32) -> StoreResult<i64> {
        // Two mechanisms hold this back and both must be consulted: `pending_*` covers
        let pending = Self::with_slot(topic, partition, |st, _| {
            Ok(if st.pending_count > 0 {
                st.pending_from
            } else {
                st.next_offset
            })
        })?;
        match pmeta::kafka_txn_lso(topic, partition)? {
            Some(kafka) if kafka >= 0 => Ok(pending.min(kafka)),
            _ => Ok(pending),
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    // The name is load-bearing: `#[pg_schema]` makes it a SQL schema and pgrx calls each test
    // as `tests.<fn>()`, so the plain unit tests above are `slot_tests`. It also cannot start
    // with `pg_`, which Postgres reserves: `CREATE EXTENSION kafgres` then fails (42939)
    // and every `#[pg_test]` in the crate fails in the harness.
    use pgrx::prelude::*;

    use crate::storage::RawBatch;
    use kafgres_codec::records::{build_batch, NewRecord};

    fn raw(value: &[u8]) -> RawBatch {
        let bytes = build_batch(&[NewRecord {
            key: None,
            value: Some(value.to_vec()),
            timestamp: 0,
        }]);
        RawBatch {
            bytes: bytes.to_vec(),
            record_count: 1,
            last_offset_delta: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
            is_control: false,
        }
    }

    /// Truncating to at or below the partition's first base removes every segment, so the loop
    /// never reaches the in-place branch that resets the slot; its only caller is the follower worker.
    #[pg_test]
    fn pg_a_full_truncation_leaves_the_partition_writable() {
        crate::ensure_tables_exist();
        let created = crate::meta::create_topic("trunc-regression", 1, &[])
            .expect("topic creation failed");
        let topic = created.topic_id;

        let mut store = crate::storage::open();
        store.append(topic, 0, raw(b"before"), None).expect("first append failed");

        // Everything this node holds is divergent: the cut is at the partition's first base.
        store.truncate_to(topic, 0, 0).expect("truncation failed");

        let base = store
            .append(topic, 0, raw(b"after"), None)
            .expect("append after a full truncation failed");
        assert_eq!(base, 0, "the refilled partition must restart at the cut");

        let slice = store
            .read(topic, 0, 0, 1 << 20, crate::storage::IsolationLevel::ReadUncommitted)
            .expect("read failed");
        assert!(
            !slice.bytes.is_empty(),
            "the partition read back empty after a full truncation and a re-append. The \
             slot still described the segment the truncation deleted, so the append either \
             wrote past a zero-filled hole that the first scan stops on, or into the \
             unlinked inode this process still held open."
        );
        assert_eq!(
            slice.next_offset, 1,
            "one record was appended after the cut, so the next offset is 1"
        );
    }

    /// A transaction-V2 producer never sends AddPartitionsToTxn, so the first transactional
    /// append is what begins the transaction. `register_txn_partition` is that append's bookkeeping.
    #[pg_test]
    fn pg_a_transactional_append_begins_the_transaction() {
        crate::ensure_tables_exist();
        let created = crate::meta::create_topic("txn-register", 1, &[])
            .expect("topic creation failed");
        let topic = created.topic_id;

        Spi::run_with_args(
            "INSERT INTO kafgres_producers (producer_id, producer_epoch, transactional_id)
             VALUES (424242, 0, 'pg-register')",
            &[],
        )
        .expect("producer insert failed");
        Spi::run_with_args(
            "INSERT INTO kafgres_txns
                    (producer_id, producer_epoch, transactional_id, state, started_at)
             VALUES (424242, 0, 'pg-register', 'empty', 1)",
            &[],
        )
        .expect("txn insert failed");

        crate::storage::pmeta::register_txn_partition(424242, 0, topic, 0, 7)
            .expect("register failed");
        let state: String = Spi::get_one(
            "SELECT (SELECT state FROM kafgres_txns WHERE producer_id = 424242)",
        )
        .expect("state query failed")
        .expect("txn row missing");
        assert_eq!(state, "ongoing", "the append must begin an empty transaction");
        let first: i64 = Spi::get_one(
            "SELECT (SELECT first_offset FROM kafgres_txn_partitions
                      WHERE producer_id = 424242)",
        )
        .expect("first offset query failed")
        .expect("partition row missing");
        assert_eq!(first, 7);

        // A later batch in the same transaction keeps the first batch's offset.
        crate::storage::pmeta::register_txn_partition(424242, 0, topic, 0, 9)
            .expect("second register failed");
        let first: i64 = Spi::get_one(
            "SELECT (SELECT first_offset FROM kafgres_txn_partitions
                      WHERE producer_id = 424242)",
        )
        .expect("first offset query failed")
        .expect("partition row missing");
        assert_eq!(first, 7);

        // The next transaction, after the previous one finished, begins fresh: the finished
        // transaction's partition rows must not feed this one's marker write or LSO.
        Spi::run("UPDATE kafgres_txns SET state = 'committed' WHERE producer_id = 424242")
            .expect("state update failed");
        crate::storage::pmeta::register_txn_partition(424242, 1, topic, 0, 11)
            .expect("third register failed");
        let state: String = Spi::get_one(
            "SELECT (SELECT state FROM kafgres_txns WHERE producer_id = 424242)",
        )
        .expect("state query failed")
        .expect("txn row missing");
        assert_eq!(state, "ongoing");
        let first: i64 = Spi::get_one(
            "SELECT (SELECT first_offset FROM kafgres_txn_partitions
                      WHERE producer_id = 424242)",
        )
        .expect("first offset query failed")
        .expect("partition row missing");
        assert_eq!(first, 11);
    }
}

#[cfg(test)]
mod slot_tests {
    use super::*;

    /// A seed read before a promotion must never override the epoch that promotion published
    #[test]
    fn a_stale_seed_never_lowers_a_published_epoch() {
        let mut slot = Slot::default();
        assert_eq!(slot.leader_epoch, -1, "a fresh slot has not learned its epoch");

        assert_eq!(slot.epoch_for_append(4).unwrap(), 4);

        slot.leader_epoch = 5;
        assert_eq!(
            slot.epoch_for_append(5).unwrap(),
            5,
            "the appender stamped an epoch the promotion had already replaced"
        );
    }

    /// The slot may be raised before its transaction commits, and that transaction can abort;
    #[test]
    fn an_uncommitted_bump_refuses_the_append() {
        let mut slot = Slot::default();
        slot.leader_epoch = 5;

        match slot.epoch_for_append(4) {
            Err(StoreError::LeaderEpochUnsettled) => {}
            other => panic!(
                "an append during an uncommitted epoch bump was allowed: {other:?}. \
                 Stamping 5 writes records the committed history cannot explain; stamping \
                 4 writes the old epoch at offsets the new one is about to claim."
            ),
        }
        assert_eq!(slot.leader_epoch, 5, "the refusal must not disturb the slot");

        assert_eq!(slot.epoch_for_append(5).unwrap(), 5);
    }

    /// A postmaster crash-restart wipes shared memory. The slot relearns from Postgres, and
    #[test]
    fn a_reset_slot_relearns_the_committed_epoch() {
        let mut slot = Slot::default();
        assert_eq!(slot.epoch_for_append(0).unwrap(), 0, "epoch 0 is real, not 'unknown'");

        slot = Slot::default();
        assert_eq!(slot.epoch_for_append(7).unwrap(), 7);
    }
}
