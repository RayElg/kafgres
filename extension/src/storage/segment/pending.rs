//! LSO reservations of in-flight `kafgres_produce()` transactions, one ring per slot.
//!
//! A transaction reserves a partition at most once, so a ring of `MaxBackends` entries
//! cannot fill: every backend and background worker counts there, and PREPARE TRANSACTION
//! after `kafgres_produce()` is refused, so a prepared transaction holds none.

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use pgrx::pg_sys;

use super::MAX_TRACKED_PARTITIONS;

/// One partition's reserved bases, ascending, in its ring of the shared region.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PendingRing {
    /// The lowest base, cached so an LSO read never touches the region. `-1` when empty.
    min: i64,
    head: u32,
    len: u32,
}

impl Default for PendingRing {
    fn default() -> Self {
        PendingRing {
            min: -1,
            head: 0,
            len: 0,
        }
    }
}

fn phys(head: usize, j: usize, cap: usize) -> usize {
    let p = head + j;
    if p >= cap {
        p - cap
    } else {
        p
    }
}

impl PendingRing {
    pub fn floor(&self) -> Option<i64> {
        (self.len > 0).then_some(self.min)
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Add `base`; false when `ring` is full. Bases arrive ascending, so this is O(1); a base
    /// below the newest (after a truncation lowers the log end) is inserted in order.
    pub fn reserve(&mut self, ring: &mut [i64], base: i64) -> bool {
        let cap = ring.len();
        let (head, len) = (self.head as usize, self.len as usize);
        if len >= cap || head >= cap {
            return false;
        }
        let mut j = len;
        while j > 0 && ring[phys(head, j - 1, cap)] > base {
            ring[phys(head, j, cap)] = ring[phys(head, j - 1, cap)];
            j -= 1;
        }
        ring[phys(head, j, cap)] = base;
        self.len += 1;
        self.min = ring[head];
        true
    }

    /// Remove one entry equal to `base`; false, changing nothing, if there is none. Binary
    /// search, then the shorter side of the ring shifts over the gap.
    pub fn release(&mut self, ring: &mut [i64], base: i64) -> bool {
        let cap = ring.len();
        let (head, len) = (self.head as usize, self.len as usize);
        // Runs in a commit or abort callback, where a panic is a PANIC.
        if len == 0 || len > cap || head >= cap {
            return false;
        }
        let (mut lo, mut hi) = (0, len);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if ring[phys(head, mid, cap)] < base {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == len || ring[phys(head, lo, cap)] != base {
            return false;
        }
        if lo < len / 2 {
            for j in (0..lo).rev() {
                ring[phys(head, j + 1, cap)] = ring[phys(head, j, cap)];
            }
            self.head = phys(head, 1, cap) as u32;
        } else {
            for j in lo..len - 1 {
                ring[phys(head, j, cap)] = ring[phys(head, j + 1, cap)];
            }
        }
        self.len -= 1;
        if self.len == 0 {
            *self = PendingRing::default();
        } else {
            self.min = ring[self.head as usize];
        }
        true
    }
}

static REGION: AtomicPtr<i64> = AtomicPtr::new(std::ptr::null_mut());
/// Ring entries per slot, fixed at postmaster start.
static CAPACITY: AtomicUsize = AtomicUsize::new(0);

pub fn capacity() -> usize {
    CAPACITY.load(Ordering::Relaxed)
}

/// Slot `index`'s ring; empty if the region is missing.
///
/// # Safety
/// The caller holds the exclusive lock of the shard that owns slot `index`.
pub unsafe fn ring_mut(index: usize) -> &'static mut [i64] {
    let base = REGION.load(Ordering::Acquire);
    let cap = capacity();
    if base.is_null() || index >= MAX_TRACKED_PARTITIONS {
        return &mut [];
    }
    // SAFETY: the region holds MAX_TRACKED_PARTITIONS rings of `cap` entries, and the
    // shard lock makes this slot's ring exclusive to the caller.
    unsafe { std::slice::from_raw_parts_mut(base.add(index * cap), cap) }
}

/// `MaxBackends`, which PG15+ computes before `shmem_request_hook`. PG13 and 14 request
/// space from `_PG_init`, before it exists, so this repeats their `InitializeMaxBackends`.
fn max_backends() -> usize {
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    {
        fn guc(name: &std::ffi::CStr) -> usize {
            // SAFETY: a built-in GUC, read in the postmaster.
            let v = unsafe { pg_sys::GetConfigOption(name.as_ptr(), false, false) };
            if v.is_null() {
                return 0;
            }
            // SAFETY: GetConfigOption returns a NUL-terminated string.
            let s = unsafe { std::ffi::CStr::from_ptr(v) };
            s.to_str().ok().and_then(|s| s.parse().ok()).unwrap_or(0)
        }
        guc(c"max_connections")
            + guc(c"autovacuum_max_workers")
            + 1
            + guc(c"max_worker_processes")
            + guc(c"max_wal_senders")
    }
    #[cfg(not(any(feature = "pg13", feature = "pg14")))]
    {
        // SAFETY: set before shmem_request_hook runs.
        unsafe { pg_sys::MaxBackends.max(0) as usize }
    }
}

fn region_bytes(cap: usize) -> usize {
    MAX_TRACKED_PARTITIONS * cap * std::mem::size_of::<i64>()
}

pub struct Region;

pub static PENDING_REGION: Region = Region;

impl pgrx::PgSharedMemoryInitialization for Region {
    type Value = ();

    unsafe fn on_shmem_request(&'static self) {
        let cap = max_backends();
        CAPACITY.store(cap, Ordering::Relaxed);
        // SAFETY: from _PG_init (PG13, 14) or shmem_request_hook (PG15+).
        unsafe { pg_sys::RequestAddinShmemSpace(region_bytes(cap)) };
    }

    unsafe fn on_shmem_startup(&'static self, _value: ()) {
        let cap = capacity();
        // SAFETY: shmem_startup_hook, under AddinShmemInitLock as pgrx's own regions are.
        unsafe {
            let lock = &raw mut (*pg_sys::MainLWLockArray.add(21)).lock;
            pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE);
            let mut found = false;
            let ptr = pg_sys::ShmemInitStruct(
                c"kafgres_pending_reservations".as_ptr(),
                region_bytes(cap),
                &mut found,
            )
            .cast::<i64>();
            REGION.store(ptr, Ordering::Release);
            pg_sys::LWLockRelease(lock);
        }
        // SAFETY: MaxBackends is set before shared memory is created.
        let actual = unsafe { pg_sys::MaxBackends.max(0) as usize };
        if actual > cap {
            pgrx::warning!(
                "kafgres: reservation rings hold {cap} transactions but MaxBackends is \
                 {actual}; kafgres_produce() refuses a partition's transaction past {cap}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contents(r: &PendingRing, ring: &[i64]) -> Vec<i64> {
        (0..r.len())
            .map(|j| ring[phys(r.head as usize, j, ring.len())])
            .collect()
    }

    #[test]
    fn the_floor_is_the_oldest_unreleased_base() {
        let mut ring = vec![0; 8];
        let mut r = PendingRing::default();
        assert_eq!(r.floor(), None);
        for b in [3, 5, 9] {
            assert!(r.reserve(&mut ring, b));
        }
        assert_eq!(r.floor(), Some(3));
        assert!(r.release(&mut ring, 3));
        assert_eq!(r.floor(), Some(5));
        assert!(r.release(&mut ring, 9));
        assert_eq!(r.floor(), Some(5));
        assert!(r.release(&mut ring, 5));
        assert_eq!(r.floor(), None);
    }

    #[test]
    fn an_out_of_order_release_keeps_the_rest_in_order() {
        let mut ring = vec![0; 8];
        let mut r = PendingRing::default();
        for b in 10..17 {
            assert!(r.reserve(&mut ring, b));
        }
        for b in [12, 15, 11, 16] {
            assert!(r.release(&mut ring, b));
        }
        assert_eq!(contents(&r, &ring), vec![10, 13, 14]);
        assert_eq!(r.floor(), Some(10));
        assert!(r.release(&mut ring, 10));
        assert_eq!(r.floor(), Some(13));
    }

    #[test]
    fn a_stray_release_changes_nothing() {
        let mut ring = vec![0; 4];
        let mut r = PendingRing::default();
        assert!(!r.release(&mut ring, 3));
        assert!(r.reserve(&mut ring, 5));
        assert!(r.reserve(&mut ring, 8));
        for stray in [3, 6, 9] {
            assert!(!r.release(&mut ring, stray));
        }
        assert_eq!(contents(&r, &ring), vec![5, 8]);
    }

    #[test]
    fn the_ring_wraps() {
        let cap = 5;
        let mut ring = vec![0; cap];
        let mut r = PendingRing::default();
        let mut next = 0;
        let mut live = std::collections::VecDeque::new();
        for _ in 0..3 {
            assert!(r.reserve(&mut ring, next));
            live.push_back(next);
            next += 1;
        }
        for _ in 0..4 * cap {
            assert!(r.release(&mut ring, live.pop_front().unwrap()));
            assert!(r.reserve(&mut ring, next));
            live.push_back(next);
            next += 1;
            assert_eq!(contents(&r, &ring), Vec::from(live.clone()));
            assert_eq!(r.floor(), live.front().copied());
        }
        // A release in the middle of a wrapped ring, on each side of the midpoint.
        assert!(r.reserve(&mut ring, next));
        live.push_back(next);
        assert!(r.reserve(&mut ring, next + 1));
        live.push_back(next + 1);
        for b in [live[1], live[3]] {
            assert!(r.release(&mut ring, b));
            live.retain(|x| *x != b);
            assert_eq!(contents(&r, &ring), Vec::from(live.clone()));
        }
    }

    #[test]
    fn capacity_is_exactly_the_bound() {
        let cap = 6;
        let mut ring = vec![0; cap];
        let mut r = PendingRing::default();
        for b in 0..cap as i64 {
            assert!(r.reserve(&mut ring, 100 + b));
        }
        assert!(!r.reserve(&mut ring, 200), "a full ring refuses");
        assert_eq!(contents(&r, &ring), (100..106).collect::<Vec<_>>());
        assert!(r.release(&mut ring, 103));
        assert!(r.reserve(&mut ring, 200));
        assert_eq!(contents(&r, &ring), vec![100, 101, 102, 104, 105, 200]);
        assert!(!r.reserve(&mut ring, 201));
        for b in [100, 101, 102, 104, 105, 200] {
            assert!(r.release(&mut ring, b));
        }
        assert_eq!(r.floor(), None);
    }

    #[test]
    fn a_base_below_the_newest_is_inserted_in_order() {
        let mut ring = vec![0; 6];
        let mut r = PendingRing::default();
        for b in [10, 20, 30] {
            assert!(r.reserve(&mut ring, b));
        }
        assert!(r.reserve(&mut ring, 15));
        assert!(r.reserve(&mut ring, 5));
        assert!(r.reserve(&mut ring, 20));
        assert_eq!(contents(&r, &ring), vec![5, 10, 15, 20, 20, 30]);
        assert_eq!(r.floor(), Some(5));
        assert!(r.release(&mut ring, 20));
        assert_eq!(contents(&r, &ring), vec![5, 10, 15, 20, 30]);
    }

    #[test]
    fn matches_a_sorted_model() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut rand = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for cap in 1..10 {
            let mut ring = vec![0; cap];
            let mut r = PendingRing::default();
            let mut model: Vec<i64> = Vec::new();
            let mut next = 0i64;
            for _ in 0..5000 {
                if rand() % 2 == 0 {
                    let ok = r.reserve(&mut ring, next);
                    assert_eq!(ok, model.len() < cap);
                    if ok {
                        model.push(next);
                    }
                    next += 1 + (rand() % 3) as i64;
                } else if !model.is_empty() {
                    let b = model.remove((rand() as usize) % model.len());
                    assert!(r.release(&mut ring, b));
                }
                assert_eq!(contents(&r, &ring), model);
                assert_eq!(r.floor(), model.first().copied());
            }
        }
    }
}
