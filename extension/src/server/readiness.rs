//! Socket readiness for the event loop, on Postgres's own `WaitEventSet`: the latch,
//! postmaster death, the listener and every client socket in one wait, with the tick as
//! its ceiling, so a request is served when it arrives rather than at the next tick.
//!
//! The set is rebuilt only when the connection set changes. An fd number is reused by the
//! next accept after a close, and epoll drops a closed fd from the interest list, so
//! membership is keyed on the connection id as well as the fd: a new connection reusing a
//! closed one's descriptor number forces a rebuild. Write interest is toggled in place
//! with `ModifyWaitEvent` for connections holding unsent bytes, so a socket that drains
//! wakes the loop.

use std::time::Duration;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::pg_sys::pg_try::PgTryBuilder;
use pgrx::prelude::*;
use pgrx::PgSqlErrorCode;

/// Position of the first socket in the set: the latch and postmaster death come first.
const FIRST_SOCKET: usize = 2;

pub(super) struct Readiness {
    set: *mut pg_sys::WaitEventSet,
    /// (fd, connection id) per registered socket, in set order; the listener is
    /// `(fd, -1)`. Compared against the live connections to decide on a rebuild.
    members: Vec<(i32, i32)>,
    /// Event mask currently registered per socket, to skip no-op modifications.
    masks: Vec<u32>,
    occurred: Vec<pg_sys::WaitEvent>,
}

impl Readiness {
    pub(super) fn new() -> Self {
        Readiness {
            set: std::ptr::null_mut(),
            members: Vec::new(),
            masks: Vec::new(),
            occurred: Vec::new(),
        }
    }

    /// Bring the set in line with the sockets to watch: `listener`, then one
    /// `(fd, id, wants_read, wants_write)` per connection.
    pub(super) fn sync(&mut self, listener: i32, conns: &[(i32, i32, bool, bool)]) {
        // Write interest only while there is something to write; level-triggered
        // writability would otherwise wake every pass. Read interest likewise. A socket
        // wanting neither is left out: Postgres asserts a socket event waits for something.
        let watched: Vec<(i32, i32, u32)> = conns
            .iter()
            .map(|&(fd, id, wants_read, wants_write)| {
                let mask = if wants_read { pg_sys::WL_SOCKET_READABLE } else { 0 }
                    | if wants_write { pg_sys::WL_SOCKET_WRITEABLE } else { 0 };
                (fd, id, mask)
            })
            .filter(|&(_, _, mask)| mask != 0)
            .collect();
        let mut want = Vec::with_capacity(watched.len() + 1);
        want.push((listener, -1));
        want.extend(watched.iter().map(|&(fd, id, _)| (fd, id)));
        if self.set.is_null() || want != self.members {
            self.rebuild(&want);
        }
        for (i, &(_, _, mask)) in watched.iter().enumerate() {
            let pos = i + 1;
            if self.masks[pos] != mask {
                unsafe {
                    pg_sys::ModifyWaitEvent(
                        self.set,
                        (FIRST_SOCKET + pos) as i32,
                        mask,
                        std::ptr::null_mut(),
                    );
                }
                self.masks[pos] = mask;
            }
        }
    }

    fn rebuild(&mut self, want: &[(i32, i32)]) {
        unsafe {
            if !self.set.is_null() {
                pg_sys::FreeWaitEventSet(self.set);
                // Cleared before create: CreateWaitEventSet can raise ERROR (epoll_create1
                // failure), and Drop would otherwise double-pfree the freed pointer.
                self.set = std::ptr::null_mut();
            }
            let n = (FIRST_SOCKET + want.len()) as i32;
            self.set = pg_sys::CreateWaitEventSet(pg_sys::TopMemoryContext, n);
            pg_sys::AddWaitEventToSet(
                self.set,
                pg_sys::WL_LATCH_SET,
                pg_sys::PGINVALID_SOCKET,
                pg_sys::MyLatch,
                std::ptr::null_mut(),
            );
            pg_sys::AddWaitEventToSet(
                self.set,
                pg_sys::WL_EXIT_ON_PM_DEATH,
                pg_sys::PGINVALID_SOCKET,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            for &(fd, _) in want {
                pg_sys::AddWaitEventToSet(
                    self.set,
                    pg_sys::WL_SOCKET_READABLE,
                    fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
            }
        }
        self.members = want.to_vec();
        self.masks = vec![pg_sys::WL_SOCKET_READABLE; want.len()];
        self.occurred
            .resize_with(FIRST_SOCKET + want.len(), pg_sys::WaitEvent::default);
    }

    /// Block until a watched socket is ready, the latch is set, or `timeout` passes.
    /// Same contract as `BackgroundWorker::wait_latch`: false means stop.
    pub(super) fn wait(&mut self, timeout: Duration) -> bool {
        if self.set.is_null() {
            return BackgroundWorker::wait_latch(Some(timeout));
        }
        let ms: i64 = timeout.as_millis().try_into().unwrap_or(i64::MAX);
        unsafe {
            // `WL_EXIT_ON_PM_DEATH` exits the process on postmaster death, so there is
            // no death flag to read back here.
            pg_sys::WaitEventSetWait(
                self.set,
                ms,
                self.occurred.as_mut_ptr(),
                self.occurred.len() as i32,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(pg_sys::MyLatch);
        }
        // `check_for_interrupts!` here is outside every `atomically`, so an ERROR from a
        // statement cancel would reach `#[pg_guard]` and restart the worker, dropping
        // every client, for something as ordinary as pg_cancel_backend or a stray
        // lock/statement timeout. There is no statement to cancel here, so it is
        // swallowed: `ProcessInterrupts` raises ERRCODE_QUERY_CANCELED or
        // ERRCODE_LOCK_NOT_AVAILABLE and clears QueryCancelPending before raising, so a
        // caught cancel cannot spin. ProcDiePending raises FATAL, which does not longjmp
        // to a PG_TRY, so termination is unaffected; any other error rethrows.
        let swallowed = PgTryBuilder::new(|| {
            unsafe { pg_sys::check_for_interrupts!() };
            false
        })
        .catch_when(PgSqlErrorCode::ERRCODE_QUERY_CANCELED, |_| true)
        .catch_when(PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE, |_| true)
        .execute();

        if swallowed {
            log!("kafgres: ignoring a statement cancel in the broker's wait loop");
            // The cancel path clears InterruptPending on entry and nothing re-sends it,
            // so a pending ProcSignalBarrier (DROP DATABASE, ALTER ... SET TABLESPACE
            // waiting on this worker) must be re-armed by hand or it hangs for good.
            unsafe {
                if pg_sys::ProcSignalBarrierPending != 0 {
                    pg_sys::InterruptPending = 1;
                }
            }
        }
        !BackgroundWorker::sigterm_received()
    }
}

impl Drop for Readiness {
    fn drop(&mut self) {
        if !self.set.is_null() {
            unsafe { pg_sys::FreeWaitEventSet(self.set) };
            self.set = std::ptr::null_mut();
        }
    }
}
