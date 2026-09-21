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
use pgrx::prelude::*;

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
    /// `(fd, id, wants_write)` per connection.
    pub(super) fn sync(&mut self, listener: i32, conns: &[(i32, i32, bool)]) {
        let mut want = Vec::with_capacity(conns.len() + 1);
        want.push((listener, -1));
        want.extend(conns.iter().map(|&(fd, id, _)| (fd, id)));
        if self.set.is_null() || want != self.members {
            self.rebuild(&want);
        }
        // Write interest only while there is something to write; level-triggered
        // writability would otherwise wake every pass.
        for (i, &(_, _, wants_write)) in conns.iter().enumerate() {
            let pos = i + 1;
            let mask = pg_sys::WL_SOCKET_READABLE
                | if wants_write { pg_sys::WL_SOCKET_WRITEABLE } else { 0 };
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
            pg_sys::check_for_interrupts!();
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
