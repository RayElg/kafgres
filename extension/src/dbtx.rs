//! Database plumbing for the request path. A Postgres `ERROR` inside a background worker is

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::handlers::HandlerError;

/// Take every table lock a request needs, up front, **without waiting**. The broker is one
fn acquire_request_locks() -> Result<(), pgrx::spi::Error> {
    crate::meta::lock_for_read()?;
    crate::group::lock_for_read()?;
    crate::acl::lock_for_read()?;
    crate::producer::lock_for_read()?;
    crate::storage::lock_for_read()?;
    // Row-lock backstop. Deliberately not tiny: a producer waiting on another producer's
    pgrx::Spi::run("SET LOCAL lock_timeout = '2s'")?;
    pgrx::Spi::run("SET LOCAL statement_timeout = '5s'")
}

/// Run `f` inside a savepoint, catching a Postgres error rather than letting it unwind: on
pub fn atomically<T, E>(
    f: impl FnOnce() -> Result<T, E>,
    // `catch_others` runs across a setjmp, so it wants an `FnMut` that is unwind-safe
    aborted: impl Fn(&str) -> E + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
) -> Result<T, E> {
    use pgrx::pg_sys::pg_try::PgTryBuilder;

    unsafe {
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null_mut());
    }
    // Whether catch_others already released the subtransaction, so the Rust-Err path
    let rolled_back_by_pg = AtomicBool::new(false);

    let result = PgTryBuilder::new(AssertUnwindSafe(f))
        .catch_others(|caught| {
            // Log what actually happened before substituting the caller's error: `aborted`
            pgrx::log!("kafgres: subtransaction aborted: {caught:?}");
            let message = match &caught {
                pgrx::pg_sys::panic::CaughtError::PostgresError(e)
                | pgrx::pg_sys::panic::CaughtError::ErrorReport(e)
                | pgrx::pg_sys::panic::CaughtError::RustPanic { ereport: e, .. } => {
                    e.message().to_string()
                }
            };
            unsafe {
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
            }
            rolled_back_by_pg.store(true, Ordering::Relaxed);
            Err(aborted(&message))
        })
        .execute();

    if result.is_ok() {
        unsafe {
            pgrx::pg_sys::ReleaseCurrentSubTransaction();
        }
    } else if !rolled_back_by_pg.load(Ordering::Relaxed) {
        unsafe {
            pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
        }
    }
    result
}

fn with_subtransaction<T>(f: impl FnOnce() -> Result<T, HandlerError>) -> Result<T, HandlerError> {
    atomically(f, |_| {
        HandlerError::Internal("query aborted (lock or statement timeout)".to_string())
    })
}

/// For a request path that touches no kafgres table: timeouts and containment, no locks.
pub fn contained<T>(f: impl FnOnce() -> Result<T, HandlerError>) -> Result<T, HandlerError> {
    with_subtransaction(|| {
        pgrx::Spi::run("SET LOCAL lock_timeout = '2s'")?;
        pgrx::Spi::run("SET LOCAL statement_timeout = '5s'")?;
        f()
    })
}

/// The wrapper every request-path transaction body should use: timeouts applied, query
pub fn guarded<T>(f: impl FnOnce() -> Result<T, HandlerError>) -> Result<T, HandlerError> {
    // Locks first, and inside the savepoint: a NOWAIT failure is an ordinary error the
    with_subtransaction(|| {
        acquire_request_locks()?;
        f()
    })
}
