//! Database plumbing for the request path. A Postgres `ERROR` inside a background worker is

use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::handlers::HandlerError;

const REQUEST_LOCKS: [fn() -> Result<(), pgrx::spi::Error>; 5] = [
    crate::meta::lock_for_read,
    crate::group::lock_for_read,
    crate::acl::lock_for_read,
    crate::producer::lock_for_read,
    crate::storage::lock_for_read,
];

/// Take every table lock a request needs up front, with `NOWAIT` (I7). A held lock fails the
/// request as [`HandlerError::Busy`] before it has done anything, and the server retries it.
fn acquire_request_locks() -> Result<(), pgrx::spi::Error> {
    if WAIT_FOR_LOCKS.with(|w| w.get()) {
        pgrx::Spi::run(&format!("SET LOCAL lock_timeout = '{SQL_LOCK_WAIT}'"))?;
    }
    // A lock failure is an ERROR, so it skips the reset below; `guarded` reads it then.
    LOCKING.with(|l| l.set(true));
    REQUEST_LOCKS.iter().try_for_each(|lock| lock())?;
    LOCKING.with(|l| l.set(false));
    // Row-lock backstop. Deliberately not tiny: a producer waiting on another producer's
    pgrx::Spi::run("SET LOCAL lock_timeout = '2s'")?;
    pgrx::Spi::run("SET LOCAL statement_timeout = '5s'")
}

thread_local! {
    /// The request-lock phase is in progress: an error now is a held lock, not the body's.
    static LOCKING: Cell<bool> = const { Cell::new(false) };
    /// A `guarded` body succeeded since [`begin_request`], so a retry would repeat its work.
    static DID_WORK: Cell<bool> = const { Cell::new(false) };
    /// Inside [`guarded_waiting`]: request locks wait up to [`SQL_LOCK_WAIT`].
    static WAIT_FOR_LOCKS: Cell<bool> = const { Cell::new(false) };
}

/// How long a SQL caller's request locks wait. A waiter is what makes vacuum truncation
/// yield, and a SQL session is not the broker loop, so waiting stalls no one else.
const SQL_LOCK_WAIT: &str = "2s";

/// `guarded` for a SQL entry point: the same locks, waited for rather than `NOWAIT`. Nothing
/// retries a SQL call the way the broker loop retries a parked request.
pub fn guarded_waiting<T>(f: impl FnOnce() -> Result<T, HandlerError>) -> Result<T, HandlerError> {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            WAIT_FOR_LOCKS.with(|w| w.set(false));
        }
    }
    WAIT_FOR_LOCKS.with(|w| w.set(true));
    let _reset = Reset;
    guarded(f)
}

/// Mark the start of a request, or of a retry of one, for [`HandlerError::Busy`].
pub fn begin_request() {
    DID_WORK.with(|d| d.set(false));
}

fn held_lock(code: pgrx::PgSqlErrorCode) -> bool {
    code == pgrx::PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE && LOCKING.with(|l| l.get())
}

/// ACCESS SHARE on `tables`, a comma-separated list: `NOWAIT` in the broker loop, waited for
/// under [`guarded_waiting`]. A held lock is a `LOCK_NOT_AVAILABLE` error either way.
pub fn lock_tables(tables: &str) -> Result<(), pgrx::spi::Error> {
    let nowait = if WAIT_FOR_LOCKS.with(|w| w.get()) { "" } else { " NOWAIT" };
    pgrx::Spi::run(&format!("LOCK TABLE {tables} IN ACCESS SHARE MODE{nowait}"))
}

/// Run `f` inside a savepoint, catching a Postgres error rather than letting it unwind: on
pub fn atomically<T, E>(
    f: impl FnOnce() -> Result<T, E>,
    // `catch_others` runs across a setjmp, so it wants an `FnMut` that is unwind-safe
    aborted: impl Fn(&str) -> E + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
) -> Result<T, E> {
    atomically_coded(f, move |message, _| aborted(message))
}

/// `atomically`, handing `aborted` the error's SQLSTATE too: messages are translated.
pub fn atomically_coded<T, E>(
    f: impl FnOnce() -> Result<T, E>,
    aborted: impl Fn(&str, pgrx::PgSqlErrorCode) -> E
        + std::panic::UnwindSafe
        + std::panic::RefUnwindSafe,
) -> Result<T, E> {
    use pgrx::pg_sys::pg_try::PgTryBuilder;

    unsafe {
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null_mut());
    }
    // Whether catch_others already released the subtransaction, so the Rust-Err path
    let rolled_back_by_pg = AtomicBool::new(false);

    let result = PgTryBuilder::new(AssertUnwindSafe(f))
        .catch_others(|caught| {
            let (message, code) = match &caught {
                pgrx::pg_sys::panic::CaughtError::PostgresError(e)
                | pgrx::pg_sys::panic::CaughtError::ErrorReport(e)
                | pgrx::pg_sys::panic::CaughtError::RustPanic { ereport: e, .. } => {
                    (e.message().to_string(), e.sql_error_code())
                }
            };
            // Log what actually happened before substituting the caller's error: `aborted`
            // Not logged: a held lock is retried every pass.
            if !held_lock(code) {
                pgrx::log!("kafgres: subtransaction aborted: {caught:?}");
            }
            unsafe {
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
            }
            rolled_back_by_pg.store(true, Ordering::Relaxed);
            Err(aborted(&message, code))
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

/// Run a read in a subtransaction that is rolled back after it, releasing the relation locks
/// it took. Otherwise a SQL caller holds them until its transaction ends, and on the table
/// engine that blocks the broker's segment `CREATE` and `DROP`. `f` must not write.
pub fn released<T>(f: impl FnOnce() -> T) -> T {
    use pgrx::pg_sys;
    use pgrx::pg_sys::pg_try::PgTryBuilder;

    struct Outer(pg_sys::MemoryContext, pg_sys::ResourceOwner);
    impl Outer {
        /// SAFETY: inside the subtransaction this was taken before.
        unsafe fn restore(&self) {
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(self.0);
            pg_sys::CurrentResourceOwner = self.1;
        }
    }
    // SAFETY: called inside a transaction; the rollback below runs on both paths.
    let outer = unsafe {
        let outer = Outer(pg_sys::CurrentMemoryContext, pg_sys::CurrentResourceOwner);
        pg_sys::BeginInternalSubTransaction(std::ptr::null_mut());
        AssertUnwindSafe(outer)
    };
    let out = PgTryBuilder::new(AssertUnwindSafe(f))
        .catch_others(|caught| {
            unsafe { outer.restore() };
            caught.rethrow()
        })
        .execute();
    unsafe { outer.restore() };
    out
}

fn with_subtransaction<T>(f: impl FnOnce() -> Result<T, HandlerError>) -> Result<T, HandlerError> {
    atomically(f, |message| HandlerError::Internal(format!("query aborted: {message}")))
}

/// Hand this worker's accumulated table statistics to the cumulative stats system.
///
/// An ordinary backend does this from `PostgresMain`'s idle loop; a background worker never
/// runs that loop, so without this call its row counts stay in process memory until the
/// worker exits, and autovacuum (which decides what to vacuum from those counts) never sees
/// the churn. Postgres's own long-lived workers call this for the same reason
/// (`replication/logical/worker.c`).
///
/// `force = false` is cheap to call every pass: no stats lock wait, and it returns early
/// when a flush happened less than `PGSTAT_MIN_INTERVAL` (1 s) ago, measured from the last
/// transaction's stop time. A quiet worker's pending counts are force-flushed at most once
/// per `PGSTAT_IDLE_INTERVAL` (10 s), matching Postgres's own idle-backend bound.
///
/// Must be called between transactions: `pgstat_report_stat` asserts
/// `!IsTransactionOrTransactionBlock()`. Call sites are each worker's loop head, never
/// inside `BackgroundWorker::transaction`.
pub fn report_stats() {
    use std::cell::Cell;
    use std::time::{Duration, Instant};

    const IDLE_INTERVAL: Duration = Duration::from_secs(10);
    thread_local! {
        static LAST_FORCED: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    let now = Instant::now();
    let force = LAST_FORCED.with(|last| match last.get() {
        Some(t) if now.duration_since(t) < IDLE_INTERVAL => false,
        _ => {
            last.set(Some(now));
            true
        }
    });
    unsafe {
        pgrx::pg_sys::pgstat_report_stat(force);
    }
}

/// Let this transaction's commit return without waiting for its WAL to reach the disk:
/// `synchronous_commit = off` lets the WAL writer batch many transactions into one flush.
/// Scoped `SET LOCAL`, so it dies with the transaction and cannot leak into a caller's transaction.
pub fn relax_commit_durability() -> Result<(), pgrx::spi::Error> {
    pgrx::Spi::run("SET LOCAL synchronous_commit = off")
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
    LOCKING.with(|l| l.set(false));
    // Locks first, and inside the savepoint: a lock failure is an ordinary error the
    let result = atomically_coded(
        || {
            acquire_request_locks()?;
            f()
        },
        |message, code| {
            if held_lock(code) && !DID_WORK.with(|d| d.get()) {
                HandlerError::Busy(message.to_string())
            } else {
                HandlerError::Internal(format!("query aborted: {message}"))
            }
        },
    );
    // Also reset after a Rust-side error, which skips `aborted`.
    LOCKING.with(|l| l.set(false));
    if result.is_ok() {
        DID_WORK.with(|d| d.set(true));
    }
    result
}

/// `guarded` in its own transaction. SPI outside a transaction takes the postmaster down.
pub fn guarded_tx<T>(
    f: impl FnOnce() -> Result<T, HandlerError> + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
) -> Result<T, HandlerError> {
    pgrx::bgworkers::BackgroundWorker::transaction(|| guarded(f))
}

/// `contained` in its own transaction, for a body that touches no kafgres table.
pub fn contained_tx<T>(
    f: impl FnOnce() -> Result<T, HandlerError> + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
) -> Result<T, HandlerError> {
    pgrx::bgworkers::BackgroundWorker::transaction(|| contained(f))
}
