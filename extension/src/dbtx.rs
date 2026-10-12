//! Database plumbing for the request path. A Postgres `ERROR` inside a background worker is

use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::handlers::HandlerError;

/// Take every table lock a request needs, up front, waiting only out of [`LOCK_WAIT_BUDGET`].
/// Vacuum truncation holds ACCESS EXCLUSIVE and yields only to a waiter, so `NOWAIT` alone
/// fails every request for its duration. The budget is the loop's, not the request's: once
/// spent, locks use `NOWAIT` until the window ends, so no run of holders can stall the loop
/// for longer. `lock_timeout` is per relation, so one statement can overrun by its table count.
fn acquire_request_locks() -> Result<(), pgrx::spi::Error> {
    let budget = lock_wait_left();
    let started = Instant::now();
    let mut timeout_set = false;
    // A lock failure is an ERROR, so it skips the reset below; `guarded` charges it then.
    LOCKING_SINCE.with(|l| l.set(Some(started)));
    let locked = (|| -> Result<(), pgrx::spi::Error> {
        let locks: [fn(LockWait) -> Result<(), pgrx::spi::Error>; 5] = [
            crate::meta::lock_for_read,
            crate::group::lock_for_read,
            crate::acl::lock_for_read,
            crate::producer::lock_for_read,
            crate::storage::lock_for_read,
        ];
        for lock in locks {
            // Until something waits, the budget is all left: set it once, not per statement.
            let waited = started.elapsed();
            let left = budget.saturating_sub(waited);
            if left < MIN_LOCK_WAIT {
                lock(LockWait::NoWait)?;
                continue;
            }
            if !timeout_set || waited >= MIN_LOCK_WAIT {
                let ms = left.as_millis();
                pgrx::Spi::run(&format!("SET LOCAL lock_timeout = '{ms}ms'"))?;
                timeout_set = true;
            }
            lock(LockWait::Bounded)?;
        }
        Ok(())
    })();
    LOCKING_SINCE.with(|l| l.set(None));
    charge_lock_wait(started.elapsed());
    locked?;
    // Row-lock backstop. Deliberately not tiny: a producer waiting on another producer's
    pgrx::Spi::run("SET LOCAL lock_timeout = '2s'")?;
    pgrx::Spi::run("SET LOCAL statement_timeout = '5s'")
}

/// Table-lock waiting allowed per [`LOCK_WAIT_WINDOW`]. Outlasts a vacuum truncation, which
/// checks for waiters every 20 ms.
const LOCK_WAIT_BUDGET: Duration = Duration::from_millis(250);
const LOCK_WAIT_WINDOW: Duration = Duration::from_secs(5);
/// A lock phase shorter than this is not charged, so an uncontended phase that is merely slow
/// (many `kafgres_log` partitions, a loaded backend) cannot drain the budget.
const MIN_LOCK_WAIT: Duration = Duration::from_millis(10);

thread_local! {
    /// Start of the current window and the waiting charged to it.
    static LOCK_WAITED: Cell<Option<(Instant, Duration)>> = const { Cell::new(None) };
    /// When the in-progress lock phase began.
    static LOCKING_SINCE: Cell<Option<Instant>> = const { Cell::new(None) };
}

fn lock_wait_left() -> Duration {
    match LOCK_WAITED.with(|w| w.get()) {
        Some((start, spent)) if start.elapsed() < LOCK_WAIT_WINDOW => {
            LOCK_WAIT_BUDGET.saturating_sub(spent)
        }
        _ => LOCK_WAIT_BUDGET,
    }
}

fn charge_lock_wait(waited: Duration) {
    if waited < MIN_LOCK_WAIT {
        return;
    }
    LOCK_WAITED.with(|w| {
        w.set(Some(match w.get() {
            Some((start, spent)) if start.elapsed() < LOCK_WAIT_WINDOW => (start, spent + waited),
            _ => (Instant::now(), waited),
        }))
    });
}

/// Whether a request's table locks wait (briefly) or fail at once.
#[derive(Clone, Copy)]
pub enum LockWait {
    Bounded,
    NoWait,
}

impl LockWait {
    /// ACCESS SHARE on `tables`, a comma-separated list.
    pub fn lock(self, tables: &str) -> Result<(), pgrx::spi::Error> {
        let nowait = match self {
            LockWait::Bounded => "",
            LockWait::NoWait => " NOWAIT",
        };
        pgrx::Spi::run(&format!("LOCK TABLE {tables} IN ACCESS SHARE MODE{nowait}"))
    }
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
            // Log what actually happened before substituting the caller's error: `aborted`
            pgrx::log!("kafgres: subtransaction aborted: {caught:?}");
            let (message, code) = match &caught {
                pgrx::pg_sys::panic::CaughtError::PostgresError(e)
                | pgrx::pg_sys::panic::CaughtError::ErrorReport(e)
                | pgrx::pg_sys::panic::CaughtError::RustPanic { ereport: e, .. } => {
                    (e.message().to_string(), e.sql_error_code())
                }
            };
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
    // Locks first, and inside the savepoint: a lock failure is an ordinary error the
    atomically(
        || {
            acquire_request_locks()?;
            f()
        },
        |message| {
            if let Some(since) = LOCKING_SINCE.with(|l| l.replace(None)) {
                charge_lock_wait(since.elapsed());
            }
            HandlerError::Internal(format!("query aborted: {message}"))
        },
    )
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
