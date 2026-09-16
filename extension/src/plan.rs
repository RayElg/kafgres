//! Cached SPI plans for the statements on the request path. `Spi::run_with_args` and
//! `Spi::get_one_with_args` build a one-shot plan per call, and the request path repeats the
//! same statement text every time. `SPI_keepplan` (pgrx's `PreparedStatement::keep`) moves a
//! plan into the cache memory context; Postgres revalidates it against catalog invalidation,
//! so DDL is handled by the plan cache.

use std::cell::RefCell;
use std::collections::HashMap;

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{Error, OwnedPreparedStatement, SpiClient, SpiTupleTable};

thread_local! {
    /// Keyed by statement text, which is `&'static str` at every call site. Process-local
    /// and never evicted: the statement set is fixed at compile time. Boxed so the address
    /// survives a rehash while `with_plan` holds a pointer across a nested SPI call.
    static PLANS: RefCell<HashMap<&'static str, Box<OwnedPreparedStatement>>> =
        RefCell::new(HashMap::new());
}

/// Whether the plan may write; a read-only statement prepared as mutating fails on a standby.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Read,
    Write,
}

fn with_plan<R>(
    sql: &'static str,
    kind: Kind,
    args: &[DatumWithOid<'_>],
    f: impl FnOnce(&mut SpiClient<'_>, &OwnedPreparedStatement) -> Result<R, Error>,
) -> Result<R, Error> {
    Spi::connect_mut(|client| {
        // The borrow is released before the statement runs: a Postgres ERROR unwinds by
        // longjmp and skips `Ref`'s destructor, so a borrow held across execution would leak
        // and the next `borrow_mut` from this backend would panic.
        let stmt: *const OwnedPreparedStatement =
            PLANS.with(|cell| -> Result<*const OwnedPreparedStatement, Error> {
                if let Some(existing) = cell.borrow().get(sql) {
                    return Ok(&**existing as *const _);
                }
                let types: Vec<PgOid> = args.iter().map(|a| PgOid::from(a.oid())).collect();
                let prepared = match kind {
                    Kind::Read => client.prepare(sql, &types)?,
                    Kind::Write => client.prepare_mut(sql, &types)?,
                }
                .keep();
                let mut plans = cell.borrow_mut();
                let boxed = plans.entry(sql).or_insert_with(|| Box::new(prepared));
                Ok(&**boxed as *const _)
            })?;

        // SAFETY: the pointee is a `Box` in a thread-local map never removed from, so the
        // allocation outlives this call and the address is stable across any rehash; Postgres
        // backends are single-threaded.
        f(client, unsafe { &*stmt })
    })
}

/// `Spi::run_with_args` against a plan prepared once per process.
pub fn run(sql: &'static str, args: &[DatumWithOid<'_>]) -> Result<(), Error> {
    with_plan(sql, Kind::Write, args, |client, stmt| {
        client.update(stmt, None, args).map(|_| ())
    })
}

/// `Spi::get_one_with_args` against a plan prepared once per process.
pub fn get_one<A: FromDatum + IntoDatum>(
    sql: &'static str,
    args: &[DatumWithOid<'_>],
) -> Result<Option<A>, Error> {
    with_plan(sql, Kind::Read, args, |client, stmt| {
        client.select(stmt, Some(1), args)?.first().get_one()
    })
}

/// A multi-row read against a plan prepared once per process.
pub fn select<R>(
    sql: &'static str,
    args: &[DatumWithOid<'_>],
    f: impl FnOnce(SpiTupleTable<'_>) -> Result<R, Error>,
) -> Result<R, Error> {
    with_plan(sql, Kind::Read, args, |client, stmt| {
        f(client.select(stmt, None, args)?)
    })
}
