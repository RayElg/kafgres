//! Reads on a snapshot taken for each query, which assign the caller's transaction no xid.
//!
//! They see everything committed so far plus the transaction's own writes, as the broker's
//! reads do. pgrx has no such read: `Spi::get_one*` assigns an xid even to a SELECT, and
//! `SpiClient::select` reads the statement's snapshot while the current subtransaction has no
//! xid. Under REPEATABLE READ every query gets the transaction's snapshot.

use std::marker::PhantomData;
use std::num::NonZeroUsize;

use pgrx::datum::{DatumWithOid, TryFromDatumError};
use pgrx::pg_sys;
use pgrx::spi::{Error, SpiErrorCodes};
use pgrx::{FromDatum, IntoDatum};

/// A column type these reads decode to. Owned only: a borrowed `&str` or `&[u8]` would point
/// into SPI memory that is freed before the caller sees it.
pub trait Owned: FromDatum + IntoDatum {}
impl Owned for bool {}
impl Owned for i32 {}
impl Owned for i64 {}
impl Owned for String {}
impl Owned for Vec<u8> {}

/// The first column of the first row; `None` for no rows or NULL.
pub fn one<T: Owned>(sql: &str, args: &[DatumWithOid<'_>]) -> Result<Option<T>, Error> {
    rows(sql, args, |mut rows| match rows.next() {
        Some(row) => row.get(1),
        None => Ok(None),
    })
}

/// Runs `sql` and hands its rows to `f`. They are freed when `f` returns.
pub fn rows<R>(
    sql: &str,
    args: &[DatumWithOid<'_>],
    f: impl FnOnce(Rows<'_>) -> Result<R, Error>,
) -> Result<R, Error> {
    let query = std::ffi::CString::new(sql).expect("SQL text contains no NUL");
    let mut types: Vec<pg_sys::Oid> = args.iter().map(|a| a.oid()).collect();
    let mut values: Vec<pg_sys::Datum> = args
        .iter()
        .map(|a| {
            a.datum()
                .map_or(pg_sys::Datum::from(0usize), |d| d.sans_lifetime())
        })
        .collect();
    let nulls: Vec<std::ffi::c_char> = args
        .iter()
        .map(|a| if a.datum().is_some() { b' ' } else { b'n' } as std::ffi::c_char)
        .collect();

    pgrx::Spi::connect(|_| {
        // SAFETY: SPI is connected for this closure, which outlives `f`'s use of the rows.
        // `read_only = false` makes SPI take a snapshot for this query.
        let (table, len) = unsafe {
            let status = pg_sys::SPI_execute_with_args(
                query.as_ptr(),
                args.len() as i32,
                types.as_mut_ptr(),
                values.as_mut_ptr(),
                nulls.as_ptr(),
                false,
                0,
            );
            if status < 0 {
                return Err(
                    SpiErrorCodes::try_from(status).map_or(Error::NoTupleTable, Error::SpiError)
                );
            }
            (pg_sys::SPI_tuptable, pg_sys::SPI_processed as usize)
        };
        if table.is_null() {
            return Err(Error::NoTupleTable);
        }
        f(Rows {
            table,
            len,
            next: 0,
            _spi: PhantomData,
        })
    })
}

/// The rows of one query, in order.
pub struct Rows<'spi> {
    table: *mut pg_sys::SPITupleTable,
    len: usize,
    next: usize,
    _spi: PhantomData<&'spi ()>,
}

impl<'spi> Iterator for Rows<'spi> {
    type Item = Row<'spi>;

    fn next(&mut self) -> Option<Row<'spi>> {
        if self.next == self.len {
            return None;
        }
        // SAFETY: `next` is in bounds, and the table lives as long as `'spi`.
        let row = unsafe {
            Row {
                tuple: *(*self.table).vals.add(self.next),
                desc: (*self.table).tupdesc,
                _spi: PhantomData,
            }
        };
        self.next += 1;
        Some(row)
    }
}

/// One row. Columns number from 1.
pub struct Row<'spi> {
    tuple: pg_sys::HeapTuple,
    desc: pg_sys::TupleDesc,
    _spi: PhantomData<&'spi ()>,
}

impl Row<'_> {
    /// Column `column` as `T`, or an error if the column is not of `T`'s type. Values are copied
    /// out of SPI's memory, so a long scan does not accumulate them in the transaction.
    pub fn get<T: Owned>(&self, column: usize) -> Result<Option<T>, Error> {
        // SAFETY: the tuple and descriptor are SPI's and live as long as `'spi`; `column` is
        // checked against the descriptor before either is read.
        unsafe {
            if column == 0 || column > (*self.desc).natts as usize {
                let n = NonZeroUsize::new(column).unwrap_or(NonZeroUsize::MIN);
                return Err(TryFromDatumError::NoSuchAttributeNumber(n).into());
            }
            let column = column as i32;
            let mut null = false;
            let datum = pg_sys::SPI_getbinval(self.tuple, self.desc, column, &mut null);
            Ok(T::try_from_datum(
                datum,
                null,
                pg_sys::SPI_gettypeid(self.desc, column),
            )?)
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn reads_assign_no_xid() {
        assert_eq!(super::one::<i64>("SELECT $1", &[7i64.into()]), Ok(Some(7)));
        let no_xid = super::one::<bool>("SELECT pg_current_xact_id_if_assigned() IS NULL", &[]);
        assert_eq!(no_xid, Ok(Some(true)));
    }

    /// Even in a subtransaction with no xid, where `SpiClient::select` misses them.
    #[pg_test]
    fn reads_see_the_transactions_own_writes() {
        Spi::run("CREATE TEMP TABLE fresh_own_writes (x int)").unwrap();
        Spi::run("INSERT INTO fresh_own_writes VALUES (1), (2)").unwrap();
        let count = crate::dbtx::released(|| {
            super::one::<i64>("SELECT count(*) FROM fresh_own_writes", &[])
        });
        assert_eq!(count, Ok(Some(2)));
    }

    #[pg_test]
    fn a_column_of_another_type_is_an_error() {
        assert!(super::one::<i64>("SELECT 'x'::text", &[]).is_err());
        assert!(super::rows("SELECT 1::bigint", &[], |mut r| r
            .next()
            .unwrap()
            .get::<i64>(2))
        .is_err());
    }
}
