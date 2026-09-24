//! Logical decoding output plugin; the plugin name must be `kafgres` because Postgres loads an output plugin by library name.

use pgrx::pg_guard;
use pgrx::pg_sys;

// Every callback carries `#[pg_guard]`: Postgres calls them from C frames that cannot
// catch a Rust unwind, so a panic (or a pgrx-reraised Postgres ERROR) would otherwise
// abort the process. The guard turns it back into an `ereport(ERROR)` instead.

/// `#[no_mangle]` and this exact name are the contract — there is no registration step.
#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn _PG_output_plugin_init(cb: *mut pg_sys::OutputPluginCallbacks) {
    let cb = &mut *cb;
    cb.startup_cb = Some(startup);
    cb.begin_cb = Some(begin);
    cb.change_cb = Some(change);
    cb.commit_cb = Some(commit);
    cb.shutdown_cb = Some(shutdown);
}

#[pg_guard]
unsafe extern "C-unwind" fn startup(
    _ctx: *mut pg_sys::LogicalDecodingContext,
    opt: *mut pg_sys::OutputPluginOptions,
    _is_init: bool,
) {
    (*opt).output_type = pg_sys::OutputPluginOutputType::OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    (*opt).receive_rewrites = false;
}

#[pg_guard]
unsafe extern "C-unwind" fn shutdown(_ctx: *mut pg_sys::LogicalDecodingContext) {}

/// Transaction boundaries are emitted so a consumer can group changes by commit.
#[pg_guard]
unsafe extern "C-unwind" fn begin(
    ctx: *mut pg_sys::LogicalDecodingContext,
    txn: *mut pg_sys::ReorderBufferTXN,
) {
    emit(ctx, &format!(r#"{{"v":2,"op":"B","xid":{}}}"#, (*txn).xid));
}

#[pg_guard]
unsafe extern "C-unwind" fn commit(
    ctx: *mut pg_sys::LogicalDecodingContext,
    txn: *mut pg_sys::ReorderBufferTXN,
    _lsn: pg_sys::XLogRecPtr,
) {
    emit(ctx, &format!(r#"{{"v":2,"op":"C","xid":{}}}"#, (*txn).xid));
}

#[pg_guard]
unsafe extern "C-unwind" fn change(
    ctx: *mut pg_sys::LogicalDecodingContext,
    txn: *mut pg_sys::ReorderBufferTXN,
    relation: pg_sys::Relation,
    change: *mut pg_sys::ReorderBufferChange,
) {
    use pg_sys::ReorderBufferChangeType as A;

    let op = match (*change).action {
        A::REORDER_BUFFER_CHANGE_INSERT => "I",
        A::REORDER_BUFFER_CHANGE_UPDATE => "U",
        A::REORDER_BUFFER_CHANGE_DELETE => "D",
        _ => return,
    };

    let rel = &*relation;
    let desc = rel.rd_att;
    let class = &*rel.rd_rel;
    let table = name_lossy(&class.relname);

    // Not tidiness: under the table engine the log is a Postgres table, so every record we
    if table.starts_with("kafgres_") {
        return;
    }
    let schema = {
        let ns = pg_sys::get_namespace_name(class.relnamespace);
        if ns.is_null() {
            String::new()
        } else {
            std::ffi::CStr::from_ptr(ns).to_string_lossy().into_owned()
        }
    };

    let tp = (*change).data.tp;
    let cols_json = columns_json(desc);
    let new_json = tuple_json(heap_tuple(tp.newtuple), desc);
    // `old` is present only when the table's REPLICA IDENTITY provides it; the default gives the key columns alone.
    let old_json = tuple_json(heap_tuple(tp.oldtuple), desc);

    // `ts` is the transaction's commit timestamp from the commit WAL record — always
    // present; `track_commit_timestamp` only backs pg_xact_commit_timestamp(). Microseconds.
    emit(
        ctx,
        &format!(
            r#"{{"v":3,"op":"{op}","xid":{},"ts":{},"schema":{},"table":{},"cols":{cols_json},"new":{new_json},"old":{old_json}}}"#,
            (*txn).xid,
            commit_time(txn),
            json_string(&schema),
            json_string(&table)
        ),
    );
}

/// The relation's shape as of this change; `format_type_with_typemod` because `numeric(10,2)` and bare `numeric` share an OID.
unsafe fn columns_json(desc: pg_sys::TupleDesc) -> String {
    let tupdesc = pgrx::PgTupleDesc::from_pg_unchecked(desc);
    let mut out = String::from("[");
    let mut first = true;
    for att in tupdesc.iter() {
        if att.attisdropped || att.attnum <= 0 {
            continue;
        }
        if !first {
            out.push(',');
        }
        first = false;
        let name = name_lossy(&att.attname);
        let ty = pg_sys::format_type_with_typemod(att.atttypid, att.atttypmod);
        let ty = if ty.is_null() {
            "text".to_string()
        } else {
            std::ffi::CStr::from_ptr(ty).to_string_lossy().into_owned()
        };
        out.push('[');
        out.push_str(&json_string(&name));
        out.push(',');
        out.push_str(&json_string(&ty));
        out.push(']');
    }
    out.push(']');
    out
}

/// A tuple as `{"col":"text value"}`, or `null`.
unsafe fn tuple_json(tuple: pg_sys::HeapTuple, desc: pg_sys::TupleDesc) -> String {
    if tuple.is_null() {
        return "null".to_string();
    }
    let tupdesc = pgrx::PgTupleDesc::from_pg_unchecked(desc);
    let mut out = String::from("{");
    let mut first = true;

    for att in tupdesc.iter() {
        if att.attisdropped || att.attnum <= 0 {
            continue;
        }
        let name = name_lossy(&att.attname);

        let mut is_null = false;
        let datum = pg_sys::heap_getattr(tuple, att.attnum as _, desc, &mut is_null);

        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&json_string(&name));
        out.push(':');

        if is_null {
            out.push_str("null");
            continue;
        }

        // The type's own output function; inferring from the JSON side would mangle types like `numeric`.
        let mut typoutput = pg_sys::Oid::INVALID;
        let mut typisvarlena = false;
        pg_sys::getTypeOutputInfo(att.atttypid, &mut typoutput, &mut typisvarlena);

        // An unchanged TOASTed value is not in the WAL — an UPDATE logs only an external pointer,
        if typisvarlena && is_external_ondisk(datum) {
            out.push_str("null");
            continue;
        }

        let cstr = pg_sys::OidOutputFunctionCall(typoutput, datum);
        let text = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
        out.push_str(&json_string(&text));
    }
    out.push('}');
    out
}

/// The commit timestamp. From PG 15 it is in the `xact_time` union with the prepare time.
#[cfg(any(feature = "pg13", feature = "pg14"))]
unsafe fn commit_time(txn: *mut pg_sys::ReorderBufferTXN) -> pg_sys::TimestampTz {
    (*txn).commit_time
}

#[cfg(not(any(feature = "pg13", feature = "pg14")))]
unsafe fn commit_time(txn: *mut pg_sys::ReorderBufferTXN) -> pg_sys::TimestampTz {
    (*txn).xact_time.commit_time
}

/// A decoded change's tuple. Through PG 16 it is wrapped in a `ReorderBufferTupleBuf`; from
/// PG 17 it is a plain `HeapTuple`. Null when the change has no tuple on that side.
#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
unsafe fn heap_tuple(buf: *mut pg_sys::ReorderBufferTupleBuf) -> pg_sys::HeapTuple {
    if buf.is_null() {
        std::ptr::null_mut()
    } else {
        &mut (*buf).tuple
    }
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
unsafe fn heap_tuple(tuple: pg_sys::HeapTuple) -> pg_sys::HeapTuple {
    tuple
}

/// Identifiers are bytes in the database encoding, not UTF-8; `pgrx::name_data_to_str` unwraps
/// the conversion, and a `LATIN1` table name would fail every decode of that transaction forever.
unsafe fn name_lossy(name: &pg_sys::NameData) -> String {
    std::ffi::CStr::from_ptr(name.data.as_ptr()).to_string_lossy().into_owned()
}

/// `VARATT_IS_EXTERNAL_ONDISK` is a C macro, open-coded: `va_header == 0x01` (little-endian) and `va_tag == VARTAG_ONDISK`.
unsafe fn is_external_ondisk(datum: pg_sys::Datum) -> bool {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return false;
    }
    *ptr == 0x01 && *ptr.add(1) as u32 == pg_sys::vartag_external::VARTAG_ONDISK
}

/// Hand-rolled: a panic inside a decoding callback takes down the walsender. Escapes exactly what RFC 8259 requires.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

unsafe fn emit(ctx: *mut pg_sys::LogicalDecodingContext, line: &str) {
    pg_sys::OutputPluginPrepareWrite(ctx, true);
    let c = std::ffi::CString::new(line).unwrap_or_default();
    pg_sys::appendStringInfoString((*ctx).out, c.as_ptr());
    pg_sys::OutputPluginWrite(ctx, true);
}
