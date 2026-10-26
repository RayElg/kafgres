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
    // `end`: the commit record's end LSN, which keys each mapping's delivered position.
    emit(
        ctx,
        &format!(r#"{{"v":2,"op":"B","xid":{},"end":{}}}"#, (*txn).xid, (*txn).end_lsn),
    );
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
    let table = name_bytes(&class.relname);

    // Not tidiness: under the table engine the log is a Postgres table, so every record we
    if table.starts_with(b"kafgres_") {
        return;
    }
    let ns = pg_sys::get_namespace_name(class.relnamespace);
    let schema: &[u8] = if ns.is_null() { b"" } else { std::ffi::CStr::from_ptr(ns).to_bytes() };

    let tp = (*change).data.tp;
    let cols_json = columns_json(desc);
    // `old` is present only when the table's REPLICA IDENTITY provides it; the default gives the key columns alone.
    let old_tuple = heap_tuple(tp.oldtuple);
    // Only FULL logs every column of the old tuple; under DEFAULT or INDEX a key change logs
    // the key columns and NULL for the rest.
    let full = class.relreplident as u8 == pg_sys::REPLICA_IDENTITY_FULL;
    let fallback = if full { old_tuple } else { std::ptr::null_mut() };
    let new_json = tuple_json(heap_tuple(tp.newtuple), desc, fallback);
    let old_json = tuple_json(old_tuple, desc, std::ptr::null_mut());

    // `ts` is the transaction's commit timestamp from the commit WAL record — always
    // present; `track_commit_timestamp` only backs pg_xact_commit_timestamp(). Microseconds.
    let mut line =
        format!(r#"{{"v":3,"op":"{op}","xid":{},"ts":{},"schema":"#, (*txn).xid, commit_time(txn))
            .into_bytes();
    json_bytes(&mut line, schema);
    line.extend_from_slice(br#","table":"#);
    json_bytes(&mut line, table);
    line.extend_from_slice(br#","cols":"#);
    line.extend_from_slice(&cols_json);
    line.extend_from_slice(br#","new":"#);
    line.extend_from_slice(&new_json);
    line.extend_from_slice(br#","old":"#);
    line.extend_from_slice(&old_json);
    line.push(b'}');
    emit(ctx, &line);
}

/// The relation's shape as of this change; `format_type_with_typemod` because `numeric(10,2)` and bare `numeric` share an OID.
unsafe fn columns_json(desc: pg_sys::TupleDesc) -> Vec<u8> {
    let tupdesc = pgrx::PgTupleDesc::from_pg_unchecked(desc);
    let mut out = vec![b'['];
    let mut first = true;
    for att in tupdesc.iter() {
        if att.attisdropped || att.attnum <= 0 {
            continue;
        }
        if !first {
            out.push(b',');
        }
        first = false;
        let ty = pg_sys::format_type_with_typemod(att.atttypid, att.atttypmod);
        let ty: &[u8] = if ty.is_null() { b"text" } else { std::ffi::CStr::from_ptr(ty).to_bytes() };
        out.push(b'[');
        json_bytes(&mut out, name_bytes(&att.attname));
        out.push(b',');
        json_bytes(&mut out, ty);
        out.push(b']');
    }
    out.push(b']');
    out
}

/// A tuple as `{"col":"text value"}`, or `null`. A column whose value the WAL does not carry
/// is taken from `fallback` when that has it, and otherwise left out of the object.
unsafe fn tuple_json(
    tuple: pg_sys::HeapTuple,
    desc: pg_sys::TupleDesc,
    fallback: pg_sys::HeapTuple,
) -> Vec<u8> {
    if tuple.is_null() {
        return b"null".to_vec();
    }
    let tupdesc = pgrx::PgTupleDesc::from_pg_unchecked(desc);
    let mut out = vec![b'{'];
    let mut first = true;

    for att in tupdesc.iter() {
        if att.attisdropped || att.attnum <= 0 {
            continue;
        }
        let mut is_null = false;
        let mut datum = pg_sys::heap_getattr(tuple, att.attnum as _, desc, &mut is_null);

        // An UPDATE that leaves a TOASTed column alone logs only its on-disk pointer. REPLICA
        // IDENTITY FULL logs the old tuple flattened; otherwise the column is left out.
        if !is_null && att.attlen == -1 && is_external_ondisk(datum) {
            if fallback.is_null() {
                continue;
            }
            datum = pg_sys::heap_getattr(fallback, att.attnum as _, desc, &mut is_null);
            if !is_null && is_external_ondisk(datum) {
                continue;
            }
        }

        if !first {
            out.push(b',');
        }
        first = false;
        json_bytes(&mut out, name_bytes(&att.attname));
        out.push(b':');

        if is_null {
            out.extend_from_slice(b"null");
            continue;
        }

        // The type's own output function; inferring from the JSON side would mangle types like `numeric`.
        let mut typoutput = pg_sys::Oid::INVALID;
        let mut typisvarlena = false;
        pg_sys::getTypeOutputInfo(att.atttypid, &mut typoutput, &mut typisvarlena);
        let cstr = pg_sys::OidOutputFunctionCall(typoutput, datum);
        json_bytes(&mut out, std::ffi::CStr::from_ptr(cstr).to_bytes());
    }
    out.push(b'}');
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
unsafe fn name_bytes(name: &pg_sys::NameData) -> &[u8] {
    std::ffi::CStr::from_ptr(name.data.as_ptr()).to_bytes()
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
/// Byte-wise, so text stays in the server encoding: every server encoding is ASCII-safe.
fn json_bytes(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'"');
    for &b in s {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b if b < 0x20 => out.extend_from_slice(format!("\\u{b:04x}").as_bytes()),
            b => out.push(b),
        }
    }
    out.push(b'"');
}

pub(crate) fn json_string(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len() + 2);
    json_bytes(&mut out, s.as_bytes());
    String::from_utf8(out).unwrap_or_default()
}

/// Text in the server encoding, as a textual output plugin must write.
unsafe fn emit(ctx: *mut pg_sys::LogicalDecodingContext, line: impl AsRef<[u8]>) {
    let line = line.as_ref();
    pg_sys::OutputPluginPrepareWrite(ctx, true);
    pg_sys::appendBinaryStringInfo((*ctx).out, line.as_ptr().cast(), line.len() as _);
    pg_sys::OutputPluginWrite(ctx, true);
}

#[cfg(test)]
mod tests {
    #[test]
    fn non_utf8_bytes_pass_through_and_only_json_specials_are_escaped() {
        let mut out = Vec::new();
        super::json_bytes(&mut out, b"caf\xe9 \"q\"\\\n\x01");
        assert_eq!(out, b"\"caf\xe9 \\\"q\\\"\\\\\\n\\u0001\"");
        assert_eq!(super::json_string("caf\u{e9}"), "\"caf\u{e9}\"");
    }
}
