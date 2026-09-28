// Fixture for test_ffi_footprint.py: one instance of each pattern the tool classifies.
// It is parsed, never compiled.
use pgrx::pg_sys;
use pgrx::prelude::*;

mod storage;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_int_guc(c"fx.n", c"", c"", &N, 1, 10, GucContext::Sighup, GucFlags::default());
    BackgroundWorkerBuilder::new("fx").set_function("fx_worker_main").set_library("fx").load();
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn fx_worker_main(_arg: pg_sys::Datum) {
    storage::sync_unhandled();
    guarded(|| storage::close_handled());
}

#[no_mangle]
pub extern "C" fn fx_unguarded() {}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn _PG_output_plugin_init(cb: *mut pg_sys::OutputPluginCallbacks) {
    let cb = &mut *cb;
    cb.startup_cb = Some(startup);
}

unsafe extern "C-unwind" fn startup(_ctx: *mut pg_sys::LogicalDecodingContext) {}

fn with_sub<T>(f: impl FnOnce() -> T) -> T {
    PgTryBuilder::new(f).catch_others(|_| panic!()).execute()
}

pub fn guarded<T>(f: impl FnOnce() -> T) -> T {
    with_sub(|| f())
}

#[pg_extern]
fn fx_produce(n: i32) -> i32 {
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, move || {
        storage::release(n);
    });
    n
}

#[cfg(feature = "pg17")]
fn only_on_17() {}

#[cfg(not(feature = "pg17"))]
fn not_on_17() {}
