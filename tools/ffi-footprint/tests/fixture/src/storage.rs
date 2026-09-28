use pgrx::pg_sys;

static SLOTS: [i32; 4] = [0; 4];

pub fn sync_unhandled() {
    unsafe {
        pg_sys::FileSync(1, 0);
    }
}

pub fn close_handled() {
    // SAFETY: the file is open and owned by this backend.
    unsafe { pg_sys::FileClose(1) };
}

pub fn release(n: i32) {
    let stripes = n.clamp(1, 4);
    let _safe = 8 % stripes;
    let _unsafe = 8 / n;
    let _ = SLOTS[n as usize];
}
