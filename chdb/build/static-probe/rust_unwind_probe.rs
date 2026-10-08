// Host-runtime consumer probe, Rust half: the chdb-io/chdb-rust#53 reproducer.
//
// Linked like chdb-rust's `static` feature (`-l static=chdb -l stdc++`). Rust std on Linux
// unwinds with libgcc_s; when the static link bound part of std's _Unwind_* references to
// the LLVM libunwind bundled in libchdb.a, every backtrace crashed and every panic aborted
// with "failed to initiate panic, error 3".

use std::ffi::{c_char, c_int, CString};
use std::os::raw::c_void;

#[allow(non_camel_case_types)]
type chdb_connection = *mut c_void;

extern "C" {
    fn chdb_set_signal_handlers_enabled(enabled: c_int);
    fn chdb_connect(argc: c_int, argv: *mut *mut c_char) -> *mut chdb_connection;
    fn chdb_close_conn(conn: *mut chdb_connection);
    fn chdb_query(conn: chdb_connection, query: *const c_char, format: *const c_char) -> *mut c_void;
    fn chdb_result_error(result: *mut c_void) -> *const c_char;
    fn chdb_destroy_query_result(result: *mut c_void);
}

fn backtrace_works(when: &str) -> bool {
    let rendered = std::backtrace::Backtrace::force_capture().to_string();
    let ok = rendered.contains("rust_unwind_probe");
    if !ok {
        eprintln!("rust_unwind_probe ({when}): backtrace does not show this program:\n{rendered}");
    }
    ok
}

fn panics_unwind(when: &str) -> bool {
    let ok = std::panic::catch_unwind(|| panic!("probe")).is_err();
    if !ok {
        eprintln!("rust_unwind_probe ({when}): catch_unwind did not catch the panic");
    }
    ok
}

fn main() {
    // Keep the expected panic quiet; a real failure aborts or crashes regardless.
    std::panic::set_hook(Box::new(|_| {}));
    unsafe { chdb_set_signal_handlers_enabled(0) };

    let program = CString::new("rust_unwind_probe").unwrap();
    let mut argv = [program.as_ptr() as *mut c_char];
    let conn = unsafe { chdb_connect(1, argv.as_mut_ptr()) };
    if conn.is_null() {
        eprintln!("rust_unwind_probe: chdb_connect returned NULL");
        std::process::exit(1);
    }

    let query = CString::new("SELECT * FROM rust_unwind_probe_missing_table").unwrap();
    let format = CString::new("CSV").unwrap();
    let query_failed = unsafe {
        let result = chdb_query(*conn, query.as_ptr(), format.as_ptr());
        let failed = !result.is_null() && !chdb_result_error(result).is_null();
        chdb_destroy_query_result(result);
        failed
    };

    let ok = query_failed
        && backtrace_works("with a connection open")
        && panics_unwind("with a connection open");
    unsafe { chdb_close_conn(conn) };

    if !query_failed {
        eprintln!("rust_unwind_probe: the failing query did not report an error");
    }
    if !ok {
        std::process::exit(1);
    }
    println!("rust_unwind_probe: backtraces and panics work after a chDB exception");
}
