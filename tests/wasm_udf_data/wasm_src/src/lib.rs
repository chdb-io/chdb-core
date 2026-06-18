//! Tiny, dependency-free chDB WASM UDF demo module (used by tests/test_wasm_udf.py).
//!
//! Imports NO host functions, so it loads on any chDB build with the WASM runtime
//! regardless of host-API revision. It exists only to prove the runtime is wired up,
//! exercising the two core UDF ABIs with one function each:
//!   * ROW_DIRECT scalar:  `wasm_add(i64, i64) -> i64`        (numeric scalar path)
//!   * BUFFERED_V1:        `count_chars(String) -> UInt64`    (String/block path),
//!     plus the required `clickhouse_create_buffer` / `clickhouse_destroy_buffer`.
//!
//! BUFFERED_V1 protocol (see src/Functions/UserDefined/UserDefinedWebAssembly.cpp):
//! the host serializes the argument columns (here serialization_format='RowBinary')
//! into a buffer and passes its handle + row count; we return a handle to the
//! RowBinary-serialized result column. A buffer handle points at an 8-byte
//! {ptr: u32, size: u32} header.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};

#[repr(C)]
struct WasmBuffer {
    ptr: u32,
    size: u32,
}

// ---- ROW_DIRECT (scalar) export ----

#[no_mangle]
pub extern "C" fn wasm_add(a: i64, b: i64) -> i64 {
    a + b
}

// ---- BUFFERED_V1 buffer management ----

#[no_mangle]
pub extern "C" fn clickhouse_create_buffer(size: u32) -> u32 {
    unsafe {
        let data = if size == 0 {
            4 as *mut u8
        } else {
            let layout = Layout::from_size_align(size as usize, 1).unwrap();
            let p = alloc(layout);
            if p.is_null() {
                handle_alloc_error(layout);
            }
            p
        };
        let header_layout = Layout::new::<WasmBuffer>();
        let header = alloc(header_layout) as *mut WasmBuffer;
        if header.is_null() {
            handle_alloc_error(header_layout);
        }
        (*header).ptr = data as u32;
        (*header).size = size;
        header as u32
    }
}

#[no_mangle]
pub extern "C" fn clickhouse_destroy_buffer(handle: u32) {
    if handle == 0 {
        return;
    }
    unsafe {
        let header = handle as *mut WasmBuffer;
        let size = (*header).size;
        if size != 0 {
            dealloc(
                (*header).ptr as *mut u8,
                Layout::from_size_align(size as usize, 1).unwrap(),
            );
        }
        dealloc(header as *mut u8, Layout::new::<WasmBuffer>());
    }
}

unsafe fn data_slice(handle: u32) -> &'static [u8] {
    let header = handle as *const WasmBuffer;
    std::slice::from_raw_parts((*header).ptr as *const u8, (*header).size as usize)
}

/// Read a LEB128 unsigned varint; returns (value, bytes_consumed).
fn read_varint(buf: &[u8], pos: usize) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut i = pos;
    loop {
        let byte = buf[i];
        result |= ((byte & 0x7f) as u64) << shift;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    (result, i - pos)
}

// ---- BUFFERED_V1 UDF: count_chars(String) -> UInt64 ----

#[no_mangle]
pub extern "C" fn count_chars(input_handle: u32, num_rows: u32) -> u32 {
    let input = unsafe { data_slice(input_handle) };

    let mut counts: Vec<u64> = Vec::with_capacity(num_rows as usize);
    let mut pos = 0usize;
    for _ in 0..num_rows {
        let (len, n) = read_varint(input, pos);
        pos += n;
        let s = std::str::from_utf8(&input[pos..pos + len as usize]).unwrap_or("");
        pos += len as usize;
        counts.push(s.chars().count() as u64);
    }

    let out_size = (num_rows as usize) * 8;
    let out_handle = clickhouse_create_buffer(out_size as u32);
    unsafe {
        let header = out_handle as *const WasmBuffer;
        let out = std::slice::from_raw_parts_mut((*header).ptr as *mut u8, out_size);
        for (i, c) in counts.iter().enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&c.to_le_bytes());
        }
    }
    out_handle
}
