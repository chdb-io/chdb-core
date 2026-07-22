// Low-level synchronous bindings over the Emscripten module. Runs INSIDE the
// worker. Wraps the flat C surface from programs/wasm/chdb_wasm.cpp and hides
// the Memory64 detail that pointers cross ccall as BigInt.
import { ChdbError } from "./status.js";
/** Normalize a pointer/length that may be a BigInt (Memory64) to a Number. */
const num = (x) => (typeof x === 'bigint' ? Number(x) : x);
export class ChdbBindings {
    mod;
    constructor(mod) {
        this.mod = mod;
    }
    /** Offset of the engine's cancel flag in wasm memory (page writes it via the heap SAB). */
    cancelFlagAddr() {
        return num(this.mod.ccall('chdb_wasm_cancel_flag_addr', 'number', [], []));
    }
    /** Offset of the live-progress struct in wasm memory (page polls it via the heap SAB). */
    progressAddr() {
        return num(this.mod.ccall('chdb_wasm_progress_addr', 'number', [], []));
    }
    /** The wasm linear memory buffer (a SharedArrayBuffer on the mt build). */
    get heapBuffer() {
        return this.mod.HEAPU8.buffer;
    }
    /** Query the implicit process-wide :memory: connection. */
    query(sql, format = 'CSV') {
        const r = this.mod.ccall('chdb_wasm_query', 'number', ['string', 'string'], [sql, format]);
        return this.consume(r, sql);
    }
    /**
     * Write a file into the wasm in-memory filesystem (MEMFS), creating parent
     * directories as needed, so `file('/path', ...)` / `INFILE` can read it.
     * Requires the module built with FORCE_FILESYSTEM and FS in EXPORTED_RUNTIME_METHODS.
     */
    writeFile(path, data) {
        const FS = this.mod.FS;
        if (!FS)
            throw new ChdbError('FS is not available in this build (need EXPORTED_RUNTIME_METHODS=FS)');
        const slash = path.lastIndexOf('/');
        if (slash > 0) {
            let cur = '';
            for (const part of path.slice(0, slash).split('/')) {
                if (!part)
                    continue;
                cur += '/' + part;
                try {
                    FS.mkdir(cur);
                }
                catch { /* already exists */ }
            }
        }
        FS.writeFile(path, data);
    }
    /**
     * Register a File/Blob (by name) for lazy reading via `file('<name>', ...)`,
     * WITHOUT copying its bytes into the wasm heap: ReadBufferFromJSFile reads byte
     * ranges on demand (Blob.slice + FileReaderSync). Ideal for large local files from
     * `<input type=file>`.
     *
     * This runs on the module's main runtime thread (a Web Worker), so the handle is
     * stored in that thread's globalThis.__CHDB_FILES. On the threaded bundle a query's
     * read executes on a pool pthread (a separate Worker that can't see this Worker's JS
     * objects); ReadBufferFromJSFile handles that by reading via MAIN_THREAD_EM_ASM, which
     * proxies the read back to this thread — so a single registry here serves both bundles.
     */
    registerFile(name, data) {
        const blob = data instanceof Uint8Array ? new Blob([data]) : data;
        const g = globalThis;
        (g.__CHDB_FILES ??= new Map()).set(name, blob);
    }
    /**
     * Drop a previously registered file, releasing the held Blob. Idempotent:
     * removing a name that isn't registered is a no-op. After this, querying
     * `file('<name>')` errors as a missing file (same path as a never-registered name).
     */
    unregisterFile(name) {
        globalThis.__CHDB_FILES?.delete(name);
    }
    /** Drop all registered files, releasing their Blobs. */
    clearFiles() {
        globalThis.__CHDB_FILES?.clear();
    }
    /** Open an explicit connection; returns an opaque handle. */
    connect(path) {
        return this.mod.ccall('chdb_wasm_connect', 'number', ['string'], [path ?? '']);
    }
    closeConn(conn) {
        this.mod.ccall('chdb_wasm_close_conn', null, ['number'], [conn]);
    }
    /** Query a specific connection handle. */
    queryConn(conn, sql, format = 'CSV') {
        const r = this.mod.ccall('chdb_wasm_query_conn', 'number', ['number', 'string', 'string'], [conn, sql, format]);
        return this.consume(r, sql);
    }
    /** Begin a streaming query on a connection; returns the opaque stream handle. */
    streamStart(conn, sql, format = 'CSV') {
        return this.mod.ccall('chdb_wasm_stream_start', 'number', ['number', 'string', 'string'], [conn, sql, format]);
    }
    /** Fetch the next chunk of a stream. done=true at end-of-stream (empty chunk). */
    streamFetch(conn, stream) {
        const chunk = this.mod.ccall('chdb_wasm_stream_fetch', 'number', ['number', 'number'], [conn, stream]);
        if (!num(chunk))
            return { done: true };
        try {
            const errPtr = this.mod.ccall('chdb_wasm_result_error', 'number', ['number'], [chunk]);
            const err = num(errPtr) ? this.mod.UTF8ToString(num(errPtr)) : '';
            if (err)
                throw new ChdbError(err);
            const bufPtr = num(this.mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [chunk]));
            const len = num(this.mod.ccall('chdb_wasm_result_length', 'number', ['number'], [chunk]));
            if (len === 0)
                return { done: true };
            const data = this.mod.HEAPU8.slice(bufPtr, bufPtr + len);
            const rowsRead = num(this.mod.ccall('chdb_wasm_result_rows_read', 'number', ['number'], [chunk]));
            const bytesRead = num(this.mod.ccall('chdb_wasm_result_bytes_read', 'number', ['number'], [chunk]));
            const scannedRows = num(this.mod.ccall('chdb_wasm_result_scanned_rows', 'number', ['number'], [chunk]));
            const scannedBytes = num(this.mod.ccall('chdb_wasm_result_scanned_bytes', 'number', ['number'], [chunk]));
            const elapsedSeconds = this.mod.ccall('chdb_wasm_result_elapsed', 'number', ['number'], [chunk]);
            return { done: false, result: { data, rowsRead, bytesRead, scannedRows, scannedBytes, elapsedSeconds } };
        }
        finally {
            this.mod.ccall('chdb_wasm_free_result', null, ['number'], [chunk]);
        }
    }
    /** Cancel an in-flight stream and free its handle. */
    streamCancel(conn, stream) {
        this.mod.ccall('chdb_wasm_stream_cancel', null, ['number', 'number'], [conn, stream]);
        this.mod.ccall('chdb_wasm_free_result', null, ['number'], [stream]);
    }
    /** Read a chdb_result*, copy its bytes out of the heap, then free it. */
    consume(r, sql) {
        if (!num(r))
            throw new ChdbError('chdb returned a null result', sql);
        try {
            const errPtr = this.mod.ccall('chdb_wasm_result_error', 'number', ['number'], [r]);
            const err = num(errPtr) ? this.mod.UTF8ToString(num(errPtr)) : '';
            if (err)
                throw new ChdbError(err, sql);
            const bufPtr = num(this.mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [r]));
            const len = num(this.mod.ccall('chdb_wasm_result_length', 'number', ['number'], [r]));
            // Copy out of the (shared/growable) heap before freeing the result.
            const data = bufPtr ? this.mod.HEAPU8.slice(bufPtr, bufPtr + len) : new Uint8Array(0);
            const rowsRead = num(this.mod.ccall('chdb_wasm_result_rows_read', 'number', ['number'], [r]));
            const bytesRead = num(this.mod.ccall('chdb_wasm_result_bytes_read', 'number', ['number'], [r]));
            const scannedRows = num(this.mod.ccall('chdb_wasm_result_scanned_rows', 'number', ['number'], [r]));
            const scannedBytes = num(this.mod.ccall('chdb_wasm_result_scanned_bytes', 'number', ['number'], [r]));
            const elapsedSeconds = this.mod.ccall('chdb_wasm_result_elapsed', 'number', ['number'], [r]);
            return { data, rowsRead, bytesRead, scannedRows, scannedBytes, elapsedSeconds };
        }
        finally {
            this.mod.ccall('chdb_wasm_free_result', null, ['number'], [r]);
        }
    }
}
//# sourceMappingURL=bindings.js.map