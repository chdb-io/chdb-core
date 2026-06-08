// Low-level synchronous bindings over the Emscripten module. Runs INSIDE the
// worker. Wraps the flat C surface from programs/wasm/chdb_wasm.cpp and hides
// the Memory64 detail that pointers cross ccall as BigInt.

import { ChdbError } from './status.ts';
import type { ConnHandle, WireResult, WireChunk } from './protocol.ts';

/** The Emscripten module instance (createChdbModule()'s resolved value). */
export type ChdbModule = any;

/** Normalize a pointer/length that may be a BigInt (Memory64) to a Number. */
const num = (x: number | bigint): number => (typeof x === 'bigint' ? Number(x) : x);

export class ChdbBindings {
  private readonly mod: ChdbModule;

  constructor(mod: ChdbModule) {
    this.mod = mod;
  }

  /** Query the implicit process-wide :memory: connection. */
  query(sql: string, format = 'CSV'): WireResult {
    const r = this.mod.ccall('chdb_wasm_query', 'number', ['string', 'string'], [sql, format]);
    return this.consume(r, sql);
  }

  /**
   * Write a file into the wasm in-memory filesystem (MEMFS), creating parent
   * directories as needed, so `file('/path', ...)` / `INFILE` can read it.
   * Requires the module built with FORCE_FILESYSTEM and FS in EXPORTED_RUNTIME_METHODS.
   */
  writeFile(path: string, data: Uint8Array): void {
    const FS = this.mod.FS;
    if (!FS) throw new ChdbError('FS is not available in this build (need EXPORTED_RUNTIME_METHODS=FS)');
    const slash = path.lastIndexOf('/');
    if (slash > 0) {
      let cur = '';
      for (const part of path.slice(0, slash).split('/')) {
        if (!part) continue;
        cur += '/' + part;
        try { FS.mkdir(cur); } catch { /* already exists */ }
      }
    }
    FS.writeFile(path, data);
  }

  /**
   * Mount a File/Blob at `path` via WORKERFS, WITHOUT copying its bytes into the
   * wasm heap: reads are lazy, on-demand byte ranges of the Blob (FileReaderSync),
   * so `file('<path>', ...)` can scan a large local file (e.g. from <input type=file>)
   * while only the bytes actually read are pulled in. One file per mount directory;
   * re-mounting the same directory replaces it.
   *
   * SINGLE-THREADED (st) BUNDLE ONLY. WORKERFS reads the Blob from the JS scope of
   * the thread that mounted it; on the threaded (mt) bundle the query reads run on a
   * separate pthread/Worker that cannot see that Blob, which would trap the module.
   * On the mt bundle this refuses cleanly — use putFile() there instead.
   * Requires the module built with `-lworkerfs.js` and WORKERFS in EXPORTED_RUNTIME_METHODS.
   */
  mountFile(path: string, data: Blob | Uint8Array): void {
    const FS = this.mod.FS;
    const WORKERFS = this.mod.WORKERFS;
    if (!FS || !WORKERFS) throw new ChdbError('WORKERFS is not available in this build (need EXPORTED_RUNTIME_METHODS=WORKERFS, -lworkerfs.js)');
    // Threaded build = SharedArrayBuffer-backed memory. Reads would run on a pthread
    // that cannot access this Blob -> module trap. Refuse before mounting.
    if (typeof SharedArrayBuffer !== 'undefined' && this.mod.HEAPU8.buffer instanceof SharedArrayBuffer)
      throw new ChdbError('mountFile (lazy WORKERFS mount) is supported only on the single-threaded bundle; use putFile() on the threaded (cross-origin-isolated) bundle');
    const slash = path.lastIndexOf('/');
    const dir = slash > 0 ? path.slice(0, slash) : '/';
    const name = path.slice(slash + 1);
    if (!name) throw new ChdbError('mountFile: path must end in a file name: ' + path);
    let cur = '';
    for (const part of dir.split('/')) {
      if (!part) continue;
      cur += '/' + part;
      try { FS.mkdir(cur); } catch { /* already exists */ }
    }
    // Re-mountable: drop any previous WORKERFS mount at this directory.
    try { FS.unmount(dir); } catch { /* not a mount point */ }
    const blob = data instanceof Uint8Array ? new Blob([data]) : data;
    FS.mount(WORKERFS, { blobs: [{ name, data: blob }] }, dir);
  }

  /** Open an explicit connection; returns an opaque handle. */
  connect(path?: string): ConnHandle {
    return this.mod.ccall('chdb_wasm_connect', 'number', ['string'], [path ?? '']);
  }

  closeConn(conn: ConnHandle): void {
    this.mod.ccall('chdb_wasm_close_conn', null, ['number'], [conn]);
  }

  /** Query a specific connection handle. */
  queryConn(conn: ConnHandle, sql: string, format = 'CSV'): WireResult {
    const r = this.mod.ccall('chdb_wasm_query_conn', 'number', ['number', 'string', 'string'], [conn, sql, format]);
    return this.consume(r, sql);
  }

  /** Begin a streaming query on a connection; returns the opaque stream handle. */
  streamStart(conn: ConnHandle, sql: string, format = 'CSV'): ConnHandle {
    return this.mod.ccall('chdb_wasm_stream_start', 'number', ['number', 'string', 'string'], [conn, sql, format]);
  }

  /** Fetch the next chunk of a stream. done=true at end-of-stream (empty chunk). */
  streamFetch(conn: ConnHandle, stream: ConnHandle): WireChunk {
    const chunk = this.mod.ccall('chdb_wasm_stream_fetch', 'number', ['number', 'number'], [conn, stream]);
    if (!num(chunk)) return { done: true };
    try {
      const errPtr = this.mod.ccall('chdb_wasm_result_error', 'number', ['number'], [chunk]);
      const err = num(errPtr) ? this.mod.UTF8ToString(num(errPtr)) : '';
      if (err) throw new ChdbError(err);
      const bufPtr = num(this.mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [chunk]));
      const len = num(this.mod.ccall('chdb_wasm_result_length', 'number', ['number'], [chunk]));
      if (len === 0) return { done: true };
      const data: Uint8Array = this.mod.HEAPU8.slice(bufPtr, bufPtr + len);
      const rowsRead = num(this.mod.ccall('chdb_wasm_result_rows_read', 'number', ['number'], [chunk]));
      const bytesRead = num(this.mod.ccall('chdb_wasm_result_bytes_read', 'number', ['number'], [chunk]));
      const elapsedSeconds = this.mod.ccall('chdb_wasm_result_elapsed', 'number', ['number'], [chunk]);
      return { done: false, result: { data, rowsRead, bytesRead, elapsedSeconds } };
    } finally {
      this.mod.ccall('chdb_wasm_free_result', null, ['number'], [chunk]);
    }
  }

  /** Cancel an in-flight stream and free its handle. */
  streamCancel(conn: ConnHandle, stream: ConnHandle): void {
    this.mod.ccall('chdb_wasm_stream_cancel', null, ['number', 'number'], [conn, stream]);
    this.mod.ccall('chdb_wasm_free_result', null, ['number'], [stream]);
  }

  /** Read a chdb_result*, copy its bytes out of the heap, then free it. */
  private consume(r: number | bigint, sql: string): WireResult {
    if (!num(r)) throw new ChdbError('chdb returned a null result', sql);
    try {
      const errPtr = this.mod.ccall('chdb_wasm_result_error', 'number', ['number'], [r]);
      const err = num(errPtr) ? this.mod.UTF8ToString(num(errPtr)) : '';
      if (err) throw new ChdbError(err, sql);

      const bufPtr = num(this.mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [r]));
      const len = num(this.mod.ccall('chdb_wasm_result_length', 'number', ['number'], [r]));
      // Copy out of the (shared/growable) heap before freeing the result.
      const data: Uint8Array = bufPtr ? this.mod.HEAPU8.slice(bufPtr, bufPtr + len) : new Uint8Array(0);

      const rowsRead = num(this.mod.ccall('chdb_wasm_result_rows_read', 'number', ['number'], [r]));
      const bytesRead = num(this.mod.ccall('chdb_wasm_result_bytes_read', 'number', ['number'], [r]));
      const elapsedSeconds = this.mod.ccall('chdb_wasm_result_elapsed', 'number', ['number'], [r]);
      return { data, rowsRead, bytesRead, elapsedSeconds };
    } finally {
      this.mod.ccall('chdb_wasm_free_result', null, ['number'], [r]);
    }
  }
}
