import type { ConnHandle, WireResult, WireChunk } from './protocol.ts';
/** The Emscripten module instance (createChdbModule()'s resolved value). */
export type ChdbModule = any;
export declare class ChdbBindings {
    private readonly mod;
    constructor(mod: ChdbModule);
    /** Offset of the engine's cancel flag in wasm memory (page writes it via the heap SAB). */
    cancelFlagAddr(): number;
    /** Offset of the live-progress struct in wasm memory (page polls it via the heap SAB). */
    progressAddr(): number;
    /** The wasm linear memory buffer (a SharedArrayBuffer on the mt build). */
    get heapBuffer(): ArrayBufferLike;
    /** Query the implicit process-wide :memory: connection. */
    query(sql: string, format?: string): WireResult;
    /**
     * Write a file into the wasm in-memory filesystem (MEMFS), creating parent
     * directories as needed, so `file('/path', ...)` / `INFILE` can read it.
     * Requires the module built with FORCE_FILESYSTEM and FS in EXPORTED_RUNTIME_METHODS.
     */
    writeFile(path: string, data: Uint8Array): void;
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
    registerFile(name: string, data: Blob | Uint8Array): void;
    /**
     * Drop a previously registered file, releasing the held Blob. Idempotent:
     * removing a name that isn't registered is a no-op. After this, querying
     * `file('<name>')` errors as a missing file (same path as a never-registered name).
     */
    unregisterFile(name: string): void;
    /** Drop all registered files, releasing their Blobs. */
    clearFiles(): void;
    /** Open an explicit connection; returns an opaque handle. */
    connect(path?: string): ConnHandle;
    closeConn(conn: ConnHandle): void;
    /** Query a specific connection handle. */
    queryConn(conn: ConnHandle, sql: string, format?: string): WireResult;
    /** Begin a streaming query on a connection; returns the opaque stream handle. */
    streamStart(conn: ConnHandle, sql: string, format?: string): ConnHandle;
    /** Fetch the next chunk of a stream. done=true at end-of-stream (empty chunk). */
    streamFetch(conn: ConnHandle, stream: ConnHandle): WireChunk;
    /** Cancel an in-flight stream and free its handle. */
    streamCancel(conn: ConnHandle, stream: ConnHandle): void;
    /** Read a chdb_result*, copy its bytes out of the heap, then free it. */
    private consume;
}
//# sourceMappingURL=bindings.d.ts.map