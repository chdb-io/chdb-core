import type { ConnHandle, WireResult, QueryProgress } from './protocol.ts';
export type { QueryProgress } from './protocol.ts';
export interface ChdbResult {
    /** Raw result bytes (CSV/JSON/… per the requested format). */
    data: Uint8Array;
    /** Result size: rows/bytes RETURNED (e.g. 1 for `SELECT count()`), not source rows scanned. */
    rowsRead: number;
    bytesRead: number;
    /** Source rows/bytes SCANNED from storage (ClickHouse read_rows) — for "Processed N rows, …". */
    scannedRows: number;
    scannedBytes: number;
    elapsedSeconds: number;
    /** Decode the bytes as UTF-8 text. */
    text(): string;
}
export interface CreateOptions {
    /** URL/path of the Emscripten module (chdb.mjs). */
    moduleUrl: string;
    /** Optional explicit .wasm URL (browser progress fetch); defaults to module-relative. */
    wasmUrl?: string;
    /** URL/path of the worker entry (defaults to the bundled worker). */
    workerUrl?: string;
    /** Progress callback during instantiation (browser). */
    onProgress?: (loaded: number, total: number) => void;
}
/** Per-call options for query()/queryStream(). */
export interface QueryOptions {
    /** Live execution-progress callback (mt bundle; throttled ~100 ms in the engine). */
    onProgress?: (p: QueryProgress) => void;
}
export declare class AsyncChdb {
    private worker;
    private nextId;
    private readonly pending;
    private onProgress?;
    /** mt-only views into the shared wasm memory (set by create() from init). */
    private cancelFlag?;
    private progressView?;
    private constructor();
    /** Spawn the worker, instantiate the wasm module, and return a ready client. */
    static create(opts: CreateOptions): Promise<AsyncChdb>;
    private attach;
    private onMessage;
    private request;
    /** Snapshot the shared progress struct, rejecting a torn read via the seq guard. */
    private readProgress;
    /**
     * Poll the shared progress struct (mt build) while a query runs, invoking cb whenever the
     * engine advances it. The worker thread blocks in the synchronous query ccall, but THIS
     * (main) thread's event loop is free, and the engine writes progress from whatever thread
     * runs the pipeline — so polling shared memory here is the only path that sees intermediate
     * progress. No-op on the st build (no shared memory). Returns a stop() that clears the
     * timer and does one final catch-up read (so the last tick is seen).
     * @internal
     */
    _startProgressPoll(cb: (p: QueryProgress) => void): () => void;
    /** Run a query on the implicit :memory: connection. opts.onProgress gets live execution progress. */
    query(sql: string, format?: string, opts?: QueryOptions): Promise<ChdbResult>;
    /**
     * Request cancellation of the currently-running query (mt bundle only): the in-flight
     * query()/queryStream() rejects with a cancellation error. Throws on the single-threaded
     * build, which runs queries synchronously and cannot be interrupted mid-query.
     */
    cancel(): void;
    /**
     * Write a file into the wasm in-memory filesystem so it can be queried with
     * `file('<path>', ...)` (or read via `INFILE`). `data` is transferred zero-copy.
     * Example: await db.putFile('/data.csv', bytes); await db.query("SELECT * FROM file('/data.csv','CSV')")
     */
    putFile(path: string, data: Uint8Array): Promise<void>;
    /**
     * Register a File/Blob for lazy, no-copy reading via `file('<name>', ...)`:
     * only the byte ranges actually read are pulled in (Blob.slice + FileReaderSync) —
     * ideal for large local files picked via `<input type=file>`. Browser only: it uses
     * FileReaderSync (a Web Worker API); under Node, use putFile() instead. A `Blob`/`File` is
     * passed by reference (no byte copy across the worker boundary); a `Uint8Array`
     * is transferred zero-copy. Then reference it by the same name in SQL — exactly
     * like a native path, no prefix. Works on both bundles (on the threaded bundle the
     * read is proxied to the runtime thread that holds the handle). Example:
     *   await db.registerFile('data.parquet', fileInput.files[0]);
     *   await db.query("SELECT count() FROM file('data.parquet','Parquet')")
     */
    registerFile(name: string, data: Blob | Uint8Array): Promise<void>;
    /**
     * Drop a file registered with registerFile(), releasing the held Blob. Idempotent:
     * dropping a name that isn't registered resolves without error. Afterwards
     * `file('<name>')` errors as a missing file.
     */
    unregisterFile(name: string): Promise<void>;
    /** Drop all files registered with registerFile(), releasing their Blobs. */
    clearFiles(): Promise<void>;
    /** Open an explicit connection (path defaults to in-memory). */
    connect(path?: string): Promise<AsyncChdbConnection>;
    /** @internal */
    _queryConn(conn: ConnHandle, sql: string, format: string): Promise<WireResult>;
    /** @internal */
    _closeConn(conn: ConnHandle): Promise<void>;
    /** @internal */
    _streamStart(conn: ConnHandle, sql: string, format: string): Promise<{
        stream: ConnHandle;
    }>;
    /** @internal */
    _streamFetch(conn: ConnHandle, stream: ConnHandle): Promise<{
        done: boolean;
        result?: WireResult;
    }>;
    /** @internal */
    _streamCancel(conn: ConnHandle, stream: ConnHandle): Promise<void>;
    /** Tear down the worker (and the wasm instance running in it). */
    terminate(): Promise<void>;
}
export declare class AsyncChdbConnection {
    private readonly db;
    private readonly conn;
    constructor(db: AsyncChdb, conn: ConnHandle);
    query(sql: string, format?: string, opts?: QueryOptions): Promise<ChdbResult>;
    /** Cancel the running query (mt only). See AsyncChdb.cancel(). */
    cancel(): void;
    /** Stream a query's results chunk-by-chunk (yields the worker between chunks). */
    queryStream(sql: string, format?: string, opts?: QueryOptions): AsyncGenerator<ChdbResult>;
    close(): Promise<void>;
}
//# sourceMappingURL=async.d.ts.map