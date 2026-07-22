// Main-thread async client: the wasm runs in a worker and queries return Promises,
// so the calling thread never blocks.
import { ChdbError } from "./status.js";
const isNode = typeof process !== 'undefined' && !!process.versions?.node;
function wrap(r) {
    return {
        data: r.data,
        rowsRead: r.rowsRead,
        bytesRead: r.bytesRead,
        scannedRows: r.scannedRows,
        scannedBytes: r.scannedBytes,
        elapsedSeconds: r.elapsedSeconds,
        text: () => new TextDecoder().decode(r.data),
    };
}
export class AsyncChdb {
    worker;
    nextId = 1;
    pending = new Map();
    onProgress;
    /** mt-only views into the shared wasm memory (set by create() from init). */
    cancelFlag;
    progressView;
    constructor(worker, onProgress) {
        this.worker = worker;
        this.onProgress = onProgress;
    }
    /** Spawn the worker, instantiate the wasm module, and return a ready client. */
    static async create(opts) {
        // Resolve the sibling worker for both source-run (.ts via Node type-strip) and built dist (.js).
        const ext = import.meta.url.endsWith('.ts') ? '.ts' : '.js';
        const workerUrl = opts.workerUrl ?? new URL('./worker' + ext, import.meta.url).href;
        let worker;
        if (isNode) {
            const { Worker } = await import('node:worker_threads');
            worker = new Worker(new URL(workerUrl));
        }
        else {
            worker = new globalThis.Worker(workerUrl, { type: 'module' });
        }
        const self = new AsyncChdb(worker, opts.onProgress);
        self.attach();
        const initResult = await self.request('init', { moduleUrl: opts.moduleUrl, wasmUrl: opts.wasmUrl });
        if (initResult?.sharedMem) {
            self.cancelFlag = new Int32Array(initResult.sharedMem, initResult.cancelAddr, 1);
            // 6 × int64: [seq, readRows, totalRowsToRead, readBytes, totalBytesToRead, elapsedNs].
            self.progressView = new BigInt64Array(initResult.sharedMem, initResult.progressAddr, 6);
        }
        return self;
    }
    attach() {
        const handler = (data) => this.onMessage(data);
        if (isNode)
            this.worker.on('message', handler);
        else
            this.worker.onmessage = (e) => handler(e.data);
    }
    onMessage(msg) {
        if ('event' in msg && msg.event === 'progress') {
            this.onProgress?.(msg.loaded, msg.total);
            return;
        }
        const p = this.pending.get(msg.id);
        if (!p)
            return;
        this.pending.delete(msg.id);
        if ('ok' in msg && msg.ok)
            p.resolve(msg.result);
        else
            p.reject(new ChdbError('error' in msg ? msg.error : 'unknown worker error'));
    }
    request(type, payload, transfer) {
        // Clear a stale cancel flag when a new query starts.
        if (this.cancelFlag && (type === 'query' || type === 'queryConn' || type === 'streamStart'))
            Atomics.store(this.cancelFlag, 0, 0);
        const id = this.nextId++;
        return new Promise((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            this.worker.postMessage({ id, type, payload }, transfer ?? []);
        });
    }
    /** Snapshot the shared progress struct, rejecting a torn read via the seq guard. */
    readProgress(v) {
        const snap = () => ({
            readRows: Number(v[1]), totalRowsToRead: Number(v[2]),
            readBytes: Number(v[3]), totalBytesToRead: Number(v[4]),
            elapsedNs: Number(v[5]),
        });
        for (let i = 0; i < 4; i++) {
            const s1 = Atomics.load(v, 0);
            const p = snap();
            if (Atomics.load(v, 0) === s1)
                return p; // seq unchanged -> consistent snapshot
        }
        // Writes are ~ms apart; after a few retries take the (cosmetically-fine) latest read.
        return snap();
    }
    /**
     * Poll the shared progress struct (mt build) while a query runs, invoking cb whenever the
     * engine advances it. The worker thread blocks in the synchronous query ccall, but THIS
     * (main) thread's event loop is free, and the engine writes progress from whatever thread
     * runs the pipeline — so polling shared memory here is the only path that sees intermediate
     * progress. No-op on the st build (no shared memory). Returns a stop() that clears the
     * timer and does one final catch-up read (so the last tick is seen).
     * @internal
     */
    _startProgressPoll(cb) {
        const view = this.progressView;
        if (!view)
            return () => { };
        let lastSeq = Number(Atomics.load(view, 0));
        const tick = () => {
            const seq = Number(Atomics.load(view, 0));
            if (seq === lastSeq)
                return;
            lastSeq = seq;
            cb(this.readProgress(view));
        };
        const timer = setInterval(tick, 80);
        return () => { clearInterval(timer); tick(); };
    }
    /** Run a query on the implicit :memory: connection. opts.onProgress gets live execution progress. */
    async query(sql, format = 'CSV', opts) {
        const stop = opts?.onProgress ? this._startProgressPoll(opts.onProgress) : () => { };
        try {
            return wrap(await this.request('query', { sql, format }));
        }
        finally {
            stop();
        }
    }
    /**
     * Request cancellation of the currently-running query (mt bundle only): the in-flight
     * query()/queryStream() rejects with a cancellation error. Throws on the single-threaded
     * build, which runs queries synchronously and cannot be interrupted mid-query.
     */
    cancel() {
        if (!this.cancelFlag)
            throw new ChdbError('query cancellation is only supported on the multi-threaded (mt) build');
        Atomics.store(this.cancelFlag, 0, 1);
    }
    /**
     * Write a file into the wasm in-memory filesystem so it can be queried with
     * `file('<path>', ...)` (or read via `INFILE`). `data` is transferred zero-copy.
     * Example: await db.putFile('/data.csv', bytes); await db.query("SELECT * FROM file('/data.csv','CSV')")
     */
    async putFile(path, data) {
        await this.request('putFile', { path, data }, [data.buffer]);
    }
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
    async registerFile(name, data) {
        // Browser-only: the lazy reader uses FileReaderSync (a Web Worker API) unavailable in
        // Node, where it would otherwise fail opaquely at first read. Fail fast with guidance.
        if (isNode)
            throw new ChdbError('registerFile() is browser-only (it reads the Blob with FileReaderSync, unavailable in Node). ' +
                'In Node, use putFile() with a path instead.');
        const transfer = data instanceof Uint8Array ? [data.buffer] : undefined;
        await this.request('registerFile', { name, data }, transfer);
    }
    /**
     * Drop a file registered with registerFile(), releasing the held Blob. Idempotent:
     * dropping a name that isn't registered resolves without error. Afterwards
     * `file('<name>')` errors as a missing file.
     */
    async unregisterFile(name) {
        await this.request('unregisterFile', { name });
    }
    /** Drop all files registered with registerFile(), releasing their Blobs. */
    async clearFiles() {
        await this.request('clearFiles');
    }
    /** Open an explicit connection (path defaults to in-memory). */
    async connect(path) {
        const { conn } = await this.request('connect', { path });
        return new AsyncChdbConnection(this, conn);
    }
    /** @internal */
    _queryConn(conn, sql, format) {
        return this.request('queryConn', { conn, sql, format });
    }
    /** @internal */
    _closeConn(conn) {
        return this.request('closeConn', { conn });
    }
    /** @internal */
    _streamStart(conn, sql, format) {
        return this.request('streamStart', { conn, sql, format });
    }
    /** @internal */
    _streamFetch(conn, stream) {
        return this.request('streamFetch', { conn, stream });
    }
    /** @internal */
    _streamCancel(conn, stream) {
        return this.request('streamCancel', { conn, stream });
    }
    /** Tear down the worker (and the wasm instance running in it). */
    async terminate() {
        try {
            await this.request('close');
        }
        catch {
            /* ignore */
        }
        if (isNode)
            await this.worker.terminate();
        else
            this.worker.terminate();
    }
}
export class AsyncChdbConnection {
    db;
    conn;
    constructor(db, conn) {
        this.db = db;
        this.conn = conn;
    }
    async query(sql, format = 'CSV', opts) {
        const stop = opts?.onProgress ? this.db._startProgressPoll(opts.onProgress) : () => { };
        try {
            return wrap(await this.db._queryConn(this.conn, sql, format));
        }
        finally {
            stop();
        }
    }
    /** Cancel the running query (mt only). See AsyncChdb.cancel(). */
    cancel() {
        this.db.cancel();
    }
    /** Stream a query's results chunk-by-chunk (yields the worker between chunks). */
    async *queryStream(sql, format = 'CSV', opts) {
        const { stream } = await this.db._streamStart(this.conn, sql, format);
        const stop = opts?.onProgress ? this.db._startProgressPoll(opts.onProgress) : () => { };
        try {
            for (;;) {
                const { done, result } = await this.db._streamFetch(this.conn, stream);
                if (done || !result)
                    break;
                yield wrap(result);
            }
        }
        finally {
            stop();
            await this.db._streamCancel(this.conn, stream);
        }
    }
    async close() {
        await this.db._closeConn(this.conn);
    }
}
//# sourceMappingURL=async.js.map