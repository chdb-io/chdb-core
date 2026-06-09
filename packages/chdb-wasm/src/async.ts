// Main-thread async client. Mirrors duckdb-wasm's AsyncDuckDB: the wasm runs in
// a worker and queries return Promises, so the calling thread never blocks.

import { ChdbError } from './status.ts';
import type { ConnHandle, RequestType, WireResult, WorkerResponse } from './protocol.ts';

const isNode = typeof process !== 'undefined' && !!(process as any).versions?.node;

export interface ChdbResult {
  /** Raw result bytes (CSV/JSON/… per the requested format). */
  data: Uint8Array;
  rowsRead: number;
  bytesRead: number;
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

interface Pending {
  resolve: (v: any) => void;
  reject: (e: any) => void;
}

function wrap(r: WireResult): ChdbResult {
  return {
    data: r.data,
    rowsRead: r.rowsRead,
    bytesRead: r.bytesRead,
    elapsedSeconds: r.elapsedSeconds,
    text: () => new TextDecoder().decode(r.data),
  };
}

export class AsyncChdb {
  private worker: any;
  private nextId = 1;
  private readonly pending = new Map<number, Pending>();
  private onProgress?: (loaded: number, total: number) => void;

  private constructor(worker: any, onProgress?: (loaded: number, total: number) => void) {
    this.worker = worker;
    this.onProgress = onProgress;
  }

  /** Spawn the worker, instantiate the wasm module, and return a ready client. */
  static async create(opts: CreateOptions): Promise<AsyncChdb> {
    // Resolve the sibling worker for both source-run (.ts via Node type-strip) and built dist (.js).
    const ext = import.meta.url.endsWith('.ts') ? '.ts' : '.js';
    const workerUrl = opts.workerUrl ?? new URL('./worker' + ext, import.meta.url).href;
    let worker: any;
    if (isNode) {
      const { Worker } = await import('node:worker_threads');
      worker = new Worker(new URL(workerUrl));
    } else {
      worker = new (globalThis as any).Worker(workerUrl, { type: 'module' });
    }

    const self = new AsyncChdb(worker, opts.onProgress);
    self.attach();
    await self.request('init', { moduleUrl: opts.moduleUrl, wasmUrl: opts.wasmUrl });
    return self;
  }

  private attach(): void {
    const handler = (data: WorkerResponse) => this.onMessage(data);
    if (isNode) this.worker.on('message', handler);
    else this.worker.onmessage = (e: MessageEvent) => handler(e.data);
  }

  private onMessage(msg: WorkerResponse): void {
    if ('event' in msg && msg.event === 'progress') {
      this.onProgress?.(msg.loaded, msg.total);
      return;
    }
    const p = this.pending.get(msg.id);
    if (!p) return;
    this.pending.delete(msg.id);
    if ('ok' in msg && msg.ok) p.resolve(msg.result);
    else p.reject(new ChdbError('error' in msg ? msg.error : 'unknown worker error'));
  }

  private request(type: RequestType, payload?: any, transfer?: Transferable[]): Promise<any> {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      (this.worker as any).postMessage({ id, type, payload }, transfer ?? []);
    });
  }

  /** Run a query on the implicit :memory: connection. */
  async query(sql: string, format = 'CSV'): Promise<ChdbResult> {
    return wrap(await this.request('query', { sql, format }));
  }

  /**
   * Write a file into the wasm in-memory filesystem so it can be queried with
   * `file('<path>', ...)` (or read via `INFILE`). `data` is transferred zero-copy.
   * Example: await db.putFile('/data.csv', bytes); await db.query("SELECT * FROM file('/data.csv','CSV')")
   */
  async putFile(path: string, data: Uint8Array): Promise<void> {
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
  async registerFile(name: string, data: Blob | Uint8Array): Promise<void> {
    // Browser-only: the lazy reader uses FileReaderSync (a Web Worker API) unavailable in
    // Node, where it would otherwise fail opaquely at first read. Fail fast with guidance.
    if (isNode)
      throw new ChdbError(
        'registerFile() is browser-only (it reads the Blob with FileReaderSync, unavailable in Node). ' +
        'In Node, use putFile() with a path instead.');
    const transfer = data instanceof Uint8Array ? [data.buffer] : undefined;
    await this.request('registerFile', { name, data }, transfer);
  }

  /**
   * Drop a file registered with registerFile(), releasing the held Blob. Idempotent:
   * dropping a name that isn't registered resolves without error. Afterwards
   * `file('<name>')` errors as a missing file.
   */
  async unregisterFile(name: string): Promise<void> {
    await this.request('unregisterFile', { name });
  }

  /** Drop all files registered with registerFile(), releasing their Blobs. */
  async clearFiles(): Promise<void> {
    await this.request('clearFiles');
  }

  /** Open an explicit connection (path defaults to in-memory). */
  async connect(path?: string): Promise<AsyncChdbConnection> {
    const { conn } = await this.request('connect', { path });
    return new AsyncChdbConnection(this, conn);
  }

  /** @internal */
  _queryConn(conn: ConnHandle, sql: string, format: string): Promise<WireResult> {
    return this.request('queryConn', { conn, sql, format });
  }
  /** @internal */
  _closeConn(conn: ConnHandle): Promise<void> {
    return this.request('closeConn', { conn });
  }
  /** @internal */
  _streamStart(conn: ConnHandle, sql: string, format: string): Promise<{ stream: ConnHandle }> {
    return this.request('streamStart', { conn, sql, format });
  }
  /** @internal */
  _streamFetch(conn: ConnHandle, stream: ConnHandle): Promise<{ done: boolean; result?: WireResult }> {
    return this.request('streamFetch', { conn, stream });
  }
  /** @internal */
  _streamCancel(conn: ConnHandle, stream: ConnHandle): Promise<void> {
    return this.request('streamCancel', { conn, stream });
  }

  /** Tear down the worker (and the wasm instance running in it). */
  async terminate(): Promise<void> {
    try {
      await this.request('close');
    } catch {
      /* ignore */
    }
    if (isNode) await this.worker.terminate();
    else this.worker.terminate();
  }
}

export class AsyncChdbConnection {
  private readonly db: AsyncChdb;
  private readonly conn: ConnHandle;
  constructor(db: AsyncChdb, conn: ConnHandle) {
    this.db = db;
    this.conn = conn;
  }
  async query(sql: string, format = 'CSV'): Promise<ChdbResult> {
    return wrap(await this.db._queryConn(this.conn, sql, format));
  }

  /** Stream a query's results chunk-by-chunk (yields the worker between chunks). */
  async *queryStream(sql: string, format = 'CSV'): AsyncGenerator<ChdbResult> {
    const { stream } = await this.db._streamStart(this.conn, sql, format);
    try {
      for (;;) {
        const { done, result } = await this.db._streamFetch(this.conn, stream);
        if (done || !result) break;
        yield wrap(result);
      }
    } finally {
      await this.db._streamCancel(this.conn, stream);
    }
  }

  async close(): Promise<void> {
    await this.db._closeConn(this.conn);
  }
}
