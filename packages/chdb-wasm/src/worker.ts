// Worker-side dispatcher. The Emscripten chdb module runs HERE, in the worker,
// so the caller's (main) thread is never blocked by a query. Works in both Node
// (worker_threads) and the browser (dedicated Worker).
//
// Protocol: receive WorkerRequest, run it synchronously against ChdbBindings,
// post back a WorkerResponse. The wasm module's own pthread pool is spawned from
// this worker, not the main thread.

import { ChdbBindings } from './bindings.ts';
import type { WorkerRequest, WorkerResponse } from './protocol.ts';

const isNode = typeof process !== 'undefined' && !!(process as any).versions?.node;

let post: (msg: WorkerResponse, transfer?: any[]) => void;
let listen: (cb: (msg: WorkerRequest) => void) => void;

if (isNode) {
  const { parentPort } = await import('node:worker_threads');
  post = (msg, transfer) => parentPort!.postMessage(msg, transfer as any);
  listen = (cb) => parentPort!.on('message', cb);
} else {
  const g: any = self;
  post = (msg, transfer) => g.postMessage(msg, transfer || []);
  listen = (cb) => {
    g.onmessage = (e: MessageEvent) => cb(e.data);
  };
}

let bindings: ChdbBindings | null = null;

/** Fetch a .wasm with byte progress (browser). Returns the bytes or null. */
async function fetchWithProgress(url: string, id: number): Promise<Uint8Array | null> {
  if (typeof fetch === 'undefined') return null;
  const resp = await fetch(url);
  // Fail fast on non-2xx: otherwise a 404/500 error page would be fed to Emscripten as
  // wasmBinary, producing a confusing instantiation failure instead of a clear error.
  if (!resp.ok) throw new Error(`failed to fetch wasm module ${url}: HTTP ${resp.status} ${resp.statusText}`);
  // OK but no streaming body (e.g. some runtimes): fall back to a single arrayBuffer read.
  if (!resp.body) return new Uint8Array(await resp.arrayBuffer());
  const total = Number(resp.headers.get('content-length') || 0);
  const reader = resp.body.getReader();
  const chunks: Uint8Array[] = [];
  let loaded = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    loaded += value.length;
    post({ id, event: 'progress', loaded, total });
  }
  const out = new Uint8Array(loaded);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

async function init(payload: { moduleUrl: string; wasmUrl?: string }, id: number): Promise<void> {
  let url = payload.moduleUrl;
  if (isNode && !/^[a-z]+:/i.test(url)) {
    const { pathToFileURL } = await import('node:url');
    url = pathToFileURL(url).href;
  }
  const factory = (await import(url)).default as (opts?: any) => Promise<any>;

  const moduleOpts: any = {};
  // Browser: pre-fetch the .wasm with progress and feed it to Emscripten.
  if (!isNode && payload.wasmUrl) {
    const bytes = await fetchWithProgress(payload.wasmUrl, id);
    if (bytes) moduleOpts.wasmBinary = bytes;
  }
  const mod = await factory(moduleOpts);
  bindings = new ChdbBindings(mod);
}

function requireBindings(): ChdbBindings {
  if (!bindings) throw new Error('chdb worker not initialized');
  return bindings;
}

async function handle(req: WorkerRequest): Promise<void> {
    try {
      let result: any;
      switch (req.type) {
        case 'init':
          await init(req.payload, req.id);
          // mt only: share the wasm Memory SAB + the cancel-flag and live-progress offsets.
          // The page sets the cancel flag (read by the C++ cancel check on any thread) and
          // polls the progress struct (written by the engine on any thread). A non-shared
          // heap (st build) => no cancel / no live progress.
          {
            const b = requireBindings();
            const sharedMem = b.heapBuffer;
            if (typeof SharedArrayBuffer !== 'undefined' && sharedMem instanceof SharedArrayBuffer) {
              result = { sharedMem, cancelAddr: b.cancelFlagAddr(), progressAddr: b.progressAddr() };
            }
          }
          break;
        case 'query':
          result = await requireBindings().query(req.payload.sql, req.payload.format);
          break;
        case 'connect':
          result = { conn: requireBindings().connect(req.payload?.path) };
          break;
        case 'closeConn':
          requireBindings().closeConn(req.payload.conn);
          break;
        case 'queryConn':
          result = await requireBindings().queryConn(req.payload.conn, req.payload.sql, req.payload.format);
          break;
        case 'streamStart':
          result = { stream: await requireBindings().streamStart(req.payload.conn, req.payload.sql, req.payload.format) };
          break;
        case 'streamFetch':
          result = await requireBindings().streamFetch(req.payload.conn, req.payload.stream);
          break;
        case 'streamCancel':
          requireBindings().streamCancel(req.payload.conn, req.payload.stream);
          break;
        case 'putFile':
          requireBindings().writeFile(req.payload.path, req.payload.data);
          break;
        case 'registerFile':
          requireBindings().registerFile(req.payload.name, req.payload.data);
          break;
        case 'unregisterFile':
          requireBindings().unregisterFile(req.payload.name);
          break;
        case 'clearFiles':
          requireBindings().clearFiles();
          break;
        case 'close':
          bindings = null;
          break;
        default:
          throw new Error('unknown request type: ' + (req as any).type);
      }
      // Transfer the result buffer (zero-copy) when present (query or stream chunk).
      const buf =
        result?.data instanceof Uint8Array
          ? result.data.buffer
          : result?.result?.data instanceof Uint8Array
            ? result.result.data.buffer
            : undefined;
      post({ id: req.id, ok: true, result }, buf ? [buf] : undefined);
    } catch (e: any) {
      post({ id: req.id, ok: false, error: e && e.message ? e.message : String(e) });
    }
}

// Strict FIFO. On a JSPI build a query suspends the wasm stack at an async
// fetch and control returns to this event loop, so a second request could
// otherwise re-enter the engine mid-query — the chdb C API is not reentrant.
// handle() never throws (it posts errors), so the chain cannot break.
// (Queries stay in-order on plain builds too, as before; cancel doesn't go
// through this queue — the page writes the shared cancel flag directly.)
let queue: Promise<void> = Promise.resolve();
listen((req: WorkerRequest) => {
  queue = queue.then(() => handle(req));
});
