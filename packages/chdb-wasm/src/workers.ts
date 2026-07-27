// Cloudflare Workers entry: drives the Emscripten module DIRECTLY. workerd has
// no Worker API, so the worker-thread dispatcher (worker.ts) and AsyncChdb
// cannot run there; this wraps everything a Worker needs instead:
//
//   * instantiateWasm from a statically-imported, deploy-time-compiled
//     WebAssembly.Module (workerd forbids compiling wasm from bytes)
//   * locateFile passthrough (workerd module names are not URLs; the glue's
//     deferred-name computation must stay off the URL parser)
//   * the Memory64 ccall protocol + result copy-out/free (via ChdbBindings)
//   * a strict FIFO mutex over ALL engine calls: one isolate serves concurrent
//     requests, and on the JSPI build a query SUSPENDS at fetch() and frees
//     the event loop — without the mutex a second request would re-enter the
//     non-reentrant chdb C API mid-query
//
//   import createChdbModule from 'chdb-cloudflare/chdb.mjs';
//   import wasmModule from 'chdb-cloudflare/chdb.wasm';
//   import { createChdb } from 'chdb-cloudflare/workers';
//
//   const db = await createChdb(createChdbModule, wasmModule);
//   const r = await db.query("SELECT * FROM url('https://…/x.csv', CSVWithNames)");
//   return new Response(r.text());
//
// The entry is runtime-agnostic on purpose (nothing Workers-specific beyond
// the constraints above), so the same code path is testable under Node with
// --experimental-wasm-jspi — which is how CI covers it.

import { ChdbBindings } from './bindings.ts';
import type { ChdbResult } from './async.ts';
import type { ConnHandle, WireResult } from './protocol.ts';

export type { ChdbResult };

/** A session (explicit connection): DDL/DML state persists across its queries. */
export interface ChdbWorkersSession {
  query(sql: string, format?: string): Promise<ChdbResult>;
  /** Stream a large result chunk by chunk (each chunk is one engine block batch). */
  queryStream(sql: string, format?: string): AsyncGenerator<ChdbResult>;
  close(): Promise<void>;
}

export interface ChdbWorkers {
  /** Query the implicit process-wide connection. */
  query(sql: string, format?: string): Promise<ChdbResult>;
  /** Write bytes into the in-memory filesystem for `file('/path', …)` reads. */
  putFile(path: string, data: Uint8Array): Promise<void>;
  /** Open a session; remember to close() it (sessions hold engine state). */
  connect(path?: string): Promise<ChdbWorkersSession>;
  /** The underlying Emscripten module — escape hatch for advanced use. */
  readonly module: any;
}

function wrap(r: WireResult): ChdbResult {
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

/**
 * Instantiate chdb from a pre-compiled wasm module and return the query API.
 *
 * @param moduleFactory the glue's default export (`import createChdbModule
 *   from 'chdb-cloudflare/chdb.mjs'`)
 * @param wasmModule the statically-imported wasm (`import wasmModule from
 *   'chdb-cloudflare/chdb.wasm'`)
 * @param moduleOverrides extra Emscripten Module options, merged last
 */
export async function createChdb(
  moduleFactory: (opts?: Record<string, unknown>) => Promise<any>,
  wasmModule: WebAssembly.Module,
  moduleOverrides: Record<string, unknown> = {},
): Promise<ChdbWorkers> {
  const mod = await moduleFactory({
    locateFile: (path: string) => path,
    instantiateWasm(imports: WebAssembly.Imports, cb: (inst: WebAssembly.Instance, mod?: WebAssembly.Module) => void) {
      const instance = new WebAssembly.Instance(wasmModule, imports);
      cb(instance, wasmModule);
      return instance.exports;
    },
    ...moduleOverrides,
  });
  const bindings = new ChdbBindings(mod);

  // Strict FIFO over every engine call (queries, session ops, stream fetches):
  // handlers never throw out of the chain, so it cannot break.
  let chain: Promise<unknown> = Promise.resolve();
  const locked = <T>(op: () => Promise<T> | T): Promise<T> => {
    const p = chain.then(op);
    chain = p.then(
      () => undefined,
      () => undefined,
    );
    return p;
  };

  const makeSession = (conn: ConnHandle): ChdbWorkersSession => ({
    query: (sql, format = 'CSV') => locked(async () => wrap(await bindings.queryConn(conn, sql, format))),
    async *queryStream(sql, format = 'CSV') {
      const stream = await locked(() => bindings.streamStart(conn, sql, format));
      try {
        for (;;) {
          const chunk = await locked(() => bindings.streamFetch(conn, stream));
          if (chunk.done || !chunk.result) return;
          yield wrap(chunk.result);
        }
      } finally {
        // Early break/throw: cancel + free the engine-side stream state.
        await locked(() => bindings.streamCancel(conn, stream));
      }
    },
    close: () => locked(() => bindings.closeConn(conn)),
  });

  return {
    query: (sql, format = 'CSV') => locked(async () => wrap(await bindings.query(sql, format))),
    putFile: (path, data) => locked(() => bindings.writeFile(path, data)),
    connect: (path) => locked(() => makeSession(bindings.connect(path))),
    module: mod,
  };
}
