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
import { ChdbError } from './status.ts';
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

  const makeSession = (conn: ConnHandle): ChdbWorkersSession => {
    // close() frees the engine-side connection; a queryStream() generator can
    // be paused at yield across it, and resuming (or abandoning) it must NOT
    // call streamFetch/streamCancel on the freed handle — that is a wasm
    // use-after-free. So the session tracks its live streams: close() cancels
    // them first (under the lock), and every later engine call fails with a
    // clear error instead. All checks run INSIDE locked ops, so they are
    // serialized with close() itself.
    let closed = false;
    const activeStreams = new Set<ConnHandle>();
    const ensureOpen = () => {
      if (closed) throw new ChdbError('session is closed');
    };
    return {
      query: (sql, format = 'CSV') =>
        locked(async () => {
          ensureOpen();
          return wrap(await bindings.queryConn(conn, sql, format));
        }),
      async *queryStream(sql, format = 'CSV') {
        const stream = await locked(async () => {
          ensureOpen();
          const s = await bindings.streamStart(conn, sql, format);
          activeStreams.add(s);
          return s;
        });
        try {
          for (;;) {
            const chunk = await locked(() => {
              // close() ran while we were paused at yield: the stream (and the
              // connection) are already freed engine-side.
              if (!activeStreams.has(stream)) throw new ChdbError('session was closed while streaming');
              return bindings.streamFetch(conn, stream);
            });
            if (chunk.done || !chunk.result) return;
            yield wrap(chunk.result);
          }
        } finally {
          // Early break/throw: cancel + free the engine-side stream state —
          // unless close() already did (delete() returns false then).
          await locked(() => {
            if (activeStreams.delete(stream)) bindings.streamCancel(conn, stream);
          });
        }
      },
      close: () =>
        locked(() => {
          if (closed) return;
          closed = true;
          // (Atomic w.r.t. the generators — this whole op holds the lock and
          // never awaits — so no snapshot dance is needed here, unlike the
          // dispatcher-based AsyncChdbConnection.)
          for (const s of activeStreams) {
            try {
              bindings.streamCancel(conn, s);
            } catch {
              // Best effort: a failed stream cancel must not leak the connection.
            }
          }
          activeStreams.clear();
          bindings.closeConn(conn);
        }),
    };
  };

  return {
    query: (sql, format = 'CSV') => locked(async () => wrap(await bindings.query(sql, format))),
    putFile: (path, data) => locked(() => bindings.writeFile(path, data)),
    connect: (path) => locked(() => makeSession(bindings.connect(path))),
    module: mod,
  };
}
