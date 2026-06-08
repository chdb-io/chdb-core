// chdb-wasm: ClickHouse (chdb) compiled to WebAssembly, with an async,
// non-blocking, worker-based JS/TS API.
//
//   import { AsyncChdb } from 'chdb-wasm';
//   const db = await AsyncChdb.create({ moduleUrl: '/path/to/chdb.mjs' });
//   const r = await db.query('SELECT 1');
//   console.log(r.text());          // "1\n"
//   await db.terminate();

export { AsyncChdb, AsyncChdbConnection } from './async.ts';
export type { ChdbResult, CreateOptions } from './async.ts';
export { ChdbError, StatusCode } from './status.ts';
export { getPlatformFeatures, selectBundle } from './platform.ts';
export type { PlatformFeatures, BundleConfig, SelectOptions } from './platform.ts';
export type { ConnHandle, WireResult } from './protocol.ts';

export const version = '0.1.0-wasm';
