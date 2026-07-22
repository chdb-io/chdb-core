// chdb-wasm: ClickHouse (chdb) compiled to WebAssembly, with an async,
// non-blocking, worker-based JS/TS API.
//
//   import { AsyncChdb } from 'chdb-wasm';
//   const db = await AsyncChdb.create({ moduleUrl: '/path/to/chdb.mjs' });
//   const r = await db.query('SELECT 1');
//   console.log(r.text());          // "1\n"
//   await db.terminate();
export { AsyncChdb, AsyncChdbConnection } from "./async.js";
export { ChdbError, StatusCode } from "./status.js";
export { getPlatformFeatures, selectBundle } from "./platform.js";
//# sourceMappingURL=index.js.map