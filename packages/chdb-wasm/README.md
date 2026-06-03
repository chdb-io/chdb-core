# @chdb/chdb-wasm

[chdb](https://github.com/chdb-io/chdb) (an embedded [ClickHouse](https://clickhouse.com)) compiled to WebAssembly, with an **async, non-blocking, worker-based** JS/TS API — modeled on [duckdb-wasm](https://github.com/duckdb/duckdb-wasm).

The wasm engine runs inside a Web Worker, so queries return Promises and the
caller's thread (your UI / event loop) is never blocked.

## Requirements

chdb's wasm bundle uses the **Memory64** ABI + native **wasm exceptions** +
**threads**. That means:

- **Node ≥ 23** (Memory64 + `WASM_BIGINT`), or a recent Chrome/Firefox.
- In the **browser**, threads need **cross-origin isolation** — serve with:
  - `Cross-Origin-Opener-Policy: same-origin`
  - `Cross-Origin-Embedder-Policy: require-corp`

ClickHouse hard-requires threads, so (unlike duckdb-wasm) there is no
single-threaded "mvp" bundle. `selectBundle()` reports if the runtime can't run it.

## Usage (Node)

```js
import { AsyncChdb } from '@chdb/chdb-wasm';

const db = await AsyncChdb.create({ moduleUrl: '/path/to/chdb.mjs' });

const r = await db.query('SELECT 1');
console.log(r.text());                 // "1\n"
console.log(r.rowsRead, r.elapsedSeconds);

// explicit connection + streaming
const conn = await db.connect();
for await (const chunk of conn.queryStream('SELECT number FROM numbers(1e7)')) {
  process.stdout.write(chunk.data);    // Uint8Array, processed chunk-by-chunk
}
await conn.close();

await db.terminate();
```

`query(sql, format?)` accepts any ClickHouse output format (`'CSV'` default,
`'JSONEachRow'`, `'Arrow'`, …). Results expose `data: Uint8Array`, `text()`,
`rowsRead`, `bytesRead`, `elapsedSeconds`.

## Usage (browser)

```js
import { AsyncChdb, selectBundle } from '@chdb/chdb-wasm';

const bundle = selectBundle({ baseUrl: '/node_modules/@chdb/chdb-wasm/dist' });
if (!bundle.supported) throw new Error(bundle.reasons.join('; '));

const db = await AsyncChdb.create({
  moduleUrl: bundle.moduleUrl,
  wasmUrl: bundle.wasmUrl,
  onProgress: (loaded, total) => console.log(`${loaded}/${total}`),
});
```

## Build / test

```bash
# 1. build the wasm (from the chdb repo root, with emsdk sourced):
cmake --build buildwasm --target chdb_wasm -- -k 0
# 2. copy artifacts into the package and build TS -> dist:
node packages/chdb-wasm/scripts/copy-artifacts.mjs
npm --prefix packages/chdb-wasm run build       # tsc -> dist/

# Node test (runs the .ts source directly via Node's type stripping):
node packages/chdb-wasm/test/smoke.test.mjs

# Browser test (needs a built dist/ + a browser):
node packages/chdb-wasm/scripts/serve.mjs       # COOP/COEP dev server
# open http://localhost:8099/test/browser.html
```

## Build knobs (CMake)

chdb ships a **single** wasm bundle (Memory64 + native exceptions + threads,
built `-Os`); there is no feature or size/speed variant. Operational knobs on the
`chdb_wasm` CMake target:

| option | default | meaning |
| --- | --- | --- |
| `WASM_STACK_SIZE` | `8MB` | main-thread C++ stack |
| `WASM_PTHREAD_STACK_SIZE` | `2MB` | per-worker stack (×pool size — dominates baseline memory) |
| `WASM_PTHREAD_POOL_SIZE` | `48` | pre-spawned worker pool (must cover ClickHouse's peak live threads) |
| `WASM_INITIAL_MEMORY` | `128MB` | initial heap |
