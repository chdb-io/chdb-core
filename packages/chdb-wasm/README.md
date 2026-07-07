# chdb-wasm

[chdb](https://github.com/chdb-io/chdb) (an embedded [ClickHouse](https://clickhouse.com)) compiled to WebAssembly, with an **async, non-blocking, worker-based** JS/TS API.

The wasm engine runs inside a Web Worker, so queries return Promises and the
caller's thread (your UI / event loop) is never blocked.

## Requirements

chdb's wasm is built for the **Memory64** ABI with native **wasm exceptions**, so it
needs a recent runtime:

- **Node ≥ 23** (Memory64 + `WASM_BIGINT`), or a recent Chrome/Firefox.

Two bundles are shipped and auto-selected by `selectBundle()`:

- **mt** (multi-threaded): in the **browser** it needs **cross-origin isolation** — serve with
  `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp`.
- **st** (single-threaded): runs on any page, no isolation required.

`selectBundle()` picks `mt` on a cross-origin-isolated page (SharedArrayBuffer available),
otherwise `st`, and reports if the runtime can't run it at all.

## Usage (Node)

```js
import { AsyncChdb } from 'chdb-wasm';

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
import { AsyncChdb, selectBundle } from 'chdb-wasm';

const bundle = selectBundle({ baseUrl: '/node_modules/chdb-wasm/dist' });
if (!bundle.supported) throw new Error(bundle.reasons.join('; '));

const db = await AsyncChdb.create({
  moduleUrl: bundle.moduleUrl,
  wasmUrl: bundle.wasmUrl,
  onProgress: (loaded, total) => console.log(`${loaded}/${total}`),
});
```

## Build / test

```bash
# 1. build both wasm bundles (from the chdb repo root, with emsdk sourced):
chdb/build-wasm.sh build                                          # mt -> buildwasm/
WASM_THREADS=OFF BUILD_DIR=buildwasm-st chdb/build-wasm.sh build   # st -> buildwasm-st/
# 2. copy both bundles into the package and build TS -> dist (mt -> dist/, st -> dist/st/):
node packages/chdb-wasm/scripts/copy-artifacts.mjs buildwasm/programs/wasm buildwasm-st/programs/wasm
npm --prefix packages/chdb-wasm run build       # tsc -> dist/

# Node test (runs the .ts source directly via Node's type stripping):
node packages/chdb-wasm/test/smoke.test.mjs

# Browser test (headless Chrome, both bundles):
node packages/chdb-wasm/test/browser-run.mjs
```

## Build knobs (CMake)

Two bundles are built — **mt** (threaded) and **st** (single-threaded) — both Memory64 +
native exceptions, `-Oz` (`WASM_THREADS=ON`/`OFF` selects which). Operational knobs on the
`chdb_wasm` CMake target:

| option | default | meaning |
| --- | --- | --- |
| `WASM_STACK_SIZE` | `8MB` | main-thread C++ stack |
| `WASM_PTHREAD_STACK_SIZE` | `2MB` | per-worker stack (×pool size — dominates baseline memory) |
| `WASM_PTHREAD_POOL_SIZE` | `16` | pre-spawned worker pool, mt only (must cover ClickHouse's peak live threads) |
| `WASM_INITIAL_MEMORY` | `128MB` | initial heap |
