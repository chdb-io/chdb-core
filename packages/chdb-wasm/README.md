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

### Full (untrimmed) engine

Both published bundles are compiled with chdb-core's `CHDB_LITE` trim set, which drops
~70 niche aggregate functions to shrink the download — among them
`quantileExactInclusive`/`quantilesExactInclusive` and `largestTriangleThreeBuckets`/`lttb`.
`CHDB_WASM_FULL=1` builds the same engine with the **complete** registry instead:

```bash
CHDB_WASM_FULL=1 chdb/build-wasm.sh build      # -> buildwasm-full/

# Assert the registry matches the variant (Node >= 23; exits non-zero on mismatch):
node programs/wasm/chdb.lite-vs-full.test.mjs buildwasm-full/programs/wasm/chdb.mjs full
node programs/wasm/chdb.lite-vs-full.test.mjs buildwasm/programs/wasm/chdb.mjs      lite
```

It is opt-in because of what it costs (measured, ClickHouse 26.5.1.1 / Emscripten 5.0.7,
MinSizeRel, threads on):

| variant | `chdb.wasm` raw | `gzip -6` |
| --- | --- | --- |
| lite (default, what npm ships) | 104,140,148 B (99.3 MiB) | 21.3 MiB |
| full (`CHDB_WASM_FULL=1`) | 158,176,747 B (150.8 MiB) | 27.4 MiB |

`CHDB_WASM_FULL=1` defaults `BUILD_DIR` to `buildwasm-full/` so it never clobbers the lite
build, and selects the flags the untrimmed configure needs (`CHDB_LITE=OFF`, explicit
`MinSizeRel`, `WERROR=0`, and the four `ENABLE_*` libraries the `CHDB_LITE` block otherwise
opts back in over `ENABLE_LIBRARIES=0`) — see the comments in `chdb/build-wasm.sh`.

To ship it, build the single-threaded bundle full as well and point `copy-artifacts.mjs` at
both — passing only the mt dir leaves the st slot on its default `buildwasm-st/`, so a lite
st build still sitting there would be packaged next to a full mt one:

```bash
CHDB_WASM_FULL=1 WASM_THREADS=OFF BUILD_DIR=buildwasm-full-st chdb/build-wasm.sh build
node packages/chdb-wasm/scripts/copy-artifacts.mjs \
    buildwasm-full/programs/wasm buildwasm-full-st/programs/wasm
```

Restoring only these two aggregate families rather than all ~70 would cost well under
1 MiB compressed instead of ~6 MiB; the drop machinery in
`src/AggregateFunctions/CMakeLists.txt` already stubs per-function register calls, so a
`CHDB_LITE_KEEP_AGGREGATES` keep-list hook there is a small change and the better
long-term fix if size matters more than completeness.

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
