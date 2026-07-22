# chdb-wasm-lite

Size-optimized [chdb](https://github.com/chdb-io/chdb) (ClickHouse) for
WebAssembly: **one `chdb.wasm` under 10 MiB gzipped**, sized to fit the
[Cloudflare Workers](https://developers.cloudflare.com/workers/platform/limits/)
paid-plan bundle limit. Same SDK and API as
[`chdb-wasm`](https://www.npmjs.com/package/chdb-wasm).

## How it differs from `chdb-wasm`

The full package splits the engine into a hot primary plus a lazily-downloaded
`chdb.deferred.wasm`, so every SQL feature works everywhere. Workers forbids
runtime wasm compilation — a deferred module can never be loaded there — so
**lite ships only the primary**, profiled against a corpus of the most common
SQL (see `tools/split/profile-corpus-lite.sql` in the repo):

- core operators: filtering, aggregation (`GROUP BY`/`HAVING`/`ROLLUP`), joins
  (INNER/LEFT/FULL/CROSS/USING), window functions, CTEs, set operations,
  `ORDER BY`/`LIMIT BY`/`DISTINCT`
- the everyday scalar functions (string/date/math/conditional/array/map/JSON/
  URL/hash) and aggregates (`count`/`sum`/`avg`/`min`/`max`/`uniq*`/
  `quantile*`/`topK`/`argMax`/`groupArray`, `-If`/`-Merge` combinators)
- `file()` over the in-memory filesystem (CSV/TSV/JSONEachRow/Parquet/Arrow/
  Native, gzip), `putFile` ingestion
- remote reads with `url()` and `s3()` (single-object, path-style) — see the
  JSPI note below
- Memory-engine tables, views, sessions, streaming queries, the common output
  formats (CSV/TSV/JSON*/Pretty*/Parquet/...)

**Networking runs on JSPI.** The full package's HTTP bridge waits
synchronously (sync XHR in browsers, a subprocess in Node), which Cloudflare's
workerd cannot do. The lite bundle instead links with WebAssembly JavaScript
Promise Integration: the wasm stack suspends at a plain async `fetch()` and
resumes when it settles — zero size cost, and it is exactly what makes
`url()`/`s3()` work *inside* Workers. The trade-off is that lite requires a
JSPI-capable engine: **workerd (Cloudflare Workers), Chrome/Edge 137+, or
Node 24+ with `--experimental-wasm-jspi`**. The data-lake stack
(Iceberg/Delta/catalogs) stays excluded for size.

Anything outside that set throws immediately with:

```
chdb-wasm-lite: this SQL feature is not included in the size-optimized lite build; use the full chdb-wasm package
```

The bundle is single-threaded (Workers has no threads) and links with a 64MB
initial memory (growable; a Workers isolate caps total memory at 128MB).

## Usage

Same API as `chdb-wasm`:

```js
import { AsyncChdb } from 'chdb-wasm-lite';

const db = await AsyncChdb.create({
  moduleUrl: new URL('chdb-wasm-lite/chdb.mjs', import.meta.url).href,
});
const result = await db.query('SELECT version()', 'CSV');
console.log(result.text());
```

Notes for Cloudflare Workers specifically: workerd has no `Worker` API, so
drive the Emscripten module directly instead of through `AsyncChdb` — import
the wasm module statically, hand it to Emscripten via `Module.instantiateWasm`
(workerd forbids compiling wasm from bytes), and pass `locateFile` (workerd
module names are not URLs). The suspendable exports return Promises, so call
them with `ccall(..., {async: true})`:

```js
import createChdbModule from 'chdb-wasm-lite/chdb.mjs';
import wasmModule from './chdb.wasm'; // static import -> compiled at deploy

const mod = await createChdbModule({
  locateFile: (path) => path,
  instantiateWasm(imports, cb) {
    const inst = new WebAssembly.Instance(wasmModule, imports);
    cb(inst, wasmModule);
    return inst.exports;
  },
});
const r = await mod.ccall('chdb_wasm_query', 'number', ['string', 'string'],
                          ["SELECT * FROM url('https://.../data.csv', CSVWithNames)", 'CSV'],
                          { async: true });
```

If you need the data-lake stack (Iceberg/Delta/catalogs) or a non-JSPI
runtime, use the full `chdb-wasm` package.
