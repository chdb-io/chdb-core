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
- Memory-engine tables, views, sessions, streaming queries, the common output
  formats (CSV/TSV/JSON*/Pretty*/Parquet/...)

**Engine-initiated networking is excluded entirely** — `url()`, `s3()` and the
data-lake stack. The engine needs synchronous HTTP, which Cloudflare's workerd
cannot provide (async `fetch` only), so these could never run inside Workers;
leaving them out keeps the bundle smaller. Fetch data with your Worker's own
JS (async `fetch`, R2/KV bindings, request body) and hand it to chdb via
`putFile` + `file()`.

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

Notes for Cloudflare Workers specifically: import the wasm module statically
and hand it to Emscripten via `Module.instantiateWasm` (workerd forbids
compiling wasm from bytes). The same lite surface works in browsers and Node;
if you need engine-side remote reads (`url()`/`s3()`/Iceberg/Delta) there, use
the full `chdb-wasm` package.
