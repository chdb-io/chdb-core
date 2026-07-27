# chdb-cloudflare

[chdb](https://github.com/chdb-io/chdb) (ClickHouse) for
[Cloudflare Workers](https://developers.cloudflare.com/workers/): **one
`chdb.wasm` under 10 MiB gzipped**, sized to fit the Workers
[paid-plan bundle limit](https://developers.cloudflare.com/workers/platform/limits/),
with a JSPI async-fetch HTTP bridge so ClickHouse's `url()` and `s3()`
table functions work inside Workers.
Same SDK and API as [`chdb-wasm`](https://www.npmjs.com/package/chdb-wasm).

## How it differs from `chdb-wasm`

The full `chdb-wasm` package splits the engine into a hot primary plus a lazily-downloaded
`chdb.deferred.wasm`, so every SQL feature works everywhere. Workers forbids
runtime wasm compilation — a deferred module can never be loaded there — so
**chdb-cloudflare ships only the primary**, profiled against a corpus of the most common
SQL (see `tools/split/profile-corpus-lite.sql` in the repo):

- core operators: filtering, aggregation (`GROUP BY`/`HAVING`/`ROLLUP`/`CUBE`/
  `WITH TOTALS`), joins (INNER/LEFT/FULL/CROSS/USING), window functions
  (`row_number`/`rank`/`dense_rank`/`first_value`/`last_value`/`lagInFrame`/
  frames), CTEs, set operations, `ORDER BY`/`LIMIT BY`/`DISTINCT`, `ARRAY JOIN`
- the everyday scalar functions — string/search/regex, date-time (incl.
  `toStartOfInterval` bucketing, `formatDateTime`, `parseDateTimeBestEffort`,
  time zones), math, conditional, arrays (incl. lambdas), maps/tuples, JSON
  extraction, URL parsing, IPv4/IPv6 helpers, hashes — and aggregates
  (`count`/`sum`/`avg`/`min`/`max`/`uniq*`/`quantile*`/`topK`/`argMin`/
  `argMax`/`groupArray`/`sumMap`/`windowFunnel`/stddev-variance, `-If`/`-Merge`
  combinators, over the common column types incl. Nullable/LowCardinality/
  Decimal/DateTime64)
- table functions over local data: `file()` on the in-memory filesystem
  (CSV/TSV/JSONEachRow/Parquet/Native, gzip; `putFile` ingestion), `format()`
  for inline strings (incl. `LineAsString`/`JSONAsString`), `values()`,
  `numbers()`, `generateRandom()`, `view()`
- remote reads with `url()` and `s3()` (single-object, path-style) — see the
  JSPI note below
- Memory-engine tables, temporary tables, views, sessions, streaming queries,
  the common output formats (CSV*/TSV*/JSON/JSONEachRow/JSONCompact*/Pretty*/
  Vertical/Markdown/Values/RowBinary/Parquet/...)

**Networking runs on JSPI.** The full package's HTTP bridge waits
synchronously (sync XHR in browsers, a subprocess in Node), which Cloudflare's
workerd cannot do. This package instead links with WebAssembly JavaScript
Promise Integration: the wasm stack suspends at a plain async `fetch()` and
resumes when it settles — zero size cost, and it is exactly what makes
`url()`/`s3()` work *inside* Workers. The trade-off is that this package requires a
JSPI-capable engine: **workerd (Cloudflare Workers), Chrome/Edge 137+, or
Node 24+ with `--experimental-wasm-jspi`**. The data-lake stack
(Iceberg/Delta/catalogs) stays excluded for size.

Anything outside that set throws immediately with:

```
chdb-cloudflare: this SQL feature is not included in this size-optimized Cloudflare Workers build; use the full chdb-wasm package
```

The bundle is single-threaded (Workers has no threads) and links with a 64MB
initial memory (growable; a Workers isolate caps total memory at 128MB).

## Usage

Same API as `chdb-wasm`:

```js
import { AsyncChdb } from 'chdb-cloudflare';

const db = await AsyncChdb.create({
  moduleUrl: new URL('chdb-cloudflare/chdb.mjs', import.meta.url).href,
});
const result = await db.query('SELECT version()', 'CSV');
console.log(result.text());
```

### Cloudflare Workers

workerd has no `Worker` API, so `AsyncChdb` cannot run there. Use the
dedicated Workers entry instead — it wraps the module wiring (deploy-time
compiled wasm via `instantiateWasm`, `locateFile`) and, importantly,
serializes engine calls: one isolate serves concurrent requests, and a query
suspended at `fetch()` (JSPI) must not be re-entered by another request.

```js
import createChdbModule from 'chdb-cloudflare/chdb.mjs';
import wasmModule from 'chdb-cloudflare/chdb.wasm';
import { createChdb } from 'chdb-cloudflare/workers';

let dbPromise = null;
export default {
  async fetch(request) {
    const db = await (dbPromise ??= createChdb(createChdbModule, wasmModule));
    const r = await db.query("SELECT count() FROM url('https://…/data.csv', CSVWithNames)");
    return new Response(r.text());
  },
};
```

`db.putFile(path, bytes)` feeds data to `file()`, and `db.connect()` opens a
session (`query`, `queryStream`, `close`). Default `wrangler` bundling works
as-is; the build prints a warning that `node:module` (imported by the
Emscripten glue) is Node-only — it is harmless, that import never executes in
workerd, and no `nodejs_compat` flag is needed.

If you need the data-lake stack (Iceberg/Delta/catalogs) or a non-JSPI
runtime, use the full `chdb-wasm` package.
