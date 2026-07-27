# chdb-cloudflare

[chdb](https://github.com/chdb-io/chdb) (ClickHouse) for
[Cloudflare Workers](https://developers.cloudflare.com/workers/): **one
`chdb.wasm` under 10 MiB gzipped**, sized to fit the Workers
[paid-plan bundle limit](https://developers.cloudflare.com/workers/platform/limits/),
with a JSPI async-fetch HTTP bridge so ClickHouse's `url()` and `s3()`
table functions work inside Workers.
Same SDK and API as [`chdb-wasm`](https://www.npmjs.com/package/chdb-wasm).

## How it differs from `chdb-wasm`

The full `chdb-wasm` package splits the engine into a hot primary plus a
lazily-downloaded `chdb.deferred.wasm`, so every SQL feature works everywhere.
Workers forbids runtime wasm compilation — a deferred module can never load
there — so **chdb-cloudflare ships only the primary**, built from a profile of
the most common SQL. Everyday analytics is covered: filters, aggregation,
joins, window functions, CTEs, the common scalar and aggregate functions,
`file()`/`format()`/`values()` over local data, remote reads with `url()` and
`s3()`, Memory-engine tables, sessions, streaming, and the common input/output
formats (CSV/TSV/JSON*/Parquet/Pretty*/...).

To stay under the size limit, everything else is left out — notably MergeTree
tables and the data-lake stack (Iceberg/Delta/catalogs). Calling an
unsupported feature fails immediately with a clear error (the instance stays
usable):

```
chdb-cloudflare: this SQL feature is not included in this size-optimized Cloudflare Workers build; use the full chdb-wasm package
```

**Networking runs on JSPI.** The full package's HTTP bridge waits
synchronously (sync XHR in browsers, a subprocess in Node), which Cloudflare's
workerd cannot do. This package instead links with WebAssembly JavaScript
Promise Integration: the wasm stack suspends at a plain async `fetch()` and
resumes when it settles — zero size cost, and it is exactly what makes
`url()`/`s3()` work *inside* Workers. The trade-off: it requires a
JSPI-capable engine — **workerd (Cloudflare Workers), Chrome/Edge 137+, or
Node 24+ with `--experimental-wasm-jspi`**.

The bundle is single-threaded (Workers has no threads) and links with a 64MB
initial memory (growable; a Workers isolate caps total memory at 128MB).

## Usage

Same API as `chdb-wasm`. Under Node, run with `node --experimental-wasm-jspi`
(the bundle needs a JSPI engine, see above):

```js
import { AsyncChdb } from 'chdb-cloudflare';

const db = await AsyncChdb.create({
  moduleUrl: import.meta.resolve('chdb-cloudflare/chdb.mjs'),
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
