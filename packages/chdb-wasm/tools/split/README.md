# wasm-split pipeline

Splits `chdb.wasm` into a **primary** module holding the profile-hot functions
(downloaded up front) and a **deferred** module holding everything else
(`chdb.deferred.wasm`). The first call into a function that lives in the
deferred module synchronously fetches + instantiates it (once per wasm
instance; the browser HTTP cache makes repeats cheap), patches the shared
function table, and the original call proceeds. **No functionality is lost** —
a query touching cold code just stalls once for the download.

Mechanics: Emscripten `-sSPLIT_MODULE` + Binaryen `wasm-split`. The split is at
function granularity, driven by an execution profile recorded while running
`profile-corpus.sql` (SQL operators, scalar/aggregate function families, table
functions, formats, DDL/DML, error paths, Iceberg/Delta/DataLakeCatalog) against
the instrumented build.

Measured on the 26.5 build (split.test 19/19, full test matrix passing):

| bundle | before | primary (up-front) | deferred (lazy) | gzip transfer |
| --- | --- | --- | --- | --- |
| mt | 99.3MB | **41.7MB** | 64.1MB | 21.2MB → **9.2MB** |
| st | 99.2MB | **39.5MB** | 66.3MB | → 8.6MB |

## Runbook

```sh
# 1. build both trees with the split link option (only relinks, cheap)
source ~/code/emsdk/emsdk_env.sh
cd buildwasm     && cmake -DWASM_SPLIT_MODULE=ON . && ninja chdb_wasm
cd ../buildwasm-st && cmake -DWASM_SPLIT_MODULE=ON . && ninja chdb_wasm
# each emits: chdb.mjs (glue with lazy-load proxy), chdb.wasm (instrumented),
#             chdb.wasm.orig (the real artifact that gets split)

# 2. single-threaded bundle FIRST — its lone instance executes the whole query
#    pipeline inline, so its hot set is the complete corpus coverage; export it
node tools/split/split-wasm.mjs --build ../../buildwasm-st/programs/wasm \
    --out /tmp/chdb-split-st --emit-hot-names /tmp/chdb-split-st/hot-names.txt

# 3. threaded bundle, mixing in the st hot set: mt pool workers park in
#    Atomics.wait and cannot answer the profile-collection message, so work
#    that ran only on worker threads is invisible to the mt profile
node tools/split/split-wasm.mjs --build ../../buildwasm/programs/wasm \
    --out /tmp/chdb-split-mt --extra-hot /tmp/chdb-split-st/hot-names.txt

# 4. verify each split bundle (hot query / cold lazy-load / negative control)
node tools/split/verify-split.mjs /tmp/chdb-split-mt
node tools/split/verify-split.mjs /tmp/chdb-split-st

# 5. install into dist/ (+ dist/st) and run the test suites against it
node scripts/copy-artifacts.mjs /tmp/chdb-split-mt /tmp/chdb-split-st
CHDB_WASM_MJS=$PWD/dist/chdb.mjs node test/smoke.test.mjs
```

Prereqs: `/tmp/node24/bin/node` (>=23), and for the data-lake corpus sections a
venv at `ICEBERG_PY` (default `/tmp/iceberg-venv/bin/python`) with
`pyiceberg[sql-sqlite] pyarrow moto[server] s3fs deltalake`; without it those
statements are skipped (and their code ends up in the deferred module).

## The lite pipeline (chdb-wasm-lite)

`packages/chdb-wasm-lite` ships a PRIMARY-ONLY bundle sized for Cloudflare
Workers (10 MiB gzipped Worker limit; workerd forbids runtime wasm
compilation, so a deferred module could never load there). Differences from
the full split: single-threaded tree relinked with `-DWASM_INITIAL_MEMORY=64MB`
(a Workers isolate caps memory at 128MB), profiled against
`profile-corpus-lite.sql` (most-common SQL incl. `file()`/`url()`/`s3()`),
`--extra-keep keep-lite.txt` (per-type specializations the corpus can't
enumerate), and `--lite-glue` so out-of-corpus calls throw a clear
"not in lite" error instead of attempting a lazy load:

```sh
cd buildwasm-st && cmake -DWASM_SPLIT_MODULE=ON -DWASM_INITIAL_MEMORY=64MB . && ninja chdb_wasm
CHDB_SKIP_LAKE=1 node tools/split/split-wasm.mjs \
    --build ../../buildwasm-st/programs/wasm --out /tmp/chdb-split-lite \
    --corpus tools/split/profile-corpus-lite.sql \
    --extra-keep tools/split/keep-lite.txt --lite-glue
cd ../chdb-wasm-lite && node scripts/build-lite.mjs /tmp/chdb-split-lite && node test/lite.test.mjs
```

Measured: 37.6MB raw / **8.0 MiB gzipped** (< the 9.5 MiB budget that leaves
room for glue + SDK inside the Worker limit). When a probe hits a cold
function on lite, extend the corpus (preferred: whole call chains go hot) or
add the exact name to `keep-lite.txt`; the convergence workflow is the same
ordinal→name mapping described under Gotchas. Aggregate internals specialize
per input type (String/Nullable/Float min-max differ from UInt64), so the
lite corpus exercises aggregates over several column types on purpose.

## Files

| file | role |
| --- | --- |
| `profile-corpus.sql` | defines the hot set; statements ending in failure are fine (error handling should be hot too); `/*mt-only*/` statements are skipped on st |
| `run-profile.mjs` | executes the corpus + streaming C API against the instrumented bundle, dumps one profile per wasm instance |
| `fixture-host.mjs` | out-of-process mocks (static HTTP for `url()`, Iceberg REST + Unity catalogs); moto runs as its own python process |
| `patch-glue.mjs` | two glue patches with hard anchor checks: `--lazy-load` (wasmBinaryFile fallback so pthread workers can resolve `chdb.deferred.wasm`) and `--profile-collect` (worker-side `chdbWriteProfile` message) |
| `split-wasm.mjs` | merge profiles → keep-list (thread-runtime safety regex + `--extra-hot`) → `wasm-split` → install |
| `verify-split.mjs` | asserts hot query works, cold query lazy-loads, and the cold query FAILS when `chdb.deferred.wasm` is hidden (negative control) |

## Gotchas learned the hard way

- **Per-instance profiles**: wasm-split's instrumentation counters are wasm
  globals — every pthread worker instance has its own. The main instance's
  `__write_profile` sees only main-thread execution. Binaryen's shared-memory
  instrumentation modes (`--in-memory`, `--in-secondary-memory`) emit invalid
  code for Memory64 (i32 addressing), so they're not usable here — hence the
  st-hot-names transfer plus the thread-runtime safety keep-list.
- **Parked workers**: idle ClickHouse pool threads sit in `Atomics.wait`; their
  workers never service `postMessage`, so per-worker collection mostly times
  out on mt. Harmless (5s timeout each), but it's why `--extra-hot` exists.
- **pthread instantiation**: the split primary's placeholder imports are read
  during `new WebAssembly.Instance` in each worker, where the stock glue never
  set `wasmBinaryFile` — without the `--lazy-load` patch the pool hangs at
  startup.
- **Names**: `-sSPLIT_MODULE` keeps the name section in `chdb.wasm.orig`
  (needed for keep-funcs matching); wasm-split strips it from both outputs by
  default. Export names are minified, so reach `malloc` via `Module._malloc`,
  not `wasmExports.malloc`; binaryen-added `__write_profile` is unminified and
  raw (BigInt i64 pointer arg, Number i32 length).
- **print-profile output exceeds V8's string limit** (~140k functions with huge
  mangled names) — always stream it.
- The corpus runner must never host HTTP fixtures in-process: queries block the
  main thread synchronously while the wasm HTTP bridge waits on a child
  process fetch, deadlocking any same-process server.
- **Worker-path blind spot**: code that runs ONLY on mt worker instances is
  invisible to every profile pass (per-instance globals + parked workers) and
  often absent from the st hot set too (st takes synchronous implementations —
  LazyOutputFormat, ParallelFormattingOutputFormat, the thread-entry
  trampolines). Covered by SAFETY/SAFETY_SUBSTR patterns and
  keep-worker-path.txt. If a new one appears, the symptom is a hang or
  "worker sent an error" on a bundle whose chdb.deferred.wasm is unreachable;
  find it by logging `base` in the glue's placeholder trampoline (workers
  can't console.log — append to a file), then map the ordinal to a name via a
  `-g` re-split and the secondary's elem section.
- keep-funcs entries must be in wasm-split's ESCAPED name form (`\28\29` for
  parens, `\20` for spaces) — raw demangled names silently match nothing.
- **Synthesized names don't transfer across links**: nameless lambda thunks
  (bare-index names) and wasm-ld's signature bridges (`trampoline_X`,
  `X_<number>`) get link-specific names, so the st hot set can't cover them on
  mt even though hot code calls straight into them. Every cold one is
  force-kept (~10k tiny functions, ~1MB).
- The engine's ONE cold start per process is claimed by whichever API runs
  first — profile both orders (main pass boots via connect, the
  CHDB_INIT_PROBE=global pass boots via connectionless query, then both run
  the corpus). And keep unqualified default-database DDL in the corpus:
  clickhouse-local's DatabaseOverlay is its own code path.
