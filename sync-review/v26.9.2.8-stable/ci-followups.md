# CI follow-ups for the v26.9.2.8-stable sync

One root cause per entry, and per commit. These commits stay separate from the two baseline
commits so the failure and its fix can be read together later.

## 1. libchdb.so does not link: local-exec TLS relocations from inline assembly

**Failure.** `Build & Test Free-Threading Wheels (Linux x86_64)`, step *Build & test FT wheels*,
run 35937391236. Hundreds of identical errors, then the linker gave up:

```
ld.lld-21: error: relocation R_X86_64_TPOFF32 against FiberLocalStorageThreadStorage
                  cannot be used with -shared
>>> defined in src/libclickhouse_common_io.a(FiberLocal.cpp.o)
>>> referenced by ThreadStatusExt.cpp
ld.lld-21: error: too many errors emitted, stopping now
```

**Root cause.** `src/Common/FiberLocal.h` is new in v26.9. Its `load()`/`store()` fast path
reaches thread-local storage from inline assembly that names the TLS symbol itself:

```
movq %fs:FiberLocalStorageThreadStorage@tpoff+N, %rax      # x86_64
add x0, x0, :tprel_hi12:FiberLocalStorageThreadStorage+N   # aarch64
```

Those spellings *are* the local-exec model. A shared object cannot carry them, because the
offset from the thread pointer is not known until the program is linked. Upstream only builds
executables, so upstream never sees it; every chdb artifact is `libchdb.so`, or an archive a
consumer may link into one.

`-ftls-model=global-dynamic` does not help, and neither does the `USE_MUSL` branch in
`CMakeLists.txt` that sets it: the relocation is written into the assembly, not chosen by the
compiler from the TLS model.

**Fix.** The header already carries the portable branch — `currentSlots()[slot]`, a `noinline`
accessor with a memory barrier, which gives the same "do not hoist this across a fiber switch"
guarantee the assembly was written for. `CMakeLists.txt` defines
`CHDB_PORTABLE_FIBER_LOCAL_TLS` in the chdb_spec section and the two `#if`s in `FiberLocal.h`
test for it, so upstream's structure survives and the next sync's diff stays two lines.

**Why the local checks missed it.** The local matrix built the Linux **static** library and ran
its Go cgo consumer, which links an executable — `-shared` never happened, so the relocation
was legal every time. macOS is Mach-O and takes the `#else` branch already. No local
configuration linked an ELF shared object, which is the only one that fails.

**What catches it next time.** Build `libchdb.so` on Linux, not only `libchdb.a`: they are
different link modes and only one of them rejects these relocations. Cheaper still, and what
was used to confirm this root cause before touching the tree: compile the TLS-touching code
`-fPIC` and link it `-shared` on its own. It reproduces in seconds, on either architecture.

## 2. Iceberg queries fail: EmbeddedServer missed two thread pools new in v26.9

**Failure.** `wasm / build-and-test`, step *Iceberg local table via MEMFS (mt + st)*, run
35941502811:

```
ChdbError: Code: 49. DB::Exception: Iceberg iterator is failed with exception:
Code: 49. DB::Exception: The IcebergManifestDecodeThreadPool is not initialized.
(LOGICAL_ERROR) ... While executing ReadFromObjectStorage
```

**Root cause.** v26.9 moved Iceberg manifest decoding onto a shared thread pool and added its
`initialize()` to `LocalServer`. `EmbeddedServer` is chdb's own entry point and appears in no
upstream diff, so the call arrived in one and not the other. Comparing only the calls v26.9
*added* to `LocalServer` against `EmbeddedServer` found a second one with it:

| Missing | Effect |
| --- | --- |
| `getIcebergManifestDecodeThreadPool().initialize()` | every Iceberg read raises LOGICAL_ERROR |
| `getDatabaseCatalogShutdownTablesThreadPool().initialize()` | only reached while shutting tables down |

Neither pool exists at v26.7. The other nine additions in that diff are CLI-only
(`ThreadFuzzer`, `seedListenerDefaultFormat`, `makeFormatOptionsPrivateToTheClient`, the three
`setSetting` calls that serve the client configuration and the protocol listeners,
`getClientConfiguration().keys()`) or `registerEmbeddedConfig`, which chdb skips deliberately.

**Fix.** Both `initialize()` calls in `EmbeddedServer::initializeThreadPools`, in the order
`LocalServer` uses, with the same settings and the same "zero means CPU cores" rule.

**Why the local checks missed it.** Twice over. The Hazard-5 comparison as run matched
`register*` and `global_context->set*`; a thread pool is initialized through neither. And the
WASM tests that would have caught it — `iceberg-local`, `datalake`, `datalake-unity` — **skip
silently when `.iceberg-venv` is absent**, so running them without the venv looks like a pass.
Only `smoke` and `matrix` had actually run.

**What catches it next time.** Compare the call vocabulary the release *added* to
`LocalServer`, not the whole vocabulary: the whole one is 71 lines of CLI noise, the added one
was 11 and named both gaps. Include `get*ThreadPool().initialize` in that comparison. And
build the data-lake venv before running the WASM suite — a skip is not a pass, and this suite
does not say which it gave you.

## 3. Iceberg fails again on the single-threaded WASM bundle

**Failure.** Found locally, before CI reached it: `iceberg-local` and `datalake` against
`buildwasm-st`, after the fix in §2.

```
Code: 439. DB::Exception: Iceberg iterator is failed with exception:
Code: 439. DB::Exception: Cannot schedule a task: thread creation unavailable
in the single-threaded WASM build (threads=0, jobs=0). (CANNOT_SCHEDULE_TASK)
```

**Root cause.** v26.9's Iceberg reader has **two** layers of concurrency, and the sync only
adapted the outer one. `DataFileEntriesStream`'s constructor spawns a producer thread — chdb's
`CHDB_WASM_SINGLE_THREADED` branch calls `run()` inline instead, and that was ported during
the merge. What the release added *inside* `run()` is a second layer: each manifest is decoded
through `threadPoolCallbackRunnerUnsafe` on the pool from §2. Initializing that pool is what
let execution reach the second layer at all, where a build with no threads refuses.

**Fix.** Under `CHDB_WASM_SINGLE_THREADED`, replace the runner with one that decodes in place
and returns a ready future. The loop below it keeps its shape and `decode_concurrency` degrades
to one manifest at a time, which is all a single thread can do.

**Why the local checks missed it.** The first pass missed it because the whole suite was
skipping (see §2). The second pass caught it, because it ran both bundles — the multi-threaded
one passes all five tests with only the §2 fix, so testing one bundle would have looked clean.

**What catches it next time.** When a release moves work onto a thread pool, grep the file for
*every* scheduling point, not the one the error named. And run the WASM suite against both
bundles: `buildwasm` and `buildwasm-st` differ in exactly the dimension these bugs live in.

## 4. WASM UDF test fails on builds that have no WebAssembly engine

**Failure.** `Test on macOS arm64`, step *Test wheel on all Python versions*, run 35941502864.
`main-shard-1`, one failure out of 652:

```
FAIL: test_wasm_udf_end_to_end (test_wasm_udf.TestWasmUDF)
AssertionError: 1 != 0 : RuntimeError: Code: 60. DB::Exception:
Table system.webassembly_modules does not exist. (UNKNOWN_TABLE)
  ... in wasm_udf_worker.py line 54,
      DELETE FROM system.webassembly_modules WHERE name = '...'
```

**Root cause.** The macOS wheels are cross-compiled with `ENABLE_RUST=0`
(`chdb/build_mac_on_linux.sh`), so they carry no wasmtime and `USE_WASMTIME` is 0. That is
deliberate and unchanged. What changed is how such a build *presents* itself.

v26.9 added a fail-closed branch to `Context::initWasmModuleManager()`: with no engine
compiled in it returns `nullptr`, so `system.webassembly_modules` is never attached and
persisted `LANGUAGE WASM` functions are not loaded at startup. At v26.7 the manager was
created either way — the table existed and only the engine-touching statements raised
SUPPORT_IS_DISABLED.

`wasm_udf_worker.py` detects "no engine in this build" and asks the test to skip. Its detector
matched only the SUPPORT_IS_DISABLED wording, and its comment said so explicitly: *"checking
the table alone is not enough (it exists even without the engine)"*. v26.9 made that sentence
false. The worker's first statement is a `DELETE` for cleanup, so it now raises UNKNOWN_TABLE
before reaching the statements the detector wraps.

**Fix.** Teach `runtime_absent()` the second spelling, and replace the comment that asserted
the old invariant with what the two spellings now mean.

**Why the local checks missed it.** Nothing was skipped this time and nothing was silent — the
local build simply has the engine. `chdb/build.sh` passes `-DENABLE_RUST=1 -DENABLE_WASMTIME=1`,
so `test_wasm_udf` really runs and really passes locally. Only a build without wasmtime takes
the path that broke, and on this machine that is the cross-compiled wheel, which CI produces.

**What catches it next time.** When a release adds a fail-closed branch behind a `USE_*` flag,
list the build variants where that flag is off — for chdb: the macOS cross-builds
(`ENABLE_RUST=0`) and the lite wheel — and ask what each one now reports differently. A test
that probes for an optional feature is written against the *old* wording of its absence.
