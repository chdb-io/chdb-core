# Baseline sync guide

This guide explains the risky parts, common past failures, and the tests that catch them. Terms used below:

- A submodule pointer is the commit recorded for a repository under `contrib/`.
- A mirror is chDB-only code that repeats part of an upstream startup or shutdown path.
- A registration object is a compiled file whose main job is to register a SQL function.
- An export list is the list of C functions visible from the final library.

The main skill controls the process. This guide supplies the technical checks. The preflight script only creates a first-pass inventory; it cannot decide whether a path or conflict is safe.

## Import rules

Use the upstream `old..new` diff as input. For modified or deleted files, import only paths present in the chDB base. For new files, admit only paths inside retained source and build trees. Treat renames as a deletion plus an addition. Keep submodule pointers out of the patch and update them separately.

Use `git diff --binary --full-index` and `git apply --3way --index`. Review every added path. A whole-tree merge often restores programs, tests, docs, or CI files that chDB removed on purpose.

For each conflict:

- take upstream when the chDB side is leftover text from an older sync;
- keep the behavior, adapted to the new API, when chDB added a real feature or fix;
- keep both when the changes are independent;
- decide words or arguments separately when one line contains several changes.

Write the decision before editing. Use this shape in `sync-review/<tag>/conflicts.md`:

```markdown
## path/to/file
- Upstream change:
- chDB change and reason:
- Proposed result:
- Risks or open questions:
- Validation:
- Reviewer decision:
```

Discover conflicts in a disposable worktree first. Regenerate and commit a patch that excludes the conflict files. Then apply the conflict files separately and resolve them after review. Git cannot create the first commit while unmerged index entries remain.

## Risky modules

| Area | What often breaks | What to do |
| --- | --- | --- |
| `EmbeddedServer` | Upstream adds a cache, setting, worker, or shutdown call only to `LocalServer` | Compare setters, config fields, start order, and stop order by hand |
| Memory and jemalloc | Wrong pointer goes to jemalloc; TLS fails after `dlopen`; worker ownership changes | Keep `je_` handling, return foreign pointers to their allocator, compare every config field, test repeated startup and exit |
| Threads, signals, shutdown | A host signal handler is replaced, or an exit hook runs after its data is destroyed | Keep host ownership boundaries and prefer explicit shutdown |
| Submodules | A pinned commit removes a source still named by `<library>-cmake` | Verify the exact commit, fork URL, nested modules, and every listed source file |
| Public C API | A function is declared but missing from Linux or macOS export lists | Run `python3 chdb/build/check_export_contract.py` and test a dynamically loaded host |
| Static library | Registration-only objects are dropped; bundled C++ runtime symbols leak into the host | Add real references in `ForceFunctionReferences.cpp`; run the hermetic check and the real Go consumer |
| Arrow, Parquet, Python | Context or Arrow data dies too early; pyarrow and bundled libraries collide; a dependency differs by CPU | Test ownership, JOIN and streaming paths, pyarrow/torch coexistence, and resolved dependency versions on each architecture |
| Full, lite, WASM | A new function pulls in a missing source, network feature, thread, socket, or OS call | Make an explicit decision for each variant and use separate build directories |
| Toolchains and packaging | One wheel or cross-build script keeps an old compiler, flag, or dependency pin | Search every workflow and install step, then build with the exact new toolchain |

### Embedded-specific review

Every upstream change under `programs/local/LocalServer.cpp` or
`programs/local/LocalServer.h` requires a manual comparison with
`programs/local/EmbeddedServer.cpp` and `programs/local/EmbeddedServer.h`.

Compare:

- server settings and cache setters;
- Context initialization and registration order;
- thread pools, MemoryWorker, jemalloc and background workers;
- user/config/path initialization;
- logger, signal and shutdown ordering.

Do not copy CLI-only listener, stdin, interactive, or query-loop logic into
`EmbeddedServer` without an explicit embedded design decision.

### Client boundary

When `ClientBase` or `LocalConnection` changes, review `ChdbClient`.

Compare:

- Session and Context creation;
- per-connection settings;
- stdin/input ownership;
- progress and cancellation callbacks;
- query context construction;
- cleanup and streaming worker ownership.

### Host lifecycle

When upstream changes `ThreadPool`, `SignalHandlers`, `Logger`,
`CurrentMemoryTracker`, jemalloc, atexit or shutdown code, verify that
embedded mode does not take ownership of host process resources and that all
embedded workers are stopped before Context and global pool destruction.

### Build/runtime boundaries

When upstream changes registration objects, CMake source lists,
`CHDB_LITE`/WASM guards, export lists, or static-library linkage, review the
chDB build and binding surfaces separately.

## Failure patterns to remember

| Symptom | Cause seen before | Early check |
| --- | --- | --- |
| Python module imports, then crashes | Conditional C++ fields changed object layout between build modes | Keep shared object layout stable; place optional fields at the end or hide them behind an implementation object |
| `free()` crashes inside jemalloc | Memory created by libc was freed by jemalloc | Detect foreign pointers and return them to libc |
| Linux wheel fails on another thread | jemalloc used `initial-exec` TLS after being loaded into an existing process | Keep the wheel-safe general dynamic TLS model |
| A setting is ignored | A new config field was missing from a chDB initializer | Compare every field, not only constructor types |
| Server settings crash | `EmbeddedServer` missed new cache setters | Compare `LocalServer` and `EmbeddedServer` setup |
| CMake reports a missing vendored file | A submodule changed but its chDB build list did not | Check every `<library>-cmake` path at the new pointer |
| SQL function is unknown only in the static library | The linker dropped its registration object | Reference its registration function and test the final archive |
| Lite build has unresolved symbols | Registration generation included a feature removed from lite | Review exclusions; do not regenerate blindly |
| WASM build sees `prctl`, sockets, or threads | New OS code lacked a WASM guard | Compile the WASM path when OS-facing code changes |
| A new C function cannot be loaded | One export list was not updated or was not passed to the linker | Compare Linux and macOS lists and inspect final link commands |
| Host hangs after loading chDB | Bundled libc++, protobuf, abseil, or Arrow symbols escaped | Run dynamic-host and static-library isolation tests |
| Only Linux aarch64 corrupts Parquet data | A newly resolved pyarrow or deltalake version differs | Print and compare installed versions before editing C++ |
| Second local build behaves differently | Old extracted objects or an old `:memory:` directory were reused | Remove only the known generated directories before rebuilding |
| Restart or process exit corrupts memory | Pools, allocator state, and engine owners were destroyed in the wrong order | Run connection churn, shutdown, restart, and exit under ASan and UBSan |

## Validation order

1. Run `git diff --check`, check for unmerged files and conflict markers, and rerun the preflight script.
2. Verify changed submodule commits, build-list paths, public C declarations, export lists, registration references, config fields, and embedded setters.
3. Build from an empty directory. Run smoke tests and the complete chdb-core test set.
4. Build full and `CHDB_LITE=1` separately. Build WASM when functions, CMake, OS calls, threads, or networking changed.
5. Run `chdb/build/check_static_lib_hermetic.sh` and the real Go example against the new archive.
6. Install the newly built artifact and run downstream DataStore and binding tests. Do not test an older package from an index.
7. Test the affected platforms and wheel architectures. Print resolved dependency versions in CI.
8. Use ASan and UBSan for allocator, ownership, thread, signal, shutdown, restart, or public-handle changes.

Before repeating a static build, remove only these generated paths if they exist:

```text
chdb/build/libchdb_objects_tmp_dir
chdb/build/create_static_lib_tmp_dir
chdb/build/go-example/:memory:
```

On a native crash, stop guessing. Build with symbols, capture the macOS crash report or Linux core/debugger stack, symbolicate the failing thread, and then compare that code with the new upstream path.

For a CI-only crash, first try to download its core, crash report, symbols, complete logs, dependency list, and test output. If artifacts are missing, reproduce the CI image, compiler, options, dependencies, and test locally. Ask for the missing environment when local reproduction is impossible. Never claim coverage for a skipped environment.

Record each CI follow-up in `sync-review/<tag>/ci-followups.md` with the failure, evidence, root cause, fix, added test, reason local checks missed it, and the skill or CI change that would catch it earlier. Keep one root cause per commit. Preserve these commits instead of squashing them into the baseline commits.

When this skill needs an update, add only a repeatable failure with its cause and early check. Remove obsolete workarounds after proving they are no longer needed.
