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

`git apply --3way` refuses a file upstream deleted and chDB modified, and one refusal rolls the
whole patch back: the run reports `patch does not apply` and leaves nothing applied, however many
other files merged. Collect those paths from the first attempt, exclude them, apply again, and
handle them by hand — mirroring the deletion is usually right, but each one is a decision.

**A clean merge can still delete chDB code.** Three-way merge only reports a conflict when both
sides touch the same lines. When upstream restructures a region that happens to contain a
chDB-only addition, the merge resolves in upstream's favour silently. Reviewing the conflicts is
therefore not enough. After resolving, audit every path where chDB had diverged and upstream also
changed something:

```bash
# For each such path: lines chDB added that are now in neither the merged file nor upstream
# were dropped by the merge.
git diff --unified=0 <old-tag> <base> -- "$path" | grep '^+' | grep -v '^+++'
```

Lines that are absent from both the working tree and `<new-tag>` are losses. Files resolved as
"take upstream" will show hits — **read them, do not assume they are intended.** Filter out
comments and formatting and look at what is left: a dropped declaration, a dropped call site, a
dropped `if (TARGET ...)` guard. A chDB-only function whose declaration and call site are gone
while its definition stays only fails at `-Wmissing-prototypes`, which is late and looks
unrelated.

**`git checkout --theirs` takes the whole file, not the conflicting hunk.** After
`git apply --3way`, stage 3 is the new release's version of the file, so resolving a conflict
that way discards *every* chDB change in it, including the ones that merged without complaint.
That is the right move only for a file being deliberately returned to upstream wholesale. For a
file where the conflict is incidental — a platform macro, a renamed constant — resolve the hunk
in place, or redo that one file properly:

```bash
git checkout <base> -- "$path"                    # chDB's version back
git diff --binary --full-index --no-renames <old-tag> <new-tag> -- "$path" > /tmp/one.patch
git apply --3way /tmp/one.patch                   # now only the real hunk conflicts
```

This is worth being strict about: a one-line macro conflict in a file that also carries a
chDB feature will take the feature with it, and the build only notices if something else
references it. Two examples from one sync: a zero-copy column path and a whole allocator's worth
of `je_`-prefixed calls, both in files whose only conflict was a platform predicate.

**Sweep the whole tree for includes of headers the release deleted, not just chDB-only files.**
Any file that keeps a chDB-side include list can name a header that is now gone, and the build
reports them one at a time — one rebuild per header. Get them in a single pass instead:

```bash
DIRS=$(ls -d src/*/ | sed 's|src/||; s|/||' | tr '\n' '|' | sed 's/|$//')
grep -rhoE '^#include[[:space:]]*<[^>]+>' src programs --include='*.cpp' --include='*.h' \
  | sed -E 's/.*<([^>]+)>.*/\1/' | grep -E "^($DIRS)/" | sort -u \
  | while read -r h; do [ -f "src/$h" ] || echo "MISSING: $h"; done
```

Split the result by whether the header existed at the old tag: gone between the tags means this
sync broke it; absent at both means it is pre-existing (`CLICKHOUSE_CLOUD`-only headers and
directories chDB keeps but does not build both land here) and not yours to fix.

### Keeping a subsystem the release deleted

Sometimes the right answer to "upstream deleted this and chDB needs it" is to keep it for one
release and port later. That decision has a tail: **an upstream deletion is transitive.** What
goes with the subsystem is everything only it used — helper classes, a settings field, the
setting's declaration, the registration that selected it. Each one surfaces as a separate build
failure, one rebuild apart, unless they are collected up front.

Before keeping anything, enumerate its dependencies against the new tag:

- every type it names — `git cat-file -e <new-tag>:src/<path>` on the header, and check the class
  is still *declared* there, not merely that the file exists;
- the settings it reads. A setting moved to `MAKE_OBSOLETE` still compiles at every call site and
  silently stops doing anything, which is worse than a build error. Grep the new tag for a
  consumer of the setting, not just for its declaration;
- the registration entry point. If the replacement now owns it, the kept code must not define it
  twice — put the dispatch in the replacement's registration and strip it from the kept file.

Write the list into the review document before resolving, so the cost of keeping is visible next
to the cost of porting.

## Risky modules

| Area | What often breaks | What to do |
| --- | --- | --- |
| `EmbeddedServer` | Upstream adds a cache, setting, worker, shutdown call **or registration** only to `LocalServer` | Diff `LocalServer`'s whole call vocabulary against `EmbeddedServer`'s, not just `global_context->set*`: `register*()`, `*Registry::register*`, config fields, start order and stop order. A missing registration compiles fine and fails at run time with an `UNKNOWN_*` error naming something that looks unrelated |
| Memory and jemalloc | Wrong pointer goes to jemalloc; TLS fails after `dlopen`; worker ownership changes | Keep `je_` handling, return foreign pointers to their allocator, compare every config field, test repeated startup and exit |
| Threads, signals, shutdown | A host signal handler is replaced, or an exit hook runs after its data is destroyed | Keep host ownership boundaries and prefer explicit shutdown |
| Submodules | A pinned commit removes a source still named by `<library>-cmake` | Verify the exact commit, fork URL, nested modules, and every listed source file |
| Public C API | A function is declared but missing from Linux or macOS export lists | Run `python3 chdb/build/check_export_contract.py` and test a dynamically loaded host |
| Static library | Registration-only objects are dropped; bundled C++ runtime symbols leak into the host | Add real references in `ForceFunctionReferences.cpp`; run the hermetic check and the real Go consumer |
| Arrow, Parquet, Python | Context or Arrow data dies too early; pyarrow and bundled libraries collide; a dependency differs by CPU | Test ownership, JOIN and streaming paths, pyarrow/torch coexistence, and resolved dependency versions on each architecture |
| Full, lite, WASM | A new function pulls in a missing source, network feature, thread, socket, or OS call | Make an explicit decision for each variant and use separate build directories |
| Version pins | A pin still names the old ClickHouse line, and CI is the first to say so. `chdb/vars.sh` restamps `CHDB_VERSION` in `programs/local/chdb.h` from `git describe` on every build, so the edit vanishes from the working tree and looks un-done - commit it and verify with `git show HEAD:programs/local/chdb.h`, which is what CI reads | Bump all three: `CHDB_VERSION` in `programs/local/chdb.h`, `SHORT_VERSION` in `programs/local/adbc/validation/python/tests/engine_version.py`, and the engine version plus `VERSION_GITHASH` embedded in the `tests/format_output.py` Parquet golden |
| New upstream source trees | A directory that is new in this release compiles nowhere, or compiles where chDB did not want it | Give every new `src/Functions/<Dir>` and every new top-level source file an explicit decision: add it to `add_headers_and_sources`, and add it to the `CHDB_LITE` drop list if its dependencies are dropped there |
| Statement classification | A release adds, renames or deletes a parser, and chDB's `programs/local/QueryClassifier.cpp` silently mis-classifies it. A new persistent object that lives beside the databases falls through `classifyByQueryKind` to `MUTATING`, which tells a durable control plane a database checkpoint carries it - and none does | Diff `src/Parsers/**` `registerStatement*` / parser classes between the two tags. For each addition decide the class from *where the statement's effect is stored*, not from its keyword: a global factory or the access store means `MUTATING_GLOBAL`. `examples/chdbDurableAbiTest.c` enumerates the engine's parser set and fails on both directions, so it is the check - but it only runs in the four wheel workflows |
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
| `Unknown query plan step`, `Unknown statement`, or another `UNKNOWN_*` at run time | A registration new in this release was added to `LocalServer` but not to chDB's `EmbeddedServer`, which is the path the Python and C APIs actually take | `comm -13` the two files' `register*` call sets, then filter to the ones new since the old tag |
| A setting is ignored | A new config field was missing from a chDB initializer | Compare every field, not only constructor types |
| Server settings crash | `EmbeddedServer` missed new cache setters | Compare `LocalServer` and `EmbeddedServer` setup |
| CMake reports a missing vendored file | A submodule changed but its chDB build list did not | Check every `<library>-cmake` path at the new pointer |
| SQL function is unknown only in the static library | The linker dropped its registration object | Reference its registration function and test the final archive |
| A Go cgo consumer fails to link against the minimized archive with an undefined vtable or typeinfo | `create_minimal_libchdb.py` dropped an object the link map asked for. Hand-written exclusions accumulate there and only bite when a release makes the kept set reference the excluded object - v26.9's AST JSON serialization made `ASTAlterQuery.cpp` instantiate `JSONObjectReader::readChildOfType<ASTSQLSecurity>`, and an exclusion carried since 2025-08 turned into an undefined `typeinfo for DB::ASTSQLSecurity` | Compare `wc -l chdb/build/chdb_objects.txt` against the minimizer's own `Found required object files: N`. A difference is the whole diagnosis: read the script for the filters between the two numbers. Confirm in minutes rather than by rebuilding - `ar x` the object out of `libchdb.a`, `ar q` it onto `libchdb_minimal.a`, rerun `test_go_example.sh` |
| `undefined symbol: registerFunction<Name>` at the final link | `ForceFunctionReferences.cpp` still names a function the release deleted. The same audit that finds missing references reports these, in the other direction — act on both halves of its output, not just the missing half | Diff the file's `extern` set against `REGISTER_FUNCTION` in `src/Functions/**` **both ways** before building |
| Lite build has unresolved symbols | Registration generation included a feature removed from lite | Review exclusions; do not regenerate blindly |
| The lite module builds and links, then fails to `dlopen` with a missing vtable | A translation unit on the lite drop list now carries an *explicit template instantiation* that a kept translation unit declares `extern template` for. The release created the coupling; dropping the file was safe until then | For each file on the drop list, grep the new tag for `template class` in it and for matching `extern template` elsewhere. A load-time symbol error, not a link error - only starting the lite module finds it |
| WASM build sees `prctl`, sockets, or threads | New OS code lacked a WASM guard | Compile the WASM path when OS-facing code changes |
| A new C function cannot be loaded | One export list was not updated or was not passed to the linker | Compare Linux and macOS lists and inspect final link commands |
| Host hangs after loading chDB | Bundled libc++, protobuf, abseil, or Arrow symbols escaped | Run dynamic-host and static-library isolation tests |
| Only Linux aarch64 corrupts Parquet data | A newly resolved pyarrow or deltalake version differs | Print and compare installed versions before editing C++ |
| Second local build behaves differently | Old extracted objects or an old `:memory:` directory were reused | Remove only the known generated directories before rebuilding |
| A chDB feature is gone and nothing conflicted | Upstream restructured the region around a chDB-only addition, so the merge took upstream's version without reporting anything | Audit dropped chDB lines after resolving (see Import rules) |
| A chDB caller suddenly cannot reach a base-class method | The release's header was taken with an access label chDB deliberately drops. One `private:` moved 34 members at once in v26.9's `ClientBase.h`, where chDB keeps the section public because `ChdbClient` drives `ClientBase` rather than subclassing its hooks | Compare the access level of every member against chDB's header, not just the ones that conflicted; treat a difference as chDB-spec when upstream's level is the same at both tags |
| A class member is declared twice | Either "keep both" on a header where upstream moved the member behind a new public entry point, or both sides independently adding the same declaration — the second needs no conflict at all | Scan every changed header for a declaration that appears more often in the merged file than in *either* source; that comparison separates a merge artefact from a pre-existing overload |
| An optional contrib target is linked unconditionally | chDB guards optional targets with `if (TARGET …)` or `$<TARGET_EXISTS:…>`; upstream links them outright, and the merge took upstream's form | Configure is enough to catch it — `target_link_libraries … but the target was not found` names the target |
| The lite build stops linking after a release adds functions | New `src/Functions/ai*.cpp` (or similar) compile in lite while the helpers they need are on the drop list | Diff the new release's function sources against the `CHDB_LITE` drop list before building |
| `runDurableAbiTest` says a parser has no row, or that a row names a parser the engine no longer has | The release changed the parser set. Both halves matter: a missing row means an unclassified statement, a stale row means chDB still reasons about a statement that is gone | Run `examples/runDurableAbiTest.sh` after the first successful build, not at PR time; it names every parser in both directions in one run |
| A chDB classification for an AST the release "deleted" | The type was **renamed**, not removed - v26.9 turned `ASTHypotheticalIndexQuery` into `ASTHypotheticalObjectQuery` and kept the syntax working. Deleting the classification makes the statement fall through to a wrong class, and nothing fails to compile | Before deleting a branch whose header vanished, grep the new tag for the statement's *keywords* rather than its type name. Syntax that still parses means the type moved; only syntax the new parser set rejects (v26.9's `WATCH`, with live views) is a real deletion |
| `libchdb.so` fails to link with `relocation R_X86_64_TPOFF32 ... cannot be used with -shared` (or `R_AARCH64_TLSLE_*`) | A release added code that reaches thread-local storage from inline assembly, which spells the local-exec relocation itself — legal in the executables upstream builds, illegal in chDB's shared object. v26.9's `src/Common/FiberLocal.h` is the case. `-ftls-model` cannot fix it: the relocation is in the asm, not chosen by the compiler | Build `libchdb.so`, not only `libchdb.a` — the static archive links into an executable and never rejects these. To confirm a suspect header in seconds, compile it `-fPIC` and link it `-shared` alone. Prefer the portable branch such a header usually already carries, gated by a chDB macro, over deleting upstream's fast path |
| Restart or process exit corrupts memory | Pools, allocator state, and engine owners were destroyed in the wrong order | Run connection churn, shutdown, restart, and exit under ASan and UBSan |

### Attributing a failing test after a baseline bump

A golden that changed is not automatically an upstream change — chDB's own resolution can move
behaviour too. What makes attribution solid:

- the chDB code involved is byte-identical to the base branch;
- the result does not depend on a choice made while resolving (test it — swapping two merged
  blocks and rebuilding costs one link and settles the question);
- upstream churn in the same area is real and named (file, line counts, submodule bump).

An upstream behaviour change usually breaks more than one test, in more than one language.
The `Nullable(Tuple)` default flip in v26.9 broke a Python test and a C-API example that
encoded the same assumption; fixing only the one the suite reported leaves the other for CI.
After fixing a test for a behaviour change, grep the tree for the same assumption.

Write those three into the test's comment, not "upstream changed this". The next person then
does not have to repeat the experiment. And regenerate binary goldens from the engine rather
than editing bytes: a hand-edited Parquet blob hides which field actually moved.

## Validation order

1. Run `git diff --check`, check for unmerged files and conflict markers, and rerun the preflight script.
2. Run the checks that cost seconds and that CI will run anyway: `.github/scripts/check-abi-version.sh`
   (it fails until `CHDB_VERSION` names the new ClickHouse line) and
   `python3 chdb/build/check_export_contract.py`. A configure-only run (`cmake` without `ninja`)
   catches missing contrib targets for the price of a minute.
3. Verify changed submodule commits, build-list paths, public C declarations, export lists, registration references, config fields, and embedded setters.
4. Build from an empty directory. Run smoke tests and the complete chdb-core test set.
5. Build full and `CHDB_LITE=1` separately. Build WASM when functions, CMake, OS calls, threads, or networking changed.
6. Run `chdb/build/check_static_lib_hermetic.sh` and the real Go example against the new archive.
7. Rerun the ADBC validation suite under `programs/local/adbc/validation` against the new engine. Bumping its version pin without rerunning it defeats the pin.
8. Install the newly built artifact and run downstream DataStore and binding tests. Do not test an older package from an index.
9. Test the affected platforms and wheel architectures. Print resolved dependency versions in CI.
10. Use ASan and UBSan for allocator, ownership, thread, signal, shutdown, restart, or public-handle changes.

### Five audits worth scripting

Each of these found a real regression in one sync, and each is a whole-tree pass that costs
seconds — far cheaper than the build discovering them one rebuild at a time. Run all five after
resolving, before the first build.

1. **Dropped chDB lines** — chDB additions now absent from both the working tree and the new tag
   (see Import rules).
2. **Duplicated members** — a declaration appearing more often in the merged header than in
   *either* source. Catches both "keep both" mistakes and the case where chDB and upstream
   independently added the same thing, which produces no conflict at all.
3. **Changed access levels** — a member whose `public`/`protected`/`private` section differs from
   chDB's header while upstream's is identical at both tags. A single misplaced label moves every
   member below it.
4. **Virtual signatures** — for every upstream base class a chDB-only type derives from, diff the
   base's virtual declarations between the two tags. A changed signature leaves the chDB override
   hiding rather than overriding, the class stays abstract, and the error surfaces as
   `field type ... is an abstract class` inside `shared_ptr`, naming libc++ and not the override.
   This hits configurations most builds never compile: in one sync only the WASM build reached
   `WasmWebObjectStorage`, so three green builds had already gone by.
5. **Constructor arity** — for every upstream type a chDB-only file instantiates, diff the
   constructor declaration between the two tags. chDB-only files appear in no upstream diff, so a
   new parameter reaches them only as a template-instantiation error deep inside `shared_ptr`,
   naming libc++ rather than the call site. When a new flag appears, pick the value that
   reproduces the old behaviour and say so at the call site, rather than the one that reads best.

All five compare the merged file against **chDB's version and both upstream tags**, not against
one of them. Two-way comparison cannot tell a deliberate chDB difference from upstream drift,
which is the whole question.

On a native crash, stop guessing. Build with symbols, capture the macOS crash report or Linux core/debugger stack, symbolicate the failing thread, and then compare that code with the new upstream path.

For a CI-only crash, first try to download its core, crash report, symbols, complete logs, dependency list, and test output. If artifacts are missing, reproduce the CI image, compiler, options, dependencies, and test locally. Ask for the missing environment when local reproduction is impossible. Never claim coverage for a skipped environment.

Record each CI follow-up in `sync-review/<tag>/ci-followups.md` with the failure, evidence, root cause, fix, added test, reason local checks missed it, and the skill or CI change that would catch it earlier. Keep one root cause per commit. Preserve these commits instead of squashing them into the baseline commits.

When this skill needs an update, add only a repeatable failure with its cause and early check. Remove obsolete workarounds after proving they are no longer needed.
