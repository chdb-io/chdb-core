# Contributing to chdb-core

Welcome — and thanks for considering a contribution. All contributors
are expected to be open, considerate, reasonable, and respectful;
see [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

This is the contributor-facing handbook. [`AGENTS.md`](./AGENTS.md)
is short and stays loaded in AI coding agents at all times; it
captures only the cross-subtree design rules. Per-directory
AGENTS.md files (under `programs/local/`, `contrib/`, `chdb/` and
the `contrib/*-cmake/` glue dirs) carry location-specific rules.
Everything human-facing — setup, build, the modify-then-test
workflow, PR conventions, CI, releases — lives here.

> **Disambiguation — chdb-core vs chdb**
> - **chdb-core** (this repository) is a fork of ClickHouse. It
>   builds the C++ engine + a minimal Python C-extension binding,
>   distributed as the `chdb-core` PyPI package. ~3,200 commits,
>   ~130 git submodules, `contrib/` is roughly 9 GB on disk.
> - **chdb** ([chdb-io/chdb](https://github.com/chdb-io/chdb)) is
>   the pure-Python user-facing wrapper: `import chdb`, `from chdb
>   import datastore`, the chdb-ds DataFrame API. It depends on
>   `chdb-core`.
>
> **If you find yourself wanting to change the user-facing Python
> API** (`import chdb`, DataStore, pandas compatibility, query
> result handling), **you are probably in the wrong repository** —
> go to chdb-io/chdb. This file (chdb-core) is for SQL engine,
> parser, formats, codecs, storage, table functions, and the
> C-extension binding.

## What chdb-core is

chdb-core is a fork of ClickHouse carved into an **embeddable** SQL
engine — the same SQL dialect, the same query optimiser, the same
format readers and codecs, but loadable in-process via Python (and
Bun/Go/Rust/Node/Zig/Ruby/.NET via separate sibling repositories
that link against `chdb-core`'s shared library). On GitHub,
`chdb-io/chdb-core` is registered as a fork of `chdb-io/chdb`
(historical — the C++ engine was carved out of chdb's earlier
life), but the kernel tracks ClickHouse upstream and is re-synced
from ClickHouse periodically (see
[`UPSTREAM_SYNC.md`](./UPSTREAM_SYNC.md)).

This repository contains, roughly:

- **The full ClickHouse SQL engine** (`src/`, `base/`, `programs/`,
  `cmake/`, `contrib/`, `utils/`, `packages/`, `docker/`, `ci/`,
  `tests/`)
- **chDB's C++ entry point** (`programs/local/chdb*.cpp`,
  `ChdbClient`, `AIQueryProcessor`, the Arrow / Python interop
  layer)
- **A minimal Python wrapper that ships with `chdb-core`** (`chdb/`)
- **The published `using-chdb` agent skill** (`agent/skills/`)

### Where does my change go?

The full cross-repo routing table lives in
[chdb-io/chdb `CONTRIBUTING.md`](https://github.com/chdb-io/chdb/blob/main/CONTRIBUTING.md).
Quick version for chdb-core's own scope:

- **C++ SQL engine** (functions, types, formats, codecs, storage) →
  `src/`
- **Public C ABI** (the contract every binding links against) →
  `programs/local/chdb*.{cpp,h}` (see
  [`programs/local/AGENTS.md`](./programs/local/AGENTS.md))
- **Minimal Python wrapper bundled in the `chdb-core` wheel**
  (`chdb.query()`, `chdb.session`, `dbapi`) → `chdb/` (this repo)
- **Wheel build scripts** → `chdb/build.sh`
- **Higher-level Python user-facing API** (`from chdb import
  datastore`, pandas compat, the DataStore API) → wrong repo, go to
  chdb-io/chdb

## Reporting issues

Search [open and closed issues](https://github.com/chdb-io/chdb-core/issues)
first. A good issue includes:

- OS + architecture + Python version
- The chdb-core version (`python -c "import chdb; print(chdb.query('SELECT version()'))"`)
- A minimal reproduction (SQL or a short Python script)
- Expected vs observed behaviour

For crashes, include a native stack trace — see "Capture the stack
trace…" below for the gdb/lldb flow.

## Things to avoid (read first — highest-ROI section)

### Prefer the smallest-scope rebuild over `make clean && make`

A full C++ rebuild takes a while. The good news is that for most
changes you only need an incremental build — figure out which layer
your change is in (see "I changed X — what to run" below) and pick
the matching command:

- Pure Python under `chdb/*.py` → no rebuild needed; just reinstall
  the wheel
- C++ under `programs/local/` → incremental ninja build; minutes on
  a primed ccache
- C++ under `src/` (engine internals) → incremental still works;
  larger blast radius depending on header reach
- `contrib/` → don't (see "Treat `contrib/` as read-only" below)

A primed ccache makes a big difference for iteration speed, so it's
worth keeping it healthy between branches.

> ⚠️ **`make clean` (and therefore `make build`) currently removes
> tracked files under `chdb/build/`** (`build_static_lib*.sh`,
> `create_*_libchdb.py`, `cpp-example/`, `go-example/`, etc.)
> because `tox -e clean` `rmtree`s the whole directory rather than
> just build artefacts. After a `make build`, restore them with
> `git checkout HEAD -- chdb/build/`. (Bug in `tox.ini`'s clean
> recipe; not part of an ordinary contributor PR.)

### Treat `contrib/` as read-only; route fixes upstream

`contrib/` is **~9 GB across ~260 entries, with ~130 git
submodules**. Every entry is a vendored upstream library — Abseil,
Arrow, Boost, RocksDB, ZSTD, ICU, OpenSSL, libcxx, … The vast
majority track ClickHouse upstream submodule pins.

If you find a bug in a `contrib/` library:

1. Resist the urge to patch it locally — the patch won't survive
   the next upstream sync.
2. Check whether ClickHouse upstream has already fixed it (their
   submodule pin may be ahead of ours).
3. If a real upstream fix is needed, the path is: PR the upstream
   library directly, then update the submodule pointer in ClickHouse
   upstream, then sync that change into chdb-core via the
   upstream-sync flow (see
   [`UPSTREAM_SYNC.md`](./UPSTREAM_SYNC.md)).

chdb-specific divergences (jemalloc / arrow / pybind11 / postgres
cmake glue) live in `contrib/*-cmake/` dirs; see each of those
dirs' AGENTS.md for what's intentional.

### Coordinate ClickHouse / contrib submodule bumps via an issue

This repository is an **active fork of ClickHouse**. Submodule
bumps need to consider:

- chdb-core's diverged patches (some files in `src/` and
  `programs/local/` are chdb-specific and may conflict with upstream
  changes)
- ABI compatibility for the C-binding consumed by `chdb-io/chdb`,
  `chdb-bun`, `chdb-go`, `chdb-rust`, `chdb-node`, `chdb-zig`,
  `chdb-ruby`, …
- Build-matrix impact (a new submodule version may add a new
  transitive toolchain requirement, breaking macOS arm64 or Linux
  musl)

If you think a submodule bump is needed, **open an issue first**
describing why, and let a maintainer drive the bump. The same
applies to enabling a new CMake feature flag in `chdb/build.sh`
(`-DENABLE_*`): each one has a binary-size and platform-compatibility
cost, so it's a project-wide decision worth discussing in an issue.

### Open an issue before changing SQL dialect or function semantics

chDB inherits the entire ClickHouse SQL surface — every function,
table engine, format, type, setting. Users *and* downstream language
bindings (Bun/Go/Rust/Node/Zig/Ruby) treat the dialect as stable.

Changing function semantics, adding a function, or removing one is
a user-facing API change, even if your patch is "just" in
`src/Functions/` or `src/AggregateFunctions/`. Open an issue first
with the proposed change, the rationale, and a migration story —
that way the discussion happens before you've written the code, and
the eventual PR is fast to review.

### Understand the C ABI before refactoring `programs/local/chdb*.cpp`

`programs/local/` contains chDB's *public C ABI*: `chdb.h`,
`chdb.cpp`, `ChdbClient`, `AIQueryProcessor`, the Arrow stream
registry, the Python interop wrappers. This ABI is what the Python
wrapper, Bun binding, Go binding, Rust binding, Node binding, Zig
binding, and Ruby binding all link against.

A "cleanup" refactor that changes function signatures, struct
layouts, or the way memory is owned across the boundary can break
every binding repository in the chDB ecosystem without an obvious
local test failure. Before changing anything in
`programs/local/chdb*.{cpp,h,hpp}`, please read
[`programs/local/AGENTS.md`](./programs/local/AGENTS.md) — it
covers the opaque-pointer pattern and the kinds of changes that
need a coordinated version bump downstream.

### Capture the stack trace before changing code on a crash

When tests crash with `SIGSEGV`, `SIGABRT`, `SIGFPE`, `SIGILL`,
`SIGBUS`, or `SIGSYS`, **always obtain the stack trace first**,
then analyse the root cause before attempting fixes. C++ engine
crashes usually point at:

- A real bug in the engine (most common)
- A refcounting / lifetime issue at the C-binding boundary
- An ASAN / UBSAN-detectable issue

Specifically, do *not*:

- Remove the failing assertion
- Add a defensive `try/catch` that swallows the error
- Mark the test as skipped to make CI green

Capture the trace (`gdb` on Linux, `lldb` on macOS, or core dump +
post-mortem analysis) and fix the root cause. If the cause is in a
ClickHouse upstream file, the fix may need to land in upstream
first.

## Setting up

The first build is heavyweight — budget hours of CPU time and
~50–60 GB of free disk. **`chdb/build.sh` requires Python 3.9 on
`PATH`** (abi3 / Limited-API anchor; built wheel runs on 3.9–3.14).
Subsequent incremental builds are fast.

```bash
git clone https://github.com/chdb-io/chdb-core
cd chdb-core
git submodule update --init --recursive
python3 --version                           # must report 3.9.x
make build                                  # = clean buildlib wheel
pip install dist/chdb_core-*.whl --force-reinstall
cd /tmp && python -c "import chdb; print(chdb.query('SELECT 1+1'))"
```

Full toolchain details (brew vs. pip-only paths, `tox` requirement,
ccache notes), the wheel-filename vs. engine-version distinction,
the "run sanity check from /tmp, not from the repo" caveat, and the
end-to-end verification path against
[`chdb-io/chdb`](https://github.com/chdb-io/chdb) all live in
[`BUILD.md`](./BUILD.md).

## I changed X — what to run

After the first-time setup, pick your row and run the rebuild +
verify commands top-to-bottom. The canonical test entry is
`make test` (= `cd tests && python3 run_all.py`), and **the full
suite is what you sign off on** — see the callout below. Targeted
pytest is fine for tight iteration while you're debugging a
specific failure, but it isn't sufficient for sign-off.

| You changed | Rebuild? | Rebuild + verify commands |
|---|---|---|
| **A. C++ in `programs/local/`** (chDB entry point — C ABI) | Incremental | `cd chdb && bash build.sh` (ninja rebuilds only what's affected; minutes with primed ccache) → `make test` (full, ~2 min). If you changed the ABI surface, also smoke-test a downstream binding (chdb-go / chdb-bun / etc.) — see [`programs/local/AGENTS.md`](./programs/local/AGENTS.md). |
| **B. C++ in `src/`** (engine internals) | Incremental | Same as A. Larger blast radius depending on header reach. The full `make test` is the default; targeted `tests/test_<area>.py` is fine as a faster inner loop while iterating. |
| **C. SQL function / type behaviour (`src/Functions/*`, `src/AggregateFunctions/*`, `src/DataTypes/*`)** | Incremental | Same as A + add a `tests/test_*.py` case that fails before the change. Because this is user-visible, open an issue describing the change before the PR. |
| **D. Python wrapper under `chdb/*.py`** | No | `pip install -e .` once → from outside the repo: `cd /tmp && python3 -c "import chdb; print(chdb.query('SELECT 1+1'))"` → `make test` |
| **E. Tests (`tests/test_*.py`)** | No | `make test` (full, ~2 min). Targeted `pytest tests/test_<file>.py -v` is fine while iterating on the new test itself. |
| **F. Docs (`docs/`)** | No | `make docs` (HTML on :8001) or `make docs-md` (markdown) |
| **G. CI workflows** (`.github/workflows/` or `ci/`) | No | Iterate via PR; `pr_ci.yaml` is cheap |
| **H. `contrib/` libraries** | — | **Don't — see "Treat contrib/ as read-only" above** |
| **I. Submodule pointers** | — | **Open an issue first — see "Coordinate ClickHouse / contrib submodule bumps" above** |

> ⚠️ **Prefer the full suite when in doubt.** chdb-core changes —
> especially anywhere in `src/` or `programs/local/` — routinely
> surface failures in unexpected places: SQL function tweaks fall
> over in format-reader paths, ABI changes break Arrow zero-copy
> tests, allocator tweaks show up in jemalloc-sensitive corners. If
> your change touches more than one row above, or you're not sure
> which code paths it reaches, **just run the full `make test`**
> (~2 min once the build is incremental).

**Before opening a PR**: lint your Python changes
(`flake8 chdb --count --show-source --statistics`) plus the full
`make test` from row A / B / C.

## Day-to-day commands

```bash
bash chdb/build.sh                              # incremental C++ rebuild
make test                                        # full test suite (~2 min)
flake8 chdb --count --show-source --statistics   # Python lint
make docs                                        # docs preview on :8001
```

Full reference (release-wheel build, platform-specific scripts,
hygiene, ccache reset, submodule re-pin, the `chdb/build/` cleanup
quirk) is in [`BUILD.md`](./BUILD.md).

## Code style

### C++

- **`clang-format`** — `.clang-format` exists at repo root; run
  before committing: `clang-format -i <file>`
- **`clang-tidy`** — `.clang-tidy` exists; CI runs static analysis
  on changed files
- **ClickHouse coding conventions** apply — naming (`PascalCaseClasses`,
  `snake_case_methods`, `ALL_CAPS_CONSTANTS`), header-include order,
  smart-pointer use, exception hierarchy. ClickHouse upstream's
  `docs/en/development/style.md` is the authoritative reference;
  deviating from it is a review-time pushback.

### Python

chdb-core's Python lint is `[flake8]` in `setup.cfg`, scoped to
critical errors only (`E9, F63, F7, F82`), line-length 120, ignore
`F811`, per-file `__init__.py:F401`. black / mypy / pre-commit are
listed in `requirements-dev.txt` but not enforced as PR gates.

### Don't run the formatter over the whole repo

Mass-format diffs are noise that drowns the actual review. Only
format the files you've already changed.

## Testing

The test runner is `tests/run_all.py` (invoked via `make test`).
It runs Python integration tests against the built wheel, so make
sure `import chdb` works first.

### Format-cases generator

`tests/gen_format_cases.py` produces `tests/format_output.py` (a
Python dict consumed by other tests). After adding or changing a
format reader/writer, edit the `formats = […]` list inside the
generator, re-run it, and commit the regenerated
`tests/format_output.py`.

### When a test crashes

See "Capture the stack trace before changing code on a crash" above
— capture the stack trace before changing code. The first line of
defence is `lldb` (macOS) / `gdb` (Linux). Set
`PYTHONFAULTHANDLER=1` for a Python-side trace too.

### Inherited ClickHouse test scaffolding (not active in chdb-core)

`tests/sqllogic/`, `tests/performance/`, and the C++ unit tests
under `src/*/tests/` are ClickHouse-upstream leftovers — not in
use in chdb-core. Don't add tests there expecting them to be
picked up.

## ClickHouse upstream sync flow

chdb-core is an active fork of
[ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse).
Submodule bumps and `src/` rebases are sequenced by maintainers
(rather than landing as drive-by PRs) because they affect the C
ABI, the build matrix, and a handful of chdb-specific divergences
in `contrib/`.

For routing of upstream-bug reports, submodule changes, `contrib/`
divergences, and the issue-first path, see
[`UPSTREAM_SYNC.md`](./UPSTREAM_SYNC.md). For chDB-specific C++
work (in `programs/local/` or chDB-only spots in `src/`), a regular
PR works — see [`programs/local/AGENTS.md`](./programs/local/AGENTS.md)
for the ABI rules.

## PR & commit conventions

**PR titles** — follow ClickHouse style: **start with a capitalised
verb, no Conventional-Commit prefix**. Describe user impact, not
internal mechanics.

Good:

- `Add setting to disable the Python function`
- `Resolve jemalloc / glibc allocator mismatch on musl`
- `Improve hash-join memory usage on 1B-row tables`
- `Simplify ChdbClient lifecycle`

Avoid:

- `feat: add setting to disable the Python function` (no
  Conventional-Commit prefix)
- `[Feature] Add setting...` (no brackets)
- `feat(local): add setting...` (no scope notation)
- `Refactored FunctionFactory::resolve_overloads to take a span`
  (describes mechanics, not user impact)

**Commit messages** inside a PR can be lowercase / informal as long
as the PR title itself follows the rule above. chdb-core uses merge
commits (not squash), so each commit title still lives in `git log`
forever — keep them readable.

**Branch names** — descriptive with a category prefix:
`fix/array-map-null`, `feat/new-table-function`,
`docs/update-readme`, `refactor/cleanup-imports`. External
contributors may also use `<github-handle>/<topic>`.

**Scope** — one concern per PR. Split refactors away from fixes.

**Tests** — every behaviour change needs one. C++ engine bug fixes
land with a `tests/test_*.py` case that fails before the change.

**PR description** — The inherited `PULL_REQUEST_TEMPLATE.md` from
ClickHouse upstream applies; just skip the ASAN/TSAN batch checkboxes
that target ClickHouse's CI rather than ours.

### Opening a PR when you can't fork chdb-core

`chdb-io/chdb-core` is itself a fork of `chdb-io/chdb` (the C++
engine was carved out of the Python repo's earlier life). The two
upstreams sit in the same GitHub fork network, which means **once
you have a fork of `chdb-io/chdb` at `<your-handle>/chdb`, GitHub
won't let you also create a fork of `chdb-io/chdb-core`** — same
network, one fork per account.

The working approach is to piggy-back on your existing `chdb` fork.
Push your chdb-core branch into it under a distinct name and PR
from there:

1. Make sure `<your-handle>/chdb` exists on GitHub (forking
   `chdb-io/chdb` is the prerequisite).
2. In your local chdb-core checkout, add a remote pointing at your
   chdb fork and push the branch under a chdb-core-prefixed name:

   ```bash
   git remote add via-chdb-fork git@github.com:<your-handle>/chdb.git
   git push via-chdb-fork <your-branch>:<your-branch>-chdb-core
   ```

3. After the push, the `chdb-io/chdb-core` repo page on GitHub will
   show a "recent push" banner with **Compare & pull request** —
   click it, double-check the diff contains only your intended
   changes (not the entire chdb-core tree), and open the PR.

   If GitHub does not show the banner, manually open:

   ```
   https://github.com/chdb-io/chdb-core/compare/main...<your-handle>:chdb:<your-branch>-chdb-core
   ```

## CI

To reproduce CI locally:

```bash
flake8 chdb --count --show-source --statistics    # lint
make build                                         # full clean + buildlib + wheel (slow)
make test                                          # full test suite
```

## Security

- **No secrets in test fixtures.** S3 keys, ClickHouse Cloud DSNs,
  OAuth tokens, signed JWT samples — none of them belong in
  `tests/` or `examples/`. Use environment variables;
  `pytest.skip` if not set.
- **Stack traces matter.** C++ engine crashes signal real bugs;
  capture and diagnose, don't paper over.
- **Vulnerability reports** go through GitHub security advisories,
  not public issues. See [`SECURITY.md`](./SECURITY.md).
- **Supply chain**: every `contrib/` change is a supply-chain risk
  (see "Treat `contrib/` as read-only" and "Coordinate ClickHouse /
  contrib submodule bumps" above).

## Related files

- [`AGENTS.md`](./AGENTS.md) — minimal always-loaded design rules
  for AI coding agents
- [`BUILD.md`](./BUILD.md) — long-form build / test manual
- [`UPSTREAM_SYNC.md`](./UPSTREAM_SYNC.md) — how chdb-core stays in
  sync with ClickHouse upstream
- Subdirectory `AGENTS.md` files (override the root for files
  inside them — agents.md spec):
  - [`programs/local/AGENTS.md`](./programs/local/AGENTS.md) — C
    ABI stability rules
  - [`contrib/AGENTS.md`](./contrib/AGENTS.md) — vendored libs +
    pointer to per-cmake-dir AGENTS.md
