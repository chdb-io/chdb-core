# AGENTS.md — chdb-core

## 1. This repository is the C++ engine

chdb-core is an active fork of ClickHouse/ClickHouse. If a change
isn't about the C++ engine, SQL surface, format readers / codecs /
storage, the public C ABI, or the wheel build, it probably belongs in
[`chdb-io/chdb`](https://github.com/chdb-io/chdb) (the Python
user-facing wrapper) — not here.

## 2. Two boundaries every agent must respect

- **`programs/local/` is the chDB extension surface; `src/` is
  upstream ClickHouse territory.** chDB-specific C++ logic goes in
  `programs/local/`. Anything in `src/` should be either a real
  ClickHouse bug fix (preferably landed upstream first) or a small
  chdb-specific patch that survives upstream syncs. See
  [`programs/local/AGENTS.md`](./programs/local/AGENTS.md) for the
  public C ABI rules — those changes ripple through every chDB
  binding repo.
- **`contrib/` is vendored upstream — don't patch it directly.** The
  next sync overwrites local edits. Bugs in vendored libs get fixed
  upstream then synced in. chdb-specific cmake glue lives in
  `contrib/<lib>-cmake/`; see each of those dirs' AGENTS.md for what's
  intentional divergence.

## 3. Things that look "small" but cost hours

- **Don't reflexively `make clean && make`.** A full rebuild is
  hours; pick the smallest-scope command for your change (see
  `CONTRIBUTING.md` → "I changed X — what to run").
- **Don't bump submodules without an issue first.** Submodule pins
  affect the C ABI, build matrix, and chdb-specific divergences. A
  drive-by PR will be asked to convert to an issue.
- **Don't change SQL function / type semantics without an issue
  first.** chDB inherits the entire ClickHouse SQL dialect, and every
  binding (chdb-go, chdb-bun, …) treats it as stable.

## 4. Capture the stack trace before changing code on a crash

When a test crashes with `SIGSEGV` / `SIGABRT` / `SIGFPE` / `SIGILL`
/ `SIGBUS` / `SIGSYS`, **always obtain the stack trace first**, then
analyse the root cause. Specifically, do *not*:

- remove the failing assertion
- add a defensive `try/catch` that swallows the error
- mark the test as skipped to make CI green

Capture the trace (`gdb` on Linux, `lldb` on macOS) and fix the bug
where it actually lives — often at the engine binding boundary.

## 5. Sign off with the full test suite

The C++ engine's blast radius is hard to predict — SQL function
tweaks fall over in format-reader paths, ABI changes break Arrow
zero-copy tests, allocator tweaks show up in jemalloc-sensitive
corners. Targeted pytest is fine for inner-loop debugging, but
**run `make test` before declaring a change done** — incremental
ccache makes the full suite cheap (~2 min) once the build is
warmed up.

---

*Standard followed: [agents.md](https://agents.md).*
