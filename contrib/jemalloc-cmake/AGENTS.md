# AGENTS.md — contrib/jemalloc-cmake/

chdb-specific CMake glue around jemalloc. **Edits here are
expected** (this is one of the deliberate divergences from
ClickHouse upstream, not vendored upstream code).

## Why this exists

- chdb builds jemalloc with `--with-jemalloc-prefix=je_` and
  link-wraps `malloc` / `free` / friends via `-Wl,-wrap,malloc`, so
  ClickHouse's allocation calls route through `je_malloc` /
  `je_free` instead of being silently bound to Python's libc
  allocator through the PLT when `chdb.so` loads into a Python
  process.
- Without this, chdb.so's allocations would go through libc
  (a perf hit) and — worse — cross-allocator frees (libc-allocated
  memory like `getcwd`'s return value passed to `je_free`) crash.

## What chdb edits

- `include_<platform>/jemalloc/internal/jemalloc_internal_defs.h.in`
  — most notably so the musl / Linux-aarch64 wheel can build
  cleanly.
- The "fix: restore chdb-specific jemalloc configurations" pattern
  in `git log` is what brings these edits forward through upstream
  syncs.

## Companion submodule

`contrib/jemalloc/` tracks **upstream** `jemalloc/jemalloc` — no
chdb fork URL. The divergence is purely build-flag + cmake glue
(this directory).
