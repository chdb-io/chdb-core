# AGENTS.md — contrib/arrow-cmake/

chdb-specific CMake glue around Apache Arrow. **Edits here are
expected** (this is one of the deliberate divergences from
ClickHouse upstream).

## Why this exists

`CMakeLists.txt` generates a small compatibility shim header
(`jemalloc_arrow_compat.h`) that maps Arrow's unprefixed jemalloc
calls (`mallocx`, `mallctl`, …) onto the `je_`-prefixed symbols
chdb exports.

Without this shim, Arrow links against a phantom jemalloc — see
`../jemalloc-cmake/AGENTS.md` for the underlying `je_` prefix
story and why it matters when chdb.so loads inside a Python
process.

## Companion submodule

`contrib/arrow/` tracks **chdb fork** `chdb-io/arrow.git`
(branched off ClickHouse/arrow). The chdb-specific commit on top
is **"Use je_ prefix"** — Arrow's internal `malloc` / `free`
calls are rewritten to the `je_`-prefixed symbols.
