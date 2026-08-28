# AGENTS.md — contrib/postgres-cmake/

chdb-specific CMake glue around Postgres source. **Edits here are
expected** (this is one of the deliberate divergences from
ClickHouse upstream).

## Why this exists

`CMakeLists.txt` adds an `explicit_bzero` shim for macOS
cross-compilation. macOS libc doesn't provide `explicit_bzero` and
Postgres source assumes it exists.
