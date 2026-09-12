# AGENTS.md — contrib/

Vendored upstream libraries — ~260 entries, ~130 git submodules,
~9 GB on disk.

**Two rules**:

1. Don't modify files inside a vendored library here — the next
   upstream sync would overwrite them. Bugs go upstream first
   (see `../CONTRIBUTING.md` → "Treat `contrib/` as read-only" and
   "ClickHouse upstream sync flow").
2. chdb-specific divergences live in per-directory AGENTS.md files;
   read the one for the directory you're editing.

## chdb-specific divergence dirs (each has its own AGENTS.md)

- [`arrow-cmake/`](./arrow-cmake/AGENTS.md) — Arrow `je_`-prefix
  jemalloc compat shim
- [`jemalloc-cmake/`](./jemalloc-cmake/AGENTS.md) — jemalloc
  allocator isolation (`je_` prefix + linker wrap) + musl-arm tweaks
- [`pybind11-cmake/`](./pybind11-cmake/AGENTS.md) — mrbind pybind11
  multi-Python build glue + torch isolation
- [`postgres-cmake/`](./postgres-cmake/AGENTS.md) — macOS
  `explicit_bzero` shim

## chdb-forked submodules (URLs don't track upstream)

- `arrow/` → `https://github.com/chdb-io/arrow.git` (chdb fork
  branched off ClickHouse/arrow, carries a "Use je_ prefix" patch)
- `pybind11/` → `https://github.com/chdb-io/mrbind-pybind11.git`
  (chdb fork producing stable-ABI Python modules)
- `jemalloc/` → upstream `https://github.com/jemalloc/jemalloc`
  (no fork; chdb divergence is build-flag + cmake glue, see
  `jemalloc-cmake/AGENTS.md`)

Submodules are external repos — we can't put AGENTS.md inside them;
the relevant notes live in the matching `*-cmake/` dir above.
