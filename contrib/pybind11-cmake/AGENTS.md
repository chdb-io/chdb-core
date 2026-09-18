# AGENTS.md — contrib/pybind11-cmake/

chdb-specific CMake glue around the mrbind pybind11 fork. **Edits
here are expected** (this is one of the deliberate divergences from
ClickHouse upstream).

## What chdb edits

`CMakeLists.txt` carries chdb-specific patches:

- Isolating pybind11 internals so importing chdb alongside libtorch
  doesn't conflict.
- musl-arm support tweaks.

## Companion submodule

`contrib/pybind11/` tracks **chdb fork**
`chdb-io/mrbind-pybind11.git` — a fork that produces stable-ABI
Python modules instead of pybind11's usual header-only, one-Python-
version-per-build setup.

The version-dependent pybind11 code is split into a tiny shared
library that must be built per Python version
(`libpybind11nonlimitedapi_chdb_<py-version>.dylib` for 3.9, 3.10,
…) and linked against; the `chdb-core` wheel itself then ships as a
single `abi3` artefact that loads the right per-version stub at
runtime.

See `../../chdb/build_pybind11.sh` for the multi-version build
loop.
