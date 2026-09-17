# AGENTS.md — chDB Core

This repository contains the embedded ClickHouse engine, its public C ABI, and
the artifacts published as chdb-core. The higher-level pandas-compatible
DataStore API lives in [chdb-io/chdb](https://github.com/chdb-io/chdb).

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for environment setup, build
commands, tests, and release mechanics.

## Scope and upstream discipline

- Keep engine, C ABI, native bindings, packaging, and engine-level tests here.
  Make DataStore and Python-facade changes in the chdb repository.
- This tree is derived from ClickHouse. Keep patches focused and avoid
  unrelated generated, vendored, or submodule changes.
- For an upstream synchronization, record the ClickHouse version or commit and
  make deliberate chDB-specific deviations easy to identify.

## Public ABI

- Treat [`programs/local/chdb.h`](./programs/local/chdb.h) as a stable public C
  ABI. Preserve symbol, ownership, lifetime, error, and cancellation contracts.
- Prefer length-aware `_n` entry points for new string-taking APIs.
- When the ABI changes, update the inventory and contracts in
  [`bindings.md`](./bindings.md), along with relevant tests and examples.
- Keep `CHDB_VERSION` aligned with the ClickHouse line checked by
  [`.github/scripts/check-abi-version.sh`](./.github/scripts/check-abi-version.sh).

## Build and test

- Start with the smallest relevant checks, then run `make test` for changes that
  can affect the Python test suite.
- Use `make buildlib` for native-library changes and `make wheel` when packaging
  needs validation.
- Report only commands and results that were actually run. Call out platform or
  toolchain coverage that was not available locally.

## Repository skills

Shared coding-agent skills live under [`.agents/skills`](./.agents/skills).
Before preparing a pull request title or description, use the
`pr-description` skill in that directory.
