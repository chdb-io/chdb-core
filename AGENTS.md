# AGENTS.md — chDB Core

This repository contains the embedded ClickHouse engine, public C ABI, native
bindings, and chdb-core release artifacts. The higher-level DataStore API lives
in [chdb-io/chdb](https://github.com/chdb-io/chdb).

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for setup, commands, and contributor
workflow.

## Scope and ABI

- Keep engine, C ABI, native binding, packaging, and engine-level test changes
  here. Make DataStore and high-level dataframe API changes in chdb.
- Keep ClickHouse-derived patches focused. For upstream syncs, record the
  ClickHouse version or commit and any intentional chDB differences.
- Treat [`programs/local/chdb.h`](./programs/local/chdb.h) as a stable ABI.
  Preserve symbol, ownership, lifetime, error, and cancellation contracts, and
  prefer length-aware `_n` entry points for new string-taking APIs.
- When the ABI changes, update [`bindings.md`](./bindings.md), relevant tests and
  examples, and `CHDB_VERSION` when the ClickHouse version line changes.

## Build and test

- Run the smallest relevant checks first. Use `make test`, `make buildlib`, and
  `make wheel` when applicable to the change.
- Report only checks that were actually run and note unavailable platform or
  toolchain coverage.

## Repository skills

Shared coding-agent skills live under [`.agents/skills`](./.agents/skills).
