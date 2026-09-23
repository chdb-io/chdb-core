# AGENTS.md — chDB Core

This repository contains the embedded ClickHouse engine, public C ABI, native
bindings, and chdb-core release artifacts. The higher-level DataStore API lives
in [chdb-io/chdb](https://github.com/chdb-io/chdb).

## Scope and ABI

- Keep engine, C ABI, native binding, and packaging changes here. Make DataStore
  and high-level dataframe API changes in chdb.
- Keep ClickHouse-derived patches focused. For upstream syncs, record the
  ClickHouse version or commit and any intentional chDB differences.
- Treat [`programs/local/chdb.h`](./programs/local/chdb.h) as a stable ABI.
  Preserve symbol, ownership, lifetime, error, and cancellation contracts.
- When the ABI changes, update [`bindings.md`](./bindings.md) and `CHDB_VERSION`
  when the ClickHouse version line changes.

## Repository skills

Shared coding-agent skills live under [`.agents/skills`](./.agents/skills).
