# UPSTREAM_SYNC.md — chdb-core ↔ ClickHouse/ClickHouse

This file describes how chdb-core stays in sync with its upstream
([ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse))
and which kinds of changes go through which path.
[`CONTRIBUTING.md`](./CONTRIBUTING.md) → "ClickHouse upstream sync
flow" has the short version; this file is the long-form reference.

## Why syncs are sensitive

chdb-core is an active fork. The bulk of `src/` and the entire
`contrib/` submodule set are inherited from ClickHouse upstream. When
we sync (bumping `contrib/` submodules or rebasing `src/` against a
newer ClickHouse release), three things have to be considered together:

- **C ABI compatibility.** Every chDB binding (chdb-go, chdb-bun,
  chdb-rust, chdb-node, chdb-zig, chdb-ruby) links against the
  `programs/local/chdb*.{cpp,h}` ABI. A sync that perturbs the ABI
  ripples through every binding repo silently.
- **chdb-specific divergences.** chdb-core carries a handful of
  deliberate patches on top of upstream (jemalloc `je_` prefix +
  jemalloc-cmake musl tweaks, arrow fork + arrow-cmake jemalloc compat
  shim, pybind11 mrbind fork — see
  [`contrib/AGENTS.md`](./contrib/AGENTS.md) for the current list).
  Syncs need to preserve these.
- **Build matrix.** A new submodule pin can introduce a transitive
  toolchain requirement (newer GCC, a new ICU minor, …) that breaks
  macOS arm64 or Linux musl wheels.

For those reasons, syncs are sequenced by maintainers rather than
landed as drive-by PRs. Sync cadence is on the order of months.

## How to land different kinds of changes

- **Bug whose fix exists in a newer ClickHouse release** — open an
  issue with the bug repro, the upstream fix (commit hash if you have
  it), and the chDB context. Maintainers fold it into the next sync
  window.
- **Generic SQL function / type / dialect bug** (not chDB-specific) —
  prefer landing the fix in ClickHouse upstream first, then opening an
  issue here pointing at the upstream commit so it gets picked up on
  the next sync.
- **chDB-specific C++ change** (in `programs/local/` or chDB-only
  spots in `src/`) — regular PR works; see
  [`programs/local/AGENTS.md`](./programs/local/AGENTS.md) for the
  ABI rules.
- **Submodule bump or mass `src/` rebase** — open an issue first
  describing what's needed and why. A solo PR that bumps submodules
  will almost always be asked to convert into an issue before merge,
  so the issue-first path saves a round-trip.
- **`contrib/` patches** — usually flow in via the upstream-sync
  pipeline. chdb-core does carry intentional divergences (listed in
  [`contrib/AGENTS.md`](./contrib/AGENTS.md)); to add or change one,
  open an issue first so maintainers can sequence it with the next
  sync and keep the divergence reproducible.

The sync flow is being formalised; this file will expand as the
policy is written down.
