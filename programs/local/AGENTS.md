# AGENTS.md — programs/local/ (chDB C++ entry point + C ABI)

This directory contains chDB's **public C ABI** and the C++ entry
points that every chDB language binding (Python, Bun, Go, Rust,
Node, Zig, Ruby) depends on. A breaking change here can ripple
through every binding repo without producing a local test failure.

## ABI stability rule

The public surface is `chdb.h` / `chdb.hpp`. Every chDB language
binding pins to a `chdb-core` version and re-declares the ABI on its
side, so changes here propagate downstream.

**Use the opaque-pointer pattern.** Public structs should expose
exactly one field — an internal pointer — and all access goes through
accessor functions, e.g.:

```c
typedef struct chdb_result_
{
    void * internal_data;
} chdb_result;
```

That keeps the struct fully opaque: callers never read fields by
offset, so the internal layout can evolve freely without breaking any
binding. Don't expose new fields, and don't try to "extend" by
appending fields at the end either — both lock down layout in ways
that hurt later.

The following kinds of changes are still ABI-breaking and need a
coordinated version bump + release notes in each binding repo:

- Function signatures in `chdb.h` / `chdb.cpp` / `chdb.hpp` —
  parameter types, return types, calling convention
- Ownership semantics (who frees what, lifetime of returned
  strings / buffers)
- Removing or renaming any public symbol

When in doubt, treat the change as ABI-breaking and open an issue.

## Internal refactors

Internal headers (`chdb-internal.h`, helper classes, anything not in
`chdb.h{,pp}`) can move around freely without binding-side
coordination. For the rebuild + test commands, see the root
[`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) → "I changed X —
what to run" row A.

## Placement rule

chDB-specific C++ logic lives in `programs/local/`, **not** in
`src/`. That boundary is what lets the upstream-sync flow rebase
`src/` cleanly without losing chDB patches. See
[`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) → "Understand the C
ABI before refactoring `programs/local/chdb*.cpp`" for the long-form
reasoning, and [`../../UPSTREAM_SYNC.md`](../../UPSTREAM_SYNC.md) for
the sync flow.
