---
name: sync-clickhouse-baseline
description: Sync chdb-core to a newer ClickHouse release without losing chDB-specific behavior. Use for ClickHouse baseline upgrades, large src or contrib updates, submodule changes, sync conflicts, and CI failures after an upgrade. Covers embedded startup and shutdown, memory and jemalloc, C exports, static libraries, Arrow and Parquet, CHDB_LITE, WASM, packaging, and downstream bindings.
---

# Sync a ClickHouse baseline

Move chdb-core from one ClickHouse release to another with a small, reviewable change. Keep intentional chDB behavior and take normal upstream fixes.

Before changing files, read the matching [guide](references/guide.md).

## How the files work together

- `SKILL.md` defines the required order, review stops, commit structure, and completion rules.
- `references/guide.md` explains risky modules, known failure patterns, and test choices.
- `scripts/sync_preflight.py` is a read-only inventory tool. It resolves the three refs, lists upstream changes, filters likely chDB paths, reports changed submodule pointers, groups risky paths, and warns about a dirty worktree. It does not fetch, edit files, apply a patch, resolve conflicts, build, or test. Its output is evidence for review, not an automatic decision.

## Rules

1. Start from a clean worktree based on the latest `origin/main`. Do not mix a sync with feature work.
2. Record the exact old and new ClickHouse tags and commits. Do not rely only on a version string.
3. Import only paths kept by chDB. Do not merge the whole ClickHouse tree.
4. Preserve the work in separate commits:
   - `upgrade clickhouse to <tag>` contains only selected upstream changes that apply without conflict, plus verified submodule pointers.
   - `resolve conflicts for <tag> upgrade` contains only reviewed conflict resolutions, required chDB adaptations, and their tests.
   - Each later CI problem gets one commit. Do not squash these commits. Record why the first sync and local tests missed the problem.
5. Before resolving conflicts, write `sync-review/<tag>/conflicts.md`. For every file, explain both sides, recommend a result, name its test, and wait for the person responsible for the sync to approve it.
6. Do not choose a whole side automatically. Decide each conflicting change from its purpose and history.
7. Do not weaken or remove a test to obtain a green build. Change an expectation only after confirming the new engine behavior.
8. For a CI crash, core dump, hang, or memory error, obtain the CI artifact and symbolicate it when possible. Otherwise reproduce the same build and failure locally. Do not guess from a short error message or push repeated trial commits. If the required platform, toolchain, symbols, or data are unavailable, state the exact gap and ask a person to provide it. Do not skip the test. When CI evidence is too weak, record a follow-up change that improves CI diagnostics.
9. Do not push or change a remote pull request without confirmation for that exact action.

## Workflow

### 1. Prepare

- Read repository instructions and run `git status --short --branch`.
- Preserve unrelated work. Create a separate clean worktree when needed.
- Record current `main` CI failures so they are not blamed on the sync.
- Find the last baseline commit and verify both tags with `git rev-parse 'refs/tags/<tag>^{commit}'`.
- Run the read-only inventory:

```bash
python3 agent/skills/sync-clickhouse-baseline/scripts/sync_preflight.py \
  --from-ref refs/tags/<old-tag> \
  --to-ref refs/tags/<new-tag> \
  --base-ref origin/main
```

If a tag is missing, stop. Fetch it only when fetching is allowed.

### 2. Import the upstream change

- Build a path list from `git diff --name-status -M -C <old>..<new>`.
- Keep changed and deleted files only if chDB already keeps them.
- Add new files only under retained chDB trees.
- Exclude upstream-only docs, CI, tests, tools, and programs removed by chDB.
- In a disposable worktree, apply the full binary patch with full object IDs using `git apply --3way --index`. Use the result only to discover the conflict-file list.
- Regenerate a patch that excludes those conflict files. Apply it in the real sync worktree and create the conflict-free upgrade commit.
- Apply the conflict-file patch separately. Do not resolve or commit it until its review document is approved.
- Update submodule pointers separately. Verify the exact commit and the matching `contrib/<library>-cmake` files.

### 3. Review conflicts

Run:

```bash
git diff --name-only --diff-filter=U
git status --short
```

Create `sync-review/<tag>/conflicts.md`. For each file, state what upstream changed, why chDB changed it, the proposed combined result, and the test that will check it. Include unresolved questions. Give the document to the person responsible for the sync and wait for approval before editing conflicts.

The conflict-free import must already be committed. After approval, resolve the separately applied conflict files and create the conflict-resolution commit.

### 4. Review silent breakage

Git reports text conflicts, but it does not report a missing setter, new config field, removed submodule source, dropped registration object, or changed shutdown order. Use [guide.md](references/guide.md) to check every affected module.

### 5. Validate

Run cheap checks first, then clean builds. Cover full and lite builds separately. Add WASM, wheel, static-library, downstream, platform, and sanitizer tests when the changed paths affect them.

If native code crashes, capture and symbolicate the native stack before changing code.

### 6. Handle CI

Classify each failure as a new sync bug, an existing `main` failure, dependency drift, stale build state, infrastructure trouble, or a confirmed behavior change. Retry only failures with evidence of infrastructure trouble.

For each new CI problem:

1. Download the full log, core dump, crash report, symbols, and other available artifacts.
2. Symbolicate the crash or reproduce the same build and test locally.
3. If the environment cannot be reproduced, report what is missing and request help. Do not mark the test as covered.
4. Fix one root cause and add or improve the test that exposes it.
5. Record the evidence, root cause, why this skill or the local test set missed it, and the prevention change in `sync-review/<tag>/ci-followups.md`.
6. Create one commit for that problem. Keep it separate for later investigation.

Do not use CI as a trial-and-error debugger. If CI does not retain enough information, plan a follow-up change to upload cores, symbols, complete logs, dependency versions, or test outputs.

## Ready to finish

Finish only when conflicts are approved and resolved, deleted upstream-only trees stay deleted, submodule files match their build lists, embedded startup and shutdown match new upstream needs, C symbols and static registrations are reachable, required build variants pass, downstream tests use the new artifact, and no test was weakened.

After a rebase, rerun the full test set. Ask again before every push.
