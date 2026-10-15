---
name: resolve-sync-conflicts
description: Finish the daily ClickHouse master sync in chdb-core once the upstream diff is applied (.github/nightly/daily_sync.py apply) - resolve its conflicts, build, run the Python tests, fix what the sync broke, and write the notes for the pull request. Use in the daily-sync workflow, or when a person finishes a sync by hand.
---

# Finish a daily sync

The daily sync applies the ClickHouse master diff `UPSTREAM_COMMIT..target` to the
paths chDB keeps. Files where that diff and chDB's own changes touch the same lines
are left with conflict markers (zdiff3 style: ours, base, theirs). "Ours" is chDB
main; "theirs" is upstream; the base section is the upstream file before the diff.
The job is to get from there to a sync that builds and passes the Python tests.

## Inputs

- `.sync/summary.json`: `base`, `target`, `conflicts` (each with the upstream pull
  requests that touched the file in this range), `submodules`, `applied_files`.
- The upstream side of each conflicted file as plain files:
  `.sync/upstream/base/<path>` and `.sync/upstream/target/<path>` (before and after
  the range) and `.sync/upstream/log/<path>.txt` (the upstream merges that touched it).
- `.sync/build.json` and the full logs in `.sync/build/` once the build has run.
- chDB's own history: `git log` and `git diff` in this checkout.
- What syncs have broken before:
  `.agents/skills/sync-clickhouse-baseline/references/guide.md`, kept for the
  person-led upgrade. Its import rules, risky modules, the embedded, client and
  host-lifecycle reviews, the failure patterns, the test attribution notes, the five
  audits and the validation order apply here as they are, as far as one Linux
  x86_64 machine reaches; the pull request's CI and main's builds cover the rest. Its review documents, approval stops and
  commit layout do not: here nobody waits for approval, and the workflow commits.

## Procedure

1. Read the guide above, then resolve every conflict, following the rules below.
2. Run the checks that cost seconds, and fix what they report that is real:
   - `python3 .github/nightly/checks.py all` scripts the guide's cheapest audits:
     chDB lines a merge dropped and upstream lines that never arrived,
     `ForceFunctionReferences.cpp` against `REGISTER_FUNCTION` both ways,
     `LocalServer` initialisation `EmbeddedServer` lacks, and submodule bumps
     against their `contrib/*-cmake` file lists. The pull request shows its last
     report.
   - `.github/scripts/check-abi-version.sh` and
     `python3 chdb/build/check_export_contract.py`, the first cheap checks of the
     guide's validation order: the C API's version line and its export lists.
3. Run `bash .github/nightly/build_and_test.sh`. The first run builds everything
   (about an hour); later runs rebuild what changed. To check one file quickly,
   build its object with `ninja -C buildlib <target>`.
4. If it fails, find the cause in its output and the full log, fix what the sync
   broke, and run it again. Repeat until it prints PASS, or until you can make no
   further progress. On a crash, get a stack from a debugger before changing code.
5. Write `.sync/resolution.md`.

## Rules

1. **Resolve by intent, hunk by hunk.** Read what each side changed and why. Take
   upstream where the chDB side is only text left over from an older sync; keep a
   real chDB feature or fix, adapted to the new upstream API; keep both when the
   changes are independent; decide words or arguments separately when one line
   carries several changes. Never take a whole side for a file.
2. **chDB changes must survive.** Code under `CHDB_LITE`, `CHDB_WASM_*`, `OS_WASM`,
   `USE_PYTHON` or another `CHDB*` guard, lines marked `// chdb:`, and embedded-server
   hooks stay. Upstream decides everything else.
3. **When upstream removed or rewrote code a chDB change depends on**, do not
   resurrect the removed upstream code. Adapt the chDB change to the new upstream
   design when the adaptation is clear. Otherwise take the upstream version, add
   `// chdb: TODO(sync) <what is lost>` at the spot, and say so in the notes.
4. **Modify/delete** (upstream deleted a file chDB changed): keep it deleted unless
   chDB code still includes or calls it; explain the choice.
5. **Fix build and test failures where chDB meets upstream**: in chDB's own files
   (those upstream does not have, such as `programs/local/EmbeddedServer.cpp`) or in
   the conflicted files. Change another upstream file only when there is no other
   way, and say why in the notes. Leave `.github/`, `UPSTREAM_COMMIT` and the
   version files alone.
6. **Never weaken or remove a test** to get a pass. Change an expectation only when
   the new upstream behavior is intended, and say so in the notes.
7. **Stage a file you create** with `git add <path>`, or it is left out. Do not
   commit or push.
8. **No conflict markers may remain**: `<<<<<<<`, `|||||||`, `=======`, `>>>>>>>`.

## Output

Write `.sync/resolution.md`; it becomes the pull request's "What the AI did":

- each conflicted file: what chDB changed, what upstream changed (with the pull
  request numbers), and how the two were combined;
- what you changed to make the build and the tests pass, and why;
- whether the last `build_and_test.sh` run passed; if not, what still fails and what
  you tried;
- a "Needs a person" list: anything left as `TODO(sync)` or that you are unsure about.
  Leave it out only if there is nothing.

Keep it short; the reviewer reads the diff next to it.
