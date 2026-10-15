# Daily ClickHouse sync and nightly builds

Two workflows keep `main` close to ClickHouse master and publish what it builds.

| Workflow | When | What it does |
|---|---|---|
| `daily_sync.yml` | 02:17 UTC, Monday to Friday, or by hand | Applies the ClickHouse master diff since `UPSTREAM_COMMIT` and opens a pull request |
| `nightly_publish.yml` | After the scheduled wheel builds (16:00 UTC, Monday to Friday) | Finishes the `nightly` pre-release the builds upload their wheels to, and updates the dashboard |

Both stay off until their repository variable is set (below).

## Daily sync

1. Pick the target: the newest ClickHouse master commit whose `MasterCI` status is
   success, usually a few hours behind HEAD. With no green commit in 24 hours it
   takes HEAD and lets chDB's CI decide.
2. Apply `git diff UPSTREAM_COMMIT..target` to the paths chDB keeps, with
   `git apply --3way`. The path rules live in
   `.agents/skills/sync-clickhouse-baseline/scripts/sync_preflight.py`: files chDB
   deleted stay deleted; upstream trees chDB drops (`tests/`, `docs/`, `ci/`,
   `utils/`, `docker/`, `.github/` ...) are never imported; chDB keeps its own
   `README.md`, `CONTRIBUTING.md`, `.gitignore` and `pyproject.toml`. Submodule
   pointers move when chDB pins the same commit upstream did, and are held when chDB
   pins its own. `CHDB_VERSION` follows the upstream line as `X.Y.0-dev`.
3. Run `checks.py`: conflict markers, chDB changes lost or upstream changes missing
   in a clean merge, `ForceFunctionReferences.cpp` against `REGISTER_FUNCTION`, new
   `LocalServer` initialisation `EmbeddedServer` lacks, and submodule bumps against
   their `contrib/*-cmake` file lists.
4. Commit everything in one commit (the sync is never landed in parts: a file held
   back would silently lose that day's upstream change), push
   `sync/clickhouse-master` and open the pull request with `SYNC_PAT`.
   - Clean and every check passes: auto-merge once the required checks are green.
   - Conflicts: a separate job runs Claude Code headless with
     `.agents/skills/resolve-sync-conflicts`, holding no push credential. Its
     resolution must pass `finalize` (no markers, no upstream file edited outside
     the conflict set) and the checks; the pull request then waits for a person.
5. The sync pull request's CI is Linux x86_64 alone; the other platforms, the
   free-threaded wheels and wasm build on `main` after it merges.

While a sync pull request is open, the daily run does nothing, so a review is never
overwritten; the next sync after it merges covers the days in between. When no sync
has landed for three working days (72 hours of Monday to Friday, UTC), the run opens
an issue titled "Daily ClickHouse sync has stalled" and closes it once one lands.

### The first sync

`UPSTREAM_COMMIT` starts at v26.9.2.8-stable. The first sync goes to a release
branch point, so that release can be cut from it:

1. Find the branch point of line X.Y: the commit tagged `v(X.Y+1).1.1-new` on
   master, e.g. the 26.10 branch point is tagged `v26.11.1.1-new`.
2. Run the workflow by hand with that commit as `target`. Expect it to conflict
   like a normal line upgrade; review it as one (see the
   `sync-clickhouse-baseline` skill).
3. After it merges, set `DAILY_SYNC_ENABLED=true`.

### Finishing a blocked sync by hand

```bash
git switch -c finish-sync origin/main
python3 .github/nightly/daily_sync.py fetch --work /tmp/ch-sync --target <sha>
python3 .github/nightly/daily_sync.py apply --work /tmp/ch-sync --target <sha>
# resolve the conflicts listed in .sync/summary.json, then:
python3 .github/nightly/daily_sync.py finalize --work /tmp/ch-sync
python3 .github/nightly/checks.py all --work /tmp/ch-sync
python3 .github/nightly/daily_sync.py commit
```

`fetch` adds the ClickHouse clone to this repository's object alternates; run it in
a throwaway clone rather than a worktree of your everyday checkout.

## Monthly release

Not automated here. A person cuts the release branch from the `main` commit whose
`UPSTREAM_COMMIT` is the line's branch point, applies the line's patches up to its
first stable tag with the `sync-clickhouse-baseline` process, and tags `vX.Y.0` on
that branch. Release branches are never merged back into `main`.

## Nightly builds

The four wheel workflows build `main` at 16:00 UTC, Monday to Friday. While a
ClickHouse sync pull request is open, a `Nightly gate` job in each holds the build
back, the way the daily sync waits for that pull request, and nothing is published
for the night.

Scheduled runs version the wheel `X.Y.Z.post1.devN` or, once `main` is on a newer
ClickHouse line than its newest tag, `X.Y.0.devN`, with no local `+` part so the
name survives as a release asset. `N` is the commit's time as yymmddhhmm in UTC,
e.g. `26.10.0.dev2610151617`. Each run uploads its wheel to the `nightly`
pre-release itself (`upload_wheel.sh`), creating the release on the first night.
`nightly_publish.yml` waits until all four runs for the commit are done, then:

- moves the `nightly` tag to the commit and keeps the wheels of the newest 20
  nights, a night being one commit. The release is created once; afterwards only
  its assets, notes and tag move, so no `release: published` event starts the
  release builds, and the release workflows skip any tag that is not `v*` anyway;
- writes a PEP 503 index from the release's asset list, whose names and sha256
  digests are all it needs (no wheel is downloaded), so
  `pip install --pre --extra-index-url <site>/simple chdb-core` installs the newest
  nightly;
- runs `report.py` on the latest chdb-core release: chDB writes the night's runs,
  test results and failure log tails as files named by the date, which the workflow
  uploads to a public Vercel Blob store, and computes the history, failing and
  flaky tests with SQL over that night and the ones before it, read straight from
  the store's URLs with `url()`;
- deploys `dashboard/`, its data and the pip index to Vercel.

## Setup

| Name | Kind | Used by | Value |
|---|---|---|---|
| `SYNC_PAT` | secret | daily sync | Fine-grained personal access token for this repository: Contents and Pull requests read/write. A pull request opened with `GITHUB_TOKEN` would start no CI. Its owner cannot approve the sync pull requests. |
| `ANTHROPIC_API_KEY` | secret | daily sync | Claude API key for conflict resolution |
| `DAILY_SYNC_ENABLED` | variable | daily sync | `true` to run on schedule |
| `SYNC_CLAUDE_MODEL` | variable | daily sync | Optional; defaults to `claude-opus-5-5` |
| `NIGHTLY_PUBLISH_ENABLED` | variable | nightly | `true` to publish |
| `NIGHTLY_SITE_URL` | variable | nightly | Public URL of the dashboard, e.g. `https://nightly.chdb.io` |
| `GH_TOKEN` | secret | nightly | The token the wheel workflows already upload release assets with |
| `VERCEL_TOKEN` | secret | nightly | Vercel access token for the team that owns the project |
| `VERCEL_ORG_ID` | variable | nightly | That team's id |
| `VERCEL_PROJECT_ID` | variable | nightly | The dashboard's project; deployment is skipped when unset |
| `BLOB_BASE_URL` | variable | nightly | Base URL of the public Blob store, `https://<store id>.public.blob.vercel-storage.com` |
| `BLOB_READ_WRITE_TOKEN` | secret | nightly | The Blob store's read-write token |

On the repository: allow auto-merge, and make the Linux x86_64 build a required
check on `main` so auto-merge waits for it. In Vercel: create the project with
`vercel project create <name>` (no Git connection, no build: the workflow deploys
a finished directory), make sure Deployment Protection leaves its production
domain public (pip and readers fetch it without a Vercel login), add a `chdb.io`
subdomain to it (a CNAME in chdb.io's DNS) or keep its `*.vercel.app` domain, and
create a public Blob store.

Without `BLOB_BASE_URL` the report still runs, with the history limited to the
night itself.

## Files

| File | Purpose |
|---|---|
| `daily_sync.py` | fetch, target, apply, finalize, commit, stall |
| `checks.py` | the checks Git does not do |
| `open_pr.sh` | commit, push and open the sync pull request |
| `resolve_prompt.md` | the prompt the conflict job gives Claude Code |
| `embedded-mirror-allowlist.txt` | `LocalServer` calls `EmbeddedServer` deliberately skips |
| `upload_wheel.sh` | a scheduled build's wheel to the nightly release |
| `publish.py` | nightly release tag, retention and pip index |
| `report.py` | dashboard data, computed by chDB |
| `dashboard/` | the static dashboard |
| `test_nightly.py` | offline tests, run by Pull-CI |
| `../../UPSTREAM_COMMIT` | the ClickHouse commit `main` is synced to |
