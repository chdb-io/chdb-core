# Daily ClickHouse sync and nightly builds

Two workflows keep `main` close to ClickHouse master and publish what it builds.

| Workflow | When | What it does |
|---|---|---|
| `daily_sync.yml` | 02:17 UTC, or by hand | Applies the ClickHouse master diff since `UPSTREAM_COMMIT` and opens a pull request |
| `nightly_publish.yml` | After the scheduled wheel builds (16:00 UTC) | Publishes the wheels to the `nightly` pre-release and updates the dashboard |

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
has landed for 72 hours, the run opens an issue titled "Daily ClickHouse sync has
stalled" and closes it once one lands.

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

The four wheel workflows build `main` on a schedule. Their scheduled runs version
the wheel `X.Y.Z.post1.devN` or, once `main` is on a newer ClickHouse line than its
newest tag, `X.Y.0.devN`, with no local `+` part so the name survives as a release
asset. `N` is the commit's time as yymmddhhmm in UTC, e.g.
`26.10.0.dev2610151617`. `nightly_publish.yml` waits until all four runs for the commit are done, then:

- uploads the `chdb_core` wheels to the `nightly` pre-release and deletes those older
  than 30 days. The release is created once; afterwards only its assets, notes and
  tag move, so no `release: published` event starts the release builds, and the
  release workflows skip any tag that is not `v*` anyway;
- writes a PEP 503 index, so
  `pip install --pre --extra-index-url <site>/simple chdb-core` installs the newest
  nightly;
- runs `report.py` on the nightly wheel itself (the latest release if it does not
  install): chDB writes the night's runs, test results and failure log tails to
  Parquet in Cloudflare R2 through `s3()`, and computes the history, failing and
  flaky tests with SQL over every night stored;
- deploys `dashboard/` and the data to Cloudflare Pages.

## Setup

| Name | Kind | Used by | Value |
|---|---|---|---|
| `SYNC_PAT` | secret | daily sync | Fine-grained personal access token for this repository: Contents and Pull requests read/write. A pull request opened with `GITHUB_TOKEN` would start no CI. Its owner cannot approve the sync pull requests. |
| `ANTHROPIC_API_KEY` | secret | daily sync | Claude API key for conflict resolution |
| `DAILY_SYNC_ENABLED` | variable | daily sync | `true` to run on schedule |
| `SYNC_CLAUDE_MODEL` | variable | daily sync | Optional; defaults to `claude-opus-5-5` |
| `NIGHTLY_PUBLISH_ENABLED` | variable | nightly | `true` to publish |
| `NIGHTLY_SITE_URL` | variable | nightly | Public URL of the dashboard, e.g. `https://nightly.chdb.io` |
| `R2_ENDPOINT` | variable | nightly | `https://<account id>.r2.cloudflarestorage.com` |
| `R2_BUCKET` | variable | nightly | Bucket for the Parquet history and logs |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | secret | nightly | R2 API token with read/write on the bucket |
| `CF_ACCOUNT_ID` | variable | nightly | Cloudflare account id |
| `CF_PAGES_PROJECT` | variable | nightly | Pages project name; deployment is skipped when unset |
| `CLOUDFLARE_API_TOKEN` | secret | nightly | Token with Pages edit permission |

On the repository: allow auto-merge, and make the Linux x86_64 build a required
check on `main` so auto-merge waits for it. In Cloudflare: create the R2 bucket
(with a lifecycle rule deleting `logs/` after 90 days) and the Pages project, and
bind a `chdb.io` subdomain to the project.

Without the R2 settings the report still runs, with the history limited to the
night itself.

## Files

| File | Purpose |
|---|---|
| `daily_sync.py` | fetch, target, apply, finalize, commit, stall |
| `checks.py` | the checks Git does not do |
| `open_pr.sh` | commit, push and open the sync pull request |
| `resolve_prompt.md` | the prompt the conflict job gives Claude Code |
| `embedded-mirror-allowlist.txt` | `LocalServer` calls `EmbeddedServer` deliberately skips |
| `publish.py` | nightly release, retention and pip index |
| `report.py` | dashboard data, computed by chDB |
| `dashboard/` | the static dashboard |
| `test_nightly.py` | offline tests, run by Pull-CI |
| `../../UPSTREAM_COMMIT` | the ClickHouse commit `main` is synced to |
