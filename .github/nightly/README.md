# Daily ClickHouse sync and nightly builds

Three workflows keep `main` close to ClickHouse master and publish what it builds.

| Workflow | When | What it does |
|---|---|---|
| `daily_sync.yml` | 22:17 UTC, Sunday to Thursday (06:17 in Beijing, Monday to Friday), or by hand | Applies the ClickHouse master diff since `UPSTREAM_COMMIT`, builds and tests it, and opens a pull request |
| `nightly_publish.yml` | After main's build of each merged sync pull request | Finishes the `nightly` pre-release that build uploads its wheels to, adds the chdb nightly, stores the night in S3 and updates the dashboard |
| `nightly_api.yml` | When `api/` or `nightly_data.py` changes on main, or by hand | Deploys the nightly API, the chDB Lambda function the dashboard reads |

They stay off until their repository variables are set (below).

## Daily sync

1. Pick the target: the newest ClickHouse master commit whose `MasterCI` status is
   success, usually a few hours behind HEAD. With no green commit in 24 hours it
   takes HEAD and lets chDB's CI decide. On its way past a release branch point,
   the master commit tagged `v(X.Y+1).1.1-new` where upstream forks line X.Y, it
   stops there, so `main` has a commit synced to exactly that point.
2. Apply `git diff UPSTREAM_COMMIT..target` to the paths chDB keeps, with
   `git apply --3way`. The path rules live in
   `.agents/skills/sync-clickhouse-baseline/scripts/sync_preflight.py`: files chDB
   deleted stay deleted; upstream trees chDB drops (`tests/`, `docs/`, `ci/`,
   `docker/`, `utils/`, `.github/` ...) are never imported, except
   `utils/list-licenses/`, which the build runs; chDB keeps its own `README.md`,
   `CONTRIBUTING.md`, `.gitignore` and
   `pyproject.toml`. A file upstream deleted after chDB changed it is left as a
   modify/delete conflict. Submodule
   pointers move when chDB pins the same commit upstream did, and are held when chDB
   pins its own. `CHDB_VERSION` follows the upstream line as `X.Y.0-dev`.
3. Build chDB and run its Python tests on Python 3.14 (`build_and_test.sh`), on the
   self-hosted x64 pool, the way the Linux x86_64 wheel workflow does.
4. When the diff conflicts, or the build or the tests fail, Claude Code takes over
   on the same machine (`claude -p`, effort max, following
   `.agents/skills/resolve-sync-conflicts`): it resolves the conflicts, fixes what
   the sync broke and reruns `build_and_test.sh` until it passes or it can make no
   further progress, and writes down what it did; its session streams into the job
   log. It is stopped at 05:00 UTC,
   before the runners' restart, and the pull request opens with whatever it got to.
   It never holds `SYNC_PAT`.
5. `checks.py` reports what a build and tests do not show: chDB changes lost or
   upstream changes missing, `ForceFunctionReferences.cpp` against
   `REGISTER_FUNCTION`, `LocalServer` initialisation `EmbeddedServer` lacks, and
   submodule bumps against their `contrib/*-cmake` file lists. Nothing here stops
   the pull request.
6. A separate job on a GitHub-hosted runner commits the sync onto the `main` commit
   it started from, in two commits: the upstream diff as applied, conflict markers
   and all, then everything changed on top of it. The sync is never landed in parts:
   a file held back would silently lose that day's upstream change. It pushes
   `sync/clickhouse-master` and opens the pull request with
   `SYNC_PAT`. Its description gives the build and test result, the conflicts and
   what the AI did, the files changed beyond the upstream diff, and the checks. A
   person approves and merges it.
7. The sync pull request gets the CI any pull request gets: the four wheel
   platforms, the free-threaded wheels and wasm.

While a sync pull request is open, the daily run does nothing, so a review is never
overwritten, and no nightly comes out either; the next sync after it merges covers
the days in between.

### The first sync

`UPSTREAM_COMMIT` starts at v26.9.2.8-stable, so the first sync carries a whole
line upgrade. Expect it to conflict like one and review it as one (see the
`sync-clickhouse-baseline` skill). Start it by hand, with `dry_run` first if you
like, or set `DAILY_SYNC_ENABLED=true` and let the next weekday's run start it. A
long first run may not finish before the runners' 06:00 UTC restart; started by hand
right after 07:00 UTC, it has most of a day.

### Finishing a blocked sync by hand

```bash
git switch -c sync/clickhouse-master origin/main
python3 .github/nightly/daily_sync.py fetch --work /tmp/ch-sync --target <sha>
python3 .github/nightly/daily_sync.py apply --work /tmp/ch-sync --target <sha>
# resolve the conflicts (git diff --name-only --diff-filter=U), then until it passes:
bash .github/nightly/build_and_test.sh
python3 .github/nightly/checks.py all --work /tmp/ch-sync
python3 .github/nightly/daily_sync.py stage
python3 .github/nightly/daily_sync.py commit
git push origin sync/clickhouse-master   # then open the pull request from it
```

Only a merge from `sync/clickhouse-master` in this repository makes a nightly, so push
the finished sync there (no sync pull request is open when a sync is blocked).

`apply` copies the upstream blobs it merges from into this repository's object store.

## Monthly release

Not automated here. A person cuts the release branch from the `main` commit whose
`UPSTREAM_COMMIT` is the line's branch point (the daily sync always stops there),
applies the line's patches up to its first stable tag with the
`sync-clickhouse-baseline` process, and tags `vX.Y.0` on that branch. Release
branches are never merged back into `main`.

## Nightly builds

There is no separate nightly build: the nightly is main's build of each merged
ClickHouse sync pull request, which the four wheel workflows run on that push. The
`Nightly gate` (`nightly_gate.yml`) tells that push apart: its commit is the merge of
a pull request from this repository's `sync/clickhouse-master` branch, which only
`daily_sync.yml` pushes. GitHub records that commit as the pull request's merge commit
whichever way it was merged, so the gate asks for it rather than read commit
messages. That build makes
and tests the Python wheel only (`WHEEL_ONLY`): every Python version, each against
one pandas line (3.14 against both), but no static library, chdb-core-lite, debug
symbols, libchdb/ADBC/Go checks or other packages' tests, which the sync pull
request's own CI ran on every platform; for the same reason the free-threaded
wheels and wasm skip that push. A merged sync's push also gets a concurrency group
of its own in each, so a later push to main cannot replace its pending build, and
its no-op runs do not cancel or replace main's. No sync
merged, no nightly: none while a sync pull request waits for review.

On a merged sync the workflows version the wheel `X.Y.Z.post1.devN` or, once `main`
is on a newer ClickHouse line than its newest tag, `X.Y.0.devN`, with no local `+`
part so the name survives as a release asset. `N` is the commit's time as yymmddhhmm
in UTC, e.g. `26.10.0.dev2610151617`. Each run uploads its wheel to the `nightly`
pre-release itself (`upload_wheel.sh`), creating the release the first time.
`nightly_publish.yml` waits until all four runs for the commit are done and, unless a
newer night is already out (a rerun of an older night's job), then:

- moves the `nightly` tag to the commit and keeps the wheels of the newest 20
  nights, a night being one commit. The release is created once; afterwards only
  its assets, notes and tag move, so no `release: published` event starts the
  release builds, and the release workflows skip any tag that is not `v*` anyway;
- builds the chdb nightly (`chdb_nightly.py`, below) and adds it to the release;
- writes a PEP 503 index from the release's asset list, whose names and sha256
  digests are all it needs (no wheel is downloaded), so
  `pip install --pre --extra-index-url <site>/simple chdb-core` installs the newest
  chdb-core nightly and `... chdb` the newest chdb nightly with it;
- runs `report.py` on the latest chdb-core release: chDB writes the night's runs,
  test results and night record as Parquet files named by the date, next to the
  complete log of every platform's run, failed or not; the workflow uploads them
  to the nightly store, an S3 bucket. chDB also computes the history with SQL over
  that night and the ones before it, read from the bucket with `s3()`, and the
  night's failing tests, as `data/*.json`;
- deploys `dashboard/`, that data and the pip index to Vercel. The dashboard reads
  the nightly API, and `data/*.json` when the API cannot be reached.

### The chdb nightly

chdb (chdb-io/chdb, the DataStore API) is a pure-Python package on top of chdb-core.
For a night with a chdb-core wheel on the release, `chdb_nightly.py` copies a
checkout of chdb's `main` and changes the copy, never the chdb repository: the
version becomes the minor after chdb's newest PyPI release with the night's dev
number, e.g. `4.5.0.dev2610180851`, newer than every release so that `pip --pre`
picks it, and the chdb-core requirement becomes the night's exact version. It builds
the wheel and uploads it to the release untested, since chdb's own CI tests its
`main`. On a platform the night has no chdb-core wheel for, pip backtracks to the
newest chdb nightly whose chdb-core it can install. chdb's wheels, like chdb-core's,
go when their night is older than the newest 20. The outcome and the build log are
on the dashboard; chdb never holds up chdb-core's night.

### The nightly store and API

The store is an S3 bucket, one file per table and night (`nightly_data.py` lists
them). The publish job writes it through a GitHub OIDC role; nothing in it is
public. The nightly API (`api/app.py`) is an AWS Lambda function, a container image
since chDB's wheel is too big for a zip deployment, behind a function URL. It runs
chDB over the bucket with the function's read-only role and answers a fixed set of
read-only requests, the same queries `report.py` runs (`nightly_data.py`):

| Request | Answer |
|---|---|
| `GET /latest` | the newest night, as `data/latest.json` |
| `GET /history?days=N` | the runs of the last N nights (90; at most 365), as `data/history.json` |
| `GET /logs/<date>/<platform>` | the complete log of that platform's run |
| `GET /logs/<date>/<platform>?tail=error` | its lines up to the first error |
| `GET /logs/<date>/chdb` | the chdb nightly's build log |
| `GET /health` | the engine version |

## Setup

| Name | Kind | Used by | Value |
|---|---|---|---|
| `SYNC_PAT` | secret | daily sync | Fine-grained personal access token for this repository: Contents and Pull requests read/write. A pull request opened with `GITHUB_TOKEN` would start no CI. Its owner cannot approve the sync pull requests. |
| `ANTHROPIC_API_KEY` | secret | daily sync | Claude API key for conflict resolution |
| `DAILY_SYNC_ENABLED` | variable | daily sync | `true` to run on its schedule |
| `SYNC_CLAUDE_MODEL` | variable | daily sync | Optional; defaults to `claude-opus-5-5` |
| `NIGHTLY_PUBLISH_ENABLED` | variable | nightly | `true` to publish |
| `NIGHTLY_SITE_URL` | variable | nightly | Public URL of the dashboard, e.g. `https://nightly.chdb.io` |
| `GH_TOKEN` | secret | nightly | The token the wheel workflows already upload release assets with |
| `VERCEL_TOKEN` | secret | nightly | Vercel access token for the team that owns the project |
| `VERCEL_ORG_ID` | variable | nightly | That team's id |
| `VERCEL_PROJECT_ID` | variable | nightly | The dashboard's project; deployment is skipped when unset |
| `NIGHTLY_AWS_REGION` | variable | nightly, API | The region of the bucket and the function, e.g. `ap-southeast-1` |
| `NIGHTLY_AWS_ROLE_ARN` | variable | nightly, API | The role the workflows assume through GitHub's OIDC provider |
| `NIGHTLY_BUCKET` | variable | nightly, API | The nightly store's bucket; without it the store stays on the runner and the history is the night alone |
| `NIGHTLY_API_URL` | variable | nightly | The function URL of the nightly API, which `config.json` gives the dashboard |
| `NIGHTLY_API_FUNCTION`, `NIGHTLY_API_REPOSITORY` | variable | API | Optional; the function and the ECR repository, both `chdb-nightly-api` by default |

In Vercel, in the dashboard or with the CLI: create the project with no Git
connection and no build, since the workflow deploys a finished directory (drop
any page on vercel.com/drop, or run `vercel project create <name>`); make sure
Deployment Protection leaves its production domain public (pip and readers fetch
it without a Vercel login); and add a `chdb.io` subdomain to it (a CNAME in
chdb.io's DNS) or keep its `*.vercel.app` domain.

In AWS, in one region:

1. An S3 bucket with all public access blocked. Optionally a lifecycle rule that
   expires `logs/` after some months.
2. The GitHub OIDC identity provider, `https://token.actions.githubusercontent.com`
   with audience `sts.amazonaws.com`, once per account.
3. The workflows' role (`NIGHTLY_AWS_ROLE_ARN`), trusting that provider for
   `repo:chdb-io/chdb-core:ref:refs/heads/main`, allowed `s3:ListBucket` on the
   bucket, `s3:GetObject` and `s3:PutObject` on its objects (`<bucket>/*`),
   `ecr:GetAuthorizationToken` and the push actions on the API's ECR
   repository, and `lambda:UpdateFunctionCode`, `lambda:GetFunction` and
   `lambda:GetFunctionConfiguration` on the function.
4. A private ECR repository for the API's image, `chdb-nightly-api`.
5. The function's execution role: `AWSLambdaBasicExecutionRole`, and
   `s3:ListBucket` and `s3:GetObject` on the bucket.
6. Run `nightly_api.yml` by hand, which pushes the first image, then create the
   function from it: container image, x86_64, the execution role, 2048 MB, a 30
   second timeout, a small reserved concurrency, and the environment variables
   `NIGHTLY_BUCKET`, `NIGHTLY_SITE_URL` and `GITHUB_REPOSITORY`. Give it a function
   URL with auth type `NONE` and CORS allowing the dashboard's origin with `GET`,
   and set `NIGHTLY_API_URL` to it. Later runs of `nightly_api.yml` update the
   function.

## Files

| File | Purpose |
|---|---|
| `daily_sync.py` | fetch, target, apply, stage, commit |
| `build_and_test.sh` | build chDB and run the Python tests on Python 3.14 |
| `checks.py` | the checks Git does not do |
| `open_pr.sh` | commit, push and open the sync pull request |
| `resolve_prompt.md` | the prompt the sync gives Claude Code |
| `embedded-mirror-allowlist.txt` | `LocalServer` calls `EmbeddedServer` deliberately skips |
| `upload_wheel.sh` | a merged sync's wheel to the nightly release |
| `publish.py` | nightly release tag, retention and pip index |
| `chdb_nightly.py` | the chdb nightly, pinned to the night's chdb-core |
| `report.py` | the nightly store and the dashboard's data, computed by chDB |
| `nightly_data.py` | the store's tables and the queries `report.py` and the API share |
| `api/` | the nightly API: chDB in AWS Lambda over the store |
| `dashboard/` | the static dashboard |
| `test_nightly.py` | offline tests, run by Pull-CI |
| `../../UPSTREAM_COMMIT` | the ClickHouse commit `main` is synced to |
