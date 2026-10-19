#!/usr/bin/env python3
"""Turn a night's results into the nightly store and the dashboard's data, with chDB.

publish.py leaves --work/night.json and the JUnit files of each platform, and
chdb_nightly.py adds the chdb nightly to night.json. This script adds the sync
state, then lets chDB:

- write the night's rows as Parquet into --work/store, one file per table
  (nightly_data.py lists them), next to the complete log of every platform's
  run; the workflow uploads the store to the S3 bucket the nightly API reads,
- compute the history with SQL over the night and the nights before it, which
  chDB reads from the bucket with s3() (from --work/store when NIGHTLY_S3_URL is
  unset), and the night's failing tests.

The results also go to --site/data/*.json, beside the static dashboard, which
reads them when the nightly API cannot be reached.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import gzip
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import nightly_data  # noqa: E402
from nightly_data import NIGHTS_SCHEMA, PLATFORMS, RUNS_SCHEMA, TESTS_SCHEMA, lit  # noqa: E402

DASHBOARD = HERE / "dashboard"
HISTORY_DAYS = 90


def gh(*args) -> str:
    proc = subprocess.run(["gh", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.stdout if proc.returncode == 0 else ""


# ---------------------------------------------------------------------- collection


def sync_state(repo: str, head_sha: str) -> dict:
    encoded = gh("api", f"repos/{repo}/contents/UPSTREAM_COMMIT?ref={head_sha}", "--jq", ".content").strip()
    upstream = base64.b64decode(encoded).decode().split()[0] if encoded else ""
    last = gh("api", f"repos/{repo}/actions/workflows/daily_sync.yml/runs?per_page=1",
              "--jq", ".workflow_runs[0] | {conclusion, url: .html_url, created_at}").strip()
    return {"upstream_commit": upstream, "last_run": json.loads(last) if last and last != "null" else None}


def junit_rows(junit_dir: Path, platform: str, night: dict) -> list[dict]:
    rows = []
    for file in sorted(junit_dir.rglob("*.xml")):
        try:
            root = ET.parse(file).getroot()
        except ET.ParseError:
            continue
        for case in root.iter("testcase"):
            status = "passed"
            for child, name in (("failure", "failure"), ("error", "error"), ("skipped", "skipped")):
                if case.find(child) is not None:
                    status = name
                    break
            rows.append({
                "date": night["date"],
                "head_sha": night["head_sha"],
                "platform": platform,
                # The Python and pandas versions (the directory) and the test shard.
                "suite": file.relative_to(junit_dir).with_suffix("").as_posix(),
                "test": f"{case.get('classname', '')}.{case.get('name', '')}".strip("."),
                "status": status,
                "time_s": float(case.get("time") or 0),
            })
    return rows


def run_logs(repo: str, run_id: int) -> list[dict]:
    """The complete log of every job of a run that ran, with its name and conclusion."""
    jobs = gh("api", f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100",
              "--jq", "[.jobs[] | select(.conclusion != null and .conclusion != \"skipped\") | {id, name, conclusion}]").strip()
    return [{**job, "log": gh("api", f"repos/{repo}/actions/jobs/{job['id']}/logs")} for job in json.loads(jobs or "[]")]


def joined_log(jobs: list[dict]) -> str:
    return "\n".join(f"==> {job['name']} ({job['conclusion']})\n{job['log']}" for job in jobs)


def failure_tail(jobs: list[dict]) -> str:
    """Each failed job's log up to its first error."""
    return "\n".join(
        f"==> {job['name']} ({nightly_data.LOG_TAIL_LINES} lines up to the first error)\n"
        f"{nightly_data.error_tail(job['log'])}"
        for job in jobs if job["conclusion"] == "failure"
    )


# ------------------------------------------------------------------------- storage


class Store:
    """The nightly store: one Parquet file per table and night, named by its date.

    The night goes to --work/store, which the workflow uploads to the S3 bucket at
    NIGHTLY_S3_URL; earlier nights are read back from there with s3(). S3 lists the
    bucket, so a glob reads exactly the nights it has, and none reads as no rows.
    Without NIGHTLY_S3_URL every night is read from --work/store.
    """

    def __init__(self, work: Path, date: str):
        self.local = work / "store"
        self.remote = os.environ.get("NIGHTLY_S3_URL", "").rstrip("/")
        self.date = date

    def path(self, rel: str) -> Path:
        path = self.local / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def night(self, table: str, schema: str) -> str:
        """The table's rows for the night alone."""
        return f"file({lit(str(self.local / table / f'{self.date}.parquet'))}, 'Parquet', {lit(schema)})"

    def nights(self, table: str, schema: str) -> str:
        """A FROM clause over the table's night and every night before it in the store."""
        if not self.remote:
            return f"file({lit(str(self.local / table / '*.parquet'))}, 'Parquet', {lit(schema)})"
        # The bucket may hold this night already, from an earlier publication of it.
        earlier = f"s3({lit(f'{self.remote}/{table}/*.parquet')}, 'Parquet', {lit(schema)})"
        return (f"(SELECT * FROM {earlier} WHERE date != {lit(self.date)} "
                f"UNION ALL SELECT * FROM {self.night(table, schema)})")

    def write(self, chdb, table: str, schema: str, rows: list[dict], work: Path) -> None:
        source = work / f"{table}.jsonl"
        write_jsonl(source, rows)
        # Named by date alone: a `date=...` directory would read as a Hive partition column.
        chdb.query(
            f"INSERT INTO FUNCTION file({lit(str(self.path(f'{table}/{self.date}.parquet')))}, 'Parquet') "
            f"SELECT * FROM file({lit(str(source))}, 'JSONEachRow', {lit(schema)}) "
            "SETTINGS engine_file_truncate_on_insert = 1"
        )


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


# --------------------------------------------------------------------------- main


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--work", required=True)
    parser.add_argument("--site", required=True)
    args = parser.parse_args(argv)

    import chdb  # the latest chdb-core release

    repo = os.environ["GITHUB_REPOSITORY"]
    work, site = Path(args.work), Path(args.site)
    night = json.loads((work / "night.json").read_text())
    date, sha = night["date"], night["head_sha"]
    sync = sync_state(repo, sha)
    engine = getattr(chdb, "__version__", "")
    store = Store(work, date)
    data = site / "data"
    data.mkdir(parents=True, exist_ok=True)

    tests = []
    for platform in PLATFORMS:
        tests += junit_rows(work / "junit" / platform, platform, night)
    sizes = {w["platform"]: w["size"] for w in night["wheels"]}
    runs, logged, static_logs = [], [], {}
    for platform in PLATFORMS:
        run = night["runs"].get(platform, {})
        mine = [t for t in tests if t["platform"] == platform]
        runs.append({
            "date": date,
            "head_sha": sha,
            "version": night["version"],
            "platform": platform,
            "conclusion": run.get("conclusion", "missing"),
            "duration_min": run.get("minutes", 0),
            "wheel_bytes": sizes.get(platform, 0),
            "tests_total": len(mine),
            "tests_failed": sum(t["status"] in ("failure", "error") for t in mine),
            "url": run.get("url", ""),
            "upstream_commit": sync["upstream_commit"],
            "engine": engine,
        })
        jobs = run_logs(repo, run["id"]) if run.get("id") else []
        if not jobs:
            continue
        # The complete log of every run, failed or not, for the API to serve.
        store.path(f"logs/{date}/{platform}.txt.gz").write_bytes(gzip.compress(joined_log(jobs).encode()))
        logged.append(platform)
        tail = failure_tail(jobs)
        if tail:
            # Without the API, the dashboard links a copy of the failures' tails beside it.
            static_logs[platform] = f"logs/{date}-{platform}.txt"
            (site / "logs").mkdir(parents=True, exist_ok=True)
            (site / static_logs[platform]).write_text(tail)

    chdb_night = night.get("chdb", {})
    last_run = sync["last_run"] or {}
    night_row = {
        "date": date,
        "head_sha": sha,
        "version": night["version"],
        "upstream_commit": sync["upstream_commit"],
        "sync_conclusion": last_run.get("conclusion") or "",
        "sync_url": last_run.get("url") or "",
        "sync_created_at": last_run.get("created_at") or "",
        "chdb_version": chdb_night.get("version", ""),
        "chdb_commit": chdb_night.get("commit", ""),
        "chdb_status": chdb_night.get("status", ""),
        "engine": engine,
        "generated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    store.write(chdb, "runs", RUNS_SCHEMA, runs, work)
    store.write(chdb, "nights", NIGHTS_SCHEMA, [night_row], work)
    if tests:
        store.write(chdb, "tests", TESTS_SCHEMA, tests, work)

    history = nightly_data.history(chdb, store.nights("runs", RUNS_SCHEMA), HISTORY_DAYS)
    failing = nightly_data.failing_tests(chdb, store.night("tests", TESTS_SCHEMA)) if tests else []

    # What the API's /latest and /history answer, for when the dashboard cannot reach it.
    static = {p: {"error_log": path} for p, path in static_logs.items()}
    latest = nightly_data.latest(night_row, runs, failing, repo=repo,
                                 site_url=os.environ.get("NIGHTLY_SITE_URL", ""), logs=static)
    (data / "latest.json").write_text(json.dumps(latest, indent=2) + "\n")
    (data / "history.json").write_text(json.dumps({"rows": history}) + "\n")
    api = os.environ.get("NIGHTLY_API_URL", "").rstrip("/")
    (site / "config.json").write_text(json.dumps({"api": api}) + "\n")
    for file in DASHBOARD.iterdir():
        if file.is_file():
            shutil.copy(file, site / file.name)
    print(f"Dashboard data for {date} ({sha[:12]}): {len(history)} history rows, {len(failing)} failing tests, "
          f"logs of {', '.join(logged) or 'no runs'}; store: {store.remote or 'local'}; API: {api or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
