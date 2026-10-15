#!/usr/bin/env python3
"""Turn a night's results into the dashboard's data, with chDB doing the work.

publish.py leaves --work/night.json and the JUnit files of each platform. This
script adds the sync state, then lets chDB:

- write the night's rows as Parquet, one file per table, and the tail of every
  failed job's log into --work/store, which the workflow uploads to Vercel Blob,
- compute the history with SQL over the night and the nights before it, which
  chDB reads straight from the Blob store's public URLs with url() (from
  --work/store when BLOB_BASE_URL is unset), and the night's failing tests.

The test results of every night are stored too, for later analysis; the
dashboard reads only the night's.

The results go to --site/data/*.json, beside the static dashboard.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
DASHBOARD = HERE / "dashboard"
UPSTREAM_REPO = "ClickHouse/ClickHouse"
PLATFORMS = ["linux-x86_64", "linux-aarch64", "macos-arm64", "macos-x86_64"]
LOG_TAIL_LINES = 300

RUNS_SCHEMA = (
    "date Date, head_sha String, version String, platform String, conclusion String, "
    "duration_min Float64, wheel_bytes UInt64, tests_total UInt32, tests_failed UInt32, "
    "url String, upstream_commit String, engine String"
)
TESTS_SCHEMA = (
    "date Date, head_sha String, platform String, suite String, test String, status String, time_s Float64"
)
RUNS_DAYS = 90
# A night without a file (no build, or no Blob store yet) reads as empty.
QUERY_SETTINGS = "SETTINGS output_format_json_quote_64bit_integers = 0, engine_url_skip_empty_files = 1"


def gh(*args) -> str:
    proc = subprocess.run(["gh", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.stdout if proc.returncode == 0 else ""


def lit(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


# ---------------------------------------------------------------------- collection


def sync_state(repo: str, head_sha: str) -> dict:
    encoded = gh("api", f"repos/{repo}/contents/UPSTREAM_COMMIT?ref={head_sha}", "--jq", ".content").strip()
    upstream = base64.b64decode(encoded).decode().split()[0] if encoded else ""
    last = gh("api", f"repos/{repo}/actions/workflows/daily_sync.yml/runs?per_page=1",
              "--jq", ".workflow_runs[0] | {conclusion, url: .html_url, created_at}").strip()
    return {
        "upstream_commit": upstream,
        "upstream_url": f"https://github.com/{UPSTREAM_REPO}/commit/{upstream}" if upstream else "",
        "last_run": json.loads(last) if last and last != "null" else None,
    }


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
                "suite": file.stem,
                "test": f"{case.get('classname', '')}.{case.get('name', '')}".strip("."),
                "status": status,
                "time_s": float(case.get("time") or 0),
            })
    return rows


def failed_log_tail(repo: str, run_id: int) -> str:
    jobs = gh("api", f"repos/{repo}/actions/runs/{run_id}/jobs",
              "--jq", '[.jobs[] | select(.conclusion == "failure") | {id, name}]').strip()
    parts = []
    for job in json.loads(jobs or "[]"):
        log = gh("api", f"repos/{repo}/actions/jobs/{job['id']}/logs")
        tail = "\n".join(log.splitlines()[-LOG_TAIL_LINES:])
        parts.append(f"==> {job['name']} (last {LOG_TAIL_LINES} lines)\n{tail}\n")
    return "\n".join(parts)


# ------------------------------------------------------------------------- storage


class Store:
    """The nightly rows: one Parquet file per table and night, named by its date.

    The night itself goes to --work/store, which the workflow uploads to the
    public Vercel Blob store at BLOB_BASE_URL; earlier nights are read back from
    there. Without BLOB_BASE_URL every night is read from --work/store.
    """

    def __init__(self, work: Path, date: str):
        self.local = work / "store"
        self.remote = os.environ.get("BLOB_BASE_URL", "").rstrip("/")
        self.date = dt.date.fromisoformat(date)

    def path(self, rel: str) -> Path:
        path = self.local / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def night(self, table: str, schema: str) -> str:
        """The table's rows for the night alone."""
        return f"file({lit(str(self.local / table / f'{self.date}.parquet'))}, 'Parquet', {lit(schema)})"

    def nights(self, table: str, schema: str, days: int) -> str:
        """A FROM clause over the table's night and the `days` nights before it."""
        if not self.remote:
            return f"file({lit(str(self.local / table / '*.parquet'))}, 'Parquet', {lit(schema)})"
        dates = ",".join((self.date - dt.timedelta(days=d)).isoformat() for d in range(1, days + 1))
        earlier = f"url({lit(f'{self.remote}/{table}/{{{dates}}}.parquet')}, 'Parquet', {lit(schema)})"
        night = f"file({lit(str(self.local / table / f'{self.date}.parquet'))}, 'Parquet', {lit(schema)})"
        return f"(SELECT * FROM {earlier} UNION ALL SELECT * FROM {night})"


def query(chdb, sql: str) -> list[dict]:
    out = str(chdb.query(sql, "JSONEachRow"))
    return [json.loads(line) for line in out.splitlines() if line.strip()]


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
    runs, logs = [], {}
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
        if run.get("conclusion") == "failure":
            tail = failed_log_tail(repo, run["id"])
            if tail:
                logs[platform] = tail

    runs_file, tests_file = work / "runs.jsonl", work / "tests.jsonl"
    write_jsonl(runs_file, runs)
    write_jsonl(tests_file, tests)
    # Named by date alone: a `date=...` directory would read as a Hive partition column.
    chdb.query(
        f"INSERT INTO FUNCTION file({lit(str(store.path(f'runs/{date}.parquet')))}, 'Parquet') "
        f"SELECT * FROM file({lit(str(runs_file))}, 'JSONEachRow', {lit(RUNS_SCHEMA)}) "
        "SETTINGS engine_file_truncate_on_insert = 1"
    )
    if tests:
        chdb.query(
            f"INSERT INTO FUNCTION file({lit(str(store.path(f'tests/{date}.parquet')))}, 'Parquet') "
            f"SELECT * FROM file({lit(str(tests_file))}, 'JSONEachRow', {lit(TESTS_SCHEMA)}) "
            "SETTINGS engine_file_truncate_on_insert = 1"
        )

    log_links = {}
    for platform, tail in logs.items():
        name = f"{date}-{platform}.txt"
        local = site / "logs" / name
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(tail)
        log_links[platform] = f"logs/{name}"
        chdb.query(
            f"INSERT INTO FUNCTION file({lit(str(store.path(f'logs/{date}/{platform}.txt.gz')))}, "
            "'LineAsString', 'line String', 'gzip') "
            f"SELECT line FROM file({lit(str(local))}, 'LineAsString', 'line String') "
            "SETTINGS engine_file_truncate_on_insert = 1"
        )

    all_runs = store.nights("runs", RUNS_SCHEMA, RUNS_DAYS)
    history = query(chdb, f"""
        SELECT toString(date) AS date, platform, conclusion, round(duration_min, 1) AS duration_min,
               round(wheel_bytes / 1048576, 1) AS wheel_mb, tests_total, tests_failed,
               version, head_sha, url
        FROM {all_runs}
        WHERE date >= today() - 90
        ORDER BY date DESC, platform
        LIMIT 1 BY date, platform
        {QUERY_SETTINGS}, prefer_column_name_to_alias = 1""")

    failing = []
    if tests:
        failing = query(chdb, f"""
            SELECT DISTINCT platform, test
            FROM {store.night("tests", TESTS_SCHEMA)}
            WHERE status IN ('failure', 'error')
            ORDER BY platform, test
            LIMIT 200
            {QUERY_SETTINGS}""")

    site_url = os.environ.get("NIGHTLY_SITE_URL", "").rstrip("/")
    latest = {
        "generated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "engine": {"version": engine},
        "night": {"date": date, "head_sha": sha, "version": night["version"],
                  "commit_url": f"https://github.com/{repo}/commit/{sha}"},
        "platforms": [
            {**{k: r[k] for k in ("platform", "conclusion", "duration_min", "tests_total", "tests_failed", "url")},
             "wheel_mb": round(r["wheel_bytes"] / 1048576, 1), "log": log_links.get(r["platform"], "")}
            for r in runs
        ],
        "sync": sync,
        "failing_tests": failing,
        "install": {
            "index_url": f"{site_url}/simple" if site_url else "",
            "release_url": f"https://github.com/{repo}/releases/tag/nightly",
        },
    }
    (data / "latest.json").write_text(json.dumps(latest, indent=2) + "\n")
    (data / "history.json").write_text(json.dumps({"rows": history}) + "\n")
    for file in DASHBOARD.iterdir():
        if file.is_file():
            shutil.copy(file, site / file.name)
    print(f"Dashboard data for {date} ({sha[:12]}): {len(history)} history rows, "
          f"{len(failing)} failing tests; storage: {'Vercel Blob' if store.remote else 'local'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
