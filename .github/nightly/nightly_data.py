"""What the nightly dashboard shows, computed by chDB from the nightly store.

Shared by report.py, which computes it in CI when a night is published, and by the
nightly API (api/app.py), which computes it on request in AWS Lambda. Both read the
store, one file per table and night:

  runs/<date>.parquet             a row per platform: conclusion, time, wheel size, tests
  tests/<date>.parquet            a row per test case and platform
  nights/<date>.parquet           a row: the commit, the versions, the sync state
  logs/<date>/<platform>.txt.gz   the complete log of every job of the platform's run;
                                  logs/<date>/chdb.txt.gz is the chdb nightly's tests

The callers pass chDB in, and the FROM clauses for the store they read.
"""

from __future__ import annotations

import json

PLATFORMS = ["linux-x86_64", "linux-aarch64", "macos-arm64", "macos-x86_64"]
LOG_TAIL_LINES = 300
UPSTREAM_REPO = "ClickHouse/ClickHouse"

RUNS_SCHEMA = (
    "date Date, head_sha String, version String, platform String, conclusion String, "
    "duration_min Float64, wheel_bytes UInt64, tests_total UInt32, tests_failed UInt32, "
    "url String, upstream_commit String, engine String"
)
TESTS_SCHEMA = (
    "date Date, head_sha String, platform String, suite String, test String, status String, time_s Float64"
)
NIGHTS_SCHEMA = (
    "date Date, head_sha String, version String, upstream_commit String, "
    "sync_conclusion String, sync_url String, sync_created_at String, "
    "chdb_version String, chdb_commit String, chdb_status String, engine String, generated_at String"
)
SETTINGS = "SETTINGS output_format_json_quote_64bit_integers = 0"


def lit(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def rows(chdb, sql: str) -> list[dict]:
    out = str(chdb.query(sql, "JSONEachRow"))
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def history(chdb, runs: str, days: int) -> list[dict]:
    """The runs of the last `days` nights, newest first; `runs` is a FROM clause."""
    return rows(chdb, f"""
        SELECT toString(date) AS date, platform, conclusion, round(duration_min, 1) AS duration_min,
               round(wheel_bytes / 1048576, 1) AS wheel_mb, tests_total, tests_failed,
               version, head_sha, url
        FROM {runs}
        WHERE date >= today() - {int(days)}
        ORDER BY date DESC, platform
        LIMIT 1 BY date, platform
        {SETTINGS}, prefer_column_name_to_alias = 1""")


def failing_tests(chdb, tests: str) -> list[dict]:
    """A night's failed test cases; `tests` is a FROM clause over that night alone."""
    return rows(chdb, f"""
        SELECT DISTINCT platform, test
        FROM {tests}
        WHERE status IN ('failure', 'error')
        ORDER BY platform, test
        LIMIT 200
        {SETTINGS}""")


def error_tail(log: str, lines: int = LOG_TAIL_LINES) -> str:
    """The lines of a log up to its first error: what follows is the always-run steps
    and the cleanup. The last lines when it has none."""
    text = log.splitlines()
    end = next((i + 1 for i, line in enumerate(text) if "##[error]" in line), len(text))
    return "\n".join(text[max(0, end - lines):end]) + "\n"


def latest(night: dict, runs: list[dict], failing: list[dict], *, repo: str, site_url: str,
           logs: dict[str, dict]) -> dict:
    """The newest night, as the dashboard reads it (data/latest.json, or the API's /latest).

    `night` is the night's row of the nights table, `runs` its rows of the runs table,
    and `logs` maps a platform to the links of its log ("log", "error_log").
    """
    upstream = night.get("upstream_commit", "")
    last_run = None
    if night.get("sync_url"):
        last_run = {"conclusion": night.get("sync_conclusion") or None, "url": night["sync_url"],
                    "created_at": night.get("sync_created_at", "")}
    site_url = site_url.rstrip("/")
    by_platform = {r["platform"]: r for r in runs}
    platforms = []
    for platform in PLATFORMS:
        r = by_platform.get(platform, {})
        platforms.append({
            "platform": platform,
            "conclusion": r.get("conclusion", "missing"),
            "duration_min": r.get("duration_min", 0),
            "tests_total": r.get("tests_total", 0),
            "tests_failed": r.get("tests_failed", 0),
            "url": r.get("url", ""),
            "wheel_mb": round(int(r.get("wheel_bytes", 0)) / 1048576, 1),
            "log": logs.get(platform, {}).get("log", ""),
            "error_log": logs.get(platform, {}).get("error_log", ""),
        })
    chdb_commit = night.get("chdb_commit", "")
    return {
        "generated_at": night.get("generated_at", ""),
        "engine": {"version": night.get("engine", "")},
        "night": {"date": str(night["date"]), "head_sha": night["head_sha"], "version": night["version"],
                  "commit_url": f"https://github.com/{repo}/commit/{night['head_sha']}"},
        "platforms": platforms,
        "sync": {
            "upstream_commit": upstream,
            "upstream_url": f"https://github.com/{UPSTREAM_REPO}/commit/{upstream}" if upstream else "",
            "last_run": last_run,
        },
        "chdb": {
            "status": night.get("chdb_status", ""),
            "version": night.get("chdb_version", ""),
            "commit": chdb_commit,
            "commit_url": f"https://github.com/chdb-io/chdb/commit/{chdb_commit}" if chdb_commit else "",
            "log": logs.get("chdb", {}).get("log", ""),
        },
        "failing_tests": failing,
        "install": {
            "index_url": f"{site_url}/simple" if site_url else "",
            "release_url": f"https://github.com/{repo}/releases/tag/nightly",
        },
    }
