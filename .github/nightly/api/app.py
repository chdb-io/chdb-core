"""The nightly API: the dashboard's data, computed by chDB from the nightly store in S3.

Runs in AWS Lambda behind a function URL (payload format 2.0). The endpoints are
fixed and read-only; no request carries SQL:

  GET /health
  GET /latest                              the newest night, as data/latest.json
  GET /history?days=N                      the runs of the last N nights (90; at most
                                           365), as data/history.json
  GET /logs/<date>/<platform>              the complete log of the platform's run
  GET /logs/<date>/<platform>?tail=error   its lines up to the first error
  GET /logs/<date>/chdb                    the chdb nightly's build

The store is the bucket NIGHTLY_BUCKET, which report.py fills (nightly_data.py lists
its files); the function's role may only read it. The function URL answers CORS.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import re
import time

# chDB keeps its working files under the current directory, and /tmp is the only one
# Lambda lets a function write.
if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
    os.chdir("/tmp")

import boto3  # noqa: E402

import nightly_data  # noqa: E402
from nightly_data import NIGHTS_SCHEMA, PLATFORMS, RUNS_SCHEMA, SETTINGS, TESTS_SCHEMA, lit  # noqa: E402

BUCKET = os.environ.get("NIGHTLY_BUCKET", "")
REGION = os.environ.get("AWS_REGION", "")
# Only to try it against a local S3 (MinIO, moto): http://host:port, path-style.
ENDPOINT = os.environ.get("NIGHTLY_S3_ENDPOINT", "").rstrip("/")
REPO = os.environ.get("GITHUB_REPOSITORY", "chdb-io/chdb-core")
SITE_URL = os.environ.get("NIGHTLY_SITE_URL", "")
CACHE_SECONDS = 300   # the store changes once a night
MAX_DAYS = 365
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

_s3 = boto3.client("s3", endpoint_url=ENDPOINT or None)
_cache: dict[str, tuple[float, dict]] = {}
_chdb = None


def engine():
    """chDB, imported by the first request rather than at start-up: loading its 600 MB
    library from a cold image can outlast the 10 seconds Lambda gives initialization,
    and an initialization that runs out starts over within the first request."""
    global _chdb
    if _chdb is None:
        import chdb
        _chdb = chdb
    return _chdb


def table(name: str, schema: str, night: str = "*") -> str:
    base = f"{ENDPOINT}/{BUCKET}" if ENDPOINT else f"https://{BUCKET}.s3.{REGION}.amazonaws.com"
    url = f"{base}/{name}/{night}.parquet"
    return f"s3({lit(url)}, 'Parquet', {lit(schema)})"


def stored(prefix: str) -> set[str]:
    listing = _s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return {item["Key"] for item in listing.get("Contents", [])}


def respond(status: int, body, content_type: str = "application/json", **headers) -> dict:
    if not isinstance(body, (str, bytes)):
        body = json.dumps(body)
    response = {"statusCode": status, "headers": {"Content-Type": content_type, **headers}}
    if isinstance(body, bytes):
        response.update(body=base64.b64encode(body).decode(), isBase64Encoded=True)
    else:
        response["body"] = body
    return response


def ok(body, content_type: str = "application/json", **headers) -> dict:
    return respond(200, body, content_type, **{"Cache-Control": f"public, max-age={CACHE_SECONDS}", **headers})


def latest(api: str) -> dict:
    nights = nightly_data.rows(engine(), f"""
        SELECT * FROM {table("nights", NIGHTS_SCHEMA)}
        ORDER BY date DESC, generated_at DESC LIMIT 1 {SETTINGS}""")
    if not nights:
        return respond(404, {"error": "no nightly has been published"})
    night = nights[0]
    date = str(night["date"])
    runs = nightly_data.rows(engine(), f"SELECT * FROM {table('runs', RUNS_SCHEMA, date)} {SETTINGS}")
    keys = stored(f"logs/{date}/") | stored(f"tests/{date}")
    failing = []
    if f"tests/{date}.parquet" in keys:
        failing = nightly_data.failing_tests(engine(), table("tests", TESTS_SCHEMA, date))
    logs = {}
    for name in [*PLATFORMS, "chdb"]:
        if f"logs/{date}/{name}.txt.gz" in keys:
            logs[name] = {"log": f"{api}/logs/{date}/{name}"}
    for run in runs:
        if run["conclusion"] == "failure" and run["platform"] in logs:
            logs[run["platform"]]["error_log"] = f"{api}/logs/{date}/{run['platform']}?tail=error"
    return ok(nightly_data.latest(night, runs, failing, repo=REPO, site_url=SITE_URL, logs=logs))


def history(query: dict) -> dict:
    try:
        days = int(query.get("days", 90))
    except ValueError:
        return respond(400, {"error": "days must be a number"})
    if not 1 <= days <= MAX_DAYS:
        return respond(400, {"error": f"days must be between 1 and {MAX_DAYS}"})
    return ok({"rows": nightly_data.history(engine(), table("runs", RUNS_SCHEMA), days)})


def log(date: str, name: str, query: dict) -> dict:
    if not DATE.fullmatch(date) or name not in [*PLATFORMS, "chdb"]:
        return respond(404, {"error": "no such log"})
    try:
        body = _s3.get_object(Bucket=BUCKET, Key=f"logs/{date}/{name}.txt.gz")["Body"].read()
    except _s3.exceptions.NoSuchKey:
        return respond(404, {"error": "no such log"})
    if query.get("tail") == "error":
        return ok(nightly_data.error_tail(gzip.decompress(body).decode(errors="replace")), "text/plain; charset=utf-8")
    # Passed on compressed: a run's log is several MB, and a function URL answers at most 6.
    return ok(body, "text/plain; charset=utf-8", **{"Content-Encoding": "gzip"})


def route(method: str, path: str, query: dict, api: str) -> dict:
    if method not in ("GET", "HEAD"):
        return respond(405, {"error": "only GET"})
    parts = [p for p in path.split("/") if p]
    if parts == ["health"]:
        return respond(200, {"status": "ok", "engine": getattr(engine(), "__version__", ""), "bucket": BUCKET})
    if parts == ["latest"]:
        return latest(api)
    if parts == ["history"]:
        return history(query)
    if len(parts) == 3 and parts[0] == "logs":
        return log(parts[1], parts[2], query)
    return respond(404, {"error": "no such endpoint"})


def handler(event, context):
    http = event.get("requestContext", {}).get("http", {})
    method, path = http.get("method", "GET"), event.get("rawPath", "/")
    query = event.get("queryStringParameters") or {}
    api = f"https://{event.get('requestContext', {}).get('domainName', '')}"
    key = f"{path}?{json.dumps(query, sort_keys=True)}"
    cached = _cache.get(key)
    if method == "GET" and cached and cached[0] > time.time():
        return cached[1]
    try:
        response = route(method, path, query, api)
    except Exception as e:  # the message, not a traceback, reaches the caller
        print(f"{method} {path}: {type(e).__name__}: {e}")
        return respond(500, {"error": "the query failed"})
    if response["statusCode"] == 200 and not path.startswith("/logs/"):
        _cache[key] = (time.time() + CACHE_SECONDS, response)
    return response
