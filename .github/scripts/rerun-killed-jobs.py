#!/usr/bin/env python3
"""Rerun a run's failed jobs once when a runner restart killed every one of them.

The self-hosted runners run unattended-upgrades at 06:00 UTC plus up to an hour,
and needrestart then restarts the runner service, killing the job it is running.
Such a job fails with "The runner has received a shutdown signal" in its log. Its
annotation only says "The operation was canceled.", which a timeout produces too,
so the log is what tells. A rerun inside that hour can land on an instance whose
restart is still to come, so it waits until the hour is over.

  decide  Runs when a watched workflow finishes. If every failed job of the run's
          first attempt was killed this way, waits out the window and reruns the
          failed jobs, which also reruns the jobs skipped because of them. A run
          that a newer run of the same branch and event has replaced is left
          alone: the rerun would join their concurrency group and cancel the
          newer one. So is a pull request's run once the pull request is closed
          or merged.
  follow  Waits for that rerun to finish, then dispatches the workflow its
          completion would have started. A rerun requested with the workflow's
          own token raises no workflow_run event, so nightly_publish.yml and
          publish_checksums.yml would otherwise never see it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SIGNATURE = "The runner has received a shutdown signal"
# The restarts are over by 07:00 UTC; the margin covers an upgrade that starts late in the hour.
WINDOW = (dt.time(6, 0), dt.time(7, 10))
# The workflows nightly_publish.yml and publish_checksums.yml follow.
WHEEL_WORKFLOWS = {
    "Build & Test Linux x86_64",
    "Build & Test Linux arm64",
    "Build & Test macOS x86_64",
    "Build & Test macOS arm64",
}
SYNC_BRANCH = "sync/clickhouse-master"
POLL_SECONDS = 300


def gh(*args: str) -> str:
    proc = subprocess.run(["gh", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {proc.stderr.strip()}")
    return proc.stdout


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class GitHub:
    def __init__(self, repo: str, token: str):
        self.repo = repo
        self.token = token

    def get(self, path: str) -> dict:
        return json.loads(gh("api", f"repos/{self.repo}/{path}"))

    def jobs(self, run_id: int, attempt: int) -> list[dict]:
        out = gh("api", "--paginate", f"repos/{self.repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100",
                 "--jq", ".jobs[]")
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    def log(self, job_id: int) -> str | None:
        """The job's log, or None when it cannot be had.

        Fetched by hand: gh refuses to print a log that holds terminal escape
        sequences, and the storage URL the API redirects to must not get the token.
        """
        request = urllib.request.Request(
            f"https://api.github.com/repos/{self.repo}/actions/jobs/{job_id}/logs",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json"},
        )
        try:
            urllib.request.build_opener(_NoRedirect).open(request, timeout=60)
        except urllib.error.HTTPError as e:
            location = e.headers.get("Location") if 300 <= e.code < 400 else None
            if not location:
                print(f"No log for job {job_id}: HTTP {e.code}")
                return None
            with urllib.request.urlopen(location, timeout=300) as response:
                return response.read().decode("utf-8", "replace")
        return None

    def open_pull_heads(self) -> set[tuple[str, str]]:
        out = gh("api", "--paginate", f"repos/{self.repo}/pulls?state=open&per_page=100",
                 "--jq", '.[] | [.head.repo.full_name // "", .head.ref] | @tsv')
        return {tuple(line.split("\t", 1)) for line in out.splitlines() if "\t" in line}

    def rerun_failed(self, run_id: int) -> None:
        gh("run", "rerun", str(run_id), "--failed", "--repo", self.repo)

    def dispatch(self, workflow: str, ref: str, inputs: dict[str, str]) -> None:
        args = ["workflow", "run", workflow, "--repo", self.repo, "--ref", ref]
        for key, value in inputs.items():
            args += ["-f", f"{key}={value}"]
        gh(*args)


def seconds_to_wait(now: dt.datetime) -> int:
    """Seconds left of the restart window, 0 outside it. `now` is in UTC."""
    start = now.replace(hour=WINDOW[0].hour, minute=WINDOW[0].minute, second=0, microsecond=0)
    end = now.replace(hour=WINDOW[1].hour, minute=WINDOW[1].minute, second=0, microsecond=0)
    return int((end - now).total_seconds()) if start <= now < end else 0


def own_group(run: dict, owner: str) -> bool:
    """Whether the run has a concurrency group of its own: main's build of a merged sync.

    The same test as the wheel workflows' concurrency keys.
    """
    message = (run.get("head_commit") or {}).get("message") or ""
    return run["event"] == "push" and f" from {owner}/{SYNC_BRANCH}" in message


def superseded(api, run: dict) -> bool:
    branch = urllib.parse.quote(run["head_branch"] or "", safe="")
    newest = api.get(f"actions/workflows/{run['workflow_id']}/runs"
                     f"?branch={branch}&event={run['event']}&per_page=1")["workflow_runs"]
    return bool(newest) and newest[0]["id"] != run["id"]


def pull_request_closed(api, run: dict) -> bool:
    """Whether the run is a pull request's whose pull request is no longer open.

    Matched by head repository and branch: the run lists no pull request for one
    from a fork.
    """
    head = ((run.get("head_repository") or {}).get("full_name") or "", run["head_branch"] or "")
    return run["event"] == "pull_request" and head not in api.open_pull_heads()


def follow_up(run: dict, default_branch: str) -> str:
    """The dispatch the rerun's completion needs, as 'workflow key=value', or ''.

    publish.py publishes nothing for a commit that merges no sync, so every push to
    the default branch gets one, squash-merged syncs included.
    """
    if run["name"] not in WHEEL_WORKFLOWS:
        return ""
    if run["event"] == "push" and run["head_branch"] == default_branch:
        return f"nightly_publish.yml head_sha={run['head_sha']}"
    if run["event"] == "release":
        return f"publish_checksums.yml TAG_NAME={run['head_branch']}"
    return ""


def decide(api, run_id: int, owner: str, default_branch: str, dry_run: bool, now, sleep) -> dict:
    nothing = {"rerun": "false", "dispatch": ""}
    run = api.get(f"actions/runs/{run_id}")
    if run["run_attempt"] != 1 or run["conclusion"] != "failure":
        print(f"Attempt {run['run_attempt']} ended {run['conclusion']}: only a failed first attempt is rerun")
        return nothing
    failed = [job for job in api.jobs(run_id, 1) if job["conclusion"] == "failure"]
    if not failed:
        print("No job failed")
        return nothing
    others = [job["name"] for job in failed if SIGNATURE not in (api.log(job["id"]) or "")]
    if others:
        print(f"Not killed by a runner restart, left alone: {', '.join(others)}")
        return nothing
    print(f"Killed by a runner restart: {', '.join(job['name'] for job in failed)}")

    wait = seconds_to_wait(now())
    if wait:
        print(f"Inside the restart window: waiting {wait // 60} min")
        if not dry_run:
            sleep(wait)
        run = api.get(f"actions/runs/{run_id}")
        if run["run_attempt"] != 1:
            print("Rerun meanwhile")
            return nothing
    if not own_group(run, owner) and superseded(api, run):
        print(f"A newer run of {run['head_branch']} ({run['event']}) replaced this one")
        return nothing
    if pull_request_closed(api, run):
        print(f"The pull request from {run['head_branch']} is no longer open")
        return nothing

    dispatch = follow_up(run, default_branch)
    if dry_run:
        print(f"Dry run: would rerun the failed jobs{f', then dispatch {dispatch}' if dispatch else ''}")
        return nothing
    api.rerun_failed(run_id)
    print(f"Reran the failed jobs of {run['html_url']}")
    return {"rerun": "true", "dispatch": dispatch}


def follow(api, run_id: int, dispatch: str, default_branch: str, minutes: int, now, sleep) -> dict:
    deadline = now() + dt.timedelta(minutes=minutes)
    while True:
        run = api.get(f"actions/runs/{run_id}")
        if run["run_attempt"] > 1 and run["status"] == "completed":
            workflow, *pairs = dispatch.split()
            api.dispatch(workflow, default_branch, dict(pair.split("=", 1) for pair in pairs))
            print(f"Attempt {run['run_attempt']} ended {run['conclusion']}; dispatched {dispatch}")
            return {"done": "true"}
        if now() >= deadline:
            print(f"Attempt {run['run_attempt']} is still {run['status']}")
            return {"done": "false"}
        sleep(POLL_SECONDS)


def set_output(values: dict) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as fh:
            for key, value in values.items():
                fh.write(f"{key}={value}\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("decide")
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--dry-run", action="store_true", help="report what would be done; no wait, no rerun")
    p = sub.add_parser("follow")
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--dispatch", required=True, help="'workflow key=value ...' to dispatch once the rerun ends")
    p.add_argument("--minutes", type=int, required=True, help="how long to wait in this job")
    args = parser.parse_args(argv)

    api = GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"])
    default_branch = os.environ.get("DEFAULT_BRANCH", "main")

    def utcnow() -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    if args.command == "decide":
        owner = os.environ["GITHUB_REPOSITORY"].split("/")[0]
        result = decide(api, args.run_id, owner, default_branch, args.dry_run, utcnow, time.sleep)
    else:
        result = follow(api, args.run_id, args.dispatch, default_branch, args.minutes, utcnow, time.sleep)
    set_output(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
