#!/usr/bin/env python3
"""Finish a nightly build: the rolling `nightly` release and its pip index.

The four wheel workflows build main on a schedule and upload their wheel to the
`nightly` pre-release themselves (upload_wheel.sh). This runs after each of them
finishes, waits until all four runs for the same main commit are done, and then:

- moves the `nightly` tag to that commit and keeps the wheels of the newest --keep
  nights, a night being one commit,
- writes a PEP 503 index (simple/) of the wheels left into --site,
- downloads the runs' test results, writes --work/night.json for report.py, and
  sets the `ready` output.

No wheel is downloaded: names, sizes and sha256 digests come from the release API.
Nothing is published for a commit whose scheduled builds the nightly gate held
back because a ClickHouse sync pull request was open.
Only the release's assets, notes and tag move, so no `release: published` event
starts the release builds.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import subprocess
import sys
from pathlib import Path

TAG = "nightly"
# Platform -> wheel workflow. Order is the dashboard's order.
PLATFORMS = {
    "linux-x86_64": "build_linux_x86_wheels.yml",
    "linux-aarch64": "build_linux_arm64_wheels-gh.yml",
    "macos-arm64": "build_macos_arm64_wheels.yml",
    "macos-x86_64": "build_macos_x86_wheels.yml",
}
PROJECT = "chdb-core"
GATE = "Nightly gate"  # the job in each wheel workflow that can hold its build back


def gh(*args, check=True) -> str:
    proc = subprocess.run(["gh", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {proc.stderr.strip()}")
    return proc.stdout if proc.returncode == 0 else ""


def set_output(**values) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as fh:
            for key, value in values.items():
                fh.write(f"{key}={value}\n")


def parse_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def latest_run(repo: str, workflow: str, head_sha: str | None) -> dict | None:
    query = f"repos/{repo}/actions/workflows/{workflow}/runs?event=schedule&per_page=10"
    if head_sha:
        query += f"&head_sha={head_sha}"
    runs = json.loads(gh("api", query, "--jq", ".workflow_runs") or "[]")
    return runs[0] if runs else None


def release_assets(repo: str, release_id: str) -> list[dict]:
    out = gh("api", "--paginate", f"repos/{repo}/releases/{release_id}/assets?per_page=100",
             "--jq", ".[] | {name, size, digest, updated_at}")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def is_wheel(name: str) -> bool:
    return name.startswith("chdb_core-") and name.endswith(".whl")


def wheel_version(name: str) -> str:
    return name.split("-")[1]


def wheel_platform(name: str) -> str:
    tag = name[: -len(".whl")].split("-")[-1]
    system = "macos" if tag.startswith("macosx") else "linux"
    arch = "aarch64" if "aarch64" in tag else "arm64" if "arm64" in tag else "x86_64"
    return f"{system}-{arch}"


def sha256_of(asset: dict) -> str:
    digest = asset.get("digest") or ""
    return digest.split(":", 1)[1] if digest.startswith("sha256:") else ""


def dev_of(name: str) -> int:
    version = wheel_version(name)
    return int(version.rsplit(".dev", 1)[1]) if ".dev" in version else 0


def dev_number(committed_at: str) -> str:
    """The dev number set-main-wheel-version.py --nightly gives a commit."""
    return parse_time(committed_at).astimezone(dt.timezone.utc).strftime("%y%m%d%H%M")


def night_wheels(assets: list[dict], dev: str) -> list[dict]:
    """The wheels built from the night's commit: their version ends in .dev<dev>."""
    return [
        {"platform": wheel_platform(a["name"]), "name": a["name"], "size": a["size"], "sha256": sha256_of(a)}
        for a in assets
        if is_wheel(a["name"]) and wheel_version(a["name"]).endswith(f".dev{dev}")
    ]


def stale(assets: list[dict], keep: int) -> list[str]:
    """Wheels of all but the newest `keep` nights; a night's wheels share its dev number."""
    nights = sorted({dev_of(a["name"]) for a in assets if is_wheel(a["name"])}, reverse=True)
    old = set(nights[keep:])
    return sorted(a["name"] for a in assets if is_wheel(a["name"]) and dev_of(a["name"]) in old)


def built(repo: str, run: dict) -> bool:
    """False when the run's nightly gate held its build back."""
    jobs = gh("api", f"repos/{repo}/actions/runs/{run['id']}/jobs", "--jq", "[.jobs[] | {name, conclusion}]")
    return any(job["conclusion"] != "skipped" for job in json.loads(jobs or "[]") if job["name"] != GATE)


def minutes_between(start: str, end: str) -> float:
    return round((parse_time(end) - parse_time(start)).total_seconds() / 60, 1)


def pep503_pages(repo: str, assets: list[dict]) -> dict[str, str]:
    """Return {relative path: html} for a PEP 503 simple index of the release's wheels."""
    links = []
    for asset in sorted(assets, key=lambda a: a["name"], reverse=True):
        if not is_wheel(asset["name"]):
            continue
        url = f"https://github.com/{repo}/releases/download/{TAG}/{asset['name']}"
        if sha256_of(asset):
            url += f"#sha256={sha256_of(asset)}"
        links.append(
            f'    <a href="{html.escape(url)}" data-requires-python="&gt;=3.9">{html.escape(asset["name"])}</a><br>'
        )
    project = (
        "<!DOCTYPE html>\n<html>\n  <head><title>Links for chdb-core</title></head>\n  <body>\n"
        "    <h1>Links for chdb-core</h1>\n" + "\n".join(links) + "\n  </body>\n</html>\n"
    )
    root = (
        "<!DOCTYPE html>\n<html>\n  <body>\n"
        f'    <a href="{PROJECT}/">{PROJECT}</a>\n  </body>\n</html>\n'
    )
    return {"simple/index.html": root, f"simple/{PROJECT}/index.html": project}


def release_notes(night: dict, site: str, keep: int) -> str:
    lines = [
        "Rolling nightly builds of chdb-core from `main`, one per day. Not for production.",
        "",
    ]
    if site:
        lines += [
            "Install the newest nightly:",
            "```",
            f"pip install --pre --extra-index-url {site.rstrip('/')}/simple {PROJECT}",
            "```",
            f"Status, history and logs: {site}",
            "",
        ]
    if night["wheels"]:
        lines.append(f"Newest: `{night['version']}`, built from {night['head_sha'][:12]} on {night['date']}.")
    lines.append(f"The newest {keep} nightly builds are kept.")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--head-sha", default="", help="main commit whose scheduled builds to publish")
    parser.add_argument("--work", required=True)
    parser.add_argument("--site", required=True)
    parser.add_argument("--keep", type=int, default=20, help="nights whose wheels to keep")
    args = parser.parse_args(argv)

    repo = os.environ["GITHUB_REPOSITORY"]
    site_url = os.environ.get("NIGHTLY_SITE_URL", "")
    work, site = Path(args.work), Path(args.site)
    work.mkdir(parents=True, exist_ok=True)
    site.mkdir(parents=True, exist_ok=True)

    head_sha = args.head_sha
    if not head_sha:
        newest = latest_run(repo, PLATFORMS["linux-x86_64"], None)
        if not newest:
            print("No scheduled build has run yet")
            set_output(ready="false")
            return 0
        head_sha = newest["head_sha"]
    runs = {platform: latest_run(repo, workflow, head_sha) for platform, workflow in PLATFORMS.items()}
    pending = [p for p, r in runs.items() if r is None or r["status"] != "completed"]
    if pending:
        print(f"Waiting for {', '.join(pending)} to finish on {head_sha[:12]}")
        set_output(ready="false")
        return 0
    if not any(built(repo, run) for run in runs.values()):
        print(f"No nightly for {head_sha[:12]}: a sync pull request was open, so the builds waited")
        set_output(ready="false")
        return 0

    for platform, run in runs.items():
        gh("run", "download", str(run["id"]), "--repo", repo, "--name", f"junit-{platform}",
           "--dir", str(work / "junit" / platform), check=False)

    # A platform whose build or tests failed uploaded no wheel; with all four
    # failing on the first night there is no release yet.
    release_id = gh("api", f"repos/{repo}/releases/tags/{TAG}", "--jq", ".id", check=False).strip()
    assets = release_assets(repo, release_id) if release_id else []
    committed_at = gh("api", f"repos/{repo}/commits/{head_sha}", "--jq", ".commit.committer.date").strip()
    wheels = night_wheels(assets, dev_number(committed_at))
    night = {
        "date": min(r["created_at"] for r in runs.values())[:10],
        "head_sha": head_sha,
        "version": wheel_version(wheels[0]["name"]) if wheels else "",
        "wheels": wheels,
        "runs": {
            platform: {
                "id": run["id"],
                "conclusion": run["conclusion"] or "unknown",
                "url": run["html_url"],
                "minutes": minutes_between(run["run_started_at"], run["updated_at"]),
            }
            for platform, run in runs.items()
        },
    }

    deleted = []
    if release_id:
        gh("api", "-X", "PATCH", f"repos/{repo}/git/refs/tags/{TAG}", "-f", f"sha={head_sha}", "-F", "force=true")
        deleted = stale(assets, args.keep)
        for name in deleted:
            gh("release", "delete-asset", TAG, name, "--repo", repo, "--yes", check=False)
        notes = work / "notes.md"
        notes.write_text(release_notes(night, site_url, args.keep))
        gh("release", "edit", TAG, "--repo", repo, "--notes-file", str(notes))

    for rel, text in pep503_pages(repo, [a for a in assets if a["name"] not in deleted]).items():
        page = site / rel
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text(text)
    (work / "night.json").write_text(json.dumps(night, indent=2) + "\n")
    print(f"Night of {night['date']} ({head_sha[:12]}): {len(wheels)} of {len(PLATFORMS)} wheels on the "
          f"release; deleted {len(deleted)} old ones")
    set_output(ready="true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
