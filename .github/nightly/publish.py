#!/usr/bin/env python3
"""Finish a nightly build: the rolling `nightly` release and its pip index.

The nightly is main's build of a merged ClickHouse sync pull request: the four
wheel workflows build that push and upload their wheel to the `nightly` pre-release
themselves (upload_wheel.sh). This runs after each of them finishes; for a sync's
merge it waits until all four runs for the commit are done, and then:

- moves the `nightly` tag to that commit and keeps the wheels of the newest --keep
  nights, a night being one commit,
- downloads the runs' test results, writes --work/night.json for chdb_nightly.py
  and report.py, and sets the `ready` output.

With --index, once chdb_nightly.py has added the chdb nightly to the release, it
writes a PEP 503 index (simple/) of the release's wheels, chdb-core's and chdb's,
into --site and the release notes.

No wheel is downloaded: names, sizes and sha256 digests come from the release API.
Only the release's assets, notes and tag move, so no `release: published` event
starts the release builds. A push that merges no sync publishes nothing.
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
# Distribution -> the prefix of its wheel names. chdb is chdb-io/chdb's pure-Python
# package, built by chdb_nightly.py against the night's chdb-core.
PROJECTS = {"chdb-core": "chdb_core-", "chdb": "chdb-"}
SYNC_BRANCH = "sync/clickhouse-master"


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
    query = f"repos/{repo}/actions/workflows/{workflow}/runs?event=push&branch=main&per_page=10"
    if head_sha:
        query += f"&head_sha={head_sha}"
    runs = json.loads(gh("api", query, "--jq", ".workflow_runs") or "[]")
    return runs[0] if runs else None


def release_assets(repo: str, release_id: str) -> list[dict]:
    out = gh("api", "--paginate", f"repos/{repo}/releases/{release_id}/assets?per_page=100",
             "--jq", ".[] | {name, size, digest, updated_at}")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def wheel_project(name: str) -> str:
    if name.endswith(".whl"):
        for project, prefix in PROJECTS.items():
            if name.startswith(prefix):
                return project
    return ""


def is_wheel(name: str) -> bool:
    return bool(wheel_project(name))


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


def night_wheels(assets: list[dict], dev: str, project: str = "chdb-core") -> list[dict]:
    """The project's wheels of the night: their version ends in .dev<dev>."""
    return [
        {"platform": wheel_platform(a["name"]), "name": a["name"], "size": a["size"], "sha256": sha256_of(a)}
        for a in assets
        if wheel_project(a["name"]) == project and wheel_version(a["name"]).endswith(f".dev{dev}")
    ]


def stale(assets: list[dict], keep: int) -> list[str]:
    """Wheels of all but the newest `keep` nights; a night's wheels, chdb's included, share
    its dev number."""
    nights = sorted({dev_of(a["name"]) for a in assets if is_wheel(a["name"])}, reverse=True)
    old = set(nights[keep:])
    return sorted(a["name"] for a in assets if is_wheel(a["name"]) and dev_of(a["name"]) in old)


def is_sync_merge(repo: str, sha: str) -> bool:
    """Whether the commit merged the ClickHouse sync pull request, as the wheel workflows' Nightly gate decides.

    A sync pull request comes from this repository's sync branch; whichever way it
    was merged, GitHub records the commit it put on main as its merge_commit_sha.
    """
    pulls = json.loads(gh("api", f"repos/{repo}/pulls?state=closed&base=main&sort=updated&direction=desc&per_page=100"))
    return any(
        pull.get("merged_at") and pull.get("merge_commit_sha") == sha and pull["head"]["ref"] == SYNC_BRANCH
        and (pull["head"].get("repo") or {}).get("full_name") == repo
        for pull in pulls
    )


def minutes_between(start: str, end: str) -> float:
    return round((parse_time(end) - parse_time(start)).total_seconds() / 60, 1)


def pep503_pages(repo: str, assets: list[dict]) -> dict[str, str]:
    """Return {relative path: html} for a PEP 503 simple index of the release's wheels."""
    links = {project: [] for project in PROJECTS}
    for asset in sorted(assets, key=lambda a: a["name"], reverse=True):
        project = wheel_project(asset["name"])
        if not project:
            continue
        url = f"https://github.com/{repo}/releases/download/{TAG}/{asset['name']}"
        if sha256_of(asset):
            url += f"#sha256={sha256_of(asset)}"
        links[project].append(
            f'    <a href="{html.escape(url)}" data-requires-python="&gt;=3.9">{html.escape(asset["name"])}</a><br>'
        )
    pages = {}
    for project, lines in links.items():
        if lines:
            pages[f"simple/{project}/index.html"] = (
                f"<!DOCTYPE html>\n<html>\n  <head><title>Links for {project}</title></head>\n  <body>\n"
                f"    <h1>Links for {project}</h1>\n" + "\n".join(lines) + "\n  </body>\n</html>\n"
            )
    pages["simple/index.html"] = (
        "<!DOCTYPE html>\n<html>\n  <body>\n"
        + "".join(f'    <a href="{project}/">{project}</a><br>\n' for project, lines in links.items() if lines)
        + "  </body>\n</html>\n"
    )
    return pages


def release_notes(night: dict, site: str, keep: int) -> str:
    lines = [
        "Rolling nightly builds of chdb-core from `main`, one per day, and of chdb from "
        "chdb-io/chdb's `main` against it. Not for production.",
        "",
    ]
    chdb = night.get("chdb", {})
    if site:
        index = f"{site.rstrip('/')}/simple"
        lines += [
            "Install the newest nightly:",
            "```",
            f"pip install --pre --extra-index-url {index} chdb-core",
            f"pip install --pre --extra-index-url {index} chdb",
            "```",
            f"Status, history and logs: {site}",
            "",
        ]
    if night["wheels"]:
        lines.append(f"Newest: chdb-core `{night['version']}`, built from {night['head_sha'][:12]} on {night['date']}.")
    if chdb.get("status") == "published":
        lines.append(f"chdb `{chdb['version']}` is chdb-io/chdb@{chdb['commit'][:12]}, "
                     f"pinned to chdb-core `{night['version']}`.")
    lines.append(f"The newest {keep} nightly builds are kept.")
    return "\n".join(lines) + "\n"


def write_index(repo: str, work: Path, site: Path, site_url: str, keep: int) -> int:
    """The --index step: the simple index of the release's wheels, and its notes."""
    night = json.loads((work / "night.json").read_text())
    release_id = gh("api", f"repos/{repo}/releases/tags/{TAG}", "--jq", ".id", check=False).strip()
    if not release_id:
        print("No nightly release yet; no index to write")
        return 0
    assets = release_assets(repo, release_id)
    for rel, text in pep503_pages(repo, assets).items():
        page = site / rel
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text(text)
    notes = work / "notes.md"
    notes.write_text(release_notes(night, site_url, keep))
    gh("release", "edit", TAG, "--repo", repo, "--notes-file", str(notes))
    wheels = sum(1 for a in assets if is_wheel(a["name"]))
    print(f"Index of {wheels} wheels written; chdb nightly: {night.get('chdb', {}).get('status', 'not built')}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--head-sha", default="", help="merged sync commit on main whose builds to publish")
    parser.add_argument("--work", required=True)
    parser.add_argument("--site", required=True)
    parser.add_argument("--keep", type=int, default=20, help="nights whose wheels to keep")
    parser.add_argument("--index", action="store_true",
                        help="only write the simple index and the release notes of the published night")
    args = parser.parse_args(argv)

    repo = os.environ["GITHUB_REPOSITORY"]
    site_url = os.environ.get("NIGHTLY_SITE_URL", "")
    work, site = Path(args.work), Path(args.site)
    work.mkdir(parents=True, exist_ok=True)
    site.mkdir(parents=True, exist_ok=True)
    if args.index:
        return write_index(repo, work, site, site_url, args.keep)

    head_sha = args.head_sha
    if not head_sha:
        newest = latest_run(repo, PLATFORMS["linux-x86_64"], None)
        if not newest:
            print("No build of main has run yet")
            set_output(ready="false")
            return 0
        head_sha = newest["head_sha"]
    if not is_sync_merge(repo, head_sha):
        print(f"{head_sha[:12]} merges no ClickHouse sync; nothing to publish")
        set_output(ready="false")
        return 0
    runs = {platform: latest_run(repo, workflow, head_sha) for platform, workflow in PLATFORMS.items()}
    pending = [p for p, r in runs.items() if r is None or r["status"] != "completed"]
    if pending:
        print(f"Waiting for {', '.join(pending)} to finish on {head_sha[:12]}")
        set_output(ready="false")
        return 0

    for platform, run in runs.items():
        gh("run", "download", str(run["id"]), "--repo", repo, "--name", f"junit-{platform}",
           "--dir", str(work / "junit" / platform), check=False)

    # A platform whose build or tests failed uploaded no wheel; with all four
    # failing on the first night there is no release yet.
    release_id = gh("api", f"repos/{repo}/releases/tags/{TAG}", "--jq", ".id", check=False).strip()
    committed_at = gh("api", f"repos/{repo}/commits/{head_sha}", "--jq", ".commit.committer.date").strip()
    # A rerun of an older night's build finishes after a newer night is out; the
    # release, its notes and the dashboard only move forward.
    tagged = gh("api", f"repos/{repo}/commits/{TAG}", "--jq", ".commit.committer.date", check=False).strip()
    if release_id and tagged and dev_number(tagged) > dev_number(committed_at):
        print(f"{head_sha[:12]} is older than the published nightly; nothing to publish")
        set_output(ready="false")
        return 0
    assets = release_assets(repo, release_id) if release_id else []
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

    # The index and the notes wait for the chdb nightly: publish.py --index.
    (work / "night.json").write_text(json.dumps(night, indent=2) + "\n")
    print(f"Night of {night['date']} ({head_sha[:12]}): {len(wheels)} of {len(PLATFORMS)} wheels on the "
          f"release; deleted {len(deleted)} old ones")
    set_output(ready="true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
