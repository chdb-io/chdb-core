#!/usr/bin/env python3
"""Publish a nightly build: wheels to the rolling `nightly` release, plus a pip index.

The four wheel workflows build main on a schedule. This runs after each of them
finishes, waits until all four runs for the same main commit are done, and then:

- uploads their chdb_core wheels to the `nightly` pre-release,
- deletes wheels older than --keep-days,
- writes a PEP 503 index (simple/) into --site,
- writes --work/night.json for report.py, and sets the `ready` output.

The release is created once. Afterwards only its assets, its notes and the
`nightly` tag move, so no `release: published` event starts the release builds.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import os
import subprocess
import sys
from pathlib import Path

TAG = "nightly"
MANIFEST = "manifest.json"
# Platform -> (workflow file, wheel artifact). Order is the dashboard's order.
PLATFORMS = {
    "linux-x86_64": ("build_linux_x86_wheels.yml", "chdb-wheel-linux-x86_64"),
    "linux-aarch64": ("build_linux_arm64_wheels-gh.yml", "chdb-wheel-linux-aarch64"),
    "macos-arm64": ("build_macos_arm64_wheels.yml", "chdb-wheel-macos-arm64"),
    "macos-x86_64": ("build_macos_x86_wheels.yml", "chdb-wheel-macos-x86_64"),
}
PROJECT = "chdb-core"


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


def latest_run(repo: str, workflow: str, head_sha: str | None) -> dict | None:
    query = f"repos/{repo}/actions/workflows/{workflow}/runs?event=schedule&per_page=10"
    if head_sha:
        query += f"&head_sha={head_sha}"
    runs = json.loads(gh("api", query, "--jq", ".workflow_runs") or "[]")
    return runs[0] if runs else None


def wheel_version(name: str) -> str:
    return name.split("-")[1]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def minutes_between(start: str, end: str) -> float:
    parse = lambda s: dt.datetime.fromisoformat(s.replace("Z", "+00:00"))  # noqa: E731
    return round((parse(end) - parse(start)).total_seconds() / 60, 1)


def prune(manifest: dict, keep_days: int, today: dt.date) -> tuple[dict, list[str]]:
    """Drop nights older than keep_days; return the manifest and wheel names to delete."""
    cutoff = (today - dt.timedelta(days=keep_days)).isoformat()
    kept, dropped = [], []
    for night in manifest.get("nights", []):
        if night["date"] >= cutoff:
            kept.append(night)
        else:
            dropped += [w["name"] for w in night.get("wheels", [])]
    kept.sort(key=lambda n: (n["date"], n["head_sha"]), reverse=True)
    still_used = {w["name"] for n in kept for w in n.get("wheels", [])}
    return {"nights": kept}, sorted(set(dropped) - still_used)


def pep503_pages(repo: str, manifest: dict) -> dict[str, str]:
    """Return {relative path: html} for a PEP 503 simple index of the kept wheels."""
    links = []
    for night in manifest["nights"]:
        for wheel in night.get("wheels", []):
            url = f"https://github.com/{repo}/releases/download/{TAG}/{wheel['name']}#sha256={wheel['sha256']}"
            links.append(
                f'    <a href="{html.escape(url)}" data-requires-python="&gt;=3.9">{html.escape(wheel["name"])}</a><br>'
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


def release_notes(repo: str, manifest: dict, site: str, keep_days: int) -> str:
    newest = manifest["nights"][0] if manifest["nights"] else None
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
    if newest:
        lines.append(f"Newest: `{newest['version']}`, built from {newest['head_sha'][:12]} on {newest['date']}.")
    lines.append(f"Wheels are deleted after {keep_days} days.")
    return "\n".join(lines) + "\n"


def ensure_release(repo: str, head_sha: str, work: Path) -> bool:
    """Create the release on first use; return True if it already existed."""
    if gh("release", "view", TAG, "--repo", repo, "--json", "tagName", check=False):
        return True
    notes = work / "notes.md"
    notes.write_text("Rolling nightly builds of chdb-core from `main`.\n")
    gh("release", "create", TAG, "--repo", repo, "--prerelease", "--latest=false",
       "--title", "Nightly builds", "--notes-file", str(notes), "--target", head_sha)
    return False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--head-sha", default="", help="main commit whose scheduled builds to publish")
    parser.add_argument("--work", required=True)
    parser.add_argument("--site", required=True)
    parser.add_argument("--keep-days", type=int, default=30)
    args = parser.parse_args(argv)

    repo = os.environ["GITHUB_REPOSITORY"]
    site_url = os.environ.get("NIGHTLY_SITE_URL", "")
    work, site = Path(args.work), Path(args.site)
    work.mkdir(parents=True, exist_ok=True)
    site.mkdir(parents=True, exist_ok=True)

    head_sha = args.head_sha
    if not head_sha:
        newest = latest_run(repo, PLATFORMS["linux-x86_64"][0], None)
        if not newest:
            print("No scheduled build has run yet")
            set_output(ready="false")
            return 0
        head_sha = newest["head_sha"]
    runs = {platform: latest_run(repo, workflow, head_sha) for platform, (workflow, _) in PLATFORMS.items()}
    pending = [p for p, r in runs.items() if r is None or r["status"] != "completed"]
    if pending:
        print(f"Waiting for {', '.join(pending)} to finish on {head_sha[:12]}")
        set_output(ready="false")
        return 0

    wheels = []
    for platform, (_, artifact) in PLATFORMS.items():
        run = runs[platform]
        out = work / "wheels" / platform
        gh("run", "download", str(run["id"]), "--repo", repo, "--name", artifact, "--dir", str(out), check=False)
        gh("run", "download", str(run["id"]), "--repo", repo, "--name", f"junit-{platform}",
           "--dir", str(work / "junit" / platform), check=False)
        for file in sorted(out.glob("chdb_core-*.whl")):
            wheels.append({"platform": platform, "name": file.name, "size": file.stat().st_size,
                           "sha256": sha256(file), "path": str(file)})

    date = min(r["created_at"] for r in runs.values())[:10]
    night = {
        "date": date,
        "head_sha": head_sha,
        "version": wheel_version(wheels[0]["name"]) if wheels else "",
        "wheels": [{k: w[k] for k in ("platform", "name", "size", "sha256")} for w in wheels],
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

    existed = ensure_release(repo, head_sha, work)
    if existed:
        gh("api", "-X", "PATCH", f"repos/{repo}/git/refs/tags/{TAG}", "-f", f"sha={head_sha}", "-F", "force=true")
    if wheels:
        gh("release", "upload", TAG, "--repo", repo, "--clobber", *[w["path"] for w in wheels])

    gh("release", "download", TAG, "--repo", repo, "--pattern", MANIFEST, "--dir", str(work), "--clobber", check=False)
    manifest_file = work / MANIFEST
    manifest = json.loads(manifest_file.read_text()) if manifest_file.exists() else {"nights": []}
    manifest["nights"] = [n for n in manifest.get("nights", []) if n["head_sha"] != head_sha]
    if wheels:
        manifest["nights"].append({k: night[k] for k in ("date", "head_sha", "version", "wheels")})
    manifest, stale = prune(manifest, args.keep_days, dt.datetime.now(dt.timezone.utc).date())
    assets = json.loads(gh("release", "view", TAG, "--repo", repo, "--json", "assets", "--jq", "[.assets[].name]") or "[]")
    referenced = {w["name"] for n in manifest["nights"] for w in n["wheels"]}
    stale = sorted(set(stale) | {a for a in assets if a.endswith(".whl") and a not in referenced})
    for name in stale:
        gh("release", "delete-asset", TAG, name, "--repo", repo, "--yes", check=False)
    manifest_file.write_text(json.dumps(manifest, indent=2) + "\n")
    gh("release", "upload", TAG, "--repo", repo, "--clobber", str(manifest_file))
    notes = work / "notes.md"
    notes.write_text(release_notes(repo, manifest, site_url, args.keep_days))
    gh("release", "edit", TAG, "--repo", repo, "--notes-file", str(notes))

    for rel, text in pep503_pages(repo, manifest).items():
        page = site / rel
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text(text)
    (work / "night.json").write_text(json.dumps(night, indent=2) + "\n")
    print(f"Published {len(wheels)} wheels for {head_sha[:12]}; deleted {len(stale)} old assets")
    set_output(ready="true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
