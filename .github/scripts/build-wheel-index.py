#!/usr/bin/env python3
"""Build a PEP 503 index over the wheels attached to this repository's releases.

    .github/scripts/build-wheel-index.py --out public

chdb-core's wheels are large -- four abi3 wheels come to roughly half a
gigabyte -- and PyPI's project quota only moves one way, so not every release
goes there. Every release does attach its wheels to GitHub, and this turns that
into something pip can resolve against:

    pip install "chdb[durable]" \
      --extra-index-url https://chdb.io/wheels/simple/ "chdb-core>=26.7.3"

Three properties are the point:

* it lists **every** release, not just the newest, so adding the index can
  never make a version harder to install than it was without it -- the index is
  a superset of PyPI, and pip takes the highest candidate across both. When a
  chdb-core does reach PyPI, users move to it with no command change;
* the platform is pip's to choose. A user copying a direct wheel URL has to
  know their own tag set; an index means `chdb-core>=26.7.3` resolves to the
  right one of macosx_11_0_arm64, macosx_10_15_x86_64, manylinux aarch64 or
  manylinux x86_64, including free-threaded variants when a release has them;
* the hash comes from GitHub's own asset digest, so the `#sha256=` fragment is
  authoritative without this script downloading half a gigabyte to compute it.

The output is static files. Point any host at the generated `simple/`.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

API = "https://api.github.com"

# Enough of PEP 427 to get the distribution name off a wheel filename:
# {name}-{version}(-{build})?-{python}-{abi}-{platform}.whl
WHEEL_RE = re.compile(r"\A(?P<name>[^-]+(?:_[^-]+)*)-(?P<version>[^-]+)-")


def normalize(name: str) -> str:
    """PEP 503 normalisation: the name pip will ask this index for."""
    return re.sub(r"[-_.]+", "-", name).lower()


def get(url: str, token: str | None) -> object:
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "chdb-core-wheel-index",
        **({"Authorization": f"Bearer {token}"} if token else {}),
    })
    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        sys.exit(f"::error::GET {url} failed: {exc.code} {exc.reason}")


def releases(repo: str, token: str | None):
    """Every release, including pre-releases and drafts' published siblings."""
    page = 1
    while True:
        batch = get(f"{API}/repos/{repo}/releases?per_page=100&page={page}", token)
        if not batch:
            return
        for release in batch:
            if not release.get("draft"):
                yield release
        page += 1


def collect(repo: str, token: str | None) -> dict[str, list[dict]]:
    """Wheels grouped by the project name pip will look them up under."""
    projects: dict[str, list[dict]] = defaultdict(list)
    for release in releases(repo, token):
        for asset in release.get("assets", []):
            name = asset["name"]
            if not name.endswith(".whl"):
                continue
            match = WHEEL_RE.match(name)
            if not match:
                print(f"skipping {name}: not a wheel filename", file=sys.stderr)
                continue
            digest = asset.get("digest") or ""
            if not digest.startswith("sha256:"):
                # Fatal rather than a warning. pip installs an unhashed link
                # without complaint, so a single missing digest would mean one
                # wheel in the index is silently unverified -- invisible in the
                # page, invisible at install time. Every asset GitHub currently
                # serves carries a digest, so this reads as "something changed"
                # rather than "an old release is different".
                sys.exit(f"::error::{release['tag_name']} asset {name} has no sha256 digest; "
                         "refusing to publish a link pip would install unverified")
            projects[normalize(match.group("name"))].append({
                "filename": name,
                "url": asset["browser_download_url"],
                "sha256": digest[len("sha256:"):],
                "release": release["tag_name"],
            })
    return projects


def page(title: str, body: str) -> str:
    return (
        "<!DOCTYPE html>\n<html>\n<head>\n"
        '  <meta name="pypi:repository-version" content="1.0">\n'
        f"  <title>{html.escape(title)}</title>\n"
        "</head>\n<body>\n" + body + "</body>\n</html>\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "chdb-io/chdb-core"))
    parser.add_argument("--out", default="public", type=Path,
                        help="directory to write simple/ into")
    # An allow-list rather than a filter, because releases carry wheels that
    # are not products. chdb-core-lite is the symbol-trimming size check, built
    # to catch a regression and attached to a few older releases by accident;
    # an index that advertised it would turn a CI artifact into something a
    # user can `pip install`. A new project has to be named here on purpose.
    parser.add_argument("--project", action="append", dest="projects",
                        metavar="NAME", help="publish this project (repeatable; default: chdb-core)")
    args = parser.parse_args()

    wanted = {normalize(name) for name in (args.projects or ["chdb-core"])}
    found = collect(args.repo, os.environ.get("GITHUB_TOKEN"))
    for name in sorted(set(found) - wanted):
        print(f"not published: {name} ({len(found[name])} wheels); pass --project {name} to include it")
    projects = {name: wheels for name, wheels in found.items() if name in wanted}

    missing = sorted(wanted - set(projects))
    if missing:
        sys.exit(f"::error::{args.repo} has no release wheel for {', '.join(missing)}; "
                 "refusing to write an index that silently omits a project")

    root = args.out / "simple"
    root.mkdir(parents=True, exist_ok=True)

    links = "".join(f'  <a href="{name}/">{html.escape(name)}</a><br>\n'
                    for name in sorted(projects))
    (root / "index.html").write_text(page("Simple index", links))

    for name, wheels in sorted(projects.items()):
        # Newest release first, so a human reading the page sees the same order
        # pip prefers. pip itself does not care about link order.
        wheels.sort(key=lambda w: (w["release"], w["filename"]), reverse=True)
        body = []
        for wheel in wheels:
            href = f"{wheel['url']}#sha256={wheel['sha256']}"
            body.append(f'  <a href="{html.escape(href)}">{html.escape(wheel["filename"])}</a><br>\n')
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "index.html").write_text(page(name, "".join(body)))
        print(f"{name}: {len(wheels)} wheels, all hashed")
        # Per release, because a release short of its siblings is the visible
        # symptom of a platform build that failed: that wheel does not exist,
        # so the index cannot carry it, and the release is published without it
        # rather than withheld from the platforms that did build.
        #
        # Counted and not judged. How many wheels a release is supposed to have
        # is not a number this can know -- four abi3 wheels usually, five while
        # the free-threading upload was enabled, fewer with DEBUG_MODE -- and a
        # guess at it would flag every normal release. The reading is left to
        # whoever is looking at a release they have reason to doubt.
        per_release: dict[str, int] = defaultdict(int)
        for wheel in wheels:
            per_release[wheel["release"]] += 1
        for release, count in sorted(per_release.items(), reverse=True):
            print(f"    {release}: {count}")


if __name__ == "__main__":
    main()
