#!/usr/bin/env python3
"""Give non-release wheels a PEP 440 version newer than their base tag.

--nightly is for the builds the nightly release publishes, main's builds of
merged ClickHouse syncs: no local version (a `+` in a wheel name does not survive
as a GitHub release asset name), and once main has moved on to a newer ClickHouse
line than its newest tag, the version names that line, so pip sorts the nightly
above every release of an older one. Its dev number is HEAD's committer time as
yymmddhhmm in UTC: it shows the day, the same commit always gets the same
version, and a later commit a larger one.
"""

from __future__ import annotations

import argparse
import datetime
import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TAG_RE = re.compile(
    r"^v?(?P<release>\d+\.\d+\.\d+)"
    r"(?:(?:[-.]?)(?P<pre>a|b|rc)[.-]?(?P<pre_number>\d+))?$"
)


def run_git(*args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=REPO_ROOT, text=True
    ).strip()


def normalize_tag(tag: str) -> str:
    match = TAG_RE.fullmatch(tag)
    if not match:
        raise ValueError(f"unsupported release tag: {tag}")

    version = match.group("release")
    if match.group("pre"):
        version += match.group("pre") + match.group("pre_number")
    return version


def main_version(tag: str, distance: int, commit: str) -> str:
    if distance < 0:
        raise ValueError("commit distance cannot be negative")
    if not re.fullmatch(r"[0-9a-fA-F]+", commit):
        raise ValueError(f"invalid Git commit: {commit}")
    return f"{normalize_tag(tag)}.post1.dev{distance}+g{commit.lower()}"


def engine_line(header: str) -> tuple[int, int] | None:
    match = re.search(r'#define CHDB_VERSION "(\d+)\.(\d+)\.', header)
    return (int(match.group(1)), int(match.group(2))) if match else None


def dev_number(commit_time: int) -> int:
    moment = datetime.datetime.fromtimestamp(commit_time, tz=datetime.timezone.utc)
    return int(moment.strftime("%y%m%d%H%M"))


def nightly_version(tag: str, commit_time: int, line: tuple[int, int] | None = None) -> str:
    base = normalize_tag(tag)
    major, minor = (int(part) for part in base.split(".")[:2])
    dev = dev_number(commit_time)
    if line and line > (major, minor):
        return f"{line[0]}.{line[1]}.0.dev{dev}"
    return f"{base}.post1.dev{dev}"


def replace_once(path: Path, pattern: str, replacement: str) -> None:
    content = path.read_text()
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"expected one version declaration in {path}, found {count}")
    path.write_text(updated)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nightly", action="store_true", help="version for the nightly release")
    args = parser.parse_args()

    tag = run_git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*")
    if args.nightly:
        header = (REPO_ROOT / "programs" / "local" / "chdb.h").read_text()
        commit_time = int(run_git("show", "-s", "--format=%ct", "HEAD"))
        version = nightly_version(tag, commit_time, engine_line(header))
    else:
        distance = int(run_git("rev-list", "--count", f"{tag}..HEAD"))
        commit = run_git("rev-parse", "--short=12", "HEAD")
        version = main_version(tag, distance, commit)

    replace_once(
        REPO_ROOT / "pyproject.toml",
        r'^version\s*=\s*"[^"]+"$',
        f'version = "{version}"',
    )
    replace_once(
        REPO_ROOT / "chdb" / "__init__.py",
        r'^__version__\s*=\s*"[^"]+"$',
        f'__version__ = "{version}"',
    )
    print(version)


if __name__ == "__main__":
    main()
