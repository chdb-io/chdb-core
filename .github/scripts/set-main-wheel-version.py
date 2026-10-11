#!/usr/bin/env python3
"""Give non-release wheels a PEP 440 version newer than their base tag."""

from __future__ import annotations

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


def replace_once(path: Path, pattern: str, replacement: str) -> None:
    content = path.read_text()
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"expected one version declaration in {path}, found {count}")
    path.write_text(updated)


def main() -> None:
    tag = run_git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*")
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
