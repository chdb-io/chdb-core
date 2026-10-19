#!/usr/bin/env python3
"""Build the chdb nightly: chdb-io/chdb's main, pinned to the night's chdb-core.

chdb, the DataStore API, is a pure-Python package on top of chdb-core. For a night
with a chdb-core wheel on the `nightly` release, this copies a checkout of chdb's
main (--src) and changes the copy, never the chdb repository:

- the version becomes <the minor after chdb's newest release on PyPI>.dev<the
  night's dev number>, newer than every release, so that
  `pip install --pre --extra-index-url <site>/simple chdb` picks it,
- the chdb-core requirement becomes the night's exact version, which that pip
  install then takes from the same index.

It builds the wheel from the copy and uploads it to the release, untested: chdb's
own CI tests its main. On a platform the night has no chdb-core wheel for, pip
backtracks to the newest chdb nightly whose chdb-core it can install.

The outcome goes into --work/night.json as "chdb" (status published, failed or
skipped) and the build output into --work/store/logs/<date>/chdb.txt.gz. It exits 0
whenever it could record the outcome: chdb never holds up chdb-core's night.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from pathlib import Path

TAG = "nightly"


def next_dev_version(released: str, dev: str) -> str:
    """4.4.0 and 2610180851 -> 4.5.0.dev2610180851."""
    match = re.match(r"(\d+)\.(\d+)", released)
    if not match:
        raise ValueError(f"cannot read chdb's PyPI version {released!r}")
    return f"{match.group(1)}.{int(match.group(2)) + 1}.0.dev{dev}"


def pin(pyproject: str, version: str, core: str) -> str:
    """chdb's pyproject.toml with the version and the chdb-core requirement replaced.

    Only [project] is touched: an optional extra that names chdb-core keeps its floor,
    which the pinned night satisfies.
    """
    start = pyproject.index("[project]\n")
    end = pyproject.find("\n[", start + 1)
    end = len(pyproject) if end < 0 else end
    head, table, tail = pyproject[:start], pyproject[start:end], pyproject[end:]
    table, versions = re.subn(r'(?m)^version\s*=\s*"[^"]*"', f'version = "{version}"', table, count=1)
    deps = re.search(r"(?ms)^dependencies\s*=\s*\[.*?\]", table)
    if not versions or not deps:
        raise ValueError("pyproject.toml has no [project] version or dependencies")
    pinned, n = re.subn(r'"chdb-core[^"]*"', f'"chdb-core=={core}"', deps.group(0))
    if not n:
        raise ValueError("chdb does not depend on chdb-core")
    text = head + table[:deps.start()] + pinned + table[deps.end():] + tail
    project = tomllib.loads(text)["project"]
    if project["version"] != version or f"chdb-core=={core}" not in project["dependencies"]:
        raise ValueError("pyproject.toml did not take the nightly version and pin")
    return text


def set_module_version(source: str, version: str) -> str:
    """datastore/__init__.py with its __version__, which bump-my-version also sets."""
    text, n = re.subn(r'(?m)^__version__\s*=\s*"[^"]*"', f'__version__ = "{version}"', source, count=1)
    if not n:
        raise ValueError("datastore/__init__.py has no __version__")
    return text


def pypi_version(project: str) -> str:
    with urllib.request.urlopen(f"https://pypi.org/pypi/{project}/json", timeout=60) as response:
        return json.load(response)["info"]["version"]


class Log:
    def __init__(self):
        self.parts: list[str] = []

    def run(self, cmd: list[str], **kwargs) -> bool:
        self.parts.append(f"$ {' '.join(cmd)}\n")
        print(f"$ {' '.join(cmd)}", flush=True)
        try:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, **kwargs)
        except subprocess.TimeoutExpired as e:
            self.parts.append(f"{e.output or ''}\ntimed out after {e.timeout} s\n")
            return False
        self.parts.append(proc.stdout)
        print(proc.stdout[-20000:], flush=True)
        return proc.returncode == 0

    def text(self) -> str:
        return "".join(self.parts)


def build(src: Path, night: dict, repo: str, log: Log, tmp: Path) -> dict:
    core = night["version"]
    version = next_dev_version(pypi_version("chdb"), core.rsplit(".dev", 1)[1])
    copy = tmp / "chdb"
    shutil.copytree(src, copy, ignore=shutil.ignore_patterns(".git"))
    pyproject = copy / "pyproject.toml"
    pyproject.write_text(pin(pyproject.read_text(), version, core))
    module = copy / "datastore" / "__init__.py"
    module.write_text(set_module_version(module.read_text(), version))
    log.parts.append(f"chdb {version}, requiring chdb-core=={core}\n")
    result = {"status": "failed", "version": version}

    dist = tmp / "dist"
    if not (log.run([sys.executable, "-m", "pip", "install", "--quiet", "build"])
            and log.run([sys.executable, "-m", "build", "--wheel", "--outdir", str(dist), str(copy)])):
        return {**result, "reason": "the wheel did not build"}
    wheel = next(dist.glob("chdb-*.whl"))
    if not log.run(["gh", "release", "upload", TAG, str(wheel), "--repo", repo, "--clobber"]):
        return {**result, "reason": "the wheel did not upload"}
    return {"status": "published", "version": version, "wheel": wheel.name, "size": wheel.stat().st_size}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--work", required=True)
    parser.add_argument("--src", required=True, help="a checkout of chdb-io/chdb's main")
    args = parser.parse_args(argv)

    repo = os.environ["GITHUB_REPOSITORY"]
    work, src = Path(args.work), Path(args.src)
    night = json.loads((work / "night.json").read_text())
    commit = subprocess.run(["git", "-C", str(src), "rev-parse", "HEAD"], stdout=subprocess.PIPE,
                            text=True, check=True).stdout.strip()
    log = Log()
    if not night["wheels"]:
        result = {"status": "skipped", "reason": "the night has no chdb-core wheel"}
    else:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                result = build(src, night, repo, log, Path(tmp))
            except Exception as e:  # recorded, and the night goes on without chdb
                log.parts.append(f"{type(e).__name__}: {e}\n")
                result = {"status": "failed", "reason": str(e)}
    result["commit"] = commit
    night["chdb"] = result
    (work / "night.json").write_text(json.dumps(night, indent=2) + "\n")
    if log.parts:
        path = work / "store" / "logs" / night["date"] / "chdb.txt.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(log.text().encode()))
    print(f"chdb nightly: {result['status']}" + (f" ({result['reason']})" if result.get("reason") else "")
          + (f", {result['version']}" if result.get("version") else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
