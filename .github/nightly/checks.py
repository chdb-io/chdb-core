#!/usr/bin/env python3
"""Checks for a daily ClickHouse sync that Git does not do.

Git reports text conflicts. It does not notice a clean merge that dropped a chDB
change, an upstream change that never arrived, a function registration the static
library no longer references, an initialisation LocalServer gained and
EmbeddedServer did not, or a submodule bump whose cmake list names files the new
pin does not have. Each of those has broken a past upgrade.

  python3 .github/nightly/checks.py all --work DIR      every check, report in .sync/
  python3 .github/nightly/checks.py markers             conflict markers in staged files

`all` writes .sync/checks.json and .sync/checks.md and sets the `result` output
to pass or fail; the report goes into the pull request's description.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / ".sync"
GENERATOR = ROOT / "chdb/build/generate_force_references.py"
FORCE_REFS = ROOT / "programs/local/ForceFunctionReferences.cpp"
LOCAL_SERVER = "programs/local/LocalServer.cpp"
EMBEDDED_SERVER = ROOT / "programs/local/EmbeddedServer.cpp"
MIRROR_ALLOWLIST = Path(__file__).with_name("embedded-mirror-allowlist.txt")
CHDB_OWNED = {".gitignore", "CONTRIBUTING.md", "README.md", "pyproject.toml"}

MARKER = re.compile(r"^(<{7}|>{7})(?: |$)", re.M)
REGISTER = re.compile(r"REGISTER_FUNCTION\s*\(\s*(\w+)\s*\)")
REFERENCE = re.compile(r"&(?:::)?registerFunction(\w+)\s*,")
# Calls in LocalServer that EmbeddedServer has had to mirror in past upgrades.
MIRRORED_CALLS = [
    re.compile(r"\b(register\w+)\s*\("),
    re.compile(r"\b(get\w+ThreadPool)\s*\(\s*\)\s*\.\s*initialize"),
    re.compile(r"global_context->(set\w+)\s*\("),
    re.compile(r"\b(\w*[Ss]hutdown\w*)\s*\("),
]


def run(cmd, *, cwd=ROOT, check=True, binary=False):
    proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=not binary)
    if check and proc.returncode != 0:
        err = proc.stderr if not binary else proc.stderr.decode(errors="replace")
        raise RuntimeError(f"{' '.join(cmd[:4])} ... failed: {err.strip()}")
    return proc.stdout


def git(*args, check=True) -> str:
    return run(["git", *args], check=check)


def ugit(work: Path, *args, check=True) -> str:
    return run(["git", f"--git-dir={work / 'clickhouse.git'}", *args], check=check)


def upstream_show(work: Path, rev: str, path: str) -> str | None:
    proc = subprocess.run(
        ["git", f"--git-dir={work / 'clickhouse.git'}", "show", f"{rev}:{path}"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    return proc.stdout.decode(errors="replace") if proc.returncode == 0 else None


def head_show(path: str) -> str | None:
    proc = subprocess.run(["git", "show", f"HEAD:{path}"], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return proc.stdout.decode(errors="replace") if proc.returncode == 0 else None


def worktree_read(path: str) -> str | None:
    file = ROOT / path
    return file.read_text(errors="replace") if file.is_file() else None


def load_summary() -> dict:
    return json.loads((STATE / "summary.json").read_text())


# ------------------------------------------------------------------------- markers


def check_markers(_work=None, _summary=None):
    errors = []
    staged = [p for p in git("diff", "--cached", "--name-only", "-z", "HEAD").split("\0") if p]
    for path in staged:
        text = worktree_read(path)
        if text is not None and MARKER.search(text):
            errors.append(f"`{path}` still has conflict markers")
    return errors, []


# --------------------------------------------------------------------------- delta


def significant_lines(text: str) -> set[str]:
    """Lines worth comparing: braces, blank lines and lone keywords are noise."""
    lines = set()
    for line in text.splitlines():
        line = line.strip()
        if len(line) >= 4 and re.search(r"[A-Za-z0-9_]{2}", line):
            lines.add(line)
    return lines


def compare_file(up_old: str, up_new: str, chdb_old: str, chdb_new: str) -> tuple[list[str], list[str]]:
    """Return (chDB lines that vanished, upstream lines that never arrived)."""
    o, n, a, b = map(significant_lines, (up_old, up_new, chdb_old, chdb_new))
    chdb_added = a - o
    chdb_removed = o - a
    lost = sorted(line for line in chdb_added if line not in b and line not in n)
    missing = sorted(line for line in n - o if line not in b and line not in chdb_removed)
    return lost, missing


def check_delta(work: Path, summary: dict):
    errors = []
    for path in summary["applied_files"]:
        if path in CHDB_OWNED:
            continue
        up_old = upstream_show(work, summary["base"], path)
        up_new = upstream_show(work, summary["target"], path)
        chdb_old = head_show(path)
        chdb_new = worktree_read(path)
        if None in (up_old, up_new, chdb_old, chdb_new):
            continue
        if chdb_old == up_old:
            if chdb_new != up_new:
                errors.append(f"`{path}` had no chDB change but differs from upstream after the sync")
            continue
        lost, missing = compare_file(up_old, up_new, chdb_old, chdb_new)
        if lost:
            errors.append(f"`{path}`: {len(lost)} chDB line(s) gone, e.g. `{lost[0][:120]}`")
        if missing:
            errors.append(f"`{path}`: {len(missing)} upstream line(s) missing, e.g. `{missing[0][:120]}`")
    return errors, []


# ---------------------------------------------------------------------- force-refs


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_force_references", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def registrations(functions_dir: Path) -> set[str]:
    names = set()
    for file in functions_dir.rglob("*.cpp"):
        names.update(REGISTER.findall(file.read_text(errors="replace")))
    return names


def check_force_refs(_work=None, _summary=None):
    generator = load_generator()
    functions = ROOT / "src/Functions"
    wanted_now = set(generator.extract_register_functions(str(functions)))
    registered_now = registrations(functions)
    referenced = set(REFERENCE.findall(FORCE_REFS.read_text()))

    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "head.tar"
        archive.write_bytes(run(["git", "archive", "HEAD", "src/Functions"], binary=True))
        with tarfile.open(archive) as tar:
            tar.extractall(tmp)
        wanted_before = set(generator.extract_register_functions(str(Path(tmp) / "src/Functions")))
    referenced_before = set(REFERENCE.findall(head_show(str(FORCE_REFS.relative_to(ROOT))) or ""))

    errors = [
        f"ForceFunctionReferences.cpp references `registerFunction{name}`, which no longer exists: "
        "the static library will not link"
        for name in sorted(referenced - registered_now)
    ]
    newly_missing = sorted((wanted_now - referenced) - (wanted_before - referenced_before))
    warnings = []
    if newly_missing:
        warnings.append(
            "new REGISTER_FUNCTION not referenced from ForceFunctionReferences.cpp, so the static "
            "library drops them: " + ", ".join(f"`{n}`" for n in newly_missing)
            + " (regenerate with chdb/build/generate_force_references.py)"
        )
    return errors, warnings


# ------------------------------------------------------------------------ embedded


def mirror_allowlist() -> set[str]:
    if not MIRROR_ALLOWLIST.exists():
        return set()
    names = set()
    for line in MIRROR_ALLOWLIST.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.add(line.split()[0])
    return names


def new_local_server_calls(diff: str) -> set[str]:
    calls = set()
    for line in diff.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            for pattern in MIRRORED_CALLS:
                calls.update(pattern.findall(line[1:]))
    return calls


def check_embedded(work: Path, summary: dict):
    diff = ugit(work, "diff", "-U0", summary["base"], summary["target"], "--", LOCAL_SERVER, check=False)
    calls = new_local_server_calls(diff)
    embedded = EMBEDDED_SERVER.read_text(errors="replace")
    allowed = mirror_allowlist()
    gaps = sorted(c for c in calls if c not in embedded and c not in allowed)
    warnings = [
        f"upstream LocalServer.cpp gained `{call}`; EmbeddedServer.cpp has no counterpart "
        f"(mirror it, or list it in {MIRROR_ALLOWLIST.name} with the reason)"
        for call in gaps
    ]
    return [], warnings


# ---------------------------------------------------------------------- submodules


def gitmodules_urls() -> dict[str, str] | None:
    """Submodule path -> URL from .gitmodules, or None if it cannot be parsed."""
    proc = subprocess.run(
        ["git", "config", "-f", ".gitmodules", "--get-regexp", r"^submodule\..*\.(path|url)$"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if proc.returncode != 0:
        return None
    paths, urls = {}, {}
    for line in proc.stdout.splitlines():
        key, value = line.split(" ", 1)
        name, _, field = key[len("submodule."):].rpartition(".")
        (paths if field == "path" else urls)[name] = value.strip()
    return {path: urls.get(name, "") for name, path in paths.items()}


def files_at(url: str, sha: str) -> set[str] | None:
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp) / "sub.git"
        run(["git", "init", "--bare", "-q", str(repo)])
        fetch = subprocess.run(
            ["git", f"--git-dir={repo}", "fetch", "--quiet", "--depth=1", "--filter=blob:none", url, sha],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if fetch.returncode != 0:
            return None
        return set(run(["git", f"--git-dir={repo}", "ls-tree", "-r", "--name-only", sha]).splitlines())


def cmake_paths(text: str, name: str) -> list[str]:
    """Source paths a contrib/<name>-cmake file names relative to the submodule."""
    var_re = re.compile(
        r'set\s*\(\s*(\w+)\s+"?\$\{(?:ClickHouse_SOURCE_DIR|CMAKE_SOURCE_DIR|PROJECT_SOURCE_DIR)\}/contrib/'
        + re.escape(name) + r'(/[^"\s)]*)?"?\s*\)',
        re.I,
    )
    paths = []
    for match in var_re.finditer(text):
        var, subdir = match.group(1), (match.group(2) or "").strip("/")
        for rel in re.findall(r"\$\{" + re.escape(var) + r"\}/([^\s\"')$;]+)", text):
            if any(ch in rel for ch in "*?$<>"):
                continue
            path = os.path.normpath(f"{subdir}/{rel}" if subdir else rel)
            if not path.startswith(".."):
                paths.append(path)
    return sorted(set(paths))


def check_submodules(_work: Path, summary: dict):
    errors = []
    moved = [s for s in summary["submodules"] if s.get("action") in ("bumped", "added")]
    urls = gitmodules_urls() if moved else {}
    if urls is None:
        return [".gitmodules cannot be parsed; resolve it before this check"], []
    for sub in moved:
        path, sha = sub["path"], sub["upstream_new"]
        url = urls.get(path)
        if not url:
            errors.append(f"`{path}` has no URL in .gitmodules")
            continue
        files = files_at(url, sha)
        if files is None:
            errors.append(f"`{path}`: {sha[:12]} cannot be fetched from {url}")
            continue
        cmake = ROOT / "contrib" / f"{Path(path).name}-cmake" / "CMakeLists.txt"
        if not cmake.is_file():
            continue
        # Only what the bump broke: a path the old pin had and the new one lacks.
        before = files_at(url, sub["upstream_old"]) if sub.get("action") == "bumped" else None
        for rel in cmake_paths(cmake.read_text(errors="replace"), Path(path).name):
            if not tree_has(files, rel) and (before is None or tree_has(before, rel)):
                errors.append(f"`{cmake.relative_to(ROOT)}` names `{rel}`, which {path} no longer has at {sha[:12]}")
    return errors, []


def tree_has(files: set[str], rel: str) -> bool:
    rel = rel.rstrip("/")
    return rel in files or any(f.startswith(rel + "/") for f in files)


# ----------------------------------------------------------------------------- all


CHECKS = {
    "markers": check_markers,
    "delta": check_delta,
    "force-refs": check_force_refs,
    "embedded": check_embedded,
    "submodules": check_submodules,
}


def report(results: dict) -> str:
    lines = ["## Sync checks", ""]
    for name, result in results.items():
        if result.get("crashed"):
            status = "could not run"
        elif result["errors"]:
            status = "failed"
        elif result["warnings"]:
            status = "warnings"
        else:
            status = "passed"
        lines.append(f"- **{name}**: {status}")
        for item in result.get("errors", []) + result.get("warnings", []) + result.get("crashed", []):
            lines.append(f"  - {item}")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("check", choices=["all", *CHECKS])
    parser.add_argument("--work", default="", help="work directory used by daily_sync.py fetch")
    args = parser.parse_args(argv)
    work = Path(args.work) if args.work else None
    summary = load_summary() if (STATE / "summary.json").exists() else None
    names = list(CHECKS) if args.check == "all" else [args.check]

    results = {}
    for name in names:
        needs_upstream = name in ("delta", "embedded", "submodules")
        if needs_upstream and (work is None or summary is None):
            results[name] = {"errors": [], "warnings": [], "crashed": ["needs --work and .sync/summary.json"]}
            continue
        try:
            errors, warnings = CHECKS[name](work, summary)
            results[name] = {"errors": errors, "warnings": warnings}
        except Exception as exc:  # a broken check must not pass silently
            results[name] = {"errors": [], "warnings": [], "crashed": [f"{type(exc).__name__}: {exc}"]}

    failed = any(r["errors"] or r.get("crashed") for r in results.values())
    text = report(results)
    print(text)
    if args.check == "all":
        STATE.mkdir(exist_ok=True)
        (STATE / "checks.json").write_text(json.dumps(results, indent=2) + "\n")
        (STATE / "checks.md").write_text(text)
        out = os.environ.get("GITHUB_OUTPUT")
        if out:
            with open(out, "a") as fh:
                fh.write(f"result={'fail' if failed else 'pass'}\n")
        return 0
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
