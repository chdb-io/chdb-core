#!/usr/bin/env python3
"""Offline tests for the daily-sync and nightly tooling. Run by Pull-CI."""

from __future__ import annotations

import collections
import importlib.util
import os
import subprocess
import tempfile
import unittest
import unittest.mock
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


sync = load("daily_sync", HERE / "daily_sync.py")
checks = load("checks", HERE / "checks.py")
publish = load("publish", HERE / "publish.py")
report = load("report", HERE / "report.py")
preflight = load("sync_preflight", sync.PREFLIGHT)

ZERO = "0" * 40
A = "a" * 40
B = "b" * 40


class ParseAndClassify(unittest.TestCase):
    def raw(self, *records):
        return "".join(f":{om} {nm} {os_} {ns} {st}\0{path}\0" for om, nm, os_, ns, st, path in records)

    def test_parse_raw(self):
        raw = self.raw(("100644", "100644", A, B, "M", "src/a.cpp"), ("000000", "160000", ZERO, B, "A", "contrib/x"))
        changes = sync.parse_raw(raw)
        self.assertEqual([c.path for c in changes], ["src/a.cpp", "contrib/x"])
        self.assertEqual(changes[1].new_mode, "160000")

    def test_classify(self):
        chdb = {"src/kept.cpp", "ci/stale.py", "utils/list-licenses/list-licenses.sh", "README.md", "src/gone.cpp"}
        raw = self.raw(
            ("100644", "100644", A, B, "M", "src/kept.cpp"),
            ("100644", "100644", A, B, "M", "src/dropped.cpp"),
            ("100644", "100644", A, B, "M", "ci/stale.py"),
            ("100644", "100644", A, B, "M", "utils/list-licenses/list-licenses.sh"),
            ("100644", "100644", A, B, "M", "README.md"),
            ("000000", "100644", ZERO, B, "A", "src/new.cpp"),
            ("000000", "100644", ZERO, B, "A", "tests/queries/0_x.sql"),
            ("000000", "100644", ZERO, B, "A", "utils/new-tool/main.cpp"),
            ("100644", "000000", A, ZERO, "D", "src/gone.cpp"),
            ("160000", "160000", A, B, "M", "contrib/lib"),
        )
        files, gitlinks, skipped = sync.classify(sync.parse_raw(raw), chdb, preflight)
        self.assertEqual(files, ["src/kept.cpp", "utils/list-licenses/list-licenses.sh", "src/new.cpp", "src/gone.cpp"])
        self.assertEqual([g.path for g in gitlinks], ["contrib/lib"])
        self.assertEqual(skipped["chDB keeps its own version"], 1)
        self.assertEqual(skipped["changed in a tree chDB drops"], 1)

    def test_parse_branch_points(self):
        out = (f"{A}\trefs/tags/v26.10.1.1-new\n{B}\trefs/tags/v26.10.1.1-new^{{}}\n"
               f"{ZERO}\trefs/tags/v26.11.1.1-new\n")
        self.assertEqual(sync.parse_branch_points(out), {"v26.10.1.1-new": B, "v26.11.1.1-new": ZERO})

    def test_conflict_marker(self):
        self.assertTrue(sync.CONFLICT_MARKER.search("a\n<<<<<<< ours\nb\n"))
        self.assertTrue(sync.CONFLICT_MARKER.search("a\n|||||||\n"))
        self.assertFalse(sync.CONFLICT_MARKER.search("a <<<<<<< b\n"))


class BranchPoints(unittest.TestCase):
    def test_sync_stops_at_the_next_branch_point(self):
        with tempfile.TemporaryDirectory() as tmp:
            up, work = Path(tmp, "up"), Path(tmp, "work")
            up.mkdir()
            work.mkdir()

            def git(*args, cwd=up):
                ident = ["-c", "user.name=t", "-c", "user.email=t@t"]
                done = subprocess.run(["git", *ident, *args], cwd=cwd, check=True, capture_output=True, text=True)
                return done.stdout.strip()

            def commit(message):
                git("commit", "-q", "--allow-empty", "-m", message)
                return git("rev-parse", "HEAD")

            git("init", "-q", "-b", "master")
            git("config", "uploadpack.allowAnySHA1InWant", "true")
            commit("26.9 branch point")
            git("tag", "-a", "-m", "branch point", "v26.10.1.1-new")
            git("checkout", "-q", "-b", "26.9")
            release = commit("v26.9.2.8-stable")
            git("checkout", "-q", "master")
            before = commit("26.10 work")
            point = commit("26.10 branch point")
            git("tag", "-a", "-m", "branch point", "v26.11.1.1-new")
            after = commit("26.11 work")

            bare = sync.upstream_dir(work)
            git("init", "-q", "--bare", str(bare), cwd=work)
            git(f"--git-dir={bare}", "remote", "add", "origin", up.as_uri(), cwd=work)
            git(f"--git-dir={bare}", "fetch", "-q", "--no-tags", "--shallow-exclude=v26.10.1.1-new", "origin",
                f"+refs/heads/master:{sync.UPSTREAM_MASTER}", release, cwd=work)
            # fetch's branch_point_before leaves the tags no older than the release here.
            for tag in ("v26.10.1.1-new", "v26.11.1.1-new"):
                git(f"--git-dir={bare}", "fetch", "-q", "--no-tags", "--depth=1", "origin",
                    f"+refs/tags/{tag}:refs/upstream-tags/{tag}", cwd=work)

            stop = ("v26.11.1.1-new", point)
            self.assertEqual(sync.next_branch_point(work, release, after), stop)
            self.assertEqual(sync.next_branch_point(work, before, after), stop)
            self.assertIsNone(sync.next_branch_point(work, point, after))
            self.assertIsNone(sync.next_branch_point(work, release, before))


def git_in(cwd: Path, *args, stdin=None) -> str:
    ident = ["-c", "user.name=t", "-c", "user.email=t@t", "-c", "init.defaultBranch=master"]
    return subprocess.run(["git", *ident, *args], cwd=cwd, input=stdin, check=True,
                          capture_output=True, text=True).stdout.strip()


class ApplyAndStage(unittest.TestCase):
    """The scripts on a copy of their own, against a blobless clone, as the workflow runs them."""

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            up, chdb, work = tmp / "up", tmp / "chdb", tmp / "work"

            def write(root, files):
                for path, text in files.items():
                    (root / path).parent.mkdir(parents=True, exist_ok=True)
                    (root / path).write_text(text)

            versions = "cmake/autogenerated_versions.txt"
            up.mkdir()
            git_in(up, "init", "-q")
            git_in(up, "config", "uploadpack.allowFilter", "true")
            git_in(up, "config", "uploadpack.allowAnySHA1InWant", "true")
            write(up, {"src/both.cpp": "int x = 1;\n", "src/gone.cpp": "int g;\n", "src/plain.cpp": "int p;\n",
                       versions: "SET(VERSION_STRING 26.9.1.1)\n"})
            git_in(up, "add", "-A")
            git_in(up, "commit", "-qm", "base")
            base = git_in(up, "rev-parse", "HEAD")
            write(up, {"src/both.cpp": "int x = 2;\n", "src/new.cpp": "int n;\n"})
            git_in(up, "rm", "-q", "src/gone.cpp", "src/plain.cpp")
            git_in(up, "add", "-A")
            git_in(up, "commit", "-qm", "Merge pull request #7 from a/b")
            target = git_in(up, "rev-parse", "HEAD")

            chdb.mkdir()
            git_in(chdb, "init", "-q")
            for script in (HERE / "daily_sync.py", sync.PREFLIGHT):
                rel = script.relative_to(ROOT)
                (chdb / rel).parent.mkdir(parents=True, exist_ok=True)
                (chdb / rel).write_text(script.read_text())
            write(chdb, {"src/both.cpp": "int x = 1; // chdb\n", "src/gone.cpp": "int g; // chdb\n",
                         "src/plain.cpp": "int p;\n", versions: "SET(VERSION_STRING 26.9.1.1)\n",
                         "programs/local/chdb.h": '#define CHDB_VERSION "26.9.0"\n', "UPSTREAM_COMMIT": base + "\n"})
            git_in(chdb, "add", "-A")
            git_in(chdb, "commit", "-qm", "chdb")
            start = git_in(chdb, "rev-parse", "HEAD")

            bare = sync.upstream_dir(work)
            git_in(tmp, "init", "-q", "--bare", str(bare))
            git_in(tmp, f"--git-dir={bare}", "remote", "add", "origin", up.as_uri())
            git_in(tmp, f"--git-dir={bare}", "config", "remote.origin.promisor", "true")
            git_in(tmp, f"--git-dir={bare}", "config", "remote.origin.partialclonefilter", "blob:none")
            git_in(tmp, f"--git-dir={bare}", "fetch", "-q", "--filter=blob:none", "origin",
                   f"+refs/heads/master:{sync.UPSTREAM_MASTER}")

            script = ["python3", ".github/nightly/daily_sync.py"]
            subprocess.run([*script, "apply", "--work", str(work), "--target", target], cwd=chdb, check=True,
                           capture_output=True)
            status = git_in(chdb, "status", "--porcelain", "--untracked-files=no")
            self.assertIn("UU src/both.cpp", status)
            self.assertIn("UD src/gone.cpp", status)  # deleted upstream after chDB changed it
            self.assertIn("D  src/plain.cpp", status)
            self.assertEqual(git_in(chdb, "show", ":1:src/gone.cpp"), "int g;")  # upstream's base, copied here
            self.assertFalse(Path(git_in(chdb, "rev-parse", "--git-path", "objects/info/alternates")).exists())
            subprocess.run([*script, "snapshot"], cwd=chdb, check=True, capture_output=True)
            self.assertIn("UD src/gone.cpp", git_in(chdb, "status", "--porcelain", "--untracked-files=no"))

            write(chdb, {"src/both.cpp": "int x = 2; // chdb\n", "src/fix.cpp": "int f;\n"})
            git_in(chdb, "add", "src/both.cpp", "src/fix.cpp")
            git_in(chdb, "rm", "-q", "src/gone.cpp")
            subprocess.run([*script, "stage"], cwd=chdb, check=True, capture_output=True)
            final = git_in(chdb, "write-tree")
            state = chdb / ".sync"
            summary = sync.json.loads((state / "summary.json").read_text())
            self.assertEqual(summary["chdb_base"], start)
            self.assertIn({"path": "src/gone.cpp", "note": "deleted upstream"}, summary["conflicts"])

            # What open_pr.sh does in the publish job: both patches on the commit the sync started from.
            pub = tmp / "pub"
            git_in(tmp, "clone", "-q", str(chdb), str(pub))
            git_in(pub, "checkout", "-q", "--detach", start)
            for patch, message in (("pr-upstream.diff", "commit-upstream.txt"), ("pr-changes.diff", "commit-changes.txt")):
                git_in(pub, "apply", "--index", "--binary", str(state / patch))
                git_in(pub, "commit", "-q", "-F", str(state / message))
            self.assertEqual(git_in(pub, "rev-parse", "HEAD~1^{tree}"), summary["apply_tree"])
            self.assertEqual(git_in(pub, "rev-parse", "HEAD^{tree}"), final)
            self.assertEqual(git_in(pub, "log", "-1", "--format=%s", "HEAD~1"), f"Sync ClickHouse master up to {target[:11]}")


class Messages(unittest.TestCase):
    summary = {
        "base": A, "target": B, "target_reason": "newest master commit that passed MasterCI",
        "upstream_prs": [1, 2, 3], "applied_files": ["src/a.cpp"], "skipped": {"x": 4},
        "conflicts": [{"path": "src/a.cpp"}],
        "changed_files": ["src/a.cpp", "programs/local/EmbeddedServer.cpp"],
        "extra_files": ["programs/local/EmbeddedServer.cpp"], "markers_left": [], "changed_by": "Claude Code",
        "build": {"result": "fail", "step": "test", "tail": "FAILED test_x", "current": False},
        "submodules": [{"path": "contrib/lib", "action": "bumped", "upstream_old": A, "upstream_new": B}],
        "chdb_version": {"from": "26.9.0", "to": "26.10.0-dev"},
    }

    def test_commit_message(self):
        message = sync.commit_message(self.summary)
        self.assertTrue(message.startswith("Sync ClickHouse master up to bbbbbbbbbbb\n\n"))
        self.assertIn("3 merged pull requests", message)
        self.assertIn("Submodules: 1 bumped.", message)
        self.assertIn("26.10.0-dev", message)

    def test_two_commits(self):
        self.assertIn("left with their markers", sync.commit_message(self.summary, "upstream"))
        self.assertIn("Conflicts resolved: 1.", sync.commit_message(self.summary))
        message = sync.changes_message(self.summary)
        self.assertTrue(message.startswith(
            "Resolve the sync's conflicts and fix the build and tests (Claude Code)\n\n"))
        self.assertIn("- 1 conflicted file resolved;", message)
        self.assertIn("- 1 other file changed", message)

    def test_pr_body(self):
        body = sync.pr_body(self.summary)
        self.assertIn("## Conflicts\n\n- `src/a.cpp`", body)
        self.assertIn("Target: newest master commit that passed MasterCI.", body)
        self.assertIn("**Files changed beyond the conflicts**", body)
        self.assertIn("`programs/local/EmbeddedServer.cpp`", body)
        self.assertIn("**Failed at test.**", body)
        self.assertIn("the result is for an earlier state", body)
        self.assertIn("FAILED test_x", body)
        self.assertNotIn("Conflict markers are still in", body)


class Delta(unittest.TestCase):
    up_old = "int f() {\n    return compute(1);\n}\n"
    up_new = "int f() {\n    return compute(2);\n}\n"
    chdb_old = "int f() {\n#if USE_CHDB_HOOK\n    chdb_hook();\n#endif\n    return compute(1);\n}\n"

    def test_good_merge(self):
        merged = "int f() {\n#if USE_CHDB_HOOK\n    chdb_hook();\n#endif\n    return compute(2);\n}\n"
        self.assertEqual(checks.compare_file(self.up_old, self.up_new, self.chdb_old, merged), ([], []))

    def test_chdb_change_lost(self):
        lost, missing = checks.compare_file(self.up_old, self.up_new, self.chdb_old, self.up_new)
        self.assertIn("chdb_hook();", lost)
        self.assertEqual(missing, [])

    def test_upstream_change_missing(self):
        lost, missing = checks.compare_file(self.up_old, self.up_new, self.chdb_old, self.chdb_old)
        self.assertEqual(lost, [])
        self.assertEqual(missing, ["return compute(2);"])


class Scanners(unittest.TestCase):
    def test_cmake_paths(self):
        text = (
            'set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib")\n'
            'set(SRCS "${LIBRARY_DIR}/common/zstd_common.c" "${LIBRARY_DIR}/*.c" "${LIBRARY_DIR}/${X}.c")\n'
        )
        self.assertEqual(checks.cmake_paths(text, "zstd"), ["lib/common/zstd_common.c"])

    def test_new_local_server_calls(self):
        diff = (
            "+++ b/programs/local/LocalServer.cpp\n"
            "+    registerPlanSteps();\n"
            "+    getIOThreadPool().initialize(1, 2, 3);\n"
            "+    global_context->setPageCache(x);\n"
            "-    registerOld();\n"
        )
        self.assertEqual(checks.new_local_server_calls(diff),
                         {"registerPlanSteps", "getIOThreadPool", "setPageCache"})


class Publishing(unittest.TestCase):
    linux = "chdb_core-26.10.0.dev2610151617-cp39-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"
    mac = "chdb_core-26.10.0.dev2610151617-cp39-abi3-macosx_11_0_arm64.whl"
    old = "chdb_core-26.10.0.dev2609010930-cp39-abi3-manylinux2014_aarch64.manylinux_2_17_aarch64.whl"

    def asset(self, name, updated_at="2026-10-15T20:00:00Z"):
        return {"name": name, "size": 7, "digest": "sha256:" + "f" * 64, "updated_at": updated_at}

    def test_is_sync_merge(self):
        def pull(ref, sha, repo="chdb-io/chdb-core", merged=True):
            return {"merged_at": "2026-10-16T08:00:00Z" if merged else None, "merge_commit_sha": sha,
                    "head": {"ref": ref, "repo": {"full_name": repo} if repo else None}}
        pulls = [pull("fix/x", A), pull("sync/clickhouse-master", B), pull("sync/clickhouse-master", ZERO, merged=False),
                 pull("sync/clickhouse-master", "c" * 40, repo="someone/chdb-core"), pull("gone", "d" * 40, repo=None)]
        with unittest.mock.patch.object(publish, "gh", return_value=publish.json.dumps(pulls)):
            self.assertTrue(publish.is_sync_merge("chdb-io/chdb-core", B))
            self.assertFalse(publish.is_sync_merge("chdb-io/chdb-core", A))         # another branch
            self.assertFalse(publish.is_sync_merge("chdb-io/chdb-core", ZERO))      # closed, not merged
            self.assertFalse(publish.is_sync_merge("chdb-io/chdb-core", "c" * 40))  # a fork's branch of that name
            self.assertFalse(publish.is_sync_merge("chdb-io/chdb-core", "d" * 40))

    def test_night_wheels(self):
        self.assertEqual(publish.dev_number("2026-10-15T16:17:59Z"), "2610151617")
        assets = [self.asset(self.linux), self.asset(self.mac), self.asset(self.old), self.asset("notes.txt")]
        wheels = publish.night_wheels(assets, "2610151617")
        self.assertEqual([w["platform"] for w in wheels], ["linux-x86_64", "macos-arm64"])
        self.assertEqual(wheels[0]["sha256"], "f" * 64)
        self.assertEqual(publish.wheel_platform(self.old), "linux-aarch64")
        self.assertEqual(publish.wheel_version(self.linux), "26.10.0.dev2610151617")

    def test_stale(self):
        assets = [self.asset(self.linux), self.asset(self.mac), self.asset(self.old), self.asset("notes.txt")]
        self.assertEqual(publish.stale(assets, 1), [self.old])
        self.assertEqual(publish.stale(assets, 2), [])
        self.assertEqual(publish.dev_of(self.old), 2609010930)

    def test_pep503(self):
        pages = publish.pep503_pages("chdb-io/chdb-core", [self.asset(self.linux), self.asset("notes.txt")])
        project = pages["simple/chdb-core/index.html"]
        self.assertIn(f"https://github.com/chdb-io/chdb-core/releases/download/nightly/{self.linux}#sha256=", project)
        self.assertNotIn("notes.txt", project)
        self.assertIn('href="chdb-core/"', pages["simple/index.html"])


class Reporting(unittest.TestCase):
    def test_junit_rows(self):
        xml = (
            '<?xml version="1.0"?><testsuite name="s">'
            '<testcase classname="test_a.T" name="test_ok" time="0.5"/>'
            '<testcase classname="test_a.T" name="test_bad" time="1"><failure message="x">t</failure></testcase>'
            '<testcase classname="test_a.T" name="test_skip" time="0"><skipped/></testcase>'
            "</testsuite>"
        )
        with tempfile.TemporaryDirectory() as tmp:
            # As the wheel workflows lay them out: CHDB_JUNIT_DIR per Python and pandas.
            (Path(tmp) / "py3.9-pandas_3.0").mkdir()
            (Path(tmp) / "py3.9-pandas_3.0" / "main-shard-1.xml").write_text(xml)
            rows = report.junit_rows(Path(tmp), "linux-x86_64", {"date": "2026-10-12", "head_sha": B})
        statuses = collections.Counter(r["status"] for r in rows)
        self.assertEqual(statuses, {"passed": 1, "failure": 1, "skipped": 1})
        self.assertEqual(rows[1]["test"], "test_a.T.test_bad")
        self.assertEqual(rows[0]["suite"], "py3.9-pandas_3.0/main-shard-1")

    def test_failed_log_tail_ends_at_the_first_error(self):
        log = "\n".join([f"line {i}" for i in range(1000)] + ["##[error]Process completed with exit code 1."]
                        + [f"cleanup {i}" for i in range(2000)])
        answers = {"jobs": '[{"id": 7, "name": "build"}]', "logs": log}
        with unittest.mock.patch.object(report, "gh", lambda *args: answers["jobs" if "/runs/" in args[1] else "logs"]):
            tail = report.failed_log_tail("o/r", 1).splitlines()
        self.assertEqual(tail[-1], "##[error]Process completed with exit code 1.")
        self.assertEqual(len(tail), report.LOG_TAIL_LINES + 1)  # the header, then the lines
        self.assertNotIn("cleanup 0", tail)

    def test_store_nights(self):
        self.assertEqual(report.lit("it's"), "'it\\'s'")
        with tempfile.TemporaryDirectory() as tmp, unittest.mock.patch.dict(os.environ, {"BLOB_BASE_URL": ""}):
            local = report.Store(Path(tmp), "2026-10-15")
            self.assertFalse(local.remote)
            self.assertTrue(local.nights("runs", "x UInt8", 3).startswith(f"file('{tmp}/store/runs/*.parquet'"))
        with unittest.mock.patch.dict(os.environ, {"BLOB_BASE_URL": "https://b.example/"}):
            remote = report.Store(Path("/w"), "2026-10-15").nights("runs", "x UInt8", 2)
        self.assertIn("url('https://b.example/runs/{2026-10-14,2026-10-13}.parquet', 'Parquet', 'x UInt8')", remote)
        self.assertIn("file('/w/store/runs/2026-10-15.parquet'", remote)


if __name__ == "__main__":
    unittest.main()
