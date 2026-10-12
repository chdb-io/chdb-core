#!/usr/bin/env python3
"""Offline tests for the daily-sync and nightly tooling. Run by Pull-CI."""

from __future__ import annotations

import collections
import datetime as dt
import importlib.util
import tempfile
import unittest
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
        chdb = {"src/kept.cpp", "utils/stale.py", "README.md", "src/gone.cpp"}
        raw = self.raw(
            ("100644", "100644", A, B, "M", "src/kept.cpp"),
            ("100644", "100644", A, B, "M", "src/dropped.cpp"),
            ("100644", "100644", A, B, "M", "utils/stale.py"),
            ("100644", "100644", A, B, "M", "README.md"),
            ("000000", "100644", ZERO, B, "A", "src/new.cpp"),
            ("000000", "100644", ZERO, B, "A", "tests/queries/0_x.sql"),
            ("100644", "000000", A, ZERO, "D", "src/gone.cpp"),
            ("160000", "160000", A, B, "M", "contrib/lib"),
        )
        files, gitlinks, skipped = sync.classify(sync.parse_raw(raw), chdb, preflight)
        self.assertEqual(files, ["src/kept.cpp", "src/new.cpp", "src/gone.cpp"])
        self.assertEqual([g.path for g in gitlinks], ["contrib/lib"])
        self.assertEqual(skipped["chDB keeps its own version"], 1)
        self.assertEqual(skipped["changed in a tree chDB drops"], 1)

    def test_conflict_marker(self):
        self.assertTrue(sync.CONFLICT_MARKER.search("a\n<<<<<<< ours\nb\n"))
        self.assertTrue(sync.CONFLICT_MARKER.search("a\n|||||||\n"))
        self.assertFalse(sync.CONFLICT_MARKER.search("a <<<<<<< b\n"))


class Messages(unittest.TestCase):
    summary = {
        "base": A, "target": B, "target_reason": "newest master commit that passed MasterCI",
        "upstream_prs": [1, 2, 3], "applied_files": ["src/a.cpp"], "skipped": {"x": 4},
        "conflicts": [{"path": "src/a.cpp", "upstream_prs": [2]}],
        "submodules": [{"path": "contrib/lib", "action": "bumped", "upstream_old": A, "upstream_new": B}],
        "chdb_version": {"from": "26.9.0", "to": "26.10.0-dev"},
    }

    def test_commit_message(self):
        message = sync.commit_message(self.summary)
        self.assertTrue(message.startswith("Sync ClickHouse master up to bbbbbbbbbbb\n\n"))
        self.assertIn("3 merged pull requests", message)
        self.assertIn("Submodules: 1 bumped.", message)
        self.assertIn("26.10.0-dev", message)

    def test_pr_body(self):
        body = sync.pr_body(self.summary)
        self.assertIn("| `src/a.cpp` | [#2](https://github.com/ClickHouse/ClickHouse/pull/2) |", body)
        self.assertIn("Target: newest master commit that passed MasterCI.", body)


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
    def wheel(self, name):
        return {"platform": "linux-x86_64", "name": name, "size": 1, "sha256": "f" * 64}

    def test_prune(self):
        manifest = {"nights": [
            {"date": "2026-09-01", "head_sha": A, "version": "1", "wheels": [self.wheel("old.whl")]},
            {"date": "2026-10-10", "head_sha": B, "version": "2", "wheels": [self.wheel("new.whl")]},
        ]}
        kept, stale = publish.prune(manifest, 30, dt.date(2026, 10, 12))
        self.assertEqual([n["head_sha"] for n in kept["nights"]], [B])
        self.assertEqual(stale, ["old.whl"])

    def test_pep503(self):
        name = "chdb_core-26.9.0.post1.dev3-cp39-abi3-manylinux2014_x86_64.whl"
        pages = publish.pep503_pages("chdb-io/chdb-core", {"nights": [
            {"date": "2026-10-10", "head_sha": B, "version": "x", "wheels": [self.wheel(name)]}]})
        project = pages["simple/chdb-core/index.html"]
        self.assertIn(f"https://github.com/chdb-io/chdb-core/releases/download/nightly/{name}#sha256=", project)
        self.assertIn('href="chdb-core/"', pages["simple/index.html"])
        self.assertEqual(publish.wheel_version(name), "26.9.0.post1.dev3")


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
            (Path(tmp) / "py3.9-pandas_3.0.xml").write_text(xml)
            rows = report.junit_rows(Path(tmp), "linux-x86_64", {"date": "2026-10-12", "head_sha": B})
        statuses = collections.Counter(r["status"] for r in rows)
        self.assertEqual(statuses, {"passed": 1, "failure": 1, "skipped": 1})
        self.assertEqual(rows[1]["test"], "test_a.T.test_bad")
        self.assertEqual(rows[0]["suite"], "py3.9-pandas_3.0")

    def test_store_tables(self):
        self.assertEqual(report.lit("it's"), "'it\\'s'")
        with tempfile.TemporaryDirectory() as tmp:
            local = report.Store(Path(tmp))
            self.assertFalse(local.remote)
            self.assertTrue(local.table("runs/x.parquet").startswith("file('"))


if __name__ == "__main__":
    unittest.main()
