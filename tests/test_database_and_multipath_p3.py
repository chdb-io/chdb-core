#!/usr/bin/env python3
"""Phase 3 (P3) regression tests for:
1. Two-level `chdb.open` / `chdb.Database` + `db.connect()` Python bindings
2. Zero-disk isolated `:memory:` `DatabaseMemory` namespaces (`Database(":memory:")`
   and `Session()`) alongside backward-compatible shared `chdb.connect(":memory:")`
3. Single-process multi-path coexistence (`/dir/A`, `/dir/B`, `:memory:`),
   `<path>/status` cross-process locking & release on close, per-database
   `backups.allowed_path`, SQL UDF isolation, and multi-threaded concurrency.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

import chdb
from chdb import session as chdb_session


class TestTwoLevelDatabaseAndZeroDiskMemory(unittest.TestCase):
    def test_zero_disk_isolated_memory_databases_and_shared_legacy_connect(self):
        tmp_root = tempfile.gettempdir()
        before_dirs = set(glob.glob(os.path.join(tmp_root, "clickhouse-embedded-*")))

        with chdb.open(":memory:") as db1, chdb.Database(":memory:") as db2:
            self.assertTrue(db1.is_open())
            self.assertTrue(db2.is_open())
            self.assertEqual(db1.path, ":memory:")

            with db1.connect() as c1a, db1.connect(read_only=True, max_threads=2) as c1b, db2.connect() as c2:
                self.assertEqual(str(c1a.query("SELECT currentDatabase()", "TSVRaw")).strip(), "default")
                c1a.query("CREATE TABLE items (id UInt64, tag String) ENGINE = Memory")
                c1a.query("INSERT INTO items VALUES (1, 'db1_one'), (2, 'db1_two')")
                c1a.query("CREATE DATABASE sub_db1")
                c1a.query("CREATE TABLE sub_db1.notes (msg String) ENGINE = Memory")
                c1a.query("INSERT INTO sub_db1.notes VALUES ('secret_db1')")
                c1a.query("CREATE FUNCTION p3_tag AS () -> 'from_db1'")

                # c1b shares db1's namespace, sees max_threads=2, and enforces read_only
                self.assertEqual(str(c1b.query("SELECT count() FROM items", "TSVRaw")).strip(), "2")
                self.assertEqual(str(c1b.query("SELECT msg FROM sub_db1.notes", "TSVRaw")).strip(), "secret_db1")
                self.assertEqual(str(c1b.query("SELECT p3_tag()", "TSVRaw")).strip(), "from_db1")
                self.assertEqual(str(c1b.query("SELECT getSetting('max_threads')", "TSVRaw")).strip(), "2")
                with self.assertRaises(chdb.ChdbError):
                    c1b.query("INSERT INTO items VALUES (3, 'blocked')")

                # db2 is completely isolated from db1 (tables, SHOW DATABASES, system.*, SQL UDFs)
                dbs_in_db2 = str(c2.query("SHOW DATABASES", "TSVRaw")).strip().splitlines()
                self.assertNotIn("sub_db1", dbs_in_db2)
                self.assertIn("default", dbs_in_db2)
                self.assertEqual(
                    str(c2.query("SELECT count() FROM system.tables WHERE database = 'default' AND name = 'items'", "TSVRaw")).strip(),
                    "0",
                )
                with self.assertRaises(chdb.ChdbError):
                    c2.query("SELECT p3_tag()", "TSVRaw")

                c2.query("CREATE TABLE items (price Float64) ENGINE = Memory")
                c2.query("INSERT INTO items VALUES (19.5), (20.5)")
                self.assertEqual(str(c2.query("SELECT sum(price) FROM items", "TSVRaw")).strip(), "40")

                # Verify zero /tmp/clickhouse-embedded-* directories were created
                during_dirs = set(glob.glob(os.path.join(tmp_root, "clickhouse-embedded-*")))
                self.assertEqual(during_dirs - before_dirs, set())

        self.assertFalse(db1.is_open())
        with self.assertRaises(RuntimeError):
            db1.connect()

        # Verify Session() instances are isolated from each other while legacy connect(':memory:') shares
        with chdb_session.Session() as s1, chdb_session.Session() as s2:
            s1.query("CREATE TABLE sess_only (x Int32) ENGINE = Memory")
            s1.query("INSERT INTO sess_only VALUES (42)")
            self.assertEqual(str(s1.query("SELECT x FROM sess_only", "TSVRaw")).strip(), "42")
            self.assertEqual(
                str(s2.query("SELECT count() FROM system.tables WHERE database = 'default' AND name = 'sess_only'", "TSVRaw")).strip(),
                "0",
            )

            # Legacy connect(':memory:') shares default namespace across concurrent legacy handles
            leg1 = chdb.connect(":memory:")
            leg2 = chdb.connect(":memory:")
            try:
                leg1.query("CREATE TABLE legacy_shared (v Int32) ENGINE = Memory")
                leg1.query("INSERT INTO legacy_shared VALUES (77)")
                self.assertEqual(str(leg2.query("SELECT v FROM legacy_shared", "TSVRaw")).strip(), "77")
                # And still does not leak into s1 or s2!
                self.assertEqual(
                    str(s1.query("SELECT count() FROM system.tables WHERE name = 'legacy_shared'", "TSVRaw")).strip(),
                    "0",
                )
            finally:
                leg2.close()
                leg1.close()


class TestMultiPathCoexistenceAndLocks(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.mkdtemp(prefix="chdb_p3_py_")
        self.dir_a = os.path.join(self.work, "dir_a")
        self.dir_b = os.path.join(self.work, "dir_b")
        self.bk_a = os.path.join(self.work, "bk_a")
        self.bk_b = os.path.join(self.work, "bk_b")
        os.makedirs(self.bk_a)
        os.makedirs(self.bk_b)

    def tearDown(self):
        shutil.rmtree(self.work, ignore_errors=True)

    def test_simultaneous_persistent_paths_memory_backups_and_lock_release(self):
        mem_sess = chdb_session.Session()
        db_a = chdb.open(f"{self.dir_a}?backups.allowed_path={self.bk_a}")
        db_b = chdb.open(f"{self.dir_b}?backups.allowed_path={self.bk_b}")
        conn_a = db_a.connect()
        conn_b = db_b.connect()

        try:
            mem_sess.query("CREATE TABLE metrics (v UInt64) ENGINE = Memory")
            mem_sess.query("INSERT INTO metrics VALUES (1000)")

            conn_a.query("CREATE TABLE metrics (v UInt64) ENGINE = MergeTree ORDER BY v")
            conn_a.query("INSERT INTO metrics VALUES (10), (20)")
            conn_a.query("CREATE DATABASE app")
            conn_a.query("CREATE TABLE app.users (name String) ENGINE = MergeTree ORDER BY name")
            conn_a.query("INSERT INTO app.users VALUES ('alice')")
            conn_a.query("CREATE FUNCTION path_id AS () -> 'A'")

            conn_b.query("CREATE TABLE metrics (v UInt64) ENGINE = MergeTree ORDER BY v")
            conn_b.query("INSERT INTO metrics VALUES (300), (400)")
            conn_b.query("CREATE DATABASE app")
            conn_b.query("CREATE TABLE app.users (name String) ENGINE = MergeTree ORDER BY name")
            conn_b.query("INSERT INTO app.users VALUES ('bob'), ('carol')")
            conn_b.query("CREATE FUNCTION path_id AS () -> 'B'")

            # Verify independent data, system views, and SQL UDFs across all 3 simultaneously!
            self.assertEqual(str(mem_sess.query("SELECT sum(v) FROM metrics", "TSVRaw")).strip(), "1000")
            self.assertEqual(str(conn_a.query("SELECT sum(v) FROM metrics", "TSVRaw")).strip(), "30")
            self.assertEqual(str(conn_b.query("SELECT sum(v) FROM metrics", "TSVRaw")).strip(), "700")
            self.assertEqual(str(conn_a.query("SELECT count() FROM app.users", "TSVRaw")).strip(), "1")
            self.assertEqual(str(conn_b.query("SELECT count() FROM app.users", "TSVRaw")).strip(), "2")
            self.assertEqual(str(conn_a.query("SELECT path_id()", "TSVRaw")).strip(), "A")
            self.assertEqual(str(conn_b.query("SELECT path_id()", "TSVRaw")).strip(), "B")

            # Verify per-database backups.allowed_path isolation while :memory: was started first!
            arc_a = os.path.join(self.bk_a, "app_a.tar.gz")
            arc_b = os.path.join(self.bk_b, "app_b.tar.gz")
            conn_a._conn.backup_database("app", arc_a, "")
            conn_b._conn.backup_database("app", arc_b, "")
            with self.assertRaises(RuntimeError):
                # dir_a is NOT allowed to write into dir_b's backup directory!
                conn_a._conn.backup_database("app", os.path.join(self.bk_b, "cross.tar.gz"), "")

            # Verify concurrent multi-threaded queries across dir_a, dir_b, and :memory:
            errors: list[Exception] = []

            def worker(conn_obj, expected_sum: int):
                try:
                    for _ in range(15):
                        got = int(str(conn_obj.query("SELECT sum(v) FROM metrics", "TSVRaw")).strip())
                        if got != expected_sum:
                            raise AssertionError(f"expected {expected_sum}, got {got}")
                except Exception as exc:
                    errors.append(exc)

            threads = [
                threading.Thread(target=worker, args=(conn_a, 30)),
                threading.Thread(target=worker, args=(conn_b, 700)),
                threading.Thread(target=worker, args=(mem_sess, 1000)),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])

            # Close ONLY dir_a while dir_b and mem_sess remain open!
            conn_a.close()
            db_a.close()

            # Subprocess must now be able to open dir_a (StatusFile lock released!) and read persisted rows
            env = dict(os.environ)
            env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
            proc = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    f"import chdb; s = chdb.session.Session({self.dir_a!r}); "
                    "print(str(s.query('SELECT sum(v), path_id() FROM metrics', 'CSV')).strip()); s.close()",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertEqual(proc.stdout.strip(), '30,"A"')

            # Meanwhile dir_b and mem_sess are still alive and intact in this process
            self.assertEqual(str(conn_b.query("SELECT sum(v) FROM metrics", "TSVRaw")).strip(), "700")
            self.assertEqual(str(mem_sess.query("SELECT sum(v) FROM metrics", "TSVRaw")).strip(), "1000")
        finally:
            conn_a.close()
            db_a.close()
            conn_b.close()
            db_b.close()
            mem_sess.close()


if __name__ == "__main__":
    unittest.main()
