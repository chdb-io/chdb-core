#!/usr/bin/env python3
"""Regression tests for session lifecycle, context isolation, callback re-entrancy,
streaming DataFrame GIL ordering, abandoned stream cleanup, and zero-boot import."""

import gc
import subprocess
import sys
import textwrap
import threading
import unittest
import weakref

import chdb


def _run_python(code: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class TestZeroBootImportAndVersions(unittest.TestCase):
    def test_import_exposes_compile_time_versions_without_booting_engine(self):
        proc = _run_python(
            """
            import glob, os, tempfile
            tmp = tempfile.gettempdir()
            before = set(glob.glob(os.path.join(tmp, "clickhouse-embedded-*")))
            import chdb
            after_import = set(glob.glob(os.path.join(tmp, "clickhouse-embedded-*")))
            assert after_import == before, f"unexpected temp dirs on import: {after_import - before}"
            assert hasattr(chdb._chdb, "engine_version")
            assert hasattr(chdb._chdb, "chdb_version")
            eng = chdb._chdb.engine_version()
            libv = chdb._chdb.chdb_version()
            assert eng and eng == chdb.engine_version, (eng, chdb.engine_version)
            assert libv and any(ch.isdigit() for ch in libv), libv

            # Verify compile-time engine_version matches runtime SELECT version()
            conn = chdb.connect(":memory:")
            try:
                sql_ver = str(conn.query("SELECT version()", "TabSeparated")).strip()
                assert eng == sql_ver, (eng, sql_ver)
            finally:
                conn.close()
            print("OK")
            """
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout)


class TestLocalServerGuardWhileConnectionOpen(unittest.TestCase):
    def test_one_shot_local_server_rejected_without_corrupting_open_session(self):
        conn = chdb.connect(":memory:")
        try:
            conn.query("CREATE TABLE guard_t (id UInt64, v String) ENGINE = Memory")
            conn.query("INSERT INTO guard_t VALUES (1, 'alive'), (2, 'intact')")

            with self.assertRaises(Exception) as ctx:
                chdb._chdb.query("SELECT 1", "CSV")
            self.assertIn("connection is open", str(ctx.exception))

            res = str(conn.query("SELECT id, v FROM guard_t ORDER BY id", "CSV")).strip().splitlines()
            self.assertEqual(res, ['1,"alive"', '2,"intact"'])
        finally:
            conn.close()


class TestSessionContextIsolation(unittest.TestCase):
    def test_set_profile_persists_across_queries_and_use_updates_classify_query(self):
        conn = chdb._chdb.connect(":memory:")
        try:
            conn.query("SET max_threads = 3")
            conn.query("SET profile = 'default'")
            # Verify session remains usable and settings queries work after SET profile
            out = conn.query("SELECT getSetting('max_threads') > 0", "TSVRaw").bytes().strip()
            self.assertEqual(out, b"1")

            conn.query("CREATE DATABASE IF NOT EXISTS iso_db")
            conn.query("USE iso_db")
            analysis = conn.classify_query("INSERT INTO local_tbl VALUES (1)", "iso_db")
            self.assertTrue(analysis["writes_only_target_database"])
            other_analysis = conn.classify_query("INSERT INTO local_tbl VALUES (1)", "default")
            self.assertFalse(other_analysis["writes_only_target_database"])
        finally:
            conn.close()


class TestCallbackReentrancyGuards(unittest.TestCase):
    def test_progress_callback_reentry_rejected_without_deadlock(self):
        proc = _run_python(
            """
            import chdb

            conn = chdb._chdb.connect(":memory:")
            other = chdb._chdb.connect(":memory:")
            reentry_errors = []
            other_results = []

            def on_progress(_progress):
                if not reentry_errors:
                    try:
                        conn.query("SELECT 1", "CSV")
                    except Exception as exc:
                        reentry_errors.append(str(exc))
                    try:
                        conn.classify_query("SELECT 1", "default")
                    except Exception as exc:
                        reentry_errors.append(str(exc))
                    # A separate connection must NOT be blocked by conn's re-entrancy guard.
                    other_results.append(other.query("SELECT 99", "TSVRaw").bytes().strip())

            conn.set_progress_callback(on_progress)
            res = conn.query(
                "SELECT sum(sleepEachRow(0.05)) FROM numbers(5) SETTINGS interactive_delay = 1000",
                "TSVRaw",
            ).bytes().strip()
            assert res == b"0", res
            assert len(reentry_errors) >= 1, "progress callback did not fire or did not reject"
            for msg in reentry_errors:
                assert "active callback" in msg, msg
            if other_results:
                assert other_results[0] == b"99", other_results
            conn.set_progress_callback(None)
            other.close()
            conn.close()
            print("PROGRESS_OK")
            """,
            timeout=20,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("PROGRESS_OK", proc.stdout)

    def test_udf_and_pyreader_reentry_rejected_while_second_connection_succeeds(self):
        proc = _run_python(
            """
            import chdb

            conn = chdb._chdb.connect(":memory:")
            conn2 = chdb._chdb.connect(":memory:")

            def udf_reenter(x):
                # Re-entering the owning connection from a pipeline worker must fail fast.
                try:
                    conn.query("SELECT 1", "CSV")
                except RuntimeError as exc:
                    assert "active callback" in str(exc), str(exc)
                else:
                    raise AssertionError("UDF re-entering own connection must fail")
                # Querying a distinct connection from inside the UDF must succeed cleanly.
                v = int(conn2.query(f"SELECT {int(x)} + 10", "TSVRaw").bytes().strip())
                return v

            chdb.create_function("udf_reenter_guard", udf_reenter, arg_types=["UInt64"], return_type="UInt64")
            out = conn.query("SELECT udf_reenter_guard(number) FROM numbers(3) ORDER BY number", "TSVRaw").bytes().decode().splitlines()
            assert out == ["10", "11", "12"], out
            chdb.drop_function("udf_reenter_guard")
            conn2.close()
            conn.close()
            print("UDF_OK")
            """,
            timeout=20,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("UDF_OK", proc.stdout)


class TestStreamingDataFrameAndAbandonedStreamCleanup(unittest.TestCase):
    def test_abandoned_stream_gc_releases_bound_dataframe_and_preserves_newer_stream(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        conn = chdb.connect(":memory:")
        try:
            def make_and_abandon():
                df = pd.DataFrame({"k": list(range(1000)), "v": [f"s_{i}" for i in range(1000)]})
                ref = weakref.ref(df)
                stream = conn.send_query(
                    "SELECT k, v FROM Python(df) SETTINGS max_block_size = 100",
                    "DataFrame",
                )
                first_chunk = stream.fetch()
                self.assertIsNotNone(first_chunk)
                self.assertGreater(len(first_chunk), 0)
                self.assertEqual(list(first_chunk.columns), ["k", "v"])
                # Drop both `stream` and `df` without calling stream.close()/cancel()
                del stream
                del df
                return ref

            df_ref = make_and_abandon()
            gc.collect()
            self.assertIsNone(
                df_ref(),
                "GC of an abandoned StreamingResult must cancel the stream and release Python(df)",
            )

            # Superseded stream: cancelling or GC-ing stream1 after stream2 started must NOT break stream2.
            stream1 = conn.send_query("SELECT number AS a FROM numbers(200000) SETTINGS max_block_size = 10000", "DataFrame")
            self.assertIsNotNone(stream1.fetch())

            stream2 = conn.send_query("SELECT number AS b FROM numbers(500)", "DataFrame")
            stream1.cancel()
            del stream1
            gc.collect()

            batches = list(stream2)
            total = sum(len(b) for b in batches)
            self.assertEqual(total, 500)
            combined = pd.concat(batches, ignore_index=True)
            self.assertEqual(list(combined.columns), ["b"])
            self.assertEqual(combined["b"].tolist(), list(range(500)))
        finally:
            conn.close()

    def test_concurrent_cursor_operations_do_not_race(self):
        conn = chdb._chdb.connect(":memory:")
        try:
            cur = conn.cursor()
            errors = []

            def worker(tid: int):
                try:
                    for i in range(15):
                        cur.execute(f"SELECT {tid * 100 + i} AS v")
                        _ = cur.rows_read()
                        _ = cur.data_size()
                        _ = cur.has_error()
                        _ = cur.get_memview().tobytes()
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            cur.close()
            self.assertEqual(errors, [])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
