#!/usr/bin/env python3
"""Phase 2 (P2) regression tests for EmbeddedSession, self-contained concurrent
streams, lock-free interrupt / error_code, and explicit Python table bindings
(tables= & conn.register / conn.unregister)."""

from __future__ import annotations

import gc
import os
import tempfile
import threading
import time
import unittest
import weakref

import chdb
from chdb import session as chdb_session


class TestEmbeddedSessionCompatibility(unittest.TestCase):
    def test_multistatement_insert_formats_set_use_and_outfile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            infile_csv = os.path.join(tmpdir, "in.csv")
            outfile_csv = os.path.join(tmpdir, "out.csv")
            with open(infile_csv, "w", encoding="utf-8") as f:
                f.write("30,thirty\n40,forty\n")

            conn = chdb.connect(":memory:")
            try:
                batch = f"""
                CREATE DATABASE IF NOT EXISTS p2_db;
                USE p2_db;
                SET max_threads = 1;
                CREATE TABLE items (id Int64, label String) ENGINE = Memory;
                INSERT INTO items VALUES (10, 'ten;semi'), (20, 'twenty');
                INSERT INTO items FORMAT CSV
15,"fifteen;csv"

                INSERT INTO items FROM INFILE '{infile_csv}' FORMAT CSV;
                SELECT id, label FROM items ORDER BY id
                """
                res = conn.query(batch, "CSV")
                lines = str(res).strip().splitlines()
                self.assertEqual(
                    lines,
                    [
                        '10,"ten;semi"',
                        '15,"fifteen;csv"',
                        '20,"twenty"',
                        '30,"thirty"',
                        '40,"forty"',
                    ],
                )

                # Verify USE persisted on session via classify_query and unqualified lookup
                raw_conn = conn._conn
                analysis = raw_conn.classify_query("INSERT INTO items VALUES (50, 'fifty')", "p2_db")
                self.assertTrue(analysis["writes_only_target_database"])

                # Verify INTO OUTFILE TRUNCATE and APPEND
                conn.query(
                    f"SELECT id, label FROM items WHERE id <= 15 ORDER BY id INTO OUTFILE '{outfile_csv}' TRUNCATE FORMAT CSV"
                )
                conn.query(
                    f"SELECT id, label FROM items WHERE id = 20 ORDER BY id INTO OUTFILE '{outfile_csv}' APPEND FORMAT CSV"
                )
                with open(outfile_csv, "r", encoding="utf-8") as f:
                    out_lines = f.read().strip().splitlines()
                self.assertEqual(
                    out_lines,
                    ['10,"ten;semi"', '15,"fifteen;csv"', '20,"twenty"'],
                )
            finally:
                conn.close()


class TestSingleConnectionConcurrencyAndStreams(unittest.TestCase):
    def test_concurrent_read_queries_and_multiple_active_streams_on_one_connection(self):
        conn = chdb.connect(":memory:")
        try:
            conn.query("CREATE TABLE conc_t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k")
            conn.query("INSERT INTO conc_t SELECT number, number * 3 FROM numbers(100000)")

            # Open two simultaneous active streams on the SAME connection
            s1 = conn.send_query(
                "SELECT k FROM conc_t ORDER BY k SETTINGS max_block_size = 12000", "CSV"
            )
            s2 = conn.send_query(
                "SELECT v FROM conc_t WHERE k < 50000 ORDER BY k SETTINGS max_block_size = 9000",
                "CSV",
            )

            c1_first = s1.fetch()
            c2_first = s2.fetch()
            self.assertIsNotNone(c1_first)
            self.assertIsNotNone(c2_first)
            rows_s1 = c1_first.rows_read()
            rows_s2 = c2_first.rows_read()

            # While both streams remain open mid-flight, run 6 concurrent reader threads on `conn`
            errors: list[Exception] = []

            def reader_worker(tid: int) -> None:
                try:
                    for step in range(20):
                        target = tid * 100 + step
                        out = conn.query(
                            "SELECT sum(v) FROM conc_t WHERE k = {target:UInt64}",
                            "TSVRaw",
                            params={"target": target},
                        )
                        self.assertEqual(int(str(out).strip()), target * 3)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=reader_worker, args=(t,)) for t in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])

            # Both streams must still drain cleanly with exact row totals
            for chunk in s1:
                rows_s1 += chunk.rows_read()
            for chunk in s2:
                rows_s2 += chunk.rows_read()

            self.assertEqual(rows_s1, 100000)
            self.assertEqual(rows_s2, 50000)
        finally:
            conn.close()


class TestInterruptAndErrorCodes(unittest.TestCase):
    def test_result_and_exception_error_codes(self):
        # One-shot _chdb.query error_code() when no session connection is open
        ok_res = chdb._chdb.query("SELECT 1", "CSV")
        self.assertFalse(ok_res.has_error())
        self.assertEqual(ok_res.error_code(), 0)

        bad_res = chdb._chdb.query("SELEC 1", "CSV")
        self.assertTrue(bad_res.has_error())
        self.assertEqual(bad_res.error_code(), 62)  # SYNTAX_ERROR

        conn = chdb.connect(":memory:")
        try:
            r_ok = conn.query("SELECT 123", "CSV")
            self.assertEqual(r_ok.error_code(), 0)

            with self.assertRaises(chdb.ChdbError) as ctx_tbl:
                conn.query("SELECT * FROM missing_table_p2", "CSV")
            self.assertEqual(ctx_tbl.exception.error_code, 60)  # UNKNOWN_TABLE

            with self.assertRaises(chdb.ChdbError) as ctx_syn:
                conn.query("SELECT FROM WHERE", "CSV")
            self.assertEqual(ctx_syn.exception.error_code, 62)  # SYNTAX_ERROR
        finally:
            conn.close()

    def test_watchdog_interrupt_and_close_interrupt(self):
        sess = chdb_session.Session()
        try:
            # Idle interrupt is a no-op
            sess.interrupt()

            started = threading.Event()

            def watchdog() -> None:
                started.wait(timeout=5.0)
                time.sleep(0.12)
                sess.interrupt()

            wd = threading.Thread(target=watchdog, daemon=True)
            wd.start()

            t0 = time.monotonic()
            started.set()
            with self.assertRaises(chdb.ChdbError) as ctx:
                sess.query(
                    "SELECT sum(sleepEachRow(0.1)) FROM numbers(100) SETTINGS max_block_size = 1",
                    "CSV",
                )
            elapsed = time.monotonic() - t0
            wd.join(timeout=2.0)

            self.assertEqual(ctx.exception.error_code, 394)  # QUERY_WAS_CANCELLED
            self.assertLess(elapsed, 4.0)

            # Session remains usable after interrupt
            self.assertEqual(str(sess.query("SELECT 88", "TSVRaw")).strip(), "88")
        finally:
            sess.close()

        # Verify close() from another thread interrupts an in-flight stuck query promptly
        conn = chdb.connect(":memory:")
        entered = threading.Event()
        query_err: list[BaseException] = []

        def blocked_query() -> None:
            entered.set()
            try:
                conn.query(
                    "SELECT sum(sleepEachRow(0.1)) FROM numbers(100) SETTINGS max_block_size = 1",
                    "CSV",
                )
            except BaseException as exc:
                query_err.append(exc)

        worker = threading.Thread(target=blocked_query)
        worker.start()
        entered.wait(timeout=5.0)
        time.sleep(0.12)

        t_close = time.monotonic()
        conn.close()
        worker.join(timeout=4.0)
        close_elapsed = time.monotonic() - t_close

        self.assertFalse(worker.is_alive())
        self.assertLess(close_elapsed, 4.0)
        self.assertEqual(len(query_err), 1)


class TestExplicitPythonTableBindings(unittest.TestCase):
    def _make_sample_tables(self):
        try:
            import pandas as pd

            t1 = pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})
            t2 = pd.DataFrame({"id": [2, 3], "score": [95, 88]})
            return t1, t2, True
        except ImportError:
            t1 = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
            t2 = {"id": [2, 3], "score": [95, 88]}
            return t1, t2, False

    def _run_in_isolated_scope_without_locals(self, conn, t_users, t_scores):
        # Neither `users_tbl` nor `scores_tbl` exists in locals() or globals() here!
        sql = """
            SELECT u.id, u.name, s.score
            FROM Python(users_tbl) AS u
            INNER JOIN Python('scores_tbl') AS s ON u.id = s.id
            ORDER BY u.id
        """
        return str(
            conn.query(sql, "CSV", tables={"users_tbl": t_users, "scores_tbl": t_scores})
        ).strip().splitlines()

    def test_tables_kwarg_and_register_unregister_and_fallback(self):
        t_users, t_scores, has_pandas = self._make_sample_tables()
        conn = chdb.connect(":memory:")
        try:
            # 1. Explicit tables= without frame variables
            rows = self._run_in_isolated_scope_without_locals(conn, t_users, t_scores)
            self.assertEqual(rows, ['2,"bob",95', '3,"carol",88'])

            # 2. Persistent register() / unregister() + per-query override + frame fallback
            conn.register("reg_users", t_users)
            frame_local_filter = {"id": [1, 3]}
            mixed_out = str(
                conn.query(
                    "SELECT u.id, u.name FROM Python(reg_users) u "
                    "INNER JOIN Python(frame_local_filter) f ON u.id = f.id ORDER BY u.id",
                    "CSV",
                )
            ).strip().splitlines()
            self.assertEqual(mixed_out, ['1,"alice"', '3,"carol"'])

            # Per-query tables= overrides registered name for one call without mutating registration
            override_users = {"id": [99], "name": ["override"]} if not has_pandas else type(t_users)({"id": [99], "name": ["override"]})
            over_out = str(
                conn.query(
                    "SELECT id, name FROM Python(reg_users)",
                    "CSV",
                    tables={"reg_users": override_users},
                )
            ).strip()
            self.assertEqual(over_out, '99,"override"')

            # Registered table is still intact after override call
            still_reg = str(
                conn.query("SELECT count() FROM Python(reg_users)", "TSVRaw")
            ).strip()
            self.assertEqual(still_reg, "3")

            # Streaming send_query with tables=
            stream_chunks = list(
                conn.send_query(
                    "SELECT id, name FROM Python(ephemeral_stream_tbl) ORDER BY id",
                    "CSV",
                    tables={"ephemeral_stream_tbl": t_users},
                )
            )
            stream_text = "".join(str(c) for c in stream_chunks).strip().splitlines()
            self.assertEqual(stream_text, ['1,"alice"', '2,"bob"', '3,"carol"'])

            # Unregister removes the table; second unregister is a safe idempotent no-op
            conn.unregister("reg_users")
            conn.unregister("reg_users")
            with self.assertRaises(chdb.ChdbError):
                conn.query("SELECT * FROM Python(reg_users)", "CSV")

            # Stateless chdb.query(..., tables=...) also works without frame vars
            stateless_out = str(
                chdb.query(
                    "SELECT sum(score) FROM Python(explicit_scores)",
                    "TSVRaw",
                    tables={"explicit_scores": t_scores},
                )
            ).strip()
            self.assertEqual(stateless_out, "183")

            # Validation errors on invalid table inputs
            with self.assertRaises(ValueError):
                conn.register("", t_users)
            with self.assertRaises(TypeError):
                conn.register("bad", "not_a_table_object")
            with self.assertRaises(TypeError):
                conn.query("SELECT 1", "CSV", tables="not_a_dict")
        finally:
            conn.close()

    def test_concurrent_same_table_name_isolation_via_tables_kwarg(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")

        conn = chdb.connect(":memory:")
        try:
            errors: list[Exception] = []

            def worker(tid: int) -> None:
                try:
                    for i in range(15):
                        expected = tid * 1000 + i
                        local_df = pd.DataFrame({"val": [expected, expected + 1]})
                        df_out = conn.query_df(
                            "SELECT sum(val) AS total FROM Python(shared_alias)",
                            tables={"shared_alias": local_df},
                        )
                        got = int(df_out["total"].iloc[0])
                        if got != 2 * expected + 1:
                            raise AssertionError(f"expected {2 * expected + 1}, got {got}")
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(t + 1,)) for t in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])

            # Verify zero-retention after unregister & query completion
            ephemeral_df = pd.DataFrame({"x": [1, 2, 3]})
            ref = weakref.ref(ephemeral_df)
            conn.register("temp_reg", ephemeral_df)
            self.assertEqual(
                str(conn.query("SELECT sum(x) FROM Python(temp_reg)", "TSVRaw")).strip(),
                "6",
            )
            conn.unregister("temp_reg")
            del ephemeral_df
            gc.collect()
            self.assertIsNone(ref())
        finally:
            conn.close()

    def test_arrow_and_polars_explicit_tables(self):
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("pyarrow not installed")

        conn = chdb.connect(":memory:")
        try:
            pa_tbl = pa.table({"k": [10, 20, 30], "v": ["x", "y", "z"]})
            reader = conn.query_arrow_reader(
                "SELECT k, v FROM Python(arrow_in) WHERE k >= 20 ORDER BY k",
                tables={"arrow_in": pa_tbl},
            )
            out_tbl = reader.read_all()
            self.assertEqual(out_tbl.column("k").to_pylist(), [20, 30])
            self.assertEqual(out_tbl.column("v").to_pylist(), ["y", "z"])

            raw = conn._conn
            stream_handle = raw.send_query_arrow(
                "SELECT k FROM Python(arrow_stream_in) ORDER BY k",
                tables={"arrow_stream_in": pa_tbl},
            )
            collected: list[int] = []
            while True:
                batch_reader = raw.streaming_fetch_arrow(stream_handle)
                batches = list(batch_reader)
                if not batches:
                    break
                for b in batches:
                    collected.extend(b.column("k").to_pylist())
            self.assertEqual(collected, [10, 20, 30])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
