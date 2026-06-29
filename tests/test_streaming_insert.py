#!python3
"""Tests for the streaming INSERT API (Connection.send_insert / StreamingInserter).

Write-side dual of test_streaming_query.py. Verifies actual inserted values and
write statistics (not just counts), per the project testing principles.
"""

import shutil
import unittest

import chdb
from chdb import session


test_dir = ".tmp_test_streaming_insert"


class TestStreamingInsertConnection(unittest.TestCase):
    def setUp(self):
        self.conn = chdb.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def _rows(self, query, fmt="CSV"):
        return self.conn.query(query, fmt).data()

    def test_basic_csv_into_mergetree_values_and_stats(self):
        self.conn.query(
            "CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a"
        )
        with self.conn.send_insert("INSERT INTO t (a, b)", "CSV") as ins:
            ins.append("1,one\n")
            ins.append("2,two\n")
            ins.append("3,three\n")
            res = ins.finish()

        self.assertEqual(res.rows_written, 3)
        self.assertGreater(res.bytes_written, 0)
        out = self._rows("SELECT a, b FROM t ORDER BY a", "CSV")
        self.assertEqual(out, "1,\"one\"\n2,\"two\"\n3,\"three\"\n")

    def test_single_chunk_multiple_rows(self):
        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a, b)", "CSV")
        ins.append("10,ten\n20,twenty\n")
        res = ins.finish()
        self.assertEqual(res.rows_written, 2)
        out = self._rows("SELECT a, b FROM t ORDER BY a", "CSV")
        self.assertEqual(out, "10,\"ten\"\n20,\"twenty\"\n")

    def test_tsv_format(self):
        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        with self.conn.send_insert("INSERT INTO t (a, b)", "TSV") as ins:
            ins.append("1\tone\n2\ttwo\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, 2)
        self.assertEqual(self._rows("SELECT a, b FROM t ORDER BY a", "TSV"), "1\tone\n2\ttwo\n")

    def test_jsoneachrow_format(self):
        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        with self.conn.send_insert("INSERT INTO t (a, b)", "JSONEachRow") as ins:
            ins.append('{"a":1,"b":"one"}\n')
            ins.append('{"a":2,"b":"two"}\n')
            res = ins.finish()
        self.assertEqual(res.rows_written, 2)
        self.assertEqual(self._rows("SELECT a, b FROM t ORDER BY a", "CSV"), "1,\"one\"\n2,\"two\"\n")

    def test_values_format(self):
        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        with self.conn.send_insert("INSERT INTO t (a, b)", "Values") as ins:
            ins.append("(1,'one'),(2,'two')")
            res = ins.finish()
        self.assertEqual(res.rows_written, 2)
        self.assertEqual(self._rows("SELECT a, b FROM t ORDER BY a", "CSV"), "1,\"one\"\n2,\"two\"\n")

    def test_bytes_input_binary_safe(self):
        # String column containing a comma and quotes round-trips when sent as
        # properly quoted CSV bytes.
        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        with self.conn.send_insert("INSERT INTO t (a, b)", "CSV") as ins:
            ins.append(b'1,"a,b"\n')
            res = ins.finish()
        self.assertEqual(res.rows_written, 1)
        self.assertEqual(self._rows("SELECT b FROM t", "CSV"), "\"a,b\"\n")

    def test_many_chunks_constant_stream(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
        n = 10000
        with self.conn.send_insert("INSERT INTO t (a)", "CSV") as ins:
            for i in range(n):
                ins.append(f"{i}\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, n)
        self.assertEqual(self._rows("SELECT count(), sum(a) FROM t", "CSV"),
                         f"{n},{n * (n - 1) // 2}\n")

    def test_empty_stream_finishes_with_zero_rows(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        res = ins.finish()
        self.assertEqual(res.rows_written, 0)
        self.assertEqual(self._rows("SELECT count() FROM t", "CSV"), "0\n")

    def test_append_after_finish_raises(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins.append("1\n")
        ins.finish()
        with self.assertRaises(RuntimeError):
            ins.append("2\n")
        with self.assertRaises(RuntimeError):
            ins.finish()

    def test_malformed_row_surfaces_error(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        # "abc" is not a UInt64; the engine should reject it on append or finish.
        with self.assertRaises(RuntimeError):
            ins.append("abc\n")
            ins.finish()

    def test_with_block_without_finish_does_not_commit(self):
        # Leaving the context without finish() must cancel (no implicit commit).
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        with self.conn.send_insert("INSERT INTO t (a)", "CSV") as ins:
            ins.append("1\n2\n")
            # no finish()
        # Memory engine: a cancelled (unfinalized) insert commits no rows.
        self.assertEqual(self._rows("SELECT count() FROM t", "CSV"), "0\n")

    def test_explicit_cancel_is_idempotent(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins.append("1\n")
        ins.cancel()
        ins.cancel()  # no-op, must not raise

    def test_concurrent_statement_rejected_during_insert(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        try:
            ins.append("1\n")
            # A second query on the same connection while the insert is open
            # must be rejected (single active statement).
            with self.assertRaises(Exception):
                self.conn.query("SELECT 1")
        finally:
            ins.finish()

    def test_bad_insert_target_errors_at_init(self):
        with self.assertRaises(RuntimeError):
            self.conn.send_insert("INSERT INTO no_such_table (a)", "CSV")


class TestStreamingInsertSession(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(test_dir, ignore_errors=True)
        self.sess = session.Session(test_dir)

    def tearDown(self):
        self.sess.close()
        shutil.rmtree(test_dir, ignore_errors=True)

    def test_session_send_insert_roundtrip(self):
        self.sess.query("CREATE DATABASE IF NOT EXISTS db")
        self.sess.query("USE db")
        self.sess.query("CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
        with self.sess.send_insert("INSERT INTO db.t (a, b)", "CSV") as ins:
            ins.append("1,one\n2,two\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, 2)
        out = self.sess.query("SELECT a, b FROM db.t ORDER BY a", "CSV").data()
        self.assertEqual(out, "1,\"one\"\n2,\"two\"\n")


if __name__ == "__main__":
    unittest.main()
