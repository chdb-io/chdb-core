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

    def test_append_rejects_invalid_type(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        # An int must fail fast, not get silently turned into NUL bytes (bytes(5)).
        with self.assertRaises(TypeError):
            ins.append(5)
        ins.cancel()
        self.assertEqual(self._rows("SELECT count() FROM t", "CSV"), "0\n")

    def test_many_chunks_constant_stream(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
        n = 20000
        with self.conn.send_insert("INSERT INTO t (a)", "CSV") as ins:
            for i in range(n):
                ins.append(f"{i}\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, n)
        self.assertEqual(self._rows("SELECT count(), sum(a) FROM t", "CSV"),
                         f"{n},{n * (n - 1) // 2}\n")

    def test_multi_block_streaming_creates_multiple_parts(self):
        # Prove TRUE streaming (not one coalesced write): shrink the insert
        # block size so the worker pushes multiple blocks through sendData as
        # appends arrive; each block lands as a separate MergeTree part. With
        # default settings (~1M-row blocks) this data would be a single part.
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
        n = 50000
        target = (
            "INSERT INTO t (a) SETTINGS max_insert_block_size=10000, "
            "min_insert_block_size_rows=10000, min_insert_block_size_bytes=0"
        )
        with self.conn.send_insert(target, "CSV") as ins:
            for i in range(n):
                ins.append(f"{i}\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, n)
        parts = int(
            self.conn.query(
                "SELECT count() FROM system.parts WHERE table='t' AND active", "CSV"
            ).data().strip()
        )
        self.assertGreaterEqual(parts, 2, "expected multiple parts => multiple streamed blocks")
        self.assertEqual(
            self._rows("SELECT count(), sum(a) FROM t", "CSV"),
            f"{n},{n * (n - 1) // 2}\n",
        )

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
        # "abc" is not a UInt64. Parsing happens asynchronously on the worker, so
        # the error may surface at append() (if already processed) or at finish();
        # the contract is that it surfaces no later than finish() and nothing is
        # committed. Assert on the outcome deterministically rather than on which
        # stage raises.
        raised = None
        try:
            ins.append("abc\n")
            ins.finish()
        except RuntimeError as e:
            raised = e
        self.assertIsNotNone(raised, "a malformed row must raise by finish()")
        self.assertEqual(self._rows("SELECT count() FROM t", "CSV"), "0\n")

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
            # And so must a second insert stream.
            with self.assertRaises(Exception):
                self.conn.send_insert("INSERT INTO t (a)", "CSV")
        finally:
            ins.finish()

    def test_bad_insert_target_errors_at_init(self):
        with self.assertRaises(RuntimeError):
            self.conn.send_insert("INSERT INTO no_such_table (a)", "CSV")

    def test_format_mismatch_payload_errors(self):
        # Declare CSV but push a JSONEachRow payload: the parser must reject
        # it no later than finish(), and nothing may be committed.
        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a, b)", "CSV")
        raised = None
        try:
            ins.append('{"a":1,"b":"one"}\n')
            ins.finish()
        except RuntimeError as e:
            raised = e
        self.assertIsNotNone(raised, "format-mismatch payload must raise by finish()")
        self.assertEqual(self._rows("SELECT count() FROM t", "CSV"), "0\n")

    def test_multi_block_streaming_into_memory_engine(self):
        # Same small-block settings as the MergeTree parts test, but against a
        # Memory sink: integrity of many incrementally pushed blocks into a
        # non-MergeTree target (no parts to count, so assert exact count/sum).
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        n = 50000
        target = (
            "INSERT INTO t (a) SETTINGS max_insert_block_size=10000, "
            "min_insert_block_size_rows=10000, min_insert_block_size_bytes=0"
        )
        with self.conn.send_insert(target, "CSV") as ins:
            for i in range(n):
                ins.append(f"{i}\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, n)
        self.assertEqual(
            self._rows("SELECT count(), sum(a) FROM t", "CSV"),
            f"{n},{n * (n - 1) // 2}\n",
        )

    def test_close_connection_with_active_insert(self):
        # Closing the connection while an insert stream is open must tear the
        # stream down safely: the stale inserter degrades to errors (no crash,
        # neither on use nor when it is garbage-collected) and a fresh
        # connection works.
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins.append("1\n")
        self.conn.close()

        with self.assertRaises(RuntimeError):
            ins.append("2\n")
        with self.assertRaises(RuntimeError):
            ins.finish()
        del ins  # GC of the stale handle must be safe

        # Fresh connection works (also keeps tearDown's close() valid).
        self.conn = chdb.connect(":memory:")
        self.assertEqual(self.conn.query("SELECT 1", "CSV").data(), "1\n")


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
