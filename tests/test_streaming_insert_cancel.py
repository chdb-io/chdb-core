#!python3
"""Cancel semantics for streaming INSERT.

Cancel follows ClickHouse defaults (no special rollback). These tests assert the
deterministic, sink-independent invariants: a cancelled (unfinalized) insert
commits no rows for single-block targets, and the connection remains usable
afterwards (resources are released, no leaked active statement).
"""

import shutil
import unittest

import chdb

test_dir = ".tmp_test_streaming_insert_cancel"


class TestStreamingInsertCancel(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(test_dir, ignore_errors=True)
        self.conn = chdb.connect(":memory:")

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(test_dir, ignore_errors=True)

    def test_cancel_unfinalized_memory_commits_nothing(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins.append("1\n2\n3\n")
        ins.cancel()
        # Single block, never finalized => nothing committed.
        self.assertEqual(self.conn.query("SELECT count() FROM t", "CSV").data(), "0\n")

    def test_connection_reusable_after_cancel(self):
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins.append("1\n")
        ins.cancel()

        # A normal query works after cancel.
        self.assertEqual(self.conn.query("SELECT 42", "CSV").data(), "42\n")

        # And a fresh streaming insert succeeds and commits correctly.
        with self.conn.send_insert("INSERT INTO t (a)", "CSV") as ins2:
            ins2.append("10\n20\n")
            res = ins2.finish()
        self.assertEqual(res.rows_written, 2)
        self.assertEqual(
            self.conn.query("SELECT a FROM t ORDER BY a", "CSV").data(), "10\n20\n"
        )

    def test_new_insert_allowed_after_cancel(self):
        # The single-active-statement lock must be released by cancel().
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = Memory")
        ins = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins.cancel()
        # No "already active" error here.
        ins2 = self.conn.send_insert("INSERT INTO t (a)", "CSV")
        ins2.append("7\n")
        res = ins2.finish()
        self.assertEqual(res.rows_written, 1)
        self.assertEqual(self.conn.query("SELECT a FROM t", "CSV").data(), "7\n")


if __name__ == "__main__":
    unittest.main()
