#!/usr/bin/env python3

import gc
import unittest
import weakref

import chdb
from chdb import session


class TestSessionChdbError(unittest.TestCase):
    """The connection/session APIs must raise ChdbError, like chdb.query does."""

    def setUp(self) -> None:
        self.sess = session.Session()
        return super().setUp()

    def tearDown(self) -> None:
        self.sess.close()
        return super().tearDown()

    def test_chdb_error_is_a_runtime_error(self):
        # backward compatibility: callers catching RuntimeError keep working
        self.assertTrue(issubclass(chdb.ChdbError, RuntimeError))

    def test_session_query_raises_chdb_error(self):
        with self.assertRaises(chdb.ChdbError):
            self.sess.query("SELECT * FROM nonexistent_table_xyz")

    def test_session_query_error_message_preserved(self):
        with self.assertRaises(chdb.ChdbError) as ctx:
            self.sess.query("SELECT * FROM nonexistent_table_xyz")
        self.assertIn("nonexistent_table_xyz", str(ctx.exception))

    def test_connection_send_query_raises_chdb_error(self):
        with self.assertRaises(chdb.ChdbError):
            self.sess.send_query("SELECT bad syntax FROM", "CSV")

    def test_stateless_query_still_raises_chdb_error(self):
        with self.assertRaises(chdb.ChdbError):
            chdb.query("SELECT * FROM nonexistent_table_xyz")

    def test_kept_error_does_not_keep_session_alive(self):
        # unittest.assertRaises keeps the exception without its traceback; the
        # chained engine error must not pin the session through its own one
        sess = session.Session()
        ref = weakref.ref(sess)
        gc.disable()
        try:
            try:
                sess.query("SELECT * FROM nonexistent_table_xyz")
            except chdb.ChdbError as e:
                kept = e.with_traceback(None)
            del sess
            self.assertIsNone(ref(), "session pinned by the kept ChdbError")
            self.assertIsInstance(kept, chdb.ChdbError)
        finally:
            gc.enable()


if __name__ == "__main__":
    unittest.main()
