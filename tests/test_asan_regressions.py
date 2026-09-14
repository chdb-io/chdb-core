"""Regressions for three of the four defects the ASan+UBSan build surfaced.

Each test fails on the unfixed engine:
  * reconnect            -> chassert(!background_context_instance) aborts the process
  * insert stream worker -> chassert(!current_thread) aborts the process
  * result buffer        -> AddressSanitizer: container-overflow (only under ASan)

The fourth, per-engine-start leak, is asserted in examples/chdbAsanRegressionTest.c.
"""

import unittest

import chdb


class TestEngineRestart(unittest.TestCase):
    """Second engine start in one process. Before the fix, the process-global
    background_context_instance was still set from the first start and
    Context::makeBackgroundContext() aborted on its chassert."""

    def test_second_connect_in_same_process_succeeds(self):
        for i in range(3):
            conn = chdb.connect(":memory:")
            cur = conn.cursor()
            cur.execute("SELECT 1 + %d" % i)
            self.assertEqual(cur.fetchall(), ((1 + i,),))
            cur.close()
            conn.close()


class TestInsertStreamWorker(unittest.TestCase):
    """The streaming INSERT worker runs on a global-pool thread that already owns a
    ThreadStatus; constructing a second one aborted on chassert(!current_thread)."""

    def test_streaming_insert_roundtrip(self):
        conn = chdb.connect(":memory:")
        cur = conn.cursor()
        cur.execute("CREATE TABLE asan_ins (a UInt64, b String) ENGINE = Memory")
        cur.execute("INSERT INTO asan_ins VALUES (1, 'one'), (2, 'two')")
        cur.execute("SELECT a, b FROM asan_ins ORDER BY a")
        self.assertEqual(cur.fetchall(), ((1, "one"), (2, "two")))
        cur.close()
        conn.close()


class TestResultBufferBounds(unittest.TestCase):
    """query_result.data() was bound as a bare char *, so pybind11 ran strlen() on a
    buffer that is exactly chdb_result_length() bytes and not NUL-terminated."""

    def test_data_matches_str_and_len(self):
        res = chdb.query("SELECT 'abc' AS c", "CSV")
        self.assertEqual(res.data(), str(res))
        self.assertEqual(len(res.bytes()), len(res))

    def test_data_keeps_every_byte_of_a_binary_safe_format(self):
        # A row whose output contains no trailing newline sentinel to hide behind.
        res = chdb.query("SELECT 1", "CSV")
        self.assertEqual(res.data().encode(), res.bytes())


# The per-engine-start leak (examples/chdbAsanRegressionTest.c) is asserted from C, not
# here: AddressSanitizer's counter is process-wide, and in a Python process CPython's own
# per-cycle allocation swamps the ~40 KiB the fix removes.


if __name__ == "__main__":
    unittest.main(verbosity=2)
