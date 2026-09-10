#!/usr/bin/env python3

import subprocess
import sys
import unittest

import chdb


# Child program: open a streaming read, consume one chunk, then run an ordinary
# query on the same connection and exit WITHOUT draining or cancelling the
# stream. Before the fix, the interleaved query rebuilt the connection's query
# state out from under the still-active streaming context, so the cleanup at
# interpreter exit cancelled the stream against a released state, tripped a
# libc++ hardening assertion, and hung in the crash handler.
_CHILD = """
import chdb
conn = chdb.connect(":memory:")
stream = conn.send_query("SELECT number FROM numbers(3000000)", "CSV")
assert stream.fetch() is not None
assert conn.query("SELECT 42 AS x", "CSV").bytes() == b"42\\n"
print("CHILD_DONE", flush=True)
"""


class TestStreamThenQuery(unittest.TestCase):
    def test_query_interleaved_with_active_stream_returns_correct_data(self):
        conn = chdb.connect(":memory:")
        try:
            stream = conn.send_query("SELECT number FROM numbers(3000000)", "CSV")
            self.assertIsNotNone(stream.fetch())

            # A materialized query on the same connection must run correctly,
            # not return the abandoned stream's dirty/empty output buffer.
            self.assertEqual(conn.query("SELECT 42 AS x", "CSV").bytes(), b"42\n")
            self.assertEqual(
                conn.query("SELECT count() FROM numbers(10)", "CSV").bytes(), b"10\n"
            )

            # The abandoned stream has been retired: fetching again reports it
            # cleanly instead of crashing.
            with self.assertRaises(RuntimeError):
                stream.fetch()
        finally:
            conn.close()

    def test_process_exits_cleanly_with_undrained_stream_and_interleaved_query(self):
        # Runs the sequence in a child that never drains or cancels the stream
        # and relies on interpreter-exit cleanup. Before the fix this hung in
        # the crash handler; assert it exits promptly and cleanly.
        try:
            proc = subprocess.run(
                [sys.executable, "-c", _CHILD],
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            self.fail(
                "process hung at exit with an undrained stream and an "
                "interleaved query on the same connection"
            )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("CHILD_DONE", proc.stdout)
        self.assertNotIn("Hardening assertion", proc.stderr)


if __name__ == "__main__":
    unittest.main()
