#!python3

import subprocess
import sys
import textwrap
import unittest


UDF_CLOSE = """
    import chdb

    conn = chdb._chdb.connect(":memory:")
    idle = chdb._chdb.connect(":memory:")

    def close_in_callback(value):
        # An unrelated idle connection is safe to close from the callback.
        idle.close()
        try:
            conn.close()
        except RuntimeError as exc:
            assert "active operations from a query callback" in str(exc), str(exc)
        else:
            raise AssertionError("closing the active connection must be rejected")
        return value

    chdb.create_function(
        "close_in_callback_issue254", close_in_callback,
        arg_types=["UInt64"], return_type="UInt64",
    )
    assert conn.query(
        "SELECT close_in_callback_issue254(number) FROM numbers(1)", "CSV"
    ).bytes() == b"0\\n"
    assert conn.query("SELECT 1", "CSV").bytes() == b"1\\n"
    conn.close()
"""


READER_CLOSE = """
    import chdb

    conn = chdb._chdb.connect(":memory:")

    class ClosingReader(chdb.PyReader):
        def __init__(self):
            self.read_count = 0
            super().__init__({"x": [42]})

        def read(self, col_names, count):
            self.read_count += 1
            if self.read_count > 1:
                return []
            try:
                conn.close()
            except RuntimeError as exc:
                assert "active operations from a query callback" in str(exc), str(exc)
            else:
                raise AssertionError("closing the active connection must be rejected")
            return [[42]]

    reader = ClosingReader()
    assert conn.query("SELECT x FROM Python(reader)", "CSV").bytes() == b"42\\n"
    assert conn.query("SELECT 1", "CSV").bytes() == b"1\\n"
    conn.close()
"""

HIGH_LEVEL_CLOSE = """
    import chdb

    conn = chdb.connect(":memory:")

    def close_in_callback(value):
        # The high-level wrapper deliberately swallows native close errors.
        # Its contract here is that the in-flight query must not deadlock.
        conn.close()
        return value

    chdb.create_function(
        "close_high_level_issue254", close_in_callback,
        arg_types=["UInt64"], return_type="UInt64",
    )
    assert conn.query(
        "SELECT close_high_level_issue254(number) FROM numbers(1)", "CSV"
    ).bytes() == b"0\\n"
"""

OTHER_THREAD_CLOSE = """
    import chdb
    import threading
    import time

    conn = chdb._chdb.connect(":memory:")
    errors = []

    def query_on_worker():
        try:
            conn.query("SELECT sleep(0.5)", "CSV")
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=query_on_worker)
    worker.start()
    deadline = time.monotonic() + 3
    while chdb._chdb._python_active_operations() == 0 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert chdb._chdb._python_active_operations() > 0
    conn.close()  # An ordinary thread still waits for the operation to finish.
    worker.join(2)
    assert not worker.is_alive()
    assert not errors, errors
"""


class TestConnectionCallbackClose(unittest.TestCase):
    def run_child(self, source):
        try:
            result = subprocess.run(
                [sys.executable, "-c", textwrap.dedent(source)],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            self.fail("closing a connection from a query callback deadlocked")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_udf_rejects_close_of_own_active_connection(self):
        self.run_child(UDF_CLOSE)

    def test_python_reader_rejects_close_of_own_active_connection(self):
        self.run_child(READER_CLOSE)

    def test_high_level_close_does_not_deadlock_query(self):
        self.run_child(HIGH_LEVEL_CLOSE)

    def test_other_thread_can_wait_to_close_active_connection(self):
        self.run_child(OTHER_THREAD_CLOSE)


if __name__ == "__main__":
    unittest.main()
