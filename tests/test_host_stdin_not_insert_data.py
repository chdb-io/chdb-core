#!python3
"""The host process's stdin must never become INSERT data (chdb-io/chdb-core#239).

ClientBase defaults its `std_in` to fd 0, which is correct for clickhouse-local as a
CLI but not for a library: fd 0 belongs to the embedding application. Left bound, the
same SQL succeeds or fails depending on how the host was started, and with
async_insert=0 the host's bytes silently land in the table as rows.

Every case here runs chdb in a child process whose stdin is a pipe holding data, which
is what a shell pipeline into the embedding application looks like.
"""

import json
import subprocess
import sys
import unittest

MARKER = "@@RESULT@@"

PRELUDE = f"""
import json, sys
from chdb.state.sqlitelike import connect

def emit(obj):
    sys.stdout.write("{MARKER}" + json.dumps(obj) + "\\n")

conn = connect(":memory:")
"""


def run_child(body, stdin_bytes, timeout=120):
    """Run `body` in a child chdb process fed `stdin_bytes` on fd 0, return its emit()."""
    proc = subprocess.run(
        [sys.executable, "-c", PRELUDE + body],
        input=stdin_bytes,
        capture_output=True,
        timeout=timeout,
    )
    stdout = proc.stdout.decode()
    if proc.returncode != 0:
        raise AssertionError(
            f"child exited {proc.returncode}\n"
            f"--- stdout ---\n{stdout}\n--- stderr ---\n{proc.stderr.decode()}"
        )
    lines = [ln for ln in stdout.splitlines() if ln.startswith(MARKER)]
    if not lines:
        raise AssertionError(f"child emitted no result\n--- stdout ---\n{stdout}")
    return json.loads(lines[-1][len(MARKER):])


class TestHostStdinIsNotInsertData(unittest.TestCase):
    def test_values_insert_succeeds_when_host_stdin_holds_data(self):
        """`INSERT ... VALUES` must not be refused as "inlined and external data"."""
        body = """
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
try:
    conn.query("INSERT INTO t VALUES (1), (2)")
    emit({"error": None})
except Exception as e:
    emit({"error": str(e)})
"""
        self.assertIsNone(run_child(body, b"(9999)\n")["error"])

    def test_values_insert_stores_only_inline_rows_with_async_insert(self):
        body = """
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
conn.query("INSERT INTO t VALUES (1), (2)")
emit(str(conn.query("SELECT a FROM t ORDER BY a", "CSV")).split())
"""
        self.assertEqual(run_child(body, b"(9999)\n"), ["1", "2"])

    def test_values_insert_stores_only_inline_rows_without_async_insert(self):
        """async_insert=0 is the path that silently ingested the host's bytes."""
        body = """
conn.query("SET async_insert = 0")
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
conn.query("INSERT INTO t VALUES (1), (2)")
emit(str(conn.query("SELECT a FROM t ORDER BY a", "CSV")).split())
"""
        self.assertEqual(run_child(body, b"(9999)\n"), ["1", "2"])

    def test_insert_without_inline_data_is_refused_not_read_from_host_stdin(self):
        """An INSERT carrying no data must be refused, never satisfied from fd 0.

        chdb short-circuits this case in ClientBase (`throw_if_no_data_to_insert`)
        before the stdin path, so it already held; assert it so a future change
        there cannot quietly turn the host's stdin back into the data source.
        """
        body = """
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
try:
    conn.query("INSERT INTO t FORMAT CSV")
    err = None
except Exception as e:
    err = str(e)
emit([err, str(conn.query("SELECT count() FROM t", "CSV")).strip()])
"""
        err, count = run_child(body, b"9999\n8888\n")
        self.assertIn("NO_DATA_TO_INSERT", err)
        self.assertEqual(count, "0")

    def test_input_table_function_reads_nothing_from_host_stdin(self):
        """connect() wires the same buffer to input(), which reads it directly.

        This is a second, independent route to the phantom rows: the query's own
        inline `1111` is ignored and the host's `7777` is stored in its place.
        """
        body = """
conn.query("SET async_insert = 0")
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
conn.query("INSERT INTO t SELECT a FROM input('a UInt32') FORMAT CSV\\n1111\\n")
emit(str(conn.query("SELECT a FROM t ORDER BY a", "CSV")).split())
"""
        self.assertEqual(run_child(body, b"7777\n"), [])

    def test_host_keeps_its_stdin_unread_after_chdb_inserts(self):
        """chdb must leave fd 0 untouched, so the application can still read it."""
        body = """
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
conn.query("INSERT INTO t VALUES (1), (2)")
emit(sys.stdin.read())
"""
        self.assertEqual(run_child(body, b"host owns this\n"), "host owns this\n")

    def test_query_results_are_identical_with_and_without_data_on_host_stdin(self):
        """Same SQL, same engine: the host's stdin must make no difference."""
        body = """
conn.query("CREATE TABLE t (a UInt32) ENGINE = Memory")
conn.query("INSERT INTO t VALUES (1), (2)")
emit(str(conn.query("SELECT a FROM t ORDER BY a", "CSV")).split())
"""
        with_data = run_child(body, b"(9999)\n")
        without_data = run_child(body, b"")
        self.assertEqual(with_data, without_data)
        self.assertEqual(with_data, ["1", "2"])


if __name__ == "__main__":
    unittest.main()
