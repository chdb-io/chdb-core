#!python3
"""Regression guard: chdb queries must not leak log noise to stdout/stderr.

History: upstream ClickHouse-local prints diagnostics (e.g. "Elapsed: N sec." via
print-time-to-stderr, ba515bfbadf2) and exception text (3d55a9203ee4 / b060193542f8)
that, after a baseline upgrade, can resurface and pollute the embedded library's
stdout/stderr. The only thing a chdb query may write to stdout is its formatted
result; stderr must stay empty.

Each scenario runs in a *fresh* subprocess so we capture exactly what the native
library writes to fd1/fd2 — not what pytest/unittest happens to intercept.
"""

import os
import subprocess
import sys
import tempfile
import unittest

# Substrings that, if they ever appear on either stream, mean diagnostic noise
# leaked out of the engine. Kept explicit so a failure points at the exact regression.
NOISE_MARKERS = (
    "Elapsed:",            # print-time-to-stderr (ba515bfbadf2)
    "Processed ",          # progress line
    "rows in set",         # interactive summary
    "<Information>",       # log level markers
    "<Debug>",
    "<Trace>",
    "<Warning>",
    "<Error>",
    "Code: ",              # raw exception dump (3d55a9203ee4)
    "DB::Exception",
)


def _run(body: str):
    """Run `body` in a clean subprocess; body must write ONLY the result to stdout."""
    code = "import sys\n" + body
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
    )


class TestStdoutNoise(unittest.TestCase):
    def _assert_clean(self, proc, expected_stdout, scenario):
        self.assertEqual(
            proc.returncode, 0,
            f"[{scenario}] non-zero exit; stderr={proc.stderr!r}",
        )
        self.assertEqual(
            proc.stderr, "",
            f"[{scenario}] stderr must be empty, got {proc.stderr!r}",
        )
        self.assertEqual(
            proc.stdout, expected_stdout,
            f"[{scenario}] stdout must be exactly the query result",
        )
        for marker in NOISE_MARKERS:
            self.assertNotIn(
                marker, proc.stdout,
                f"[{scenario}] noise marker {marker!r} leaked to stdout",
            )

    def test_simple_select_emits_only_result(self):
        proc = _run("import chdb; sys.stdout.write(str(chdb.query('SELECT 1 AS a', 'CSV')))")
        self._assert_clean(proc, "1\n", "simple_select")

    def test_numbers_select_emits_only_rows(self):
        proc = _run("import chdb; sys.stdout.write(str(chdb.query('SELECT number FROM numbers(3)', 'CSV')))")
        self._assert_clean(proc, "0\n1\n2\n", "numbers_select")

    def test_session_query_emits_only_result(self):
        proc = _run(
            "from chdb import session as s\n"
            "sess = s.Session()\n"
            "sys.stdout.write(str(sess.query('SELECT 42', 'CSV')))\n"
        )
        self._assert_clean(proc, "42\n", "session_query")

    def test_create_then_select_no_progress_noise(self):
        proc = _run(
            "import chdb\n"
            "chdb.query('CREATE DATABASE IF NOT EXISTS noise_probe')\n"
            "sys.stdout.write(str(chdb.query('SELECT 7', 'CSV')))\n"
        )
        self._assert_clean(proc, "7\n", "create_then_select")

    def test_aggregate_over_million_rows_no_elapsed_print(self):
        # Larger scan is where an "Elapsed:"/progress regression would surface.
        proc = _run("import chdb; sys.stdout.write(str(chdb.query('SELECT sum(number) FROM numbers(1000000)', 'CSV')))")
        self._assert_clean(proc, "499999500000\n", "aggregate_million")

    def test_error_query_does_not_dump_exception_to_streams(self):
        # The Python binding must raise (caught here); the native layer must NOT
        # print the exception text to stdout/stderr.
        proc = _run(
            "import chdb\n"
            "try:\n"
            "    chdb.query('SELECT nonexistent_func_xyz(1)', 'CSV')\n"
            "    sys.stdout.write('NO_RAISE')\n"
            "except Exception:\n"
            "    sys.stdout.write('CAUGHT')\n"
        )
        self._assert_clean(proc, "CAUGHT", "error_query")

    def test_file_implicit_table_query_no_noise(self):
        # The file()/implicit-table path historically hit the initial-query
        # print-time code path (ba515bfbadf2).
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            f.write("a,b\n1,x\n2,y\n3,z\n")
            csv_path = f.name
        try:
            proc = _run(
                f"import chdb\n"
                f"sys.stdout.write(str(chdb.query(\"SELECT count() FROM file('{csv_path}', 'CSVWithNames')\", 'CSV')))\n"
            )
            self._assert_clean(proc, "3\n", "file_query")
        finally:
            os.unlink(csv_path)


if __name__ == "__main__":
    unittest.main()
