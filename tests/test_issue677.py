#!python3

import os
import subprocess
import sys
import textwrap
import unittest

# Each case runs in its own process: the working directory is process-wide, and the engine has to start from
# inside the removed directory (or a directory it then leaves) to exercise the problem.


def _run(body):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(body)],
        env=env, capture_output=True, text=True, timeout=300,
    )


_REMOVED_CWD = """
import os, tempfile
data_dir = tempfile.mkdtemp()
with open(os.path.join(data_dir, "x.csv"), "w") as f:
    f.write("42\\n")
csv = os.path.join(data_dir, "x.csv")
gone = tempfile.mkdtemp()
os.chdir(gone)
os.rmdir(gone)

import chdb
"""


class TestIssue677(unittest.TestCase):
    """chDB works in a process whose working directory has been removed, except for relative file paths."""

    def _check(self, body):
        proc = _run(_REMOVED_CWD + textwrap.dedent(body))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout, proc.stdout + proc.stderr)

    def test_engine_starts_and_queries(self):
        self._check("""
            assert chdb.query("SELECT 1", "CSV").bytes() == b"1\\n"
            conn = chdb.connect(":memory:")
            assert conn.query("SELECT 2", "CSV").bytes() == b"2\\n"
            conn.close()
            print("OK")
        """)

    def test_absolute_file_paths(self):
        self._check("""
            assert chdb.query(f"SELECT * FROM file('{csv}', CSV)", "CSV").bytes() == b"42\\n"
            assert chdb.query(f"SELECT * FROM file('{data_dir}/*.csv', CSV)", "CSV").bytes() == b"42\\n"
            assert chdb.query(f"SELECT * FROM '{csv}'", "CSV").bytes() == b"42\\n"
            print("OK")
        """)

    def test_relative_file_path_names_the_problem(self):
        self._check("""
            try:
                chdb.query("SELECT * FROM file('x.csv', CSV)", "CSV")
            except Exception as e:
                message = str(e)
            else:
                raise AssertionError("query succeeded")
            assert "x.csv" in message and "working directory is not accessible" in message, message
            print("OK")
        """)

    def test_persistent_connection_and_session(self):
        self._check("""
            from chdb import session
            db = os.path.join(data_dir, "db")
            conn = chdb.connect(db)
            conn.query("CREATE TABLE t (a Int32) ENGINE = MergeTree ORDER BY a")
            conn.query("INSERT INTO t VALUES (7)")
            assert conn.query("SELECT a FROM t", "CSV").bytes() == b"7\\n"
            conn.close()
            sess = session.Session(os.path.join(data_dir, "sess"))
            assert sess.query("SELECT 3", "CSV").bytes() == b"3\\n"
            sess.close()
            print("OK")
        """)

    def test_relative_connection_path_names_the_problem(self):
        self._check("""
            try:
                chdb.connect("reldb")
            except Exception as e:
                message = str(e)
            else:
                raise AssertionError("connected")
            assert "'reldb'" in message, message
            print("OK")
        """)

    def test_absolute_udf_path(self):
        self._check("""
            conn = chdb.connect(f":memory:?udf_path={data_dir}")
            assert conn.query("SELECT 5", "CSV").bytes() == b"5\\n"
            conn.close()
            print("OK")
        """)

    def test_connection_error_keeps_the_reason(self):
        # A Poco exception used to reach Python as just "System exception" or the class name.
        proc = _run("""
            import os, tempfile
            bad = os.path.join(tempfile.mkdtemp(), "bad.xml")
            with open(bad, "w") as f:
                f.write("<clickhouse><a></b></clickhouse>")
            import chdb
            try:
                chdb.connect(f":memory:?config-file={bad}")
            except Exception as e:
                message = str(e)
            else:
                raise AssertionError("connected")
            assert "bad.xml" in message, message
            print("OK")
        """)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout, proc.stdout + proc.stderr)

    def test_relative_names_follow_the_current_working_directory(self):
        # The engine starts in one directory; after a chdir, a table name and file() both read the new one.
        proc = _run("""
            import os, tempfile
            first, second = tempfile.mkdtemp(), tempfile.mkdtemp()
            for directory, value in ((first, 1), (second, 2)):
                with open(os.path.join(directory, "x.csv"), "w") as f:
                    f.write(f"{value}\\n")
            os.chdir(first)
            import chdb
            conn = chdb.connect(":memory:")
            assert conn.query("SELECT * FROM 'x.csv'", "CSV").bytes() == b"1\\n"
            os.chdir(second)
            assert conn.query("SELECT * FROM 'x.csv'", "CSV").bytes() == b"2\\n"
            assert conn.query("SELECT * FROM file('x.csv', CSV)", "CSV").bytes() == b"2\\n"
            conn.close()
            print("OK")
        """)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main()
