#!python3

# pandas and pyarrow are runtime-optional: `import chdb` and every non-Arrow,
# non-DataFrame query path must work when neither is installed (e.g.
# `pip install chdb-core --no-deps`). Each case runs in a subprocess with an
# import hook that simulates the missing packages.

import os
import subprocess
import sys
import unittest

import chdb

# Directory containing the `chdb` package under test, so subprocesses
# exercise the same code as this test process.
_CHDB_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(chdb.__file__)))

_BLOCKER_PRELUDE = """
import sys

class _Blocker:
    def __init__(self, blocked):
        self.blocked = blocked

    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in self.blocked:
            raise ModuleNotFoundError(
                f"No module named {{name!r}} (blocked by test)", name=name
            )

sys.meta_path.insert(0, _Blocker({blocked!r}))
sys.path.insert(0, {chdb_parent!r})
"""

_BODY_QUERY_PATHS_WORK = """
import chdb

res = chdb.query("SELECT 1 as a, 'x' as b", "CSV")
assert res.bytes() == b'1,"x"\\n', res.bytes()

res = chdb.query("SELECT number FROM numbers(3)", "JSON")
assert '"number"' in str(res), str(res)

# Raw Arrow bytes are produced by the engine and need no pyarrow.
res = chdb.query("SELECT 1 as a", "Arrow")
assert len(res.bytes()) > 0

conn = chdb.connect(":memory:")
assert "42" in str(conn.query("SELECT 42", "CSV"))

cur = conn.cursor()
cur.execute("SELECT 1 as v, 'hello' as s")
rows = cur.fetchall()
assert rows == ((1, "hello"),), rows

chunks = list(conn.send_query("SELECT number FROM numbers(10)", "CSV"))
assert len(chunks) > 0
conn.close()

from chdb import session
s = session.Session()
assert "7" in str(s.query("SELECT 7", "CSV"))
s.close()

print("QUERY_PATHS_OK")
"""

_BODY_ARROW_DF_RAISE = """
import chdb

conn = chdb.connect(":memory:")
stream = conn.send_query("SELECT 1", "Arrow")
try:
    stream.record_batch()
except ImportError as e:
    assert "pyarrow" in str(e), e
else:
    raise AssertionError("record_batch() should require pyarrow")
finally:
    stream.close()

try:
    conn.query("SELECT 1", "ArrowTable")
except ImportError:
    pass
else:
    raise AssertionError("ArrowTable output should require pyarrow")

# send_query must fail fast too, not on the first fetch()
try:
    conn.send_query("SELECT 1", "ArrowTable")
except ImportError as e:
    assert "pyarrow" in str(e), e
else:
    raise AssertionError("ArrowTable streaming should require pyarrow")
conn.close()

try:
    chdb.query("SELECT 1", "ArrowTable")
except ImportError:
    pass
else:
    raise AssertionError("ArrowTable output should require pyarrow")

try:
    chdb.query("SELECT 1", "DataFrame")
except Exception as e:
    assert "pandas" in str(e).lower(), e
else:
    raise AssertionError("DataFrame output should require pandas")

print("ERRORS_INFORMATIVE_OK")
"""

_BODY_PANDAS_ONLY_MISSING = """
import chdb

assert chdb.query("SELECT 1", "CSV").bytes() == b"1\\n"

try:
    chdb.query("SELECT 1", "DataFrame")
except Exception as e:
    assert "pandas" in str(e).lower(), e
else:
    raise AssertionError("DataFrame output should require pandas")

print("PANDAS_ONLY_OK")
"""

_BODY_PYTHON_TABLE_FUNCTION = """
import chdb

# Object discovery for Python() must not be derailed by the pandas/pyarrow
# type probes failing to import them: a plain dict is still queryable.
data = {"a": [1, 2, 3], "b": ["x", "y", "z"]}
res = chdb.query("SELECT b, a * 2 AS a2 FROM Python(data) ORDER BY a", "CSV")
assert res.bytes() == b'"x",2\\n"y",4\\n"z",6\\n', res.bytes()

class Reader(chdb.PyReader):
    def __init__(self, data):
        self.data = data
        self.cursor = 0
        super().__init__(data)

    def get_schema(self):
        return [("a", "int"), ("b", "str")]

    def read(self, col_names, count):
        if self.cursor >= 3:
            return []
        block = [self.data[c][self.cursor:self.cursor + count] for c in col_names]
        self.cursor += count
        return block

reader = Reader(data)
assert chdb.query("SELECT count() FROM Python(reader)", "CSV").bytes() == b"3\\n"

print("PYTHON_TABLE_OK")
"""


class TestQueryWithoutPandasPyarrow(unittest.TestCase):
    def _run_blocked(self, blocked, body):
        script = (
            _BLOCKER_PRELUDE.format(blocked=blocked, chdb_parent=_CHDB_PARENT_DIR)
            + body
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=300,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"subprocess failed\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )
        return proc.stdout

    def test_import_and_text_query_paths_work_without_pandas_and_pyarrow(self):
        out = self._run_blocked(("pandas", "pyarrow"), _BODY_QUERY_PATHS_WORK)
        self.assertIn("QUERY_PATHS_OK", out)

    def test_arrow_and_dataframe_outputs_raise_informative_errors(self):
        out = self._run_blocked(("pandas", "pyarrow"), _BODY_ARROW_DF_RAISE)
        self.assertIn("ERRORS_INFORMATIVE_OK", out)

    def test_query_works_with_only_pandas_missing(self):
        out = self._run_blocked(("pandas",), _BODY_PANDAS_ONLY_MISSING)
        self.assertIn("PANDAS_ONLY_OK", out)

    def test_python_table_function_works_without_pandas_and_pyarrow(self):
        out = self._run_blocked(
            ("pandas", "pyarrow", "numpy"), _BODY_PYTHON_TABLE_FUNCTION
        )
        self.assertIn("PYTHON_TABLE_OK", out)


if __name__ == "__main__":
    unittest.main()
