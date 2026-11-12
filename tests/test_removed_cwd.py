#!python3

"""chDB must start when the process working directory has been removed.

Long-running processes and test runners can outlive their working directory.
Poco's Application asked for it while being constructed, so every engine start
failed with a bare "Connection failed: System exception". Nothing about an
in-memory engine needs it. Each case runs in a subprocess whose working
directory is removed before the engine starts: before `import chdb`, whose
probe connection starts it, or between that and the next start.

The failure was hard to place because a connection reported a Poco exception
by its class name alone; it now carries the message.
"""

import os
import subprocess
import sys
import tempfile
import unittest

_REMOVE_CWD = """
import os
import tempfile
gone = tempfile.mkdtemp()
os.chdir(gone)
os.rmdir(gone)
"""

# A file named by an absolute path needs no working directory either, through
# file() or the default database's file overlay.
_CONNECTION = """
import shutil
r = chdb.query("SELECT 40 + 2", "CSV")
assert r.bytes().strip() == b"42", r.bytes()
data = tempfile.mkdtemp()
csv = os.path.join(data, "x.csv")
with open(csv, "w") as f:
    f.write("a,b\\n1,2\\n")
conn = chdb.connect(":memory:")
assert conn.query("SELECT 'ok'", "CSV").bytes().strip() == b'"ok"'
assert conn.query(f"SELECT b FROM file('{csv}')", "CSV").bytes().strip() == b"2"
assert conn.query(f"SELECT b FROM '{csv}'", "CSV").bytes().strip() == b"2"
conn.close()
shutil.rmtree(data)
print("QUERY_OK")
"""

# chdb._chdb.query() boots a one-shot LocalServer, which goes through
# Application::init() rather than EmbeddedServer.
_ONE_SHOT = """
from chdb import _chdb
r = _chdb.query("SELECT 40 + 2", "CSV")
assert not r.has_error(), r.error_message()
assert r.bytes().strip() == b"42", r.bytes()
print("QUERY_OK")
"""

# A Poco (not DB::) exception during engine startup: the XML parser's.
_BAD_CONFIG = """
import sys
import chdb
try:
    chdb.connect(":memory:?config-file=" + sys.argv[1])
except Exception as e:
    print("CONNECT_FAILED", e)
"""


def _run(code, *args):
    env = dict(os.environ)
    # The child has no working directory to resolve relative entries against.
    env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(p) for p in sys.path if p)
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


class TestRemovedWorkingDirectory(unittest.TestCase):
    def _check(self, code):
        proc = _run(code)
        self.assertEqual(proc.returncode, 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
        self.assertIn("QUERY_OK", proc.stdout)

    def test_import_after_cwd_removed(self):
        self._check(_REMOVE_CWD + "import chdb\n" + _CONNECTION)

    def test_connect_after_cwd_removed(self):
        self._check("import chdb\n" + _REMOVE_CWD + _CONNECTION)

    def test_one_shot_query_after_cwd_removed(self):
        self._check("import chdb\n" + _REMOVE_CWD + _ONE_SHOT)


class TestPocoExceptionMessage(unittest.TestCase):
    def test_connect_reports_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = os.path.join(tmp, "bad.xml")
            with open(config, "w") as f:
                f.write("<clickhouse><bad></clickhouse>")
            proc = _run(_BAD_CONFIG, config)
        self.assertEqual(proc.returncode, 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
        self.assertIn("CONNECT_FAILED", proc.stdout)
        self.assertIn("Tag mismatch", proc.stdout)
        self.assertIn(config, proc.stdout)


if __name__ == "__main__":
    unittest.main()
