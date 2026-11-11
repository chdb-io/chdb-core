"""chdb must free foreign pointers into the process allocator, not glibc's.

When jemalloc is LD_PRELOAD'ed (or linked into the interpreter), glibc functions
such as realpath() and strdup(), and Python itself, allocate from it. `import chdb`
used to hand those pointers to glibc's free and abort. Each case runs in a
subprocess so the allocator is in place before the interpreter starts.
"""

import ctypes.util
import os
import subprocess
import sys
import tempfile
import unittest

_JEMALLOC = ctypes.util.find_library("jemalloc") if sys.platform == "linux" else None

_BODY = """
import os

with open("/proc/self/maps") as maps:
    assert "libjemalloc" in maps.read(), "jemalloc is not loaded"

import chdb
from chdb import session

assert chdb.query("SELECT 1 + 2", "CSV").bytes() == b"3\\n"
with open("data.csv", "w") as f:
    f.write("1\\n2\\n3\\n")
assert chdb.query("SELECT sum(x) FROM file('data.csv', CSV, 'x UInt64')", "CSV").bytes() == b"6\\n"

sess = session.Session(os.path.join(os.getcwd(), "db"))
sess.query("CREATE TABLE issue252_t (x UInt64) ENGINE = MergeTree ORDER BY x")
sess.query("INSERT INTO issue252_t SELECT number FROM numbers(1000)")
assert sess.query("SELECT count() FROM issue252_t", "CSV").bytes() == b"1000\\n"
sess.close()
print("OK")
"""


@unittest.skipUnless(_JEMALLOC, "needs Linux with libjemalloc installed")
class TestIssue252(unittest.TestCase):
    def test_jemalloc_preloaded(self):
        env = dict(os.environ)
        env["LD_PRELOAD"] = _JEMALLOC
        env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
        with tempfile.TemporaryDirectory() as cwd:
            proc = subprocess.run(
                [sys.executable, "-c", _BODY],
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
            )
        self.assertEqual(proc.returncode, 0, proc.stderr[-4000:])
        self.assertEqual(proc.stdout.strip().splitlines()[-1], "OK", proc.stdout)


if __name__ == "__main__":
    unittest.main()
