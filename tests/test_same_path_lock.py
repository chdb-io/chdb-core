#!python3

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest

# The holder and the second instance are separate processes: within one process the
# connections to a path share one engine, and the lock is between processes.
_HOLDER = """
import sys, chdb
conn = chdb.connect(sys.argv[1])
conn.query("SELECT 1")
print("READY", flush=True)
sys.stdin.readline()
conn.close()
"""

_OPENER = """
import sys, chdb
try:
    chdb.connect(sys.argv[1])
except Exception as e:
    print("FAILED:", e)
else:
    print("OPENED")
"""


def _python(code, path, **kwargs):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    return subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(code), path],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)


class TestSamePathLock(unittest.TestCase):
    """A process that cannot lock a directory fails before it changes anything in it."""

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix="chdb_same_path_")

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_second_process_fails_and_keeps_the_temporary_files_of_the_holder(self):
        holder = _python(_HOLDER, self.path, stdin=subprocess.PIPE)
        try:
            self.assertEqual(holder.stdout.readline().strip(), "READY", holder.stderr.read() if holder.poll() is not None else "")

            # Stands for the spill file of a query that runs in the holder.
            spill = os.path.join(self.path, "tmp", "tmp_spill_of_the_holder")
            with open(spill, "w") as f:
                f.write("data")
            status_before = open(os.path.join(self.path, "status")).read()

            opener = _python(_OPENER, self.path)
            out, err = opener.communicate(timeout=120)

            self.assertIn("FAILED:", out, out + err)
            self.assertIn("Cannot lock file", out, out + err)
            self.assertTrue(os.path.exists(spill), "the second process removed the temporary file of the holder")
            self.assertEqual(open(spill).read(), "data")
            self.assertEqual(open(os.path.join(self.path, "status")).read(), status_before)
            self.assertIn("inside one process", out, out + err)
        finally:
            holder.stdin.write("\n")
            holder.stdin.flush()
            holder.communicate(timeout=120)
        self.assertEqual(holder.returncode, 0)

    def test_path_opens_again_after_the_holder_exits(self):
        holder = _python(_HOLDER, self.path, stdin=subprocess.PIPE)
        self.assertEqual(holder.stdout.readline().strip(), "READY")
        holder.communicate("\n", timeout=120)
        self.assertEqual(holder.returncode, 0)

        opener = _python(_OPENER, self.path)
        out, err = opener.communicate(timeout=120)
        self.assertEqual(out.strip(), "OPENED", out + err)


if __name__ == "__main__":
    unittest.main()
