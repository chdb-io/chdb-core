#!python3

import subprocess
import sys
import unittest

# Exiting the interpreter with a connection left open must not crash: the
# EmbeddedServer atexit hook has to destroy the server while logging and the
# statics its teardown touches are still alive. Regression test for the
# DataStore-suite subprocess SIGSEGV (exit -11) on v26.7-based builds.

CHILD_PLAIN = (
    "import chdb\n"
    "conn = chdb.connect(':memory:')\n"
    "r = conn.query('SELECT 1', 'CSV')\n"
    "assert '1' in str(r)\n"
)

# Mirrors how DataStore connects (settings in the connection string).
CHILD_WITH_SETTINGS = (
    "import chdb\n"
    "conn = chdb.connect(':memory:?memory_worker_correct_memory_tracker=0')\n"
    "r = conn.query(\n"
    "    \"SELECT value FROM system.server_settings \"\n"
    "    \"WHERE name = 'memory_worker_correct_memory_tracker'\",\n"
    "    'CSV',\n"
    ")\n"
    "assert '0' in str(r)\n"
)


class TestExitWithOpenConnection(unittest.TestCase):
    def _run_child(self, child, runs):
        for i in range(runs):
            result = subprocess.run(
                [sys.executable, "-c", child],
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(
                result.returncode,
                0,
                f"run {i}: exit {result.returncode}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
            )

    def test_exit_without_close(self):
        self._run_child(CHILD_PLAIN, 5)

    def test_exit_without_close_with_connection_settings(self):
        self._run_child(CHILD_WITH_SETTINGS, 5)


if __name__ == "__main__":
    unittest.main()
