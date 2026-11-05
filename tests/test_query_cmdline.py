#!python3

import subprocess
import sys
import unittest

# chdb_query_cmdline() and the deprecated query_stable*() boot a one-shot LocalServer
# per call; chdb._chdb.query() is the Python entry to that path. The server's global
# context initializes the process-wide EventNotifier, which nothing shut down, so every
# call after the first in a process failed with "EventNotifier is initialized twice".
# And a call that failed reported its error only on stderr: the result carried neither
# data nor the error. A fresh process, because the path cannot run while a connection
# is open.
CHILD = (
    "from chdb import _chdb\n"
    "for i in range(3):\n"
    "    r = _chdb.query(f'SELECT {i} + 40', 'CSV')\n"
    "    assert not r.has_error(), r.error_message()\n"
    "    assert r.bytes().strip() == str(i + 40).encode(), r.bytes()\n"
    "r = _chdb.query('SELEC 1', 'CSV')\n"
    "assert r.has_error(), 'a failed one-shot query must report its error'\n"
    "assert 'Syntax error' in r.error_message(), r.error_message()\n"
    "r = _chdb.query('SELECT 7', 'CSV')\n"
    "assert not r.has_error(), r.error_message()\n"
    "assert r.bytes().strip() == b'7', r.bytes()\n"
)


class TestQueryCmdline(unittest.TestCase):
    def test_repeated_one_shot_queries(self):
        result = subprocess.run(
            [sys.executable, "-c", CHILD],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        self.assertNotIn("initialized twice", result.stderr)


if __name__ == "__main__":
    unittest.main()
