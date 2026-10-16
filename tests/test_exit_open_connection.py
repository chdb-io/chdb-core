#!python3

import subprocess
import sys
import unittest

# Exiting the interpreter with a connection left open must not crash: the
# Python-module exit callback must close its connections before C++ static
# objects are destroyed. The older C++ atexit fallback ran too late for
# NamedSessionsStorage and left the catalog alive during the DataStore suite.

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

# Register this probe before importing the extension: Python atexit callbacks
# run in reverse registration order, so it executes after the extension's exit
# callback but before C++ static destruction. A successful process exit alone
# cannot detect the incomplete catalog shutdown in issue #254.
CHILD_VERIFY_EARLY_SHUTDOWN = (
    "import atexit, os\n"
    "def verify_shutdown():\n"
    "    status = getattr(chdb._chdb, '_python_exit_cleanup_complete', lambda: False)\n"
    "    if not status():\n"
    "        os._exit(96)\n"
    "    try:\n"
    "        chdb._chdb.connect()\n"
    "    except Exception as exc:\n"
    "        if 'shutting down' in str(exc) or 'shut down' in str(exc):\n"
    "            return\n"
    "    os._exit(97)\n"
    "atexit.register(verify_shutdown)\n"
    "import chdb\n"
    "conn = chdb.connect(':memory:')\n"
    "r = conn.query('SELECT 1', 'CSV')\n"
    "assert '1' in str(r)\n"
)

CHILD_VERIFY_LATE_HANDLES = (
    "import atexit, os\n"
    "def verify_handles():\n"
    "    if not getattr(chdb._chdb, '_python_exit_cleanup_complete', lambda: False)():\n"
    "        os._exit(96)\n"
    "    for operation in (lambda: conn.query('SELECT 3'),\n"
    "                      lambda: cursor.execute('SELECT 4'),\n"
    "                      lambda: chdb._chdb.query('SELECT 5'),\n"
    "                      lambda: chdb._chdb.drop_function('missing'),\n"
    "                      lambda: conn.streaming_fetch_result(stream)):\n"
    "        try:\n"
    "            operation()\n"
    "        except RuntimeError as exc:\n"
    "            if 'shutting down' not in str(exc) and 'closed' not in str(exc):\n"
    "                os._exit(98)\n"
    "        else:\n"
    "            os._exit(99)\n"
    "    if b'2' not in result.bytes():\n"
    "        os._exit(95)\n"
    "atexit.register(verify_handles)\n"
    "import chdb\n"
    "conn = chdb._chdb.connect()\n"
    "cursor = conn.cursor()\n"
    "cursor.execute('SELECT 1')\n"
    "result = conn.query('SELECT 2', 'CSV')\n"
    "stream = conn.send_query('SELECT number FROM numbers(2)', 'CSV')\n"
)

CHILD_VERIFY_IN_FLIGHT = (
    "import atexit, os, threading, time\n"
    "def verify_shutdown():\n"
    "    if not chdb._chdb._python_exit_cleanup_complete():\n"
    "        os._exit(96)\n"
    "    try:\n"
    "        conn.query('SELECT 1')\n"
    "    except RuntimeError:\n"
    "        return\n"
    "    os._exit(97)\n"
    "atexit.register(verify_shutdown)\n"
    "import chdb\n"
    "conn = chdb._chdb.connect()\n"
    "def run_query():\n"
    "    try:\n"
    "        conn.query('SELECT sleep(1)', 'CSV')\n"
    "    except RuntimeError:\n"
    "        pass  # Cancellation during interpreter exit is expected.\n"
    "threading.Thread(target=run_query, daemon=True).start()\n"
    "deadline = time.monotonic() + 5\n"
    "while chdb._chdb._python_active_operations() == 0 and time.monotonic() < deadline:\n"
    "    time.sleep(0.001)\n"
    "assert chdb._chdb._python_active_operations() > 0\n"
)

CHILD_CURSOR_OWNS_CONNECTION = (
    "import gc, chdb\n"
    "conn = chdb._chdb.connect()\n"
    "cursor = conn.cursor()\n"
    "del conn\n"
    "gc.collect()\n"
    "cursor.execute('SELECT 42')\n"
    "assert cursor.data_size() > 0\n"
    "cursor.close()\n"
)

CHILD_VERIFY_ACTIVE_INSERT = (
    "import atexit, os\n"
    "def verify_shutdown():\n"
    "    if not chdb._chdb._python_exit_cleanup_complete():\n"
    "        os._exit(96)\n"
    "    try:\n"
    "        conn.insert_append(stream, b'2\\n')\n"
    "    except RuntimeError:\n"
    "        return\n"
    "    os._exit(97)\n"
    "atexit.register(verify_shutdown)\n"
    "import chdb\n"
    "conn = chdb._chdb.connect()\n"
    "conn.query('CREATE TABLE t (x UInt64) ENGINE = Memory')\n"
    "stream = conn.send_insert('INSERT INTO t (x)', 'CSV')\n"
    "conn.insert_append(stream, b'1\\n')\n"
)

CHILD_VERIFY_RECOVERABLE_TABLE_SHUTDOWN = (
    "import atexit, os\n"
    "def verify_shutdown():\n"
    "    if not chdb._chdb._python_exit_cleanup_complete():\n"
    "        os._exit(96)\n"
    "atexit.register(verify_shutdown)\n"
    "import chdb\n"
    "conn = chdb._chdb.connect()\n"
    "conn.query('CREATE DATABASE issue254 ENGINE = Atomic')\n"
    "conn.query('CREATE TABLE issue254.t (x UInt64) ENGINE = Memory')\n"
    "conn.query('SYSTEM ENABLE FAILPOINT database_catalog_throw_on_table_shutdown')\n"
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

    def test_python_exit_closes_engine_before_static_destructors(self):
        self._run_child(CHILD_VERIFY_EARLY_SHUTDOWN, 5)

    def test_python_exit_invalidates_live_handles(self):
        self._run_child(CHILD_VERIFY_LATE_HANDLES, 5)

    def test_python_exit_drains_in_flight_query(self):
        self._run_child(CHILD_VERIFY_IN_FLIGHT, 3)

    def test_native_cursor_keeps_its_connection_alive(self):
        self._run_child(CHILD_CURSOR_OWNS_CONNECTION, 3)

    def test_python_exit_cancels_unfinished_insert(self):
        self._run_child(CHILD_VERIFY_ACTIVE_INSERT, 3)

    def test_python_exit_cleans_catalog_after_table_shutdown_error(self):
        self._run_child(CHILD_VERIFY_RECOVERABLE_TABLE_SHUTDOWN, 3)


if __name__ == "__main__":
    unittest.main()
