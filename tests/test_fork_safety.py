#!python3

import os
import subprocess
import sys
import unittest


def _run_script(code: str, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork()")
class TestForkSafety(unittest.TestCase):
    def test_import_does_not_boot_engine_and_fork_before_init_succeeds(self):
        code = """
import os, sys, time
import chdb

assert isinstance(chdb.engine_version, str) and len(chdb.engine_version) > 0

pid = os.fork()
if pid == 0:
    # Engine was NOT initialized in parent; child must be able to boot and query.
    r1 = str(chdb.query("SELECT 42", "CSV")).strip()
    assert r1 == "42", f"unexpected query result: {r1!r}"
    conn = chdb.connect(":memory:")
    r2 = str(conn.query("SELECT version()", "CSV")).strip().strip('"')
    assert r2 == chdb.engine_version, f"version mismatch: {r2!r} != {chdb.engine_version!r}"
    conn.close()
    sys.exit(0)

_, status = os.waitpid(pid, 0)
assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, f"child status={status}"
"""
        proc = _run_script(code)
        self.assertEqual(
            proc.returncode,
            0,
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )

    def test_fork_after_init_child_exits_cleanly_with_open_connection(self):
        code = """
import gc, os, sys
import chdb

conn = chdb.connect(":memory:")
assert str(conn.query("SELECT 1", "CSV")).strip() == "1"

# Child A: exits immediately without calling any chDB method (GC + atexit + C++ static teardown).
pid_a = os.fork()
if pid_a == 0:
    sys.exit(0)

_, status_a = os.waitpid(pid_a, 0)
assert os.WIFEXITED(status_a) and os.WEXITSTATUS(status_a) == 0, f"child A status={status_a}"

# Child B: explicitly closes inherited connection, forces GC, and exits.
pid_b = os.fork()
if pid_b == 0:
    conn.close()
    del conn
    gc.collect()
    sys.exit(0)

_, status_b = os.waitpid(pid_b, 0)
assert os.WIFEXITED(status_b) and os.WEXITSTATUS(status_b) == 0, f"child B status={status_b}"

# Parent connection must still be usable.
assert str(conn.query("SELECT 2", "CSV")).strip() == "2"
conn.close()
"""
        proc = _run_script(code)
        self.assertEqual(
            proc.returncode,
            0,
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )

    def test_fork_after_init_child_queries_fail_fast(self):
        code = """
import os, sys
import chdb
from chdb import _chdb

conn = chdb.connect(":memory:")
assert str(conn.query("SELECT 1", "CSV")).strip() == "1"
cursor = conn.cursor()

pid = os.fork()
if pid == 0:
    ops = [
        ("chdb.connect", lambda: chdb.connect(":memory:")),
        ("chdb.query", lambda: chdb.query("SELECT 1", "CSV")),
        ("_chdb.connect", lambda: _chdb.connect(":memory:")),
        ("_chdb.query", lambda: _chdb.query("SELECT 1", "CSV")),
        ("conn.query", lambda: conn.query("SELECT 1", "CSV")),
        ("conn.send_query", lambda: conn.send_query("SELECT 1", "CSV")),
        ("cursor.execute", lambda: cursor.execute("SELECT 1")),
    ]
    for name, fn in ops:
        try:
            fn()
        except RuntimeError as exc:
            msg = str(exc).lower()
            assert "fork" in msg, f"{name}: expected 'fork' in error, got: {exc}"
            assert "spawn" in msg, f"{name}: expected 'spawn' hint in error, got: {exc}"
        else:
            raise AssertionError(f"{name} unexpectedly succeeded after fork()")
    sys.exit(0)

_, status = os.waitpid(pid, 0)
assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, f"child status={status}"

assert str(conn.query("SELECT 3", "CSV")).strip() == "3"
cursor.close()
conn.close()
"""
        proc = _run_script(code)
        self.assertEqual(
            proc.returncode,
            0,
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )

    def test_fork_during_active_background_query_no_mutex_deadlock(self):
        code = """
import os, sys, threading, time
import chdb

conn = chdb.connect(":memory:")
assert str(conn.query("SELECT 1", "CSV")).strip() == "1"

started_conn = threading.Event()
started_global = threading.Event()
errors = []

def run_conn_query():
    started_conn.set()
    try:
        conn.query("SELECT sleepEachRow(0.2) FROM numbers(10)", "CSV")
    except Exception as exc:
        errors.append(exc)

def run_global_query():
    started_global.set()
    try:
        chdb.query("SELECT sleepEachRow(0.2) FROM numbers(10)", "CSV")
    except Exception as exc:
        errors.append(exc)

t1 = threading.Thread(target=run_conn_query)
t2 = threading.Thread(target=run_global_query)
t1.start()
t2.start()
started_conn.wait(5)
started_global.wait(5)
time.sleep(0.2)

pid = os.fork()
if pid == 0:
    # Must NOT deadlock on g_conn_lock, registry.mutex, lifecycle_mutex, client_mutex, or instance_mutex.
    for fn in (
        lambda: chdb.query("SELECT 1", "CSV"),
        lambda: chdb.connect(":memory:"),
        lambda: conn.query("SELECT 1", "CSV"),
    ):
        try:
            fn()
        except RuntimeError as exc:
            assert "fork" in str(exc).lower(), f"unexpected error: {exc}"
        else:
            raise AssertionError("expected RuntimeError after fork()")
    conn.close()
    sys.exit(0)

_, status = os.waitpid(pid, 0)
assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, f"child status={status}"

t1.join(timeout=15)
t2.join(timeout=15)
assert not t1.is_alive() and not t2.is_alive(), "background threads did not finish"
assert not errors, f"parent background errors: {errors}"
assert str(conn.query("SELECT 99", "CSV")).strip() == "99"
conn.close()
"""
        proc = _run_script(code)
        self.assertEqual(
            proc.returncode,
            0,
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )

    def test_fork_with_active_streaming_query_child_exits_cleanly(self):
        code = """
import os, sys
import chdb

conn = chdb.connect(":memory:")
stream = conn.send_query("SELECT number FROM numbers(100000)", "CSV")
first = stream.fetch()
assert first is not None and len(first) > 0

pid = os.fork()
if pid == 0:
    stream.close()
    conn.close()
    sys.exit(0)

_, status = os.waitpid(pid, 0)
assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, f"child status={status}"

stream.close()
conn.close()
"""
        proc = _run_script(code)
        self.assertEqual(
            proc.returncode,
            0,
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
