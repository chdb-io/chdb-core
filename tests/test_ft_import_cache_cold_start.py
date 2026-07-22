#!/usr/bin/env python3
"""Regression test for the free-threading cold-start UDF deadlock (issue #131).

On free-threaded builds, the lazy import-cache load ran inside std::call_once
with the waiting threads blocked on the once_flag futex while still attached.
An attached thread parked on a futex never reaches a safepoint, so a GC
stop-the-world issued while the call_once winner runs the import can never
complete: the winner parks forever inside the import machinery, the flag is
never released, and the first parallel UDF query of a cold process hangs.

The deadlock needs a stop-the-world to land inside the few-millisecond import
window, so the test forces it: a spammer thread issues gc.collect()
continuously while the cold query runs. Before the fix this hangs 5/5 on a
16-core box; with the fix it completes with the correct result.

Two properties matter for CI:
- the query must run in a fresh subprocess (the import cache is per-process;
  any earlier query in this process would warm it and mask the bug), and
- a regression must FAIL the test instead of hanging the suite, hence the
  subprocess timeout.

On stock (GIL) builds the scenario cannot deadlock; the test then simply
asserts the query result, at negligible cost.
"""

import subprocess
import sys
import textwrap
import unittest

ROWS = 4_000_000
TIMEOUT_SECONDS = 180

CHILD_SCRIPT = textwrap.dedent(
    f"""
    import gc
    import threading

    import chdb
    from chdb.sqltypes import INT64

    @chdb.func([INT64, INT64], INT64)
    def fadd(a, b):
        return (a * 31 + b) % 97

    stop = False

    def collector():
        # Continuous stop-the-world requests, covering the cold lazy-import
        # window of the first parallel query.
        while not stop:
            gc.collect()

    thread = threading.Thread(target=collector, daemon=True)
    thread.start()

    result = chdb.query(
        "SELECT sum(fadd(toInt64(number), 1)) FROM numbers_mt({ROWS}) "
        "SETTINGS max_threads=32"
    )
    stop = True
    thread.join(timeout=5)
    print("RESULT:" + str(result).strip(), flush=True)
    """
)


class TestFTImportCacheColdStart(unittest.TestCase):
    def test_cold_parallel_udf_survives_gc_stop_the_world_storm(self):
        try:
            proc = subprocess.run(
                [sys.executable, "-c", CHILD_SCRIPT],
                capture_output=True,
                text=True,
                timeout=TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as e:
            stderr = (e.stderr or b"")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", "replace")
            self.fail(
                "cold parallel UDF query did not finish within "
                f"{TIMEOUT_SECONDS}s — the import-cache stop-the-world "
                "deadlock (issue #131) is back. Child stderr:\n" + stderr[-2000:]
            )

        self.assertEqual(
            proc.returncode,
            0,
            "child process failed:\n" + (proc.stderr or "")[-2000:],
        )

        # fadd is deterministic: sum((i * 31 + 1) % 97 for i in range(ROWS)),
        # computed via the period-97 structure of the sequence.
        period = [(i * 31 + 1) % 97 for i in range(97)]
        full, rest = divmod(ROWS, 97)
        expected = sum(period) * full + sum(period[:rest])

        self.assertIn(f"RESULT:{expected}", proc.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
