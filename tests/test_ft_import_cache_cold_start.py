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

# The deadlock races the FIRST lazy import-cache load, in the first rows each
# worker processes — total row volume adds nothing to the repro (a regressed
# build hangs forever at any size and still trips the timeout). What DOES
# matter is how many threads actually execute the UDF concurrently, and that
# is the number of data blocks: with the default max_block_size (65409) a
# small row count degenerates to a single block, i.e. a single racing thread
# and no deadlock window at all. So the block size is pinned small to keep a
# full 32-way race (100k rows / 1k = 100 blocks) while the total work stays
# tiny — the original 4M rows were CPU-bound past the 180s budget on the FT
# CI runner (per-row Python UDF × oversubscribed threads × collect storm),
# misreporting plain slowness as the deadlock.
ROWS = 100_000
MAX_BLOCK_SIZE = 1_000
# The race is probabilistic: on a pre-fix build one cold start hangs in ~3 of
# 4 attempts (the import can win the race against the first stop-the-world).
# Several independent cold processes push the per-test catch rate past 98%,
# while a healthy build pays well under a second per attempt.
ATTEMPTS = 3
# With the storm time-boxed below, a healthy attempt is structurally bounded
# (~10-20s even with pandas loaded), so the timeout's only job is detecting an
# infinite hang and it is deliberately generous: ~28x the measured healthy
# attempt, absorbing even pathological slowness spikes. A real regression
# still fails fast — the first hanging attempt trips it, so the worst-case
# CI cost of a regression is a single timeout, not ATTEMPTS times it.
TIMEOUT_SECONDS = 300
# The storm only needs to cover the cold-import window at query start: once
# the deadlock forms it self-sustains (the collector itself is then stuck
# inside gc.collect(), waiting on a stop-the-world that can never finish), so
# bounding the storm loses no detection power. Left unbounded, it throttled
# the whole healthy run instead: with pandas installed (as on CI) the lazy
# import loads the real package, the heap grows, every gc.collect() costs
# tens of milliseconds, and the run took >130s even on a 16-core box.
STORM_SECONDS = 10

CHILD_SCRIPT = textwrap.dedent(
    f"""
    import gc
    import threading
    import time

    import chdb
    from chdb.sqltypes import INT64

    @chdb.func([INT64, INT64], INT64)
    def fadd(a, b):
        return (a * 31 + b) % 97

    stop = threading.Event()
    storm_deadline = time.monotonic() + {STORM_SECONDS}

    def collector():
        # Continuous stop-the-world requests, covering the cold lazy-import
        # window of the first parallel query; time-boxed so a healthy run is
        # not throttled end-to-end (a formed deadlock self-sustains anyway).
        while not stop.is_set() and time.monotonic() < storm_deadline:
            gc.collect()

    thread = threading.Thread(target=collector, daemon=True)
    thread.start()
    time.sleep(0.05)  # make sure the storm is already running when the query starts

    try:
        result = chdb.query(
            "SELECT sum(fadd(toInt64(number), 1)) FROM numbers_mt({ROWS}) "
            "SETTINGS max_threads=32, max_block_size={MAX_BLOCK_SIZE}"
        )
    finally:
        # Always stop the collector: a spinning gc.collect() thread during
        # interpreter finalization can stall the child and turn an ordinary
        # query error into a bogus deadlock timeout.
        stop.set()
        thread.join(timeout=5)
    print("RESULT:" + str(result).strip(), flush=True)
    """
)


class TestFTImportCacheColdStart(unittest.TestCase):
    def test_cold_parallel_udf_survives_gc_stop_the_world_storm(self):
        # fadd is deterministic: sum((i * 31 + 1) % 97 for i in range(ROWS)),
        # computed via the period-97 structure of the sequence.
        period = [(i * 31 + 1) % 97 for i in range(97)]
        full, rest = divmod(ROWS, 97)
        expected = sum(period) * full + sum(period[:rest])

        for attempt in range(ATTEMPTS):
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
                    f"cold parallel UDF query (attempt {attempt + 1}/{ATTEMPTS}) "
                    f"did not finish within {TIMEOUT_SECONDS}s — the import-cache "
                    "stop-the-world deadlock (issue #131) is back. "
                    "Child stderr:\n" + stderr[-2000:]
                )

            self.assertEqual(
                proc.returncode,
                0,
                f"child process failed (attempt {attempt + 1}):\n"
                + (proc.stderr or "")[-2000:],
            )
            self.assertIn(f"RESULT:{expected}", proc.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
