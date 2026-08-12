#!python3

"""Lifetime, memory-integrity, and repeated-execution tests for the pandas
scan optimizations (metadata cache + zero-copy mounts).

Invariants pinned here:
- Borrowed buffers are never modified by query execution (checksums).
- Python references taken for a query are released at that query's end;
  nothing waits for a future query.
- DDL that stores scan output (CREATE TABLE ... AS SELECT) keeps data valid
  after the source DataFrame dies, and connection close releases everything.
- Repeated and concurrent execution is stable and does not accumulate memory.
"""

import gc
import hashlib
import threading
import sys
import unittest
import weakref

import numpy as np
import pandas as pd

import chdb

PANDAS3 = int(pd.__version__.split(".")[0]) >= 3


def refresh_frame_locals():
    """chdb's variable lookup walks every stack frame's f_locals; on
    Python <= 3.12 (pre-PEP 667) that access materializes a cached snapshot
    dict on the frame which holds strong references and is NOT updated by
    `del`. Re-reading f_locals refreshes the snapshot and drops deleted
    entries, so weakref-release assertions see the true liveness."""
    sys._getframe(1).f_locals  # noqa: B018


def make_df(n=100_000, tag=""):
    return pd.DataFrame(
        {
            "i64": np.arange(n, dtype=np.int64),
            "f64": np.arange(n, dtype=np.float64) / 7.0,
            "s": pd.array([f"{tag}payload-{i % 9973:05d}" for i in range(n)], dtype="str")
            if PANDAS3
            else [f"{tag}payload-{i % 9973:05d}" for i in range(n)],
        }
    )


def buffer_checksums(df):
    """Hashes of the raw memory the engine may borrow: numpy data + arrow chunk buffers."""
    sums = [hashlib.sha256(df["i64"].to_numpy().tobytes()).hexdigest(),
            hashlib.sha256(df["f64"].to_numpy().tobytes()).hexdigest()]
    if PANDAS3:
        for chunk in df["s"].array._pa_array.chunks:
            for buf in chunk.buffers():
                if buf is not None:
                    sums.append(hashlib.sha256(bytes(buf)).hexdigest())
    return sums


class TestBorrowedBufferIntegrity(unittest.TestCase):
    """Query execution must never write through borrowed df buffers."""

    def test_query_matrix_leaves_buffers_untouched(self):
        conn = chdb.connect(":memory:")
        try:
            df = make_df()  # noqa: F841
            before = buffer_checksums(df)
            queries = [
                "SELECT count(), sum(i64), sum(f64) FROM Python(df)",
                "SELECT s, count() FROM Python(df) GROUP BY s ORDER BY count() DESC, s LIMIT 10",
                "SELECT * FROM Python(df) ORDER BY i64 DESC LIMIT 5",
                "SELECT * FROM Python(df) WHERE s LIKE '%payload-000%' ORDER BY i64 LIMIT 7",
                "SELECT lower(s), upper(s), length(s) FROM Python(df) ORDER BY i64 LIMIT 3",
                "SELECT a.i64 FROM Python(df) a JOIN Python(df) b ON a.i64 = b.i64 WHERE a.i64 < 100 ORDER BY a.i64",
                "SELECT i64, f64, s FROM Python(df) WHERE i64 % 3 = 0 ORDER BY f64 DESC LIMIT 11",
                "SELECT DISTINCT s FROM Python(df) ORDER BY s LIMIT 13",
                "SELECT sum(cityHash64(s)) FROM Python(df)",
                "SELECT max(s), min(s) FROM Python(df)",
            ]
            for q in queries:
                str(conn.query(q, "CSV"))
            conn.query("CREATE TABLE zc_mat ENGINE = Memory AS SELECT * FROM Python(df)", "CSV")
            conn.query("DROP TABLE zc_mat", "CSV")
            self.assertEqual(before, buffer_checksums(df),
                             "query execution modified borrowed DataFrame memory")
        finally:
            conn.close()


class TestDDLRetention(unittest.TestCase):
    """Scan output stored into a table must stay valid after the df dies."""

    def test_memory_table_correct_after_df_deleted(self):
        conn = chdb.connect(":memory:")
        try:
            n = 200_000
            df = make_df(n)  # noqa: F841
            wr_df = weakref.ref(df)
            conn.query("CREATE TABLE zc_keep ENGINE = Memory AS SELECT * FROM Python(df)", "CSV")
            del df
            refresh_frame_locals()
            gc.collect()
            self.assertIsNone(wr_df(), "df itself must not be pinned after the DDL query ends")
            got = str(conn.query(
                "SELECT count(), sum(i64), min(s), max(s) FROM zc_keep", "CSV")).strip()
            expected = f'{n},{(n - 1) * n // 2},"payload-00000","payload-09972"'
            self.assertEqual(got, expected)
            conn.query("DROP TABLE zc_keep", "CSV")
        finally:
            conn.close()

    @unittest.skipUnless(PANDAS3, "arrow-backed str dtype requires pandas 3")
    def test_stored_table_owns_its_data(self):
        # Sinks materialize borrowed columns, so a table built from Python(df)
        # owns copies: nothing pins the df once the DDL query has ended.
        conn = chdb.connect(":memory:")
        try:
            df = make_df(200_000)
            wr_pa = weakref.ref(df["s"].array._pa_array)
            conn.query("CREATE TABLE zc_pin ENGINE = Memory AS SELECT * FROM Python(df)", "CSV")
            del df
            refresh_frame_locals()
            gc.collect()
            self.assertIsNone(wr_pa(),
                              "table must store owned copies; df backing still pinned")
            conn.query("DROP TABLE zc_pin", "CSV")
        finally:
            conn.close()

    def test_memory_table_is_snapshot_not_alias(self):
        # Mutating the DataFrame after INSERT must not rewrite table data.
        conn = chdb.connect(":memory:")
        try:
            n = 100_000
            arr = np.arange(n, dtype=np.int64)
            df = pd.DataFrame({"x": arr}, copy=False)  # noqa: F841
            conn.query("CREATE TABLE zc_snap ENGINE = Memory AS SELECT * FROM Python(df)", "CSV")
            arr[:] = 0
            got = str(conn.query("SELECT sum(x), min(x), max(x) FROM zc_snap", "CSV")).strip()
            self.assertEqual(got, f"{(n - 1) * n // 2},0,{n - 1}",
                             "Memory table aliases the numpy buffer instead of owning a snapshot")
            if df["x"].to_numpy().base is not None:  # buffer actually shared
                live = str(conn.query("SELECT sum(x) FROM Python(df)", "CSV")).strip()
                self.assertEqual(live, "0", "direct scans do read live values by design")
            conn.query("DROP TABLE zc_snap", "CSV")
        finally:
            conn.close()

    def test_join_table_is_snapshot(self):
        conn = chdb.connect(":memory:")
        try:
            n = 50_000
            arr = np.arange(n, dtype=np.int64)
            df = pd.DataFrame({"k": arr, "v": arr * 2}, copy=False)  # noqa: F841
            conn.query("CREATE TABLE zc_join (k Int64, v Int64) ENGINE = Join(ANY, LEFT, k)", "CSV")
            conn.query("INSERT INTO zc_join SELECT * FROM Python(df)", "CSV")
            arr[:] = 0
            got = str(conn.query("SELECT joinGet('zc_join', 'v', toInt64(100))", "CSV")).strip()
            self.assertEqual(got, "200", "Join table payload aliases the numpy buffer")
            conn.query("DROP TABLE zc_join", "CSV")
        finally:
            conn.close()


class TestZeroCopyEligibility(unittest.TestCase):
    def test_broadcast_stride_zero_column(self):
        # numpy broadcast views (stride 0) must yield the broadcast value,
        # never a walk over unrelated process memory.
        conn = chdb.connect(":memory:")
        try:
            n = 100_000
            df = pd.DataFrame({"f": np.broadcast_to(np.float64(7.0), (n,)),
                               "i": np.broadcast_to(np.int64(42), (n,))}, copy=False)
            self.assertEqual(df["f"].to_numpy().strides[0], 0)
            got = str(conn.query(
                "SELECT count(), min(f), max(f), min(i), max(i) FROM Python(df)", "CSV")).strip()
            self.assertEqual(got, f"{n},7,7,42,42")
        finally:
            conn.close()


class TestErrorPathStateReset(unittest.TestCase):
    def test_streaming_init_error_then_rebind(self):
        # A failed send_query must not leave a stale df handle: rebinding the
        # name serves the new data, deleting it raises instead of hanging.
        conn = chdb.connect(":memory:")
        try:
            global sdf
            sdf = pd.DataFrame({"a": np.array([1, 2, 3], dtype=np.int64)})
            with self.assertRaises(Exception):
                conn.send_query("SELECT * FROM Python(sdf) WHERE ((", "CSV")
            sdf = pd.DataFrame({"a": np.array([99], dtype=np.int64)})
            got = str(conn.query("SELECT sum(a) FROM Python(sdf)", "CSV")).strip()
            self.assertEqual(got, "99", "stale handle from failed streaming init was reused")
            del sdf
            gc.collect()
            with self.assertRaises(Exception):
                conn.query("SELECT sum(a) FROM Python(sdf)", "CSV")
        finally:
            globals().pop("sdf", None)
            conn.close()


class TestReleaseWithoutFutureQuery(unittest.TestCase):
    """Refs taken for a query are dropped at that query's end - never later."""

    @unittest.skipUnless(PANDAS3, "arrow-backed str dtype requires pandas 3")
    def test_plain_query_releases_at_query_end(self):
        conn = chdb.connect(":memory:")
        try:
            df = make_df()
            wr_pa = weakref.ref(df["s"].array._pa_array)
            str(conn.query("SELECT count(), max(s) FROM Python(df)", "CSV"))
            del df
            refresh_frame_locals()
            gc.collect()
            self.assertIsNone(wr_pa())
        finally:
            conn.close()

    @unittest.skipUnless(PANDAS3, "arrow-backed str dtype requires pandas 3")
    def test_streaming_close_releases(self):
        conn = chdb.connect(":memory:")
        try:
            df = make_df()
            wr_pa = weakref.ref(df["s"].array._pa_array)
            stream = conn.send_query("SELECT i64, s FROM Python(df)", "CSV")
            next(iter(stream))  # consume one chunk, abandon the rest
            stream.close()
            del stream, df
            refresh_frame_locals()
            gc.collect()
            self.assertIsNone(wr_pa(), "abandoned stream must release on close, not on a later query")
        finally:
            conn.close()

    @unittest.skipUnless(PANDAS3, "arrow-backed str dtype requires pandas 3")
    def test_failed_query_releases(self):
        conn = chdb.connect(":memory:")
        try:
            df = make_df()
            wr_pa = weakref.ref(df["s"].array._pa_array)
            with self.assertRaises(Exception):
                conn.query("SELECT throwIf(i64 = 5000, 'boom'), s FROM Python(df)", "CSV")
            del df
            refresh_frame_locals()
            gc.collect()
            self.assertIsNone(wr_pa(), "error path must release without a follow-up query")
        finally:
            conn.close()


class TestRepeatedAndConcurrent(unittest.TestCase):
    def test_repeated_queries_stable_and_bounded(self):
        conn = chdb.connect(":memory:")
        try:
            df = make_df()  # noqa: F841
            expected = str(conn.query(
                "SELECT s, count() AS c FROM Python(df) GROUP BY s ORDER BY c DESC, s LIMIT 5", "CSV"))

            def rss_kb():
                if sys.platform != "linux":
                    return None  # /proc is Linux-only; result stability is still checked
                with open("/proc/self/status") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            return int(line.split()[1])
                return None

            samples = []
            for i in range(100):
                got = str(conn.query(
                    "SELECT s, count() AS c FROM Python(df) GROUP BY s ORDER BY c DESC, s LIMIT 5", "CSV"))
                self.assertEqual(got, expected, f"iteration {i} diverged")
                samples.append(rss_kb())
            if samples[-1] is not None and samples[9] is not None:
                growth_kb = samples[-1] - samples[9]
                self.assertLess(growth_kb, 200_000,
                                f"RSS grew {growth_kb} KB over 90 repeated queries")
        finally:
            conn.close()

    def test_cross_thread_serialized_queries(self):
        """Queries issued from different threads (one at a time) must behave
        exactly like same-thread queries: frame discovery, cache revalidation,
        and per-query release all work per issuing thread."""
        conn = chdb.connect(":memory:")
        lock = threading.Lock()
        try:
            n1, n2 = 50_000, 30_000
            global tdf1, tdf2
            tdf1, tdf2 = make_df(n1, "a"), make_df(n2, "b")
            exp = {"tdf1": f"{n1},{(n1 - 1) * n1 // 2}", "tdf2": f"{n2},{(n2 - 1) * n2 // 2}"}
            errors = []

            def worker(name):
                try:
                    for _ in range(20):
                        with lock:
                            got = str(conn.query(
                                f"SELECT count(), sum(i64) FROM Python({name})", "CSV")).strip()
                        if got != exp[name]:
                            errors.append(f"{name}: {got} != {exp[name]}")
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{name}: {type(e).__name__}: {e}")

            threads = [threading.Thread(target=worker, args=(n,))
                       for n in ("tdf1", "tdf2", "tdf1", "tdf2")]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])
        finally:
            del tdf1, tdf2
            conn.close()

    def test_unsynchronized_threads_never_return_wrong_data(self):
        """True concurrent submission on one connection is a pre-existing
        weak spot (the stock 26.5 wheel fails most such queries with
        PY_OBJECT_NOT_FOUND). Pin the safety property that matters: a query
        either returns the exact correct result or raises - it must never
        return wrong data, crash, or hang."""
        conn = chdb.connect(":memory:")
        try:
            n1, n2 = 50_000, 30_000
            global udf1, udf2
            udf1, udf2 = make_df(n1, "a"), make_df(n2, "b")
            exp = {"udf1": f"{n1},{(n1 - 1) * n1 // 2}", "udf2": f"{n2},{(n2 - 1) * n2 // 2}"}
            wrong = []

            def worker(name):
                for _ in range(20):
                    try:
                        got = str(conn.query(
                            f"SELECT count(), sum(i64) FROM Python({name})", "CSV")).strip()
                        if got != exp[name]:
                            wrong.append(f"{name}: {got} != {exp[name]}")
                    except Exception:  # noqa: BLE001 - acceptable failure mode
                        pass

            threads = [threading.Thread(target=worker, args=(n,))
                       for n in ("udf1", "udf2", "udf1", "udf2")]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(wrong, [], "concurrent queries returned WRONG data")
        finally:
            del udf1, udf2
            conn.close()


if __name__ == "__main__":
    unittest.main()
