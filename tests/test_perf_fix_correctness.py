#!python3

"""Correctness tests for the many-core / DataFrame-scan performance fixes.

Covers:
- the buffer-wide arrow LIKE-contains predicate (PandasScan::evalArrowPredSegment):
  row attribution, needle straddling a row boundary, hits inside null slots
  (Arrow permits non-zero-extent null slots), empty-needle edge case, and
  agreement with the generic non-PREWHERE execution path;
- pandas column resolution through NumpyExtensionArray._ndarray (fillColumn):
  the numeric/bool/datetime/nullable dtype matrix against pandas ground truth;
- the multi-NUMA default max_threads cap: the adjusted default is not marked
  as an explicitly-changed setting, and explicit user values are honoured.
"""

import unittest
import numpy as np
import pandas as pd
import pyarrow as pa
import chdb


def _csv(conn, query):
    return str(conn.query(query, "CSV")).strip()


class TestArrowLikePredicate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = chdb.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_contains_matches_generic_path_and_python_truth(self):
        # Adversarial layout: needle fragments, straddle candidates, empties,
        # multi-hit rows, needles at row starts/ends.
        frags = ["goo", "gle", "google", "gogole", "ggoogle", "xgooglex", "",
                 "g", "googl", "oogle", "a google b google c", "GOOGLE",
                 "http://google.com", "ends with goo", "gle starts"]
        n = 100_000
        vals = [frags[i % len(frags)] + ("!" if i % 7 == 0 else "") for i in range(n)]
        rng = np.random.default_rng(42)
        for i in rng.integers(0, n, 2000):
            vals[i] = "".join(rng.choice(list("gogle!"), size=rng.integers(0, 12)))
        df = pd.DataFrame({"s": np.array(vals, dtype=object),
                           "t": np.arange(n, dtype=np.int64)})
        globals()["_like_df"] = df
        self.addCleanup(globals().pop, "_like_df", None)

        truth = sum("google" in v for v in vals)
        base = "SELECT count() FROM (SELECT * FROM Python(_like_df) WHERE s LIKE '%google%')"
        got_prewhere = int(_csv(self.conn, base))
        got_generic = int(_csv(self.conn, base + " SETTINGS optimize_move_to_prewhere=0"))
        self.assertEqual(got_prewhere, truth)
        self.assertEqual(got_generic, truth)

        # Same top-N rows on both paths (deterministic tie-break via s).
        topn = ("SELECT s, t FROM (SELECT * FROM Python(_like_df) "
                "WHERE s LIKE '%google%' ORDER BY t, s LIMIT 10)")
        self.assertEqual(
            str(self.conn.query(topn, "CSV")),
            str(self.conn.query(topn + " SETTINGS optimize_move_to_prewhere=0", "CSV")),
        )

    def test_hit_inside_nonzero_extent_null_slot_is_discarded(self):
        # The Arrow spec only requires monotonic offsets: a null slot may span
        # bytes, and those bytes may contain the needle. Build such an array
        # by hand; the null row must not leak through the LIKE filter.
        values = b"http://google.com/xAAAgoogleBBBhttp://ok.io!"
        offsets = pa.py_buffer(np.array([0, 19, 31, 44], dtype=np.int32).tobytes())
        validity = pa.py_buffer(bytes([0b101]))  # rows 0,2 valid; row 1 null
        arr = pa.Array.from_buffers(pa.utf8(), 3, [validity, offsets, pa.py_buffer(values)])

        df = pd.DataFrame({"URL": pd.arrays.ArrowStringArray(pa.chunked_array([arr])),
                           "x": np.arange(3, dtype=np.int64)})
        globals()["_null_slot_df"] = df
        self.addCleanup(globals().pop, "_null_slot_df", None)

        self.assertEqual(
            _csv(self.conn, "SELECT count() FROM (SELECT * FROM Python(_null_slot_df) "
                            "WHERE URL LIKE '%google%')"),
            "1")
        self.assertEqual(
            _csv(self.conn, "SELECT x FROM (SELECT * FROM Python(_null_slot_df) "
                            "WHERE URL LIKE '%google%')"),
            "0")

    def test_sliced_and_multichunk_arrays(self):
        # Non-zero chunk offsets and multi-chunk layouts stress the validity
        # bit indexing (bit_base = chunk.offset + local_start) and per-chunk
        # hit attribution of the buffer-wide scan.
        vals1 = ["google one", None, "nope", "a google b", "xx"] * 40
        vals2 = [None, "google tail", "yy", None] * 25
        arr1 = pa.array(vals1, type=pa.utf8())
        arr2 = pa.array(vals2, type=pa.utf8())
        truth = sum(1 for v in vals1 + vals2 if v is not None and "google" in v)

        multi = pd.DataFrame({
            "s": pd.arrays.ArrowStringArray(pa.chunked_array([arr1, arr2])),
        })
        globals()["_multi_df"] = multi
        self.addCleanup(globals().pop, "_multi_df", None)
        self.assertEqual(
            int(_csv(self.conn, "SELECT count() FROM (SELECT * FROM Python(_multi_df) "
                                "WHERE s LIKE '%google%')")),
            truth)

        k = 7  # slice off a non-multiple-of-8 prefix so validity bits shift
        sliced = pd.DataFrame({
            "s": pd.arrays.ArrowStringArray(pa.chunked_array([arr1.slice(k)])),
        })
        globals()["_sliced_df"] = sliced
        self.addCleanup(globals().pop, "_sliced_df", None)
        truth_sliced = sum(1 for v in vals1[k:] if v is not None and "google" in v)
        self.assertEqual(
            int(_csv(self.conn, "SELECT count() FROM (SELECT * FROM Python(_sliced_df) "
                                "WHERE s LIKE '%google%')")),
            truth_sliced)

    def test_empty_needle_matches_every_valid_row(self):
        df = pd.DataFrame({"s": pd.array(["a", None, "", "b"], dtype="string[pyarrow]"),
                           "x": np.arange(4, dtype=np.int64)})
        globals()["_empty_needle_df"] = df
        self.addCleanup(globals().pop, "_empty_needle_df", None)
        self.assertEqual(
            _csv(self.conn, "SELECT count() FROM (SELECT * FROM Python(_empty_needle_df) "
                            "WHERE s LIKE '%%')"),
            "3")


class TestPandasColumnResolution(unittest.TestCase):
    """The _ndarray fast path must yield the same values as pandas."""

    @classmethod
    def setUpClass(cls):
        cls.conn = chdb.connect(":memory:")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_dtype_matrix_against_pandas_ground_truth(self):
        df = pd.DataFrame({
            "i16": np.array([1, -2, 3], dtype=np.int16),
            "u32": np.array([1, 2, 3], dtype=np.uint32),
            "f64": np.array([1.5, np.nan, 3.0]),
            "b": np.array([True, False, True]),
            "dt": pd.to_datetime([1, 2, 3], unit="s"),
            "ni64": pd.array([1, None, 3], dtype="Int64"),
            "obj": np.array(["x", "y", "z"], dtype=object),
        })
        globals()["_dtype_df"] = df
        self.addCleanup(globals().pop, "_dtype_df", None)
        got = _csv(self.conn,
                   "SELECT sum(i16), sum(u32), sum(f64), countIf(b), max(dt), "
                   "sum(ni64), max(obj) FROM Python(_dtype_df)")
        self.assertEqual(got, '2,6,4.5,2,"1970-01-01 00:00:03",4,"z"')

    def test_wide_frame_row_integrity(self):
        # Row-aligned values across many plain numpy columns: any dtype or
        # buffer mixup in column resolution shows up as a checksum mismatch.
        n = 50_000
        rng = np.random.default_rng(7)
        data = {f"c{i}": rng.integers(0, 1_000_000, n) for i in range(40)}
        df = pd.DataFrame(data)
        globals()["_wide_df"] = df
        self.addCleanup(globals().pop, "_wide_df", None)
        expr = " + ".join(f"sum(c{i})" for i in range(40))
        got = int(_csv(self.conn, f"SELECT {expr} FROM Python(_wide_df)"))
        self.assertEqual(got, int(sum(df[c].sum() for c in df.columns)))


class TestMaxThreadsDefault(unittest.TestCase):
    def test_default_is_not_marked_changed_and_override_works(self):
        conn = chdb.connect(":memory:")
        try:
            value, changed = _csv(
                conn, "SELECT value, changed FROM system.settings "
                      "WHERE name='max_threads'").rsplit(",", 1)
            # The (possibly NUMA-capped) default must not masquerade as an
            # explicitly-set value: it would otherwise propagate to remote
            # servers in distributed queries.
            self.assertEqual(changed, "0")
            self.assertNotEqual(value, "")
            # Explicit per-query values are always honoured.
            self.assertEqual(
                _csv(conn, "SELECT sum(number) FROM numbers(100) SETTINGS max_threads=3"),
                "4950")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
