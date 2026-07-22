#!/usr/bin/env python3
"""Python(df) behavior for pandas ArrowDtype (dtype_backend="pyarrow") columns.

ArrowDtype columns are not supported by the Python() table engine yet.
chdb-core <= 26.5.0 silently misread their buffers (int64[pyarrow] with nulls
surfaced float64 bit patterns as integers, bool[pyarrow] surfaced raw bitmap
bytes), and later revisions rejected the whole DataFrame in isPandasDataframe,
which surfaced as a misleading "Python object not found in the Python
environment" error.

A DataFrame containing ArrowDtype columns must still be recognized as a
DataFrame, and schema inference must fail with a clear per-column error that
names the offending column and dtype and suggests a workaround.
"""

import unittest

import pandas as pd

import chdb

try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
class TestDataFrameArrowDtype(unittest.TestCase):
    """ArrowDtype columns fail loudly with an actionable message."""

    def _assert_clear_error(self, df, col_name):  # noqa: F841 -- df referenced by Python(df)
        # Engine query failures surface as RuntimeError on the connection path
        # (which chdb.query uses) and as chdb.ChdbError on the stateless buffer
        # path; anything else (TypeError, ...) would be a binding bug, so keep
        # the assertion tight.
        with self.assertRaises((chdb.ChdbError, RuntimeError)) as ctx:
            chdb.query("SELECT * FROM Python(df)", "CSV")
        msg = str(ctx.exception)
        self.assertIn("'%s'" % col_name, msg)
        # The C++ side formats the dtype with py::str(col_type), i.e. exactly
        # str(df[col].dtype); derive the expectation so the test does not
        # depend on per-version ArrowDtype reprs.
        self.assertIn(str(df[col_name].dtype), msg)
        # The old whole-DataFrame rejection produced this misleading error.
        self.assertNotIn("not found in the Python environment", msg)

    def test_int64_arrow_dtype_with_nulls(self):
        df = pd.DataFrame(
            {"a": pd.array([1, 2, None], dtype=pd.ArrowDtype(pa.int64()))}
        )
        self._assert_clear_error(df, "a")

    def test_int64_arrow_dtype_without_nulls(self):
        # Without nulls the buffers happened to be readable on old versions,
        # which made the corruption data-dependent. Now it must fail the same
        # way regardless of null presence.
        df = pd.DataFrame(
            {"a": pd.array([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))}
        )
        self._assert_clear_error(df, "a")

    def test_bool_arrow_dtype(self):
        df = pd.DataFrame(
            {"b": pd.array([True, False], dtype=pd.ArrowDtype(pa.bool_()))}
        )
        self._assert_clear_error(df, "b")

    def test_timestamp_arrow_dtype(self):
        df = pd.DataFrame(
            {
                "t": pd.array(
                    [pd.Timestamp("2024-01-01")],
                    dtype=pd.ArrowDtype(pa.timestamp("us")),
                )
            }
        )
        self._assert_clear_error(df, "t")

    def test_string_arrow_dtype(self):
        df = pd.DataFrame(
            {
                "s": pd.array(
                    ["x", "中文", None], dtype=pd.ArrowDtype(pa.large_string())
                )
            }
        )
        self._assert_clear_error(df, "s")

    def test_mixed_frame_names_offending_column(self):
        df = pd.DataFrame(
            {
                "ok_int": [1, 2, 3],
                "bad": pd.array([1, 2, None], dtype=pd.ArrowDtype(pa.int64())),
            }
        )
        self._assert_clear_error(df, "bad")

    def test_astype_workaround(self):
        df = pd.DataFrame(
            {"a": pd.array([1, 2, 3], dtype=pd.ArrowDtype(pa.int64()))}
        )
        df["a"] = df["a"].astype("int64")
        res = str(chdb.query("SELECT sum(a) FROM Python(df)", "CSV"))
        self.assertEqual(res, "6\n")

    def test_pyarrow_table_alternative(self):
        # The error message suggests passing a pyarrow.Table; verify it works,
        # including nulls.
        tbl = pa.table({"a": pa.array([1, 2, None], type=pa.int64())})
        res = str(
            chdb.query(
                "SELECT sum(a) AS s, countIf(a IS NULL) AS n FROM Python(tbl)",
                "CSV",
            )
        )
        self.assertEqual(res, "3,1\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
