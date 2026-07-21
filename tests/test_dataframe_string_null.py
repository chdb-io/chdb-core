#!/usr/bin/env python3
"""Regression tests for string columns containing nulls scanned via Python(df).

chdb-core <= 26.5.0 spun an infinite loop (busy CPU, query never returns) in
the Python() table engine when scanning a pandas StringDtype column that
contains missing values:
- pandas >= 3.0 default `str` dtype columns with any None/NaN (the engine read
  the `ArrowStringArray._data` attribute, which pandas 3.0 removed),
- `string[python]`-backed StringDtype columns with pd.NA (all pandas versions).
"""

import unittest

import pandas as pd

import chdb

PANDAS_MAJOR = int(pd.__version__.split(".")[0])

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


class TestDataFrameStringNull(unittest.TestCase):
    """String columns with missing values must scan correctly, never hang."""

    def _check_string_null_scan(self, df):
        """Scan a single-column frame and verify values and null flags."""
        res = str(
            chdb.query(
                "SELECT s, isNull(s) AS n FROM Python(df) ORDER BY isNull(s), s",
                "CSV",
            )
        )
        self.assertEqual(res, '"apple",0\n"cherry",0\n\\N,1\n')

    def test_default_inference_with_none(self):
        # object dtype on pandas 2.x, the new default `str` dtype on pandas 3.x
        df = pd.DataFrame({"s": ["apple", None, "cherry"]})
        self._check_string_null_scan(df)

    @unittest.skipUnless(PANDAS_MAJOR >= 3, "`str` dtype is pandas 3.x+")
    def test_str_dtype_with_none(self):
        df = pd.DataFrame({"s": pd.array(["apple", None, "cherry"], dtype="str")})
        self.assertEqual(str(df["s"].dtype), "str")
        self._check_string_null_scan(df)

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_string_pyarrow_dtype_with_na(self):
        df = pd.DataFrame(
            {"s": pd.array(["apple", pd.NA, "cherry"], dtype="string[pyarrow]")}
        )
        self._check_string_null_scan(df)

    def test_string_python_dtype_with_na(self):
        df = pd.DataFrame(
            {"s": pd.array(["apple", pd.NA, "cherry"], dtype="string[python]")}
        )
        self._check_string_null_scan(df)

    def test_object_dtype_with_none(self):
        df = pd.DataFrame(
            {"s": pd.Series(["apple", None, "cherry"], dtype=object)}
        )
        self._check_string_null_scan(df)

    def test_large_string_column_with_nulls(self):
        # Enough rows to span several validity-bitmap bytes and arrow chunks.
        values = (["value_%d" % i for i in range(50)] + [None] * 14) * 40
        df = pd.DataFrame({"s": values})
        res = str(
            chdb.query(
                "SELECT count() AS c, countIf(s IS NULL) AS nulls,"
                " sum(length(s)) AS bytes FROM Python(df)",
                "CSV",
            )
        )
        total = len(values)
        nulls = sum(1 for v in values if v is None)
        nbytes = sum(len(v) for v in values if v is not None)
        self.assertEqual(res, "%d,%d,%d\n" % (total, nulls, nbytes))

    def test_filter_on_nullable_string(self):
        df = pd.DataFrame({"s": ["a", None, "b", None, "a"]})
        res = str(
            chdb.query(
                "SELECT countDistinct(s) AS d, countIf(s IS NULL) AS n"
                " FROM Python(df)",
                "CSV",
            )
        )
        self.assertEqual(res, "2,2\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
