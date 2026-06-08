#!/usr/bin/env python3
"""Tests for the pandas-string DataFrame output / input fast paths.

Covers three optimizations:
  - PyUnicode dedup in PandasDataFrameBuilder string output
  - ArrowStringArray output when pandas >= 3
  - ArrowStringArray input zero-copy via Python(__df__) table function
"""

import os
import unittest
import shutil

import numpy as np
import pandas as pd

import chdb


PANDAS_MAJOR = int(pd.__version__.split(".")[0])


class TestStringOutput(unittest.TestCase):
    def setUp(self):
        self.dir = ".tmp_test_string_output_fast"
        shutil.rmtree(self.dir, ignore_errors=True)
        self.session = chdb.session.Session(self.dir)

    def tearDown(self):
        self.session.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_low_cardinality_string_values_correct(self):
        sql = (
            "SELECT ['a','b','c','d'][1 + intDiv(number, 25) % 4] AS s "
            "FROM numbers(100)"
        )
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 100)
        self.assertEqual(df["s"].iloc[0], "a")
        self.assertEqual(df["s"].iloc[25], "b")
        self.assertEqual(df["s"].iloc[75], "d")
        self.assertEqual(sorted(df["s"].unique().tolist()), ["a", "b", "c", "d"])

    def test_unique_string_values_correct(self):
        sql = "SELECT lpad(toString(number), 6, '0') AS s FROM numbers(1000)"
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 1000)
        self.assertEqual(df["s"].iloc[0], "000000")
        self.assertEqual(df["s"].iloc[999], "000999")
        self.assertEqual(df["s"].nunique(), 1000)

    def test_string_with_nulls_values_correct(self):
        sql = (
            "SELECT if(number % 3 = 0, NULL, toString(number)) AS s "
            "FROM numbers(30)"
        )
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 30)
        self.assertTrue(pd.isna(df["s"].iloc[0]))
        self.assertEqual(df["s"].iloc[1], "1")
        self.assertEqual(df["s"].iloc[2], "2")
        self.assertTrue(pd.isna(df["s"].iloc[3]))

    def test_mixed_int_and_string_columns(self):
        sql = (
            "SELECT toInt64(number) AS n, lpad(toString(number), 4, '0') AS s "
            "FROM numbers(50)"
        )
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(list(df.columns), ["n", "s"])
        self.assertEqual(df["n"].iloc[10], 10)
        self.assertEqual(df["s"].iloc[10], "0010")
        self.assertEqual(df["n"].dtype.kind, "i")

    def test_empty_string_column(self):
        sql = "SELECT '' AS s FROM numbers(5)"
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 5)
        for i in range(5):
            self.assertEqual(df["s"].iloc[i], "")

    def test_utf8_multibyte_string(self):
        sql = "SELECT '中文' AS s FROM numbers(3)"
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 3)
        self.assertEqual(df["s"].iloc[0], "中文")

    @unittest.skipIf(PANDAS_MAJOR < 3, "Arrow string default needs pandas >= 3")
    def test_pandas3_string_dtype_is_str(self):
        df = self.session.query(
            "SELECT toString(number) AS s FROM numbers(10)", "DataFrame"
        )
        self.assertEqual(str(df["s"].dtype), "str")

    def test_large_string_column_correct(self):
        sql = "SELECT lpad(toString(number), 50, 'x') AS s FROM numbers(200)"
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 200)
        self.assertEqual(df["s"].iloc[0].count("x"), 49)
        self.assertEqual(len(df["s"].iloc[42]), 50)

    def test_low_cardinality_string_column(self):
        """LowCardinality(String) must keep working — Arrow fast path does
        not handle the dictionary representation, so it falls back to the
        numpy/PyUnicode path with the dedup cache."""
        sql = (
            "SELECT toLowCardinality(['a','b','c'][1 + number%3]) AS s "
            "FROM numbers(30)"
        )
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 30)
        self.assertEqual(df["s"].iloc[0], "a")
        self.assertEqual(df["s"].iloc[1], "b")
        self.assertEqual(df["s"].iloc[2], "c")
        self.assertEqual(sorted(df["s"].unique().tolist()), ["a", "b", "c"])

    def test_string_column_with_non_utf8_bytes_falls_back(self):
        """String columns containing invalid UTF-8 (binary blobs) must keep
        working — the Arrow fast path can't represent them, so we fall back
        to the numpy path which yields bytes/bytearray for bad sequences."""
        sql = "SELECT unhex('FF8086') AS s FROM numbers(3)"
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 3)
        v = df["s"].iloc[0]
        self.assertEqual(bytes(v), b"\xff\x80\x86")

    def test_fixed_string_column(self):
        """FixedString stays on the numpy/PyUnicode output path."""
        sql = "SELECT toFixedString(toString(number), 4) AS s FROM numbers(20)"
        df = self.session.query(sql, "DataFrame")
        self.assertEqual(len(df), 20)
        # toFixedString right-pads with zero bytes to reach the target width.
        self.assertEqual(df["s"].iloc[0][:1], "0")
        self.assertEqual(df["s"].iloc[19][:2], "19")


if __name__ == "__main__":
    unittest.main()
