#!python3

import unittest
from decimal import Decimal

import numpy as np
import pandas as pd
import chdb

SAMPLE = 10000
# With 30001 rows every third row is sampled, so row 1 is not.
ROWS = 3 * SAMPLE + 1


def _column(default, unsampled):
    values = [default] * ROWS
    values[1] = unsampled
    return pd.DataFrame({"a": pd.Series(values, dtype=object)})


def _query(sql, df, sample=SAMPLE):
    return chdb.query(sql + f" SETTINGS pandas_analyze_sample = {sample}", "DataFrame")


class TestIssue674(unittest.TestCase):
    """A value outside the analyzed sample is never changed to fit the inferred type."""

    def test_unsampled_large_integer_widens_the_column(self):
        df = _column(1, 2**40)
        ret = _query("SELECT toTypeName(a) AS t, a, a IS NULL AS n FROM Python(df) LIMIT 3", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Int64)"] * 3)
        self.assertEqual(ret["a"].tolist(), [1, 2**40, 1])
        self.assertEqual(ret["n"].tolist(), [0, 0, 0])

    def test_unsampled_float_in_integer_column(self):
        df = _column(1, 2.5)
        ret = _query("SELECT toTypeName(any(a)) AS t, sum(a) AS s, countIf(a IS NULL) AS n FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Float64)"])
        self.assertEqual(ret["s"].tolist(), [ROWS - 1 + 2.5])
        self.assertEqual(ret["n"].tolist(), [0])

    def test_unsampled_string_in_integer_column_keeps_the_value(self):
        df = _column(1, "abc")
        ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df) LIMIT 2", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
        self.assertEqual(ret["a"].tolist(), ["1", "abc"])

    def test_unsampled_string_in_float_column_keeps_the_value(self):
        df = _column(0.5, "abc")
        ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df) LIMIT 2", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
        self.assertEqual(ret["a"].tolist(), ["0.5", "abc"])

    def test_unsampled_string_in_bool_column_keeps_the_value(self):
        df = _column(True, "no")
        ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df) LIMIT 2", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
        self.assertEqual(ret["a"].tolist(), ["True", "no"])

    def test_unsampled_string_in_dict_column_keeps_the_value(self):
        df = _column({"k": 1}, "abc")
        ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df) LIMIT 2", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
        self.assertEqual(ret["a"].tolist()[1], "abc")

    def test_missing_values_stay_null(self):
        df = _column(1, None)
        df.loc[4, "a"] = float("nan")
        ret = _query("SELECT toTypeName(any(a)) AS t, countIf(a IS NULL) AS n, sum(a) AS s FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Int32)"])
        self.assertEqual(ret["n"].tolist(), [2])
        self.assertEqual(ret["s"].tolist(), [ROWS - 2])

    def test_column_of_missing_values_only_reads_as_string(self):
        df = pd.DataFrame({"a": pd.Series([None] * ROWS, dtype=object)})
        ret = _query("SELECT toTypeName(a) AS t, countIf(a IS NULL) AS n FROM Python(df) GROUP BY t", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"])
        self.assertEqual(ret["n"].tolist(), [ROWS])

    def test_first_valid_value_outside_the_sample_decides(self):
        values = [None] * ROWS
        values[1] = 7
        df = pd.DataFrame({"a": pd.Series(values, dtype=object)})
        ret = _query("SELECT toTypeName(any(a)) AS t, sum(a) AS s FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Int32)"])
        self.assertEqual(ret["s"].tolist(), [7])

    def test_zero_sample_reads_object_columns_as_string(self):
        df = _column(1, 2)
        ret = _query("SELECT toTypeName(a) AS t FROM Python(df) LIMIT 1", df, sample=0)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"])

    # With pandas_analyze_sample = 1 only row 0 is sampled, so every other row is outside the sample.
    MISSING = [None, float("nan"), pd.NA, pd.NaT, Decimal("NaN"), Decimal("sNaN")]

    def test_missing_markers_in_every_column_type(self):
        for first, expected in [(1, "Nullable(Int32)"), (0.5, "Nullable(Float64)"), (True, "Nullable(Bool)")]:
            df = pd.DataFrame({"a": pd.Series([first] + self.MISSING, dtype=object)})
            ret = _query("SELECT toTypeName(any(a)) AS t, countIf(a IS NULL) AS n FROM Python(df)", df, sample=1)
            self.assertEqual(ret["t"].tolist(), [expected], first)
            self.assertEqual(ret["n"].tolist(), [len(self.MISSING)], first)

    def test_integers_beyond_64_bits_read_as_string(self):
        for big in (2**64 + 1, -(2**63) - 1, 10**400):
            df = pd.DataFrame({"a": pd.Series([1, big], dtype=object)})
            ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df)", df, sample=1)
            self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
            self.assertEqual(ret["a"].tolist(), ["1", str(big)])

    def test_negative_and_large_unsigned_integers_read_as_string(self):
        df = pd.DataFrame({"a": pd.Series([-1, 2**63 + 1], dtype=object)})
        ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df)", df, sample=1)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
        self.assertEqual(ret["a"].tolist(), ["-1", str(2**63 + 1)])

    def test_non_negative_integers_above_int64_read_as_uint64(self):
        for values in ([1, 2**63], [2**64 - 1, 0], [None, 5, 2**63 + 1]):
            df = pd.DataFrame({"a": pd.Series(values, dtype=object)})
            ret = _query("SELECT toTypeName(a) AS t, a FROM Python(df)", df, sample=1)
            self.assertEqual(ret["t"].tolist(), ["Nullable(UInt64)"] * len(values), values)
            self.assertEqual([None if pd.isna(v) else int(v) for v in ret["a"].tolist()], values)

    def test_sampled_integers_that_do_not_merge_leave_a_decimal_readable(self):
        # A sample of rows 0 and 2 holds -1 and 2**63, which no integer type holds together.
        df = pd.DataFrame({"a": pd.Series([-1, 0, 2**63, Decimal("0.5")], dtype=object)})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df, sample=2)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Decimal(20, 1))"] * 4)
        self.assertEqual(ret["v"].tolist(), ["-1", "0", str(2**63), "0.5"])

    def test_integers_beyond_64_bits_before_a_decimal(self):
        for values, expected in (
            ([2**70, Decimal("1")], "Nullable(Decimal(22, 0))"),
            ([Decimal("1"), 2**70], "Nullable(Decimal(22, 0))"),
            ([1, 2**64, Decimal("0.5")], "Nullable(Decimal(21, 1))"),
            ([2**70], "Nullable(String)"),
            ([2**70, "x"], "Nullable(String)"),
        ):
            df = pd.DataFrame({"a": pd.Series(values, dtype=object)})
            ret = _query("SELECT toTypeName(a) AS t FROM Python(df) LIMIT 1", df, sample=1)
            self.assertEqual(ret["t"].tolist(), [expected], values)

    def test_columns_of_a_two_dimensional_object_array(self):
        # Each column is a strided view into the same object array.
        df = pd.DataFrame(np.array([[1, "a", 0.5], [2, "b", None], [3, "c", 1.5]], dtype=object), columns=["i", "s", "f"])
        ret = _query("SELECT toTypeName(i) AS ti, i, toTypeName(s) AS ts, s, f FROM Python(df)", df, sample=1)
        self.assertEqual(ret["ti"].tolist(), ["Nullable(Int32)"] * 3)
        self.assertEqual(ret["i"].tolist(), [1, 2, 3])
        self.assertEqual(ret["ts"].tolist(), ["Nullable(String)"] * 3)
        self.assertEqual(ret["s"].tolist(), ["a", "b", "c"])
        self.assertEqual(ret["f"].isna().tolist(), [False, True, False])


if __name__ == "__main__":
    unittest.main()
