#!python3

import unittest
from decimal import Decimal

import pandas as pd
import chdb

SAMPLE = 10000
# Row 1 is outside the analyzer's sample: with 30001 rows every third row is sampled.
UNSAMPLED_ROWS = 3 * SAMPLE + 1


def _query(sql, df):
    return chdb.query(sql + f" SETTINGS pandas_analyze_sample = {SAMPLE}", "DataFrame")


class TestIssue323(unittest.TestCase):
    """An object column of decimal.Decimal reads as a ClickHouse Decimal."""

    def test_sum_decimal_column(self):
        df = pd.DataFrame({"a": [Decimal("1.10"), Decimal("2.20")], "b": ["x", "y"]})
        ret = _query("SELECT toTypeName(a) AS t, toString(sum(a)) AS s FROM Python(df) GROUP BY t", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Decimal(3, 2))"])
        self.assertEqual(ret["s"].tolist(), ["3.3"])

    def test_scales_merge_and_values_are_exact(self):
        df = pd.DataFrame({"a": [Decimal("1.5"), Decimal("-2.25"), Decimal("1E+2")]})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Decimal(5, 2))"] * 3)
        self.assertEqual(ret["v"].tolist(), ["1.5", "-2.25", "100"])

    def test_exponent_notation_and_negative_zero(self):
        df = pd.DataFrame({"a": [Decimal("1.2E-7"), Decimal("-0.00")]})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist()[0], "Nullable(Decimal(8, 8))")
        self.assertEqual(ret["v"].tolist(), ["0.00000012", "0"])

    def test_missing_values_are_null(self):
        df = pd.DataFrame({"a": [Decimal("1.25"), None, Decimal("NaN"), float("nan")]})
        ret = _query("SELECT a IS NULL AS n, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["n"].tolist(), [0, 1, 1, 1])
        self.assertEqual(ret["v"].tolist()[0], "1.25")

    def test_more_than_38_digits_uses_decimal256(self):
        big = Decimal("1" * 45 + ".5")
        df = pd.DataFrame({"a": [big, Decimal("2")]})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist()[0], "Nullable(Decimal(46, 1))")
        self.assertEqual(ret["v"].tolist(), [str(big), "2"])

    def test_integers_mix_with_decimals(self):
        df = pd.DataFrame({"a": [Decimal("1.5"), 1000, -3, None]})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist()[0], "Nullable(Decimal(5, 1))")
        self.assertEqual(ret["v"].tolist()[:3], ["1.5", "1000", "-3"])

    def test_unsampled_rows_with_more_digits_are_exact(self):
        values = [Decimal("1.00")] * UNSAMPLED_ROWS
        values[1] = Decimal("12345678901234567890.125")
        df = pd.DataFrame({"a": values})
        ret = _query("SELECT toTypeName(a) AS t, toString(sum(a)) AS s FROM Python(df) GROUP BY t", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(Decimal(23, 3))"])
        self.assertEqual(ret["s"].tolist(), ["12345678901234597890.125"])

    def test_unsampled_integer_is_not_lost(self):
        values = [Decimal("0.5")] * UNSAMPLED_ROWS
        values[1] = 7
        df = pd.DataFrame({"a": values})
        ret = _query("SELECT toString(sum(a)) AS s, countIf(a IS NULL) AS n FROM Python(df)", df)
        self.assertEqual(ret["s"].tolist(), ["15007"])
        self.assertEqual(ret["n"].tolist(), [0])

    def test_unsampled_float_falls_back_to_string(self):
        values = [Decimal("0.5")] * UNSAMPLED_ROWS
        values[1] = 1.25
        df = pd.DataFrame({"a": values})
        ret = _query("SELECT toTypeName(a) AS t, a AS v FROM Python(df) LIMIT 2", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"] * 2)
        self.assertEqual(ret["v"].tolist(), ["0.5", "1.25"])

    def test_zero_with_large_exponent(self):
        df = pd.DataFrame({"a": [Decimal("1.5"), Decimal("0E+76"), Decimal("0E-80")]})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist()[0], "Nullable(Decimal(2, 1))")
        self.assertEqual(ret["v"].tolist(), ["1.5", "0", "0"])

    def test_integers_sampled_before_decimals_do_not_decide_the_type(self):
        df = pd.DataFrame({"a": pd.Series([1, 2**63, Decimal("0.5")], dtype=object)})
        ret = _query("SELECT toTypeName(a) AS t, toString(a) AS v FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist()[0], "Nullable(Decimal(20, 1))")
        self.assertEqual(ret["v"].tolist(), ["1", str(2**63), "0.5"])

    def test_every_nan_spelling_is_missing(self):
        df = pd.DataFrame({"a": [Decimal("1.5"), Decimal("NaN123"), Decimal("sNaN"), Decimal("-NaN")]})
        ret = _query("SELECT toTypeName(a) AS t, a IS NULL AS n FROM Python(df)", df)
        self.assertEqual(ret["t"].tolist()[0], "Nullable(Decimal(2, 1))")
        self.assertEqual(ret["n"].tolist(), [0, 1, 1, 1])

    def test_infinity_keeps_string_fallback(self):
        df = pd.DataFrame({"a": [Decimal("1.5"), Decimal("Infinity")]})
        ret = _query("SELECT toTypeName(a) AS t FROM Python(df) LIMIT 1", df)
        self.assertEqual(ret["t"].tolist(), ["Nullable(String)"])


if __name__ == "__main__":
    unittest.main()
