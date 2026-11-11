#!python3

import os
import subprocess
import sys
import textwrap
import unittest
from decimal import Decimal

import pandas as pd
import chdb


class TestIssue673(unittest.TestCase):
    """DataFrame output keeps Decimal values exact, as decimal.Decimal objects like pyarrow's to_pandas()."""

    def test_top_level_values_are_exact(self):
        df = chdb.query(
            "SELECT toDecimal128('12345678901234597890.25', 2) AS d, "
            "toDecimal64('0.1', 1) + toDecimal64('0.2', 1) AS s, "
            "toDecimal32('3.30', 2) AS t",
            "DataFrame",
        )
        self.assertEqual(df["d"].dtype, object)
        self.assertEqual(df["d"].tolist(), [Decimal("12345678901234597890.25")])
        self.assertEqual(df["s"].tolist(), [Decimal("0.3")])
        # The scale is kept: 3.30 has two fractional digits, as in pyarrow.
        self.assertEqual(str(df["t"][0]), "3.30")

    def test_decimal256(self):
        df = chdb.query("SELECT toDecimal256('-1234567890123456789012345678901234567890.5', 1) AS d", "DataFrame")
        self.assertEqual(df["d"].tolist(), [Decimal("-1234567890123456789012345678901234567890.5")])

    def test_null_reads_as_missing(self):
        df = chdb.query("SELECT CAST(number = 1 ? NULL : number, 'Nullable(Decimal(10, 2))') AS d FROM numbers(3)", "DataFrame")
        self.assertEqual(df["d"].isna().tolist(), [False, True, False])
        # Like chDB's other object columns (UUID), a NULL is pd.NA.
        self.assertIs(df["d"][1], pd.NA)
        self.assertEqual([df["d"][0], df["d"][2]], [Decimal("0.00"), Decimal("2.00")])

    def test_nested_values_are_exact(self):
        df = chdb.query(
            "SELECT [toDecimal64('1.10', 2), toDecimal64('2.25', 2)] AS a, "
            "tuple(toDecimal128('0.000000000000000001', 18)) AS t",
            "DataFrame",
        )
        self.assertEqual(list(df["a"][0]), [Decimal("1.10"), Decimal("2.25")])
        self.assertEqual(tuple(df["t"][0]), (Decimal("0.000000000000000001"),))

    def test_map_values_are_exact(self):
        df = chdb.query("SELECT map('k', toDecimal64('1.10', 2)) AS m", "DataFrame")
        self.assertEqual(df["m"][0], {"k": Decimal("1.10")})
        self.assertEqual(str(df["m"][0]["k"]), "1.10")

    def test_conversion_logs_no_errors(self):
        # Runs in its own process: logging is a startup option of the engine.
        body = textwrap.dedent("""
            import chdb
            conn = chdb.connect(":memory:?logger.console=1&log-level=error")
            df = conn.query("SELECT toDecimal128(number, 2) / 3 AS d FROM numbers(1000)", "DataFrame")
            assert len(df) == 1000
            conn.close()
            print("OK")
        """)
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
        proc = subprocess.run([sys.executable, "-c", body], env=env, capture_output=True, text=True, timeout=300)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout)
        self.assertNotIn("neither finalized nor canceled", proc.stdout + proc.stderr)

    def test_matches_arrow_output(self):
        sql = "SELECT toDecimal128(number, 4) / 3 AS d FROM numbers(5)"
        self.assertEqual(
            chdb.query(sql, "DataFrame")["d"].tolist(),
            chdb.query(sql, "ArrowTable").to_pandas()["d"].tolist(),
        )

    def test_round_trip_through_python_table(self):
        df = chdb.query("SELECT toDecimal64(number, 2) / 4 AS d FROM numbers(4)", "DataFrame")
        ret = chdb.query("SELECT toTypeName(d) AS t, toString(sum(d)) AS s FROM Python(df) GROUP BY t", "DataFrame")
        self.assertEqual(ret["t"].tolist(), ["Nullable(Decimal(2, 2))"])
        self.assertEqual(ret["s"].tolist(), ["1.5"])


if __name__ == "__main__":
    unittest.main()
