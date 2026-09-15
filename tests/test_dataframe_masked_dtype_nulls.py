#!/usr/bin/env python3
"""Python(df) nullability for pandas masked extension dtypes.

pandas nullable dtypes (Float64/Float32, Int*/UInt*, boolean) keep validity in
an explicit ``_mask``; the values sitting under a set mask bit are undefined.
``pyarrow.Table.to_pandas(types_mapper=...)`` leaves ``0`` / ``0.0`` there, not
a NaN sentinel, so any read path that infers nullability from the values buffer
silently turns NULL into 0 (chdb-core issue #225, floats only).

These tests pin the mask as the single source of truth for every masked dtype,
and keep the NaN-sentinel convention for plain numpy float columns, which have
no mask.
"""

import unittest

import numpy as np
import pandas as pd

import chdb

try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


def _arrow_to_masked(table, float_dtype=pd.Float64Dtype()):
    """Arrow -> pandas masked dtypes; nulls become a 0 sentinel under the mask."""

    def mapper(arrow_type):
        if pa.types.is_floating(arrow_type):
            return float_dtype
        if pa.types.is_boolean(arrow_type):
            return pd.BooleanDtype()
        if pa.types.is_integer(arrow_type):
            return pd.Int64Dtype()
        return None

    return table.to_pandas(types_mapper=mapper)


class TestMaskedDtypeNulls(unittest.TestCase):
    def setUp(self):
        self.conn = chdb.connect()

    def tearDown(self):
        self.conn.close()

    def assertMatchesPandas(self, result, expected, columns):
        """Compare a chdb DataFrame result against the pandas source, cell by cell."""
        self.assertEqual(list(result.columns), list(columns))
        self.assertEqual(len(result), len(expected))
        for col in columns:
            for i in range(len(expected)):
                want = expected[col].iloc[i]
                got = result[col].iloc[i]
                msg = "col={} row={} expected={!r} got={!r}".format(col, i, want, got)
                if pd.isna(want):
                    self.assertTrue(pd.isna(got), msg)
                else:
                    self.assertEqual(got, want, msg)

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_float64_null_under_zero_sentinel_reads_as_null(self):
        table = pa.table({"f": pa.array([1.5, None, 3.5], pa.float64())})
        df = _arrow_to_masked(table)  # noqa: F841 -- referenced by Python(df)

        # Precondition: the null slot holds 0.0, so a NaN check cannot see it.
        self.assertEqual(df["f"].array._data.tolist(), [1.5, 0.0, 3.5])
        self.assertEqual(df["f"].array._mask.tolist(), [False, True, False])

        result = self.conn.query(
            "SELECT f, isNull(f) AS f_null FROM Python(df)", "DataFrame"
        )
        self.assertTrue(pd.isna(result["f"].iloc[1]))
        self.assertEqual(result["f_null"].tolist(), [0, 1, 0])
        self.assertEqual(result["f"].iloc[0], 1.5)
        self.assertEqual(result["f"].iloc[2], 3.5)

        # Aggregates must skip the null instead of folding a fabricated 0.0 in.
        agg = self.conn.query(
            "SELECT count(f) AS c, sum(f) AS s, avg(f) AS a FROM Python(df)", "DataFrame"
        )
        self.assertEqual(agg["c"].iloc[0], int(df["f"].count()))
        self.assertAlmostEqual(agg["s"].iloc[0], float(df["f"].sum()), places=9)
        self.assertAlmostEqual(agg["a"].iloc[0], float(df["f"].mean()), places=9)

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_float32_null_under_zero_sentinel_reads_as_null(self):
        table = pa.table({"f": pa.array([1.5, None, 3.5], pa.float32())})
        df = _arrow_to_masked(table, pd.Float32Dtype())  # noqa: F841

        self.assertEqual(df["f"].array._data.tolist(), [1.5, 0.0, 3.5])

        result = self.conn.query(
            "SELECT f, isNull(f) AS f_null FROM Python(df)", "DataFrame"
        )
        self.assertEqual(result["f_null"].tolist(), [0, 1, 0])
        self.assertMatchesPandas(result[["f"]], df[["f"]].astype("Float32"), ["f"])

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_all_masked_dtypes_agree_with_pandas(self):
        table = pa.table(
            {
                "f64": pa.array([1.5, None, 3.5], pa.float64()),
                "f32": pa.array([1.5, None, 3.5], pa.float32()),
                "i64": pa.array([1, None, 3], pa.int64()),
                "b": pa.array([True, None, False], pa.bool_()),
            }
        )
        df = _arrow_to_masked(table)  # noqa: F841

        result = self.conn.query(
            "SELECT isNull(f64) AS f64, isNull(f32) AS f32, isNull(i64) AS i64,"
            " isNull(b) AS b FROM Python(df)",
            "DataFrame",
        )
        for col in ("f64", "f32", "i64", "b"):
            self.assertEqual(
                result[col].tolist(),
                [int(v) for v in df[col].isna().tolist()],
                "nullability mismatch for {}".format(col),
            )

    def test_unmasked_nan_stays_nan_in_masked_float_column(self):
        """A NaN the mask does not cover is a value, not a null -- as pandas reports it."""
        arr = pd.arrays.FloatingArray(
            np.array([1.5, np.nan, 3.5]), np.array([False, False, True])
        )
        df = pd.DataFrame({"f": arr})  # noqa: F841
        self.assertEqual(df["f"].isna().tolist(), [False, False, True])

        result = self.conn.query(
            "SELECT f, isNull(f) AS f_null, isNaN(assumeNotNull(f)) AS f_nan FROM Python(df)",
            "DataFrame",
        )
        self.assertEqual(result["f_null"].tolist(), [0, 0, 1])
        self.assertEqual(result["f_nan"].tolist(), [0, 1, 0])
        self.assertEqual(result["f"].iloc[0], 1.5)
        self.assertTrue(np.isnan(result["f"].iloc[1]))

    def test_plain_numpy_float_keeps_nan_as_null(self):
        """Unmasked numpy float columns have no mask; NaN remains the null sentinel."""
        df = pd.DataFrame({"f": [1.5, np.nan, 3.5]})  # noqa: F841
        self.assertEqual(df["f"].dtype, np.dtype("float64"))

        result = self.conn.query(
            "SELECT f, isNull(f) AS f_null FROM Python(df)", "DataFrame"
        )
        self.assertEqual(result["f_null"].tolist(), [0, 1, 0])
        self.assertMatchesPandas(result[["f"]], df, ["f"])

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_sliced_masked_float_uses_strided_mask(self):
        """A sliced frame gives a strided _mask; the slow path must honour it too."""
        values = [None if i % 3 == 0 else float(i) for i in range(20)]
        table = pa.table({"f": pa.array(values, pa.float64())})
        df = _arrow_to_masked(table).iloc[::2]  # noqa: F841

        self.assertNotEqual(df["f"].array._mask.strides[0], 1)

        result = self.conn.query("SELECT f FROM Python(df)", "DataFrame")
        self.assertMatchesPandas(result, df.reset_index(drop=True), ["f"])

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_masked_float_nulls_survive_selective_filter(self):
        """Low-selectivity filters take the per-row gather path; it must read the mask."""
        rows = 200000
        values = [None if i % 7 == 0 else float(i) for i in range(rows)]
        table = pa.table(
            {"f": pa.array(values, pa.float64()), "k": pa.array(list(range(rows)), pa.int64())}
        )
        df = _arrow_to_masked(table)  # noqa: F841

        result = self.conn.query(
            "SELECT count() AS c, countIf(f IS NULL) AS nulls, sum(f) AS s"
            " FROM Python(df) WHERE modulo(k, 1000) = 0",
            "DataFrame",
        )
        selected = df[df["k"] % 1000 == 0]
        self.assertEqual(result["c"].iloc[0], len(selected))
        self.assertEqual(result["nulls"].iloc[0], int(selected["f"].isna().sum()))
        self.assertAlmostEqual(result["s"].iloc[0], float(selected["f"].sum()), places=9)

        full = self.conn.query(
            "SELECT count() AS c, countIf(f IS NULL) AS nulls, sum(f) AS s FROM Python(df)",
            "DataFrame",
        )
        self.assertEqual(full["c"].iloc[0], len(df))
        self.assertEqual(full["nulls"].iloc[0], int(df["f"].isna().sum()))
        self.assertAlmostEqual(full["s"].iloc[0], float(df["f"].sum()), places=9)


if __name__ == "__main__":
    unittest.main(verbosity=2)
