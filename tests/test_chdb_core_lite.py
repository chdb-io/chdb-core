#!python3
"""Tests specific to the chdb-core-lite (slim) variant.

These run only when CHDB_LITE=1 is set in the environment. They verify that
functions trimmed from the lite build fail with a clean "function not found"
error (Code: 46 or 63) rather than crashing, and that core functionality is
preserved.

For the full chdb-core wheel, this whole class is skipped at setUpClass.
"""

import os
import unittest
import chdb


class TestChDBCoreLite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("CHDB_LITE") != "1":
            raise unittest.SkipTest("Lite-only tests; set CHDB_LITE=1 to run")

    def _assert_not_found(self, sql):
        with self.assertRaises(Exception) as ctx:
            chdb.query(sql)
        msg = str(ctx.exception)
        self.assertTrue(
            "Code: 46" in msg or "Code: 63" in msg,
            f"expected UNKNOWN_FUNCTION/UNKNOWN_AGGREGATE_FUNCTION, got: {msg[:200]}",
        )

    def test_trimmed_scalar_functions_throw_cleanly(self):
        for sql in [
            "SELECT murmurHash3_64('x')",          # FunctionsHashingMurmur
            "SELECT ngramSimHash('hello world')",  # FunctionsStringHash (LSH)
            "SELECT bitmapBuild([1, 2, 3])",       # FunctionsBitmap
            "SELECT bitShiftLeft(1, 2)",           # bit-shift trim
            "SELECT gcd(12, 8)",                   # math trim
            "SELECT FQDN()",                       # niche fn
            "SELECT filesystemAvailable()",        # niche fn
        ]:
            with self.subTest(sql=sql):
                self._assert_not_found(sql)

    def test_trimmed_array_and_map_functions_throw_cleanly(self):
        for sql in [
            "SELECT arrayCumSum([1, 2, 3])",
            "SELECT arrayShuffle([1, 2, 3])",
            "SELECT mapAdd(map(1, 1), map(1, 2))",
            "SELECT indexOfAssumeSorted([1, 2, 3], 2)",
        ]:
            with self.subTest(sql=sql):
                self._assert_not_found(sql)

    def test_array_fold_is_available(self):
        for values, expected in [
            ("[toInt64(2), -3, 4]", b"-24\n"),
            ("[toInt64(2), 0, 4]", b"0\n"),
            ("CAST([], 'Array(Int64)')", b"1\n"),
        ]:
            with self.subTest(values=values):
                result = chdb.query(
                    f"SELECT arrayFold((acc, v) -> acc * v, {values}, toInt64(1))",
                    "CSV",
                )
                self.assertEqual(result.bytes(), expected)

    def test_grouped_product_uses_array_fold(self):
        # DataStore v4.5.0 lowers groupby().prod() to arrayFold over groupArray.
        result = chdb.query(
            "SELECT g, arrayFold((acc, v) -> acc * v, groupArray(v), toInt64(1)) "
            "FROM values('g String, v Nullable(Int64)', "
            "('a', 2), ('a', -3), ('a', NULL), ('b', 4), ('b', 0)) "
            "GROUP BY g ORDER BY g",
            "CSV",
        )
        self.assertEqual(result.bytes(), b'"a",-6\n"b",0\n')

    def test_trimmed_aggregates_throw_cleanly(self):
        # Keep this list aligned with src/AggregateFunctions/CMakeLists.txt drop list.
        # corr/covar/skew/kurt/topK/windowFunnel/groupUniqArray are KEPT in lite
        # because upstream DataStore tests use them.
        for sql in [
            "SELECT groupBitOr(toUInt32(number)) FROM numbers(10)",
            "SELECT sumMap(['a'], [1])",
            "SELECT groupArrayIntersect([1, 2, 3])",
            "SELECT groupBitmap(toUInt32(number)) FROM numbers(10)",
            "SELECT exponentialMovingAverage(0.5)(number, number) FROM numbers(10)",
        ]:
            with self.subTest(sql=sql):
                self._assert_not_found(sql)

    def test_trimmed_big_int_conversions_throw_cleanly(self):
        # toInt128/256, toUInt128/256, toDecimal128/256, toBFloat16 are removed
        # in lite; the corresponding UDF tests in test_func_udf_types.py are
        # skipped via @unittest.skipIf(_LITE).
        for sql in [
            "SELECT toInt128('1')",
            "SELECT toInt256('1')",
            "SELECT toUInt128('1')",
            "SELECT toUInt256('1')",
            "SELECT toDecimal128('1.5', 2)",
            "SELECT toDecimal256('1.5', 2)",
            "SELECT toBFloat16(1.0)",
        ]:
            with self.subTest(sql=sql):
                self._assert_not_found(sql)


if __name__ == "__main__":
    unittest.main()
