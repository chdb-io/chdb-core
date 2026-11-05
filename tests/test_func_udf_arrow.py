#!python3

import datetime
import os
import unittest
from decimal import Decimal

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

import chdb
from chdb import func
from chdb.session import Session
from chdb.sqltypes import BOOL, DATE, FLOAT64, INT32, INT64, STRING, UINT8

try:
    import polars as pl
except ImportError:
    pl = None

# chdb-core-lite drops the Int128/256 and UInt128/256 conversion functions the big
# integer test below builds its input with; see tests/test_func_udf_types.py.
_LITE = os.environ.get("CHDB_LITE") == "1"


def csv(session, sql):
    return str(session.query(sql, "CSV")).strip()


def recorder(seen):
    def identity(x):
        seen.append(type(x))
        return x

    return identity


def typed_recorder(seen):
    def record(x):
        seen.append((x.type, x.to_pylist()))
        return pa.array(["ok"] * len(x))

    return record


class ArrowUDFTestCase(unittest.TestCase):
    def setUp(self):
        self.session = Session()
        self.registered = []

    def tearDown(self):
        for name in self.registered:
            chdb.drop_function(name)
        self.session.close()

    def create(self, name, fn, arg_types=None, return_type=None, **kwargs):
        kwargs.setdefault("type", "arrow")
        chdb.create_function(name, fn, arg_types, return_type, **kwargs)
        self.registered.append(name)

    def query(self, sql):
        return csv(self.session, sql)


class TestArrowUDFBasics(ArrowUDFTestCase):
    def test_receives_chunked_arrays(self):
        seen = []

        def plus_one(x):
            seen.append((type(x), x.type, len(x)))
            return pc.add(x, 1)

        self.create("arrow_plus_one", plus_one, [INT64], INT64)
        self.assertEqual(self.query("SELECT arrow_plus_one(toInt64(number)) FROM numbers(5)"), "1\n2\n3\n4\n5")
        self.assertEqual(seen, [(pa.ChunkedArray, pa.int64(), 5)])

    def test_called_once_per_block(self):
        sizes = []

        def double(x):
            sizes.append(len(x))
            return pc.multiply(x, 2)

        self.create("arrow_double", double, [INT64], INT64)
        ret = self.query(
            "SELECT sum(arrow_double(toInt64(number))) FROM numbers(10000) "
            "SETTINGS max_block_size = 1000, max_threads = 1")
        self.assertEqual(ret, str(2 * sum(range(10000))))
        self.assertEqual(sizes, [1000] * 10)

    def test_multiple_arguments_and_constants(self):
        def combine(a, b, c):
            return pc.add(pc.multiply(a, b), c)

        self.create("arrow_combine", combine, [INT64, INT64, INT64], INT64)
        ret = self.query("SELECT arrow_combine(toInt64(number), 10, toInt64(number)) FROM numbers(4)")
        self.assertEqual(ret, "0\n11\n22\n33")
        # Every argument constant: still one value per row.
        self.assertEqual(self.query("SELECT arrow_combine(2, 3, 4)"), "10")
        self.assertEqual(self.query("SELECT arrow_combine(2, 3, 4) FROM numbers(3)"), "10\n10\n10")

    def test_matches_native_udf(self):
        def native(x, y):
            return x * 3 - y

        def arrow(x, y):
            return pc.subtract(pc.multiply(x, 3), y)

        self.create("udf_native_ref", native, [INT64, INT64], INT64, type="native")
        self.create("udf_arrow_ref", arrow, [INT64, INT64], INT64)
        sql = (
            "SELECT count(), sum({f}(toInt64(number), toInt64(number % 7))) "
            "FROM numbers(200000)")
        self.assertEqual(self.query(sql.format(f="udf_arrow_ref")), self.query(sql.format(f="udf_native_ref")))
        ret = self.query(
            "SELECT countIf(udf_arrow_ref(a, b) != udf_native_ref(a, b)) FROM "
            "(SELECT toInt64(number) AS a, toInt64(number % 13) AS b FROM numbers(100000))")
        self.assertEqual(ret, "0")

    def test_parallel_execution(self):
        # numbers_mt feeds several threads, which then call the function concurrently.
        self.create("arrow_inc", lambda x: pc.add(x, 1), [INT64], INT64)
        ret = self.query(
            "SELECT sum(arrow_inc(toInt64(number))) FROM numbers_mt(2000000) SETTINGS max_threads = 8")
        self.assertEqual(ret, str(sum(range(2000000)) + 2000000))

    def test_low_cardinality_argument(self):
        seen = []

        def parse(s):
            seen.extend(s.to_pylist())
            return pc.cast(s, pa.int64())

        self.create("arrow_lc_parse", parse, [STRING], INT64)
        # Only the values of the rows that pass the filter reach the function, not the
        # LowCardinality dictionary, which still holds 'bad' (and the default value '').
        ret = self.query(
            "SELECT arrow_lc_parse(x) FROM "
            "(SELECT toLowCardinality(arrayJoin(['1', 'bad', '2', '1'])) AS x) WHERE x != 'bad'")
        self.assertEqual(ret, "1\n2\n1")
        self.assertEqual(seen, ["1", "2", "1"])

    def test_in_where_group_by_and_order_by(self):
        self.create("arrow_mod3", lambda x: pc.bit_wise_and(x, 3), [INT64], INT64)
        ret = self.query(
            "SELECT arrow_mod3(toInt64(number)) AS k, count() FROM numbers(100) "
            "WHERE arrow_mod3(toInt64(number)) != 0 GROUP BY k ORDER BY k")
        self.assertEqual(ret, "1,25\n2,25\n3,25")

    def test_func_decorator(self):
        @func([INT64], INT64, type="arrow")
        def arrow_decorated(x):
            return pc.multiply(x, 10)

        self.registered.append("arrow_decorated")
        self.assertEqual(self.query("SELECT arrow_decorated(toInt64(4))"), "40")
        # The decorated function is still plain Python.
        self.assertEqual(arrow_decorated(pa.array([1, 2])).to_pylist(), [10, 20])

    def test_type_spellings(self):
        for i, udf_type in enumerate(["arrow", "Arrow", "ARROW", chdb.UDFType.ARROW]):
            name = f"arrow_spelling_{i}"
            seen = []
            chdb.create_function(name, recorder(seen), [INT64], INT64, type=udf_type)
            self.registered.append(name)
            self.assertEqual(self.query(f"SELECT {name}(materialize(toInt64(7)))"), "7")
            self.assertEqual(seen, [pa.ChunkedArray])

        for i, udf_type in enumerate([None, "native", chdb.UDFType.NATIVE]):
            name = f"native_spelling_{i}"
            seen = []
            chdb.create_function(name, recorder(seen), [INT64], INT64, type=udf_type)
            self.registered.append(name)
            self.assertEqual(self.query(f"SELECT {name}(materialize(toInt64(7)))"), "7")
            self.assertEqual(seen, [int])

    def test_invalid_type(self):
        with self.assertRaisesRegex(RuntimeError, "type must be 'native' or 'arrow'"):
            chdb.create_function("arrow_bad_type", lambda x: x, [INT64], INT64, type="vectorized")
        with self.assertRaisesRegex(RuntimeError, "type must be a UDFType instance or a string"):
            chdb.create_function("arrow_bad_type", lambda x: x, [INT64], INT64, type=1)

    def test_types_inferred_from_annotations(self):
        seen = []

        def scale(x: int, factor: float) -> float:
            seen.append((x.type, factor.type))
            return pc.multiply(x, factor)

        self.create("arrow_scale", scale)
        self.assertEqual(self.query("SELECT arrow_scale(materialize(toInt32(3)), 1.5)"), "4.5")
        self.assertEqual(seen, [(pa.int64(), pa.float64())])

    def test_varargs(self):
        def first(*columns):
            return columns[0]

        self.create("arrow_first", first, return_type=INT64)
        self.assertEqual(self.query("SELECT arrow_first(toInt64(number), 'x', 1.5) FROM numbers(3)"), "0\n1\n2")
        with self.assertRaisesRegex(RuntimeError, "needs at least one argument"):
            self.query("SELECT arrow_first()")

    def test_zero_parameters_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "must take at least one argument"):
            chdb.create_function("arrow_no_args", lambda: [1], return_type=INT64, type="arrow")
        # Nothing was registered.
        with self.assertRaises(RuntimeError):
            self.query("SELECT arrow_no_args()")

    def test_drop_and_recreate(self):
        chdb.create_function("arrow_temp", lambda x: pc.add(x, 1), [INT64], INT64, type="arrow")
        self.assertEqual(self.query("SELECT arrow_temp(toInt64(1))"), "2")
        with self.assertRaises(RuntimeError):
            chdb.create_function("arrow_temp", lambda x: x, [INT64], INT64, type="arrow")
        chdb.drop_function("arrow_temp")
        with self.assertRaises(RuntimeError):
            self.query("SELECT arrow_temp(toInt64(1))")
        # The name is free again, for either kind.
        self.create("arrow_temp", lambda x: x * 3, [INT64], INT64, type="native")
        self.assertEqual(self.query("SELECT arrow_temp(toInt64(1))"), "3")


class TestArrowUDFResults(ArrowUDFTestCase):
    def check(self, name, fn, return_type, expected, sql="SELECT {name}(toInt64(number)) FROM numbers(4)"):
        self.create(name, fn, [INT64], return_type)
        self.assertEqual(self.query(sql.format(name=name)), expected)

    def test_accepted_result_kinds(self):
        cases = {
            "res_array": lambda x: x.combine_chunks(),
            "res_chunked": lambda x: x,
            "res_two_chunks": lambda x: pa.chunked_array([x.combine_chunks()[:1], x.combine_chunks()[1:]]),
            "res_list": lambda x: x.to_pylist(),
            "res_numpy": lambda x: x.to_numpy(),
            "res_pandas": lambda x: x.to_pandas(),
            "res_table": lambda x: pa.table({"only": x}),
            "res_record_batch": lambda x: pa.record_batch([x.combine_chunks()], names=["only"]),
        }
        for name, fn in cases.items():
            with self.subTest(name):
                self.check(name, fn, INT64, "0\n1\n2\n3")

    def test_cast_to_declared_return_type(self):
        self.check("res_to_int32", lambda x: pc.multiply(x, 1000), INT32, "0\n1000\n2000\n3000")
        self.check("res_str_to_int", lambda x: pc.cast(x, pa.string()), INT64, "0\n1\n2\n3")
        self.check("res_int_to_str", lambda x: x, STRING, '"0"\n"1"\n"2"\n"3"')
        self.check("res_int_to_float", lambda x: x, FLOAT64, "0\n1\n2\n3")
        self.check("res_whole_float_to_int", lambda x: pc.cast(x, pa.float64()), INT64, "0\n1\n2\n3")
        # Losing float precision is fine: NumPy results are float64.
        self.check("res_float_to_float32", lambda x: pc.add(pc.cast(x, pa.float64()), 0.1), "Float32",
                   "0.1\n1.1\n2.1\n3.1")

    def test_values_that_do_not_fit_are_rejected(self):
        self.create("res_overflow", lambda x: pc.multiply(x, 10**10), [INT64], INT32)
        with self.assertRaisesRegex(RuntimeError, "cannot be safely converted"):
            self.query("SELECT res_overflow(toInt64(number)) FROM numbers(3)")

        self.create("res_negative", lambda x: pc.subtract(x, 5), [INT64], UINT8)
        with self.assertRaisesRegex(RuntimeError, "cannot be safely converted"):
            self.query("SELECT res_negative(toInt64(number)) FROM numbers(3)")

        self.create("res_fraction", lambda x: pc.divide(pc.cast(x, pa.float64()), 2), [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "cannot be safely converted"):
            self.query("SELECT res_fraction(toInt64(1))")

        self.create("res_bad_string", lambda x: pa.array(["abc"] * len(x)), [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "Cannot parse string 'abc' as Int64"):
            self.query("SELECT res_bad_string(toInt64(1))")

    def test_wrong_number_of_rows(self):
        self.create("res_too_few", lambda x: x.combine_chunks()[:1], [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "returned 1 rows for a batch of 3 rows"):
            self.query("SELECT res_too_few(toInt64(number)) FROM numbers(3)")

        self.create("res_too_many", lambda x: pa.chunked_array(x.chunks * 2), [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "returned 6 rows for a batch of 3 rows"):
            self.query("SELECT res_too_many(toInt64(number)) FROM numbers(3)")

    def test_unusable_results(self):
        self.create("res_two_columns", lambda x: pa.table({"a": x, "b": x}), [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "returned a Table with 2 columns, expected exactly one"):
            self.query("SELECT res_two_columns(toInt64(1))")

        self.create("res_none", lambda x: None, [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "returned None, expected an array with one value per row"):
            self.query("SELECT res_none(toInt64(1))")

        self.create("res_object", lambda x: object(), [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "cannot convert the returned object to an Arrow array"):
            self.query("SELECT res_object(toInt64(1))")

        self.create("res_mixed", lambda x: [1, "a"], [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "cannot convert the returned list to an Arrow array"):
            self.query("SELECT res_mixed(toInt64(number)) FROM numbers(2)")

    def test_nulls_and_nan_in_result(self):
        self.check("res_nulls", lambda x: pa.nulls(len(x)), INT64, "\\N\n\\N\n\\N\n\\N")
        self.check(
            "res_some_nulls",
            lambda x: pc.if_else(pc.equal(pc.bit_wise_and(x, 1), 1), pa.scalar(None, pa.int64()), x),
            INT64, "0\n\\N\n2\n\\N")
        # As for the per-row UDF, a float NaN becomes NULL.
        self.check("res_nan", lambda x: pa.array([float("nan"), 1.5, float("nan"), 2.5]), FLOAT64,
                   "\\N\n1.5\n\\N\n2.5")
        self.check("res_nan_float32", lambda x: np.array([np.nan, 1.5, 2.5, np.nan]), "Float32",
                   "\\N\n1.5\n2.5\n\\N")
        # Whatever the return type, as the per-row UDF does.
        self.check("res_nan_int", lambda x: np.array([np.nan, 2.0, np.nan, 4.0]), INT64, "\\N\n2\n\\N\n4")
        self.check("res_nan_string", lambda x: np.array([np.nan, 2.5, np.nan, 1.0]), STRING,
                   '\\N\n"2.5"\n\\N\n"1"')

    @unittest.skipIf(_LITE, "chdb-core-lite drops Int128/256, UInt128/256 conversions")
    def test_python_ints_for_wide_integer_results(self):
        # pyarrow cannot hold a Python int wider than 64 bits, so these go through their text.
        self.check("res_wide_ints", lambda x: [1, 2**70, None, -(2**100)], "Int128",
                   "1\n1180591620717411303424\n\\N\n-1267650600228229401496703205376")
        self.create("res_wide_too_big", lambda x: [2**256], [INT64], "UInt256")
        with self.assertRaisesRegex(RuntimeError, "does not fit into UInt256"):
            self.query("SELECT res_wide_too_big(toInt64(1))")

    def test_uint64_list_beyond_int64(self):
        # pyarrow infers int64 from the first value and overflows on the second one; the
        # return type settles it.
        self.check("res_uint64_list", lambda x: [0, 2**64 - 1, 1, 2**63], "UInt64",
                   "0\n18446744073709551615\n1\n9223372036854775808")

    def test_results_from_other_arrow_producers(self):
        # Dictionary-encoded and 64-bit-offset strings decode like plain ones.
        self.check("res_dictionary", lambda x: pc.cast(pc.bit_wise_and(x, 1), pa.string()).dictionary_encode(),
                   STRING, '"0"\n"1"\n"0"\n"1"')
        self.check("res_large_string", lambda x: pc.cast(pc.cast(x, pa.string()), pa.large_string()),
                   STRING, '"0"\n"1"\n"2"\n"3"')

    @unittest.skipIf(pl is None, "polars is not installed")
    def test_polars_series_result(self):
        self.check("res_polars", lambda x: pl.Series(x.to_pylist()) * 2, INT64, "0\n2\n4\n6")

    def test_naive_datetime_is_read_in_the_return_type_time_zone(self):
        # A naive datetime names its wall clock in the time zone of the return type, as ClickHouse
        # parses '2024-01-02 03:04:05' into a column of that type: it reads back unchanged.
        naive = lambda x: pa.array([datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)] * len(x))  # noqa: E731
        self.check(
            "res_naive_zoned", naive, "DateTime64(6, 'Asia/Shanghai')",
            '"2024-01-02 03:04:05.123456","2024-01-01 19:04:05.123456"',
            sql="SELECT toString({name}(toInt64(1))), toString({name}(toInt64(1)), 'UTC')")
        self.check(
            "res_naive_seconds", naive, "DateTime('Asia/Shanghai')",
            '"2024-01-02 03:04:05","2024-01-01 19:04:05"',
            sql="SELECT toString({name}(toInt64(1))), toString({name}(toInt64(1)), 'UTC')")
        self.check(
            "res_naive_implicit", naive, "DateTime64(6)",
            '"2024-01-02 03:04:05.123456"',
            sql="SELECT toString({name}(toInt64(1)))")
        # Other return types take the session time zone.
        self.session.query("SET session_timezone = 'Asia/Shanghai'")
        self.check("res_naive_date", naive, DATE, '"2024-01-02"', sql="SELECT {name}(toInt64(1))")
        # A time zone the datetime carries is kept.
        self.check(
            "res_aware",
            lambda x: pa.array([datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)] * len(x)),
            "DateTime",
            '"2024-01-02 03:04:05"',
            sql="SELECT toString({name}(toInt64(1)), 'UTC')")

    def test_bool_and_string_results(self):
        self.check("res_bool", lambda x: pc.greater(x, 1), BOOL, "false\nfalse\ntrue\ntrue")
        self.check("res_upper", lambda x: pc.utf8_upper(pc.cast(x, pa.string())), STRING, '"0"\n"1"\n"2"\n"3"')
        self.check(
            "res_binary",
            lambda x: pa.array([b"\xff\x00"] * len(x), pa.binary()),
            STRING,
            "1",
            sql="SELECT countIf({name}(toInt64(number)) = unhex('ff00')) = 4 FROM numbers(4)")

    def test_temporal_results(self):
        utc = datetime.timezone.utc
        self.check("res_date", lambda x: pa.array([datetime.date(2024, 1, 2)] * len(x)), DATE,
                   '"2024-01-02"', sql="SELECT {name}(toInt64(1))")
        self.check("res_date_from_string", lambda x: pa.array(["2024-01-02"] * len(x)), DATE,
                   '"2024-01-02"', sql="SELECT {name}(toInt64(1))")
        self.check(
            "res_timestamp",
            lambda x: pa.array([datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=utc)] * len(x)),
            "DateTime64(6)",
            '"2024-01-02 03:04:05.123456"',
            sql="SELECT toString({name}(toInt64(1)), 'UTC')")
        self.check(
            "res_epoch_seconds",
            lambda x: x,
            "DateTime",
            '"2023-11-14 22:13:20"',
            sql="SELECT toString({name}(toInt64(1700000000)), 'UTC')")


class TestArrowUDFNullAndErrorHandling(ArrowUDFTestCase):
    def test_skip_leaves_null_rows_out(self):
        seen = []

        def tracked(x):
            seen.append((x.to_pylist(), x.null_count))
            return pc.multiply(x, 10)

        self.create("arrow_skip", tracked, [INT64], INT64)
        ret = self.query(
            "SELECT arrow_skip(x) FROM (SELECT CAST(arrayJoin([1, NULL, 3, NULL, 5]) AS Nullable(Int64)) AS x)")
        self.assertEqual(ret, "10\n\\N\n30\n\\N\n50")
        self.assertEqual(seen, [([1, 3, 5], 0)])

    def test_skip_with_several_arguments(self):
        seen = []

        def add(a, b):
            seen.append((a.to_pylist(), b.to_pylist()))
            return pc.add(a, b)

        self.create("arrow_skip_add", add, [INT64, INT64], INT64, on_null="skip")
        self.session.query(
            "CREATE TABLE arrow_null_pairs (id Int64, a Nullable(Int64), b Nullable(Int64)) ENGINE = Memory")
        self.session.query(
            "INSERT INTO arrow_null_pairs VALUES (1, 1, 10), (2, NULL, 20), (3, 3, NULL), (4, NULL, NULL), (5, 5, 50)")
        try:
            ret = self.query("SELECT arrow_skip_add(a, b) FROM arrow_null_pairs ORDER BY id")
        finally:
            self.session.query("DROP TABLE arrow_null_pairs")
        self.assertEqual(ret, "11\n\\N\n\\N\n\\N\n55")
        self.assertEqual(seen, [([1, 5], [10, 50])])

    def test_skip_does_not_call_for_all_null_block(self):
        calls = []

        def tracked(x):
            calls.append(len(x))
            return x

        self.create("arrow_skip_all_null", tracked, [INT64], INT64)
        self.assertEqual(self.query("SELECT arrow_skip_all_null(NULL) FROM numbers(3)"), "\\N\n\\N\n\\N")
        self.assertEqual(calls, [])

    def test_pass_hands_over_nulls(self):
        seen = []

        def fill(x):
            seen.append((x.to_pylist(), x.null_count))
            return pc.fill_null(x, -1)

        self.create("arrow_pass", fill, [INT64], INT64, on_null="pass")
        ret = self.query(
            "SELECT arrow_pass(x) FROM (SELECT CAST(arrayJoin([1, NULL, 3]) AS Nullable(Int64)) AS x)")
        self.assertEqual(ret, "1\n-1\n3")
        self.assertEqual(seen, [([1, None, 3], 1)])

    def test_pass_null_literal_takes_the_declared_type(self):
        seen = []

        def fill(x):
            seen.append(x.type)
            return pc.fill_null(x, 0)

        self.create("arrow_pass_literal", fill, [INT64], INT64, on_null=chdb.NullHandling.PASS)
        self.assertEqual(self.query("SELECT arrow_pass_literal(materialize(NULL))"), "0")
        self.assertEqual(seen, [pa.int64()])

    def test_error_propagates(self):
        def boom(x):
            raise ValueError("boom in arrow udf")

        self.create("arrow_boom", boom, [INT64], INT64)
        with self.assertRaisesRegex(RuntimeError, "raised an exception on a batch of 3 rows: ValueError: boom in arrow udf"):
            self.query("SELECT arrow_boom(toInt64(number)) FROM numbers(3)")

    def test_error_ignore_nulls_the_failing_block_only(self):
        def picky(x):
            if pc.any(pc.equal(x, 1500)).as_py():
                raise ValueError("no 1500 please")
            return x

        self.create("arrow_picky", picky, [INT64], INT64, on_error="ignore")
        ret = self.query(
            "SELECT countIf(v IS NULL), countIf(v IS NOT NULL), sum(v) FROM "
            "(SELECT arrow_picky(toInt64(number)) AS v FROM numbers(3000)) "
            "SETTINGS max_block_size = 1000, max_threads = 1")
        self.assertEqual(ret, f"1000,2000,{sum(range(1000)) + sum(range(2000, 3000))}")

    def test_pass_with_nulls_in_different_arguments(self):
        seen = []

        def add(a, b):
            seen.append((a.to_pylist(), b.to_pylist()))
            return pc.add(pc.fill_null(a, 0), pc.fill_null(b, 0))

        self.create("arrow_pass_add", add, [INT64, INT64], INT64, on_null="pass")
        ret = self.query(
            "SELECT arrow_pass_add(a, b) FROM values('a Nullable(Int64), b Nullable(Int64)', "
            "(1, NULL), (NULL, 2), (NULL, NULL), (3, 4))")
        self.assertEqual(ret, "1\n2\n0\n7")
        self.assertEqual(seen, [([1, None, None, 3], [None, 2, None, 4])])

    def test_pass_and_ignore(self):
        def boom(x):
            raise ValueError("boom")

        self.create("arrow_pass_ignore", boom, [INT64], INT64, on_null="pass", on_error="ignore")
        ret = self.query("SELECT arrow_pass_ignore(a) FROM values('a Nullable(Int64)', (1), (NULL), (3))")
        self.assertEqual(ret, "\\N\n\\N\n\\N")

    def test_skip_drops_variant_and_dynamic_nulls(self):
        # A NULL inside a Variant or Dynamic column is a NULL argument too, as for the per-row UDF.
        seen = []

        def record(x):
            seen.append(x.to_pylist())
            return pa.array(["ok"] * len(x))

        self.create("arrow_skip_variant", record, return_type=STRING)
        ret = self.query(
            "SELECT arrow_skip_variant(v) FROM (SELECT arrayJoin([1::UInt64::Variant(UInt64, String), NULL, "
            "'x'::Variant(UInt64, String)]) AS v)")
        self.assertEqual(ret, '"ok"\n\\N\n"ok"')
        self.assertEqual(seen, [[1, "x"]])

        seen.clear()
        ret = self.query("SELECT arrow_skip_variant(v) FROM (SELECT arrayJoin([1::Dynamic, NULL::Dynamic]) AS v)")
        self.assertEqual(ret, '"ok"\n\\N')
        self.assertEqual(len(seen), 1)
        self.assertEqual(len(seen[0]), 1)

    def test_error_ignore_does_not_hide_conversion_errors(self):
        self.create("arrow_ignore_short", lambda x: [1], [INT64], INT64, on_error="ignore")
        with self.assertRaisesRegex(RuntimeError, "returned 1 rows for a batch of 3 rows"):
            self.query("SELECT arrow_ignore_short(toInt64(number)) FROM numbers(3)")


class TestArrowUDFBlocks(ArrowUDFTestCase):
    def test_empty_block_does_not_call(self):
        calls = []

        def tracked(x):
            calls.append(len(x))
            return x

        self.create("arrow_empty", tracked, [INT64], INT64)
        self.assertEqual(self.query("SELECT arrow_empty(toInt64(number)) FROM numbers(10) WHERE number > 100"), "")
        self.assertEqual(calls, [])

    def test_large_block_arrives_in_chunks(self):
        # A block holding more than output_format_arrow_max_record_batch_size_bytes (by default what
        # 32-bit Arrow offsets address) reaches the function as ChunkedArrays of several chunks.
        seen = []

        def lengths(text, data):
            seen.append((text.num_chunks, data.num_chunks, len(text)))
            return pc.add(pc.utf8_length(text), pc.binary_length(data))

        self.create("arrow_large_block", lengths, [STRING, bytes], INT64)
        ret = self.query(
            "SELECT sum(arrow_large_block(s, s)) FROM (SELECT repeat('x', 1000) AS s FROM numbers(1000)) "
            "SETTINGS max_block_size = 1000, output_format_arrow_max_record_batch_size_bytes = 100000")
        self.assertEqual(ret, str(2 * 1000 * 1000))
        self.assertEqual(len(seen), 1)
        text_chunks, data_chunks, rows = seen[0]
        self.assertGreater(text_chunks, 1)
        self.assertGreater(data_chunks, 1)
        self.assertEqual(rows, 1000)

    def test_merge_tree_join_and_filter(self):
        self.create("arrow_twice", lambda x: pc.multiply(x, 2), [FLOAT64], FLOAT64)
        self.session.query("CREATE TABLE arrow_mt (k Int64, v Float64) ENGINE = MergeTree ORDER BY k")
        try:
            self.session.query("INSERT INTO arrow_mt SELECT number, number / 2 FROM numbers(100000)")
            ret = self.query(
                "SELECT count(), sum(arrow_twice(a.v)) FROM arrow_mt AS a JOIN arrow_mt AS b ON a.k = b.k "
                "WHERE arrow_twice(b.v) > 10")
        finally:
            self.session.query("DROP TABLE arrow_mt")
        # arrow_twice(v) = k, so the filter keeps k > 10.
        self.assertEqual(ret, f"{100000 - 11},{sum(range(11, 100000))}")


class TestArrowUDFArgumentTypes(ArrowUDFTestCase):
    def check_arg(self, name, declared, expr, expected_type, expected_values):
        seen = []

        def record(x):
            seen.append((x.type, x.to_pylist()))
            return pa.array(["ok"] * len(x))

        self.create(name, record, None if declared is None else [declared], STRING)
        # materialize() keeps a constant argument from also being evaluated at analysis time.
        self.assertEqual(self.query(f"SELECT {name}(materialize({expr}))"), '"ok"')
        self.assertEqual(seen, [(expected_type, expected_values)])

    def test_declared_numeric_types(self):
        for i, (declared, expr, arrow_type, values) in enumerate([
            ("Int8", "toInt8(-5)", pa.int8(), [-5]),
            ("UInt16", "toUInt16(65535)", pa.uint16(), [65535]),
            ("Int32", "toInt32(-7)", pa.int32(), [-7]),
            ("UInt64", "toUInt64(18446744073709551615)", pa.uint64(), [18446744073709551615]),
            ("Float32", "toFloat32(1.5)", pa.float32(), [1.5]),
            # The declared Float64 is what arrives, even for an integer column.
            ("Float64", "toInt32(3)", pa.float64(), [3.0]),
            ("Bool", "true", pa.bool_(), [True]),
        ]):
            with self.subTest(declared):
                self.check_arg(f"arrow_num_{i}", declared, expr, arrow_type, values)

    def test_strings_and_bytes(self):
        self.check_arg("arrow_str", STRING, "'héllo'", pa.string(), ["héllo"])
        self.check_arg("arrow_bytes", bytes, "unhex('ff00')", pa.binary(), [b"\xff\x00"])

    def test_temporal_types(self):
        utc = datetime.timezone.utc
        for i, (declared, expr, arrow_type, values) in enumerate([
            ("Date", "toDate('2024-01-02')", pa.date32(), [datetime.date(2024, 1, 2)]),
            ("Date32", "toDate32('1960-01-02')", pa.date32(), [datetime.date(1960, 1, 2)]),
            ("DateTime('UTC')", "toDateTime('2024-01-02 03:04:05', 'UTC')", pa.timestamp("s", "UTC"),
             [datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=utc)]),
            ("DateTime64(3, 'UTC')", "toDateTime64('2024-01-02 03:04:05.123', 3, 'UTC')", pa.timestamp("ms", "UTC"),
             [datetime.datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=utc)]),
            ("DateTime64(6, 'UTC')", "toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC')", pa.timestamp("us", "UTC"),
             [datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=utc)]),
        ]):
            with self.subTest(declared):
                self.check_arg(f"arrow_time_{i}", declared, expr, arrow_type, values)

    @unittest.skipIf(_LITE, "chdb-core-lite drops Int128/256, UInt128/256 conversions")
    def test_big_integers_arrive_as_decimal_text(self):
        self.check_arg("arrow_int128", "Int128", "toInt128('-170141183460469231731687303715884105728')",
                       pa.string(), ["-170141183460469231731687303715884105728"])
        self.check_arg("arrow_uint256_undeclared", None, "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')",
                       pa.string(), ["115792089237316195423570985008687907853269984665640564039457584007913129639935"])

    def test_undeclared_types(self):
        for i, (expr, arrow_type, values) in enumerate([
            ("CAST('b' AS Enum8('a' = 1, 'b' = 2))", pa.string(), ["b"]),
            ("toLowCardinality('lc')", pa.string(), ["lc"]),
            ("[1, 2, 3]", pa.list_(pa.field("item", pa.uint8(), nullable=False)), [[1, 2, 3]]),
            ("toDecimal64(3.14, 2)", pa.decimal128(18, 2), [Decimal("3.14")]),
            ("toFixedString('ab', 3)", pa.binary(3), [b"ab\x00"]),
            ("toIPv4('1.2.3.4')", pa.uint32(), [0x01020304]),
        ]):
            with self.subTest(expr):
                self.check_arg(f"arrow_undeclared_{i}", None, expr, arrow_type, values)

    def test_map_becomes_map(self):
        seen = []

        def record(m):
            seen.append((m.type.key_type, m.type.item_type, m.to_pylist()))
            return [len(entries) for entries in m.to_pylist()]

        self.create("arrow_map_len", record, return_type=INT64)
        self.assertEqual(self.query("SELECT arrow_map_len(materialize(map('k', 1, 'l', 2)))"), "2")
        self.assertEqual(seen, [(pa.string(), pa.uint8(), [[("k", 1), ("l", 2)]])])

    def test_enum_and_big_integers_convert_inside_containers(self):
        cases = [
            ("[CAST('b' AS Enum8('a' = 1, 'b' = 2))]", pa.list_(pa.field("item", pa.string(), nullable=False)),
             [["b"]]),
            ("[toLowCardinality('x'), toLowCardinality('y')]",
             pa.list_(pa.field("item", pa.string(), nullable=False)), [["x", "y"]]),
            ("[CAST(NULL AS Nullable(Enum8('a' = 1))), 'a']", pa.list_(pa.string()), [[None, "a"]]),
            ("map('k', CAST('a' AS Enum8('a' = 1)))", pa.map_(pa.string(), pa.string()), [[("k", "a")]]),
        ]
        if not _LITE:
            cases += [
                ("tuple(toInt128(5), 'x')", None, [{"1": "5", "2": "x"}]),
                ("CAST((toUInt256(7), 'y') AS Tuple(n UInt256, s String))", None, [{"n": "7", "s": "y"}]),
            ]
        for i, (expr, arrow_type, values) in enumerate(cases):
            with self.subTest(expr):
                seen = []
                name = f"arrow_nested_{i}"
                self.create(name, typed_recorder(seen), return_type=STRING)
                self.assertEqual(self.query(f"SELECT {name}(materialize({expr}))"), '"ok"')
                if arrow_type is not None and pa.types.is_map(arrow_type):
                    self.assertEqual(seen[0][0].item_type, arrow_type.item_type)
                elif arrow_type is not None:
                    self.assertEqual(seen[0][0], arrow_type)
                self.assertEqual(seen[0][1], values)

    def test_bfloat16_arrives_as_float32(self):
        self.check_arg("arrow_bfloat16", None, "toBFloat16(1.5)", pa.float32(), [1.5])

    def test_invalid_utf8_is_rejected_unless_declared_as_bytes(self):
        self.session.query("CREATE TABLE arrow_bytes_t (id Int64, s String) ENGINE = Memory")
        self.session.query(
            "INSERT INTO arrow_bytes_t VALUES (1, 'é'), (2, unhex('c3')), (3, unhex('a9')), (4, unhex('ff'))")
        try:
            self.create("arrow_utf8_len", lambda x: pc.utf8_length(x), [STRING], INT64)
            self.assertEqual(self.query("SELECT arrow_utf8_len(s) FROM arrow_bytes_t WHERE id = 1"), "1")
            # Each of these is invalid on its own, though 2 and 3 together spell 'é'.
            for ids in ["(2, 3)", "(3)", "(4)"]:
                with self.subTest(ids):
                    with self.assertRaisesRegex(RuntimeError, "is not valid UTF-8. Declare a String argument as bytes"):
                        self.query(f"SELECT arrow_utf8_len(s) FROM arrow_bytes_t WHERE id IN {ids} ORDER BY id")

            self.create("arrow_bytes_len", lambda x: pc.binary_length(x), [bytes], INT64)
            self.assertEqual(self.query("SELECT arrow_bytes_len(s) FROM arrow_bytes_t ORDER BY id"), "2\n1\n1\n1")

            # Inside an Array or a Map too.
            self.create("arrow_nested_len", lambda x: pa.array([0] * len(x)), return_type=INT64)
            for expr in ["[s]", "map(s, id)"]:
                with self.subTest(expr):
                    with self.assertRaisesRegex(RuntimeError, "is not valid UTF-8"):
                        self.query(f"SELECT arrow_nested_len({expr}) FROM arrow_bytes_t")
                    self.assertEqual(
                        self.query(f"SELECT arrow_nested_len({expr}) FROM arrow_bytes_t WHERE id = 1"), "0")
        finally:
            self.session.query("DROP TABLE arrow_bytes_t")

    def test_tuple_becomes_struct(self):
        seen = []

        def record(t):
            seen.append(t.to_pylist())
            return pc.struct_field(t, [0])

        self.create("arrow_tuple_first", record, return_type=INT64)
        self.assertEqual(self.query("SELECT arrow_tuple_first(materialize(tuple(toInt64(42), 'x')))"), "42")
        self.assertEqual(seen, [[{"1": 42, "2": "x"}]])

    def test_pandas_and_numpy_roundtrip(self):
        def zscore(x):
            values = x.to_numpy()
            return (values - values.mean()) / values.std()

        self.create("arrow_zscore", zscore, [FLOAT64], FLOAT64)
        ret = self.query("SELECT round(sum(arrow_zscore(toFloat64(number))), 6) FROM numbers(1000) SETTINGS max_threads = 1")
        self.assertEqual(float(ret), 0.0)

        def label(x):
            return pd.Series(np.where(x.to_numpy() % 2 == 0, "even", "odd"))

        self.create("arrow_label", label, [INT64], STRING)
        self.assertEqual(self.query("SELECT arrow_label(toInt64(number)) FROM numbers(3)"), '"even"\n"odd"\n"even"')


if __name__ == "__main__":
    unittest.main()
