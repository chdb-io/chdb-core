#!python3

import unittest
import datetime
import chdb
from chdb.udf import chdb_udf
from chdb import func
from chdb.sqltypes import INT64, FLOAT64, STRING, BOOL, DATE
from chdb.session import Session
from chdb import query, sql


@chdb_udf()
def sum_udf(lhs, rhs):
    return int(lhs) + int(rhs)


@chdb_udf(return_type="Int32")
def mul_udf(lhs, rhs):
    return int(lhs) * int(rhs)


class TestUDF(unittest.TestCase):
    def test_sum_udf(self):
        ret = query("select sum_udf(12,22)", "Debug")
        self.assertEqual(str(ret), '"34"\n')

    def test_return_Int32(self):
        ret = query("select mul_udf(12,22) + 1", "Debug")
        self.assertEqual(str(ret), "265\n")

    def test_define_in_function(self):
        @chdb_udf()
        def sum_udf2(lhs, rhs):
            return int(lhs) + int(rhs)

        # sql is a alias for query
        ret = sql("select sum_udf2(11, 22)", "Debug")
        self.assertEqual(str(ret), '"33"\n')


class TestUDFinSession(unittest.TestCase):
    def test_sum_udf(self):
        with Session(":memory:?verbose&log-level=test") as session:
            ret = session.query("select sum_udf(12,22)")
            self.assertEqual(str(ret), '"34"\n')

    def test_return_Int32(self):
        with Session("file::memory:") as session:
            ret = session.query("select mul_udf(12,22) + 1")
            self.assertEqual(str(ret), "265\n")

    def test_define_in_function(self):
        @chdb_udf()
        def sum_udf2(lhs, rhs):
            return int(lhs) + int(rhs)

        with Session() as session:
            # sql is a alias for query
            ret = session.sql("select sum_udf2(11, 22)", "CSV")
            self.assertEqual(str(ret), '"33"\n')


class TestCreateFunction(unittest.TestCase):

    def _register_and_query(self, name, func, return_type, sql_expr, expected):
        """Helper: register UDF, run query, verify result, then drop."""
        chdb.create_function(name, func, return_type)
        with Session() as session:
            ret = session.query(f"SELECT {sql_expr}")
            self.assertEqual(str(ret), expected)
        chdb.drop_function(name)

    # ── Bool ──

    def test_bool_lambda_with_type(self):
        from chdb.sqltypes import BOOL
        self._register_and_query(
            "udf_bool_lt", lambda a: a > 0, BOOL,
            "udf_bool_lt(1)", "true\n")

    def test_bool_def_with_string(self):
        def is_positive(a):
            return a > 0
        self._register_and_query(
            "udf_bool_ds", is_positive, "Bool",
            "udf_bool_ds(1)", "true\n")

    # ── Integer types ──

    def test_int8_lambda_with_type(self):
        from chdb.sqltypes import INT8
        self._register_and_query(
            "udf_i8_lt", lambda a: a + 1, INT8,
            "udf_i8_lt(toInt8(126))", "127\n")

    def test_int64_def_with_string(self):
        def add(a, b):
            return a + b
        self._register_and_query(
            "udf_i64_ds", add, "Int64",
            "udf_i64_ds(6, 7) + 1", "14\n")

    def test_uint32_def_with_type(self):
        from chdb.sqltypes import UINT32
        def double_it(a):
            return a * 2
        self._register_and_query(
            "udf_u32_dt", double_it, UINT32,
            "udf_u32_dt(toUInt32(100))", "200\n")

    def test_uint32_lambda_with_string(self):
        self._register_and_query(
            "udf_u32_ls", lambda a: a * 3, "UInt32",
            "udf_u32_ls(toUInt32(10))", "30\n")

    # ── Float types ──

    def test_float64_def_with_type(self):
        from chdb.sqltypes import FLOAT64
        def divide(a, b):
            return a / b
        self._register_and_query(
            "udf_f64_dt", divide, FLOAT64,
            "udf_f64_dt(7, 2)", "3.5\n")

    def test_float32_lambda_with_string(self):
        self._register_and_query(
            "udf_f32_ls", lambda a: a * 0.5, "Float32",
            "udf_f32_ls(toFloat32(4))", "2\n")

    # ── String ──

    def test_string_lambda_with_type(self):
        from chdb.sqltypes import STRING
        self._register_and_query(
            "udf_str_lt", lambda a, b: a + b, STRING,
            "udf_str_lt('hello', ' world')", "hello world\n")

    def test_string_def_with_string(self):
        def to_upper(a):
            return a.upper()
        self._register_and_query(
            "udf_str_ds", to_upper, "String",
            "udf_str_ds('abc')", "ABC\n")

    # ── Date / DateTime ──

    def test_date_def_with_type(self):
        from chdb.sqltypes import DATE
        def fixed_date():
            return datetime.date(2025, 1, 1)
        self._register_and_query(
            "udf_date_dt", fixed_date, DATE,
            "udf_date_dt()", "2025-01-01\n")

    def test_date_lambda_with_string(self):
        self._register_and_query(
            "udf_date_ls", lambda: datetime.date(2024, 12, 31), "Date",
            "udf_date_ls()", "2024-12-31\n")

    def test_datetime64_def_with_string(self):
        def fixed_dt():
            return datetime.datetime(2025, 6, 15, 12, 30, 45, 123000)
        self._register_and_query(
            "udf_dt64_ds", fixed_dt, "DateTime64(3)",
            "udf_dt64_ds()", "2025-06-15 12:30:45.123\n")

    def test_datetime64_lambda_with_type(self):
        from chdb.sqltypes import DATETIME64
        self._register_and_query(
            "udf_dt64_lt",
            lambda: datetime.datetime(2025, 3, 1, 0, 0, 0),
            DATETIME64,
            "udf_dt64_lt()", "2025-03-01 00:00:00.000\n")

    # ── drop_function ──

    def test_drop_function_makes_udf_unavailable(self):
        chdb.create_function("udf_drop_test", lambda a: a + 1, "Int64")
        with Session() as session:
            ret = session.query("SELECT udf_drop_test(5)")
            self.assertEqual(str(ret), "6\n")

        chdb.drop_function("udf_drop_test")
        with Session() as session:
            with self.assertRaises(Exception):
                session.query("SELECT udf_drop_test(5)")

    def test_drop_nonexistent_does_not_raise(self):
        chdb.drop_function("no_such_function")

    # ── UDF not registered ──

    def test_call_unregistered_udf_raises(self):
        with Session() as session:
            with self.assertRaises(Exception):
                session.query("SELECT totally_missing_udf(1)")

    # ── UDF execution error ──

    def test_udf_exception_propagates_lambda(self):
        chdb.create_function("udf_bad_l", lambda a: 1 / 0, "Int64")
        with Session() as session:
            with self.assertRaises(Exception) as ctx:
                session.query("SELECT udf_bad_l(1)")
            self.assertIn("division by zero", str(ctx.exception))
        chdb.drop_function("udf_bad_l")

    def test_udf_exception_propagates_def(self):
        def bad_func(a):
            raise ValueError("intentional error")

        chdb.create_function("udf_bad_d", bad_func, "Int64")
        with Session() as session:
            with self.assertRaises(Exception) as ctx:
                session.query("SELECT udf_bad_d(1)")
            self.assertIn("intentional error", str(ctx.exception))
        chdb.drop_function("udf_bad_d")

    # ── Invalid return_type ──

    def test_invalid_type_string_raises(self):
        with self.assertRaises(RuntimeError):
            chdb.create_function("udf_bad_type", lambda a: a, "NoSuchType")

    def test_invalid_type_argument_raises(self):
        with self.assertRaises(RuntimeError):
            chdb.create_function("udf_bad_arg", lambda a: a, 12345)


class TestFuncDecorator(unittest.TestCase):
    """Tests for the @func decorator using the native Python UDF mechanism."""

    def _query(self, sql_expr):
        with Session() as session:
            return str(session.query(f"SELECT {sql_expr}"))

    # ── ChdbType return_type ──

    def test_func_decorator_int64_with_type(self):
        @func(INT64)
        def func_add_i64(a, b):
            return a + b

        self.assertEqual(self._query("func_add_i64(10, 20)"), "30\n")
        chdb.drop_function("func_add_i64")

    def test_func_decorator_float64_with_type(self):
        @func(FLOAT64)
        def func_div_f64(a, b):
            return a / b

        self.assertEqual(self._query("func_div_f64(7, 2)"), "3.5\n")
        chdb.drop_function("func_div_f64")

    def test_func_decorator_string_with_type(self):
        @func(STRING)
        def func_upper_str(s):
            return s.upper()

        self.assertEqual(self._query("func_upper_str('hello')"), '"HELLO"\n')
        chdb.drop_function("func_upper_str")

    def test_func_decorator_bool_with_type(self):
        @func(BOOL)
        def func_is_pos(a):
            return a > 0

        self.assertEqual(self._query("func_is_pos(5)"), "true\n")
        self.assertEqual(self._query("func_is_pos(-1)"), "false\n")
        chdb.drop_function("func_is_pos")

    def test_func_decorator_date_with_type(self):
        @func(DATE)
        def func_fixed_date():
            return datetime.date(2025, 6, 15)

        self.assertEqual(self._query("func_fixed_date()"), '"2025-06-15"\n')
        chdb.drop_function("func_fixed_date")

    # ── String return_type ──

    def test_func_decorator_int64_with_string(self):
        @func("Int64")
        def func_mul_i64s(a, b):
            return a * b

        self.assertEqual(self._query("func_mul_i64s(6, 7)"), "42\n")
        chdb.drop_function("func_mul_i64s")

    def test_func_decorator_string_with_string(self):
        @func("String")
        def func_concat_s(a, b):
            return a + b

        self.assertEqual(self._query("func_concat_s('foo', 'bar')"), '"foobar"\n')
        chdb.drop_function("func_concat_s")

    def test_func_decorator_datetime64_with_string(self):
        @func("DateTime64(3)")
        def func_fixed_dt64():
            return datetime.datetime(2025, 3, 1, 12, 0, 0)

        self.assertEqual(self._query("func_fixed_dt64()"), '"2025-03-01 12:00:00.000"\n')
        chdb.drop_function("func_fixed_dt64")

    # ── Decorated function remains callable as Python ──

    def test_func_decorator_still_callable_as_python(self):
        @func(INT64)
        def func_py_call(a, b):
            return a + b

        self.assertEqual(func_py_call(3, 4), 7)
        chdb.drop_function("func_py_call")

    # ── Define inside a method ──

    def test_func_decorator_defined_in_method(self):
        @func(INT64)
        def func_local_add(a, b):
            return a + b

        self.assertEqual(self._query("func_local_add(100, 200)"), "300\n")
        chdb.drop_function("func_local_add")

    # ── Error propagation ──

    def test_func_decorator_exception_propagates(self):
        @func(INT64)
        def func_bad_div(a):
            return 1 // 0

        with Session() as session:
            with self.assertRaises(Exception) as ctx:
                session.query("SELECT func_bad_div(1)")
            self.assertIn("ZeroDivisionError", str(ctx.exception))
        chdb.drop_function("func_bad_div")

    # ── Invalid return_type ──

    def test_func_decorator_invalid_type_string_raises(self):
        with self.assertRaises(RuntimeError):
            @func("NoSuchType")
            def func_bad_type(a):
                return a


if __name__ == "__main__":
    unittest.main()
