#!python3

import os
import sys
import unittest
import datetime
from typing import Optional, Union
import chdb
from chdb import func
from chdb.sqltypes import (
    BOOL, INT8, INT16, INT32, INT64, INT128, INT256,
    UINT8, UINT16, UINT32, UINT64, UINT128, UINT256,
    FLOAT32, FLOAT64, STRING, DATE, DATE32, DATETIME, DATETIME64,
)
from chdb.session import Session

# chdb-core-lite drops Int128/256 + UInt128/256 conversion (toInt128, toUInt256, ...)
# and the matching arithmetic/comparison template instantiations. The UDF tests
# below register big-int UDFs and exercise them via these conversion functions,
# so they cannot run on the lite wheel; skip at class level. tests/test_chdb_core_lite.py
# asserts the trim itself (toInt128/... raise Code 46).
_LITE = os.environ.get("CHDB_LITE") == "1"
_LITE_BIG_INT_REASON = "chdb-core-lite drops Int128/256, UInt128/256 conversions"


class TestBoolUDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_bool_return_explicit_arg_types(self):
        def is_positive(x):
            return x > 0

        chdb.create_function("is_positive", is_positive, arg_types=[INT64], return_type=BOOL)
        ret = self.session.query("SELECT is_positive(5)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT is_positive(-3)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("is_positive")

    def test_create_function_bool_lambda_explicit(self):
        chdb.create_function("is_even", lambda x: x % 2 == 0, arg_types=[INT64], return_type=BOOL)
        ret = self.session.query("SELECT is_even(4)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT is_even(3)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("is_even")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_bool_return_no_arg_types(self):
        def is_long(s):
            return len(s) > 3

        chdb.create_function("is_long", is_long, return_type=BOOL)
        ret = self.session.query("SELECT is_long('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT is_long('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("is_long")

    # ── create_function: infer return_type from annotation ──

    def test_create_function_bool_infer_return_from_annotation(self):
        def is_negative(x) -> bool:
            return x < 0

        chdb.create_function("is_negative", is_negative)
        ret = self.session.query("SELECT is_negative(-1)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT is_negative(1)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("is_negative")

    # ── create_function: infer both arg_types and return_type from annotations ──

    def test_create_function_bool_infer_all_from_annotations(self):
        def both_positive(a: int, b: int) -> bool:
            return a > 0 and b > 0

        chdb.create_function("both_positive", both_positive)
        ret = self.session.query("SELECT both_positive(1, 2)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT both_positive(1, -2)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("both_positive")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def is_big(x: int) -> bool:
            return x > 100

        chdb.create_function("is_big", is_big, arg_types=[INT64], return_type=BOOL)
        ret = self.session.query("SELECT is_big(200)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT is_big(50)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("is_big")

    # ── create_function: string type names ──

    def test_create_function_bool_string_types(self):
        chdb.create_function("str_eq", lambda a, b: a == b, arg_types=["String", "String"], return_type="Bool")
        ret = self.session.query("SELECT str_eq('abc', 'abc')", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT str_eq('abc', 'xyz')", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("str_eq")

    # ── create_function: bool as arg_type ──

    def test_create_function_bool_as_arg_type(self):
        def negate(x):
            return not x

        chdb.create_function("negate", negate, arg_types=[BOOL], return_type=BOOL)
        ret = self.session.query("SELECT negate(true)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        ret = self.session.query("SELECT negate(false)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        chdb.drop_function("negate")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return True

        with self.assertRaises(RuntimeError):
            chdb.create_function("dummy", dummy, arg_types=[INT64], return_type=BOOL)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        def check_int(x):
            return x > 0

        chdb.create_function("check_int", check_int, arg_types=[INT64], return_type=BOOL)
        with self.assertRaises(Exception):
            self.session.query("SELECT check_int('hello')", "CSV")
        chdb.drop_function("check_int")

    # ── create_function: compatible arg type (e.g. UInt8 → Int64) ──

    def test_create_function_compatible_arg_type(self):
        def is_one(x):
            return x == 1

        chdb.create_function("is_one", is_one, arg_types=[INT64], return_type=BOOL)
        ret = self.session.query("SELECT is_one(toUInt8(1))", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        chdb.drop_function("is_one")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_bool_explicit_all(self):
        @func(arg_types=[INT64], return_type=BOOL)
        def dec_is_positive(x):
            return x > 0

        ret = self.session.query("SELECT dec_is_positive(10)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT dec_is_positive(-10)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("dec_is_positive")

    def test_func_decorator_bool_return_only(self):
        @func(return_type=BOOL)
        def dec_is_zero(x):
            return x == 0

        ret = self.session.query("SELECT dec_is_zero(0)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT dec_is_zero(1)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("dec_is_zero")

    # ── @func decorator: infer all from annotations ──

    def test_func_decorator_bool_infer_all(self):
        @func()
        def dec_both_true(a: bool, b: bool) -> bool:
            return a and b

        ret = self.session.query("SELECT dec_both_true(true, true)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT dec_both_true(true, false)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("dec_both_true")

    # ── @func decorator: infer return_type only ──

    def test_func_decorator_bool_infer_return(self):
        @func(arg_types=[INT64, INT64])
        def dec_gt(a, b) -> bool:
            return a > b

        ret = self.session.query("SELECT dec_gt(5, 3)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT dec_gt(1, 3)", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("dec_gt")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_bool_udf(self):
        chdb.create_function("to_drop", lambda x: x > 0, arg_types=[INT64], return_type=BOOL)
        ret = self.session.query("SELECT to_drop(1)", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        chdb.drop_function("to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT to_drop(1)", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=BOOL)
        def py_callable(x):
            return x > 0

        self.assertTrue(py_callable(5))
        self.assertFalse(py_callable(-1))
        chdb.drop_function("py_callable")


# ═══════════════════════════════════════════════════════════════════
# Signed Integer Types
# ═══════════════════════════════════════════════════════════════════


class TestInt8UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_int8_return_explicit_arg_types(self):
        def add_i8(x, y):
            return x + y

        chdb.create_function("i8_add", add_i8, arg_types=[INT8, INT8], return_type=INT8)
        ret = self.session.query("SELECT i8_add(toInt8(10), toInt8(20))", "CSV")
        self.assertEqual(str(ret).strip(), "30")
        ret = self.session.query("SELECT i8_add(toInt8(-5), toInt8(3))", "CSV")
        self.assertEqual(str(ret).strip(), "-2")
        chdb.drop_function("i8_add")

    def test_create_function_int8_lambda_explicit(self):
        chdb.create_function("i8_double", lambda x: x * 2, arg_types=[INT8], return_type=INT8)
        ret = self.session.query("SELECT i8_double(toInt8(10))", "CSV")
        self.assertEqual(str(ret).strip(), "20")
        ret = self.session.query("SELECT i8_double(toInt8(-5))", "CSV")
        self.assertEqual(str(ret).strip(), "-10")
        chdb.drop_function("i8_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_int8_return_no_arg_types(self):
        def str_len_i8(s):
            return len(s)

        chdb.create_function("i8_strlen", str_len_i8, return_type=INT8)
        ret = self.session.query("SELECT i8_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT i8_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i8_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_i8(x: int) -> int:
            return x + 1

        chdb.create_function("i8_inc_override", inc_i8, arg_types=[INT8], return_type=INT8)
        ret = self.session.query("SELECT i8_inc_override(toInt8(10))", "CSV")
        self.assertEqual(str(ret).strip(), "11")
        ret = self.session.query("SELECT i8_inc_override(toInt8(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i8_inc_override")

    # ── create_function: string type names ──

    def test_create_function_int8_string_types(self):
        chdb.create_function("i8_inc_str", lambda x: x + 1, arg_types=["Int8"], return_type="Int8")
        ret = self.session.query("SELECT i8_inc_str(toInt8(9))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        ret = self.session.query("SELECT i8_inc_str(toInt8(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i8_inc_str")

    # ── create_function: int8 as arg_type ──

    def test_create_function_int8_as_arg_type(self):
        def neg_i8(x):
            return -x

        chdb.create_function("i8_neg", neg_i8, arg_types=[INT8], return_type=INT8)
        ret = self.session.query("SELECT i8_neg(toInt8(42))", "CSV")
        self.assertEqual(str(ret).strip(), "-42")
        ret = self.session.query("SELECT i8_neg(toInt8(-10))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("i8_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("i8_dummy", dummy, arg_types=[INT8], return_type=INT8)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("i8_check", lambda x: x, arg_types=[INT8], return_type=INT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT i8_check('hello')", "CSV")
        chdb.drop_function("i8_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("i8_compat", lambda x: x + 1, arg_types=[INT16], return_type=INT8)
        ret = self.session.query("SELECT i8_compat(toInt8(5))", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        chdb.drop_function("i8_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_int8_explicit_all(self):
        @func(arg_types=[INT8, INT8], return_type=INT8)
        def dec_i8_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_i8_add(toInt8(3), toInt8(4))", "CSV")
        self.assertEqual(str(ret).strip(), "7")
        ret = self.session.query("SELECT dec_i8_add(toInt8(-3), toInt8(4))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i8_add")

    def test_func_decorator_int8_return_only(self):
        @func(return_type=INT8)
        def dec_i8_one(x):
            return 1

        ret = self.session.query("SELECT dec_i8_one(toInt8(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_i8_one(toInt8(99))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i8_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("i8_overflow", lambda x: 200, arg_types=[INT8], return_type=INT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT i8_overflow(toInt8(1))", "CSV")
        chdb.drop_function("i8_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("i8_underflow", lambda x: -200, arg_types=[INT8], return_type=INT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT i8_underflow(toInt8(1))", "CSV")
        chdb.drop_function("i8_underflow")

    # ── input arg out of range (larger type passed to Int8 arg_type) ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("i8_input_ovf", lambda x: x, arg_types=[INT8], return_type=INT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT i8_input_ovf(toInt16(200))", "CSV")
        chdb.drop_function("i8_input_ovf")

    def test_input_arg_underflow_raises(self):
        chdb.create_function("i8_input_udf", lambda x: x, arg_types=[INT8], return_type=INT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT i8_input_udf(toInt16(-200))", "CSV")
        chdb.drop_function("i8_input_udf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_int8_udf(self):
        chdb.create_function("i8_to_drop", lambda x: x + 1, arg_types=[INT8], return_type=INT8)
        ret = self.session.query("SELECT i8_to_drop(toInt8(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i8_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT i8_to_drop(toInt8(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=INT8)
        def i8_py_callable(x):
            return x + 1

        self.assertEqual(i8_py_callable(5), 6)
        self.assertEqual(i8_py_callable(-1), 0)
        chdb.drop_function("i8_py_callable")


class TestInt16UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_int16_return_explicit_arg_types(self):
        def add_i16(x, y):
            return x + y

        chdb.create_function("i16_add", add_i16, arg_types=[INT16, INT16], return_type=INT16)
        ret = self.session.query("SELECT i16_add(toInt16(1000), toInt16(2000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000")
        ret = self.session.query("SELECT i16_add(toInt16(-500), toInt16(300))", "CSV")
        self.assertEqual(str(ret).strip(), "-200")
        chdb.drop_function("i16_add")

    def test_create_function_int16_lambda_explicit(self):
        chdb.create_function("i16_double", lambda x: x * 2, arg_types=[INT16], return_type=INT16)
        ret = self.session.query("SELECT i16_double(toInt16(100))", "CSV")
        self.assertEqual(str(ret).strip(), "200")
        ret = self.session.query("SELECT i16_double(toInt16(-50))", "CSV")
        self.assertEqual(str(ret).strip(), "-100")
        chdb.drop_function("i16_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_int16_return_no_arg_types(self):
        def str_len_i16(s):
            return len(s)

        chdb.create_function("i16_strlen", str_len_i16, return_type=INT16)
        ret = self.session.query("SELECT i16_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT i16_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i16_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_i16(x: int) -> int:
            return x + 1

        chdb.create_function("i16_inc_override", inc_i16, arg_types=[INT16], return_type=INT16)
        ret = self.session.query("SELECT i16_inc_override(toInt16(100))", "CSV")
        self.assertEqual(str(ret).strip(), "101")
        ret = self.session.query("SELECT i16_inc_override(toInt16(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i16_inc_override")

    # ── create_function: string type names ──

    def test_create_function_int16_string_types(self):
        chdb.create_function("i16_inc_str", lambda x: x + 1, arg_types=["Int16"], return_type="Int16")
        ret = self.session.query("SELECT i16_inc_str(toInt16(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT i16_inc_str(toInt16(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i16_inc_str")

    # ── create_function: int16 as arg_type ──

    def test_create_function_int16_as_arg_type(self):
        def neg_i16(x):
            return -x

        chdb.create_function("i16_neg", neg_i16, arg_types=[INT16], return_type=INT16)
        ret = self.session.query("SELECT i16_neg(toInt16(1000))", "CSV")
        self.assertEqual(str(ret).strip(), "-1000")
        ret = self.session.query("SELECT i16_neg(toInt16(-500))", "CSV")
        self.assertEqual(str(ret).strip(), "500")
        chdb.drop_function("i16_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("i16_dummy", dummy, arg_types=[INT16], return_type=INT16)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("i16_check", lambda x: x, arg_types=[INT16], return_type=INT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT i16_check('hello')", "CSV")
        chdb.drop_function("i16_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("i16_compat", lambda x: x + 1, arg_types=[INT16], return_type=INT16)
        ret = self.session.query("SELECT i16_compat(toInt8(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("i16_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_int16_explicit_all(self):
        @func(arg_types=[INT16, INT16], return_type=INT16)
        def dec_i16_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_i16_add(toInt16(300), toInt16(400))", "CSV")
        self.assertEqual(str(ret).strip(), "700")
        ret = self.session.query("SELECT dec_i16_add(toInt16(-300), toInt16(400))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        chdb.drop_function("dec_i16_add")

    def test_func_decorator_int16_return_only(self):
        @func(return_type=INT16)
        def dec_i16_one(x):
            return 1

        ret = self.session.query("SELECT dec_i16_one(toInt16(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_i16_one(toInt16(9999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i16_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("i16_overflow", lambda x: 40000, arg_types=[INT16], return_type=INT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT i16_overflow(toInt16(1))", "CSV")
        chdb.drop_function("i16_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("i16_underflow", lambda x: -40000, arg_types=[INT16], return_type=INT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT i16_underflow(toInt16(1))", "CSV")
        chdb.drop_function("i16_underflow")

    # ── input arg out of range (larger type passed to Int16 arg_type) ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("i16_input_ovf", lambda x: x, arg_types=[INT16], return_type=INT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT i16_input_ovf(toInt32(40000))", "CSV")
        chdb.drop_function("i16_input_ovf")

    def test_input_arg_underflow_raises(self):
        chdb.create_function("i16_input_udf", lambda x: x, arg_types=[INT16], return_type=INT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT i16_input_udf(toInt32(-40000))", "CSV")
        chdb.drop_function("i16_input_udf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_int16_udf(self):
        chdb.create_function("i16_to_drop", lambda x: x + 1, arg_types=[INT16], return_type=INT16)
        ret = self.session.query("SELECT i16_to_drop(toInt16(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i16_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT i16_to_drop(toInt16(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=INT16)
        def i16_py_callable(x):
            return x + 1

        self.assertEqual(i16_py_callable(5), 6)
        self.assertEqual(i16_py_callable(-1), 0)
        chdb.drop_function("i16_py_callable")


class TestInt32UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_int32_return_explicit_arg_types(self):
        def add_i32(x, y):
            return x + y

        chdb.create_function("i32_add", add_i32, arg_types=[INT32, INT32], return_type=INT32)
        ret = self.session.query("SELECT i32_add(toInt32(100000), toInt32(200000))", "CSV")
        self.assertEqual(str(ret).strip(), "300000")
        ret = self.session.query("SELECT i32_add(toInt32(-50000), toInt32(30000))", "CSV")
        self.assertEqual(str(ret).strip(), "-20000")
        chdb.drop_function("i32_add")

    def test_create_function_int32_lambda_explicit(self):
        chdb.create_function("i32_double", lambda x: x * 2, arg_types=[INT32], return_type=INT32)
        ret = self.session.query("SELECT i32_double(toInt32(50000))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        ret = self.session.query("SELECT i32_double(toInt32(-25000))", "CSV")
        self.assertEqual(str(ret).strip(), "-50000")
        chdb.drop_function("i32_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_int32_return_no_arg_types(self):
        def str_len_i32(s):
            return len(s)

        chdb.create_function("i32_strlen", str_len_i32, return_type=INT32)
        ret = self.session.query("SELECT i32_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT i32_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i32_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_i32(x: int) -> int:
            return x + 1

        chdb.create_function("i32_inc_override", inc_i32, arg_types=[INT32], return_type=INT32)
        ret = self.session.query("SELECT i32_inc_override(toInt32(99999))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        ret = self.session.query("SELECT i32_inc_override(toInt32(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i32_inc_override")

    # ── create_function: string type names ──

    def test_create_function_int32_string_types(self):
        chdb.create_function("i32_inc_str", lambda x: x + 1, arg_types=["Int32"], return_type="Int32")
        ret = self.session.query("SELECT i32_inc_str(toInt32(99999))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        ret = self.session.query("SELECT i32_inc_str(toInt32(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i32_inc_str")

    # ── create_function: int32 as arg_type ──

    def test_create_function_int32_as_arg_type(self):
        def neg_i32(x):
            return -x

        chdb.create_function("i32_neg", neg_i32, arg_types=[INT32], return_type=INT32)
        ret = self.session.query("SELECT i32_neg(toInt32(12345))", "CSV")
        self.assertEqual(str(ret).strip(), "-12345")
        ret = self.session.query("SELECT i32_neg(toInt32(-67890))", "CSV")
        self.assertEqual(str(ret).strip(), "67890")
        chdb.drop_function("i32_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("i32_dummy", dummy, arg_types=[INT32], return_type=INT32)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("i32_check", lambda x: x, arg_types=[INT32], return_type=INT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT i32_check('hello')", "CSV")
        chdb.drop_function("i32_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("i32_compat", lambda x: x + 1, arg_types=[INT32], return_type=INT32)
        ret = self.session.query("SELECT i32_compat(toInt16(1000))", "CSV")
        self.assertEqual(str(ret).strip(), "1001")
        chdb.drop_function("i32_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_int32_explicit_all(self):
        @func(arg_types=[INT32, INT32], return_type=INT32)
        def dec_i32_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_i32_add(toInt32(30000), toInt32(40000))", "CSV")
        self.assertEqual(str(ret).strip(), "70000")
        ret = self.session.query("SELECT dec_i32_add(toInt32(-30000), toInt32(40000))", "CSV")
        self.assertEqual(str(ret).strip(), "10000")
        chdb.drop_function("dec_i32_add")

    def test_func_decorator_int32_return_only(self):
        @func(return_type=INT32)
        def dec_i32_one(x):
            return 1

        ret = self.session.query("SELECT dec_i32_one(toInt32(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_i32_one(toInt32(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i32_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("i32_overflow", lambda x: 2200000000, arg_types=[INT32], return_type=INT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT i32_overflow(toInt32(1))", "CSV")
        chdb.drop_function("i32_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("i32_underflow", lambda x: -2200000000, arg_types=[INT32], return_type=INT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT i32_underflow(toInt32(1))", "CSV")
        chdb.drop_function("i32_underflow")

    # ── input arg out of range (larger type passed to Int32 arg_type) ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("i32_input_ovf", lambda x: x, arg_types=[INT32], return_type=INT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT i32_input_ovf(toInt64(2200000000))", "CSV")
        chdb.drop_function("i32_input_ovf")

    def test_input_arg_underflow_raises(self):
        chdb.create_function("i32_input_udf", lambda x: x, arg_types=[INT32], return_type=INT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT i32_input_udf(toInt64(-2200000000))", "CSV")
        chdb.drop_function("i32_input_udf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_int32_udf(self):
        chdb.create_function("i32_to_drop", lambda x: x + 1, arg_types=[INT32], return_type=INT32)
        ret = self.session.query("SELECT i32_to_drop(toInt32(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i32_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT i32_to_drop(toInt32(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=INT32)
        def i32_py_callable(x):
            return x + 1

        self.assertEqual(i32_py_callable(5), 6)
        self.assertEqual(i32_py_callable(-1), 0)
        chdb.drop_function("i32_py_callable")


class TestInt64UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_int64_return_explicit_arg_types(self):
        def add_i64(x, y):
            return x + y

        chdb.create_function("i64_add", add_i64, arg_types=[INT64, INT64], return_type=INT64)
        ret = self.session.query("SELECT i64_add(toInt64(1000000), toInt64(2000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000")
        ret = self.session.query("SELECT i64_add(toInt64(-500000), toInt64(300000))", "CSV")
        self.assertEqual(str(ret).strip(), "-200000")
        chdb.drop_function("i64_add")

    def test_create_function_int64_lambda_explicit(self):
        chdb.create_function("i64_double", lambda x: x * 2, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT i64_double(toInt64(500000))", "CSV")
        self.assertEqual(str(ret).strip(), "1000000")
        ret = self.session.query("SELECT i64_double(toInt64(-250000))", "CSV")
        self.assertEqual(str(ret).strip(), "-500000")
        chdb.drop_function("i64_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_int64_return_no_arg_types(self):
        def str_len_i64(s):
            return len(s)

        chdb.create_function("i64_strlen", str_len_i64, return_type=INT64)
        ret = self.session.query("SELECT i64_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT i64_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i64_strlen")

    # ── create_function: infer return_type from annotation ──

    def test_create_function_int64_infer_return_from_annotation(self):
        def triple_i64(x) -> int:
            return x * 3

        chdb.create_function("i64_triple_infer", triple_i64)
        ret = self.session.query("SELECT i64_triple_infer(toInt64(5))", "CSV")
        self.assertEqual(str(ret).strip(), "15")
        ret = self.session.query("SELECT i64_triple_infer(toInt64(-3))", "CSV")
        self.assertEqual(str(ret).strip(), "-9")
        chdb.drop_function("i64_triple_infer")

    # ── create_function: infer both arg_types and return_type from annotations ──

    def test_create_function_int64_infer_all_from_annotations(self):
        def add_annotated(a: int, b: int) -> int:
            return a + b

        chdb.create_function("i64_annotated", add_annotated)
        ret = self.session.query("SELECT i64_annotated(toInt64(10), toInt64(20))", "CSV")
        self.assertEqual(str(ret).strip(), "30")
        ret = self.session.query("SELECT i64_annotated(toInt64(-10), toInt64(20))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("i64_annotated")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_i64(x: float) -> float:
            return x + 1

        chdb.create_function("i64_inc_override", inc_i64, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT i64_inc_override(toInt64(99999))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        ret = self.session.query("SELECT i64_inc_override(toInt64(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i64_inc_override")

    # ── create_function: string type names ──

    def test_create_function_int64_string_types(self):
        chdb.create_function("i64_inc_str", lambda x: x + 1, arg_types=["Int64"], return_type="Int64")
        ret = self.session.query("SELECT i64_inc_str(toInt64(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1000000")
        ret = self.session.query("SELECT i64_inc_str(toInt64(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i64_inc_str")

    # ── create_function: int64 as arg_type ──

    def test_create_function_int64_as_arg_type(self):
        def neg_i64(x):
            return -x

        chdb.create_function("i64_neg", neg_i64, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT i64_neg(toInt64(123456))", "CSV")
        self.assertEqual(str(ret).strip(), "-123456")
        ret = self.session.query("SELECT i64_neg(toInt64(-789012))", "CSV")
        self.assertEqual(str(ret).strip(), "789012")
        chdb.drop_function("i64_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("i64_dummy", dummy, arg_types=[INT64], return_type=INT64)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("i64_check", lambda x: x, arg_types=[INT64], return_type=INT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT i64_check('hello')", "CSV")
        chdb.drop_function("i64_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("i64_compat", lambda x: x + 1, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT i64_compat(toInt32(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("i64_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_int64_explicit_all(self):
        @func(arg_types=[INT64, INT64], return_type=INT64)
        def dec_i64_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_i64_add(toInt64(300000), toInt64(400000))", "CSV")
        self.assertEqual(str(ret).strip(), "700000")
        ret = self.session.query("SELECT dec_i64_add(toInt64(-300000), toInt64(400000))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        chdb.drop_function("dec_i64_add")

    def test_func_decorator_int64_return_only(self):
        @func(return_type=INT64)
        def dec_i64_one(x):
            return 1

        ret = self.session.query("SELECT dec_i64_one(toInt64(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_i64_one(toInt64(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i64_one")

    # ── @func decorator: infer all from annotations ──

    def test_func_decorator_int64_infer_all(self):
        @func()
        def dec_i64_sum(a: int, b: int) -> int:
            return a + b

        ret = self.session.query("SELECT dec_i64_sum(toInt64(10), toInt64(20))", "CSV")
        self.assertEqual(str(ret).strip(), "30")
        ret = self.session.query("SELECT dec_i64_sum(toInt64(-10), toInt64(20))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("dec_i64_sum")

    # ── @func decorator: infer return_type only ──

    def test_func_decorator_int64_infer_return(self):
        @func(arg_types=[INT64, INT64])
        def dec_i64_diff(a, b) -> int:
            return a - b

        ret = self.session.query("SELECT dec_i64_diff(toInt64(50), toInt64(30))", "CSV")
        self.assertEqual(str(ret).strip(), "20")
        ret = self.session.query("SELECT dec_i64_diff(toInt64(10), toInt64(30))", "CSV")
        self.assertEqual(str(ret).strip(), "-20")
        chdb.drop_function("dec_i64_diff")

    # ── return value out of range ──
    # Int64 range is [-9223372036854775808, 9223372036854775807]
    # Python int has no overflow, but PyLong_AsLongLongAndOverflow will detect it

    def test_return_value_overflow_raises(self):
        chdb.create_function("i64_overflow", lambda x: 2**63, arg_types=[INT64], return_type=INT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT i64_overflow(toInt64(1))", "CSV")
        chdb.drop_function("i64_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("i64_underflow", lambda x: -(2**63) - 1, arg_types=[INT64], return_type=INT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT i64_underflow(toInt64(1))", "CSV")
        chdb.drop_function("i64_underflow")

    # ── input arg out of range (larger type passed to Int64 arg_type) ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("i64_input_ovf", lambda x: x, arg_types=[INT64], return_type=INT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT i64_input_ovf(toUInt64(9223372036854775808))", "CSV")
        chdb.drop_function("i64_input_ovf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_int64_udf(self):
        chdb.create_function("i64_to_drop", lambda x: x + 1, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT i64_to_drop(toInt64(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i64_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT i64_to_drop(toInt64(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=INT64)
        def i64_py_callable(x):
            return x + 1

        self.assertEqual(i64_py_callable(5), 6)
        self.assertEqual(i64_py_callable(-1), 0)
        chdb.drop_function("i64_py_callable")


@unittest.skipIf(_LITE, _LITE_BIG_INT_REASON)
class TestInt128UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_int128_return_explicit_arg_types(self):
        def add_i128(x, y):
            return x + y

        chdb.create_function("i128_add", add_i128, arg_types=[INT128, INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_add(toInt128(100), toInt128(200))", "CSV")
        self.assertEqual(str(ret).strip(), "300")
        ret = self.session.query("SELECT i128_add(toInt128(-50), toInt128(30))", "CSV")
        self.assertEqual(str(ret).strip(), "-20")
        chdb.drop_function("i128_add")

    def test_create_function_int128_lambda_explicit(self):
        chdb.create_function("i128_double", lambda x: x * 2, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_double(toInt128(500))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT i128_double(toInt128(-250))", "CSV")
        self.assertEqual(str(ret).strip(), "-500")
        chdb.drop_function("i128_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_int128_return_no_arg_types(self):
        def str_len_i128(s):
            return len(s)

        chdb.create_function("i128_strlen", str_len_i128, return_type=INT128)
        ret = self.session.query("SELECT i128_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT i128_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i128_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_i128(x: int) -> int:
            return x + 1

        chdb.create_function("i128_inc_override", inc_i128, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_inc_override(toInt128(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT i128_inc_override(toInt128(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i128_inc_override")

    # ── create_function: string type names ──

    def test_create_function_int128_string_types(self):
        chdb.create_function("i128_inc_str", lambda x: x + 1, arg_types=["Int128"], return_type="Int128")
        ret = self.session.query("SELECT i128_inc_str(toInt128(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT i128_inc_str(toInt128(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i128_inc_str")

    # ── create_function: int128 as arg_type ──

    def test_create_function_int128_as_arg_type(self):
        def neg_i128(x):
            return -x

        chdb.create_function("i128_neg", neg_i128, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_neg(toInt128(12345))", "CSV")
        self.assertEqual(str(ret).strip(), "-12345")
        ret = self.session.query("SELECT i128_neg(toInt128(-67890))", "CSV")
        self.assertEqual(str(ret).strip(), "67890")
        chdb.drop_function("i128_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("i128_dummy", dummy, arg_types=[INT128], return_type=INT128)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("i128_check", lambda x: x, arg_types=[INT128], return_type=INT128)
        with self.assertRaises(Exception):
            self.session.query("SELECT i128_check('hello')", "CSV")
        chdb.drop_function("i128_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("i128_compat", lambda x: x + 1, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_compat(toInt64(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("i128_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_int128_explicit_all(self):
        @func(arg_types=[INT128, INT128], return_type=INT128)
        def dec_i128_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_i128_add(toInt128(300), toInt128(400))", "CSV")
        self.assertEqual(str(ret).strip(), "700")
        ret = self.session.query("SELECT dec_i128_add(toInt128(-300), toInt128(400))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        chdb.drop_function("dec_i128_add")

    def test_func_decorator_int128_return_only(self):
        @func(return_type=INT128)
        def dec_i128_one(x):
            return 1

        ret = self.session.query("SELECT dec_i128_one(toInt128(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_i128_one(toInt128(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i128_one")

    # ── return value out of range (wide integer overflow not detected, wraps silently) ──

    def test_return_value_overflow_wraps(self):
        chdb.create_function("i128_overflow", lambda x: 2**127, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_overflow(toInt128(1))", "CSV")
        self.assertEqual(str(ret).strip(), str(-(2**127)))
        chdb.drop_function("i128_overflow")

    def test_return_value_underflow_wraps(self):
        chdb.create_function("i128_underflow", lambda x: -(2**127) - 1, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_underflow(toInt128(1))", "CSV")
        self.assertEqual(str(ret).strip(), str(2**127 - 1))
        chdb.drop_function("i128_underflow")

    # ── input arg out of range ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("i128_input_ovf", lambda x: x, arg_types=[INT128], return_type=INT128)
        with self.assertRaises(Exception):
            self.session.query("SELECT i128_input_ovf(toInt256(170141183460469231731687303715884105728))", "CSV")
        chdb.drop_function("i128_input_ovf")

    def test_input_arg_underflow_raises(self):
        chdb.create_function("i128_input_udf", lambda x: x, arg_types=[INT128], return_type=INT128)
        with self.assertRaises(Exception):
            self.session.query("SELECT i128_input_udf(toInt256(-170141183460469231731687303715884105729))", "CSV")
        chdb.drop_function("i128_input_udf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_int128_udf(self):
        chdb.create_function("i128_to_drop", lambda x: x + 1, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_to_drop(toInt128(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i128_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT i128_to_drop(toInt128(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=INT128)
        def i128_py_callable(x):
            return x + 1

        self.assertEqual(i128_py_callable(5), 6)
        self.assertEqual(i128_py_callable(-1), 0)
        chdb.drop_function("i128_py_callable")

    # ── large integers beyond Int64 range ──

    def test_large_integer_beyond_int64_range(self):
        chdb.create_function("i128_big_add", lambda x, y: x + y, arg_types=[INT128, INT128], return_type=INT128)
        ret = self.session.query(
            "SELECT i128_big_add(toInt128('10000000000000000000'), toInt128('20000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "30000000000000000000")
        chdb.drop_function("i128_big_add")

    def test_large_negative_integer_beyond_int64_range(self):
        chdb.create_function("i128_big_neg", lambda x: -x, arg_types=[INT128], return_type=INT128)
        ret = self.session.query("SELECT i128_big_neg(toInt128('10000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "-10000000000000000000")
        ret = self.session.query("SELECT i128_big_neg(toInt128('-10000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "10000000000000000000")
        chdb.drop_function("i128_big_neg")


@unittest.skipIf(_LITE, _LITE_BIG_INT_REASON)
class TestInt256UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_int256_return_explicit_arg_types(self):
        def add_i256(x, y):
            return x + y

        chdb.create_function("i256_add", add_i256, arg_types=[INT256, INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_add(toInt256(100), toInt256(200))", "CSV")
        self.assertEqual(str(ret).strip(), "300")
        ret = self.session.query("SELECT i256_add(toInt256(-50), toInt256(30))", "CSV")
        self.assertEqual(str(ret).strip(), "-20")
        chdb.drop_function("i256_add")

    def test_create_function_int256_lambda_explicit(self):
        chdb.create_function("i256_double", lambda x: x * 2, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_double(toInt256(500))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT i256_double(toInt256(-250))", "CSV")
        self.assertEqual(str(ret).strip(), "-500")
        chdb.drop_function("i256_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_int256_return_no_arg_types(self):
        def str_len_i256(s):
            return len(s)

        chdb.create_function("i256_strlen", str_len_i256, return_type=INT256)
        ret = self.session.query("SELECT i256_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT i256_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i256_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_i256(x: int) -> int:
            return x + 1

        chdb.create_function("i256_inc_override", inc_i256, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_inc_override(toInt256(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT i256_inc_override(toInt256(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i256_inc_override")

    # ── create_function: string type names ──

    def test_create_function_int256_string_types(self):
        chdb.create_function("i256_inc_str", lambda x: x + 1, arg_types=["Int256"], return_type="Int256")
        ret = self.session.query("SELECT i256_inc_str(toInt256(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT i256_inc_str(toInt256(-1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("i256_inc_str")

    # ── create_function: int256 as arg_type ──

    def test_create_function_int256_as_arg_type(self):
        def neg_i256(x):
            return -x

        chdb.create_function("i256_neg", neg_i256, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_neg(toInt256(12345))", "CSV")
        self.assertEqual(str(ret).strip(), "-12345")
        ret = self.session.query("SELECT i256_neg(toInt256(-67890))", "CSV")
        self.assertEqual(str(ret).strip(), "67890")
        chdb.drop_function("i256_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("i256_dummy", dummy, arg_types=[INT256], return_type=INT256)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("i256_check", lambda x: x, arg_types=[INT256], return_type=INT256)
        with self.assertRaises(Exception):
            self.session.query("SELECT i256_check('hello')", "CSV")
        chdb.drop_function("i256_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("i256_compat", lambda x: x + 1, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_compat(toInt128(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("i256_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_int256_explicit_all(self):
        @func(arg_types=[INT256, INT256], return_type=INT256)
        def dec_i256_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_i256_add(toInt256(300), toInt256(400))", "CSV")
        self.assertEqual(str(ret).strip(), "700")
        ret = self.session.query("SELECT dec_i256_add(toInt256(-300), toInt256(400))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        chdb.drop_function("dec_i256_add")

    def test_func_decorator_int256_return_only(self):
        @func(return_type=INT256)
        def dec_i256_one(x):
            return 1

        ret = self.session.query("SELECT dec_i256_one(toInt256(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_i256_one(toInt256(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_i256_one")

    # ── return value out of range (wide integer overflow not detected, wraps silently) ──

    def test_return_value_overflow_wraps(self):
        chdb.create_function("i256_overflow", lambda x: 2**255, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_overflow(toInt256(1))", "CSV")
        self.assertEqual(str(ret).strip(), str(-(2**255)))
        chdb.drop_function("i256_overflow")

    def test_return_value_underflow_wraps(self):
        chdb.create_function("i256_underflow", lambda x: -(2**255) - 1, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_underflow(toInt256(1))", "CSV")
        self.assertEqual(str(ret).strip(), str(2**255 - 1))
        chdb.drop_function("i256_underflow")

    # (no input arg overflow/underflow — Int256 is the widest signed integer type)

    # ── drop_function removes UDF ──

    def test_drop_function_removes_int256_udf(self):
        chdb.create_function("i256_to_drop", lambda x: x + 1, arg_types=[INT256], return_type=INT256)
        ret = self.session.query("SELECT i256_to_drop(toInt256(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("i256_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT i256_to_drop(toInt256(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=INT256)
        def i256_py_callable(x):
            return x + 1

        self.assertEqual(i256_py_callable(5), 6)
        self.assertEqual(i256_py_callable(-1), 0)
        chdb.drop_function("i256_py_callable")

    # ── large integers beyond Int64 range ──

    def test_large_integer_beyond_int64_range(self):
        chdb.create_function("i256_big64", lambda x, y: x + y, arg_types=[INT256, INT256], return_type=INT256)
        ret = self.session.query(
            "SELECT i256_big64(toInt256('10000000000000000000'), toInt256('20000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "30000000000000000000")
        chdb.drop_function("i256_big64")

    # ── large integers beyond Int128 range ──

    def test_large_integer_beyond_int128_range(self):
        v = 200000000000000000000000000000000000000
        chdb.create_function("i256_huge_add", lambda x, y: x + y, arg_types=[INT256, INT256], return_type=INT256)
        ret = self.session.query(f"SELECT i256_huge_add(toInt256('{v}'), toInt256('{v}'))", "CSV")
        self.assertEqual(str(ret).strip(), str(v * 2))
        chdb.drop_function("i256_huge_add")

    def test_large_negative_integer_beyond_int128_range(self):
        v = 200000000000000000000000000000000000000
        chdb.create_function("i256_huge_neg", lambda x: -x, arg_types=[INT256], return_type=INT256)
        ret = self.session.query(f"SELECT i256_huge_neg(toInt256('{v}'))", "CSV")
        self.assertEqual(str(ret).strip(), str(-v))
        ret = self.session.query(f"SELECT i256_huge_neg(toInt256('{-v}'))", "CSV")
        self.assertEqual(str(ret).strip(), str(v))
        chdb.drop_function("i256_huge_neg")


# ═══════════════════════════════════════════════════════════════════
# Unsigned Integer Types
# ═══════════════════════════════════════════════════════════════════


class TestUInt8UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_uint8_return_explicit_arg_types(self):
        def add_u8(x, y):
            return x + y

        chdb.create_function("u8_add", add_u8, arg_types=[UINT8, UINT8], return_type=UINT8)
        ret = self.session.query("SELECT u8_add(toUInt8(10), toUInt8(20))", "CSV")
        self.assertEqual(str(ret).strip(), "30")
        ret = self.session.query("SELECT u8_add(toUInt8(100), toUInt8(50))", "CSV")
        self.assertEqual(str(ret).strip(), "150")
        chdb.drop_function("u8_add")

    def test_create_function_uint8_lambda_explicit(self):
        chdb.create_function("u8_double", lambda x: x * 2, arg_types=[UINT8], return_type=UINT8)
        ret = self.session.query("SELECT u8_double(toUInt8(50))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT u8_double(toUInt8(10))", "CSV")
        self.assertEqual(str(ret).strip(), "20")
        chdb.drop_function("u8_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_uint8_return_no_arg_types(self):
        def str_len_u8(s):
            return len(s)

        chdb.create_function("u8_strlen", str_len_u8, return_type=UINT8)
        ret = self.session.query("SELECT u8_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT u8_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u8_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_u8(x: int) -> int:
            return x + 1

        chdb.create_function("u8_inc_override", inc_u8, arg_types=[UINT8], return_type=UINT8)
        ret = self.session.query("SELECT u8_inc_override(toUInt8(10))", "CSV")
        self.assertEqual(str(ret).strip(), "11")
        ret = self.session.query("SELECT u8_inc_override(toUInt8(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u8_inc_override")

    # ── create_function: string type names ──

    def test_create_function_uint8_string_types(self):
        chdb.create_function("u8_inc_str", lambda x: x + 1, arg_types=["UInt8"], return_type="UInt8")
        ret = self.session.query("SELECT u8_inc_str(toUInt8(9))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        ret = self.session.query("SELECT u8_inc_str(toUInt8(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u8_inc_str")

    # ── create_function: uint8 as arg_type ──

    def test_create_function_uint8_as_arg_type(self):
        def identity_u8(x):
            return x

        chdb.create_function("u8_id", identity_u8, arg_types=[UINT8], return_type=UINT8)
        ret = self.session.query("SELECT u8_id(toUInt8(0))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT u8_id(toUInt8(255))", "CSV")
        self.assertEqual(str(ret).strip(), "255")
        chdb.drop_function("u8_id")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("u8_dummy", dummy, arg_types=[UINT8], return_type=UINT8)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("u8_check", lambda x: x, arg_types=[UINT8], return_type=UINT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT u8_check('hello')", "CSV")
        chdb.drop_function("u8_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("u8_compat", lambda x: x + 1, arg_types=[UINT16], return_type=UINT8)
        ret = self.session.query("SELECT u8_compat(toUInt8(5))", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        chdb.drop_function("u8_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_uint8_explicit_all(self):
        @func(arg_types=[UINT8, UINT8], return_type=UINT8)
        def dec_u8_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_u8_add(toUInt8(3), toUInt8(4))", "CSV")
        self.assertEqual(str(ret).strip(), "7")
        ret = self.session.query("SELECT dec_u8_add(toUInt8(100), toUInt8(50))", "CSV")
        self.assertEqual(str(ret).strip(), "150")
        chdb.drop_function("dec_u8_add")

    def test_func_decorator_uint8_return_only(self):
        @func(return_type=UINT8)
        def dec_u8_one(x):
            return 1

        ret = self.session.query("SELECT dec_u8_one(toUInt8(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_u8_one(toUInt8(99))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_u8_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("u8_overflow", lambda x: 300, arg_types=[UINT8], return_type=UINT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT u8_overflow(toUInt8(1))", "CSV")
        chdb.drop_function("u8_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("u8_underflow", lambda x: -1, arg_types=[UINT8], return_type=UINT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT u8_underflow(toUInt8(1))", "CSV")
        chdb.drop_function("u8_underflow")

    # ── input arg out of range ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("u8_input_ovf", lambda x: x, arg_types=[UINT8], return_type=UINT8)
        with self.assertRaises(Exception):
            self.session.query("SELECT u8_input_ovf(toUInt16(300))", "CSV")
        chdb.drop_function("u8_input_ovf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_uint8_udf(self):
        chdb.create_function("u8_to_drop", lambda x: x + 1, arg_types=[UINT8], return_type=UINT8)
        ret = self.session.query("SELECT u8_to_drop(toUInt8(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u8_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT u8_to_drop(toUInt8(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=UINT8)
        def u8_py_callable(x):
            return x + 1

        self.assertEqual(u8_py_callable(5), 6)
        self.assertEqual(u8_py_callable(0), 1)
        chdb.drop_function("u8_py_callable")


class TestUInt16UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_uint16_return_explicit_arg_types(self):
        def add_u16(x, y):
            return x + y

        chdb.create_function("u16_add", add_u16, arg_types=[UINT16, UINT16], return_type=UINT16)
        ret = self.session.query("SELECT u16_add(toUInt16(1000), toUInt16(2000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000")
        ret = self.session.query("SELECT u16_add(toUInt16(30000), toUInt16(20000))", "CSV")
        self.assertEqual(str(ret).strip(), "50000")
        chdb.drop_function("u16_add")

    def test_create_function_uint16_lambda_explicit(self):
        chdb.create_function("u16_double", lambda x: x * 2, arg_types=[UINT16], return_type=UINT16)
        ret = self.session.query("SELECT u16_double(toUInt16(100))", "CSV")
        self.assertEqual(str(ret).strip(), "200")
        ret = self.session.query("SELECT u16_double(toUInt16(500))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        chdb.drop_function("u16_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_uint16_return_no_arg_types(self):
        def str_len_u16(s):
            return len(s)

        chdb.create_function("u16_strlen", str_len_u16, return_type=UINT16)
        ret = self.session.query("SELECT u16_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT u16_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u16_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_u16(x: int) -> int:
            return x + 1

        chdb.create_function("u16_inc_override", inc_u16, arg_types=[UINT16], return_type=UINT16)
        ret = self.session.query("SELECT u16_inc_override(toUInt16(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT u16_inc_override(toUInt16(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u16_inc_override")

    # ── create_function: string type names ──

    def test_create_function_uint16_string_types(self):
        chdb.create_function("u16_inc_str", lambda x: x + 1, arg_types=["UInt16"], return_type="UInt16")
        ret = self.session.query("SELECT u16_inc_str(toUInt16(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT u16_inc_str(toUInt16(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u16_inc_str")

    # ── create_function: uint16 as arg_type ──

    def test_create_function_uint16_as_arg_type(self):
        def identity_u16(x):
            return x

        chdb.create_function("u16_id", identity_u16, arg_types=[UINT16], return_type=UINT16)
        ret = self.session.query("SELECT u16_id(toUInt16(0))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT u16_id(toUInt16(65535))", "CSV")
        self.assertEqual(str(ret).strip(), "65535")
        chdb.drop_function("u16_id")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("u16_dummy", dummy, arg_types=[UINT16], return_type=UINT16)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("u16_check", lambda x: x, arg_types=[UINT16], return_type=UINT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT u16_check('hello')", "CSV")
        chdb.drop_function("u16_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("u16_compat", lambda x: x + 1, arg_types=[UINT16], return_type=UINT16)
        ret = self.session.query("SELECT u16_compat(toUInt8(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("u16_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_uint16_explicit_all(self):
        @func(arg_types=[UINT16, UINT16], return_type=UINT16)
        def dec_u16_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_u16_add(toUInt16(300), toUInt16(400))", "CSV")
        self.assertEqual(str(ret).strip(), "700")
        ret = self.session.query("SELECT dec_u16_add(toUInt16(30000), toUInt16(20000))", "CSV")
        self.assertEqual(str(ret).strip(), "50000")
        chdb.drop_function("dec_u16_add")

    def test_func_decorator_uint16_return_only(self):
        @func(return_type=UINT16)
        def dec_u16_one(x):
            return 1

        ret = self.session.query("SELECT dec_u16_one(toUInt16(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_u16_one(toUInt16(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_u16_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("u16_overflow", lambda x: 70000, arg_types=[UINT16], return_type=UINT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT u16_overflow(toUInt16(1))", "CSV")
        chdb.drop_function("u16_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("u16_underflow", lambda x: -1, arg_types=[UINT16], return_type=UINT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT u16_underflow(toUInt16(1))", "CSV")
        chdb.drop_function("u16_underflow")

    # ── input arg out of range ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("u16_input_ovf", lambda x: x, arg_types=[UINT16], return_type=UINT16)
        with self.assertRaises(Exception):
            self.session.query("SELECT u16_input_ovf(toUInt32(70000))", "CSV")
        chdb.drop_function("u16_input_ovf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_uint16_udf(self):
        chdb.create_function("u16_to_drop", lambda x: x + 1, arg_types=[UINT16], return_type=UINT16)
        ret = self.session.query("SELECT u16_to_drop(toUInt16(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u16_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT u16_to_drop(toUInt16(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=UINT16)
        def u16_py_callable(x):
            return x + 1

        self.assertEqual(u16_py_callable(5), 6)
        self.assertEqual(u16_py_callable(0), 1)
        chdb.drop_function("u16_py_callable")


class TestUInt32UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_uint32_return_explicit_arg_types(self):
        def add_u32(x, y):
            return x + y

        chdb.create_function("u32_add", add_u32, arg_types=[UINT32, UINT32], return_type=UINT32)
        ret = self.session.query("SELECT u32_add(toUInt32(100000), toUInt32(200000))", "CSV")
        self.assertEqual(str(ret).strip(), "300000")
        ret = self.session.query("SELECT u32_add(toUInt32(3000000000), toUInt32(1000000000))", "CSV")
        self.assertEqual(str(ret).strip(), "4000000000")
        chdb.drop_function("u32_add")

    def test_create_function_uint32_lambda_explicit(self):
        chdb.create_function("u32_double", lambda x: x * 2, arg_types=[UINT32], return_type=UINT32)
        ret = self.session.query("SELECT u32_double(toUInt32(500))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT u32_double(toUInt32(1000000))", "CSV")
        self.assertEqual(str(ret).strip(), "2000000")
        chdb.drop_function("u32_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_uint32_return_no_arg_types(self):
        def str_len_u32(s):
            return len(s)

        chdb.create_function("u32_strlen", str_len_u32, return_type=UINT32)
        ret = self.session.query("SELECT u32_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT u32_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u32_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_u32(x: int) -> int:
            return x + 1

        chdb.create_function("u32_inc_override", inc_u32, arg_types=[UINT32], return_type=UINT32)
        ret = self.session.query("SELECT u32_inc_override(toUInt32(99999))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        ret = self.session.query("SELECT u32_inc_override(toUInt32(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u32_inc_override")

    # ── create_function: string type names ──

    def test_create_function_uint32_string_types(self):
        chdb.create_function("u32_inc_str", lambda x: x + 1, arg_types=["UInt32"], return_type="UInt32")
        ret = self.session.query("SELECT u32_inc_str(toUInt32(99999))", "CSV")
        self.assertEqual(str(ret).strip(), "100000")
        ret = self.session.query("SELECT u32_inc_str(toUInt32(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u32_inc_str")

    # ── create_function: uint32 as arg_type ──

    def test_create_function_uint32_as_arg_type(self):
        def identity_u32(x):
            return x

        chdb.create_function("u32_id", identity_u32, arg_types=[UINT32], return_type=UINT32)
        ret = self.session.query("SELECT u32_id(toUInt32(0))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT u32_id(toUInt32(4294967295))", "CSV")
        self.assertEqual(str(ret).strip(), "4294967295")
        chdb.drop_function("u32_id")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("u32_dummy", dummy, arg_types=[UINT32], return_type=UINT32)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("u32_check", lambda x: x, arg_types=[UINT32], return_type=UINT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT u32_check('hello')", "CSV")
        chdb.drop_function("u32_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("u32_compat", lambda x: x + 1, arg_types=[UINT32], return_type=UINT32)
        ret = self.session.query("SELECT u32_compat(toUInt16(1000))", "CSV")
        self.assertEqual(str(ret).strip(), "1001")
        chdb.drop_function("u32_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_uint32_explicit_all(self):
        @func(arg_types=[UINT32, UINT32], return_type=UINT32)
        def dec_u32_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_u32_add(toUInt32(30000), toUInt32(40000))", "CSV")
        self.assertEqual(str(ret).strip(), "70000")
        ret = self.session.query("SELECT dec_u32_add(toUInt32(2000000000), toUInt32(1000000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000000")
        chdb.drop_function("dec_u32_add")

    def test_func_decorator_uint32_return_only(self):
        @func(return_type=UINT32)
        def dec_u32_one(x):
            return 1

        ret = self.session.query("SELECT dec_u32_one(toUInt32(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_u32_one(toUInt32(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_u32_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("u32_overflow", lambda x: 4300000000, arg_types=[UINT32], return_type=UINT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT u32_overflow(toUInt32(1))", "CSV")
        chdb.drop_function("u32_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("u32_underflow", lambda x: -1, arg_types=[UINT32], return_type=UINT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT u32_underflow(toUInt32(1))", "CSV")
        chdb.drop_function("u32_underflow")

    # ── input arg out of range ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("u32_input_ovf", lambda x: x, arg_types=[UINT32], return_type=UINT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT u32_input_ovf(toUInt64(4300000000))", "CSV")
        chdb.drop_function("u32_input_ovf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_uint32_udf(self):
        chdb.create_function("u32_to_drop", lambda x: x + 1, arg_types=[UINT32], return_type=UINT32)
        ret = self.session.query("SELECT u32_to_drop(toUInt32(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u32_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT u32_to_drop(toUInt32(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=UINT32)
        def u32_py_callable(x):
            return x + 1

        self.assertEqual(u32_py_callable(5), 6)
        self.assertEqual(u32_py_callable(0), 1)
        chdb.drop_function("u32_py_callable")


class TestUInt64UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_uint64_return_explicit_arg_types(self):
        def add_u64(x, y):
            return x + y

        chdb.create_function("u64_add", add_u64, arg_types=[UINT64, UINT64], return_type=UINT64)
        ret = self.session.query("SELECT u64_add(toUInt64(1000000), toUInt64(2000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000")
        ret = self.session.query("SELECT u64_add(toUInt64(10000000000), toUInt64(5000000000))", "CSV")
        self.assertEqual(str(ret).strip(), "15000000000")
        chdb.drop_function("u64_add")

    def test_create_function_uint64_lambda_explicit(self):
        chdb.create_function("u64_double", lambda x: x * 2, arg_types=[UINT64], return_type=UINT64)
        ret = self.session.query("SELECT u64_double(toUInt64(500000))", "CSV")
        self.assertEqual(str(ret).strip(), "1000000")
        ret = self.session.query("SELECT u64_double(toUInt64(5000000000))", "CSV")
        self.assertEqual(str(ret).strip(), "10000000000")
        chdb.drop_function("u64_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_uint64_return_no_arg_types(self):
        def str_len_u64(s):
            return len(s)

        chdb.create_function("u64_strlen", str_len_u64, return_type=UINT64)
        ret = self.session.query("SELECT u64_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT u64_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u64_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_u64(x: int) -> int:
            return x + 1

        chdb.create_function("u64_inc_override", inc_u64, arg_types=[UINT64], return_type=UINT64)
        ret = self.session.query("SELECT u64_inc_override(toUInt64(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1000000")
        ret = self.session.query("SELECT u64_inc_override(toUInt64(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u64_inc_override")

    # ── create_function: string type names ──

    def test_create_function_uint64_string_types(self):
        chdb.create_function("u64_inc_str", lambda x: x + 1, arg_types=["UInt64"], return_type="UInt64")
        ret = self.session.query("SELECT u64_inc_str(toUInt64(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1000000")
        ret = self.session.query("SELECT u64_inc_str(toUInt64(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u64_inc_str")

    # ── create_function: uint64 as arg_type ──

    def test_create_function_uint64_as_arg_type(self):
        def identity_u64(x):
            return x

        chdb.create_function("u64_id", identity_u64, arg_types=[UINT64], return_type=UINT64)
        ret = self.session.query("SELECT u64_id(toUInt64(0))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT u64_id(toUInt64(18446744073709551615))", "CSV")
        self.assertEqual(str(ret).strip(), "18446744073709551615")
        chdb.drop_function("u64_id")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("u64_dummy", dummy, arg_types=[UINT64], return_type=UINT64)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("u64_check", lambda x: x, arg_types=[UINT64], return_type=UINT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT u64_check('hello')", "CSV")
        chdb.drop_function("u64_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("u64_compat", lambda x: x + 1, arg_types=[UINT64], return_type=UINT64)
        ret = self.session.query("SELECT u64_compat(toUInt32(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("u64_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_uint64_explicit_all(self):
        @func(arg_types=[UINT64, UINT64], return_type=UINT64)
        def dec_u64_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_u64_add(toUInt64(300000), toUInt64(400000))", "CSV")
        self.assertEqual(str(ret).strip(), "700000")
        ret = self.session.query("SELECT dec_u64_add(toUInt64(10000000000), toUInt64(5000000000))", "CSV")
        self.assertEqual(str(ret).strip(), "15000000000")
        chdb.drop_function("dec_u64_add")

    def test_func_decorator_uint64_return_only(self):
        @func(return_type=UINT64)
        def dec_u64_one(x):
            return 1

        ret = self.session.query("SELECT dec_u64_one(toUInt64(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_u64_one(toUInt64(999999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_u64_one")

    # ── return value out of range ──

    def test_return_value_overflow_raises(self):
        chdb.create_function("u64_overflow", lambda x: 2**64, arg_types=[UINT64], return_type=UINT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT u64_overflow(toUInt64(1))", "CSV")
        chdb.drop_function("u64_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("u64_underflow", lambda x: -1, arg_types=[UINT64], return_type=UINT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT u64_underflow(toUInt64(1))", "CSV")
        chdb.drop_function("u64_underflow")

    # ── input arg out of range ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("u64_input_ovf", lambda x: x, arg_types=[UINT64], return_type=UINT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT u64_input_ovf(toUInt128(18446744073709551616))", "CSV")
        chdb.drop_function("u64_input_ovf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_uint64_udf(self):
        chdb.create_function("u64_to_drop", lambda x: x + 1, arg_types=[UINT64], return_type=UINT64)
        ret = self.session.query("SELECT u64_to_drop(toUInt64(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u64_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT u64_to_drop(toUInt64(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=UINT64)
        def u64_py_callable(x):
            return x + 1

        self.assertEqual(u64_py_callable(5), 6)
        self.assertEqual(u64_py_callable(0), 1)
        chdb.drop_function("u64_py_callable")


@unittest.skipIf(_LITE, _LITE_BIG_INT_REASON)
class TestUInt128UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_uint128_return_explicit_arg_types(self):
        def add_u128(x, y):
            return x + y

        chdb.create_function("u128_add", add_u128, arg_types=[UINT128, UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_add(toUInt128(100), toUInt128(200))", "CSV")
        self.assertEqual(str(ret).strip(), "300")
        ret = self.session.query("SELECT u128_add(toUInt128(1000000), toUInt128(2000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000")
        chdb.drop_function("u128_add")

    def test_create_function_uint128_lambda_explicit(self):
        chdb.create_function("u128_double", lambda x: x * 2, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_double(toUInt128(500))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT u128_double(toUInt128(1000000))", "CSV")
        self.assertEqual(str(ret).strip(), "2000000")
        chdb.drop_function("u128_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_uint128_return_no_arg_types(self):
        def str_len_u128(s):
            return len(s)

        chdb.create_function("u128_strlen", str_len_u128, return_type=UINT128)
        ret = self.session.query("SELECT u128_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT u128_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u128_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_u128(x: int) -> int:
            return x + 1

        chdb.create_function("u128_inc_override", inc_u128, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_inc_override(toUInt128(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT u128_inc_override(toUInt128(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u128_inc_override")

    # ── create_function: string type names ──

    def test_create_function_uint128_string_types(self):
        chdb.create_function("u128_inc_str", lambda x: x + 1, arg_types=["UInt128"], return_type="UInt128")
        ret = self.session.query("SELECT u128_inc_str(toUInt128(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT u128_inc_str(toUInt128(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u128_inc_str")

    # ── create_function: uint128 as arg_type ──

    def test_create_function_uint128_as_arg_type(self):
        def identity_u128(x):
            return x

        chdb.create_function("u128_id", identity_u128, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_id(toUInt128(0))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT u128_id(toUInt128(12345))", "CSV")
        self.assertEqual(str(ret).strip(), "12345")
        chdb.drop_function("u128_id")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("u128_dummy", dummy, arg_types=[UINT128], return_type=UINT128)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("u128_check", lambda x: x, arg_types=[UINT128], return_type=UINT128)
        with self.assertRaises(Exception):
            self.session.query("SELECT u128_check('hello')", "CSV")
        chdb.drop_function("u128_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("u128_compat", lambda x: x + 1, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_compat(toUInt64(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("u128_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_uint128_explicit_all(self):
        @func(arg_types=[UINT128, UINT128], return_type=UINT128)
        def dec_u128_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_u128_add(toUInt128(300), toUInt128(400))", "CSV")
        self.assertEqual(str(ret).strip(), "700")
        ret = self.session.query("SELECT dec_u128_add(toUInt128(1000000), toUInt128(2000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000")
        chdb.drop_function("dec_u128_add")

    def test_func_decorator_uint128_return_only(self):
        @func(return_type=UINT128)
        def dec_u128_one(x):
            return 1

        ret = self.session.query("SELECT dec_u128_one(toUInt128(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_u128_one(toUInt128(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_u128_one")

    # ── return value out of range (wide integer overflow not detected, wraps silently) ──

    def test_return_value_overflow_wraps(self):
        chdb.create_function("u128_overflow", lambda x: 2**128, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_overflow(toUInt128(1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("u128_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("u128_underflow", lambda x: -1, arg_types=[UINT128], return_type=UINT128)
        with self.assertRaises(Exception):
            self.session.query("SELECT u128_underflow(toUInt128(1))", "CSV")
        chdb.drop_function("u128_underflow")

    # ── input arg out of range ──

    def test_input_arg_overflow_raises(self):
        chdb.create_function("u128_input_ovf", lambda x: x, arg_types=[UINT128], return_type=UINT128)
        with self.assertRaises(Exception):
            self.session.query("SELECT u128_input_ovf(toUInt256(340282366920938463463374607431768211456))", "CSV")
        chdb.drop_function("u128_input_ovf")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_uint128_udf(self):
        chdb.create_function("u128_to_drop", lambda x: x + 1, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_to_drop(toUInt128(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u128_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT u128_to_drop(toUInt128(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=UINT128)
        def u128_py_callable(x):
            return x + 1

        self.assertEqual(u128_py_callable(5), 6)
        self.assertEqual(u128_py_callable(0), 1)
        chdb.drop_function("u128_py_callable")

    # ── large integers beyond UInt64 range ──

    def test_large_integer_beyond_uint64_range(self):
        chdb.create_function("u128_big_add", lambda x, y: x + y, arg_types=[UINT128, UINT128], return_type=UINT128)
        ret = self.session.query(
            "SELECT u128_big_add(toUInt128('100000000000000000000'), toUInt128('200000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "300000000000000000000")
        chdb.drop_function("u128_big_add")

    def test_large_integer_identity_beyond_uint64_range(self):
        chdb.create_function("u128_big_id", lambda x: x, arg_types=[UINT128], return_type=UINT128)
        ret = self.session.query("SELECT u128_big_id(toUInt128('100000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "100000000000000000000")
        chdb.drop_function("u128_big_id")


@unittest.skipIf(_LITE, _LITE_BIG_INT_REASON)
class TestUInt256UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_uint256_return_explicit_arg_types(self):
        def add_u256(x, y):
            return x + y

        chdb.create_function("u256_add", add_u256, arg_types=[UINT256, UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_add(toUInt256(100), toUInt256(200))", "CSV")
        self.assertEqual(str(ret).strip(), "300")
        ret = self.session.query("SELECT u256_add(toUInt256(1000000), toUInt256(2000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000")
        chdb.drop_function("u256_add")

    def test_create_function_uint256_lambda_explicit(self):
        chdb.create_function("u256_double", lambda x: x * 2, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_double(toUInt256(500))", "CSV")
        self.assertEqual(str(ret).strip(), "1000")
        ret = self.session.query("SELECT u256_double(toUInt256(1000000))", "CSV")
        self.assertEqual(str(ret).strip(), "2000000")
        chdb.drop_function("u256_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_uint256_return_no_arg_types(self):
        def str_len_u256(s):
            return len(s)

        chdb.create_function("u256_strlen", str_len_u256, return_type=UINT256)
        ret = self.session.query("SELECT u256_strlen('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT u256_strlen('hi')", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u256_strlen")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_u256(x: int) -> int:
            return x + 1

        chdb.create_function("u256_inc_override", inc_u256, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_inc_override(toUInt256(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT u256_inc_override(toUInt256(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u256_inc_override")

    # ── create_function: string type names ──

    def test_create_function_uint256_string_types(self):
        chdb.create_function("u256_inc_str", lambda x: x + 1, arg_types=["UInt256"], return_type="UInt256")
        ret = self.session.query("SELECT u256_inc_str(toUInt256(99))", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        ret = self.session.query("SELECT u256_inc_str(toUInt256(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("u256_inc_str")

    # ── create_function: uint256 as arg_type ──

    def test_create_function_uint256_as_arg_type(self):
        def identity_u256(x):
            return x

        chdb.create_function("u256_id", identity_u256, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_id(toUInt256(0))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT u256_id(toUInt256(12345))", "CSV")
        self.assertEqual(str(ret).strip(), "12345")
        chdb.drop_function("u256_id")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("u256_dummy", dummy, arg_types=[UINT256], return_type=UINT256)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("u256_check", lambda x: x, arg_types=[UINT256], return_type=UINT256)
        with self.assertRaises(Exception):
            self.session.query("SELECT u256_check('hello')", "CSV")
        chdb.drop_function("u256_check")

    # ── create_function: compatible arg type (smaller type → declared type) ──

    def test_create_function_compatible_arg_type(self):
        chdb.create_function("u256_compat", lambda x: x + 1, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_compat(toUInt128(42))", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("u256_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_uint256_explicit_all(self):
        @func(arg_types=[UINT256, UINT256], return_type=UINT256)
        def dec_u256_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_u256_add(toUInt256(300), toUInt256(400))", "CSV")
        self.assertEqual(str(ret).strip(), "700")
        ret = self.session.query("SELECT dec_u256_add(toUInt256(1000000), toUInt256(2000000))", "CSV")
        self.assertEqual(str(ret).strip(), "3000000")
        chdb.drop_function("dec_u256_add")

    def test_func_decorator_uint256_return_only(self):
        @func(return_type=UINT256)
        def dec_u256_one(x):
            return 1

        ret = self.session.query("SELECT dec_u256_one(toUInt256(0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query("SELECT dec_u256_one(toUInt256(999))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_u256_one")

    # ── return value out of range (wide integer overflow not detected, wraps silently) ──

    def test_return_value_overflow_wraps(self):
        chdb.create_function("u256_overflow", lambda x: 2**256, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_overflow(toUInt256(1))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("u256_overflow")

    def test_return_value_underflow_raises(self):
        chdb.create_function("u256_underflow", lambda x: -1, arg_types=[UINT256], return_type=UINT256)
        with self.assertRaises(Exception):
            self.session.query("SELECT u256_underflow(toUInt256(1))", "CSV")
        chdb.drop_function("u256_underflow")

    # (no input arg overflow — UInt256 is the widest unsigned integer type)

    # ── drop_function removes UDF ──

    def test_drop_function_removes_uint256_udf(self):
        chdb.create_function("u256_to_drop", lambda x: x + 1, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query("SELECT u256_to_drop(toUInt256(1))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("u256_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT u256_to_drop(toUInt256(1))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=UINT256)
        def u256_py_callable(x):
            return x + 1

        self.assertEqual(u256_py_callable(5), 6)
        self.assertEqual(u256_py_callable(0), 1)
        chdb.drop_function("u256_py_callable")

    # ── large integers beyond UInt64 range ──

    def test_large_integer_beyond_uint64_range(self):
        chdb.create_function("u256_big64", lambda x, y: x + y, arg_types=[UINT256, UINT256], return_type=UINT256)
        ret = self.session.query(
            "SELECT u256_big64(toUInt256('100000000000000000000'), toUInt256('200000000000000000000'))", "CSV")
        self.assertEqual(str(ret).strip(), "300000000000000000000")
        chdb.drop_function("u256_big64")

    # ── large integers beyond UInt128 range ──

    def test_large_integer_beyond_uint128_range(self):
        v = 400000000000000000000000000000000000000
        chdb.create_function("u256_huge_add", lambda x, y: x + y, arg_types=[UINT256, UINT256], return_type=UINT256)
        ret = self.session.query(f"SELECT u256_huge_add(toUInt256('{v}'), toUInt256('{v}'))", "CSV")
        self.assertEqual(str(ret).strip(), str(v * 2))
        chdb.drop_function("u256_huge_add")

    def test_large_integer_identity_beyond_uint128_range(self):
        v = 400000000000000000000000000000000000000
        chdb.create_function("u256_huge_id", lambda x: x, arg_types=[UINT256], return_type=UINT256)
        ret = self.session.query(f"SELECT u256_huge_id(toUInt256('{v}'))", "CSV")
        self.assertEqual(str(ret).strip(), str(v))
        chdb.drop_function("u256_huge_id")


# ═══════════════════════════════════════════════════════════════════
# Float Types
# ═══════════════════════════════════════════════════════════════════


class TestFloat32UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_float32_return_explicit_arg_types(self):
        def half_f32(x):
            return x / 2

        chdb.create_function("f32_half", half_f32, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_half(toFloat32(7.0))", "CSV")
        self.assertEqual(str(ret).strip(), "3.5")
        ret = self.session.query("SELECT f32_half(toFloat32(-4.0))", "CSV")
        self.assertEqual(str(ret).strip(), "-2")
        chdb.drop_function("f32_half")

    def test_create_function_float32_lambda_explicit(self):
        chdb.create_function("f32_double", lambda x: x * 2, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_double(toFloat32(1.5))", "CSV")
        self.assertEqual(str(ret).strip(), "3")
        ret = self.session.query("SELECT f32_double(toFloat32(-2.5))", "CSV")
        self.assertEqual(str(ret).strip(), "-5")
        chdb.drop_function("f32_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_float32_return_no_arg_types(self):
        def const_pi(x):
            return 3.14

        chdb.create_function("f32_pi", const_pi, return_type=FLOAT32)
        ret = self.session.query("SELECT f32_pi(toFloat32(0.0))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 3.14, places=2)
        chdb.drop_function("f32_pi")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_f32(x: float) -> float:
            return x + 1.0

        chdb.create_function("f32_inc_override", inc_f32, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_inc_override(toFloat32(2.5))", "CSV")
        self.assertEqual(str(ret).strip(), "3.5")
        chdb.drop_function("f32_inc_override")

    # ── create_function: string type names ──

    def test_create_function_float32_string_types(self):
        chdb.create_function("f32_inc_str", lambda x: x + 1, arg_types=["Float32"], return_type="Float32")
        ret = self.session.query("SELECT f32_inc_str(toFloat32(9.5))", "CSV")
        self.assertEqual(str(ret).strip(), "10.5")
        chdb.drop_function("f32_inc_str")

    # ── create_function: float32 as arg_type ──

    def test_create_function_float32_as_arg_type(self):
        def neg_f32(x):
            return -x

        chdb.create_function("f32_neg", neg_f32, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_neg(toFloat32(3.5))", "CSV")
        self.assertEqual(str(ret).strip(), "-3.5")
        ret = self.session.query("SELECT f32_neg(toFloat32(-1.25))", "CSV")
        self.assertEqual(str(ret).strip(), "1.25")
        chdb.drop_function("f32_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("f32_dummy", dummy, arg_types=[FLOAT32], return_type=FLOAT32)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("f32_check", lambda x: x, arg_types=[FLOAT32], return_type=FLOAT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT f32_check('hello')", "CSV")
        chdb.drop_function("f32_check")

    # ── create_function: compatible arg type (integer → Float32) ──

    def test_create_function_compatible_arg_type_int_to_float32(self):
        chdb.create_function("f32_compat", lambda x: x + 0.5, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_compat(toInt8(3))", "CSV")
        self.assertEqual(str(ret).strip(), "3.5")
        chdb.drop_function("f32_compat")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_float32_explicit_all(self):
        @func(arg_types=[FLOAT32, FLOAT32], return_type=FLOAT32)
        def dec_f32_add(x, y):
            return x + y

        ret = self.session.query("SELECT dec_f32_add(toFloat32(1.5), toFloat32(2.5))", "CSV")
        self.assertEqual(str(ret).strip(), "4")
        chdb.drop_function("dec_f32_add")

    def test_func_decorator_float32_return_only(self):
        @func(return_type=FLOAT32)
        def dec_f32_one(x):
            return 1.0

        ret = self.session.query("SELECT dec_f32_one(toFloat32(999.0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_f32_one")

    # ── special float values ──

    def test_float32_zero_and_negative_zero(self):
        chdb.create_function("f32_id", lambda x: x, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_id(toFloat32(0.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), 0.0)
        ret = self.session.query("SELECT f32_id(toFloat32(-0.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), 0.0)
        chdb.drop_function("f32_id")

    def test_float32_very_small_value(self):
        chdb.create_function("f32_tiny", lambda x: x * 2, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_tiny(toFloat32(0.001))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 0.002, places=3)
        chdb.drop_function("f32_tiny")

    def test_float32_return_int_as_float(self):
        chdb.create_function("f32_ret_int", lambda x: 42, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_ret_int(toFloat32(0.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), 42.0)
        chdb.drop_function("f32_ret_int")

    def test_float32_return_computed_int_as_float(self):
        chdb.create_function("f32_ret_comp_int", lambda x: int(x) * 3, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_ret_comp_int(toFloat32(4.7))", "CSV")
        self.assertEqual(float(str(ret).strip()), 12.0)
        chdb.drop_function("f32_ret_comp_int")

    def test_float32_return_negative_int_as_float(self):
        chdb.create_function("f32_ret_neg_int", lambda x: -5, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_ret_neg_int(toFloat32(1.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), -5.0)
        chdb.drop_function("f32_ret_neg_int")

    def test_float32_return_zero_int_as_float(self):
        chdb.create_function("f32_ret_zero_int", lambda x: 0, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_ret_zero_int(toFloat32(9.9))", "CSV")
        self.assertEqual(float(str(ret).strip()), 0.0)
        chdb.drop_function("f32_ret_zero_int")

    def test_float32_return_bool_as_float_rejected(self):
        chdb.create_function("f32_ret_bool_t", lambda x: True, arg_types=[FLOAT32], return_type=FLOAT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT f32_ret_bool_t(toFloat32(0.0))", "CSV")
        chdb.drop_function("f32_ret_bool_t")

    # ── compatible arg type: various int types → Float32 ──
    # Int8/Int16/UInt8/UInt16 auto-convert to Float32 (no precision loss)

    def test_float32_compatible_arg_int16_to_float32(self):
        chdb.create_function("f32_from_i16", lambda x: x + 0.5, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_from_i16(toInt16(10))", "CSV")
        self.assertEqual(str(ret).strip(), "10.5")
        chdb.drop_function("f32_from_i16")

    def test_float32_compatible_arg_uint16_to_float32(self):
        chdb.create_function("f32_from_u16", lambda x: x + 0.1, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_from_u16(toUInt16(20))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 20.1, places=1)
        chdb.drop_function("f32_from_u16")

    # Int32/Int64/UInt32 do NOT auto-convert to Float32 (possible precision loss)

    def test_float32_incompatible_arg_int32_rejected(self):
        chdb.create_function("f32_from_i32", lambda x: x + 0.25, arg_types=[FLOAT32], return_type=FLOAT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT f32_from_i32(toInt32(100))", "CSV")
        chdb.drop_function("f32_from_i32")

    def test_float32_incompatible_arg_int64_rejected(self):
        chdb.create_function("f32_from_i64", lambda x: x * 2.0, arg_types=[FLOAT32], return_type=FLOAT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT f32_from_i64(toInt64(7))", "CSV")
        chdb.drop_function("f32_from_i64")

    def test_float32_incompatible_arg_uint32_rejected(self):
        chdb.create_function("f32_from_u32", lambda x: x / 2.0, arg_types=[FLOAT32], return_type=FLOAT32)
        with self.assertRaises(Exception):
            self.session.query("SELECT f32_from_u32(toUInt32(50))", "CSV")
        chdb.drop_function("f32_from_u32")

    # explicit cast workaround: toFloat32(intValue) works

    def test_float32_explicit_cast_int32_to_float32(self):
        chdb.create_function("f32_cast_i32", lambda x: x + 0.25, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_cast_i32(toFloat32(toInt32(100)))", "CSV")
        self.assertEqual(str(ret).strip(), "100.25")
        chdb.drop_function("f32_cast_i32")

    def test_float32_explicit_cast_int64_to_float32(self):
        chdb.create_function("f32_cast_i64", lambda x: x * 2.0, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_cast_i64(toFloat32(toInt64(7)))", "CSV")
        self.assertEqual(str(ret).strip(), "14")
        chdb.drop_function("f32_cast_i64")

    def test_float32_compatible_arg_negative_int8_to_float32(self):
        chdb.create_function("f32_neg_int_arg", lambda x: x + 1.5, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_neg_int_arg(toInt8(-10))", "CSV")
        self.assertEqual(str(ret).strip(), "-8.5")
        chdb.drop_function("f32_neg_int_arg")

    # ── verify Python receives float (not int) when arg_type is Float32 ──

    def test_float32_int_input_received_as_python_float(self):
        results = []
        def capture(x):
            results.append((type(x).__name__, x))
            return x + 0.5
        chdb.create_function("f32_type_chk", capture, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_type_chk(toInt8(42))", "CSV")
        self.assertEqual(results[0][0], "float")
        self.assertEqual(results[0][1], 42.0)
        self.assertEqual(str(ret).strip(), "42.5")
        chdb.drop_function("f32_type_chk")

    def test_float32_uint16_input_received_as_python_float(self):
        results = []
        def capture(x):
            results.append((type(x).__name__, x))
            return x * 2.0
        chdb.create_function("f32_type_chk_u16", capture, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_type_chk_u16(toUInt16(100))", "CSV")
        self.assertEqual(results[0][0], "float")
        self.assertEqual(results[0][1], 100.0)
        self.assertEqual(str(ret).strip(), "200")
        chdb.drop_function("f32_type_chk_u16")

    def test_float32_mixed_int_and_float_args_with_cast(self):
        chdb.create_function("f32_mix", lambda a, b: a + b, arg_types=[FLOAT32, FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_mix(toFloat32(3), toFloat32(0.14))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 3.14, places=2)
        chdb.drop_function("f32_mix")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_float32_udf(self):
        chdb.create_function("f32_to_drop", lambda x: x + 1, arg_types=[FLOAT32], return_type=FLOAT32)
        ret = self.session.query("SELECT f32_to_drop(toFloat32(1.0))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("f32_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT f32_to_drop(toFloat32(1.0))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=FLOAT32)
        def f32_py_callable(x):
            return x + 1.0

        self.assertEqual(f32_py_callable(2.5), 3.5)
        chdb.drop_function("f32_py_callable")


class TestFloat64UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_float64_return_explicit_arg_types(self):
        def half_f64(x):
            return x / 2

        chdb.create_function("f64_half", half_f64, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_half(toFloat64(7.0))", "CSV")
        self.assertEqual(str(ret).strip(), "3.5")
        ret = self.session.query("SELECT f64_half(toFloat64(-6.0))", "CSV")
        self.assertEqual(str(ret).strip(), "-3")
        chdb.drop_function("f64_half")

    def test_create_function_float64_lambda_explicit(self):
        chdb.create_function("f64_double", lambda x: x * 2, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_double(toFloat64(1.25))", "CSV")
        self.assertEqual(str(ret).strip(), "2.5")
        ret = self.session.query("SELECT f64_double(toFloat64(-3.5))", "CSV")
        self.assertEqual(str(ret).strip(), "-7")
        chdb.drop_function("f64_double")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_float64_return_no_arg_types(self):
        def const_e(x):
            return 2.718281828

        chdb.create_function("f64_e", const_e, return_type=FLOAT64)
        ret = self.session.query("SELECT f64_e(toFloat64(0.0))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 2.718281828, places=6)
        chdb.drop_function("f64_e")

    # ── create_function: infer return_type from annotation ──

    def test_create_function_float64_infer_return_from_annotation(self):
        def f64_ret_only(x) -> float:
            return x * 0.5

        chdb.create_function("f64_ret_only", f64_ret_only)
        ret = self.session.query("SELECT f64_ret_only(toFloat64(5.0))", "CSV")
        self.assertEqual(str(ret).strip(), "2.5")
        chdb.drop_function("f64_ret_only")

    # ── create_function: infer both arg_types and return_type from annotations ──

    def test_create_function_float64_infer_all_from_annotations(self):
        def f64_mul(a: float, b: float) -> float:
            return a * b

        chdb.create_function("f64_mul_ann", f64_mul)
        ret = self.session.query("SELECT f64_mul_ann(toFloat64(2.5), toFloat64(4.0))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("f64_mul_ann")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def inc_f64(x: float) -> float:
            return x + 1.0

        chdb.create_function("f64_inc_override", inc_f64, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_inc_override(toFloat64(2.5))", "CSV")
        self.assertEqual(str(ret).strip(), "3.5")
        chdb.drop_function("f64_inc_override")

    # ── create_function: string type names ──

    def test_create_function_float64_string_types(self):
        chdb.create_function("f64_inc_str", lambda x: x + 1, arg_types=["Float64"], return_type="Float64")
        ret = self.session.query("SELECT f64_inc_str(toFloat64(9.25))", "CSV")
        self.assertEqual(str(ret).strip(), "10.25")
        chdb.drop_function("f64_inc_str")

    # ── create_function: float64 as arg_type ──

    def test_create_function_float64_as_arg_type(self):
        def neg_f64(x):
            return -x

        chdb.create_function("f64_neg", neg_f64, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_neg(toFloat64(3.14))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), -3.14, places=10)
        ret = self.session.query("SELECT f64_neg(toFloat64(-1.5))", "CSV")
        self.assertEqual(str(ret).strip(), "1.5")
        chdb.drop_function("f64_neg")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("f64_dummy", dummy, arg_types=[FLOAT64], return_type=FLOAT64)

    # ── create_function: arg type validation at query time ──

    def test_create_function_arg_type_mismatch_at_query(self):
        chdb.create_function("f64_check", lambda x: x, arg_types=[FLOAT64], return_type=FLOAT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT f64_check('hello')", "CSV")
        chdb.drop_function("f64_check")

    # ── create_function: compatible arg type (Float32 → Float64, Int → Float64) ──

    def test_create_function_compatible_arg_type_float32_to_float64(self):
        chdb.create_function("f64_compat_f32", lambda x: x, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_compat_f32(toFloat32(2.5))", "CSV")
        self.assertEqual(str(ret).strip(), "2.5")
        chdb.drop_function("f64_compat_f32")

    def test_create_function_compatible_arg_type_int_to_float64(self):
        chdb.create_function("f64_compat_int", lambda x: x + 0.1, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_compat_int(toInt32(3))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 3.1, places=10)
        chdb.drop_function("f64_compat_int")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_float64_explicit_all(self):
        @func(arg_types=[FLOAT64, FLOAT64], return_type=FLOAT64)
        def dec_f64_avg(a, b):
            return (a + b) / 2

        ret = self.session.query("SELECT dec_f64_avg(toFloat64(3.0), toFloat64(5.0))", "CSV")
        self.assertEqual(str(ret).strip(), "4")
        chdb.drop_function("dec_f64_avg")

    def test_func_decorator_float64_return_only(self):
        @func(return_type=FLOAT64)
        def dec_f64_one(x):
            return 1.0

        ret = self.session.query("SELECT dec_f64_one(toFloat64(999.0))", "CSV")
        self.assertEqual(str(ret).strip(), "1")
        chdb.drop_function("dec_f64_one")

    # ── @func decorator: infer all from annotations ──

    def test_func_decorator_float64_infer_all(self):
        @func()
        def dec_f64_sum(a: float, b: float) -> float:
            return a + b

        ret = self.session.query("SELECT dec_f64_sum(toFloat64(1.1), toFloat64(2.2))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 3.3, places=10)
        chdb.drop_function("dec_f64_sum")

    # ── @func decorator: infer return_type only ──

    def test_func_decorator_float64_infer_return(self):
        @func(arg_types=[FLOAT64, FLOAT64])
        def dec_f64_diff(a, b) -> float:
            return a - b

        ret = self.session.query("SELECT dec_f64_diff(toFloat64(5.5), toFloat64(2.2))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 3.3, places=10)
        chdb.drop_function("dec_f64_diff")

    # ── special float values ──

    def test_float64_zero_and_negative_zero(self):
        chdb.create_function("f64_id", lambda x: x, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_id(toFloat64(0.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), 0.0)
        ret = self.session.query("SELECT f64_id(toFloat64(-0.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), 0.0)
        chdb.drop_function("f64_id")

    def test_float64_very_small_value(self):
        chdb.create_function("f64_tiny", lambda x: x * 2, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_tiny(toFloat64(0.0000001))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 0.0000002, places=10)
        chdb.drop_function("f64_tiny")

    def test_float64_large_value(self):
        chdb.create_function("f64_large", lambda x: x + 1, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_large(toFloat64(1e15))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 1e15 + 1, places=0)
        chdb.drop_function("f64_large")

    def test_float64_return_int_as_float(self):
        chdb.create_function("f64_ret_int", lambda x: 42, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_ret_int(toFloat64(0.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), 42.0)
        chdb.drop_function("f64_ret_int")

    def test_float64_return_computed_int_as_float(self):
        chdb.create_function("f64_ret_comp_int", lambda x: int(x) * 5, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_ret_comp_int(toFloat64(3.9))", "CSV")
        self.assertEqual(float(str(ret).strip()), 15.0)
        chdb.drop_function("f64_ret_comp_int")

    def test_float64_return_negative_int_as_float(self):
        chdb.create_function("f64_ret_neg_int", lambda x: -99, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_ret_neg_int(toFloat64(1.0))", "CSV")
        self.assertEqual(float(str(ret).strip()), -99.0)
        chdb.drop_function("f64_ret_neg_int")

    def test_float64_return_zero_int_as_float(self):
        chdb.create_function("f64_ret_zero_int", lambda x: 0, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_ret_zero_int(toFloat64(5.5))", "CSV")
        self.assertEqual(float(str(ret).strip()), 0.0)
        chdb.drop_function("f64_ret_zero_int")

    def test_float64_return_bool_as_float_rejected(self):
        chdb.create_function("f64_ret_bool_t", lambda x: True, arg_types=[FLOAT64], return_type=FLOAT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT f64_ret_bool_t(toFloat64(0.0))", "CSV")
        chdb.drop_function("f64_ret_bool_t")

    # ── compatible arg type: various int types → Float64 ──
    # Int8/Int16/Int32/UInt8/UInt16/UInt32 auto-convert to Float64 (no precision loss)

    def test_float64_compatible_arg_int8_to_float64(self):
        chdb.create_function("f64_from_i8", lambda x: x + 0.5, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_from_i8(toInt8(5))", "CSV")
        self.assertEqual(str(ret).strip(), "5.5")
        chdb.drop_function("f64_from_i8")

    def test_float64_compatible_arg_int16_to_float64(self):
        chdb.create_function("f64_from_i16", lambda x: x + 0.25, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_from_i16(toInt16(200))", "CSV")
        self.assertEqual(str(ret).strip(), "200.25")
        chdb.drop_function("f64_from_i16")

    def test_float64_compatible_arg_int32_to_float64(self):
        chdb.create_function("f64_from_i32", lambda x: x + 0.125, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_from_i32(toInt32(1000))", "CSV")
        self.assertEqual(str(ret).strip(), "1000.125")
        chdb.drop_function("f64_from_i32")

    def test_float64_compatible_arg_uint8_to_float64(self):
        chdb.create_function("f64_from_u8", lambda x: x + 0.1, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_from_u8(toUInt8(50))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 50.1, places=10)
        chdb.drop_function("f64_from_u8")

    def test_float64_compatible_arg_uint32_to_float64(self):
        chdb.create_function("f64_from_u32", lambda x: x / 4.0, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_from_u32(toUInt32(100))", "CSV")
        self.assertEqual(str(ret).strip(), "25")
        chdb.drop_function("f64_from_u32")

    def test_float64_compatible_arg_negative_int32_to_float64(self):
        chdb.create_function("f64_neg_int_arg", lambda x: x + 0.5, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_neg_int_arg(toInt32(-42))", "CSV")
        self.assertEqual(str(ret).strip(), "-41.5")
        chdb.drop_function("f64_neg_int_arg")

    # ── verify Python receives float (not int) when arg_type is Float64 ──

    def test_float64_int32_input_received_as_python_float(self):
        results = []
        def capture(x):
            results.append((type(x).__name__, x))
            return x + 0.1
        chdb.create_function("f64_type_chk_i32", capture, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_type_chk_i32(toInt32(99))", "CSV")
        self.assertEqual(results[0][0], "float")
        self.assertEqual(results[0][1], 99.0)
        self.assertAlmostEqual(float(str(ret).strip()), 99.1, places=10)
        chdb.drop_function("f64_type_chk_i32")

    def test_float64_int8_input_received_as_python_float(self):
        results = []
        def capture(x):
            results.append((type(x).__name__, x))
            return x * 3.0
        chdb.create_function("f64_type_chk_i8", capture, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_type_chk_i8(toInt8(7))", "CSV")
        self.assertEqual(results[0][0], "float")
        self.assertEqual(results[0][1], 7.0)
        self.assertEqual(str(ret).strip(), "21")
        chdb.drop_function("f64_type_chk_i8")

    def test_float64_uint32_input_received_as_python_float(self):
        results = []
        def capture(x):
            results.append((type(x).__name__, x))
            return x / 4.0
        chdb.create_function("f64_type_chk_u32", capture, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_type_chk_u32(toUInt32(500))", "CSV")
        self.assertEqual(results[0][0], "float")
        self.assertEqual(results[0][1], 500.0)
        self.assertEqual(str(ret).strip(), "125")
        chdb.drop_function("f64_type_chk_u32")

    def test_float64_native_float_still_received_as_python_float(self):
        results = []
        def capture(x):
            results.append((type(x).__name__, x))
            return x
        chdb.create_function("f64_type_chk_f", capture, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_type_chk_f(toFloat64(3.14))", "CSV")
        self.assertEqual(results[0][0], "float")
        self.assertAlmostEqual(results[0][1], 3.14, places=10)
        self.assertAlmostEqual(float(str(ret).strip()), 3.14, places=10)
        chdb.drop_function("f64_type_chk_f")

    # Int64/UInt64 do NOT auto-convert to Float64 (possible precision loss)

    def test_float64_incompatible_arg_int64_rejected(self):
        chdb.create_function("f64_from_i64", lambda x: x * 2.0, arg_types=[FLOAT64], return_type=FLOAT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT f64_from_i64(toInt64(1000))", "CSV")
        chdb.drop_function("f64_from_i64")

    def test_float64_incompatible_arg_uint64_rejected(self):
        chdb.create_function("f64_from_u64", lambda x: x / 3.0, arg_types=[FLOAT64], return_type=FLOAT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT f64_from_u64(toUInt64(90))", "CSV")
        chdb.drop_function("f64_from_u64")

    # explicit cast workaround: toFloat64(intValue) works

    def test_float64_explicit_cast_int64_to_float64(self):
        chdb.create_function("f64_cast_i64", lambda x: x * 2.0, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_cast_i64(toFloat64(toInt64(1000)))", "CSV")
        self.assertEqual(str(ret).strip(), "2000")
        chdb.drop_function("f64_cast_i64")

    def test_float64_mixed_int_and_float_args_with_cast(self):
        chdb.create_function("f64_mix", lambda a, b: a + b, arg_types=[FLOAT64, FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_mix(toFloat64(3), toFloat64(0.14159))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 3.14159, places=5)
        chdb.drop_function("f64_mix")

    def test_float64_precision(self):
        chdb.create_function("f64_prec", lambda x: x, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_prec(toFloat64(1.23456789012345))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 1.23456789012345, places=12)
        chdb.drop_function("f64_prec")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_float64_udf(self):
        chdb.create_function("f64_to_drop", lambda x: x + 1, arg_types=[FLOAT64], return_type=FLOAT64)
        ret = self.session.query("SELECT f64_to_drop(toFloat64(1.0))", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("f64_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT f64_to_drop(toFloat64(1.0))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=FLOAT64)
        def f64_py_callable(x):
            return x + 1.0

        self.assertEqual(f64_py_callable(2.5), 3.5)
        chdb.drop_function("f64_py_callable")


# ═══════════════════════════════════════════════════════════════════
# String Type
# ═══════════════════════════════════════════════════════════════════


class TestStringUDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_string_return_explicit_arg_types(self):
        def upper_str(s):
            return s.upper()

        chdb.create_function("str_upper", upper_str, arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_upper('hello')", "CSV")
        self.assertEqual(str(ret).strip(), '"HELLO"')
        ret = self.session.query("SELECT str_upper('World')", "CSV")
        self.assertEqual(str(ret).strip(), '"WORLD"')
        chdb.drop_function("str_upper")

    def test_create_function_string_lambda_explicit(self):
        chdb.create_function("str_rev", lambda s: s[::-1], arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_rev('abcde')", "CSV")
        self.assertEqual(str(ret).strip(), '"edcba"')
        ret = self.session.query("SELECT str_rev('12345')", "CSV")
        self.assertEqual(str(ret).strip(), '"54321"')
        chdb.drop_function("str_rev")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_string_return_no_arg_types(self):
        def to_greeting(x):
            return "hello"

        chdb.create_function("str_greet", to_greeting, return_type=STRING)
        ret = self.session.query("SELECT str_greet(42)", "CSV")
        self.assertEqual(str(ret).strip(), '"hello"')
        chdb.drop_function("str_greet")

    # ── create_function: infer return_type from annotation ──

    def test_create_function_string_infer_return_from_annotation(self):
        def str_ret_only(s) -> str:
            return s + "!"

        chdb.create_function("str_ret_only", str_ret_only)
        ret = self.session.query("SELECT str_ret_only('hi')", "CSV")
        self.assertEqual(str(ret).strip(), '"hi!"')
        chdb.drop_function("str_ret_only")

    # ── create_function: infer both arg_types and return_type from annotations ──

    def test_create_function_string_infer_all_from_annotations(self):
        def str_lower(s: str) -> str:
            return s.lower()

        chdb.create_function("str_lower_ann", str_lower)
        ret = self.session.query("SELECT str_lower_ann('HELLO')", "CSV")
        self.assertEqual(str(ret).strip(), '"hello"')
        chdb.drop_function("str_lower_ann")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def str_override(s: str) -> str:
            return s + "_suffix"

        chdb.create_function("str_override", str_override, arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_override('test')", "CSV")
        self.assertEqual(str(ret).strip(), '"test_suffix"')
        chdb.drop_function("str_override")

    # ── create_function: string type names ──

    def test_create_function_string_string_types(self):
        chdb.create_function("str_len_s", lambda s: str(len(s)), arg_types=["String"], return_type="String")
        ret = self.session.query("SELECT str_len_s('hello')", "CSV")
        self.assertEqual(str(ret).strip(), '"5"')
        chdb.drop_function("str_len_s")

    # ── create_function: string as arg_type ──

    def test_create_function_string_as_arg_type(self):
        def repeat_str(s):
            return s + s

        chdb.create_function("str_repeat", repeat_str, arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_repeat('ab')", "CSV")
        self.assertEqual(str(ret).strip(), '"abab"')
        chdb.drop_function("str_repeat")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("str_dummy", dummy, arg_types=[STRING], return_type=STRING)

    # ── create_function: multiple string args ──

    def test_create_function_multi_string_args(self):
        def concat_three(a, b, c):
            return a + b + c

        chdb.create_function("str_cat3", concat_three, arg_types=[STRING, STRING, STRING], return_type=STRING)
        ret = self.session.query("SELECT str_cat3('a', 'b', 'c')", "CSV")
        self.assertEqual(str(ret).strip(), '"abc"')
        chdb.drop_function("str_cat3")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_string_explicit_all(self):
        @func(arg_types=[STRING, STRING], return_type=STRING)
        def dec_str_concat(a, b):
            return a + b

        ret = self.session.query("SELECT dec_str_concat('hello', ' world')", "CSV")
        self.assertEqual(str(ret).strip(), '"hello world"')
        chdb.drop_function("dec_str_concat")

    def test_func_decorator_string_return_only(self):
        @func(return_type=STRING)
        def dec_str_const(x):
            return "constant"

        ret = self.session.query("SELECT dec_str_const('anything')", "CSV")
        self.assertEqual(str(ret).strip(), '"constant"')
        chdb.drop_function("dec_str_const")

    # ── @func decorator: infer all from annotations ──

    def test_func_decorator_string_infer_all(self):
        @func()
        def dec_str_strip(s: str) -> str:
            return s.strip()

        ret = self.session.query("SELECT dec_str_strip('  hello  ')", "CSV")
        self.assertEqual(str(ret).strip(), '"hello"')
        chdb.drop_function("dec_str_strip")

    # ── @func decorator: infer return_type only ──

    def test_func_decorator_string_infer_return(self):
        @func(arg_types=[STRING])
        def dec_str_title(s) -> str:
            return s.title()

        ret = self.session.query("SELECT dec_str_title('hello world')", "CSV")
        self.assertEqual(str(ret).strip(), '"Hello World"')
        chdb.drop_function("dec_str_title")

    # ── special string cases ──

    def test_string_empty(self):
        chdb.create_function("str_is_empty", lambda s: str(len(s) == 0), arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_is_empty('')", "CSV")
        self.assertEqual(str(ret).strip(), '"True"')
        ret = self.session.query("SELECT str_is_empty('x')", "CSV")
        self.assertEqual(str(ret).strip(), '"False"')
        chdb.drop_function("str_is_empty")

    def test_string_with_spaces(self):
        chdb.create_function("str_trim", lambda s: s.strip(), arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_trim('  hello  ')", "CSV")
        self.assertEqual(str(ret).strip(), '"hello"')
        chdb.drop_function("str_trim")

    def test_string_unicode(self):
        chdb.create_function("str_uni_len", lambda s: str(len(s)), arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_uni_len('中文测试')", "CSV")
        self.assertEqual(str(ret).strip(), '"4"')
        chdb.drop_function("str_uni_len")

    def test_string_with_special_chars(self):
        chdb.create_function("str_has_at", lambda s: str('@' in s), arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_has_at('user@example.com')", "CSV")
        self.assertEqual(str(ret).strip(), '"True"')
        ret = self.session.query("SELECT str_has_at('no-at-sign')", "CSV")
        self.assertEqual(str(ret).strip(), '"False"')
        chdb.drop_function("str_has_at")

    def test_string_return_none_becomes_null(self):
        chdb.create_function("str_none", lambda s: None, arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_none('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("str_none")

    def test_string_return_int_to_string(self):
        chdb.create_function("str_from_int", lambda x: str(x), return_type=STRING)
        ret = self.session.query("SELECT str_from_int(42)", "CSV")
        self.assertEqual(str(ret).strip(), '"42"')
        chdb.drop_function("str_from_int")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_string_udf(self):
        chdb.create_function("str_to_drop", lambda s: s.upper(), arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT str_to_drop('test')", "CSV")
        self.assertEqual(str(ret).strip(), '"TEST"')
        chdb.drop_function("str_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT str_to_drop('test')", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=STRING)
        def str_py_callable(s):
            return s.upper()

        self.assertEqual(str_py_callable("hello"), "HELLO")
        chdb.drop_function("str_py_callable")


# ═══════════════════════════════════════════════════════════════════
# Date Types
# ═══════════════════════════════════════════════════════════════════


class TestDateUDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_date_return_explicit_arg_types(self):
        def add_day(d):
            return d + datetime.timedelta(days=1)

        chdb.create_function("date_add_day", add_day, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_add_day(toDate('2024-01-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16"')
        ret = self.session.query("SELECT date_add_day(toDate('2024-02-28'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-02-29"')
        chdb.drop_function("date_add_day")

    def test_create_function_date_lambda_explicit(self):
        chdb.create_function("date_id", lambda d: d, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_id(toDate('2024-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-06-15"')
        chdb.drop_function("date_id")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_date_return_no_arg_types(self):
        def const_date(x):
            return datetime.date(2024, 1, 1)

        chdb.create_function("date_const", const_date, return_type=DATE)
        ret = self.session.query("SELECT date_const(42)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-01"')
        chdb.drop_function("date_const")

    # ── create_function: infer return_type from annotation ──

    def test_create_function_date_infer_return_from_annotation(self):
        def date_ret_only(d) -> datetime.date:
            return d

        chdb.create_function("date_ret_only", date_ret_only)
        ret = self.session.query("SELECT date_ret_only(toDate('2024-03-20'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-03-20"')
        chdb.drop_function("date_ret_only")

    # ── create_function: infer both arg_types and return_type from annotations ──

    def test_create_function_date_infer_all_from_annotations(self):
        def date_infer(d: datetime.date) -> datetime.date:
            return d + datetime.timedelta(days=10)

        chdb.create_function("date_infer_all", date_infer)
        ret = self.session.query("SELECT date_infer_all(toDate('2024-01-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-25"')
        chdb.drop_function("date_infer_all")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def date_override(d: datetime.date) -> datetime.date:
            return d

        chdb.create_function("date_override", date_override, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_override(toDate('2024-07-04'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-04"')
        chdb.drop_function("date_override")

    # ── create_function: string type names ──

    def test_create_function_date_string_types(self):
        chdb.create_function("date_str_id", lambda d: d, arg_types=["Date"], return_type="Date")
        ret = self.session.query("SELECT date_str_id(toDate('2024-12-25'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-12-25"')
        chdb.drop_function("date_str_id")

    # ── create_function: date as arg_type ──

    def test_create_function_date_as_arg_type(self):
        def add_week(d):
            return d + datetime.timedelta(weeks=1)

        chdb.create_function("date_add_wk", add_week, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_add_wk(toDate('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-08"')
        chdb.drop_function("date_add_wk")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a

        with self.assertRaises(RuntimeError):
            chdb.create_function("date_dummy", dummy, arg_types=[DATE], return_type=DATE)

    # ── extract date components as integers ──

    def test_extract_year(self):
        chdb.create_function("date_year", lambda d: d.year, arg_types=[DATE], return_type=INT32)
        ret = self.session.query("SELECT date_year(toDate('2024-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), "2024")
        chdb.drop_function("date_year")

    def test_extract_month(self):
        chdb.create_function("date_month", lambda d: d.month, arg_types=[DATE], return_type=INT32)
        ret = self.session.query("SELECT date_month(toDate('2024-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        chdb.drop_function("date_month")

    def test_extract_day(self):
        chdb.create_function("date_day", lambda d: d.day, arg_types=[DATE], return_type=INT32)
        ret = self.session.query("SELECT date_day(toDate('2024-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), "15")
        chdb.drop_function("date_day")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_date_explicit_all(self):
        @func(arg_types=[DATE], return_type=DATE)
        def dec_date_add_week(d):
            return d + datetime.timedelta(weeks=1)

        ret = self.session.query("SELECT dec_date_add_week(toDate('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-08"')
        chdb.drop_function("dec_date_add_week")

    def test_func_decorator_date_return_only(self):
        @func(return_type=DATE)
        def dec_date_epoch(x):
            return datetime.date(1970, 1, 1)

        ret = self.session.query("SELECT dec_date_epoch(42)", "CSV")
        self.assertEqual(str(ret).strip(), '"1970-01-01"')
        chdb.drop_function("dec_date_epoch")

    # ── @func decorator: infer all from annotations ──

    def test_func_decorator_date_infer_all(self):
        @func()
        def dec_date_id(d: datetime.date) -> datetime.date:
            return d

        ret = self.session.query("SELECT dec_date_id(toDate('2024-09-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-09-01"')
        chdb.drop_function("dec_date_id")

    # ── special date cases ──

    def test_date_leap_year(self):
        chdb.create_function("date_leap", lambda d: d + datetime.timedelta(days=1), arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_leap(toDate('2024-02-28'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-02-29"')
        ret = self.session.query("SELECT date_leap(toDate('2023-02-28'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2023-03-01"')
        chdb.drop_function("date_leap")

    def test_date_year_boundary(self):
        chdb.create_function("date_next", lambda d: d + datetime.timedelta(days=1), arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_next(toDate('2024-12-31'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2025-01-01"')
        chdb.drop_function("date_next")

    def test_date_weekday(self):
        chdb.create_function("date_wd", lambda d: d.weekday(), arg_types=[DATE], return_type=INT32)
        ret = self.session.query("SELECT date_wd(toDate('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("date_wd")

    def test_date_isoformat(self):
        chdb.create_function("date_iso", lambda d: d.isoformat(), arg_types=[DATE], return_type=STRING)
        ret = self.session.query("SELECT date_iso(toDate('2024-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-06-15"')
        chdb.drop_function("date_iso")

    # ── Date boundary: return value out of range (UInt16 days, 1970-01-01 ~ 2149-06-06) ──

    def test_return_date_before_epoch_wraps(self):
        chdb.create_function("date_pre_epoch", lambda d: datetime.date(1969, 12, 31),
                             arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_pre_epoch(toDate('2024-01-01'))", "CSV")
        self.assertIsNotNone(ret)
        chdb.drop_function("date_pre_epoch")

    def test_return_date_far_future_wraps(self):
        chdb.create_function("date_far_future", lambda d: datetime.date(2200, 1, 1),
                             arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_far_future(toDate('2024-01-01'))", "CSV")
        self.assertIsNotNone(ret)
        chdb.drop_function("date_far_future")

    def test_date_max_boundary(self):
        chdb.create_function("date_max", lambda d: d, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_max(toDate('2149-06-06'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2149-06-06"')
        chdb.drop_function("date_max")

    def test_date_min_boundary(self):
        chdb.create_function("date_min", lambda d: d, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_min(toDate('1970-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"1970-01-01"')
        chdb.drop_function("date_min")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_date_udf(self):
        chdb.create_function("date_to_drop", lambda d: d, arg_types=[DATE], return_type=DATE)
        ret = self.session.query("SELECT date_to_drop(toDate('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-01"')
        chdb.drop_function("date_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT date_to_drop(toDate('2024-01-01'))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=DATE)
        def date_py_callable(d):
            return d

        result = date_py_callable(datetime.date(2024, 1, 1))
        self.assertEqual(result, datetime.date(2024, 1, 1))
        chdb.drop_function("date_py_callable")

class TestDate32UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_date32_return_explicit_arg_types(self):
        def add_day32(d):
            return d + datetime.timedelta(days=1)

        chdb.create_function("d32_add_day", add_day32, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_add_day(toDate32('2024-01-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16"')
        ret = self.session.query("SELECT d32_add_day(toDate32('2024-02-28'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-02-29"')
        chdb.drop_function("d32_add_day")

    def test_create_function_date32_lambda_explicit(self):
        chdb.create_function("d32_id", lambda d: d, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_id(toDate32('2024-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-06-15"')
        chdb.drop_function("d32_id")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_date32_return_no_arg_types(self):
        def const_d32(x):
            return datetime.date(2024, 1, 1)

        chdb.create_function("d32_const", const_d32, return_type=DATE32)
        ret = self.session.query("SELECT d32_const(42)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-01"')
        chdb.drop_function("d32_const")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def d32_override(d: datetime.date) -> datetime.date:
            return d

        chdb.create_function("d32_override", d32_override, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_override(toDate32('2024-07-04'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-04"')
        chdb.drop_function("d32_override")

    # ── create_function: string type names ──

    def test_create_function_date32_string_types(self):
        chdb.create_function("d32_str_id", lambda d: d, arg_types=["Date32"], return_type="Date32")
        ret = self.session.query("SELECT d32_str_id(toDate32('2024-12-25'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-12-25"')
        chdb.drop_function("d32_str_id")

    # ── create_function: date32 as arg_type ──

    def test_create_function_date32_as_arg_type(self):
        def sub_day(d):
            return d - datetime.timedelta(days=1)

        chdb.create_function("d32_sub_day", sub_day, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_sub_day(toDate32('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2023-12-31"')
        chdb.drop_function("d32_sub_day")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a

        with self.assertRaises(RuntimeError):
            chdb.create_function("d32_dummy", dummy, arg_types=[DATE32], return_type=DATE32)

    # ── compatible arg type: Date → Date32 ──

    def test_create_function_compatible_arg_type_date_to_date32(self):
        chdb.create_function("d32_compat", lambda d: d, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_compat(toDate('2024-03-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-03-01"')
        chdb.drop_function("d32_compat")

    # ── extract date components ──

    def test_extract_year(self):
        chdb.create_function("d32_year", lambda d: d.year, arg_types=[DATE32], return_type=INT32)
        ret = self.session.query("SELECT d32_year(toDate32('1900-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), "1900")
        chdb.drop_function("d32_year")

    def test_extract_month_and_day(self):
        chdb.create_function("d32_md", lambda d: d.month * 100 + d.day, arg_types=[DATE32], return_type=INT32)
        ret = self.session.query("SELECT d32_md(toDate32('2024-11-23'))", "CSV")
        self.assertEqual(str(ret).strip(), "1123")
        chdb.drop_function("d32_md")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_date32_explicit_all(self):
        @func(arg_types=[DATE32], return_type=DATE32)
        def dec_d32_add_wk(d):
            return d + datetime.timedelta(weeks=1)

        ret = self.session.query("SELECT dec_d32_add_wk(toDate32('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-08"')
        chdb.drop_function("dec_d32_add_wk")

    def test_func_decorator_date32_return_only(self):
        @func(return_type=DATE32)
        def dec_d32_epoch(x):
            return datetime.date(1970, 1, 1)

        ret = self.session.query("SELECT dec_d32_epoch(42)", "CSV")
        self.assertEqual(str(ret).strip(), '"1970-01-01"')
        chdb.drop_function("dec_d32_epoch")

    # ── special date32 cases ──

    def test_date32_far_past(self):
        chdb.create_function("d32_past", lambda d: d, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_past(toDate32('1925-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"1925-01-01"')
        chdb.drop_function("d32_past")

    def test_date32_far_future(self):
        chdb.create_function("d32_future", lambda d: d, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_future(toDate32('2283-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2283-01-01"')
        chdb.drop_function("d32_future")

    def test_date32_isoformat(self):
        chdb.create_function("d32_iso", lambda d: d.isoformat(), arg_types=[DATE32], return_type=STRING)
        ret = self.session.query("SELECT d32_iso(toDate32('1950-06-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"1950-06-15"')
        chdb.drop_function("d32_iso")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_date32_udf(self):
        chdb.create_function("d32_to_drop", lambda d: d, arg_types=[DATE32], return_type=DATE32)
        ret = self.session.query("SELECT d32_to_drop(toDate32('2024-01-01'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-01"')
        chdb.drop_function("d32_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT d32_to_drop(toDate32('2024-01-01'))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=DATE32)
        def d32_py_callable(d):
            return d

        result = d32_py_callable(datetime.date(2024, 1, 1))
        self.assertEqual(result, datetime.date(2024, 1, 1))
        chdb.drop_function("d32_py_callable")


# ═══════════════════════════════════════════════════════════════════
# DateTime Types
# ═══════════════════════════════════════════════════════════════════

class TestDateTimeUDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_datetime_return_explicit_arg_types(self):
        def dt_identity(d):
            return d

        chdb.create_function("dt_id", dt_identity, arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_id(toDateTime('2024-01-15 10:30:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:00"')
        chdb.drop_function("dt_id")

    def test_create_function_datetime_lambda_explicit(self):
        chdb.create_function("dt_id2", lambda d: d, arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_id2(toDateTime('2024-06-15 08:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-06-15 08:00:00"')
        chdb.drop_function("dt_id2")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_datetime_return_no_arg_types(self):
        def const_dt(x):
            return datetime.datetime(2024, 1, 1, 0, 0, 0,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt_const", const_dt, return_type=DATETIME)
        ret = self.session.query("SELECT dt_const(42)", "CSV")
        self.assertIn("2024-01-01", str(ret).strip())
        chdb.drop_function("dt_const")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def dt_override(d: datetime.datetime) -> datetime.datetime:
            return d

        chdb.create_function("dt_override", dt_override, arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_override(toDateTime('2024-07-04 12:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-04 12:00:00"')
        chdb.drop_function("dt_override")

    # ── create_function: string type names ──

    def test_create_function_datetime_string_types(self):
        chdb.create_function("dt_str_id", lambda d: d, arg_types=["DateTime"], return_type="DateTime")
        ret = self.session.query("SELECT dt_str_id(toDateTime('2024-12-25 18:30:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-12-25 18:30:00"')
        chdb.drop_function("dt_str_id")

    # ── create_function: datetime as arg_type ──

    def test_create_function_datetime_as_arg_type(self):
        def add_hour(d):
            return d + datetime.timedelta(hours=1)

        chdb.create_function("dt_add_hr", add_hour, arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_add_hr(toDateTime('2024-01-15 23:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:00:00"')
        chdb.drop_function("dt_add_hr")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a

        with self.assertRaises(RuntimeError):
            chdb.create_function("dt_dummy", dummy, arg_types=[DATETIME], return_type=DATETIME)

    # ── extract datetime components ──

    def test_extract_hour(self):
        chdb.create_function("dt_hour", lambda d: d.hour, arg_types=[DATETIME], return_type=INT32)
        ret = self.session.query("SELECT dt_hour(toDateTime('2024-01-15 14:30:00'))", "CSV")
        self.assertEqual(str(ret).strip(), "14")
        chdb.drop_function("dt_hour")

    def test_extract_minute(self):
        chdb.create_function("dt_minute", lambda d: d.minute, arg_types=[DATETIME], return_type=INT32)
        ret = self.session.query("SELECT dt_minute(toDateTime('2024-01-15 10:45:00'))", "CSV")
        self.assertEqual(str(ret).strip(), "45")
        chdb.drop_function("dt_minute")

    def test_extract_second(self):
        chdb.create_function("dt_second", lambda d: d.second, arg_types=[DATETIME], return_type=INT32)
        ret = self.session.query("SELECT dt_second(toDateTime('2024-01-15 10:30:59'))", "CSV")
        self.assertEqual(str(ret).strip(), "59")
        chdb.drop_function("dt_second")

    def test_extract_date_from_datetime(self):
        chdb.create_function("dt_date_part", lambda d: d.date(), arg_types=[DATETIME], return_type=DATE)
        ret = self.session.query("SELECT dt_date_part(toDateTime('2024-01-15 14:30:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15"')
        chdb.drop_function("dt_date_part")

    # ── @func decorator: explicit return_type + explicit arg_types ──

    def test_func_decorator_datetime_explicit_all(self):
        @func(arg_types=[DATETIME], return_type=DATETIME)
        def dec_dt_add_day(d):
            return d + datetime.timedelta(days=1)

        ret = self.session.query("SELECT dec_dt_add_day(toDateTime('2024-01-15 10:30:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 10:30:00"')
        chdb.drop_function("dec_dt_add_day")

    def test_func_decorator_datetime_return_only(self):
        @func(return_type=DATETIME)
        def dec_dt_const(x):
            return datetime.datetime(2024, 6, 1, 12, 0, 0,
                                     tzinfo=datetime.timezone.utc)

        ret = self.session.query("SELECT dec_dt_const(42)", "CSV")
        self.assertIn("2024-06-01", str(ret).strip())
        chdb.drop_function("dec_dt_const")

    # ── special datetime cases ──

    def test_datetime_add_timedelta(self):
        chdb.create_function("dt_add_30m", lambda d: d + datetime.timedelta(minutes=30),
                             arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_add_30m(toDateTime('2024-01-15 23:45:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:15:00"')
        chdb.drop_function("dt_add_30m")

    def test_datetime_midnight(self):
        chdb.create_function("dt_midnight", lambda d: d, arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_midnight(toDateTime('2024-01-15 00:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 00:00:00"')
        chdb.drop_function("dt_midnight")

    def test_datetime_to_string(self):
        chdb.create_function("dt_to_str", lambda d: d.strftime("%Y/%m/%d %H:%M"),
                             arg_types=[DATETIME], return_type=STRING)
        ret = self.session.query("SELECT dt_to_str(toDateTime('2024-01-15 14:30:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024/01/15 14:30"')
        chdb.drop_function("dt_to_str")

    def test_datetime_has_timezone(self):
        chdb.create_function("dt_has_tz", lambda d: d.tzinfo is not None,
                             arg_types=[DATETIME], return_type=BOOL)
        ret = self.session.query("SELECT dt_has_tz(toDateTime('2024-01-15 10:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        chdb.drop_function("dt_has_tz")

    # ── string type with timezone ──

    def test_datetime_string_type_utc(self):
        chdb.create_function("dt_utc_id", lambda d: d,
                             arg_types=["DateTime('UTC')"], return_type="DateTime('UTC')")
        ret = self.session.query(
            "SELECT dt_utc_id(toDateTime('2024-01-15 10:30:00', 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:00"')
        chdb.drop_function("dt_utc_id")

    def test_datetime_string_type_shanghai(self):
        chdb.create_function("dt_sh_id", lambda d: d,
                             arg_types=["DateTime('Asia/Shanghai')"],
                             return_type="DateTime('Asia/Shanghai')")
        ret = self.session.query(
            "SELECT dt_sh_id(toDateTime('2024-01-15 18:30:00', 'Asia/Shanghai'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 18:30:00"')
        chdb.drop_function("dt_sh_id")

    def test_datetime_string_type_new_york(self):
        chdb.create_function("dt_ny_id", lambda d: d,
                             arg_types=["DateTime('America/New_York')"],
                             return_type="DateTime('America/New_York')")
        ret = self.session.query(
            "SELECT dt_ny_id(toDateTime('2024-07-15 14:30:00', 'America/New_York'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-15 14:30:00"')
        chdb.drop_function("dt_ny_id")

    def test_datetime_roundtrip_utc_timezone_preserves_time(self):
        """UTC datetime round-trip: the displayed time must not shift."""
        def add_one_hour(d):
            return d + datetime.timedelta(hours=1)

        chdb.create_function("dt_utc_add1h", add_one_hour,
                             arg_types=["DateTime('UTC')"],
                             return_type="DateTime('UTC')")
        ret = self.session.query(
            "SELECT dt_utc_add1h(toDateTime('2024-01-15 23:00:00', 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:00:00"')
        chdb.drop_function("dt_utc_add1h")

    def test_datetime_roundtrip_shanghai_timezone_preserves_time(self):
        """Shanghai datetime round-trip: the displayed time must not shift."""
        def add_one_hour(d):
            return d + datetime.timedelta(hours=1)

        chdb.create_function("dt_sh_add1h", add_one_hour,
                             arg_types=["DateTime('Asia/Shanghai')"],
                             return_type="DateTime('Asia/Shanghai')")
        ret = self.session.query(
            "SELECT dt_sh_add1h(toDateTime('2024-01-15 23:00:00', 'Asia/Shanghai'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:00:00"')
        chdb.drop_function("dt_sh_add1h")

    def test_datetime_udf_receives_correct_timezone_info(self):
        """Verify the Python datetime object received by UDF carries the correct tzinfo."""
        def get_tz_name(d):
            return str(d.tzinfo)

        chdb.create_function("dt_tz_name", get_tz_name,
                             arg_types=["DateTime('Asia/Shanghai')"],
                             return_type=STRING)
        ret = self.session.query(
            "SELECT dt_tz_name(toDateTime('2024-06-15 12:00:00', 'Asia/Shanghai'))", "CSV")
        self.assertIn("Asia/Shanghai", str(ret).strip())
        chdb.drop_function("dt_tz_name")

    def test_datetime_udf_receives_utc_timezone_info(self):
        def get_tz_name(d):
            return str(d.tzinfo)

        chdb.create_function("dt_tz_name_utc", get_tz_name,
                             arg_types=["DateTime('UTC')"],
                             return_type=STRING)
        ret = self.session.query(
            "SELECT dt_tz_name_utc(toDateTime('2024-06-15 12:00:00', 'UTC'))", "CSV")
        self.assertIn("UTC", str(ret).strip())
        chdb.drop_function("dt_tz_name_utc")

    def test_datetime_cross_timezone_same_epoch(self):
        """The same absolute moment stored in different timezones must produce the same epoch."""
        results = {}
        for tz, display_time, func_name in [
            ("UTC", "2024-01-15 00:00:00", "dt_epoch_utc"),
            ("Asia/Shanghai", "2024-01-15 08:00:00", "dt_epoch_sh"),
            ("America/New_York", "2024-01-14 19:00:00", "dt_epoch_ny"),
        ]:
            chdb.create_function(
                func_name, lambda d: int(d.timestamp()),
                arg_types=[f"DateTime('{tz}')"], return_type=INT64)
            ret = self.session.query(
                f"SELECT {func_name}(toDateTime('{display_time}', '{tz}'))", "CSV")
            results[tz] = int(str(ret).strip())
            chdb.drop_function(func_name)

        self.assertEqual(results["UTC"], results["Asia/Shanghai"],
                         "UTC and Shanghai should represent the same epoch")
        self.assertEqual(results["UTC"], results["America/New_York"],
                         "UTC and New York should represent the same epoch")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_datetime_udf(self):
        chdb.create_function("dt_to_drop", lambda d: d, arg_types=[DATETIME], return_type=DATETIME)
        ret = self.session.query("SELECT dt_to_drop(toDateTime('2024-01-01 00:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-01 00:00:00"')
        chdb.drop_function("dt_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT dt_to_drop(toDateTime('2024-01-01 00:00:00'))", "CSV")

    # ── timezone mismatch: UDF returns datetime in different tz than return_type ──

    def test_udf_returns_utc_datetime_but_return_type_is_shanghai(self):
        """UDF returns a UTC aware datetime, but return_type is DateTime('Asia/Shanghai').
        ClickHouse stores the UTC epoch; display converts to Shanghai (+8h)."""
        def make_utc(x):
            return datetime.datetime(2024, 1, 15, 0, 0, 0,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt_tz_mis1", make_utc,
                             return_type="DateTime('Asia/Shanghai')")
        ret = self.session.query("SELECT dt_tz_mis1(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 08:00:00"')
        chdb.drop_function("dt_tz_mis1")

    def test_udf_returns_shanghai_datetime_but_return_type_is_utc(self):
        """UDF returns a Shanghai aware datetime, but return_type is DateTime('UTC').
        ClickHouse stores the UTC epoch; display shows UTC time."""
        def make_shanghai(x):
            tz_sh = datetime.timezone(datetime.timedelta(hours=8))
            return datetime.datetime(2024, 1, 15, 8, 0, 0, tzinfo=tz_sh)

        chdb.create_function("dt_tz_mis2", make_shanghai,
                             return_type="DateTime('UTC')")
        ret = self.session.query("SELECT dt_tz_mis2(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 00:00:00"')
        chdb.drop_function("dt_tz_mis2")

    def test_udf_returns_ny_datetime_but_return_type_is_shanghai(self):
        """UDF returns New York time, return_type is Shanghai. Display should shift accordingly."""
        def make_ny(x):
            tz_ny = datetime.timezone(datetime.timedelta(hours=-5))
            return datetime.datetime(2024, 7, 15, 10, 0, 0, tzinfo=tz_ny)

        chdb.create_function("dt_tz_mis3", make_ny,
                             return_type="DateTime('Asia/Shanghai')")
        ret = self.session.query("SELECT dt_tz_mis3(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-15 23:00:00"')
        chdb.drop_function("dt_tz_mis3")

    # ── timezone mismatch: ClickHouse input tz differs from arg_type tz ──
    #
    # ClickHouse preserves the INPUT's original timezone when passing to UDF,
    # regardless of the arg_type's declared timezone.

    def test_input_utc_value_but_arg_type_is_shanghai(self):
        def get_hour(d):
            return d.hour

        chdb.create_function("dt_in_mis1", get_hour,
                             arg_types=["DateTime('Asia/Shanghai')"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt_in_mis1(toDateTime('2024-01-15 00:00:00', 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), "0",
                         "UDF should see hour=0 (UTC), not 8 (Shanghai)")
        chdb.drop_function("dt_in_mis1")

    def test_input_shanghai_value_but_arg_type_is_utc(self):
        def get_hour(d):
            return d.hour

        chdb.create_function("dt_in_mis2", get_hour,
                             arg_types=["DateTime('UTC')"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt_in_mis2(toDateTime('2024-01-15 08:00:00', 'Asia/Shanghai'))", "CSV")
        self.assertEqual(str(ret).strip(), "8",
                         "UDF should see hour=8 (Shanghai), not 0 (UTC)")
        chdb.drop_function("dt_in_mis2")

    def test_input_tz_mismatch_same_epoch_verification(self):
        """Both inputs represent the same absolute moment but in different tz.
        Despite the arg_type tz mismatch, both should yield the same epoch."""
        chdb.create_function("dt_epoch_a", lambda d: int(d.timestamp()),
                             arg_types=["DateTime('Asia/Shanghai')"], return_type=INT64)
        chdb.create_function("dt_epoch_b", lambda d: int(d.timestamp()),
                             arg_types=["DateTime('UTC')"], return_type=INT64)

        ret_a = self.session.query(
            "SELECT dt_epoch_a(toDateTime('2024-01-15 00:00:00', 'UTC'))", "CSV")
        ret_b = self.session.query(
            "SELECT dt_epoch_b(toDateTime('2024-01-15 08:00:00', 'Asia/Shanghai'))", "CSV")

        self.assertEqual(int(str(ret_a).strip()), int(str(ret_b).strip()),
                         "Same absolute moment should yield same epoch regardless of arg_type tz")

        chdb.drop_function("dt_epoch_a")
        chdb.drop_function("dt_epoch_b")

    def test_full_roundtrip_cross_tz_datetime(self):
        """Full round-trip: input UTC, UDF adds 1h, output as Shanghai.
        UTC 23:00 + 1h = UTC 00:00 next day = Shanghai 08:00 next day."""
        def add_one_hour(d):
            return d + datetime.timedelta(hours=1)

        chdb.create_function("dt_full_rt", add_one_hour,
                             arg_types=["DateTime('UTC')"],
                             return_type="DateTime('Asia/Shanghai')")
        ret = self.session.query(
            "SELECT dt_full_rt(toDateTime('2024-01-15 23:00:00', 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 08:00:00"')
        chdb.drop_function("dt_full_rt")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=DATETIME)
        def dt_py_callable(d):
            return d

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        result = dt_py_callable(now)
        self.assertEqual(result, now)
        chdb.drop_function("dt_py_callable")


# ═══════════════════════════════════════════════════════════════════
# DateTime64
# ═══════════════════════════════════════════════════════════════════

class TestDateTime64UDF(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── create_function: explicit return_type + explicit arg_types ──

    def test_create_function_datetime64_return_explicit_arg_types(self):
        def dt64_identity(d):
            return d

        chdb.create_function("dt64_id", dt64_identity, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_id(toDateTime64('2024-01-15 10:30:00.123456', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:00.123456"')
        chdb.drop_function("dt64_id")

    def test_create_function_datetime64_lambda_explicit(self):
        chdb.create_function("dt64_id2", lambda d: d, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_id2(toDateTime64('2024-06-15 08:00:00.456789', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-06-15 08:00:00.456789"')
        chdb.drop_function("dt64_id2")

    # ── create_function: return_type only, no arg_types ──

    def test_create_function_datetime64_return_no_arg_types(self):
        def const_dt64(x):
            return datetime.datetime(2024, 1, 1, 0, 0, 0, 123000,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_const", const_dt64, return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_const(42)", "CSV")
        self.assertIn("2024-01-01", str(ret).strip())
        chdb.drop_function("dt64_const")

    # ── create_function: infer return_type from annotation ──

    def test_create_function_datetime64_infer_return_from_annotation(self):
        def dt64_ret_only(d) -> datetime.datetime:
            return d

        chdb.create_function("dt64_ret_only", dt64_ret_only)
        ret = self.session.query("SELECT dt64_ret_only(toDateTime64('2024-03-20 12:00:00.500', 3))", "CSV")
        self.assertIn("2024-03-20", str(ret).strip())
        self.assertIn("12:00:00", str(ret).strip())
        chdb.drop_function("dt64_ret_only")

    # ── create_function: infer both arg_types and return_type from annotations ──

    def test_create_function_datetime64_infer_all_from_annotations(self):
        def dt64_infer(d: datetime.datetime) -> datetime.datetime:
            return d

        chdb.create_function("dt64_infer_all", dt64_infer)
        ret = self.session.query("SELECT dt64_infer_all(toDateTime64('2024-01-15 10:30:00.123', 3))", "CSV")
        self.assertIn("2024-01-15", str(ret).strip())
        self.assertIn("10:30:00", str(ret).strip())
        chdb.drop_function("dt64_infer_all")

    # ── annotation infers DateTime64(6): microsecond precision preserved ──

    def test_datetime64_annotation_infers_scale6_microsecond_preserved(self):
        """datetime.datetime annotation should infer DateTime64(6), preserving microseconds."""
        def dt64_us(d: datetime.datetime) -> datetime.datetime:
            return d

        chdb.create_function("dt64_us", dt64_us)
        ret = self.session.query(
            "SELECT dt64_us(toDateTime64('2024-07-04 12:00:00.789012', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-04 12:00:00.789012"')
        chdb.drop_function("dt64_us")

    def test_datetime64_annotation_infers_scale6_construct_with_microseconds(self):
        """UDF constructs datetime with microseconds; annotation-inferred scale=6 keeps them."""
        def dt64_mk_us() -> datetime.datetime:
            return datetime.datetime(2024, 1, 15, 12, 0, 0, 123456,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_mk_us", dt64_mk_us)
        ret = self.session.query("SELECT dt64_mk_us()", "CSV")
        self.assertIn(".123456", str(ret).strip())
        chdb.drop_function("dt64_mk_us")

    def test_datetime64_annotation_infers_scale6_decorator(self):
        """@func() with datetime annotations should also use scale=6."""
        @func()
        def dec_dt64_us(d: datetime.datetime) -> datetime.datetime:
            return d

        ret = self.session.query(
            "SELECT dec_dt64_us(toDateTime64('2024-09-01 08:15:30.456789', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-09-01 08:15:30.456789"')
        chdb.drop_function("dec_dt64_us")

    # ── create_function: explicit arg_types override annotations ──

    def test_create_function_explicit_arg_types_override_annotations(self):
        def dt64_override(d: datetime.datetime) -> datetime.datetime:
            return d

        chdb.create_function("dt64_override", dt64_override, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_override(toDateTime64('2024-07-04 12:00:00.789012', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-04 12:00:00.789012"')
        chdb.drop_function("dt64_override")

    # ── create_function: string type names ──

    def test_create_function_datetime64_string_types(self):
        chdb.create_function("dt64_str_id", lambda d: d,
                             arg_types=["DateTime64(6)"], return_type="DateTime64(6)")
        ret = self.session.query("SELECT dt64_str_id(toDateTime64('2024-12-25 18:30:00.999999', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-12-25 18:30:00.999999"')
        chdb.drop_function("dt64_str_id")

    # ── create_function: datetime64 as arg_type ──

    def test_create_function_datetime64_as_arg_type(self):
        def add_half_sec(d):
            return d + datetime.timedelta(milliseconds=500)

        chdb.create_function("dt64_add_half", add_half_sec, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_add_half(toDateTime64('2024-01-15 10:30:00.000000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:00.500000"')
        chdb.drop_function("dt64_add_half")

    # ── create_function: arg_types count mismatch ──

    def test_create_function_arg_types_count_mismatch(self):
        def dummy(a, b):
            return a

        with self.assertRaises(RuntimeError):
            chdb.create_function("dt64_dummy", dummy, arg_types=[DATETIME64], return_type=DATETIME64)

    # ── compatible arg type: DateTime → DateTime64 ──

    def test_create_function_compatible_arg_type_datetime_to_datetime64(self):
        chdb.create_function("dt64_compat", lambda d: d, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_compat(toDateTime('2024-03-01 12:00:00'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-03-01 12:00:00.000000"')
        chdb.drop_function("dt64_compat")

    # ── extract datetime64 components ──

    def test_extract_hour(self):
        chdb.create_function("dt64_hour", lambda d: d.hour, arg_types=[DATETIME64], return_type=INT32)
        ret = self.session.query("SELECT dt64_hour(toDateTime64('2024-01-15 14:30:00.000', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "14")
        chdb.drop_function("dt64_hour")

    def test_extract_minute(self):
        chdb.create_function("dt64_minute", lambda d: d.minute, arg_types=[DATETIME64], return_type=INT32)
        ret = self.session.query("SELECT dt64_minute(toDateTime64('2024-01-15 10:45:00.000', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "45")
        chdb.drop_function("dt64_minute")

    def test_extract_second(self):
        chdb.create_function("dt64_second", lambda d: d.second, arg_types=[DATETIME64], return_type=INT32)
        ret = self.session.query("SELECT dt64_second(toDateTime64('2024-01-15 10:30:59.000', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "59")
        chdb.drop_function("dt64_second")

    def test_extract_milliseconds(self):
        chdb.create_function("dt64_ms", lambda d: d.microsecond // 1000,
                             arg_types=[DATETIME64], return_type=INT32)
        ret = self.session.query("SELECT dt64_ms(toDateTime64('2024-01-15 10:30:00.789', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "789")
        chdb.drop_function("dt64_ms")

    def test_extract_microseconds(self):
        chdb.create_function("dt64_us", lambda d: d.microsecond,
                             arg_types=["DateTime64(6)"], return_type=INT32)
        ret = self.session.query("SELECT dt64_us(toDateTime64('2024-01-15 10:30:00.123456', 6))", "CSV")
        self.assertEqual(str(ret).strip(), "123456")
        chdb.drop_function("dt64_us")

    def test_extract_date_from_datetime64(self):
        chdb.create_function("dt64_date", lambda d: d.date(), arg_types=[DATETIME64], return_type=DATE)
        ret = self.session.query("SELECT dt64_date(toDateTime64('2024-01-15 14:30:00.123', 3))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15"')
        chdb.drop_function("dt64_date")

    # ── @func decorator ──

    def test_func_decorator_datetime64_explicit_all(self):
        @func(arg_types=[DATETIME64], return_type=DATETIME64)
        def dec_dt64_add_day(d):
            return d + datetime.timedelta(days=1)

        ret = self.session.query("SELECT dec_dt64_add_day(toDateTime64('2024-01-15 10:30:00.123456', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 10:30:00.123456"')
        chdb.drop_function("dec_dt64_add_day")

    def test_func_decorator_datetime64_return_only(self):
        @func(return_type=DATETIME64)
        def dec_dt64_const(x):
            return datetime.datetime(2024, 6, 1, 12, 0, 0, 500000,
                                     tzinfo=datetime.timezone.utc)

        ret = self.session.query("SELECT dec_dt64_const(42)", "CSV")
        self.assertIn("2024-06-01", str(ret).strip())
        chdb.drop_function("dec_dt64_const")

    def test_func_decorator_datetime64_infer_all(self):
        @func()
        def dec_dt64_id(d: datetime.datetime) -> datetime.datetime:
            return d

        ret = self.session.query("SELECT dec_dt64_id(toDateTime64('2024-09-01 08:15:30.456', 3))", "CSV")
        self.assertIn("2024-09-01", str(ret).strip())
        self.assertIn("08:15:30", str(ret).strip())
        chdb.drop_function("dec_dt64_id")

    # ── special datetime64 cases ──

    def test_datetime64_sub_second_precision(self):
        chdb.create_function("dt64_sub_sec", lambda d: d + datetime.timedelta(milliseconds=1),
                             arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_sub_sec(toDateTime64('2024-01-15 10:30:00.999000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:01.000000"')
        chdb.drop_function("dt64_sub_sec")

    def test_datetime64_midnight(self):
        chdb.create_function("dt64_midnight", lambda d: d, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_midnight(toDateTime64('2024-01-15 00:00:00.000000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 00:00:00.000000"')
        chdb.drop_function("dt64_midnight")

    def test_datetime64_add_timedelta_30min(self):
        chdb.create_function("dt64_add_30m", lambda d: d + datetime.timedelta(minutes=30),
                             arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_add_30m(toDateTime64('2024-01-15 23:45:00.000000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:15:00.000000"')
        chdb.drop_function("dt64_add_30m")

    def test_datetime64_to_string(self):
        chdb.create_function("dt64_to_str", lambda d: d.strftime("%Y/%m/%d %H:%M:%S"),
                             arg_types=[DATETIME64], return_type=STRING)
        ret = self.session.query("SELECT dt64_to_str(toDateTime64('2024-01-15 14:30:45.123', 3))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024/01/15 14:30:45"')
        chdb.drop_function("dt64_to_str")

    def test_datetime64_to_isoformat(self):
        chdb.create_function("dt64_iso", lambda d: d.isoformat(),
                             arg_types=[DATETIME64], return_type=STRING)
        ret = self.session.query("SELECT dt64_iso(toDateTime64('2024-01-15 10:30:00.123', 3))", "CSV")
        self.assertIn("2024-01-15", str(ret).strip())
        self.assertIn("10:30:00", str(ret).strip())
        chdb.drop_function("dt64_iso")

    def test_datetime64_has_timezone(self):
        chdb.create_function("dt64_has_tz", lambda d: d.tzinfo is not None,
                             arg_types=[DATETIME64], return_type=BOOL)
        ret = self.session.query("SELECT dt64_has_tz(toDateTime64('2024-01-15 10:00:00.000', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        chdb.drop_function("dt64_has_tz")

    def test_datetime64_zero_fractional(self):
        chdb.create_function("dt64_zero_f", lambda d: d, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_zero_f(toDateTime64('2024-01-15 10:30:00.000000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:00.000000"')
        chdb.drop_function("dt64_zero_f")

    # ── different scales via string type ──

    def test_scale0_second_precision(self):
        chdb.create_function("dt64_s0_id", lambda d: d,
                             arg_types=["DateTime64(0)"], return_type="DateTime64(0)")
        ret = self.session.query(
            "SELECT dt64_s0_id(toDateTime64('2024-01-15 10:30:45', 0))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45"')
        chdb.drop_function("dt64_s0_id")

    def test_scale3_millisecond_precision(self):
        chdb.create_function("dt64_s3_id", lambda d: d,
                             arg_types=["DateTime64(3)"], return_type="DateTime64(3)")
        ret = self.session.query(
            "SELECT dt64_s3_id(toDateTime64('2024-01-15 10:30:45.123', 3))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123"')
        chdb.drop_function("dt64_s3_id")

    def test_scale6_microsecond_precision(self):
        chdb.create_function("dt64_s6_id", lambda d: d,
                             arg_types=["DateTime64(6)"], return_type="DateTime64(6)")
        ret = self.session.query(
            "SELECT dt64_s6_id(toDateTime64('2024-01-15 10:30:45.123456', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123456"')
        chdb.drop_function("dt64_s6_id")

    def test_scale9_nanosecond_precision(self):
        chdb.create_function("dt64_s9_id", lambda d: d,
                             arg_types=["DateTime64(9)"], return_type="DateTime64(9)")
        ret = self.session.query(
            "SELECT dt64_s9_id(toDateTime64('2024-01-15 10:30:45.123456000', 9))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123456000"')
        chdb.drop_function("dt64_s9_id")

    def test_scale6_preserves_full_microseconds(self):
        chdb.create_function("dt64_s6_us", lambda d: d.microsecond,
                             arg_types=["DateTime64(6)"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt64_s6_us(toDateTime64('2024-01-15 10:30:45.654321', 6))", "CSV")
        self.assertEqual(str(ret).strip(), "654321")
        chdb.drop_function("dt64_s6_us")

    def test_scale3_truncates_microseconds(self):
        """scale=3 only preserves milliseconds; sub-ms digits are lost."""
        chdb.create_function("dt64_s3_us", lambda d: d.microsecond,
                             arg_types=["DateTime64(3)"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt64_s3_us(toDateTime64('2024-01-15 10:30:45.789', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "789000")
        chdb.drop_function("dt64_s3_us")

    def test_add_microseconds_at_scale6(self):
        def add_microseconds(d):
            return d + datetime.timedelta(microseconds=500)

        chdb.create_function("dt64_s6_add", add_microseconds,
                             arg_types=["DateTime64(6)"], return_type="DateTime64(6)")
        ret = self.session.query(
            "SELECT dt64_s6_add(toDateTime64('2024-01-15 10:30:45.000000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.000500"')
        chdb.drop_function("dt64_s6_add")

    # ── cross-scale: arg_types and return_type have different scales ──

    def test_input_scale6_return_scale3(self):
        chdb.create_function("dt64_6to3", lambda d: d,
                             arg_types=["DateTime64(6)"], return_type="DateTime64(3)")
        ret = self.session.query(
            "SELECT dt64_6to3(toDateTime64('2024-01-15 10:30:45.123456', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123"')
        chdb.drop_function("dt64_6to3")

    def test_input_scale3_return_scale6(self):
        chdb.create_function("dt64_3to6", lambda d: d,
                             arg_types=["DateTime64(3)"], return_type="DateTime64(6)")
        ret = self.session.query(
            "SELECT dt64_3to6(toDateTime64('2024-01-15 10:30:45.789', 3))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.789000"')
        chdb.drop_function("dt64_3to6")

    def test_input_scale6_return_scale9(self):
        chdb.create_function("dt64_6to9", lambda d: d,
                             arg_types=["DateTime64(6)"], return_type="DateTime64(9)")
        ret = self.session.query(
            "SELECT dt64_6to9(toDateTime64('2024-01-15 10:30:45.123456', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123456000"')
        chdb.drop_function("dt64_6to9")

    def test_input_scale9_return_scale3(self):
        chdb.create_function("dt64_9to3", lambda d: d,
                             arg_types=["DateTime64(9)"], return_type="DateTime64(3)")
        ret = self.session.query(
            "SELECT dt64_9to3(toDateTime64('2024-01-15 10:30:45.123456789', 9))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123"')
        chdb.drop_function("dt64_9to3")

    def test_input_scale0_return_scale6(self):
        chdb.create_function("dt64_0to6", lambda d: d,
                             arg_types=["DateTime64(0)"], return_type="DateTime64(6)")
        ret = self.session.query(
            "SELECT dt64_0to6(toDateTime64('2024-01-15 10:30:45', 0))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.000000"')
        chdb.drop_function("dt64_0to6")

    def test_input_scale3_return_scale9(self):
        chdb.create_function("dt64_3to9", lambda d: d,
                             arg_types=["DateTime64(3)"], return_type="DateTime64(9)")
        ret = self.session.query(
            "SELECT dt64_3to9(toDateTime64('2024-01-15 10:30:45.123', 3))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123000000"')
        chdb.drop_function("dt64_3to9")

    def test_input_scale9_return_scale6(self):
        chdb.create_function("dt64_9to6", lambda d: d,
                             arg_types=["DateTime64(9)"], return_type="DateTime64(6)")
        ret = self.session.query(
            "SELECT dt64_9to6(toDateTime64('2024-01-15 10:30:45.123456789', 9))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.123456"')
        chdb.drop_function("dt64_9to6")

    def test_input_scale0_return_scale3(self):
        chdb.create_function("dt64_0to3", lambda d: d,
                             arg_types=["DateTime64(0)"], return_type="DateTime64(3)")
        ret = self.session.query(
            "SELECT dt64_0to3(toDateTime64('2024-01-15 10:30:45', 0))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.000"')
        chdb.drop_function("dt64_0to3")

    def test_input_scale0_return_scale9(self):
        chdb.create_function("dt64_0to9", lambda d: d,
                             arg_types=["DateTime64(0)"], return_type="DateTime64(9)")
        ret = self.session.query(
            "SELECT dt64_0to9(toDateTime64('2024-01-15 10:30:45', 0))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:45.000000000"')
        chdb.drop_function("dt64_0to9")

    # ── return_type scale with UDF-constructed datetime ──

    def test_udf_construct_datetime_return_scale3(self):
        """UDF constructs datetime with microseconds, but return_type is scale=3 (truncated)."""
        def make_dt(x):
            return datetime.datetime(2024, 1, 15, 12, 0, 0, 123456,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_mk_s3", make_dt, return_type="DateTime64(3, 'UTC')")
        ret = self.session.query("SELECT dt64_mk_s3(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 12:00:00.123"')
        chdb.drop_function("dt64_mk_s3")

    def test_udf_construct_datetime_return_scale9(self):
        """UDF constructs datetime with microseconds, return_type is scale=9 (zero-padded)."""
        def make_dt(x):
            return datetime.datetime(2024, 1, 15, 12, 0, 0, 123456,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_mk_s9", make_dt, return_type="DateTime64(9, 'UTC')")
        ret = self.session.query("SELECT dt64_mk_s9(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 12:00:00.123456000"')
        chdb.drop_function("dt64_mk_s9")

    def test_udf_construct_datetime_return_scale0(self):
        """UDF constructs datetime with microseconds, return_type is scale=0 (seconds only)."""
        def make_dt(x):
            return datetime.datetime(2024, 1, 15, 12, 0, 0, 999999,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_mk_s0", make_dt, return_type="DateTime64(0, 'UTC')")
        ret = self.session.query("SELECT dt64_mk_s0(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 12:00:00"')
        chdb.drop_function("dt64_mk_s0")

    # ── different timezones via string type ──

    def test_utc_timezone(self):
        chdb.create_function("dt64_utc_id", lambda d: d,
                             arg_types=["DateTime64(3, 'UTC')"],
                             return_type="DateTime64(3, 'UTC')")
        ret = self.session.query(
            "SELECT dt64_utc_id(toDateTime64('2024-01-15 10:30:00.123', 3, 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 10:30:00.123"')
        chdb.drop_function("dt64_utc_id")

    def test_shanghai_timezone(self):
        chdb.create_function("dt64_sh_id", lambda d: d,
                             arg_types=["DateTime64(3, 'Asia/Shanghai')"],
                             return_type="DateTime64(3, 'Asia/Shanghai')")
        ret = self.session.query(
            "SELECT dt64_sh_id(toDateTime64('2024-01-15 18:30:00.456', 3, 'Asia/Shanghai'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 18:30:00.456"')
        chdb.drop_function("dt64_sh_id")

    def test_new_york_timezone(self):
        chdb.create_function("dt64_ny_id", lambda d: d,
                             arg_types=["DateTime64(6, 'America/New_York')"],
                             return_type="DateTime64(6, 'America/New_York')")
        ret = self.session.query(
            "SELECT dt64_ny_id(toDateTime64('2024-07-15 14:30:00.789012', 6, 'America/New_York'))",
            "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-15 14:30:00.789012"')
        chdb.drop_function("dt64_ny_id")

    def test_roundtrip_utc_preserves_time(self):
        def add_half_sec(d):
            return d + datetime.timedelta(milliseconds=500)

        chdb.create_function("dt64_utc_add", add_half_sec,
                             arg_types=["DateTime64(3, 'UTC')"],
                             return_type="DateTime64(3, 'UTC')")
        ret = self.session.query(
            "SELECT dt64_utc_add(toDateTime64('2024-01-15 23:59:59.500', 3, 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:00:00.000"')
        chdb.drop_function("dt64_utc_add")

    def test_roundtrip_shanghai_preserves_time(self):
        def add_half_sec(d):
            return d + datetime.timedelta(milliseconds=500)

        chdb.create_function("dt64_sh_add", add_half_sec,
                             arg_types=["DateTime64(3, 'Asia/Shanghai')"],
                             return_type="DateTime64(3, 'Asia/Shanghai')")
        ret = self.session.query(
            "SELECT dt64_sh_add(toDateTime64('2024-01-15 23:59:59.500', 3, 'Asia/Shanghai'))",
            "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-16 00:00:00.000"')
        chdb.drop_function("dt64_sh_add")

    def test_udf_receives_correct_timezone_info(self):
        def get_tz_name(d):
            return str(d.tzinfo)

        chdb.create_function("dt64_tz_name", get_tz_name,
                             arg_types=["DateTime64(3, 'Asia/Shanghai')"],
                             return_type=STRING)
        ret = self.session.query(
            "SELECT dt64_tz_name(toDateTime64('2024-06-15 12:00:00.000', 3, 'Asia/Shanghai'))",
            "CSV")
        self.assertIn("Asia/Shanghai", str(ret).strip())
        chdb.drop_function("dt64_tz_name")

    def test_cross_timezone_same_epoch(self):
        results = {}
        for tz, display_time, func_name in [
            ("UTC", "2024-01-15 00:00:00.000", "dt64_ep_utc"),
            ("Asia/Shanghai", "2024-01-15 08:00:00.000", "dt64_ep_sh"),
            ("America/New_York", "2024-01-14 19:00:00.000", "dt64_ep_ny"),
        ]:
            chdb.create_function(
                func_name, lambda d: int(d.timestamp()),
                arg_types=[f"DateTime64(3, '{tz}')"], return_type=INT64)
            ret = self.session.query(
                f"SELECT {func_name}(toDateTime64('{display_time}', 3, '{tz}'))", "CSV")
            results[tz] = int(str(ret).strip())
            chdb.drop_function(func_name)

        self.assertEqual(results["UTC"], results["Asia/Shanghai"],
                         "UTC and Shanghai should represent the same epoch")
        self.assertEqual(results["UTC"], results["America/New_York"],
                         "UTC and New York should represent the same epoch")

    def test_scale6_with_timezone_preserves_microseconds(self):
        chdb.create_function("dt64_s6tz_us", lambda d: d.microsecond,
                             arg_types=["DateTime64(6, 'UTC')"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt64_s6tz_us(toDateTime64('2024-01-15 10:30:45.654321', 6, 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), "654321")
        chdb.drop_function("dt64_s6tz_us")

    def test_construct_aware_datetime_in_udf(self):
        def make_utc_noon(x):
            return datetime.datetime(2024, 7, 1, 12, 0, 0, 500000,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_mk_utc", make_utc_noon,
                             return_type="DateTime64(6, 'UTC')")
        ret = self.session.query("SELECT dt64_mk_utc(1)", "CSV")
        self.assertIn("2024-07-01", str(ret).strip())
        self.assertIn("12:00:00.500000", str(ret).strip())
        chdb.drop_function("dt64_mk_utc")

    # ── timezone mismatch: UDF returns datetime in different tz than return_type ──

    def test_udf_returns_utc_but_return_type_is_shanghai(self):
        def make_utc_dt64(x):
            return datetime.datetime(2024, 1, 15, 0, 0, 0, 123456,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_tz_mis1", make_utc_dt64,
                             return_type="DateTime64(6, 'Asia/Shanghai')")
        ret = self.session.query("SELECT dt64_tz_mis1(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 08:00:00.123456"')
        chdb.drop_function("dt64_tz_mis1")

    def test_udf_returns_shanghai_but_return_type_is_utc(self):
        def make_shanghai_dt64(x):
            tz_sh = datetime.timezone(datetime.timedelta(hours=8))
            return datetime.datetime(2024, 1, 15, 8, 0, 0, 654321, tzinfo=tz_sh)

        chdb.create_function("dt64_tz_mis2", make_shanghai_dt64,
                             return_type="DateTime64(6, 'UTC')")
        ret = self.session.query("SELECT dt64_tz_mis2(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 00:00:00.654321"')
        chdb.drop_function("dt64_tz_mis2")

    def test_udf_returns_ny_but_return_type_is_shanghai(self):
        def make_ny_dt64(x):
            tz_ny = datetime.timezone(datetime.timedelta(hours=-5))
            return datetime.datetime(2024, 7, 15, 10, 0, 0, 100000, tzinfo=tz_ny)

        chdb.create_function("dt64_tz_mis3", make_ny_dt64,
                             return_type="DateTime64(6, 'Asia/Shanghai')")
        ret = self.session.query("SELECT dt64_tz_mis3(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-07-15 23:00:00.100000"')
        chdb.drop_function("dt64_tz_mis3")

    # ── timezone mismatch: input tz differs from arg_type tz ──
    #
    # ClickHouse preserves the INPUT's original timezone when passing to UDF.

    def test_input_utc_value_but_arg_type_is_shanghai(self):
        def get_hour(d):
            return d.hour

        chdb.create_function("dt64_in_mis1", get_hour,
                             arg_types=["DateTime64(3, 'Asia/Shanghai')"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt64_in_mis1(toDateTime64('2024-01-15 00:00:00.000', 3, 'UTC'))", "CSV")
        self.assertEqual(str(ret).strip(), "0",
                         "UDF should see hour=0 (UTC), not 8 (Shanghai)")
        chdb.drop_function("dt64_in_mis1")

    def test_input_shanghai_value_but_arg_type_is_utc(self):
        def get_hour(d):
            return d.hour

        chdb.create_function("dt64_in_mis2", get_hour,
                             arg_types=["DateTime64(3, 'UTC')"], return_type=INT32)
        ret = self.session.query(
            "SELECT dt64_in_mis2(toDateTime64('2024-01-15 08:00:00.000', 3, 'Asia/Shanghai'))",
            "CSV")
        self.assertEqual(str(ret).strip(), "8",
                         "UDF should see hour=8 (Shanghai), not 0 (UTC)")
        chdb.drop_function("dt64_in_mis2")

    def test_full_roundtrip_cross_tz(self):
        """Input Shanghai, UDF adds 500ms, output as UTC."""
        def add_half_sec(d):
            return d + datetime.timedelta(milliseconds=500)

        chdb.create_function("dt64_full_rt", add_half_sec,
                             arg_types=["DateTime64(3, 'Asia/Shanghai')"],
                             return_type="DateTime64(3, 'UTC')")
        ret = self.session.query(
            "SELECT dt64_full_rt(toDateTime64('2024-01-15 08:00:00.500', 3, 'Asia/Shanghai'))",
            "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-15 00:00:01.000"')
        chdb.drop_function("dt64_full_rt")

    # ── return_type scale + timezone combined ──

    def test_return_scale3_with_utc_timezone(self):
        def make_dt(x):
            return datetime.datetime(2024, 3, 15, 10, 30, 45, 123456,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_s3tz", make_dt,
                             return_type="DateTime64(3, 'UTC')")
        ret = self.session.query("SELECT dt64_s3tz(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-03-15 10:30:45.123"')
        chdb.drop_function("dt64_s3tz")

    def test_return_scale9_with_shanghai_timezone(self):
        def make_dt(x):
            return datetime.datetime(2024, 3, 15, 10, 30, 45, 123456,
                                     tzinfo=datetime.timezone.utc)

        chdb.create_function("dt64_s9tz", make_dt,
                             return_type="DateTime64(9, 'Asia/Shanghai')")
        ret = self.session.query("SELECT dt64_s9tz(1)", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-03-15 18:30:45.123456000"')
        chdb.drop_function("dt64_s9tz")

    # ── drop_function removes UDF ──

    def test_drop_function_removes_datetime64_udf(self):
        chdb.create_function("dt64_to_drop", lambda d: d, arg_types=[DATETIME64], return_type=DATETIME64)
        ret = self.session.query("SELECT dt64_to_drop(toDateTime64('2024-01-01 00:00:00.000000', 6))", "CSV")
        self.assertEqual(str(ret).strip(), '"2024-01-01 00:00:00.000000"')
        chdb.drop_function("dt64_to_drop")
        with self.assertRaises(Exception):
            self.session.query("SELECT dt64_to_drop(toDateTime64('2024-01-01 00:00:00.000', 3))", "CSV")

    # ── Python callability preserved ──

    def test_func_decorator_preserves_python_callability(self):
        @func(return_type=DATETIME64)
        def dt64_py_callable(d):
            return d

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        result = dt64_py_callable(now)
        self.assertEqual(result, now)
        chdb.drop_function("dt64_py_callable")


class TestBytesUDF(unittest.TestCase):
    """bytes / bytearray / memoryview support (chdb-core#115).

    ClickHouse String is binary-safe. The Python bytes / bytearray annotations
    map to String, and values carry raw bytes in both directions: returning
    bytes/bytearray/memoryview inserts the bytes verbatim, and a bytes-declared
    argument receives a Python ``bytes`` value rather than a UTF-8-decoded str.
    """

    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── return direction: the exact issue repro ──

    def test_return_bytes_value(self):
        # Before the fix this raised:
        #   Cannot convert Python object of type '<class 'bytes'>' to String
        chdb.create_function("ret_bytes", lambda: b"xy", arg_types=[], return_type=bytes)
        ret = self.session.query("SELECT ret_bytes()", "CSV")
        self.assertEqual(str(ret).strip(), '"xy"')
        chdb.drop_function("ret_bytes")

    def test_return_str_with_bytes_return_type_still_works(self):
        # A str return into a bytes-declared (String) column keeps working.
        chdb.create_function("ret_str", lambda: "xy", arg_types=[], return_type=bytes)
        ret = self.session.query("SELECT ret_str()", "CSV")
        self.assertEqual(str(ret).strip(), '"xy"')
        chdb.drop_function("ret_str")

    def test_return_bytearray_value(self):
        chdb.create_function("ret_ba", lambda: bytearray(b"hello"), return_type=bytes)
        ret = self.session.query("SELECT ret_ba()", "CSV")
        self.assertEqual(str(ret).strip(), '"hello"')
        chdb.drop_function("ret_ba")

    def test_return_memoryview_value(self):
        chdb.create_function("ret_mv", lambda: memoryview(b"mv"), return_type=bytes)
        ret = self.session.query("SELECT ret_mv()", "CSV")
        self.assertEqual(str(ret).strip(), '"mv"')
        chdb.drop_function("ret_mv")

    def test_return_non_contiguous_memoryview(self):
        # PyBytes_FromObject materializes a contiguous copy, so a strided view
        # must survive intact (guards against a future raw-buffer-copy refactor).
        chdb.create_function("ret_mv_sliced", lambda: memoryview(b"abcdef")[::2], return_type=bytes)
        ret = self.session.query("SELECT ret_mv_sliced()", "CSV")
        self.assertEqual(str(ret).strip(), '"ace"')
        chdb.drop_function("ret_mv_sliced")

    def test_return_bytes_is_binary_safe(self):
        # Non-UTF-8 bytes must survive verbatim; assert via hex() to avoid CSV
        # encoding ambiguity.
        chdb.create_function("ret_raw", lambda: b"\x00\x01\xff\xfe", return_type=bytes)
        ret = self.session.query("SELECT hex(ret_raw())", "CSV")
        self.assertEqual(str(ret).strip(), '"0001FFFE"')
        chdb.drop_function("ret_raw")

    def test_return_bytes_length(self):
        chdb.create_function("ret_len5", lambda: b"abcde", return_type=bytes)
        ret = self.session.query("SELECT length(ret_len5())", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("ret_len5")

    def test_return_empty_bytes(self):
        chdb.create_function("ret_empty", lambda: b"", return_type=bytes)
        ret = self.session.query("SELECT length(ret_empty())", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("ret_empty")

    def test_return_bytes_inferred_from_annotation(self):
        def make_bytes() -> bytes:
            return b"zz"

        chdb.create_function("ret_bytes_ann", make_bytes)
        ret = self.session.query("SELECT ret_bytes_ann()", "CSV")
        self.assertEqual(str(ret).strip(), '"zz"')
        chdb.drop_function("ret_bytes_ann")

    def test_return_bytes_to_non_string_type_raises(self):
        # Returning bytes where the column is not String is a clear error.
        chdb.create_function("ret_bytes_bad", lambda: b"xy", arg_types=[], return_type=INT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT ret_bytes_bad()", "CSV")
        chdb.drop_function("ret_bytes_bad")

    # ── input direction: bytes-declared argument receives Python bytes ──

    def test_bytes_arg_receives_bytes_explicit(self):
        chdb.create_function("arg_kind", lambda x: type(x).__name__, arg_types=[bytes], return_type=STRING)
        ret = self.session.query("SELECT arg_kind('abc')", "CSV")
        self.assertEqual(str(ret).strip(), '"bytes"')
        chdb.drop_function("arg_kind")

    def test_bytes_arg_receives_bytes_annotation(self):
        def kind(x: bytes) -> str:
            return type(x).__name__

        chdb.create_function("arg_kind_ann", kind)
        ret = self.session.query("SELECT arg_kind_ann('abc')", "CSV")
        self.assertEqual(str(ret).strip(), '"bytes"')
        chdb.drop_function("arg_kind_ann")

    def test_bytearray_arg_receives_bytes(self):
        # bytearray annotation also maps to String; the value is delivered as
        # (immutable) bytes.
        chdb.create_function("arg_kind_ba", lambda x: type(x).__name__, arg_types=[bytearray], return_type=STRING)
        ret = self.session.query("SELECT arg_kind_ba('abc')", "CSV")
        self.assertEqual(str(ret).strip(), '"bytes"')
        chdb.drop_function("arg_kind_ba")

    def test_str_arg_still_receives_str(self):
        # No regression: a str-declared argument still receives a str.
        chdb.create_function("arg_kind_str", lambda x: type(x).__name__, arg_types=[STRING], return_type=STRING)
        ret = self.session.query("SELECT arg_kind_str('abc')", "CSV")
        self.assertEqual(str(ret).strip(), '"str"')
        chdb.drop_function("arg_kind_str")

    def test_bytes_arg_value_content(self):
        # The bytes value carries the actual column bytes.
        chdb.create_function("arg_upper", lambda x: x.upper(), arg_types=[bytes], return_type=bytes)
        ret = self.session.query("SELECT arg_upper('abc')", "CSV")
        self.assertEqual(str(ret).strip(), '"ABC"')
        chdb.drop_function("arg_upper")

    def test_bytes_arg_binary_input_not_utf8(self):
        # A bytes-declared arg receives raw non-UTF-8 bytes without a decode error.
        def to_hex(x: bytes) -> str:
            return x.hex().upper()

        chdb.create_function("bytes_to_hex", to_hex)
        ret = self.session.query("SELECT bytes_to_hex(unhex('00FF'))", "CSV")
        self.assertEqual(str(ret).strip(), '"00FF"')
        chdb.drop_function("bytes_to_hex")

    def test_bytes_arg_null_literal_skipped(self):
        # Default on_null='skip': a NULL argument yields NULL without calling the
        # UDF, same as every other type — the bytes fast path checks isNullAt first.
        chdb.create_function("arg_upper_n", lambda x: x.upper(), arg_types=[bytes], return_type=bytes)
        ret = self.session.query("SELECT arg_upper_n(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("arg_upper_n")

    def test_bytes_arg_nullable_column_mix(self):
        # NULL rows become NULL; non-NULL rows still receive bytes and run.
        chdb.create_function("arg_upper_c", lambda x: x.upper(), arg_types=[bytes], return_type=bytes)
        ret = self.session.query(
            "SELECT arg_upper_c(x) FROM "
            "(SELECT v AS x FROM values('v Nullable(String)', ('ab'), (NULL), ('cd')))",
            "CSV",
        )
        self.assertEqual(str(ret).strip(), '"AB"\n\\N\n"CD"')
        chdb.drop_function("arg_upper_c")

    # ── end-to-end binary round-trip: def f(x: bytes) -> bytes ──

    def test_bytes_round_trip_binary_safe(self):
        def echo(x: bytes) -> bytes:
            return x

        chdb.create_function("bytes_echo", echo)
        ret = self.session.query("SELECT hex(bytes_echo(unhex('0001FFFE')))", "CSV")
        self.assertEqual(str(ret).strip(), '"0001FFFE"')
        chdb.drop_function("bytes_echo")


class TestOptionalAnnotationUDF(unittest.TestCase):
    """Optional[X] / Union[X, None] / PEP 604 X | None annotations (chdb-core#188).

    Every UDF argument and return type is Nullable engine-side, so ``Optional[X]``
    maps one-to-one onto that: the annotation only selects the base type ``X`` and
    is otherwise identical to a bare ``X``. Multi-member unions (e.g.
    ``Union[int, str]``) stay ambiguous and are rejected at registration.
    """

    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── the exact issue repro: Optional[str] arg + return, both inferred ──

    def test_optional_str_annotation_infers_string(self):
        # Before the fix this raised at registration:
        #   RuntimeError: Failed to create function 'opt_echo':
        #     Unknown Python UDF type annotation: <class 'typing.Union'>
        @func()
        def opt_echo(x: Optional[str]) -> Optional[str]:
            return x

        ret = self.session.query("SELECT opt_echo('hello')", "CSV")
        self.assertEqual(str(ret).strip(), '"hello"')
        chdb.drop_function("opt_echo")

    def test_optional_int_annotation_infers_int64(self):
        @func()
        def opt_inc(x: Optional[int]) -> Optional[int]:
            return None if x is None else x + 1

        ret = self.session.query("SELECT opt_inc(toInt64(5))", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        ret = self.session.query("SELECT opt_inc(toInt64(-3))", "CSV")
        self.assertEqual(str(ret).strip(), "-2")
        chdb.drop_function("opt_inc")

    def test_optional_float_annotation_infers_float64(self):
        @func()
        def opt_half(x: Optional[float]) -> Optional[float]:
            return None if x is None else x / 2

        ret = self.session.query("SELECT opt_half(toFloat64(9))", "CSV")
        self.assertEqual(str(ret).strip(), "4.5")
        chdb.drop_function("opt_half")

    def test_optional_date_annotation_infers_date(self):
        @func()
        def opt_day(d: Optional[datetime.date]) -> Optional[datetime.date]:
            return d

        ret = self.session.query("SELECT opt_day(toDate('2020-01-15'))", "CSV")
        self.assertEqual(str(ret).strip(), '"2020-01-15"')
        chdb.drop_function("opt_day")

    # ── typing.Union[X, None] spelled explicitly (same as Optional[X]) ──

    def test_union_x_none_equivalent_to_optional(self):
        def union_echo(x: Union[str, None]) -> Union[str, None]:
            return x

        chdb.create_function("union_echo", union_echo)
        ret = self.session.query("SELECT union_echo('hey')", "CSV")
        self.assertEqual(str(ret).strip(), '"hey"')
        chdb.drop_function("union_echo")

    def test_union_none_x_order_independent(self):
        # None first: Union[None, int] must also resolve to the base type.
        def union_inc(x: Union[None, int]) -> Union[None, int]:
            return None if x is None else x + 1

        chdb.create_function("union_inc", union_inc)
        ret = self.session.query("SELECT union_inc(toInt64(7))", "CSV")
        self.assertEqual(str(ret).strip(), "8")
        chdb.drop_function("union_inc")

    # ── PEP 604 (X | None), Python 3.10+ only ──

    @unittest.skipIf(sys.version_info < (3, 10), "PEP 604 X | None requires Python 3.10+")
    def test_pep604_union_infers_types(self):
        # Defined inside the (skipped-on-3.9) method so the file still imports on
        # 3.9, where `int | None` at def-time would raise TypeError.
        def pep604_inc(x: int | None) -> int | None:  # noqa: E999
            return None if x is None else x + 1

        chdb.create_function("pep604_inc", pep604_inc)
        ret = self.session.query("SELECT pep604_inc(toInt64(4))", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("pep604_inc")

    # ── Optional return actually yields SQL NULL (Nullable return path) ──

    def test_optional_return_none_yields_sql_null(self):
        @func()
        def opt_maybe(x: Optional[int]) -> Optional[int]:
            return None if x > 10 else x

        # non-None value returned verbatim
        ret = self.session.query("SELECT opt_maybe(toInt64(5))", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        # None returned → NULL
        ret = self.session.query("SELECT opt_maybe(toInt64(20))", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("opt_maybe")

    # ── Optional arg + on_null="pass": the motivating use case (handle x is None) ──

    def test_optional_arg_on_null_pass_receives_none(self):
        @func(on_null="pass")
        def opt_none_to_zero(x: Optional[int]) -> int:
            return 0 if x is None else x + 1

        ret = self.session.query("SELECT opt_none_to_zero(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT opt_none_to_zero(toInt64(9))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("opt_none_to_zero")

    def test_optional_arg_on_null_pass_mixed_column(self):
        @func(on_null="pass")
        def opt_double(x: Optional[int]) -> Optional[int]:
            return None if x is None else x * 2

        ret = self.session.query(
            "SELECT opt_double(x) FROM "
            "(SELECT CAST(arrayJoin([1, NULL, 3]) AS Nullable(Int64)) AS x)",
            "CSV",
        )
        self.assertEqual(str(ret).strip(), "2\n\\N\n6")
        chdb.drop_function("opt_double")

    # ── Optional[bytes]: the arg still receives raw bytes, not a decoded str ──

    def test_optional_bytes_arg_receives_bytes(self):
        def kind(x: Optional[bytes]) -> str:
            return type(x).__name__

        chdb.create_function("opt_bytes_kind", kind)
        ret = self.session.query("SELECT opt_bytes_kind('abc')", "CSV")
        self.assertEqual(str(ret).strip(), '"bytes"')
        chdb.drop_function("opt_bytes_kind")

    def test_optional_bytes_round_trip_binary_safe(self):
        def echo(x: Optional[bytes]) -> Optional[bytes]:
            return x

        chdb.create_function("opt_bytes_echo", echo)
        ret = self.session.query("SELECT hex(opt_bytes_echo(unhex('0001FFFE')))", "CSV")
        self.assertEqual(str(ret).strip(), '"0001FFFE"')
        chdb.drop_function("opt_bytes_echo")

    # ── Optional in the explicit arg_types / return_type params, not annotations ──

    def test_explicit_arg_types_optional(self):
        chdb.create_function(
            "opt_arg_explicit", lambda x: x + 1, arg_types=[Optional[int]], return_type=INT64)
        ret = self.session.query("SELECT opt_arg_explicit(toInt64(9))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("opt_arg_explicit")

    def test_explicit_return_type_optional(self):
        chdb.create_function(
            "opt_ret_explicit", lambda: "hi", arg_types=[], return_type=Optional[str])
        ret = self.session.query("SELECT opt_ret_explicit()", "CSV")
        self.assertEqual(str(ret).strip(), '"hi"')
        chdb.drop_function("opt_ret_explicit")

    # ── decorator still returns a normally-callable Python function ──

    def test_optional_preserves_python_callability(self):
        @func()
        def opt_call(x: Optional[int]) -> Optional[int]:
            return None if x is None else x + 1

        self.assertEqual(opt_call(5), 6)
        self.assertIsNone(opt_call(None))
        chdb.drop_function("opt_call")

    # ── multi-member unions stay ambiguous → rejected at registration ──

    def test_multi_member_union_arg_rejected(self):
        def bad(x: Union[int, str]) -> int:
            return 0

        with self.assertRaisesRegex(RuntimeError, "union type annotation"):
            chdb.create_function("bad_union_arg", bad)

    def test_multi_member_union_return_rejected(self):
        def bad_ret() -> Union[int, str]:
            return 0

        with self.assertRaisesRegex(RuntimeError, "union type annotation"):
            chdb.create_function("bad_union_ret", bad_ret)

    def test_multi_member_union_explicit_arg_types_rejected(self):
        # Distinct C++ path (resolveArgTypes) from the inferred-annotation path.
        with self.assertRaisesRegex(RuntimeError, "union type annotation"):
            chdb.create_function(
                "bad_union_arg_explicit", lambda x: x, arg_types=[Union[int, str]], return_type=INT64)

    def test_multi_member_union_explicit_return_type_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "union type annotation"):
            chdb.create_function(
                "bad_union_explicit", lambda x: x, arg_types=[INT64], return_type=Union[int, str])


if __name__ == "__main__":
    unittest.main()
