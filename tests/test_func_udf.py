#!python3

import unittest
import chdb
from chdb import func
from chdb.sqltypes import INT64, FLOAT64, STRING, BOOL
from chdb.session import Session


class TestUDFParameterKind(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── KEYWORD_ONLY rejected ──

    def test_rejects_keyword_only_param(self):
        def bad_kw(a, *, b):
            return a + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("bad_kw", bad_kw, return_type=INT64)

    def test_rejects_keyword_only_param_no_positional(self):
        def only_kw(*, x):
            return x

        with self.assertRaises(RuntimeError):
            chdb.create_function("only_kw", only_kw, return_type=INT64)

    def test_decorator_rejects_keyword_only_param(self):
        with self.assertRaises(RuntimeError):
            @func(return_type=INT64)
            def dec_bad_kw(a, *, b):
                return a + b

    # ── VAR_KEYWORD rejected ──

    def test_rejects_var_keyword_param(self):
        def bad_kwargs(a, **kwargs):
            return a

        with self.assertRaises(RuntimeError):
            chdb.create_function("bad_kwargs", bad_kwargs, return_type=INT64)

    # ── VAR_POSITIONAL at the end: accepted ──

    def test_accepts_var_positional_only(self):
        def variadic(*args):
            return sum(args)

        chdb.create_function("variadic_ok", variadic, return_type=INT64)
        ret = self.session.query("SELECT variadic_ok(1, 2, 3)", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        ret = self.session.query("SELECT variadic_ok(42)", "CSV")
        self.assertEqual(str(ret).strip(), "42")
        chdb.drop_function("variadic_ok")

    def test_accepts_positional_then_var_positional(self):
        def with_varargs(a, *args):
            return a + sum(args)

        chdb.create_function("with_varargs_ok", with_varargs, return_type=INT64)
        ret = self.session.query("SELECT with_varargs_ok(10, 20, 40)", "CSV")
        self.assertEqual(str(ret).strip(), "70")
        ret = self.session.query("SELECT with_varargs_ok(100)", "CSV")
        self.assertEqual(str(ret).strip(), "100")
        chdb.drop_function("with_varargs_ok")

    # ── VAR_POSITIONAL followed by more params: rejected ──

    def test_rejects_param_after_var_positional(self):
        def bad_after_varargs(*args, b):
            return sum(args) + b

        with self.assertRaises(RuntimeError):
            chdb.create_function("bad_after_varargs", bad_after_varargs, return_type=INT64)

    def test_rejects_kwargs_after_var_positional(self):
        def bad_all(*args, **kwargs):
            return 0

        with self.assertRaises(RuntimeError):
            chdb.create_function("bad_all", bad_all, return_type=INT64)


class TestNullAndExceptionHandling(unittest.TestCase):
    """Tests for on_null and on_error policies, including Nullable input types."""

    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── on_null="skip" (default): NULL input → NULL output, function not called ──

    def test_null_skip_default_returns_null(self):
        chdb.create_function("add_one", lambda x: x + 1, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT add_one(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("add_one")

    def test_null_skip_explicit_string(self):
        chdb.create_function("add_one_s", lambda x: x + 1, arg_types=[INT64], return_type=INT64, on_null="skip")
        ret = self.session.query("SELECT add_one_s(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("add_one_s")

    def test_null_skip_explicit_enum(self):
        chdb.create_function("add_one_e", lambda x: x + 1, arg_types=[INT64], return_type=INT64,
                             on_null=chdb.NullHandling.SKIP)
        ret = self.session.query("SELECT add_one_e(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("add_one_e")

    def test_null_skip_case_insensitive(self):
        chdb.create_function("add_one_ci", lambda x: x + 1, arg_types=[INT64], return_type=INT64, on_null="SKIP")
        ret = self.session.query("SELECT add_one_ci(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("add_one_ci")

    def test_null_skip_non_null_still_works(self):
        chdb.create_function("add_two", lambda x: x + 2, arg_types=[INT64], return_type=INT64, on_null="skip")
        ret = self.session.query("SELECT add_two(3)", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("add_two")

    # ── on_null="skip" over mixed NULL/non-NULL columns: function must not be
    #    called for NULL rows (not even with discarded placeholder values) ──

    def test_null_skip_mixed_column_skips_calls(self):
        calls = []

        def tracked(x):
            calls.append(x)
            return x * 10

        chdb.create_function("tracked_skip", tracked, arg_types=[INT64], return_type=INT64, on_null="skip")
        ret = self.session.query(
            "SELECT tracked_skip(x) FROM (SELECT CAST(arrayJoin([1, 2, NULL]) AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "10\n20\n\\N")
        self.assertEqual(sorted(calls), [1, 2])
        chdb.drop_function("tracked_skip")

    def test_null_skip_propagate_no_placeholder_exception(self):
        # With skip + propagate (the defaults), a NULL row must not fail the
        # query by invoking the function with a placeholder value (0 here,
        # which would raise ZeroDivisionError).
        chdb.create_function("recip_skip", lambda x: 100 // x, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT recip_skip(x) FROM (SELECT CAST(arrayJoin([50, NULL, 20]) AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "2\n\\N\n5")
        chdb.drop_function("recip_skip")

    def test_null_skip_multi_arg_mixed_column(self):
        calls = []

        def tracked_add(a, b):
            calls.append((a, b))
            return a + b

        chdb.create_function("tracked_add", tracked_add, arg_types=[INT64, INT64], return_type=INT64, on_null="skip")
        self.session.query(
            "CREATE TABLE null_pairs (id Int64, a Nullable(Int64), b Nullable(Int64)) ENGINE = Memory")
        self.session.query(
            "INSERT INTO null_pairs VALUES (1, 1, 10), (2, NULL, 20), (3, 3, NULL), (4, NULL, NULL), (5, 5, 50)")
        ret = self.session.query("SELECT tracked_add(a, b) FROM null_pairs ORDER BY id", "CSV")
        self.assertEqual(str(ret).strip(), "11\n\\N\n\\N\n\\N\n55")
        self.assertEqual(sorted(calls), [(1, 10), (5, 50)])
        self.session.query("DROP TABLE null_pairs")
        chdb.drop_function("tracked_add")

    def test_null_pass_mixed_column_receives_none(self):
        calls = []

        def collect(x):
            calls.append(x)
            return -1 if x is None else x * 10

        chdb.create_function("collect_pass", collect, arg_types=[INT64], return_type=INT64, on_null="pass")
        ret = self.session.query(
            "SELECT collect_pass(x) FROM (SELECT CAST(arrayJoin([1, NULL, 3]) AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "10\n-1\n30")
        self.assertEqual(calls, [1, None, 3])
        chdb.drop_function("collect_pass")

    # ── on_null="pass": NULL input → None passed to function ──

    def test_null_pass_receives_none(self):
        def null_to_zero(x):
            return 0 if x is None else x + 1

        chdb.create_function("null_to_zero", null_to_zero, arg_types=[INT64], return_type=INT64, on_null="pass")
        ret = self.session.query("SELECT null_to_zero(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT null_to_zero(5)", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        chdb.drop_function("null_to_zero")

    def test_null_pass_enum(self):
        def safe_len(s):
            return 0 if s is None else len(s)

        chdb.create_function("safe_len", safe_len, arg_types=[STRING], return_type=INT64,
                             on_null=chdb.NullHandling.PASS)
        ret = self.session.query("SELECT safe_len(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT safe_len('hello')", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("safe_len")

    def test_null_pass_case_insensitive(self):
        chdb.create_function("null_pass_ci", lambda x: 0 if x is None else x,
                             arg_types=[INT64], return_type=INT64, on_null="Pass")
        ret = self.session.query("SELECT null_pass_ci(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("null_pass_ci")

    def test_null_pass_multiple_args_one_null(self):
        def add_or_zero(a, b):
            if a is None:
                a = 0
            if b is None:
                b = 0
            return a + b

        chdb.create_function("add_or_zero", add_or_zero, arg_types=[INT64, INT64], return_type=INT64, on_null="pass")
        ret = self.session.query("SELECT add_or_zero(NULL, 5)", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        ret = self.session.query("SELECT add_or_zero(3, NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "3")
        ret = self.session.query("SELECT add_or_zero(NULL, NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query("SELECT add_or_zero(3, 4)", "CSV")
        self.assertEqual(str(ret).strip(), "7")
        chdb.drop_function("add_or_zero")

    # ── @func decorator with on_null ──

    def test_func_decorator_null_skip(self):
        @func(arg_types=[INT64], return_type=INT64, on_null="skip")
        def dec_add_one(x):
            return x + 1

        ret = self.session.query("SELECT dec_add_one(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        ret = self.session.query("SELECT dec_add_one(10)", "CSV")
        self.assertEqual(str(ret).strip(), "11")
        chdb.drop_function("dec_add_one")

    def test_func_decorator_null_pass(self):
        @func(arg_types=[INT64], return_type=INT64, on_null="pass")
        def dec_null_safe(x):
            return -1 if x is None else x * 2

        ret = self.session.query("SELECT dec_null_safe(NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "-1")
        ret = self.session.query("SELECT dec_null_safe(5)", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("dec_null_safe")

    # ── on_error="propagate" (default): exception raised ──

    def test_error_propagate_default(self):
        chdb.create_function("div_udf", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64)
        with self.assertRaises(Exception):
            self.session.query("SELECT div_udf(1, 0)", "CSV")
        chdb.drop_function("div_udf")

    def test_error_propagate_explicit_string(self):
        chdb.create_function("div_p", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error="propagate")
        with self.assertRaises(Exception):
            self.session.query("SELECT div_p(1, 0)", "CSV")
        chdb.drop_function("div_p")

    def test_error_propagate_explicit_enum(self):
        chdb.create_function("div_pe", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error=chdb.ExceptionHandling.PROPAGATE)
        with self.assertRaises(Exception):
            self.session.query("SELECT div_pe(1, 0)", "CSV")
        chdb.drop_function("div_pe")

    # ── on_error="ignore": exception → NULL for that row ──

    def test_error_ignore_returns_null(self):
        chdb.create_function("div_ign", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error="ignore")
        ret = self.session.query("SELECT div_ign(1, 0)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("div_ign")

    def test_error_ignore_enum(self):
        chdb.create_function("div_ign_e", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error=chdb.ExceptionHandling.IGNORE)
        ret = self.session.query("SELECT div_ign_e(1, 0)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("div_ign_e")

    def test_error_ignore_case_insensitive(self):
        chdb.create_function("div_ign_ci", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error="IGNORE")
        ret = self.session.query("SELECT div_ign_ci(1, 0)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("div_ign_ci")

    def test_error_ignore_good_rows_still_work(self):
        chdb.create_function("div_mix", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error="ignore")
        ret = self.session.query("SELECT div_mix(10, 2)", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("div_mix")

    # ── @func decorator with on_error ──

    def test_func_decorator_error_propagate(self):
        @func(arg_types=[INT64, INT64], return_type=INT64, on_error="propagate")
        def dec_div_p(a, b):
            return a // b

        with self.assertRaises(Exception):
            self.session.query("SELECT dec_div_p(1, 0)", "CSV")
        chdb.drop_function("dec_div_p")

    def test_func_decorator_error_ignore(self):
        @func(arg_types=[INT64, INT64], return_type=INT64, on_error="ignore")
        def dec_div_i(a, b):
            return a // b

        ret = self.session.query("SELECT dec_div_i(1, 0)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        ret = self.session.query("SELECT dec_div_i(10, 5)", "CSV")
        self.assertEqual(str(ret).strip(), "2")
        chdb.drop_function("dec_div_i")

    # ── Combined: on_null + on_error ──

    def test_null_skip_error_ignore(self):
        chdb.create_function("combo_si", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_null="skip", on_error="ignore")
        ret = self.session.query("SELECT combo_si(NULL, 2)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        ret = self.session.query("SELECT combo_si(1, 0)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        ret = self.session.query("SELECT combo_si(10, 2)", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("combo_si")

    def test_null_pass_error_ignore(self):
        def safe_div(a, b):
            if a is None or b is None:
                return -1
            return a // b

        chdb.create_function("combo_pi", safe_div, arg_types=[INT64, INT64], return_type=INT64,
                             on_null="pass", on_error="ignore")
        ret = self.session.query("SELECT combo_pi(NULL, 2)", "CSV")
        self.assertEqual(str(ret).strip(), "-1")
        ret = self.session.query("SELECT combo_pi(10, NULL)", "CSV")
        self.assertEqual(str(ret).strip(), "-1")
        ret = self.session.query("SELECT combo_pi(10, 2)", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("combo_pi")

    def test_null_pass_error_propagate(self):
        def must_not_be_null(x):
            if x is None:
                raise ValueError("got None")
            return x + 1

        chdb.create_function("combo_pp", must_not_be_null, arg_types=[INT64], return_type=INT64,
                             on_null="pass", on_error="propagate")
        ret = self.session.query("SELECT combo_pp(5)", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        with self.assertRaises(Exception):
            self.session.query("SELECT combo_pp(NULL)", "CSV")
        chdb.drop_function("combo_pp")

    # ── Nullable input column with arg_type validation ──

    def test_nullable_input_with_arg_type_skip(self):
        chdb.create_function("inc_nullable", lambda x: x + 1, arg_types=[INT64], return_type=INT64, on_null="skip")
        ret = self.session.query(
            "SELECT inc_nullable(x) FROM (SELECT CAST(NULL AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        ret = self.session.query(
            "SELECT inc_nullable(x) FROM (SELECT CAST(42 AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("inc_nullable")

    def test_nullable_input_with_arg_type_pass(self):
        def nullable_handler(x):
            return 0 if x is None else x + 1

        chdb.create_function("inc_nullable_p", nullable_handler, arg_types=[INT64], return_type=INT64, on_null="pass")
        ret = self.session.query(
            "SELECT inc_nullable_p(x) FROM (SELECT CAST(NULL AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        ret = self.session.query(
            "SELECT inc_nullable_p(x) FROM (SELECT CAST(42 AS Nullable(Int64)) AS x)", "CSV")
        self.assertEqual(str(ret).strip(), "43")
        chdb.drop_function("inc_nullable_p")

    # ── Invalid on_null / on_error values ──

    def test_invalid_on_null_value(self):
        with self.assertRaises(RuntimeError):
            chdb.create_function("bad_null", lambda x: x, return_type=INT64, on_null="invalid")

    def test_invalid_on_error_value(self):
        with self.assertRaises(RuntimeError):
            chdb.create_function("bad_err", lambda x: x, return_type=INT64, on_error="invalid")


class TestUDFBulkAndComplexSQL(unittest.TestCase):
    """Tests for large data volumes, complex SQL, and multiple UDF calls."""

    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── Large data volume ──

    def test_int_udf_10k_rows(self):
        chdb.create_function("double_it", lambda x: x * 2, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT sum(double_it(toInt64(number))) FROM numbers(10000)", "CSV")
        self.assertEqual(str(ret).strip(), "99990000")
        chdb.drop_function("double_it")

    def test_string_udf_10k_rows(self):
        chdb.create_function("add_prefix", lambda s: "px_" + s, arg_types=[STRING], return_type=STRING)
        ret = self.session.query(
            "SELECT count() FROM (SELECT add_prefix(toString(number)) AS s FROM numbers(10000)) WHERE s LIKE 'px_%'",
            "CSV")
        self.assertEqual(str(ret).strip(), "10000")
        chdb.drop_function("add_prefix")

    def test_int_udf_200k_rows(self):
        chdb.create_function("inc_200k", lambda x: x + 1, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT sum(inc_200k(toInt64(number))) FROM numbers(200000)", "CSV")
        self.assertEqual(str(ret).strip(), "20000100000")
        chdb.drop_function("inc_200k")

    # ── Same UDF called multiple times in one query ──

    def test_same_udf_multiple_columns(self):
        chdb.create_function("sq", lambda x: x * x, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT sq(toInt64(number)), sq(toInt64(number) + 1), sq(toInt64(number) + 2) FROM numbers(5)", "CSV")
        lines = str(ret).strip().split("\n")
        self.assertEqual(len(lines), 5)
        for i, line in enumerate(lines):
            vals = [int(v) for v in line.split(",")]
            self.assertEqual(vals, [i*i, (i+1)*(i+1), (i+2)*(i+2)])
        chdb.drop_function("sq")

    def test_same_udf_nested_calls(self):
        chdb.create_function("add_ten", lambda x: x + 10, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT add_ten(add_ten(add_ten(toInt64(5))))", "CSV")
        self.assertEqual(str(ret).strip(), "35")
        chdb.drop_function("add_ten")

    def test_same_udf_in_where_and_select(self):
        chdb.create_function("triple", lambda x: x * 3, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT triple(toInt64(number)) FROM numbers(20) "
            "WHERE triple(toInt64(number)) > 30 ORDER BY number", "CSV")
        lines = str(ret).strip().split("\n")
        expected = [str(i * 3) for i in range(20) if i * 3 > 30]
        self.assertEqual(lines, expected)
        chdb.drop_function("triple")

    # ── Multiple different UDFs in one query ──

    def test_two_int_udfs_combined(self):
        chdb.create_function("udf_a", lambda x: x + 1, arg_types=[INT64], return_type=INT64)
        chdb.create_function("udf_b", lambda x: x * 2, arg_types=[INT64], return_type=INT64)
        ret = self.session.query("SELECT udf_b(udf_a(toInt64(number))) FROM numbers(5)", "CSV")
        lines = str(ret).strip().split("\n")
        expected = [str((i + 1) * 2) for i in range(5)]
        self.assertEqual(lines, expected)
        chdb.drop_function("udf_a")
        chdb.drop_function("udf_b")

    def test_int_and_string_udfs_combined(self):
        chdb.create_function("to_label", lambda x: "val_" + str(x), arg_types=[INT64], return_type=STRING)
        chdb.create_function("label_len", lambda s: len(s), arg_types=[STRING], return_type=INT64)
        ret = self.session.query(
            "SELECT label_len(to_label(toInt64(number))) FROM numbers(5)", "CSV")
        lines = str(ret).strip().split("\n")
        expected = [str(len("val_" + str(i))) for i in range(5)]
        self.assertEqual(lines, expected)
        chdb.drop_function("to_label")
        chdb.drop_function("label_len")

    def test_three_udfs_pipeline(self):
        chdb.create_function("step1", lambda x: x + 100, arg_types=[INT64], return_type=INT64)
        chdb.create_function("step2", lambda x: str(x), arg_types=[INT64], return_type=STRING)
        chdb.create_function("step3", lambda s: len(s), arg_types=[STRING], return_type=INT64)
        ret = self.session.query(
            "SELECT step3(step2(step1(toInt64(number)))) FROM numbers(5)", "CSV")
        lines = str(ret).strip().split("\n")
        self.assertEqual(lines, ["3", "3", "3", "3", "3"])
        chdb.drop_function("step1")
        chdb.drop_function("step2")
        chdb.drop_function("step3")

    # ── UDF with GROUP BY ──

    def test_udf_with_group_by(self):
        chdb.create_function("mod3", lambda x: x % 3, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT mod3(toInt64(number)) AS g, count() AS c FROM numbers(9) GROUP BY g ORDER BY g", "CSV")
        lines = str(ret).strip().split("\n")
        self.assertEqual(lines, ["0,3", "1,3", "2,3"])
        chdb.drop_function("mod3")

    # ── UDF with ORDER BY ──

    def test_udf_in_order_by(self):
        chdb.create_function("neg", lambda x: -x, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT number FROM numbers(5) ORDER BY neg(toInt64(number))", "CSV")
        lines = str(ret).strip().split("\n")
        self.assertEqual(lines, ["4", "3", "2", "1", "0"])
        chdb.drop_function("neg")

    # ── UDF with subquery ──

    def test_udf_in_subquery(self):
        chdb.create_function("plus_five", lambda x: x + 5, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT sum(v) FROM (SELECT plus_five(toInt64(number)) AS v FROM numbers(10))", "CSV")
        self.assertEqual(str(ret).strip(), "95")
        chdb.drop_function("plus_five")

    # ── UDF with HAVING ──

    def test_udf_with_having(self):
        chdb.create_function("bucket", lambda x: x // 10, arg_types=[INT64], return_type=INT64)
        ret = self.session.query(
            "SELECT bucket(toInt64(number)) AS b, count() AS c "
            "FROM numbers(50) GROUP BY b HAVING c > 5 ORDER BY b", "CSV")
        lines = str(ret).strip().split("\n")
        self.assertEqual(len(lines), 5)
        for line in lines:
            self.assertEqual(int(line.split(",")[1]), 10)
        chdb.drop_function("bucket")

    # ── UDF with JOIN-like pattern ──

    def test_udf_classify_rows(self):
        chdb.create_function("classify", lambda x: "big" if x >= 5 else "small",
                             arg_types=[INT64], return_type=STRING)
        ret = self.session.query(
            "SELECT toInt64(number) AS n, classify(toInt64(number)) "
            "FROM numbers(10) ORDER BY n", "CSV")
        lines = str(ret).strip().split("\n")
        self.assertEqual(len(lines), 10)
        for i, line in enumerate(lines):
            parts = line.split(",")
            self.assertEqual(int(parts[0]), i)
            expected_label = '"big"' if i >= 5 else '"small"'
            self.assertEqual(parts[1], expected_label)
        chdb.drop_function("classify")

    # ── Large volume with NULL and error handling ──

    def test_bulk_with_null_skip(self):
        chdb.create_function("safe_inc", lambda x: x + 1, arg_types=[INT64], return_type=INT64, on_null="skip")
        ret = self.session.query(
            "SELECT count() FROM ("
            "  SELECT safe_inc(if(number % 3 = 0, NULL, toInt64(number))) AS v FROM numbers(9000)"
            ") WHERE v IS NOT NULL", "CSV")
        self.assertEqual(str(ret).strip(), "6000")
        chdb.drop_function("safe_inc")

    def test_bulk_with_error_ignore(self):
        chdb.create_function("risky_div", lambda a, b: a // b, arg_types=[INT64, INT64], return_type=INT64,
                             on_error="ignore")
        ret = self.session.query(
            "SELECT count() FROM ("
            "  SELECT risky_div(toInt64(100), toInt64(number)) AS v FROM numbers(100)"
            ") WHERE v IS NOT NULL", "CSV")
        self.assertEqual(str(ret).strip(), "99")
        chdb.drop_function("risky_div")

    # ── String UDF with complex SQL ──

    def test_string_udf_concat_with_aggregate(self):
        chdb.create_function("tag", lambda s: "[" + s + "]", arg_types=[STRING], return_type=STRING)
        ret = self.session.query(
            "SELECT groupArray(tag(toString(number))) FROM numbers(5)", "CSV")
        result = str(ret).strip()
        for i in range(5):
            self.assertIn(f"[{i}]", result)
        chdb.drop_function("tag")

    def test_string_udf_filter_and_transform(self):
        chdb.create_function("upper_if_long", lambda s: s.upper() if len(s) > 1 else s,
                             arg_types=[STRING], return_type=STRING)
        ret = self.session.query(
            "SELECT upper_if_long(toString(number)) FROM numbers(15) "
            "WHERE length(toString(number)) > 1 ORDER BY number", "CSV")
        lines = str(ret).strip().split("\n")
        expected = [f'"{str(i).upper()}"' for i in range(10, 15)]
        self.assertEqual(lines, expected)
        chdb.drop_function("upper_if_long")


class TestUDFPythonNativeTypes(unittest.TestCase):
    """Verify that arg_types and return_type accept Python native types,
    and that annotation-based inference produces identical results."""

    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    def test_annotation_equivalent_to_explicit_native_types(self):
        def fmt_anno(n: int, s: str) -> str:
            return f"{s}:{n}"

        def fmt_explicit(n, s):
            return f"{s}:{n}"

        chdb.create_function("fmt_anno", fmt_anno)
        chdb.create_function("fmt_explicit", fmt_explicit, arg_types=[int, str], return_type=str)

        sql = "SELECT {}(toInt64(42), 'val')"
        ret_anno = str(self.session.query(sql.format("fmt_anno"), "CSV")).strip()
        ret_explicit = str(self.session.query(sql.format("fmt_explicit"), "CSV")).strip()
        self.assertEqual(ret_anno, '"val:42"')
        self.assertEqual(ret_anno, ret_explicit)

        chdb.drop_function("fmt_anno")
        chdb.drop_function("fmt_explicit")


class TestUDFUnsupportedArgTypes(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    # ── Array → Python list ──

    def test_array_input_sum(self):
        def sum_arr(arr):
            return sum(arr)

        chdb.create_function("udf_sum_arr", sum_arr, return_type=INT64)
        ret = self.session.query("SELECT udf_sum_arr([1, 2, 3, 4])", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("udf_sum_arr")

    def test_array_input_len(self):
        def arr_len(arr):
            return len(arr)

        chdb.create_function("udf_arr_len", arr_len, return_type=INT64)
        ret = self.session.query("SELECT udf_arr_len([10, 20, 30])", "CSV")
        self.assertEqual(str(ret).strip(), "3")
        ret = self.session.query("SELECT udf_arr_len([]::Array(Int64))", "CSV")
        self.assertEqual(str(ret).strip(), "0")
        chdb.drop_function("udf_arr_len")

    def test_array_string_input(self):
        def join_arr(arr):
            return ",".join(arr)

        chdb.create_function("udf_join_arr", join_arr, return_type=STRING)
        ret = self.session.query("SELECT udf_join_arr(['a', 'b', 'c'])", "CSV")
        self.assertEqual(str(ret).strip(), '"a,b,c"')
        chdb.drop_function("udf_join_arr")

    # ── IPv4 → Python ipaddress.IPv4Address ──

    def test_ipv4_input_to_string(self):
        def ip_to_str(ip):
            return str(ip)

        chdb.create_function("udf_ip_str", ip_to_str, return_type=STRING)
        ret = self.session.query("SELECT udf_ip_str(toIPv4('192.168.1.1'))", "CSV")
        self.assertEqual(str(ret).strip(), '"192.168.1.1"')
        chdb.drop_function("udf_ip_str")

    def test_ipv4_is_private(self):
        def ip_is_private(ip):
            return ip.is_private

        chdb.create_function("udf_ip_priv", ip_is_private, return_type=BOOL)
        ret = self.session.query("SELECT udf_ip_priv(toIPv4('192.168.1.1'))", "CSV")
        self.assertEqual(str(ret).strip(), "true")
        ret = self.session.query("SELECT udf_ip_priv(toIPv4('8.8.8.8'))", "CSV")
        self.assertEqual(str(ret).strip(), "false")
        chdb.drop_function("udf_ip_priv")

    # ── IPv6 → Python ipaddress.IPv6Address ──

    def test_ipv6_input_to_string(self):
        def ipv6_to_str(ip):
            return str(ip)

        chdb.create_function("udf_ipv6_str", ipv6_to_str, return_type=STRING)
        ret = self.session.query("SELECT udf_ipv6_str(toIPv6('::1'))", "CSV")
        self.assertEqual(str(ret).strip(), '"::1"')
        chdb.drop_function("udf_ipv6_str")

    # ── UUID → Python uuid.UUID ──

    def test_uuid_input_to_string(self):
        def uuid_str(u):
            return str(u)

        chdb.create_function("udf_uuid_str", uuid_str, return_type=STRING)
        ret = self.session.query(
            "SELECT udf_uuid_str(toUUID('550e8400-e29b-41d4-a716-446655440000'))", "CSV")
        self.assertEqual(str(ret).strip(), '"550e8400-e29b-41d4-a716-446655440000"')
        chdb.drop_function("udf_uuid_str")

    def test_uuid_version(self):
        def uuid_ver(u):
            return u.version

        chdb.create_function("udf_uuid_ver", uuid_ver, return_type=INT64)
        ret = self.session.query(
            "SELECT udf_uuid_ver(toUUID('550e8400-e29b-41d4-a716-446655440000'))", "CSV")
        self.assertEqual(str(ret).strip(), "4")
        chdb.drop_function("udf_uuid_ver")

    # ── Tuple → Python tuple ──

    def test_tuple_input_len(self):
        def tup_len(t):
            return len(t)

        chdb.create_function("udf_tup_len", tup_len, return_type=INT64)
        ret = self.session.query("SELECT udf_tup_len(tuple(1, 'hello', 3.14))", "CSV")
        self.assertEqual(str(ret).strip(), "3")
        chdb.drop_function("udf_tup_len")

    def test_tuple_input_first_element(self):
        def tup_first(t):
            return t[0]

        chdb.create_function("udf_tup_first", tup_first, return_type=INT64)
        ret = self.session.query("SELECT udf_tup_first(tuple(42, 99))", "CSV")
        self.assertEqual(str(ret).strip(), "42")
        chdb.drop_function("udf_tup_first")

    # ── Map → Python dict ──

    def test_map_input_len(self):
        def map_len(m):
            return len(m)

        chdb.create_function("udf_map_len", map_len, return_type=INT64)
        ret = self.session.query("SELECT udf_map_len(map('a', 1, 'b', 2, 'c', 3))", "CSV")
        self.assertEqual(str(ret).strip(), "3")
        chdb.drop_function("udf_map_len")

    def test_map_input_get_value(self):
        def map_get_a(m):
            return m.get("a", -1)

        chdb.create_function("udf_map_get", map_get_a, return_type=INT64)
        ret = self.session.query("SELECT udf_map_get(map('a', 10, 'b', 20))", "CSV")
        self.assertEqual(str(ret).strip(), "10")
        chdb.drop_function("udf_map_get")

    # ── Enum → Python string ──

    def test_enum_input_as_string(self):
        def enum_upper(s):
            return s.upper()

        chdb.create_function("udf_enum_upper", enum_upper, return_type=STRING)
        ret = self.session.query(
            "SELECT udf_enum_upper(CAST('hello' AS Enum8('hello' = 1, 'world' = 2)))", "CSV")
        self.assertEqual(str(ret).strip(), '"HELLO"')
        chdb.drop_function("udf_enum_upper")

    # ── Decimal → Python float ──

    def test_decimal_input(self):
        def dec_double(x):
            return x * 2

        chdb.create_function("udf_dec_double", dec_double, return_type=FLOAT64)
        ret = self.session.query("SELECT udf_dec_double(toDecimal64(3.14, 2))", "CSV")
        self.assertAlmostEqual(float(str(ret).strip()), 6.28, places=2)
        chdb.drop_function("udf_dec_double")

    # ── FixedString → Python string ──

    def test_fixedstring_input(self):
        def fstr_len(s):
            return len(s)

        chdb.create_function("udf_fstr_len", fstr_len, return_type=INT64)
        ret = self.session.query("SELECT udf_fstr_len(toFixedString('abc', 5))", "CSV")
        self.assertEqual(str(ret).strip(), "5")
        chdb.drop_function("udf_fstr_len")

    # ── Multiple unsupported types as args in one call ──

    def test_mixed_unsupported_types(self):
        def mixed(arr, ip):
            return str(len(arr)) + "_" + str(ip)

        chdb.create_function("udf_mixed", mixed, return_type=STRING)
        ret = self.session.query(
            "SELECT udf_mixed([1,2,3], toIPv4('10.0.0.1'))", "CSV")
        self.assertEqual(str(ret).strip(), '"3_10.0.0.1"')
        chdb.drop_function("udf_mixed")

    # ── Return None from UDF ──

    def test_return_none_becomes_null(self):
        def maybe_none(x):
            return None

        chdb.create_function("udf_none", maybe_none, return_type=INT64)
        ret = self.session.query("SELECT udf_none(toIPv4('1.2.3.4'))", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("udf_none")

    def test_conditional_none(self):
        def none_if_empty(arr):
            if len(arr) == 0:
                return None
            return sum(arr)

        chdb.create_function("udf_none_empty", none_if_empty, return_type=INT64)
        ret = self.session.query("SELECT udf_none_empty([1,2,3])", "CSV")
        self.assertEqual(str(ret).strip(), "6")
        ret = self.session.query("SELECT udf_none_empty([]::Array(Int64))", "CSV")
        self.assertEqual(str(ret).strip(), "\\N")
        chdb.drop_function("udf_none_empty")


if __name__ == "__main__":
    unittest.main()
