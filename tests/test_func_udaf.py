#!python3

import unittest

import chdb
from chdb import agg
from chdb.session import Session
from chdb.sqltypes import FLOAT64, INT64, STRING, UINT64


# Accumulators live at module level on purpose: -State/-Merge and spill-to-disk
# aggregation pickle the accumulator, which requires the class to be importable by name.


class PySum:
    """Minimal accumulator, types inferred from the annotations."""

    def __init__(self):
        self.total = 0

    def update(self, value: int) -> None:
        self.total += value

    def merge(self, other):
        self.total += other.total

    def evaluate(self) -> int:
        return self.total


class PyCountNonNull:
    """on_null="pass" accumulator: it sees the NULLs and counts what is not NULL."""

    def __init__(self):
        self.seen = 0
        self.nulls = 0

    def update(self, value):
        if value is None:
            self.nulls += 1
        else:
            self.seen += 1

    def merge(self, other):
        self.seen += other.seen
        self.nulls += other.nulls

    def evaluate(self):
        return self.seen * 1000 + self.nulls


class PyWeightedAvg:
    def __init__(self):
        self.num = 0.0
        self.den = 0.0

    def update(self, value, weight):
        self.num += value * weight
        self.den += weight

    def merge(self, other):
        self.num += other.num
        self.den += other.den

    def evaluate(self):
        return self.num / self.den if self.den else None


class PyMaxLen:
    """String in, String out; returns None (SQL NULL) when nothing was aggregated."""

    def __init__(self):
        self.best = None

    def update(self, value):
        if self.best is None or len(value) > len(self.best):
            self.best = value

    def merge(self, other):
        if other.best is not None and (self.best is None or len(other.best) > len(self.best)):
            self.best = other.best

    def evaluate(self):
        return self.best


class PySumBatch:
    """Same result as PySum, but consumes whole blocks through update_batch."""

    def __init__(self):
        self.total = 0
        self.update_calls = 0
        self.batch_calls = 0

    def update(self, value):
        self.total += value
        self.update_calls += 1

    def update_batch(self, values):
        self.total += sum(values)
        self.batch_calls += 1

    def merge(self, other):
        self.total += other.total
        self.update_calls += other.update_calls
        self.batch_calls += other.batch_calls

    def evaluate(self):
        return self.total


class PySumBatchProbe(PySumBatch):
    """Reports which code path consumed the rows instead of the aggregate value."""

    def evaluate(self):
        return self.batch_calls * 1000000 + self.update_calls


class PyVariadicSum:
    def __init__(self):
        self.total = 0

    def update(self, *args):
        self.total += sum(args)

    def merge(self, other):
        self.total += other.total

    def evaluate(self):
        return self.total


class PyRaisingUpdate:
    def __init__(self):
        self.total = 0

    def update(self, value):
        if value == 3:
            raise ValueError("boom at 3")
        self.total += value

    def merge(self, other):
        self.total += other.total

    def evaluate(self):
        return self.total


class PyRaisingEvaluate:
    def __init__(self):
        self.total = 0

    def update(self, value):
        self.total += value

    def merge(self, other):
        self.total += other.total

    def evaluate(self):
        raise ValueError("evaluate exploded")


class PyRaisingMerge:
    def __init__(self):
        self.total = 0

    def update(self, value):
        self.total += value

    def merge(self, other):
        raise ValueError("merge exploded")

    def evaluate(self):
        return self.total


class PyConcat:
    """Order-dependent accumulator, used to check the -State/-Merge round trip."""

    def __init__(self):
        self.parts = []

    def update(self, value):
        self.parts.append(str(value))

    def merge(self, other):
        self.parts.extend(other.parts)

    def evaluate(self):
        return ",".join(sorted(self.parts))


class PyPairCount:
    """Two-argument accumulator that reports how many of each argument were NULL."""

    def __init__(self):
        self.pairs = 0
        self.left_nulls = 0
        self.right_nulls = 0

    def update(self, left, right):
        self.pairs += 1
        if left is None:
            self.left_nulls += 1
        if right is None:
            self.right_nulls += 1

    def merge(self, other):
        self.pairs += other.pairs
        self.left_nulls += other.left_nulls
        self.right_nulls += other.right_nulls

    def evaluate(self):
        return self.pairs * 10000 + self.left_nulls * 100 + self.right_nulls


class PyPairBatch:
    """Two-argument accumulator whose update_batch receives one list per argument."""

    def __init__(self):
        self.total = 0.0
        self.batch_calls = 0

    def update(self, value, weight):
        self.total += value * weight

    def update_batch(self, values, weights):
        assert len(values) == len(weights)
        self.total += sum(v * w for v, w in zip(values, weights))
        self.batch_calls += 1

    def merge(self, other):
        self.total += other.total
        self.batch_calls += other.batch_calls

    def evaluate(self):
        return self.total


class PyUnpicklable:
    """Its state holds a lambda, so pickling the accumulator fails."""

    def __init__(self):
        self.total = 0
        self.hook = lambda x: x

    def update(self, value):
        self.total += value

    def merge(self, other):
        self.total += other.total

    def evaluate(self):
        return self.total


class PyRowCount:
    """Zero-argument accumulator: counts the rows it is fed."""

    def __init__(self):
        self.rows = 0

    def update(self):
        self.rows += 1

    def merge(self, other):
        self.rows += other.rows

    def evaluate(self):
        return self.rows


class PyMutateThenRaise:
    """Mutates itself and only then raises, to pin what on_error="ignore" can undo."""

    def __init__(self):
        self.total = 0

    def update(self, value):
        self.total += value
        if value == 3:
            raise ValueError("boom after mutating")

    def merge(self, other):
        self.total += other.total

    def evaluate(self):
        return self.total


class NotAnAccumulator:
    def update(self, value):
        pass


class UDAFTestCase(unittest.TestCase):
    """Drops everything registered through self.register() so tests stay independent."""

    def setUp(self):
        self.session = Session()
        self._registered = []

    def tearDown(self):
        try:
            for name in self._registered:
                chdb.drop_aggregate_function(name)
        finally:
            self.session.close()

    def register(self, name, accumulator, arg_types=None, return_type=None, **kwargs):
        chdb.create_aggregate_function(name, accumulator, arg_types, return_type, **kwargs)
        self._registered.append(name)

    def scalar(self, sql):
        return str(self.session.query(sql, "CSV")).strip()

    def rows(self, sql):
        text = str(self.session.query(sql, "CSV")).strip()
        return [line for line in text.split("\n") if line]


class TestUDAFBasics(UDAFTestCase):
    def test_ungrouped_sum_matches_builtin_sum(self):
        self.register("u_sum", PySum)

        py_result = self.scalar("SELECT u_sum(toInt64(number)) FROM numbers(100)")
        builtin_result = self.scalar("SELECT sum(toInt64(number)) FROM numbers(100)")

        self.assertEqual(py_result, "4950")
        self.assertEqual(py_result, builtin_result)

    def test_group_by_matches_builtin_sum_per_group(self):
        self.register("g_sum", PySum)

        py_rows = self.rows(
            "SELECT number % 4 AS k, g_sum(toInt64(number)) FROM numbers(100) GROUP BY k ORDER BY k"
        )
        builtin_rows = self.rows(
            "SELECT number % 4 AS k, sum(toInt64(number)) FROM numbers(100) GROUP BY k ORDER BY k"
        )

        self.assertEqual(py_rows, ["0,1200", "1,1225", "2,1250", "3,1275"])
        self.assertEqual(py_rows, builtin_rows)

    def test_multi_argument_weighted_average(self):
        self.register("w_avg", PyWeightedAvg, [FLOAT64, FLOAT64], FLOAT64)

        values = [(1.0, 1.0), (2.0, 3.0), (4.0, 2.0)]
        expected = sum(v * w for v, w in values) / sum(w for _, w in values)

        result = self.scalar(
            "SELECT w_avg(v, w) FROM values('v Float64, w Float64', (1.0, 1.0), (2.0, 3.0), (4.0, 2.0))"
        )

        self.assertEqual(float(result), expected)
        self.assertEqual(float(result), 2.5)

    def test_variadic_update_accepts_any_arity(self):
        self.register("v_sum", PyVariadicSum, return_type=INT64)

        self.assertEqual(self.scalar("SELECT v_sum(toInt64(1))"), "1")
        self.assertEqual(self.scalar("SELECT v_sum(toInt64(1), toInt64(2), toInt64(4))"), "7")

    def test_string_argument_and_string_result(self):
        self.register("s_maxlen", PyMaxLen, [STRING], STRING)

        result = self.scalar(
            "SELECT s_maxlen(s) FROM values('s String', ('a'), ('abcd'), ('ab'))"
        )

        self.assertEqual(result, '"abcd"')

    def test_evaluate_returning_none_becomes_null(self):
        self.register("s_maxlen_null", PyMaxLen, [STRING], STRING)

        result = self.scalar(
            "SELECT isNull(s_maxlen_null(s)) FROM values('s String', ('a')) WHERE s = 'never'"
        )

        self.assertEqual(result, "1")

    def test_empty_input_finalizes_a_fresh_accumulator(self):
        self.register("e_sum", PySum)

        self.assertEqual(self.scalar("SELECT e_sum(toInt64(number)) FROM numbers(0)"), "0")

    def test_uint64_argument_declared_as_uint64(self):
        self.register("u64_sum", PySum, [UINT64], INT64)

        self.assertEqual(self.scalar("SELECT u64_sum(number) FROM numbers(10)"), "45")

    def test_aggregate_alongside_builtin_in_one_query(self):
        self.register("m_sum", PySum)

        row = self.scalar(
            "SELECT m_sum(toInt64(number)), sum(toInt64(number)), count() FROM numbers(10)"
        )

        self.assertEqual(row, "45,45,10")


class TestUDAFDecorator(UDAFTestCase):
    def test_decorator_registers_under_class_name(self):
        @agg([INT64], INT64)
        class dec_sum:  # noqa: N801 - the class name is the SQL name
            def __init__(self):
                self.total = 0

            def update(self, value):
                self.total += value

            def merge(self, other):
                self.total += other.total

            def evaluate(self):
                return self.total

        self._registered.append("dec_sum")

        self.assertEqual(self.scalar("SELECT dec_sum(toInt64(number)) FROM numbers(5)"), "10")

    def test_decorator_name_override_and_class_still_usable(self):
        @agg([INT64], INT64, name="renamed_sum")
        class DecoratedSum:
            def __init__(self):
                self.total = 0

            def update(self, value):
                self.total += value

            def merge(self, other):
                self.total += other.total

            def evaluate(self):
                return self.total

        self._registered.append("renamed_sum")

        self.assertEqual(self.scalar("SELECT renamed_sum(toInt64(number)) FROM numbers(5)"), "10")

        # The decorator returns the class unchanged, so it is still a normal Python class.
        accumulator = DecoratedSum()
        accumulator.update(3)
        accumulator.update(4)
        self.assertEqual(accumulator.evaluate(), 7)


class TestUDAFTypeDeclaration(UDAFTestCase):
    def test_types_inferred_from_update_and_evaluate_annotations(self):
        self.register("inferred_sum", PySum)

        self.assertEqual(self.scalar("SELECT toTypeName(inferred_sum(toInt64(1)))"), '"Nullable(Int64)"')

    def test_explicit_types_override_inference(self):
        self.register("explicit_sum", PySum, [INT64], FLOAT64)

        self.assertEqual(
            self.scalar("SELECT toTypeName(explicit_sum(toInt64(1)))"), '"Nullable(Float64)"'
        )
        self.assertEqual(self.scalar("SELECT explicit_sum(toInt64(number)) FROM numbers(5)"), "10")

    def test_argument_type_mismatch_is_rejected(self):
        self.register("typed_sum", PySum, [INT64], INT64)

        with self.assertRaises(Exception) as ctx:
            self.session.query("SELECT typed_sum('not a number')", "CSV")
        self.assertIn("typed_sum", str(ctx.exception))
        self.assertIn("argument 1 type mismatch: expected Int64, got String", str(ctx.exception))

    def test_argument_count_mismatch_is_rejected(self):
        self.register("one_arg_sum", PySum, [INT64], INT64)

        with self.assertRaises(Exception) as ctx:
            self.session.query("SELECT one_arg_sum(toInt64(1), toInt64(2))", "CSV")
        self.assertIn("one_arg_sum", str(ctx.exception))
        self.assertIn("expected 1 arguments, got 2", str(ctx.exception))

    def test_non_class_factory_requires_explicit_types(self):
        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("lambda_sum_bad", lambda: PySum())
        self.assertIn("arg_types and return_type", str(ctx.exception))

    def test_zero_argument_accumulator_counts_rows(self):
        self.register("py_rows", PyRowCount, [], INT64)

        self.assertEqual(self.scalar("SELECT py_rows() FROM numbers(7)"), "7")

    def test_zero_argument_accumulator_from_a_non_class_factory(self):
        # An explicit empty arg_types list declares "no arguments"; it must not be read as
        # "arg_types was not given", which is what None means.
        self.register("py_rows_lambda", lambda: PyRowCount(), [], INT64)

        self.assertEqual(self.scalar("SELECT py_rows_lambda() FROM numbers(4)"), "4")

    def test_non_class_factory_works_with_explicit_types(self):
        self.register("lambda_sum", lambda: PySum(), [INT64], INT64)

        self.assertEqual(self.scalar("SELECT lambda_sum(toInt64(number)) FROM numbers(5)"), "10")

    def test_accumulator_without_required_methods_is_rejected(self):
        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("incomplete", NotAnAccumulator, [INT64], INT64)
        self.assertIn("has no 'merge' method", str(ctx.exception))

    def test_non_callable_accumulator_is_rejected(self):
        with self.assertRaises(RuntimeError):
            chdb.create_aggregate_function("not_callable", 42, [INT64], INT64)

    def test_arg_types_length_must_match_update_positional_parameters(self):
        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("arity_mismatch", PySum, [INT64, INT64], INT64)
        self.assertIn("arg_types has 2 elements but update() has 1 positional parameters",
                      str(ctx.exception))

    def test_arg_types_length_is_checked_for_variadic_update_too(self):
        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("variadic_mismatch", PyVariadicSum, [INT64], INT64)
        self.assertIn("arg_types has 1 elements but update() has 0 positional parameters",
                      str(ctx.exception))

    def test_builtin_aggregate_name_is_rejected(self):
        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("sum", PySum)
        self.assertIn("built-in aggregate function with that name already exists",
                      str(ctx.exception))

    def test_scalar_udf_name_is_rejected(self):
        # Ordinary functions resolve before aggregates, so a same-named scalar UDF would
        # take every call and the aggregate would never run.
        chdb.create_function("shadowing_udf", lambda x: x, [INT64], INT64)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                chdb.create_aggregate_function("shadowing_udf", PySum)
            self.assertIn("Python scalar UDF with that name already exists", str(ctx.exception))
        finally:
            chdb.drop_function("shadowing_udf")

    def test_scalar_udf_cannot_shadow_an_existing_udaf(self):
        self.register("shadowed_agg", PySum)

        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_function("shadowed_agg", lambda x: x, [INT64], INT64)
        self.assertIn("Python aggregate function with that name already exists", str(ctx.exception))

    def test_ordinary_function_name_is_rejected(self):
        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("abs", PySum)
        self.assertIn("ordinary function with that name already exists", str(ctx.exception))

    def test_parameters_are_rejected(self):
        self.register("nonparametric", PySum, [INT64], INT64)

        with self.assertRaises(Exception) as ctx:
            self.session.query("SELECT nonparametric(2)(toInt64(1))", "CSV")
        self.assertIn("nonparametric", str(ctx.exception))
        self.assertIn("is not parametric", str(ctx.exception))


class TestUDAFRegistry(UDAFTestCase):
    def test_duplicate_registration_raises(self):
        self.register("dup_sum", PySum)

        with self.assertRaises(RuntimeError) as ctx:
            chdb.create_aggregate_function("dup_sum", PySum)
        self.assertIn("already registered", str(ctx.exception))

    def test_dropped_function_stops_resolving(self):
        self.register("temp_sum", PySum)
        self.assertEqual(self.scalar("SELECT temp_sum(toInt64(number)) FROM numbers(5)"), "10")

        chdb.drop_aggregate_function("temp_sum")
        self._registered.remove("temp_sum")

        with self.assertRaises(Exception):
            self.session.query("SELECT temp_sum(toInt64(1))", "CSV")

    def test_dropping_unknown_function_is_a_no_op(self):
        chdb.drop_aggregate_function("never_registered_udaf")

    def test_udaf_name_appears_in_unknown_function_hints(self):
        self.register("hintable_sum", PySum)

        with self.assertRaises(Exception) as ctx:
            self.session.query("SELECT hintablesum(toInt64(1))", "CSV")
        self.assertIn("hintable_sum", str(ctx.exception))

    def test_scalar_udf_and_udaf_coexist(self):
        self.register("coexist_sum", PySum)
        chdb.create_function("coexist_double", lambda x: x * 2, [INT64], INT64)
        try:
            self.assertEqual(
                self.scalar("SELECT coexist_sum(coexist_double(toInt64(number))) FROM numbers(5)"),
                "20",
            )
        finally:
            chdb.drop_function("coexist_double")


class TestUDAFNullHandling(UDAFTestCase):
    def test_default_skips_rows_with_null_arguments(self):
        self.register("skip_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT skip_sum(v) FROM values('v Nullable(Int64)', (1), (NULL), (2), (NULL), (4))"
        )

        self.assertEqual(result, "7")

    def test_on_null_pass_delivers_none_to_update(self):
        self.register("pass_count", PyCountNonNull, [INT64], INT64, on_null="pass")

        result = self.scalar(
            "SELECT pass_count(v) FROM values('v Nullable(Int64)', (1), (NULL), (2), (NULL), (4))"
        )

        # 3 non-NULL values, 2 NULLs -> 3 * 1000 + 2
        self.assertEqual(result, "3002")

    def test_all_null_input_with_skip_finalizes_empty_accumulator(self):
        self.register("allnull_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT allnull_sum(v) FROM values('v Nullable(Int64)', (NULL), (NULL))"
        )

        self.assertEqual(result, "0")

    def test_all_null_input_with_pass_sees_every_null(self):
        self.register("allnull_count", PyCountNonNull, [INT64], INT64, on_null="pass")

        result = self.scalar(
            "SELECT allnull_count(v) FROM values('v Nullable(Int64)', (NULL), (NULL))"
        )

        self.assertEqual(result, "2")

    def test_on_null_pass_still_sees_nulls_under_the_if_combinator(self):
        # getOwnNullAdapter is only consulted on the outermost function, so relying on it
        # alone would let the -If wrapper strip NULL rows before Python sees them.
        self.register("pass_count_if", PyCountNonNull, [INT64], INT64, on_null="pass")

        result = self.scalar(
            "SELECT pass_count_ifIf(v, c) FROM "
            "values('v Nullable(Int64), c UInt8', (1, 1), (NULL, 1), (2, 0), (NULL, 0))"
        )

        # Only the two rows with c = 1 are aggregated: one value, one NULL.
        self.assertEqual(result, "1001")

    def test_on_null_pass_is_not_rewritten_into_the_if_combinator(self):
        # optimize_rewrite_aggregate_function_with_if turns f(if(cond, x, NULL)) into
        # fIf(x, cond), which drops the NULL rows. That rewrite is invalid for on_null="pass".
        self.register("pass_count_rw", PyCountNonNull, [INT64], INT64, on_null="pass")

        result = self.scalar(
            "SELECT pass_count_rw(if(number % 2 = 0, toInt64(number), NULL)) FROM numbers(10) "
            "SETTINGS optimize_rewrite_aggregate_function_with_if = 1"
        )

        # 5 even numbers become values, 5 odd ones become NULL and must still reach update().
        self.assertEqual(result, "5005")

    def test_on_null_pass_reports_nulls_per_argument(self):
        self.register("pass_pairs", PyPairCount, [INT64, INT64], INT64, on_null="pass")

        result = self.scalar(
            "SELECT pass_pairs(a, b) FROM values('a Nullable(Int64), b Nullable(Int64)', "
            "(1, 1), (NULL, 2), (3, NULL), (NULL, NULL))"
        )

        # 4 rows, 2 NULL on the left, 2 NULL on the right.
        self.assertEqual(result, "40202")

    def test_skip_drops_a_row_when_any_argument_is_null(self):
        self.register("skip_pairs", PyPairCount, [INT64, INT64], INT64)

        result = self.scalar(
            "SELECT skip_pairs(a, b) FROM values('a Nullable(Int64), b Nullable(Int64)', "
            "(1, 1), (NULL, 2), (3, NULL), (4, 4))"
        )

        # Only (1, 1) and (4, 4) survive, and neither argument is NULL in them.
        self.assertEqual(result, "20000")

    def test_literal_null_argument_matches_builtin_for_skip_mode(self):
        # An argument that is only ever NULL short-circuits to NULL, exactly as it does for
        # a built-in NULL-skipping aggregate.
        self.register("literal_null_sum", PySum, [INT64], INT64)

        self.assertEqual(self.scalar("SELECT literal_null_sum(NULL)"),
                         self.scalar("SELECT sum(NULL)"))
        self.assertEqual(self.scalar("SELECT literal_null_sum(NULL)"), r"\N")

    def test_literal_null_argument_reaches_update_in_pass_mode(self):
        self.register("literal_null_pass", PyCountNonNull, [INT64], INT64, on_null="pass")

        # One row, one None seen.
        self.assertEqual(self.scalar("SELECT literal_null_pass(NULL)"), "1")

    def test_or_null_returns_null_when_every_row_was_skipped(self):
        # -OrNull decides from the Null adapter whether any row contributed. With on_null
        # ="skip" the adapter must stay in place, or the flag is set for rows we dropped.
        self.register("ornull_sum", PySum, [INT64], INT64)

        py_result = self.scalar(
            "SELECT ornull_sumOrNull(v) FROM values('v Nullable(Int64)', (NULL), (NULL))"
        )
        builtin_result = self.scalar(
            "SELECT sumOrNull(v) FROM values('v Nullable(Int64)', (NULL), (NULL))"
        )

        self.assertEqual(py_result, r"\N")
        self.assertEqual(py_result, builtin_result)

    def test_or_null_returns_a_value_when_some_row_contributed(self):
        self.register("ornull_sum2", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT ornull_sum2OrNull(v) FROM values('v Nullable(Int64)', (NULL), (4), (NULL))"
        )

        self.assertEqual(result, "4")

    def test_aggregate_functions_null_for_empty_returns_null_for_all_null_input(self):
        # The setting rewrites the call into the -OrNull form automatically.
        self.register("nfe_sum", PySum, [INT64], INT64)

        py_result = self.scalar(
            "SELECT nfe_sum(v) FROM values('v Nullable(Int64)', (NULL), (NULL)) "
            "SETTINGS aggregate_functions_null_for_empty = 1"
        )
        builtin_result = self.scalar(
            "SELECT sum(v) FROM values('v Nullable(Int64)', (NULL), (NULL)) "
            "SETTINGS aggregate_functions_null_for_empty = 1"
        )

        self.assertEqual(py_result, r"\N")
        self.assertEqual(py_result, builtin_result)

    def test_on_null_pass_or_null_counts_the_null_rows_as_contributions(self):
        # In "pass" mode every row genuinely reaches the accumulator, so -OrNull must not
        # report the group as empty.
        self.register("ornull_pass", PyCountNonNull, [INT64], INT64, on_null="pass")

        result = self.scalar(
            "SELECT ornull_passOrNull(v) FROM values('v Nullable(Int64)', (NULL), (NULL))"
        )

        self.assertEqual(result, "2")

    def test_nullable_result_type_for_nullable_argument(self):
        self.register("nullable_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT toTypeName(nullable_sum(v)) FROM values('v Nullable(Int64)', (1))"
        )

        self.assertEqual(result, '"Nullable(Int64)"')


class TestUDAFErrorHandling(UDAFTestCase):
    def test_update_exception_propagates_by_default(self):
        self.register("raise_sum", PyRaisingUpdate, [INT64], INT64)

        with self.assertRaises(Exception) as ctx:
            self.session.query("SELECT raise_sum(toInt64(number)) FROM numbers(5)", "CSV")
        message = str(ctx.exception)
        self.assertIn("raise_sum", message)
        self.assertIn("boom at 3", message)

    def test_on_error_ignore_drops_only_the_failing_row(self):
        self.register("ignore_sum", PyRaisingUpdate, [INT64], INT64, on_error="ignore")

        # numbers 0..4 sum to 10; row with value 3 raises and is dropped.
        self.assertEqual(self.scalar("SELECT ignore_sum(toInt64(number)) FROM numbers(5)"), "7")

    def test_on_error_ignore_does_not_roll_back_a_partial_update(self):
        # "ignore" suppresses the exception and moves to the next row; it cannot undo what
        # update() already changed, because rolling back would mean copying the accumulator
        # on every row. This pins the documented contract.
        self.register("partial_sum", PyMutateThenRaise, [INT64], INT64, on_error="ignore")

        # 0..4 sum to 10; row 3 raises *after* adding itself, so its contribution remains.
        self.assertEqual(self.scalar("SELECT partial_sum(toInt64(number)) FROM numbers(5)"), "10")

    def test_evaluate_exception_propagates(self):
        self.register("eval_raise", PyRaisingEvaluate, [INT64], INT64, on_error="ignore")

        with self.assertRaises(Exception) as ctx:
            self.session.query("SELECT eval_raise(toInt64(number)) FROM numbers(5)", "CSV")
        message = str(ctx.exception)
        self.assertIn("eval_raise", message)
        self.assertIn("evaluate exploded", message)

    def test_merge_exception_propagates(self):
        self.register("merge_raise", PyRaisingMerge, [INT64], INT64)

        with self.assertRaises(Exception) as ctx:
            self.session.query(
                "SELECT merge_raiseMerge(s) FROM ("
                "  SELECT merge_raiseState(toInt64(number)) AS s FROM numbers(10) GROUP BY number % 2"
                ")",
                "CSV",
            )
        self.assertIn("merge exploded", str(ctx.exception))


class TestUDAFParallelMerge(UDAFTestCase):
    def test_parallel_ungrouped_aggregation_merges_partial_states(self):
        self.register("mt_sum", PySum)

        expected = sum(range(200000))
        result = self.scalar(
            "SELECT mt_sum(toInt64(number)) FROM numbers_mt(200000) SETTINGS max_threads = 4"
        )

        self.assertEqual(int(result), expected)

    def test_parallel_group_by_matches_builtin_for_every_group(self):
        self.register("mt_group_sum", PySum)

        py_rows = self.rows(
            "SELECT number % 7 AS k, mt_group_sum(toInt64(number)) FROM numbers_mt(100000) "
            "GROUP BY k ORDER BY k SETTINGS max_threads = 4"
        )
        builtin_rows = self.rows(
            "SELECT number % 7 AS k, sum(toInt64(number)) FROM numbers_mt(100000) "
            "GROUP BY k ORDER BY k SETTINGS max_threads = 4"
        )

        self.assertEqual(len(py_rows), 7)
        self.assertEqual(py_rows, builtin_rows)

    def test_high_cardinality_group_by_matches_builtin(self):
        self.register("hc_sum", PySum)

        py_rows = self.rows(
            "SELECT k, hc_sum(v) FROM ("
            "  SELECT number % 5000 AS k, toInt64(number) AS v FROM numbers_mt(100000)"
            ") GROUP BY k ORDER BY k LIMIT 5 SETTINGS max_threads = 4"
        )
        builtin_rows = self.rows(
            "SELECT k, sum(v) FROM ("
            "  SELECT number % 5000 AS k, toInt64(number) AS v FROM numbers_mt(100000)"
            ") GROUP BY k ORDER BY k LIMIT 5 SETTINGS max_threads = 4"
        )

        self.assertEqual(len(py_rows), 5)
        self.assertEqual(py_rows, builtin_rows)

    def test_uint8_group_key_matches_builtin(self):
        self.register("u8_sum", PySum)

        py_rows = self.rows(
            "SELECT k, u8_sum(v) FROM ("
            "  SELECT toUInt8(number % 251) AS k, toInt64(number) AS v FROM numbers(50000)"
            ") GROUP BY k ORDER BY k LIMIT 4"
        )
        builtin_rows = self.rows(
            "SELECT k, sum(v) FROM ("
            "  SELECT toUInt8(number % 251) AS k, toInt64(number) AS v FROM numbers(50000)"
            ") GROUP BY k ORDER BY k LIMIT 4"
        )

        self.assertEqual(len(py_rows), 4)
        self.assertEqual(py_rows, builtin_rows)


class TestUDAFUpdateBatch(UDAFTestCase):
    def test_update_batch_result_matches_per_row_update(self):
        self.register("batch_sum", PySumBatch, [INT64], INT64)
        self.register("row_sum", PySum, [INT64], INT64)

        batch_result = self.scalar("SELECT batch_sum(toInt64(number)) FROM numbers(10000)")
        row_result = self.scalar("SELECT row_sum(toInt64(number)) FROM numbers(10000)")

        self.assertEqual(batch_result, str(sum(range(10000))))
        self.assertEqual(batch_result, row_result)

    def test_ungrouped_aggregation_uses_update_batch_not_update(self):
        self.register("probe_ungrouped", PySumBatchProbe, [INT64], INT64)

        # evaluate() encodes batch_calls * 1000000 + update_calls.
        encoded = int(self.scalar("SELECT probe_ungrouped(toInt64(number)) FROM numbers(1000)"))

        self.assertGreaterEqual(encoded // 1000000, 1)
        self.assertEqual(encoded % 1000000, 0)

    def test_group_by_with_interleaved_keys_uses_per_row_update(self):
        self.register("probe_grouped", PySumBatchProbe, [INT64], INT64)

        rows = self.rows(
            "SELECT k, probe_grouped(v) FROM ("
            "  SELECT number % 2 AS k, toInt64(number) AS v FROM numbers(1000)"
            ") GROUP BY k ORDER BY k"
        )

        self.assertEqual(len(rows), 2)
        for row in rows:
            encoded = int(row.split(",")[1])
            self.assertEqual(encoded // 1000000, 0)
            self.assertEqual(encoded % 1000000, 500)

    def test_on_error_ignore_disables_the_batch_path(self):
        self.register("probe_ignore", PySumBatchProbe, [INT64], INT64, on_error="ignore")

        encoded = int(self.scalar("SELECT probe_ignore(toInt64(number)) FROM numbers(1000)"))

        self.assertEqual(encoded // 1000000, 0)
        self.assertEqual(encoded % 1000000, 1000)

    def test_update_batch_receives_one_list_per_argument(self):
        self.register("batch_pairs", PyPairBatch, [FLOAT64, FLOAT64], FLOAT64)

        result = self.scalar(
            "SELECT batch_pairs(v, w) FROM values('v Float64, w Float64', "
            "(1.0, 2.0), (3.0, 4.0), (5.0, 6.0))"
        )

        self.assertEqual(float(result), 1.0 * 2.0 + 3.0 * 4.0 + 5.0 * 6.0)

    def test_update_batch_honours_null_skipping(self):
        self.register("batch_null_sum", PySumBatch, [INT64], INT64)

        result = self.scalar(
            "SELECT batch_null_sum(v) FROM values('v Nullable(Int64)', (1), (NULL), (2), (NULL), (4))"
        )

        self.assertEqual(result, "7")


class TestUDAFCombinators(UDAFTestCase):
    def test_if_combinator_filters_rows(self):
        self.register("comb_sum", PySum)

        py_result = self.scalar(
            "SELECT comb_sumIf(toInt64(number), number % 2 = 0) FROM numbers(10)"
        )
        builtin_result = self.scalar(
            "SELECT sumIf(toInt64(number), number % 2 = 0) FROM numbers(10)"
        )

        self.assertEqual(py_result, "20")
        self.assertEqual(py_result, builtin_result)

    def test_if_combinator_inside_group_by(self):
        self.register("gcomb_sum", PySum)

        py_rows = self.rows(
            "SELECT number % 3 AS k, gcomb_sumIf(toInt64(number), number % 2 = 0) "
            "FROM numbers(30) GROUP BY k ORDER BY k"
        )
        builtin_rows = self.rows(
            "SELECT number % 3 AS k, sumIf(toInt64(number), number % 2 = 0) "
            "FROM numbers(30) GROUP BY k ORDER BY k"
        )

        self.assertEqual(len(py_rows), 3)
        self.assertEqual(py_rows, builtin_rows)

    def test_array_combinator_flattens_arrays(self):
        self.register("arr_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT arr_sumArray(a) FROM values('a Array(Int64)', ([1, 2]), ([3]), ([4, 5, 6]))"
        )

        self.assertEqual(result, "21")

    def test_distinct_combinator_deduplicates(self):
        self.register("dist_sum", PySum, [INT64], INT64)

        py_result = self.scalar(
            "SELECT dist_sum(DISTINCT v) FROM values('v Int64', (1), (1), (2), (2), (3))"
        )
        builtin_result = self.scalar(
            "SELECT sum(DISTINCT v) FROM values('v Int64', (1), (1), (2), (2), (3))"
        )

        self.assertEqual(py_result, "6")
        self.assertEqual(py_result, builtin_result)

    def test_array_reduce_resolves_the_udaf_by_name(self):
        self.register("reduce_sum", PySum, [INT64], INT64)

        result = self.scalar("SELECT arrayReduce('reduce_sum', [toInt64(1), toInt64(2), toInt64(4)])")

        self.assertEqual(result, "7")

    def test_rewrite_of_sum_if_null_pattern_still_resolves(self):
        # optimize_rewrite_aggregate_function_with_if rewrites this into rif_sumIf(...),
        # which must still resolve through the UDAF registry.
        self.register("rif_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT rif_sum(if(number % 2 = 0, toInt64(number), NULL)) FROM numbers(10) "
            "SETTINGS optimize_rewrite_aggregate_function_with_if = 1"
        )

        self.assertEqual(result, "20")


class TestUDAFStateSerialization(UDAFTestCase):
    def test_state_and_merge_combinators_round_trip_in_memory(self):
        self.register("st_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT st_sumMerge(s) FROM ("
            "  SELECT st_sumState(toInt64(number)) AS s FROM numbers(100) GROUP BY number % 4"
            ")"
        )

        self.assertEqual(result, "4950")

    def test_aggregate_function_column_round_trips_through_a_table(self):
        self.register("tbl_sum", PySum, [INT64], INT64)

        self.session.query("CREATE DATABASE IF NOT EXISTS udaf_db")
        self.session.query(
            "CREATE TABLE udaf_db.states (k Int64, s AggregateFunction(tbl_sum, Int64)) "
            "ENGINE = MergeTree ORDER BY k"
        )
        self.session.query(
            "INSERT INTO udaf_db.states "
            "SELECT number % 3 AS k, tbl_sumState(toInt64(number)) FROM numbers(30) GROUP BY k"
        )

        py_rows = self.rows("SELECT k, tbl_sumMerge(s) FROM udaf_db.states GROUP BY k ORDER BY k")
        builtin_rows = self.rows(
            "SELECT number % 3 AS k, sum(toInt64(number)) FROM numbers(30) GROUP BY k ORDER BY k"
        )

        self.assertEqual(py_rows, ["0,135", "1,145", "2,155"])
        self.assertEqual(py_rows, builtin_rows)

    def test_state_round_trip_preserves_a_non_numeric_accumulator(self):
        self.register("cat_concat", PyConcat, [INT64], STRING)

        self.session.query("CREATE DATABASE IF NOT EXISTS udaf_db2")
        self.session.query(
            "CREATE TABLE udaf_db2.states (s AggregateFunction(cat_concat, Int64)) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )
        self.session.query(
            "INSERT INTO udaf_db2.states SELECT cat_concatState(toInt64(number)) FROM numbers(5)"
        )

        result = self.scalar("SELECT cat_concatMerge(s) FROM udaf_db2.states")

        self.assertEqual(result, '"0,1,2,3,4"')

    def test_cast_to_aggregate_function_type_parses_the_udaf_name(self):
        # The CAST target type is parsed from text, so AggregateFunction(cast_sum, Int64)
        # has to resolve `cast_sum` through the registry rather than the builtin map.
        # This is the same lookup external aggregation performs when it re-reads a spilled
        # block, and the one AggregatingMergeTree performs on attach.
        self.register("cast_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT cast_sumMerge(CAST(s AS AggregateFunction(cast_sum, Int64))) FROM ("
            "  SELECT cast_sumState(toInt64(number)) AS s FROM numbers(10)"
            ")"
        )

        self.assertEqual(result, "45")

    def test_state_of_an_empty_group_round_trips(self):
        # serialize() runs on a state that never received a row; deserialize() has to turn it
        # back into something evaluate() can finalize.
        self.register("empty_state_sum", PySum, [INT64], INT64)

        result = self.scalar(
            "SELECT empty_state_sumMerge(s) FROM ("
            "  SELECT empty_state_sumState(v) AS s FROM values('v Int64', (1)) WHERE v = 999"
            ")"
        )

        self.assertEqual(result, "0")

    def test_unpicklable_accumulator_reports_a_clear_error(self):
        self.register("unpicklable_sum", PyUnpicklable, [INT64], INT64)

        self.session.query("CREATE DATABASE IF NOT EXISTS udaf_db3")
        self.session.query(
            "CREATE TABLE udaf_db3.states (s AggregateFunction(unpicklable_sum, Int64)) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )

        with self.assertRaises(Exception) as ctx:
            self.session.query(
                "INSERT INTO udaf_db3.states "
                "SELECT unpicklable_sumState(toInt64(number)) FROM numbers(5)",
                "CSV",
            )
        message = str(ctx.exception)
        self.assertIn("unpicklable_sum", message)
        self.assertIn("could not be pickled", message)

    def test_group_by_with_external_aggregation_settings_matches_builtin(self):
        # Runs a group-by that crosses the two-level conversion threshold with external
        # aggregation enabled. Whether the engine actually spills depends on its memory
        # accounting, so this asserts results only; the serialize/deserialize round trip
        # itself is covered by the AggregateFunction-column tests above.
        self.register("spill_sum", PySum)

        py_rows = self.rows(
            "SELECT k, spill_sum(v) FROM ("
            "  SELECT number % 2000 AS k, toInt64(number) AS v FROM numbers(20000)"
            ") GROUP BY k ORDER BY k LIMIT 3 "
            "SETTINGS max_bytes_before_external_group_by = 1, "
            "max_bytes_ratio_before_external_group_by = 0, "
            "group_by_two_level_threshold = 100, group_by_two_level_threshold_bytes = 1"
        )
        builtin_rows = self.rows(
            "SELECT k, sum(v) FROM ("
            "  SELECT number % 2000 AS k, toInt64(number) AS v FROM numbers(20000)"
            ") GROUP BY k ORDER BY k LIMIT 3"
        )

        self.assertEqual(py_rows, ["0,90000", "1,90010", "2,90020"])
        self.assertEqual(py_rows, builtin_rows)


class TestUDAFQueryShapes(UDAFTestCase):
    def test_having_on_udaf_result(self):
        self.register("hav_sum", PySum)

        rows = self.rows(
            "SELECT number % 4 AS k, hav_sum(toInt64(number)) AS s FROM numbers(100) "
            "GROUP BY k HAVING s > 1225 ORDER BY k"
        )

        self.assertEqual(rows, ["2,1250", "3,1275"])

    def test_order_by_udaf_result(self):
        self.register("ord_sum", PySum)

        rows = self.rows(
            "SELECT number % 4 AS k, ord_sum(toInt64(number)) AS s FROM numbers(100) "
            "GROUP BY k ORDER BY s DESC"
        )

        self.assertEqual(rows, ["3,1275", "2,1250", "1,1225", "0,1200"])

    def test_udaf_inside_a_subquery_feeding_another_aggregate(self):
        self.register("nested_sum", PySum)

        result = self.scalar(
            "SELECT max(s) FROM ("
            "  SELECT number % 4 AS k, nested_sum(toInt64(number)) AS s FROM numbers(100) GROUP BY k"
            ")"
        )

        self.assertEqual(result, "1275")

    def test_group_by_with_rollup(self):
        self.register("rollup_sum", PySum)

        py_rows = self.rows(
            "SELECT number % 2 AS k, rollup_sum(toInt64(number)) FROM numbers(10) "
            "GROUP BY k WITH ROLLUP ORDER BY k, 2"
        )
        builtin_rows = self.rows(
            "SELECT number % 2 AS k, sum(toInt64(number)) FROM numbers(10) "
            "GROUP BY k WITH ROLLUP ORDER BY k, 2"
        )

        self.assertEqual(len(py_rows), 3)
        self.assertEqual(py_rows, builtin_rows)

    def test_window_function_over_a_growing_frame(self):
        self.register("win_sum", PySum)

        py_rows = self.rows(
            "SELECT number, win_sum(toInt64(number)) OVER "
            "(ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(5)"
        )
        builtin_rows = self.rows(
            "SELECT number, sum(toInt64(number)) OVER "
            "(ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(5)"
        )

        self.assertEqual(py_rows, ["0,0", "1,1", "2,3", "3,6", "4,10"])
        self.assertEqual(py_rows, builtin_rows)

    def test_query_result_cache_treats_the_udaf_as_non_deterministic(self):
        self.register("qc_sum", PySum)

        with self.assertRaises(Exception) as ctx:
            self.session.query(
                "SELECT qc_sum(toInt64(number)) FROM numbers(5) SETTINGS use_query_cache = 1, "
                "query_cache_nondeterministic_function_handling = 'throw'",
                "CSV",
            )
        self.assertIn("non-deterministic", str(ctx.exception))

    def test_query_result_cache_sees_a_udaf_named_by_arrayreduce(self):
        # arrayReduce names the aggregate in a string literal, so the cache matcher has to
        # look inside it; the AST function name is arrayReduce, which is deterministic.
        self.register("qc_reduce_sum", PySum, [INT64], INT64)

        with self.assertRaises(Exception) as ctx:
            self.session.query(
                "SELECT arrayReduce('qc_reduce_sum', [toInt64(1), toInt64(2)]) "
                "SETTINGS use_query_cache = 1, "
                "query_cache_nondeterministic_function_handling = 'throw'",
                "CSV",
            )
        self.assertIn("non-deterministic", str(ctx.exception))

    def test_group_by_use_nulls_with_rollup(self):
        self.register("gbun_sum", PySum)

        py_rows = self.rows(
            "SELECT number % 2 AS k, gbun_sum(k) FROM numbers(10) "
            "GROUP BY k WITH ROLLUP ORDER BY k, 2 SETTINGS group_by_use_nulls = 1"
        )
        builtin_rows = self.rows(
            "SELECT number % 2 AS k, sum(k) FROM numbers(10) "
            "GROUP BY k WITH ROLLUP ORDER BY k, 2 SETTINGS group_by_use_nulls = 1"
        )

        self.assertEqual(len(py_rows), 3)
        self.assertEqual(py_rows, builtin_rows)


if __name__ == "__main__":
    unittest.main()
