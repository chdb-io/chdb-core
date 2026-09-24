#!python3
"""Tests for the polars output format.

Covers the eager path (``chdb.query(sql, "polars")``, ``conn.query``,
``conn.send_query``, ``conn.pl``/``Session.pl``) and the lazy path
(``conn.pl(sql, lazy=True)``), which registers a polars IO plugin that folds
projection, LIMIT and predicates into the SQL chdb executes.
"""

import datetime
import decimal
import unittest

import chdb
from chdb import session as chs

try:
    import polars as pl
    from polars.testing import assert_frame_equal
except ImportError:
    pl = None

from chdb.polars_io import _plan_query, _predicate_to_sql, chdb_source, to_polars


SAMPLE_SQL = """
    SELECT * FROM (
        SELECT 1 AS id, 'Alice' AS name, 9.5 AS score
        UNION ALL SELECT 2, '宝贝', 7.25
        UNION ALL SELECT 3, NULL, -1.5
    ) ORDER BY id
"""

EXPECTED_ROWS = [
    {"id": 1, "name": "Alice", "score": 9.5},
    {"id": 2, "name": "宝贝", "score": 7.25},
    {"id": 3, "name": None, "score": -1.5},
]


class _RecordingConnection:
    """Connection proxy that records the SQL the IO plugin actually sends."""

    def __init__(self, conn):
        self._conn = conn
        self.statements = []

    def query(self, sql, format="CSV", params=None):
        self.statements.append(sql)
        return self._conn.query(sql, format, params=params)

    def send_query(self, sql, format="CSV", params=None):
        self.statements.append(sql)
        return self._conn.send_query(sql, format, params=params)

    @property
    def data_statements(self):
        """Statements other than the ``LIMIT 0`` schema probe."""
        return [s for s in self.statements if not s.endswith("LIMIT 0")]


@unittest.skipIf(pl is None, "polars not installed")
class TestPolarsOutputFormat(unittest.TestCase):
    def test_module_query_polars_returns_exact_columns_values_and_order(self):
        df = chdb.query(SAMPLE_SQL, "polars")
        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(df.columns, ["id", "name", "score"])
        self.assertEqual(df.to_dicts(), EXPECTED_ROWS)

    def test_polars_format_is_case_insensitive(self):
        self.assertEqual(
            chdb.query(SAMPLE_SQL, "Polars").to_dicts(),
            chdb.query(SAMPLE_SQL, "polars").to_dicts(),
        )

    def test_connection_query_polars_matches_arrowtable_path(self):
        conn = chdb.connect(":memory:")
        try:
            # arrowtable path (pyarrow) and polars path must agree on values.
            table = conn.query(SAMPLE_SQL, "arrowtable")
            df = conn.query(SAMPLE_SQL, "polars")
            self.assertEqual(df.columns, table.column_names)
            self.assertEqual(df.to_dicts(), table.to_pylist())
        finally:
            conn.close()

    def test_connection_pl_eager_equals_query_polars(self):
        conn = chdb.connect(":memory:")
        try:
            assert_frame_equal(conn.pl(SAMPLE_SQL), conn.query(SAMPLE_SQL, "polars"))
        finally:
            conn.close()

    def test_session_pl_and_polars_format_agree(self):
        with chs.Session() as session:
            session.query("CREATE DATABASE IF NOT EXISTS pl_out ENGINE = Atomic")
            session.query(
                "CREATE TABLE pl_out.t (a UInt32, b String) ENGINE = MergeTree ORDER BY a"
            )
            session.query("INSERT INTO pl_out.t VALUES (1, 'x'), (2, 'y')")
            expected = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
            self.assertEqual(
                session.query("SELECT * FROM pl_out.t ORDER BY a", "polars").to_dicts(),
                expected,
            )
            self.assertEqual(
                session.pl("SELECT * FROM pl_out.t ORDER BY a").to_dicts(), expected
            )

    def test_send_query_polars_yields_frames_covering_every_row(self):
        conn = chdb.connect(":memory:")
        try:
            stream = conn.send_query(
                "SELECT number AS n FROM numbers(100000) SETTINGS max_block_size = 8192",
                "polars",
            )
            frames = list(stream)
            self.assertTrue(frames)
            for frame in frames:
                self.assertIsInstance(frame, pl.DataFrame)
                self.assertEqual(frame.columns, ["n"])
            combined = pl.concat(frames)
            self.assertEqual(combined.height, 100000)
            self.assertEqual(combined["n"].sum(), 100000 * 99999 // 2)
        finally:
            conn.close()

    def test_send_query_polars_does_not_offer_record_batches(self):
        # The chunks are already polars frames, so the pyarrow reader on the
        # stream has nothing to read; it must say so instead of failing later.
        conn = chdb.connect(":memory:")
        try:
            stream = conn.send_query("SELECT 1", "polars")
            with self.assertRaisesRegex(ValueError, "arrow format"):
                stream.record_batch()
            stream.close()
        finally:
            conn.close()

    def test_empty_result_keeps_column_names_and_dtypes(self):
        df = chdb.query("SELECT 1 AS a, 'x' AS b WHERE 0", "polars")
        self.assertEqual(df.height, 0)
        self.assertEqual(df.columns, ["a", "b"])
        self.assertEqual(df.dtypes, [pl.UInt8, pl.String])

    def test_statement_without_result_set_returns_empty_frame(self):
        conn = chdb.connect(":memory:")
        try:
            df = conn.query(
                "CREATE TABLE no_rs (a UInt8) ENGINE = Memory", "polars"
            )
            self.assertIsInstance(df, pl.DataFrame)
            self.assertEqual(df.shape, (0, 0))
        finally:
            conn.close()

    def test_to_polars_on_non_arrow_result_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "Arrow"):
            to_polars(chdb.query("SELECT 1", "CSV"))

    def test_polars_module_does_not_import_pyarrow(self):
        # The capsule is exported from C++, so the polars path must not pull in
        # pyarrow; tests/test_query_without_pandas_pyarrow.py relies on chdb
        # staying usable without it.
        import ast
        import pathlib

        import chdb.polars_io as polars_io

        tree = ast.parse(pathlib.Path(polars_io.__file__).read_text(encoding="utf-8"))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
        self.assertTrue(imported)
        self.assertFalse({name for name in imported if name.split(".")[0] == "pyarrow"})

    def test_polars_roundtrips_temporal_decimal_and_nullable_types(self):
        df = chdb.query(
            """
            SELECT toDate('2020-01-02')                             AS d,
                   toDateTime64('2020-01-02 03:04:05.000001', 6, 'UTC') AS t,
                   toDecimal64('1.25', 2)                           AS dec,
                   CAST(NULL AS Nullable(Int64))                    AS n,
                   ['a', 'b']                                       AS arr
            """,
            "polars",
        )
        row = df.row(0, named=True)
        self.assertEqual(row["d"], datetime.date(2020, 1, 2))
        self.assertEqual(
            row["t"],
            datetime.datetime(2020, 1, 2, 3, 4, 5, 1, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(row["dec"], decimal.Decimal("1.25"))
        self.assertIsNone(row["n"])
        self.assertEqual(row["arr"], ["a", "b"])


@unittest.skipIf(pl is None, "polars not installed")
class TestPolarsLazySource(unittest.TestCase):
    BASE = "SELECT number AS n, toString(number) AS s FROM numbers(1000)"

    def setUp(self):
        self.conn = chdb.connect(":memory:")
        self.recorder = _RecordingConnection(self.conn)

    def tearDown(self):
        self.conn.close()

    def lazy(self, sql=None):
        return chdb_source(self.recorder, sql or self.BASE)

    def test_lazy_frame_reports_schema_without_reading_rows(self):
        lf = self.lazy()
        self.assertEqual(
            dict(lf.collect_schema()), {"n": pl.UInt64, "s": pl.String}
        )
        self.assertEqual(self.recorder.data_statements, [])

    def test_collect_without_pushdown_returns_every_row(self):
        df = self.lazy().collect()
        self.assertEqual(df.height, 1000)
        self.assertEqual(df.columns, ["n", "s"])
        self.assertEqual(df["n"].sum(), 1000 * 999 // 2)

    def test_projection_is_pushed_into_the_select_list(self):
        df = self.lazy().select("s").collect()
        self.assertEqual(df.columns, ["s"])
        self.assertEqual(df.height, 1000)
        sql = self.recorder.data_statements[-1]
        self.assertIn('SELECT "s" FROM', sql)
        self.assertNotIn('"n"', sql)

    def test_predicate_is_pushed_as_a_where_clause(self):
        df = self.lazy().filter(pl.col("n") > 996).collect()
        self.assertEqual(df["n"].to_list(), [997, 998, 999])
        sql = self.recorder.data_statements[-1]
        self.assertIn('WHERE ("n" > 996)', sql)

    def test_limit_is_pushed_as_a_limit_clause(self):
        df = self.lazy().head(3).collect()
        self.assertEqual(df["n"].to_list(), [0, 1, 2])
        self.assertIn("LIMIT 3", self.recorder.data_statements[-1])

    def test_projection_predicate_and_limit_combine_correctly(self):
        df = self.lazy().filter(pl.col("n") >= 10).select("s").head(2).collect()
        self.assertEqual(df.columns, ["s"])
        self.assertEqual(df["s"].to_list(), ["10", "11"])
        self.assertIn('WHERE ("n" >= 10)', self.recorder.data_statements[-1])

    def test_untranslatable_predicate_is_applied_in_polars(self):
        # str.starts_with has no entry in the translator, so no WHERE may be
        # emitted -- and the answer must still be exactly right.
        df = self.lazy().filter(pl.col("s").str.starts_with("99")).collect()
        expected = [n for n in range(1000) if str(n).startswith("99")]
        self.assertEqual(df["n"].to_list(), expected)
        self.assertNotIn("WHERE", self.recorder.data_statements[-1])

    def test_untranslatable_predicate_combined_with_projection(self):
        # 's' is only read by the fallback filter, 'n' is the only column the
        # caller wants back.
        df = self.lazy().filter(pl.col("s").str.starts_with("99")).select("n").collect()
        self.assertEqual(df.columns, ["n"])
        self.assertEqual(
            df["n"].to_list(), [n for n in range(1000) if str(n).startswith("99")]
        )

    def test_untranslatable_predicate_still_honours_the_row_limit(self):
        df = self.lazy().filter(pl.col("s").str.starts_with("1")).head(4).collect()
        expected = [n for n in range(1000) if str(n).startswith("1")][:4]
        self.assertEqual(df["n"].to_list(), expected)

    def test_lazy_result_matches_the_eager_result(self):
        lazy = self.lazy().filter(pl.col("n") % 7 == 0).select("n").collect()
        eager = self.conn.pl(
            f'SELECT "n" FROM ({self.BASE}) WHERE n % 7 = 0'
        )
        assert_frame_equal(lazy, eager)

    def test_empty_lazy_result_keeps_the_declared_schema(self):
        df = self.lazy().filter(pl.col("n") > 10_000).collect()
        self.assertEqual(df.height, 0)
        self.assertEqual(df.columns, ["n", "s"])
        self.assertEqual(df.dtypes, [pl.UInt64, pl.String])

    def test_empty_lazy_result_honours_projection(self):
        df = self.lazy().filter(pl.col("n") > 10_000).select("s").collect()
        self.assertEqual(df.height, 0)
        self.assertEqual(df.columns, ["s"])

    def test_wrapping_the_query_preserves_its_order_by(self):
        # The base query becomes a subquery; a user's ORDER BY has to survive
        # that, or head()/collect() would hand back arbitrary rows.
        base = "SELECT number AS n FROM numbers(200000) ORDER BY n DESC"
        lf = chdb_source(self.recorder, base)
        self.assertEqual(lf.collect()["n"].to_list(), list(range(199999, -1, -1)))
        self.assertEqual(
            lf.head(5).collect()["n"].to_list(), [199999, 199998, 199997, 199996, 199995]
        )

    def test_collect_twice_reruns_the_query(self):
        lf = self.lazy()
        first = lf.collect()
        before = len(self.recorder.data_statements)
        second = lf.collect()
        assert_frame_equal(first, second)
        self.assertGreater(len(self.recorder.data_statements), before)

    def test_trailing_semicolon_is_accepted(self):
        df = chdb_source(self.recorder, "SELECT 1 AS a;  ").collect()
        self.assertEqual(df.to_dicts(), [{"a": 1}])

    def test_query_parameters_reach_every_statement(self):
        lf = chdb_source(
            self.recorder,
            "SELECT number AS n FROM numbers(10) WHERE number > {lo:UInt64}",
            params={"lo": 7},
        )
        self.assertEqual(lf.collect()["n"].to_list(), [8, 9])

    def test_connection_pl_lazy_returns_a_lazyframe(self):
        lf = self.conn.pl(self.BASE, lazy=True)
        self.assertIsInstance(lf, pl.LazyFrame)
        self.assertEqual(lf.filter(pl.col("n") > 997).collect()["n"].to_list(), [998, 999])

    def test_session_pl_lazy_returns_a_lazyframe(self):
        with chs.Session() as session:
            lf = session.pl("SELECT number AS n FROM numbers(5)", lazy=True)
            self.assertIsInstance(lf, pl.LazyFrame)
            self.assertEqual(lf.collect()["n"].to_list(), [0, 1, 2, 3, 4])

    def test_quoted_identifiers_survive_projection_pushdown(self):
        lf = chdb_source(self.recorder, "SELECT 1 AS \"we'ird col\", 2 AS b")
        df = lf.select("we'ird col").collect()
        self.assertEqual(df.columns, ["we'ird col"])
        self.assertEqual(df.to_dicts(), [{"we'ird col": 1}])


@unittest.skipIf(pl is None, "polars not installed")
class TestLazyQueryPlanning(unittest.TestCase):
    """_plan_query decides what reaches SQL.

    Tested directly because which combinations polars pushes down is up to its
    optimizer and varies between versions, while every combination the plugin
    protocol allows has to be handled correctly.
    """

    BASE = "SELECT 1"

    def plan(self, with_columns=None, predicate=None, n_rows=None, batch_size=None):
        return _plan_query(self.BASE, with_columns, predicate, n_rows, batch_size)

    def test_nothing_pushed_selects_everything(self):
        sql, fallback = self.plan()
        self.assertEqual(sql, "SELECT * FROM (SELECT 1)")
        self.assertIsNone(fallback)

    def test_projection_quotes_every_column(self):
        sql, fallback = self.plan(with_columns=["a", 'we"ird'])
        self.assertEqual(sql, 'SELECT "a", "we""ird" FROM (SELECT 1)')
        self.assertIsNone(fallback)

    def test_translated_predicate_becomes_a_where_clause(self):
        sql, fallback = self.plan(predicate=pl.col("a") > 1)
        self.assertEqual(sql, 'SELECT * FROM (SELECT 1) WHERE ("a" > 1)')
        self.assertIsNone(fallback)

    def test_translated_predicate_allows_limit_pushdown(self):
        sql, fallback = self.plan(predicate=pl.col("a") > 1, n_rows=5)
        self.assertEqual(sql, 'SELECT * FROM (SELECT 1) WHERE ("a" > 1) LIMIT 5')
        self.assertIsNone(fallback)

    def test_untranslated_predicate_is_returned_for_polars(self):
        predicate = pl.col("a").is_in([1, 2])
        sql, fallback = self.plan(predicate=predicate)
        self.assertEqual(sql, "SELECT * FROM (SELECT 1)")
        self.assertIs(fallback, predicate)

    def test_untranslated_predicate_suppresses_limit_pushdown(self):
        # LIMIT before an unapplied filter would drop matching rows.
        sql, fallback = self.plan(predicate=pl.col("a").is_in([1, 2]), n_rows=5)
        self.assertNotIn("LIMIT", sql)
        self.assertIsNotNone(fallback)

    def test_untranslated_predicate_keeps_the_columns_it_reads(self):
        sql, fallback = self.plan(
            with_columns=["b"], predicate=pl.col("a").is_in([1, 2])
        )
        self.assertEqual(sql, 'SELECT "b", "a" FROM (SELECT 1)')
        self.assertIsNotNone(fallback)

    def test_translated_predicate_does_not_widen_the_projection(self):
        sql, _ = self.plan(with_columns=["b"], predicate=pl.col("a") > 1)
        self.assertEqual(sql, 'SELECT "b" FROM (SELECT 1) WHERE ("a" > 1)')

    def test_batch_size_becomes_a_max_block_size_setting(self):
        sql, _ = self.plan(n_rows=2, batch_size=4096)
        self.assertEqual(
            sql, "SELECT * FROM (SELECT 1) LIMIT 2 SETTINGS max_block_size = 4096"
        )


@unittest.skipIf(pl is None, "polars not installed")
class TestPredicateTranslation(unittest.TestCase):
    def assertTranslates(self, expr, expected):
        self.assertEqual(_predicate_to_sql(expr), expected)

    def test_comparisons_and_boolean_operators(self):
        self.assertTranslates(pl.col("a") > 5, '("a" > 5)')
        self.assertTranslates(pl.col("a") >= 5, '("a" >= 5)')
        self.assertTranslates(pl.col("a") < 5, '("a" < 5)')
        self.assertTranslates(pl.col("a") <= 5, '("a" <= 5)')
        self.assertTranslates(pl.col("a") == 5, '("a" = 5)')
        self.assertTranslates(pl.col("a") != 5, '("a" != 5)')
        self.assertTranslates(
            (pl.col("a") > 1) & (pl.col("b") < 2), '(("a" > 1) AND ("b" < 2))'
        )
        self.assertTranslates(
            (pl.col("a") > 1) | (pl.col("b") < 2), '(("a" > 1) OR ("b" < 2))'
        )

    def test_null_checks_and_negation(self):
        self.assertTranslates(pl.col("a").is_null(), '("a" IS NULL)')
        self.assertTranslates(pl.col("a").is_not_null(), '("a" IS NOT NULL)')
        self.assertTranslates(~(pl.col("a") == 1), '(NOT ("a" = 1))')

    def test_scalar_literals(self):
        self.assertTranslates(pl.col("f") < 1.5, '("f" < 1.5)')
        self.assertTranslates(pl.col("b") == True, '("b" = true)')  # noqa: E712
        self.assertTranslates(pl.col("s") == "x", "(\"s\" = 'x')")
        self.assertTranslates(
            pl.col("d") == datetime.date(2020, 1, 2), '("d" = toDate32(\'2020-01-02\'))'
        )
        self.assertTranslates(
            pl.col("t") > datetime.datetime(2020, 1, 2, 3, 4, 5),
            '("t" > fromUnixTimestamp64Micro(1577934245000000, \'UTC\'))',
        )

    def test_timezone_aware_literals_translate_to_their_utc_instant(self):
        from zoneinfo import ZoneInfo

        shanghai = datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
        self.assertEqual(int(shanghai.timestamp() * 1_000_000), 1577905445000000)
        self.assertTranslates(
            pl.col("t") > shanghai,
            '("t" > fromUnixTimestamp64Micro(1577905445000000, \'UTC\'))',
        )

    def test_pushed_datetime_predicate_agrees_with_polars(self):
        conn = chdb.connect(":memory:")
        try:
            base = (
                "SELECT number AS n, "
                "toDateTime64('2020-01-02 00:00:00', 6, 'UTC') + toIntervalHour(number) AS t "
                "FROM numbers(10)"
            )
            cutoff = datetime.datetime(2020, 1, 2, 7, tzinfo=datetime.timezone.utc)
            pushed = chdb_source(conn, base).filter(pl.col("t") > cutoff).collect()
            in_polars = conn.pl(base).filter(pl.col("t") > cutoff)
            self.assertEqual(pushed["n"].to_list(), [8, 9])
            assert_frame_equal(pushed, in_polars)
        finally:
            conn.close()

    def test_string_and_identifier_quoting_is_escaped(self):
        self.assertTranslates(pl.col("s") == "it's", "(\"s\" = 'it\\'s')")
        self.assertTranslates(pl.col("s") == "a\\b", "(\"s\" = 'a\\\\b')")
        self.assertTranslates(pl.col('we"ird') == 1, '("we""ird" = 1)')

    def test_arithmetic_inside_a_comparison(self):
        self.assertTranslates(pl.col("a") + 1 > 2, '(("a" + 1) > 2)')
        self.assertTranslates(pl.col("a") - 1 > 2, '(("a" - 1) > 2)')
        self.assertTranslates(pl.col("a") * 2 > 2, '(("a" * 2) > 2)')

    def test_unsupported_expressions_fall_back_to_none(self):
        for expr in (
            pl.col("a").is_in([1, 2]),
            pl.col("s").str.starts_with("ab"),
            pl.col("a") % 2 == 0,
            pl.col("a") / 2 > 1,
            pl.col("a").cast(pl.Int8) == 1,
            pl.col("s").str.contains("x"),
        ):
            with self.subTest(expr=str(expr)):
                self.assertIsNone(_predicate_to_sql(expr))

    def test_escaped_literals_round_trip_through_the_engine(self):
        # A translation is only useful if ClickHouse parses it back to the
        # same value, so run the generated predicate for real.
        conn = chdb.connect(":memory:")
        try:
            lf = chdb_source(
                conn,
                """SELECT * FROM (
                       SELECT 1 AS id, 'it''s' AS s
                       UNION ALL SELECT 2, 'a\\\\b'
                       UNION ALL SELECT 3, 'plain'
                   ) ORDER BY id""",
            )
            self.assertEqual(lf.filter(pl.col("s") == "it's").collect()["id"].to_list(), [1])
            self.assertEqual(lf.filter(pl.col("s") == "a\\b").collect()["id"].to_list(), [2])
        finally:
            conn.close()


class TestPolarsFormatWithoutPolars(unittest.TestCase):
    def test_error_message_names_the_missing_package(self):
        # sqlitelike imports the guard by value, so patch it where it is used.
        import chdb.state.sqlitelike as sqlitelike

        def missing(feature):
            raise ImportError(
                f'{feature} requires polars. Install it via "pip install polars".'
            )

        original = sqlitelike._require_polars
        sqlitelike._require_polars = missing
        try:
            conn = chdb.connect(":memory:")
            try:
                with self.assertRaisesRegex(ImportError, "pip install polars"):
                    conn.query("SELECT 1", "polars")
                with self.assertRaisesRegex(ImportError, "pip install polars"):
                    conn.send_query("SELECT 1", "polars")
            finally:
                conn.close()
        finally:
            sqlitelike._require_polars = original


if __name__ == "__main__":
    unittest.main()
