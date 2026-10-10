#!python3
"""Tests for the polars output format.

Covers ``chdb.query(sql, "polars")``, ``Connection.query``/``send_query``,
``Session.query`` and ``chdb.to_polars``.
"""

import datetime
import decimal
import unittest

import chdb
from chdb import session as chs

try:
    import polars as pl
except ImportError:
    pl = None

from chdb.polars_io import to_polars


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

    def test_session_query_polars_returns_exact_values(self):
        with chs.Session() as session:
            session.query("CREATE DATABASE IF NOT EXISTS pl_out ENGINE = Atomic")
            session.query(
                "CREATE TABLE pl_out.t (a UInt32, b String) ENGINE = MergeTree ORDER BY a"
            )
            session.query("INSERT INTO pl_out.t VALUES (1, 'x'), (2, 'y')")
            self.assertEqual(
                session.query("SELECT * FROM pl_out.t ORDER BY a", "polars").to_dicts(),
                [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
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

    def test_multi_block_result_is_read_completely(self):
        # One record batch per block: polars releases that import only the
        # first batch of a stream would return 8192 of these rows.
        df = chdb.query(
            "SELECT number AS n FROM numbers(100000) SETTINGS max_block_size = 8192", "polars"
        )
        self.assertEqual(df.height, 100000)
        self.assertEqual(df["n"].sum(), 100000 * 99999 // 2)

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
class TestPolarsMinimumVersion(unittest.TestCase):
    """polars 1.3-1.9 import only the first record batch of an Arrow stream."""

    def with_polars_version(self, version, fn):
        real = pl.__version__
        pl.__version__ = version
        try:
            return fn()
        finally:
            pl.__version__ = real

    def test_releases_before_1_10_are_rejected_on_every_entry_point(self):
        conn = chdb.connect(":memory:")
        try:
            arrow_result = chdb.query("SELECT 1 AS a", "Arrow")
            calls = {
                "chdb.query": lambda: chdb.query("SELECT 1", "polars"),
                "Connection.query": lambda: conn.query("SELECT 1", "polars"),
                "Connection.send_query": lambda: conn.send_query("SELECT 1", "polars"),
                "to_polars": lambda: to_polars(arrow_result),
            }
            for name, call in calls.items():
                with self.subTest(entry_point=name):
                    with self.assertRaisesRegex(ImportError, r"polars>=1\.10\.0, found 1\.9\.0"):
                        self.with_polars_version("1.9.0", call)
        finally:
            conn.close()

    def test_release_1_10_is_accepted(self):
        df = self.with_polars_version("1.10.0", lambda: chdb.query("SELECT 1 AS a", "polars"))
        self.assertEqual(df.to_dicts(), [{"a": 1}])

    def test_unparseable_version_is_not_blocked(self):
        df = self.with_polars_version("dev", lambda: chdb.query("SELECT 1 AS a", "polars"))
        self.assertEqual(df.to_dicts(), [{"a": 1}])


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
