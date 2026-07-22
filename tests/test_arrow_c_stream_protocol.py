#!python3
"""Tests for the Arrow PyCapsule interface (__arrow_c_stream__) on both sides:

Output: chdb query results (query_result with "Arrow"/"ArrowStream" output
format, and StreamingResult) export an ArrowArrayStream PyCapsule, so
Arrow-native consumers (pyarrow, polars, duckdb, ...) can ingest chdb results
zero-copy without going through explicit IPC parsing.

Input: any Python object exposing __arrow_c_stream__ (polars DataFrame,
pyarrow.RecordBatchReader, duckdb relations, chdb results, ...) can be scanned
with the Python() table engine, in addition to the dedicated pandas /
pyarrow.Table paths.
"""

import unittest
from decimal import Decimal

import chdb

import pyarrow as pa
import pyarrow.compute as pc

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import duckdb
except ImportError:
    duckdb = None


EXPECTED_ROWS = [
    {"id": 1, "name": "Alice", "score": 9.5},
    {"id": 2, "name": "宝贝", "score": 7.25},
    {"id": 3, "name": None, "score": -1.5},
]

SAMPLE_SQL = """
    SELECT * FROM (
        SELECT 1 AS id, 'Alice' AS name, 9.5 AS score
        UNION ALL SELECT 2, '宝贝', 7.25
        UNION ALL SELECT 3, NULL, -1.5
    ) ORDER BY id
"""


class TestArrowCStreamOutput(unittest.TestCase):
    def test_pyarrow_table_from_query_result(self):
        res = chdb.query(SAMPLE_SQL, "Arrow")
        table = pa.table(res)
        self.assertEqual(table.column_names, ["id", "name", "score"])
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.to_pylist(), EXPECTED_ROWS)

    def test_pyarrow_recordbatchreader_from_stream(self):
        res = chdb.query(SAMPLE_SQL, "Arrow")
        reader = pa.RecordBatchReader.from_stream(res)
        self.assertEqual(reader.schema.names, ["id", "name", "score"])
        table = reader.read_all()
        self.assertEqual(table.to_pylist(), EXPECTED_ROWS)

    def test_arrowstream_format_also_exports(self):
        res = chdb.query(SAMPLE_SQL, "ArrowStream")
        table = pa.table(res)
        self.assertEqual(table.column_names, ["id", "name", "score"])
        self.assertEqual(table.to_pylist(), EXPECTED_ROWS)

    def test_multiple_exports_are_independent(self):
        res = chdb.query(SAMPLE_SQL, "Arrow")
        first = pa.table(res)
        second = pa.table(res)
        self.assertTrue(first.equals(second))
        self.assertEqual(second.to_pylist(), EXPECTED_ROWS)

    def test_non_arrow_format_raises_value_error(self):
        res = chdb.query("SELECT 1 AS x", "CSV")
        with self.assertRaisesRegex(ValueError, "Arrow"):
            res.__arrow_c_stream__()

    def test_empty_payload_raises_value_error(self):
        # Zero-row CSV output produces an empty buffer -> no Arrow payload.
        res = chdb.query("SELECT 1 AS x WHERE 0", "CSV")
        with self.assertRaisesRegex(ValueError, "Arrow"):
            res.__arrow_c_stream__()

    def test_empty_result_preserves_schema(self):
        res = chdb.query("SELECT number AS n, toString(number) AS s FROM numbers(10) WHERE number < 0", "Arrow")
        table = pa.table(res)
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.column_names, ["n", "s"])
        self.assertEqual(table.schema.field("n").type, pa.uint64())

    def test_type_coverage(self):
        res = chdb.query(
            """
            SELECT
                toInt8(-8) AS i8,
                toUInt64(18446744073709551615) AS u64,
                toFloat32(0.5) AS f32,
                toNullable('x') AS ns,
                NULL AS always_null,
                toDate('2026-07-22') AS d,
                toDateTime64('2026-07-22 01:02:03.456', 3, 'UTC') AS dt64,
                toDecimal64(123.4567, 4) AS dec,
                [1, 2, 3] AS arr
            """,
            "Arrow",
        )
        table = pa.table(res)
        row = table.to_pylist()[0]
        self.assertEqual(row["i8"], -8)
        self.assertEqual(row["u64"], 18446744073709551615)
        self.assertEqual(row["f32"], 0.5)
        self.assertEqual(row["ns"], "x")
        self.assertIsNone(row["always_null"])
        self.assertEqual(str(row["d"]), "2026-07-22")
        self.assertEqual(row["dt64"].isoformat(), "2026-07-22T01:02:03.456000+00:00")
        self.assertEqual(row["dec"], Decimal("123.4567"))
        self.assertEqual(row["arr"], [1, 2, 3])

    def test_large_result_multiple_batches(self):
        res = chdb.query("SELECT number FROM numbers(300000)", "Arrow")
        reader = pa.RecordBatchReader.from_stream(res)
        batches = list(reader)
        self.assertGreater(len(batches), 1, "expected multiple record batches")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.num_rows, 300000)
        self.assertEqual(pc.sum(table.column("number")).as_py(), 300000 * 299999 // 2)

    @unittest.skipIf(pl is None, "polars not installed")
    def test_polars_dataframe_from_query_result(self):
        res = chdb.query(SAMPLE_SQL, "Arrow")
        df = pl.DataFrame(res)
        self.assertEqual(df.shape, (3, 3))
        self.assertEqual(df.columns, ["id", "name", "score"])
        self.assertEqual(df.to_dicts(), EXPECTED_ROWS)

    @unittest.skipIf(duckdb is None, "duckdb not installed")
    def test_duckdb_from_query_result(self):
        res = chdb.query(SAMPLE_SQL, "Arrow")
        rows = duckdb.from_arrow(res).fetchall()
        self.assertEqual(rows, [(1, "Alice", 9.5), (2, "宝贝", 7.25), (3, None, -1.5)])

    def test_streaming_result_arrow_c_stream(self):
        conn = chdb.connect(":memory:")
        try:
            stream = conn.send_query("SELECT number FROM numbers(100000)", "Arrow")
            table = pa.table(stream)
            self.assertEqual(table.num_rows, 100000)
            self.assertEqual(pc.sum(table.column("number")).as_py(), 100000 * 99999 // 2)
        finally:
            conn.close()

    @unittest.skipIf(pl is None, "polars not installed")
    def test_streaming_result_to_polars(self):
        conn = chdb.connect(":memory:")
        try:
            stream = conn.send_query("SELECT number AS n FROM numbers(1000)", "Arrow")
            df = pl.DataFrame(stream)
            self.assertEqual(df.shape, (1000, 1))
            self.assertEqual(df["n"].sum(), 1000 * 999 // 2)
        finally:
            conn.close()

    def test_streaming_result_non_arrow_raises(self):
        conn = chdb.connect(":memory:")
        try:
            stream = conn.send_query("SELECT 1", "CSV")
            with self.assertRaisesRegex(ValueError, "Arrow"):
                stream.__arrow_c_stream__()
            stream.cancel()
        finally:
            conn.close()


class TestArrowCStreamInput(unittest.TestCase):
    @unittest.skipIf(pl is None, "polars not installed")
    def test_query_polars_dataframe(self):
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "宝贝", None, "Dave"],
                "score": [9.5, 7.25, -1.5, 0.0],
            }
        )
        res = chdb.query(
            "SELECT id, name, score FROM Python(df) WHERE id != 4 ORDER BY id", "ArrowTable"
        )
        self.assertEqual(res.to_pylist(), EXPECTED_ROWS)

    @unittest.skipIf(pl is None, "polars not installed")
    def test_query_polars_aggregation(self):
        df = pl.DataFrame({"g": ["a", "b", "a", "b", "a"], "v": [1, 2, 3, 4, 5]})
        out = chdb.query(
            "SELECT g, sum(v) AS s, count() AS c FROM Python(df) GROUP BY g ORDER BY g", "CSV"
        )
        self.assertEqual(str(out), '"a",9,3\n"b",6,2\n')

    @unittest.skipIf(pl is None, "polars not installed")
    def test_query_polars_list_column(self):
        df = pl.DataFrame({"id": [1, 2], "vals": [[1, 2, 3], [4, 5]]})
        # Arrow list items are nullable -> Array(Nullable(Int64)) on the CH side.
        out = chdb.query(
            "SELECT id, arraySum(x -> assumeNotNull(x), vals) AS s, length(vals) AS l"
            " FROM Python(df) ORDER BY id",
            "CSV",
        )
        self.assertEqual(str(out), "1,6,3\n2,9,2\n")

    @unittest.skipIf(pl is None, "polars not installed")
    def test_query_polars_projection_subset(self):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [1.5, 2.5]})
        out = chdb.query("SELECT b FROM Python(df) ORDER BY b DESC", "CSV")
        self.assertEqual(str(out), '"y"\n"x"\n')

    @unittest.skipIf(pl is None, "polars not installed")
    def test_query_polars_empty_dataframe(self):
        df = pl.DataFrame(schema={"a": pl.Int64, "b": pl.String})
        out = chdb.query("SELECT count() FROM Python(df)", "CSV")
        self.assertEqual(str(out), "0\n")

    @unittest.skipIf(pl is None, "polars not installed")
    def test_query_polars_self_join_rescans(self):
        # Each scan calls __arrow_c_stream__ again; polars produces a fresh
        # stream per call, so a self-join must see the full data twice.
        df = pl.DataFrame({"x": [1, 2, 3]})
        out = chdb.query(
            "SELECT count() FROM Python(df) AS a JOIN Python(df) AS b ON a.x = b.x", "CSV"
        )
        self.assertEqual(str(out), "3\n")

    def test_query_pyarrow_recordbatchreader(self):
        schema = pa.schema([("x", pa.int64()), ("s", pa.string())])
        batches = [
            pa.record_batch([[1, 2], ["a", "b"]], schema=schema),
            pa.record_batch([[3, 4], ["c", "d"]], schema=schema),
        ]
        reader = pa.RecordBatchReader.from_batches(schema, batches)
        out = chdb.query(
            "SELECT sum(x) AS total, count() AS c, min(s) AS m FROM Python(reader)", "CSV"
        )
        self.assertEqual(str(out), '10,4,"a"\n')

    def test_query_exhausted_recordbatchreader_returns_empty(self):
        # Streams are single-use: a second query over the same reader sees an
        # exhausted stream and yields zero rows (matching Arrow stream semantics).
        schema = pa.schema([("x", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(
            schema, [pa.record_batch([[1, 2, 3]], schema=schema)]
        )
        first = chdb.query("SELECT count() FROM Python(reader)", "CSV")
        self.assertEqual(str(first), "3\n")
        second = chdb.query("SELECT count() FROM Python(reader)", "CSV")
        self.assertEqual(str(second), "0\n")

    @unittest.skipIf(duckdb is None, "duckdb not installed")
    def test_query_duckdb_relation(self):
        rel = duckdb.sql("SELECT range AS x, 'v' || range::VARCHAR AS s FROM range(5)")
        out = chdb.query("SELECT sum(x) AS total, max(s) AS m FROM Python(rel)", "CSV")
        self.assertEqual(str(out), '10,"v4"\n')

    def test_query_chdb_result_roundtrip(self):
        res = chdb.query("SELECT number AS n FROM numbers(10)", "Arrow")
        out = chdb.query("SELECT count() AS c, sum(n) AS s FROM Python(res)", "CSV")
        self.assertEqual(str(out), "10,45\n")

    def test_pyarrow_table_still_works(self):
        # pyarrow.Table must keep taking its dedicated (pushdown-capable) path.
        table = pa.table({"x": [1, 2, 3], "s": ["a", "b", "c"]})
        out = chdb.query("SELECT sum(x), max(s) FROM Python(table)", "CSV")
        self.assertEqual(str(out), '6,"c"\n')

    def test_pandas_still_works(self):
        # pandas >= 2.2 exposes __arrow_c_stream__ too, but must stay on the
        # dedicated pandas scan (is_pandas_df guard).
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2, 3], "s": ["a", "b", "c"]})
        out = chdb.query("SELECT sum(x), max(s) FROM Python(df) WHERE x > 1", "CSV")
        self.assertEqual(str(out), '5,"c"\n')


if __name__ == "__main__":
    unittest.main()
