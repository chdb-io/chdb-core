#!python3
"""Tests for polars objects as query inputs.

A polars DataFrame is scanned by the generic Arrow PyCapsule path (covered in
tests/test_arrow_c_stream_protocol.py). This file covers the two siblings that
path cannot read on their own: LazyFrame and Series.
"""

import unittest

import chdb

try:
    import polars as pl
except ImportError:
    pl = None


def csv(result):
    return str(result).strip()


@unittest.skipIf(pl is None, "polars not installed")
class TestPolarsLazyFrameInput(unittest.TestCase):
    def test_lazyframe_is_collected_and_scanned(self):
        frame = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        lazy = frame.lazy()
        self.assertEqual(
            csv(chdb.query("SELECT sum(a), max(b) FROM Python(lazy)")), '6,"z"'
        )

    def test_pending_operations_are_applied_before_the_scan(self):
        lazy = pl.DataFrame({"a": [1, 2, 3, 4]}).lazy().filter(pl.col("a") % 2 == 0)
        # Mirror: what polars would collect is what chdb must see.
        self.assertEqual(lazy.collect()["a"].to_list(), [2, 4])
        self.assertEqual(csv(chdb.query("SELECT sum(a) FROM Python(lazy)")), "6")

    def test_lazyframe_can_be_queried_repeatedly(self):
        lazy = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        for _ in range(3):
            self.assertEqual(csv(chdb.query("SELECT count() FROM Python(lazy)")), "3")

    def test_lazyframe_self_join_sees_the_same_rows_twice(self):
        lazy = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        self.assertEqual(
            csv(
                chdb.query(
                    "SELECT count() FROM Python(lazy) AS l JOIN Python(lazy) AS r ON l.a = r.a"
                )
            ),
            "3",
        )

    def test_empty_lazyframe_yields_no_rows(self):
        lazy = pl.DataFrame({"a": []}, schema={"a": pl.Int64}).lazy()
        self.assertEqual(csv(chdb.query("SELECT count() FROM Python(lazy)")), "0")

    def test_failing_lazyframe_surfaces_the_polars_error(self):
        lazy = pl.DataFrame({"a": [1]}).lazy().select(pl.col("missing"))
        with self.assertRaises(Exception) as ctx:
            chdb.query("SELECT count() FROM Python(lazy)")
        self.assertIn("missing", str(ctx.exception))

    def test_query_after_a_failing_lazyframe_still_works(self):
        broken = pl.DataFrame({"a": [1]}).lazy().select(pl.col("missing"))
        with self.assertRaises(Exception):
            chdb.query("SELECT count() FROM Python(broken)")
        good = pl.DataFrame({"a": [1, 2]}).lazy()
        self.assertEqual(csv(chdb.query("SELECT sum(a) FROM Python(good)")), "3")


@unittest.skipIf(pl is None, "polars not installed")
class TestPolarsSeriesInput(unittest.TestCase):
    def test_named_series_becomes_a_single_column_table(self):
        values = pl.Series("v", [1, 2, 3])
        self.assertEqual(csv(chdb.query("SELECT sum(v) FROM Python(values)")), "6")
        self.assertEqual(
            csv(chdb.query("DESCRIBE TABLE Python(values)")).splitlines()[0].split(",")[0],
            '"v"',
        )

    def test_series_of_strings_keeps_its_values(self):
        values = pl.Series("s", ["a", "宝贝", None])
        self.assertEqual(
            csv(chdb.query("SELECT s FROM Python(values) ORDER BY s")),
            '"a"\n"宝贝"\n\\N',
        )

    def test_series_can_be_joined_with_a_dataframe(self):
        values = pl.Series("a", [1, 2])
        frame = pl.DataFrame({"a": [2, 3], "b": ["two", "three"]})
        self.assertEqual(
            csv(
                chdb.query(
                    "SELECT b FROM Python(values) AS s JOIN Python(frame) AS f ON s.a = f.a"
                )
            ),
            '"two"',
        )


class TestObjectNotFoundMessage(unittest.TestCase):
    def test_message_names_polars_and_the_arrow_protocol(self):
        with self.assertRaises(Exception) as ctx:
            chdb.query("SELECT count() FROM Python(no_such_object_here)")
        message = str(ctx.exception)
        self.assertIn("polars", message)
        self.assertIn("__arrow_c_stream__", message)


if __name__ == "__main__":
    unittest.main()
