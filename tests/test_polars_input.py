#!python3
"""Tests for polars objects as query inputs.

A polars DataFrame is scanned by the generic Arrow PyCapsule path (covered in
tests/test_arrow_c_stream_protocol.py). This file covers the two siblings that
path cannot read on their own -- LazyFrame and Series -- and register_table(),
which binds an object to a name instead of looking for a variable in the
caller's frames.
"""

import gc
import unittest

import chdb
from chdb import session as chs

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow as pa
except ImportError:
    pa = None


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


@unittest.skipIf(pl is None, "polars not installed")
class TestRegisterTable(unittest.TestCase):
    def setUp(self):
        self.conn = chdb.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def test_registered_object_is_queryable_by_name(self):
        self.conn.register_table("t", pl.DataFrame({"a": [1, 2, 3]}))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "6")

    def test_registration_survives_the_caller_dropping_its_reference(self):
        frame = pl.DataFrame({"a": [1, 2, 3]})
        self.conn.register_table("t", frame)
        del frame
        gc.collect()
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "6")

    def test_registration_reaches_objects_no_variable_names(self):
        holder = {"inner": pl.DataFrame({"a": [1, 2, 3, 4]})}
        # Python(holder['inner']) is not a name the frame walk can extract.
        self.conn.register_table("held", holder["inner"])
        self.assertEqual(csv(self.conn.query("SELECT count() FROM Python(held)")), "4")

    def test_registered_name_shadows_a_local_variable(self):
        t = pl.DataFrame({"a": [100]})  # noqa: F841 - visible to the frame walk
        self.conn.register_table("t", pl.DataFrame({"a": [1, 2, 3]}))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "6")

    def test_unregistering_restores_the_local_variable(self):
        t = pl.DataFrame({"a": [100]})  # noqa: F841 - visible to the frame walk
        self.conn.register_table("t", pl.DataFrame({"a": [1, 2, 3]}))
        self.assertTrue(self.conn.unregister_table("t"))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "100")

    def test_unregistering_an_unknown_name_returns_false(self):
        self.assertFalse(self.conn.unregister_table("never_registered"))

    def test_registering_the_same_name_replaces_the_object(self):
        self.conn.register_table("t", pl.DataFrame({"a": [1]}))
        self.conn.register_table("t", pl.DataFrame({"a": [2, 3]}))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "5")
        self.assertEqual(self.conn.registered_tables(), ["t"])

    def test_registered_tables_lists_every_name(self):
        self.conn.register_table("one", pl.DataFrame({"a": [1]}))
        self.conn.register_table("two", pl.DataFrame({"a": [2]}))
        self.assertEqual(sorted(self.conn.registered_tables()), ["one", "two"])
        self.conn.unregister_table("one")
        self.assertEqual(self.conn.registered_tables(), ["two"])

    def test_registrations_are_scoped_to_one_connection(self):
        other = chdb.connect(":memory:")
        try:
            self.conn.register_table("t", pl.DataFrame({"a": [1, 2, 3]}))
            self.assertEqual(other.registered_tables(), [])
            with self.assertRaises(Exception) as ctx:
                other.query("SELECT count() FROM Python(t)")
            self.assertIn("not found", str(ctx.exception))
        finally:
            other.close()

    def test_quoted_reference_finds_a_name_with_a_space(self):
        self.conn.register_table("my table", pl.DataFrame({"a": [1, 2]}))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python('my table')")), "3")

    def test_registered_lazyframe_is_collected_per_query(self):
        self.conn.register_table("lz", pl.DataFrame({"a": [1, 2, 3]}).lazy())
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(lz)")), "6")
        self.assertEqual(csv(self.conn.query("SELECT count() FROM Python(lz)")), "3")

    def test_registered_series_becomes_a_single_column_table(self):
        self.conn.register_table("v", pl.Series("v", [1, 2, 3]))
        self.assertEqual(csv(self.conn.query("SELECT sum(v) FROM Python(v)")), "6")

    @unittest.skipIf(pd is None, "pandas not installed")
    def test_pandas_dataframe_can_be_registered(self):
        self.conn.register_table("t", pd.DataFrame({"a": [1, 2, 3]}))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "6")

    @unittest.skipIf(pa is None, "pyarrow not installed")
    def test_pyarrow_table_can_be_registered(self):
        self.conn.register_table("t", pa.table({"a": [1, 2, 3]}))
        self.assertEqual(csv(self.conn.query("SELECT sum(a) FROM Python(t)")), "6")

    def test_registering_a_non_table_object_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not queryable"):
            self.conn.register_table("t", 42)

    def test_registering_a_string_is_rejected(self):
        # str satisfies the __getitem__ fallback; catching it here turns the
        # swapped-arguments mistake into an immediate error.
        with self.assertRaisesRegex(ValueError, "not a data source"):
            self.conn.register_table("t", "some_table")

    def test_registering_an_unusable_name_is_rejected(self):
        frame = pl.DataFrame({"a": [1]})
        for name in ("", "quo'te", 'dou"ble'):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "Invalid table name"):
                    self.conn.register_table(name, frame)

    def test_session_registration_round_trip(self):
        with chs.Session() as session:
            session.register_table("t", pl.DataFrame({"a": [1, 2, 3]}))
            self.assertEqual(session.registered_tables(), ["t"])
            self.assertEqual(csv(session.query("SELECT sum(a) FROM Python(t)")), "6")
            self.assertTrue(session.unregister_table("t"))
            self.assertEqual(session.registered_tables(), [])


class TestObjectNotFoundMessage(unittest.TestCase):
    def test_message_names_polars_and_register_table(self):
        with self.assertRaises(Exception) as ctx:
            chdb.query("SELECT count() FROM Python(no_such_object_here)")
        message = str(ctx.exception)
        self.assertIn("polars", message)
        self.assertIn("register_table", message)
        self.assertIn("__arrow_c_stream__", message)


if __name__ == "__main__":
    unittest.main()
