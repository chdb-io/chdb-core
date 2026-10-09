#!/usr/bin/env python3

import os
import unittest
import chdb
from chdb.session import Session


# chdb-core-lite disables ENABLE_ULID; generateULID/ULIDStringToDateTime are absent
# from FunctionFactory and any call raises Code 46.
_LITE = os.environ.get("CHDB_LITE") == "1"


class TestUlidFunctions(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def tearDown(self):
        self.session.close()

    def _expect_lite_missing(self, sql):
        with self.assertRaises(Exception) as ctx:
            self.session.query(sql, "CSV")
        self.assertIn("Code: 46", str(ctx.exception))

    def test_generate_ulid_length(self):
        if _LITE:
            self._expect_lite_missing("SELECT generateULID()")
            return
        ret = self.session.query("SELECT length(toString(generateULID()))", "CSV")
        self.assertEqual(str(ret).strip(), "26")

    def test_generate_ulid_unique(self):
        if _LITE:
            self._expect_lite_missing("SELECT generateULID(1)")
            return
        ret = self.session.query(
            "SELECT uniqExact(generateULID(number)) FROM numbers(1000)", "CSV"
        )
        self.assertEqual(str(ret).strip(), "1000")

    def test_generate_ulid_constant_argument_per_row(self):
        # ClickHouse#117224: a constant argument must still yield one ULID per row.
        sql = "SELECT uniqExact(generateULID(1)) FROM numbers(100)"
        if _LITE:
            self._expect_lite_missing(sql)
            return
        ret = self.session.query(sql, "CSV")
        self.assertEqual(str(ret).strip(), "100")

    def test_ulid_string_to_datetime_non_ascii(self):
        # ClickHouse#100842: non-ASCII input must raise, not read out of bounds.
        sql = "SELECT ULIDStringToDateTime(concat('01ARZ3NDEKTSV4RRFFQ69G5FA', char(200)))"
        if _LITE:
            self._expect_lite_missing(sql)
            return
        with self.assertRaises(Exception) as ctx:
            self.session.query(sql, "CSV")
        self.assertIn("Code: 36", str(ctx.exception))

    def test_ulid_string_to_datetime_wrong_length(self):
        sql = "SELECT ULIDStringToDateTime('01ARZ3NDEK')"
        if _LITE:
            self._expect_lite_missing(sql)
            return
        with self.assertRaises(Exception) as ctx:
            self.session.query(sql, "CSV")
        self.assertIn("Code: 44", str(ctx.exception))

    def test_ulid_string_to_datetime(self):
        sql = "SELECT ULIDStringToDateTime('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'UTC')"
        if _LITE:
            self._expect_lite_missing(sql)
            return
        ret = self.session.query(sql, "CSV")
        self.assertEqual(str(ret).strip(), '"2016-07-30 23:54:10.259"')

    def test_ulid_string_to_datetime_type(self):
        sql = "SELECT toTypeName(ULIDStringToDateTime('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'UTC'))"
        if _LITE:
            self._expect_lite_missing(sql)
            return
        ret = self.session.query(sql, "CSV")
        self.assertEqual(str(ret).strip(), "\"DateTime64(3, 'UTC')\"")

    def test_ulid_roundtrip_timestamp(self):
        sql = (
            "SELECT abs(dateDiff('second', ULIDStringToDateTime(toString(generateULID())), now())) < 60"
        )
        if _LITE:
            self._expect_lite_missing(sql)
            return
        ret = self.session.query(sql, "CSV")
        self.assertEqual(str(ret).strip(), "1")

    def test_ulid_partition_expression(self):
        # Partition keys of the form toYearWeek(ULIDStringToDateTime(id)) must work
        # in table definitions, not only in SELECT.
        if _LITE:
            self._expect_lite_missing(
                "SELECT toYearWeek(ULIDStringToDateTime('01KY0ERWKP0PXFPKG0H11ZZ8NG'))"
            )
            return
        self.session.query(
            "CREATE TABLE ulid_t (run_id String) ENGINE = MergeTree "
            "PARTITION BY toYearWeek(ULIDStringToDateTime(run_id)) ORDER BY run_id"
        )
        self.session.query(
            "INSERT INTO ulid_t VALUES ('01KY0ERWKP0PXFPKG0H11ZZ8NG'), ('01ARZ3NDEKTSV4RRFFQ69G5FAV')"
        )
        ret = self.session.query(
            "SELECT count() FROM ulid_t "
            "WHERE ULIDStringToDateTime(run_id, 'UTC') = toDateTime64('2026-07-20 19:06:47.286', 3, 'UTC')",
            "CSV",
        )
        self.assertEqual(str(ret).strip(), "1")
        ret = self.session.query(
            "SELECT uniqExact(partition_id) FROM system.parts WHERE table = 'ulid_t' AND active",
            "CSV",
        )
        self.assertEqual(str(ret).strip(), "2")
        self.session.query("DROP TABLE ulid_t")


if __name__ == "__main__":
    unittest.main()
