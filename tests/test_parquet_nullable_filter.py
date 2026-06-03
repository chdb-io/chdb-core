#!python3
"""
Regression test for LOGICAL_ERROR crash in Parquet V3 reader with nullable columns
and WHERE filter.

Two bugs fixed in src/Processors/Formats/Impl/Parquet/Reader.cpp:

1. Inverted null_map memchr check: searched for byte 0 (non-null) instead of byte 1
   (null). When a filter left only NULL rows in a subchunk for a non-nullable output
   column with null_as_default=0, the check incorrectly cleared null_map, leaving 0
   rows while rows_pass=1 → LOGICAL_ERROR "Unexpected number of rows in column subchunk".

2. use_filter_in_decoder enabled for nullable columns: the optimization processes ALL
   rows through processDefLevelsForInnermostColumn (null_map gets entries for all rows,
   not just filtered rows) then hands the filter to the decoder. With nullable columns
   the decoder applies the filter at consecutive encoded-value indices, but null rows
   have no encoded values, so the filter maps to wrong positions →
   LOGICAL_ERROR "Too many bytes in mask".

Cherry-pick of ClickHouse/ClickHouse#102628 (commit 8e7137b625ec56d40af3a06b24dd01bbc8df4005).

Reported as chdb-core issue #53 (windowFunnel against file('*.parquet') fails with
LOGICAL_ERROR "Too many bytes in mask").
"""

import csv
import io
import os
import tempfile
import unittest

import chdb
from chdb import session as chdb_session


def _make_nullable_parquet(path: str) -> None:
    """
    Create a Parquet file with id UInt64 and val Nullable(String).
    Every row where id % 3 == 0 has val = NULL.
    Uses output_format_parquet_use_custom_encoder=0 to exercise the V3 reader path.
    """
    chdb.query(
        f"""
        INSERT INTO FUNCTION file('{path}', Parquet)
        SELECT number AS id, if(number % 3 = 0, NULL, toString(number)) AS val
        FROM numbers(20)
        SETTINGS output_format_parquet_use_custom_encoder = 0
        """,
        "CSV",
    )


class TestParquetNullableFilter(unittest.TestCase):
    """
    Regression tests for LOGICAL_ERROR crash in the Parquet V3 reader when
    applying WHERE filters over nullable columns.
    """

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._parquet = os.path.join(self._tmpdir, "nullable_test.parquet")
        _make_nullable_parquet(self._parquet)

    # ------------------------------------------------------------------
    # 1. Nullable output — NULLs preserved, filter on NULL rows should work
    # ------------------------------------------------------------------
    def test_nullable_output_filter_on_null_rows(self):
        """Reading Nullable output with WHERE filter on NULL rows should succeed."""
        result = chdb.query(
            f"""
            SELECT id, val
            FROM file('{self._parquet}', Parquet, 'id UInt64, val Nullable(String)')
            WHERE id IN (0, 3, 6)
            ORDER BY id
            """,
            "CSV",
        )
        self.assertFalse(result.has_error(), result.error_message())
        lines = result.bytes().decode().strip().splitlines()
        self.assertEqual(len(lines), 3)
        for line in lines:
            self.assertIn(r"\N", line)

    # ------------------------------------------------------------------
    # 2. Non-nullable + null_as_default=1 — NULLs become empty strings
    # ------------------------------------------------------------------
    def test_non_nullable_null_as_default_filter_on_null_rows(self):
        """null_as_default=1: NULL rows become empty string; no error expected."""
        result = chdb.query(
            f"""
            SELECT id, val
            FROM file('{self._parquet}', Parquet, 'id UInt64, val String')
            WHERE id IN (0, 3, 6)
            ORDER BY id
            SETTINGS input_format_null_as_default = 1
            """,
            "CSV",
        )
        self.assertFalse(result.has_error(), result.error_message())
        # ClickHouse CSV quotes empty strings, so the val field renders as
        # `""` (not bare `,`). Parse the result with csv.reader and inspect
        # the second field directly instead of doing a fragile string-suffix
        # check on the raw line.
        rows = list(csv.reader(io.StringIO(result.bytes().decode())))
        self.assertEqual(len(rows), 3)
        expected_ids = ["0", "3", "6"]
        for row, expected_id in zip(rows, expected_ids):
            self.assertEqual(
                len(row), 2, f"expected 2 fields, got: {row!r}"
            )
            self.assertEqual(row[0], expected_id, f"unexpected id in row: {row!r}")
            self.assertEqual(
                row[1],
                "",
                f"expected empty val (NULL→default), got: {row[1]!r} (row={row!r})",
            )

    # ------------------------------------------------------------------
    # 3. Non-nullable + null_as_default=0 + filter hits ONLY NULL rows →
    #    must throw CANNOT_INSERT_NULL, NOT a LOGICAL_ERROR
    # ------------------------------------------------------------------
    def test_non_nullable_no_default_filter_only_null_rows_raises_correctly(self):
        """Filter on a row whose value is NULL must raise CANNOT_INSERT_NULL, not crash."""
        with self.assertRaises(Exception) as ctx:
            chdb.query(
                f"""
                SELECT id, val
                FROM file('{self._parquet}', Parquet, 'id UInt64, val String')
                WHERE id = 0
                SETTINGS input_format_null_as_default = 0
                """,
                "CSV",
            )
        err = str(ctx.exception)
        # Before fix: LOGICAL_ERROR ("Too many bytes in mask" or
        #   "Unexpected number of rows in column subchunk").
        # After fix: CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN.
        self.assertNotIn(
            "LOGICAL_ERROR",
            err,
            f"Got LOGICAL_ERROR instead of CANNOT_INSERT_NULL: {err}",
        )
        # Should be a proper user-facing error, not a crash
        self.assertTrue(
            "CANNOT_INSERT_NULL" in err or "Cannot convert NULL" in err,
            f"Expected CANNOT_INSERT_NULL error, got: {err}",
        )

    # ------------------------------------------------------------------
    # 4. Non-nullable + null_as_default=0 + filter hits ONLY non-null rows →
    #    should succeed
    # ------------------------------------------------------------------
    def test_non_nullable_no_default_filter_non_null_rows_succeeds(self):
        """Filter on rows where val IS NOT NULL must succeed without error."""
        result = chdb.query(
            f"""
            SELECT id, val
            FROM file('{self._parquet}', Parquet, 'id UInt64, val String')
            WHERE id IN (1, 2, 4)
            ORDER BY id
            SETTINGS input_format_null_as_default = 0
            """,
            "CSV",
        )
        self.assertFalse(result.has_error(), result.error_message())
        self.assertIn("1", result.bytes().decode())
        self.assertIn("2", result.bytes().decode())
        self.assertIn("4", result.bytes().decode())

    # ------------------------------------------------------------------
    # 5. windowFunnel over Parquet with nullable timestamp — original report
    #    (simplified, no network download required)
    # ------------------------------------------------------------------
    def test_window_funnel_over_parquet_nullable_column(self):
        """
        windowFunnel over a Parquet file with a nullable timestamp column
        must not crash with LOGICAL_ERROR "Too many bytes in mask".

        This is the exact usage pattern reported in chdb-core issue #53.
        """
        tmpdir = tempfile.mkdtemp()
        parquet = os.path.join(tmpdir, "funnel_test.parquet")

        # Build a small dataset: user_id, event_time (nullable), fare (<15 or >50)
        chdb.query(
            f"""
            INSERT INTO FUNCTION file('{parquet}', Parquet)
            SELECT
                number % 5                                          AS user_id,
                if(number % 7 = 0, NULL,
                   toDateTime('2024-01-01') + toIntervalSecond(number * 60))
                                                                    AS event_time,
                if(number % 2 = 0, toFloat64(number % 20),
                   toFloat64(number % 20 + 55))                    AS fare_amount
            FROM numbers(100)
            SETTINGS output_format_parquet_use_custom_encoder = 0
            """,
            "CSV",
        )

        # This triggered "Too many bytes in mask" before the fix because the
        # use_filter_in_decoder optimization was incorrectly applied to nullable
        # columns.
        #
        # Note: explicit toDateTime() cast on event_time is required because
        # ClickHouse v26.5+ infers Parquet TIMESTAMP_MILLIS as DateTime64(3),
        # which windowFunnel rejects (it accepts only Date / DateTime / unsigned).
        result = chdb.query(
            f"""
            SELECT funnel_level, count(*) AS cnt
            FROM (
                SELECT user_id,
                       windowFunnel(3600)(
                           toDateTime(event_time),
                           fare_amount < 15,
                           fare_amount > 50
                       ) AS funnel_level
                FROM file('{parquet}', Parquet)
                WHERE fare_amount < 15 OR fare_amount > 50
                GROUP BY user_id
            )
            WHERE funnel_level > 0
            GROUP BY funnel_level
            ORDER BY funnel_level
            """,
            "CSV",
        )
        self.assertFalse(result.has_error(), result.error_message())
        # Just verify we get results — exact values depend on engine internals
        self.assertGreater(len(result.bytes()), 0)


if __name__ == "__main__":
    unittest.main()
