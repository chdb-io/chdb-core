#!/usr/bin/env python3
"""Tests for parallel Arrow IPC encoding (output_format_arrow_parallel_encoding).

Validates that:
  * Parallel encoding produces output that decodes to the same pyarrow Table
    as serial encoding for diverse data types.
  * Row order is preserved across multiple chunks.
  * LowCardinality + low_cardinality_as_dictionary transparently falls back
    to serial encoding (the dictionary state is per-converter and would
    otherwise diverge across worker threads).
  * Both Arrow (file) and ArrowStream (stream) IPC formats are supported.
"""

import io
import json
import os
import threading
import time
import unittest

import pyarrow as pa
import chdb

_LITE = os.environ.get("CHDB_LITE") == "1"


def _extension_name(field):
    if hasattr(field.type, "extension_name"):
        return field.type.extension_name
    value = (field.metadata or {}).get(b"ARROW:extension:name")
    return value.decode() if value else None


def _json_value(value):
    value = value.as_py() if hasattr(value, "as_py") else value
    if isinstance(value, bytes):
        value = value.decode()
    return json.loads(value) if isinstance(value, str) else value


def _query_arrow(
    sql: str,
    parallel: bool,
    fmt: str = "Arrow",
    threads: int = 4,
    block_size: int = 4096,
    extra_settings: str = "",
) -> pa.Table:
    full = (
        f"{sql} SETTINGS max_threads = {threads}, max_block_size = {block_size}, "
        f"output_format_arrow_parallel_encoding = {1 if parallel else 0}"
    )
    if extra_settings:
        full += ", " + extra_settings
    data = chdb.query(full, fmt).bytes()
    buf = io.BytesIO(data)
    if fmt == "Arrow":
        return pa.ipc.open_file(buf).read_all()
    if fmt == "ArrowStream":
        return pa.ipc.open_stream(buf).read_all()
    raise ValueError(fmt)


class TestArrowParallelEncoding(unittest.TestCase):
    def _assert_parallel_matches_serial(
        self,
        sql: str,
        fmt: str = "Arrow",
        threads: int = 4,
        block_size: int = 4096,
        extra_settings: str = "",
    ):
        kwargs = dict(fmt=fmt, threads=threads, block_size=block_size, extra_settings=extra_settings)
        serial = _query_arrow(sql, parallel=False, **kwargs)
        parallel = _query_arrow(sql, parallel=True, **kwargs)
        self.assertEqual(serial.column_names, parallel.column_names)
        self.assertEqual(serial.num_rows, parallel.num_rows)
        # Strict equality across the entire table - this catches both data
        # corruption and reordering caused by parallel workers.
        self.assertTrue(
            serial.equals(parallel),
            msg=f"parallel Arrow output differs from serial for: {sql} ({extra_settings})",
        )
        return serial

    # --- Basic types --------------------------------------------------------

    def test_numeric_strings_match_serial(self):
        sql = (
            "SELECT number AS i, "
            "toFloat64(number) AS f, "
            "toString(number) AS s, "
            "toUInt32(number * 2) AS u "
            "FROM numbers(50000)"
        )
        tbl = self._assert_parallel_matches_serial(sql)
        self.assertEqual(tbl.num_rows, 50000)
        self.assertEqual(tbl.column_names, ["i", "f", "s", "u"])

    def test_nullable_columns_match_serial(self):
        sql = (
            "SELECT number AS k, "
            "if(number % 7 = 0, NULL, toString(number)) AS s, "
            "if(number % 5 = 0, NULL, toFloat64(number) / 3) AS f "
            "FROM numbers(20000)"
        )
        tbl = self._assert_parallel_matches_serial(sql)
        # Make sure nulls actually round-trip; otherwise the equality above
        # would still pass on tables with no nulls and silently weaken the test.
        self.assertGreater(tbl.column("s").null_count, 0)
        self.assertGreater(tbl.column("f").null_count, 0)

    def test_arrays_and_tuples_match_serial(self):
        sql = (
            "SELECT number AS k, "
            "range(toUInt32(number % 8)) AS arr, "
            "tuple(number, toString(number)) AS tup "
            "FROM numbers(10000)"
        )
        tbl = self._assert_parallel_matches_serial(sql)
        self.assertEqual(tbl.num_rows, 10000)
        # Verify nested types survived the round-trip in both engines.
        self.assertTrue(pa.types.is_list(tbl.schema.field("arr").type))

    # --- Multi-chunk ordering ----------------------------------------------

    def test_row_order_preserved_across_many_chunks(self):
        # max_block_size=4096 + 50_000 rows guarantees ~12 chunks delivered to
        # the format. Workers may finish out of order but the writer must
        # serialize batches in arrival order.
        sql = "SELECT number AS n FROM numbers(50000)"
        tbl = self._assert_parallel_matches_serial(sql)
        col = tbl.column("n").to_pylist()
        self.assertEqual(col, list(range(50000)))

    def test_arrow_stream_format_match_serial(self):
        sql = (
            "SELECT number AS i, toString(number) AS s "
            "FROM numbers(20000)"
        )
        self._assert_parallel_matches_serial(sql, fmt="ArrowStream")

    def test_null_literal_arrow_stream(self):
        for native_writer in (0, 1):
            tbl = _query_arrow(
                "SELECT NULL AS v",
                parallel=False,
                fmt="ArrowStream",
                extra_settings=f"output_format_arrow_use_native_writer = {native_writer}",
            )
            self.assertTrue(pa.types.is_null(tbl.schema.field("v").type))
            self.assertEqual(tbl.column("v").to_pylist(), [None])

    def test_json_dynamic_arrow_json(self):
        sql = (
            "SELECT CAST(concat('{\"n\":', toString(number), '}'), 'JSON') AS j, "
            "CAST(number, 'Dynamic') AS d FROM numbers(3)"
        )
        for native_writer in (0, 1):
            tbl = _query_arrow(
                sql,
                parallel=False,
                fmt="ArrowStream",
                extra_settings=f"output_format_arrow_use_native_writer = {native_writer}",
            )
            self.assertEqual(_extension_name(tbl.schema.field("j")), "arrow.json")
            self.assertEqual(_extension_name(tbl.schema.field("d")), "arrow.json")
            self.assertEqual([_json_value(v) for v in tbl.column("j")], [{"n": 0}, {"n": 1}, {"n": 2}])
            self.assertEqual([_json_value(v) for v in tbl.column("d")], [0, 1, 2])

    def test_uuid_extension_metadata_opt_out(self):
        sql = "SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS u"
        for native_writer in (0, 1):
            tbl = _query_arrow(
                sql,
                parallel=False,
                fmt="ArrowStream",
                extra_settings=(
                    "output_format_arrow_uuid_as_fixed_byte_array = 1, "
                    f"output_format_arrow_use_native_writer = {native_writer}"
                ),
            )
            field = tbl.schema.field("u")
            self.assertEqual(field.type, pa.binary(16))
            self.assertNotIn(b"ARROW:extension:name", field.metadata or {})

    def test_variant_as_string_option(self):
        sql = (
            "SELECT multiIf("
            "number = 0, CAST(toUInt64(1), 'Variant(UInt64, String)'), "
            "number = 1, CAST('abc', 'Variant(UInt64, String)'), "
            "CAST(NULL, 'Variant(UInt64, String)')) AS v "
            "FROM numbers(3)"
        )
        for native_writer in (0, 1):
            tbl = _query_arrow(
                sql,
                parallel=False,
                fmt="ArrowStream",
                extra_settings=(
                    "output_format_arrow_variant_as_string = 1, "
                    f"output_format_arrow_use_native_writer = {native_writer}"
                ),
            )
            self.assertEqual(tbl.schema.field("v").type, pa.string())
            self.assertEqual(tbl.column("v").to_pylist(), ["1", '"abc"', None])

    # --- LowCardinality fallback -------------------------------------------

    def test_low_cardinality_dictionary_falls_back_to_serial(self):
        # When LowCardinality is emitted as Arrow Dictionary, the converter's
        # dictionary_values map is per-instance and parallel encoding cannot
        # produce a coherent shared dictionary. We must auto fall back to
        # serial encoding and still produce the correct result.
        sql = (
            "SELECT toLowCardinality(concat('cat-', toString(number % 16))) AS c, "
            "number AS n "
            "FROM numbers(20000) "
            "SETTINGS output_format_arrow_low_cardinality_as_dictionary = 1"
        )
        # Note: the SETTINGS clause already lives inside the query, so we
        # bypass the helper that would append its own SETTINGS.
        def run(parallel: bool) -> pa.Table:
            full = sql + (
                f", max_threads = 4, max_block_size = 4096, "
                f"output_format_arrow_parallel_encoding = {1 if parallel else 0}"
            )
            data = chdb.query(full, "Arrow").bytes()
            return pa.ipc.open_file(io.BytesIO(data)).read_all()

        serial = run(parallel=False)
        parallel = run(parallel=True)

        self.assertEqual(serial.num_rows, 20000)
        self.assertTrue(
            pa.types.is_dictionary(serial.schema.field("c").type),
            "expected LowCardinality column to surface as Arrow Dictionary",
        )
        self.assertTrue(
            serial.equals(parallel),
            "parallel encoding must transparently fall back to serial for "
            "LowCardinality-as-Dictionary and produce identical bytes-decoded data",
        )

    def test_low_cardinality_without_dictionary_still_uses_parallel(self):
        # Default low_cardinality_as_dictionary = 0 strips LC at conversion
        # time, so parallel encoding stays enabled and must match serial.
        sql = (
            "SELECT toLowCardinality(concat('cat-', toString(number % 16))) AS c, "
            "number AS n "
            "FROM numbers(20000)"
        )
        self._assert_parallel_matches_serial(sql)

    # --- Edge cases ---------------------------------------------------------

    def test_empty_result_writes_valid_schema(self):
        sql = "SELECT number AS n FROM numbers(0)"
        tbl = self._assert_parallel_matches_serial(sql)
        self.assertEqual(tbl.num_rows, 0)
        self.assertEqual(tbl.column_names, ["n"])

    def test_single_thread_setting_uses_serial_path(self):
        # max_threads = 1 must short-circuit to the serial path (no thread
        # pool created) but still produce identical results.
        sql = "SELECT number AS n, toString(number) AS s FROM numbers(5000)"
        serial = _query_arrow(sql, parallel=False, threads=1)
        parallel = _query_arrow(sql, parallel=True, threads=1)
        self.assertTrue(serial.equals(parallel))

    # --- Compression codecs (writer runs on main thread) -------------------

    def test_all_compression_codecs_match_serial(self):
        sql = (
            "SELECT number AS n, toString(number) AS s, "
            "toFloat64(number) / 7 AS f FROM numbers(20000)"
        )
        for codec in ("none", "lz4_frame", "zstd"):
            with self.subTest(codec=codec):
                self._assert_parallel_matches_serial(
                    sql,
                    extra_settings=f"output_format_arrow_compression_method = '{codec}'",
                )

    # --- Extended type coverage --------------------------------------------

    def test_datetime_and_date_types_match_serial(self):
        sql = (
            "SELECT number AS k, "
            "toDate('2024-01-01') + toIntervalDay(number % 365) AS d, "
            "toDateTime('2024-01-01 00:00:00') + toIntervalSecond(number) AS dt, "
            "toDateTime64('2024-01-01 00:00:00.000', 3) "
            "  + toIntervalMillisecond(number * 7) AS dt64 "
            "FROM numbers(20000)"
        )
        tbl = self._assert_parallel_matches_serial(sql)
        self.assertEqual(tbl.num_rows, 20000)

    @unittest.skipIf(_LITE, "toDecimal128 not registered in chdb-core-lite")
    def test_decimal_types_match_serial(self):
        sql = (
            "SELECT number AS k, "
            "toDecimal32(number / 100, 4) AS d32, "
            "toDecimal64(number * 1.5, 6) AS d64, "
            "toDecimal128(number, 10) AS d128 "
            "FROM numbers(20000)"
        )
        tbl = self._assert_parallel_matches_serial(sql)
        self.assertEqual(tbl.num_rows, 20000)

    def test_fixed_string_match_serial(self):
        sql = (
            "SELECT number AS k, "
            "toFixedString(leftPad(toString(number), 8, '0'), 8) AS fs "
            "FROM numbers(20000)"
        )
        for as_fixed in (0, 1):
            with self.subTest(as_fixed=as_fixed):
                self._assert_parallel_matches_serial(
                    sql,
                    extra_settings=f"output_format_arrow_fixed_string_as_fixed_byte_array = {as_fixed}",
                )

    def test_map_type_match_serial(self):
        sql = (
            "SELECT number AS k, "
            "map('a', number, 'b', number + 1) AS m, "
            "map(toString(number % 4), arrayMap(x -> x * 2, range(toUInt32(number % 5)))) AS m2 "
            "FROM numbers(20000)"
        )
        self._assert_parallel_matches_serial(sql)

    # --- Backpressure path (in-flight cap = 4 * max_threads) ---------------

    def test_backpressure_with_many_small_chunks(self):
        # max_threads=2 → backpressure cap = 8 in-flight tasks.
        # 200 chunks of 1024 rows each greatly exceeds the cap, forcing
        # the producer to wait on the condition variable repeatedly.
        sql = "SELECT number AS n, toString(number) AS s FROM numbers(204800)"
        tbl = self._assert_parallel_matches_serial(sql, threads=2, block_size=1024)
        self.assertEqual(tbl.column("n").to_pylist()[:5], [0, 1, 2, 3, 4])
        self.assertEqual(tbl.column("n").to_pylist()[-5:], [204795, 204796, 204797, 204798, 204799])

    # --- Concurrent queries (per-instance ThreadPool lifecycle) ------------

    def test_concurrent_queries_share_no_state(self):
        # Each query owns its own ArrowBlockOutputFormat (and its own
        # ThreadPool). Running many in parallel exercises pool create/destroy,
        # ThreadGroupSwitcher state, and CurrentMetrics counters.
        from concurrent.futures import ThreadPoolExecutor

        sqls = [
            f"SELECT number AS n, toString(number * {i}) AS s FROM numbers(30000)"
            for i in range(1, 9)
        ]

        def run(sql_with_i):
            i, sql = sql_with_i
            tbl = _query_arrow(sql, parallel=True)
            return i, tbl

        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(run, enumerate(sqls)))

        for i, tbl in results:
            self.assertEqual(tbl.num_rows, 30000)
            # Cross-check value at row 12345 was multiplied by the right i.
            expected = str(12345 * (i + 1))
            self.assertEqual(tbl.column("s")[12345].as_py(), expected)

    # --- Backpressure deadlock regression -----------------------------------

    def test_backpressure_wait_drains_and_does_not_deadlock(self):
        """The parallel encoder's backpressure wait must keep draining finished
        tasks while it waits. ``in_flight`` is only decremented by the drain path,
        so a wait that merely blocks on ``in_flight >= max_in_flight`` deadlocks
        once the in-flight encoders all finish while the consumer is parked: their
        completion only notifies, it never drains, so ``in_flight`` never drops.

        The ``arrow_output_parallel_pause_first_encode`` failpoint pins the front
        (seq 0) encode task; later tasks finish and ``in_flight`` climbs to the
        cap, parking the consumer. Releasing the front task must let the query
        finish (with the bug it never returns).
        """
        fp = "arrow_output_parallel_pause_first_encode"
        ctl = chdb.connect(":memory:")
        available = (
            ctl.query(
                f"SELECT count() FROM system.fail_points WHERE name = '{fp}'", "CSV"
            ).bytes().decode().strip()
        )
        if available != "1":
            self.skipTest("failpoint infrastructure not available in this build")

        # max_threads=2 => backpressure cap = max(2, 2*4) = 8 in-flight tasks;
        # 200 chunks of 1000 rows each greatly exceeds the cap.
        sql = (
            "SELECT number, toString(number) AS s FROM numbers(200000) "
            "SETTINGS max_threads = 2, max_block_size = 1000, "
            "output_format_arrow_parallel_encoding = 1"
        )

        box = {}

        def run():
            try:
                conn = chdb.connect(":memory:")
                data = conn.query(sql, "Arrow").bytes()
                box["rows"] = pa.ipc.open_file(io.BytesIO(data)).read_all().num_rows
            except BaseException as exc:  # noqa: BLE001
                box["err"] = repr(exc)

        ctl.query(f"SYSTEM ENABLE FAILPOINT {fp}", "CSV")
        try:
            worker = threading.Thread(target=run, name="arrow-query", daemon=True)
            worker.start()

            # The front task is pinned, so the query cannot complete until we release
            # it; this window lets the consumer reach the backpressure wait.
            time.sleep(3.0)
            self.assertNotIn(
                "rows", box, "query completed while the front task was paused"
            )
            self.assertNotIn("err", box, f"query errored early: {box.get('err')}")

            ctl.query(f"SYSTEM DISABLE FAILPOINT {fp}", "CSV")
            worker.join(60)

            self.assertFalse(
                worker.is_alive(),
                "parallel Arrow encoder deadlocked: query() did not return after "
                "the front encode task was released",
            )
            self.assertNotIn("err", box, f"query failed: {box.get('err')}")
            self.assertEqual(box.get("rows"), 200000)
        finally:
            try:
                ctl.query(f"SYSTEM DISABLE FAILPOINT {fp}", "CSV")
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
