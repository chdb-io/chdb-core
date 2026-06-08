#!python3
"""
Tests for chdb_query_arrow / chdb_stream_query_arrow / chdb_stream_fetch_arrow.

These tests call the non-Python libchdb.so directly via ctypes, decode the
Arrow C Data Interface stream with pyarrow's `_import_from_c`, and value-check
the result.

Requires libchdb.so built from a NOT USE_PYTHON configuration with the new
arrow symbols. If libchdb.so is missing or built without them, the tests are
skipped.
"""

import ctypes
import os
import shutil
import unittest

import pyarrow as pa


def _candidate_libchdb_paths():
    here = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(here)
    names = ["libchdb.so", "libchdb.dylib"]
    yield from (os.path.join(project_root, n) for n in names)
    yield from (os.path.join(project_root, "buildlib", n) for n in names)
    on_path = shutil.which("libchdb.so")
    if on_path:
        yield on_path


def _find_libchdb_path():
    for path in _candidate_libchdb_paths():
        if os.path.exists(path):
            return path
    return None


_LIBCHDB_PATH = _find_libchdb_path()


class ArrowArrayStruct(ctypes.Structure):
    pass


# ArrowArrayStream from arrow/c/abi.h — opaque to ctypes; we only need its
# address. pyarrow will dereference it via _import_from_c.
class ArrowArrayStream(ctypes.Structure):
    _fields_ = [
        # Callbacks (pointers). Treated as opaque void*.
        ("get_schema", ctypes.c_void_p),
        ("get_next", ctypes.c_void_p),
        ("get_last_error", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


class ChdbArrowOptions(ctypes.Structure):
    _fields_ = [
        ("unsupported_as_binary", ctypes.c_int),
        ("low_cardinality_as_dictionary", ctypes.c_int),
        ("string_as_string", ctypes.c_int),
    ]


def _bind(lib):
    """Bind argtypes/restypes for the symbols we use."""
    lib.chdb_connect.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
    lib.chdb_connect.restype = ctypes.c_void_p
    lib.chdb_close_conn.argtypes = [ctypes.c_void_p]
    lib.chdb_close_conn.restype = None

    lib.chdb_query_arrow.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_void_p
    ]
    lib.chdb_query_arrow.restype = ctypes.c_void_p

    lib.chdb_stream_query_arrow.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p
    ]
    lib.chdb_stream_query_arrow.restype = ctypes.c_void_p
    lib.chdb_stream_fetch_arrow.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p
    ]
    lib.chdb_stream_fetch_arrow.restype = ctypes.c_int

    lib.chdb_destroy_query_result.argtypes = [ctypes.c_void_p]
    lib.chdb_destroy_query_result.restype = None
    lib.chdb_result_error.argtypes = [ctypes.c_void_p]
    lib.chdb_result_error.restype = ctypes.c_char_p


LIBCHDB = None
if _LIBCHDB_PATH is not None:
    _lib = ctypes.CDLL(_LIBCHDB_PATH)
    if hasattr(_lib, "chdb_query_arrow"):
        LIBCHDB = _lib
        _bind(LIBCHDB)


@unittest.skipUnless(LIBCHDB is not None,
                     f"libchdb.so with chdb_query_arrow not found "
                     f"(searched {_LIBCHDB_PATH!r})")
class TestArrowCDataOutput(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # chdb_connect() returns chdb_connection* (i.e. chdb_conn**).
        # All other APIs take chdb_connection (chdb_conn*), so we dereference
        # one level when calling them.
        argv = (ctypes.c_char_p * 2)(b"clickhouse", b"--multiquery")
        cls.conn_pp = LIBCHDB.chdb_connect(2, argv)
        assert cls.conn_pp, "chdb_connect failed"
        # Read the inner chdb_connection out of the chdb_connection* slot.
        cls.conn = ctypes.c_void_p.from_address(cls.conn_pp).value

    @classmethod
    def tearDownClass(cls):
        LIBCHDB.chdb_close_conn(cls.conn_pp)

    def _query_arrow_table(self, sql, opts=None):
        """Run sql via the new C ABI and return a pyarrow.Table."""
        stream = ArrowArrayStream()
        # `conn` is a chdb_connection* (pointer-to-pointer-to-struct). We
        # pass it through as-is — the C side dereferences once internally.
        opts_ptr = ctypes.byref(opts) if opts else None
        result = LIBCHDB.chdb_query_arrow(
            self.conn,
            sql.encode("utf-8"),
            ctypes.byref(stream),
            opts_ptr,
        )
        err = LIBCHDB.chdb_result_error(result)
        if err:
            LIBCHDB.chdb_destroy_query_result(result)
            raise RuntimeError(err.decode())

        try:
            stream_ptr = ctypes.addressof(stream)
            reader = pa.RecordBatchReader._import_from_c(stream_ptr)
            return reader.read_all()
        finally:
            LIBCHDB.chdb_destroy_query_result(result)

    def test_integers_value_correctness(self):
        sql = "SELECT number AS n FROM numbers(100)"
        tbl = self._query_arrow_table(sql)
        self.assertEqual(tbl.schema.names, ["n"])
        self.assertEqual(tbl.num_rows, 100)
        flat = tbl.column("n").combine_chunks().to_pylist()
        self.assertEqual(flat, list(range(100)))

    def test_string_default_is_utf8(self):
        sql = "SELECT 'hello' AS s"
        tbl = self._query_arrow_table(sql)
        self.assertEqual(tbl.schema.field(0).type, pa.string())

    def test_string_as_binary_when_disabled(self):
        opts = ChdbArrowOptions(0, 0, 0)  # string_as_string=0
        tbl = self._query_arrow_table("SELECT 'hello' AS s", opts)
        self.assertEqual(tbl.schema.field(0).type, pa.binary())

    def test_datetime_is_uint32(self):
        # The new C ABI matches ClickHouse format="ArrowStream" — DateTime
        # exports as raw uint32 (Unix seconds). Callers that want a
        # timezone-tagged timestamp should write toDateTime64 in SQL.
        sql = "SELECT toDateTime('2024-01-02 03:04:05','UTC') AS dt"
        tbl = self._query_arrow_table(sql)
        self.assertEqual(tbl.schema.field(0).type, pa.uint32())

    def test_datetime64_maps_to_timestamp(self):
        # The kernel's DateTime64 path produces arrow::timestamp(unit, tz).
        sql = "SELECT toDateTime64('2024-01-02 03:04:05', 0, 'UTC') AS dt"
        tbl = self._query_arrow_table(sql)
        ty = tbl.schema.field(0).type
        self.assertTrue(pa.types.is_timestamp(ty), f"expected timestamp, got {ty}")
        self.assertEqual(ty.unit, "s")
        self.assertEqual(ty.tz, "UTC")

    def test_empty_result(self):
        tbl = self._query_arrow_table(
            "SELECT number FROM numbers(10) WHERE number > 100")
        self.assertEqual(tbl.num_rows, 0)
        self.assertEqual(tbl.schema.names, ["number"])

    def test_low_cardinality_materialized_by_default(self):
        sql = "SELECT toLowCardinality(toString(number % 3)) AS lc FROM numbers(10)"
        tbl = self._query_arrow_table(sql)
        # Default low_cardinality_as_dictionary=0 -> materialized to utf8
        self.assertEqual(tbl.schema.field(0).type, pa.string())

    def test_streaming_total_rows_match(self):
        # Use the streaming API and assert total row count equals materialized path.
        sql = "SELECT number FROM numbers(1000)"
        stream_result = LIBCHDB.chdb_stream_query_arrow(
            self.conn, sql.encode("utf-8"), None)
        err = LIBCHDB.chdb_result_error(stream_result)
        if err:
            LIBCHDB.chdb_destroy_query_result(stream_result)
            self.fail(err.decode())

        try:
            total_rows = 0
            for _ in range(2048):  # safety bound
                batch = ArrowArrayStream()
                state = LIBCHDB.chdb_stream_fetch_arrow(
                    self.conn, stream_result, ctypes.byref(batch))
                if state != 0:
                    break
                ptr = ctypes.addressof(batch)
                reader = pa.RecordBatchReader._import_from_c(ptr)
                tbl = reader.read_all()
                if tbl.num_rows == 0:
                    break
                total_rows += tbl.num_rows
        finally:
            LIBCHDB.chdb_destroy_query_result(stream_result)

        self.assertEqual(total_rows, 1000)


if __name__ == "__main__":
    unittest.main()
