#!/usr/bin/env python3
"""Regression test for the C ABI streaming-query surface (Issue #143).

Reproduces chdb-io/chdb-core#143: a query-level ``SETTINGS`` clause
(e.g. ``date_time_output_format='iso'``) must be applied to the CLIENT-SIDE
output format of a STREAMING query issued via ``chdb_stream_query`` +
``chdb_stream_fetch_result``, exactly as it already is for a materialized
``chdb_query``. Before the fix the streaming path rolled the query settings
back to defaults before the output format was created at fetch time, so a
``DateTime64`` was emitted as ``2025-12-19 13:52:04.496187`` instead of the
ISO form ``2025-12-19T13:52:04.496187Z``.

Loads libchdb.so / libchdb.dylib directly via ctypes (those binaries export
the full C ABI; the Python wheel's ``_chdb.abi3.so`` is not used for these
ctypes tests). When no standalone library is found (e.g. running against an
installed wheel without the source tree) the whole suite is skipped.
"""

import ctypes
import os
import platform
import unittest


def _find_libchdb():
    """Locate libchdb.so / libchdb.dylib. Returns the path or None."""
    candidates = []
    if os.environ.get("CHDB_LIB_PATH"):
        candidates.append(os.environ["CHDB_LIB_PATH"])

    # tests/ -> repo root -> buildlib/
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    lib_name = "libchdb.dylib" if platform.system() == "Darwin" else "libchdb.so"
    candidates.append(os.path.join(repo_root, "buildlib", lib_name))

    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


_LIB_PATH = _find_libchdb()


@unittest.skipIf(
    _LIB_PATH is None,
    "libchdb.so/dylib not found; build chdb-core or set CHDB_LIB_PATH to run",
)
class TestCApiStreamQuery(unittest.TestCase):
    """Verify the C-ABI streaming-query entry points honor query SETTINGS."""

    @classmethod
    def setUpClass(cls):
        cls.lib = ctypes.CDLL(_LIB_PATH)

        cls.lib.chdb_connect.restype = ctypes.c_void_p
        cls.lib.chdb_connect.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        cls.lib.chdb_close_conn.restype = None
        cls.lib.chdb_close_conn.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

        # Materialized query (control).
        cls.lib.chdb_query.restype = ctypes.c_void_p
        cls.lib.chdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]

        # Streaming query: init + fetch loop.
        cls.lib.chdb_stream_query.restype = ctypes.c_void_p
        cls.lib.chdb_stream_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        cls.lib.chdb_stream_fetch_result.restype = ctypes.c_void_p
        cls.lib.chdb_stream_fetch_result.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        cls.lib.chdb_stream_cancel_query.restype = None
        cls.lib.chdb_stream_cancel_query.argtypes = [ctypes.c_void_p, ctypes.c_void_p]

        # Result accessors.
        cls.lib.chdb_result_buffer.restype = ctypes.POINTER(ctypes.c_char)
        cls.lib.chdb_result_buffer.argtypes = [ctypes.c_void_p]
        cls.lib.chdb_result_length.restype = ctypes.c_size_t
        cls.lib.chdb_result_length.argtypes = [ctypes.c_void_p]
        cls.lib.chdb_result_error.restype = ctypes.c_char_p
        cls.lib.chdb_result_error.argtypes = [ctypes.c_void_p]
        cls.lib.chdb_destroy_query_result.restype = None
        cls.lib.chdb_destroy_query_result.argtypes = [ctypes.c_void_p]

        # Memory-only connection: chdb.state.connect(":memory:") collapses to
        # argv = ["clickhouse"] (no --path), so do the same here.
        argv = (ctypes.c_char_p * 1)(b"clickhouse")
        cls.conn_ptr = cls.lib.chdb_connect(1, argv)
        if not cls.conn_ptr:
            raise RuntimeError("chdb_connect returned NULL")
        # chdb_connect returns chdb_connection*; deref once to get the
        # chdb_connection value the query functions expect.
        cls.conn = ctypes.c_void_p.from_address(cls.conn_ptr).value

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn_ptr", None):
            cls.lib.chdb_close_conn(ctypes.cast(cls.conn_ptr, ctypes.POINTER(ctypes.c_void_p)))

    # ------------------------------------------------------------------ helpers

    def _materialized_text(self, sql, fmt=b"TSV"):
        result = self.lib.chdb_query(self.conn, sql, fmt)
        try:
            err = self.lib.chdb_result_error(result)
            self.assertIsNone(err, err.decode("utf-8", "replace") if err else None)
            n = self.lib.chdb_result_length(result)
            buf = self.lib.chdb_result_buffer(result)
            return ctypes.string_at(buf, n).decode("utf-8") if buf and n else ""
        finally:
            self.lib.chdb_destroy_query_result(result)

    def _stream_text(self, sql, fmt=b"TSV"):
        stream = self.lib.chdb_stream_query(self.conn, sql, fmt)
        err = self.lib.chdb_result_error(stream)
        self.assertIsNone(err, err.decode("utf-8", "replace") if err else None)

        chunks = []
        try:
            while True:
                chunk = self.lib.chdb_stream_fetch_result(self.conn, stream)
                chunk_err = self.lib.chdb_result_error(chunk)
                self.assertIsNone(
                    chunk_err, chunk_err.decode("utf-8", "replace") if chunk_err else None
                )
                n = self.lib.chdb_result_length(chunk)
                if n == 0:
                    self.lib.chdb_destroy_query_result(chunk)
                    break
                buf = self.lib.chdb_result_buffer(chunk)
                chunks.append(ctypes.string_at(buf, n))
                self.lib.chdb_destroy_query_result(chunk)
        finally:
            self.lib.chdb_stream_cancel_query(self.conn, stream)
            self.lib.chdb_destroy_query_result(stream)

        return b"".join(chunks).decode("utf-8")

    # -------------------------------------------------------------------- tests

    def test_stream_query_applies_date_time_output_format_iso(self):
        iso_sql = (
            b"SELECT toDateTime64('2025-12-19 13:52:04.496187', 6, 'UTC') AS ts "
            b"SETTINGS date_time_output_format = 'iso'"
        )
        # TSV: bare value + trailing newline (no quoting, no header).
        expected_iso = "2025-12-19T13:52:04.496187Z\n"

        # Control: the materialized path already honors the SETTINGS clause.
        self.assertEqual(self._materialized_text(iso_sql), expected_iso)

        # The bug: the streaming path must produce the SAME ISO-formatted output.
        streaming = self._stream_text(iso_sql)
        # Specific checks first for clearer failure diagnostics, then exact match.
        self.assertIn("2025-12-19T13:52:04.496187Z", streaming)
        self.assertNotIn("2025-12-19 13:52:04.496187", streaming)
        self.assertEqual(streaming, expected_iso)

        # Parity: streaming and materialized must agree exactly.
        self.assertEqual(streaming, self._materialized_text(iso_sql))

        # Leak guard: a subsequent streaming query with no SETTINGS clause must
        # fall back to the default (non-ISO) form.
        default_sql = b"SELECT toDateTime64('2025-12-19 13:52:04.496187', 6, 'UTC') AS ts"
        self.assertEqual(self._stream_text(default_sql), "2025-12-19 13:52:04.496187\n")


if __name__ == "__main__":
    unittest.main()
