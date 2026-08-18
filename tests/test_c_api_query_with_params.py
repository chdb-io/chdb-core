#!/usr/bin/env python3
"""Regression tests for the C ABI parameter-binding surface (Issue #73).

Exercises:
  * chdb_query_with_params         (NUL-terminated params)
  * chdb_query_with_params_n       (length-prefixed; binary-safe String params)
  * chdb_stream_query_with_params  (streaming variant)
  * chdb_stream_query_with_params_n

These call the same engine plumbing as Python's ``chdb.query(..., params={...})``
(``ChdbClient::setQueryParameters``/``clearQueryParameters`` + ``QueryParameterGuard``);
they're the C-ABI siblings used by chdb-node / chdb-go / chdb-rust.

Loads libchdb.so / libchdb.dylib directly via ctypes (those binaries export the
full C ABI; the Python wheel's ``_chdb.abi3.so`` is not used for these ctypes
tests). When no standalone library is found (e.g. running against an installed
wheel without the source tree) the whole suite is skipped.
"""

import ctypes
import os
import platform
import sys
import unittest


def _find_libchdb():
    """Locate libchdb.so / libchdb.dylib. Returns the path or None."""
    candidates = []
    if os.environ.get("CHDB_LIB_PATH"):
        candidates.append(os.environ["CHDB_LIB_PATH"])

    # tests/ → repo root → buildlib/
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
class TestCApiQueryWithParams(unittest.TestCase):
    """Verify the C-ABI parameter-binding entry points."""

    @classmethod
    def setUpClass(cls):
        cls.lib = ctypes.CDLL(_LIB_PATH)

        # chdb_connect(int argc, char ** argv) -> chdb_connection *
        cls.lib.chdb_connect.restype = ctypes.c_void_p
        cls.lib.chdb_connect.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]

        cls.lib.chdb_close_conn.restype = None
        cls.lib.chdb_close_conn.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

        cls.lib.chdb_query_with_params.restype = ctypes.c_void_p
        cls.lib.chdb_query_with_params.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_size_t,
        ]
        cls.lib.chdb_query_with_params_n.restype = ctypes.c_void_p
        cls.lib.chdb_query_with_params_n.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_size_t,
        ]
        cls.lib.chdb_stream_query_with_params.restype = ctypes.c_void_p
        cls.lib.chdb_stream_query_with_params.argtypes = (
            cls.lib.chdb_query_with_params.argtypes
        )
        cls.lib.chdb_stream_query_with_params_n.restype = ctypes.c_void_p
        cls.lib.chdb_stream_query_with_params_n.argtypes = (
            cls.lib.chdb_query_with_params_n.argtypes
        )

        # Result accessors
        cls.lib.chdb_result_buffer.restype = ctypes.POINTER(ctypes.c_char)
        cls.lib.chdb_result_buffer.argtypes = [ctypes.c_void_p]
        cls.lib.chdb_result_length.restype = ctypes.c_size_t
        cls.lib.chdb_result_length.argtypes = [ctypes.c_void_p]
        cls.lib.chdb_result_error.restype = ctypes.c_char_p
        cls.lib.chdb_result_error.argtypes = [ctypes.c_void_p]
        cls.lib.chdb_destroy_query_result.restype = None
        cls.lib.chdb_destroy_query_result.argtypes = [ctypes.c_void_p]

        # Streaming
        cls.lib.chdb_stream_fetch_result.restype = ctypes.c_void_p
        cls.lib.chdb_stream_fetch_result.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        cls.lib.chdb_stream_cancel_query.restype = None
        cls.lib.chdb_stream_cancel_query.argtypes = [ctypes.c_void_p, ctypes.c_void_p]

        # Build a memory-only connection. ``chdb.state.connect(":memory:")`` collapses
        # to argv = ["clickhouse"] (no --path), so do the same here.
        argv = (ctypes.c_char_p * 1)(b"clickhouse")
        cls.conn_ptr = cls.lib.chdb_connect(1, argv)
        if not cls.conn_ptr:
            raise RuntimeError("chdb_connect returned NULL")
        # chdb_connect returns chdb_connection*; deref once to get the chdb_connection
        # value the *query* functions expect.
        cls.conn = ctypes.c_void_p.from_address(cls.conn_ptr).value

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn_ptr", None):
            cls.lib.chdb_close_conn(ctypes.cast(cls.conn_ptr, ctypes.POINTER(ctypes.c_void_p)))

    # ------------------------------------------------------------------ helpers

    def _result_bytes(self, result_ptr):
        """Pull (error_msg, buffer_bytes) from a chdb_result*. Caller destroys."""
        err = self.lib.chdb_result_error(result_ptr)
        if err:
            return err.decode("utf-8", errors="replace"), b""
        n = self.lib.chdb_result_length(result_ptr)
        buf = self.lib.chdb_result_buffer(result_ptr)
        if not buf or n == 0:
            return None, b""
        return None, ctypes.string_at(buf, n)

    def _materialized_query(self, sql, names, values, fmt=b"CSV"):
        c_names = (ctypes.c_char_p * len(names))(*names)
        c_values = (ctypes.c_char_p * len(values))(*values)
        result = self.lib.chdb_query_with_params(
            self.conn, sql, fmt, c_names, c_values, len(names)
        )
        try:
            return self._result_bytes(result)
        finally:
            self.lib.chdb_destroy_query_result(result)

    def _materialized_query_n(self, sql, names, values, fmt=b"CSV"):
        c_names = (ctypes.c_char_p * len(names))(*names)
        c_values = (ctypes.c_char_p * len(values))(*values)
        c_name_lens = (ctypes.c_size_t * len(names))(*(len(n) for n in names))
        c_value_lens = (ctypes.c_size_t * len(values))(*(len(v) for v in values))
        result = self.lib.chdb_query_with_params_n(
            self.conn,
            sql,
            len(sql),
            fmt,
            len(fmt),
            c_names,
            c_name_lens,
            c_values,
            c_value_lens,
            len(names),
        )
        try:
            return self._result_bytes(result)
        finally:
            self.lib.chdb_destroy_query_result(result)

    # ------------------------------------------------------------------- tests

    def test_int64_param_returns_bound_value(self):
        err, body = self._materialized_query(
            b"SELECT {x:Int64} AS v",
            names=[b"x"],
            values=[b"42"],
        )
        self.assertIsNone(err)
        self.assertEqual(body, b"42\n")

    def test_string_param_returns_bound_value(self):
        err, body = self._materialized_query(
            b"SELECT {s:String} AS v",
            names=[b"s"],
            values=[b"hello"],
        )
        self.assertIsNone(err)
        self.assertEqual(body, b'"hello"\n')

    def test_date_param_arithmetic_returns_expected_value(self):
        # Mirrors test_query_params.test_connection_query_with_params but via the C ABI.
        err, body = self._materialized_query(
            b"SELECT toDate({d:Date}) + 1 AS d",
            names=[b"d"],
            values=[b"2025-01-01"],
        )
        self.assertIsNone(err)
        self.assertEqual(body, b'"2025-01-02"\n')

    def test_multiple_params_match_python_semantics(self):
        err, body = self._materialized_query(
            b"SELECT {x:UInt64} + {y:UInt64} AS total",
            names=[b"x", b"y"],
            values=[b"5", b"7"],
        )
        self.assertIsNone(err)
        self.assertEqual(body, b"12\n")

    def test_missing_param_returns_error_not_crash(self):
        err, body = self._materialized_query(
            b"SELECT {x:UInt64} AS v",
            names=[],
            values=[],
        )
        self.assertIsNotNone(err)
        self.assertIn("Substitution", err)
        self.assertEqual(body, b"")

    def test_invalid_type_returns_error_not_crash(self):
        err, body = self._materialized_query(
            b"SELECT {x:UInt64} AS v",
            names=[b"x"],
            values=[b"not-a-number"],
        )
        self.assertIsNotNone(err)
        self.assertIn("cannot be parsed as UInt64", err)
        self.assertEqual(body, b"")

    def test_params_cleared_after_call(self):
        # First call binds {x}; second call uses the same param name with a different
        # value. If RAII cleanup ran, this works. If parameters leaked, the second
        # call's behavior would depend on residual state from the first.
        err1, body1 = self._materialized_query(
            b"SELECT {x:Int64} AS v", names=[b"x"], values=[b"1"]
        )
        err2, body2 = self._materialized_query(
            b"SELECT {x:Int64} AS v", names=[b"x"], values=[b"99"]
        )
        self.assertIsNone(err1)
        self.assertIsNone(err2)
        self.assertEqual(body1, b"1\n")
        self.assertEqual(body2, b"99\n")

        # Now call WITHOUT params — if cleanup didn't run, leftover {x} from the
        # previous call would still satisfy the substitution. Expect an error.
        err3, body3 = self._materialized_query(
            b"SELECT {x:Int64} AS v", names=[], values=[]
        )
        self.assertIsNotNone(err3, "expected 'Substitution `x` is not set' after RAII cleanup")
        self.assertIn("Substitution", err3)
        self.assertEqual(body3, b"")

    def test_binary_safe_string_param_via_n_variant(self):
        # String param contains an embedded NUL — exercises the _n variant's
        # binary-safe path. Use base64 to make the assertion deterministic
        # without dragging UTF-8 / CSV-escape rules into the test.
        raw = b"ab\x00cd"
        err, body = self._materialized_query_n(
            b"SELECT base64Encode({s:String}) AS v",
            names=[b"s"],
            values=[raw],
        )
        self.assertIsNone(err)
        # base64("ab\x00cd") = "YWIAY2Q="
        self.assertEqual(body, b'"YWIAY2Q="\n')

    def test_zero_param_count_falls_through_to_plain_query(self):
        # Passing NULL arrays + count=0 should behave like chdb_query.
        result = self.lib.chdb_query_with_params(
            self.conn, b"SELECT 1 AS v", b"CSV", None, None, 0
        )
        try:
            err, body = self._result_bytes(result)
        finally:
            self.lib.chdb_destroy_query_result(result)
        self.assertIsNone(err)
        self.assertEqual(body, b"1\n")

    def test_null_param_pointers_with_nonzero_count_returns_error_not_crash(self):
        # Defensive: param_count > 0 with NULL arrays must not segfault.
        result = self.lib.chdb_query_with_params(
            self.conn, b"SELECT {x:Int64}", b"CSV", None, None, 1
        )
        try:
            err, body = self._result_bytes(result)
        finally:
            self.lib.chdb_destroy_query_result(result)
        self.assertIsNotNone(err)
        self.assertEqual(body, b"")

    def test_duplicate_param_names_last_wins(self):
        # NameToNameMap == std::unordered_map, so the last occurrence of a key wins.
        # This matches the documented Python behavior (dict-merge semantics).
        err, body = self._materialized_query(
            b"SELECT {x:Int64} AS v",
            names=[b"x", b"x"],
            values=[b"1", b"42"],
        )
        self.assertIsNone(err)
        self.assertEqual(body, b"42\n")

    # ------------------------------------------------------------- streaming

    def _stream_to_bytes(self, sql, names, values, fmt=b"CSV"):
        c_names = (ctypes.c_char_p * len(names))(*names)
        c_values = (ctypes.c_char_p * len(values))(*values)
        stream = self.lib.chdb_stream_query_with_params(
            self.conn, sql, fmt, c_names, c_values, len(names)
        )
        err = self.lib.chdb_result_error(stream)
        if err:
            err_str = err.decode("utf-8", errors="replace")
            self.lib.chdb_destroy_query_result(stream)
            return err_str, b""

        chunks = []
        try:
            while True:
                chunk = self.lib.chdb_stream_fetch_result(self.conn, stream)
                chunk_err = self.lib.chdb_result_error(chunk)
                if chunk_err:
                    self.lib.chdb_destroy_query_result(chunk)
                    break
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

        return None, b"".join(chunks)

    def test_streaming_param_binding_matches_materialized(self):
        err, body = self._stream_to_bytes(
            b"SELECT {x:UInt64} AS v",
            names=[b"x"],
            values=[b"11"],
        )
        self.assertIsNone(err)
        self.assertEqual(body, b"11\n")


if __name__ == "__main__":
    if _LIB_PATH is None:
        print(
            "libchdb.so/dylib not found; build chdb-core (or set CHDB_LIB_PATH) to run this suite.",
            file=sys.stderr,
        )
        sys.exit(0)
    unittest.main()
