#!/usr/bin/env python3
"""C-ABI tests for the streaming INSERT surface (chdb_stream_insert family).

Loads libchdb.so directly via ctypes (the standalone library exports the C ABI;
the Python wheel's _chdb.abi3.so only exports PyInit__chdb). Skipped when no
standalone library is found; set CHDB_LIB_PATH to point at one.

Exercises: full lifecycle (insert -> append -> done -> rows_written -> destroy),
binary-safe length-respecting append, init error reporting, and cancel + safe
double-destroy.
"""

import ctypes
import os
import platform
import unittest

CHDBSuccess = 0
CHDBError = 1


def _find_libchdb():
    candidates = []
    if os.environ.get("CHDB_LIB_PATH"):
        candidates.append(os.environ["CHDB_LIB_PATH"])
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    lib_name = "libchdb.dylib" if platform.system() == "Darwin" else "libchdb.so"
    candidates.append(os.path.join(repo_root, "buildlib", lib_name))
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


_LIB_PATH = _find_libchdb()


@unittest.skipIf(_LIB_PATH is None, "libchdb.so/dylib not found; set CHDB_LIB_PATH")
class TestCApiStreamInsert(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lib = ctypes.CDLL(_LIB_PATH)
        lib = cls.lib

        lib.chdb_connect.restype = ctypes.c_void_p
        lib.chdb_connect.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        lib.chdb_close_conn.restype = None
        lib.chdb_close_conn.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

        lib.chdb_query.restype = ctypes.c_void_p
        lib.chdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]

        lib.chdb_result_buffer.restype = ctypes.POINTER(ctypes.c_char)
        lib.chdb_result_buffer.argtypes = [ctypes.c_void_p]
        lib.chdb_result_length.restype = ctypes.c_size_t
        lib.chdb_result_length.argtypes = [ctypes.c_void_p]
        lib.chdb_result_error.restype = ctypes.c_char_p
        lib.chdb_result_error.argtypes = [ctypes.c_void_p]
        lib.chdb_result_rows_written.restype = ctypes.c_uint64
        lib.chdb_result_rows_written.argtypes = [ctypes.c_void_p]
        lib.chdb_destroy_query_result.restype = None
        lib.chdb_destroy_query_result.argtypes = [ctypes.c_void_p]

        # Streaming INSERT
        lib.chdb_stream_insert_n.restype = ctypes.c_void_p
        lib.chdb_stream_insert_n.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t,
        ]
        lib.chdb_stream_append.restype = ctypes.c_int
        lib.chdb_stream_append.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
        lib.chdb_stream_done.restype = ctypes.c_void_p
        lib.chdb_stream_done.argtypes = [ctypes.c_void_p]
        lib.chdb_stream_cancel_insert.restype = None
        lib.chdb_stream_cancel_insert.argtypes = [ctypes.c_void_p]
        lib.chdb_stream_insert_error.restype = ctypes.c_char_p
        lib.chdb_stream_insert_error.argtypes = [ctypes.c_void_p]
        lib.chdb_destroy_insert_stream.restype = None
        lib.chdb_destroy_insert_stream.argtypes = [ctypes.c_void_p]

        argv = (ctypes.c_char_p * 1)(b"clickhouse")
        cls.conn_ptr = lib.chdb_connect(1, argv)
        if not cls.conn_ptr:
            raise RuntimeError("chdb_connect returned NULL")
        cls.conn = ctypes.c_void_p.from_address(cls.conn_ptr).value

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn_ptr", None):
            cls.lib.chdb_close_conn(ctypes.cast(cls.conn_ptr, ctypes.POINTER(ctypes.c_void_p)))

    def _query(self, sql, fmt=b"CSV"):
        result = self.lib.chdb_query(self.conn, sql, fmt)
        try:
            err = self.lib.chdb_result_error(result)
            if err:
                raise RuntimeError(err.decode("utf-8", "replace"))
            n = self.lib.chdb_result_length(result)
            buf = self.lib.chdb_result_buffer(result)
            return ctypes.string_at(buf, n) if (buf and n) else b""
        finally:
            self.lib.chdb_destroy_query_result(result)

    def _append(self, stream, data, length=None):
        if length is None:
            length = len(data)
        buf = (ctypes.c_char * len(data)).from_buffer_copy(data) if data else None
        return self.lib.chdb_stream_append(stream, buf, length)

    def test_full_lifecycle_rows_written_and_values(self):
        self._query(b"CREATE TABLE c1 (a UInt64, b String) ENGINE = Memory")
        q = b"INSERT INTO c1 (a, b)"
        fmt = b"CSV"
        stream = self.lib.chdb_stream_insert_n(self.conn, q, len(q), fmt, len(fmt))
        self.assertTrue(stream)
        self.assertIsNone(self.lib.chdb_stream_insert_error(stream))

        self.assertEqual(self._append(stream, b"1,one\n"), CHDBSuccess)
        self.assertEqual(self._append(stream, b"2,two\n"), CHDBSuccess)

        result = self.lib.chdb_stream_done(stream)
        try:
            self.assertFalse(self.lib.chdb_result_error(result))
            self.assertEqual(self.lib.chdb_result_rows_written(result), 2)
        finally:
            self.lib.chdb_destroy_query_result(result)
        self.lib.chdb_destroy_insert_stream(stream)

        self.assertEqual(self._query(b"SELECT a, b FROM c1 ORDER BY a"),
                         b'1,"one"\n2,"two"\n')

    def test_append_respects_length_binary_safe(self):
        # The buffer carries trailing garbage beyond `length`; only the first
        # `length` bytes must be parsed (proves we do not rely on NUL termination).
        self._query(b"CREATE TABLE c2 (a UInt64) ENGINE = Memory")
        q = b"INSERT INTO c2 (a)"
        fmt = b"CSV"
        stream = self.lib.chdb_stream_insert_n(self.conn, q, len(q), fmt, len(fmt))
        payload = b"1\n2\n3\n" + b"\x00THIS_IS_GARBAGE_BEYOND_LEN"
        self.assertEqual(self._append(stream, payload, length=6), CHDBSuccess)
        result = self.lib.chdb_stream_done(stream)
        try:
            self.assertFalse(self.lib.chdb_result_error(result))
            self.assertEqual(self.lib.chdb_result_rows_written(result), 3)
        finally:
            self.lib.chdb_destroy_query_result(result)
        self.lib.chdb_destroy_insert_stream(stream)
        self.assertEqual(self._query(b"SELECT sum(a) FROM c2"), b"6\n")

    def test_init_error_on_bad_target(self):
        q = b"INSERT INTO does_not_exist (a)"
        fmt = b"CSV"
        stream = self.lib.chdb_stream_insert_n(self.conn, q, len(q), fmt, len(fmt))
        self.assertTrue(stream)  # handle is non-NULL even on init failure
        err = self.lib.chdb_stream_insert_error(stream)
        self.assertIsNotNone(err)
        self.lib.chdb_destroy_insert_stream(stream)

    def test_cancel_and_double_destroy_safe(self):
        self._query(b"CREATE TABLE c3 (a UInt64) ENGINE = Memory")
        q = b"INSERT INTO c3 (a)"
        fmt = b"CSV"
        stream = self.lib.chdb_stream_insert_n(self.conn, q, len(q), fmt, len(fmt))
        self.assertEqual(self._append(stream, b"1\n"), CHDBSuccess)
        self.lib.chdb_stream_cancel_insert(stream)
        # destroy after cancel must be safe (destroy internally re-cancels, guarded).
        self.lib.chdb_destroy_insert_stream(stream)
        # Connection still usable.
        self.assertEqual(self._query(b"SELECT 1"), b"1\n")


if __name__ == "__main__":
    unittest.main()
