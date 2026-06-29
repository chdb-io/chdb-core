#!python3
"""Streaming INSERT into file() targets, and non-streamable formats (Parquet).

Verifies that a streaming insert writes a correct local file (read back and
compared), and that a non-streamable input format (Parquet) is transparently
buffered until finish() and produces the right rows.
"""

import os
import shutil
import unittest

import chdb

test_dir = ".tmp_test_streaming_insert_file"


class TestStreamingInsertFile(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(test_dir, ignore_errors=True)
        os.makedirs(test_dir, exist_ok=True)
        self.conn = chdb.connect(":memory:")

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(test_dir, ignore_errors=True)

    def test_insert_into_file_csv_roundtrip(self):
        path = os.path.join(test_dir, "out.csv")
        target = f"INSERT INTO FUNCTION file('{path}', 'CSV', 'a UInt64, b String')"
        with self.conn.send_insert(target, "CSV") as ins:
            ins.append("1,one\n")
            ins.append("2,two\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, 2)
        self.assertTrue(os.path.exists(path))

        # Read the file back through the engine and compare exact values.
        out = self.conn.query(
            f"SELECT a, b FROM file('{path}', 'CSV', 'a UInt64, b String') ORDER BY a",
            "CSV",
        ).data()
        self.assertEqual(out, "1,\"one\"\n2,\"two\"\n")

    def test_insert_parquet_input_buffered_until_finish(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import io
        except ImportError:
            self.skipTest("pyarrow not available")

        # Build a Parquet payload in memory.
        table = pa.table({"a": pa.array([1, 2, 3], pa.uint64()),
                          "b": pa.array(["x", "y", "z"])})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        parquet_bytes = buf.getvalue()

        self.conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = Memory")
        with self.conn.send_insert("INSERT INTO t (a, b)", "Parquet") as ins:
            # Parquet has a trailing footer => not streamable; chunks are
            # accumulated and parsed at finish(). Split into chunks to exercise that.
            mid = len(parquet_bytes) // 2
            ins.append(parquet_bytes[:mid])
            ins.append(parquet_bytes[mid:])
            res = ins.finish()
        self.assertEqual(res.rows_written, 3)
        out = self.conn.query("SELECT a, b FROM t ORDER BY a", "CSV").data()
        self.assertEqual(out, "1,\"x\"\n2,\"y\"\n3,\"z\"\n")


if __name__ == "__main__":
    unittest.main()
