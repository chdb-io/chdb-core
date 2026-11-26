"""Native PyArrow query output through the chDB Python extension."""

import unittest

import pyarrow as pa

import chdb


class TestPythonArrowDirect(unittest.TestCase):
    def test_table_input_and_materialized_output(self):
        conn = chdb._chdb.connect()
        try:
            tbl = pa.table({"id": [1, 2], "name": ["é", None]})
            reader = conn.query_arrow_reader(
                "SELECT id, name FROM Python(tbl) ORDER BY id"
            )
            result = reader.read_all()
            reader.close()
            self.assertEqual(result.column("id").to_pylist(), [1, 2])
            self.assertEqual(result.column("name").to_pylist(), ["é", None])
            self.assertEqual(result.schema.names, ["id", "name"])
        finally:
            conn.close()

    def test_empty_result_retains_schema_after_close(self):
        conn = chdb._chdb.connect()
        try:
            reader = conn.query_arrow_reader("SELECT toInt64(7) AS n WHERE 0")
        finally:
            conn.close()
        result = reader.read_all()
        reader.close()
        self.assertEqual(result.num_rows, 0)
        self.assertEqual(result.schema.names, ["n"])
        self.assertEqual(result.schema.field("n").type, pa.int64())

    def test_streamed_batches_and_early_cancel(self):
        conn = chdb._chdb.connect()
        try:
            handle = conn.send_query_arrow("SELECT number FROM numbers(1000)")
            try:
                batches = []
                while True:
                    reader = conn.streaming_fetch_arrow(handle)
                    try:
                        batches.append(reader.read_next_batch())
                    except StopIteration:
                        break
                    finally:
                        reader.close()
                self.assertEqual(sum(len(batch) for batch in batches), 1000)
                self.assertEqual(batches[0].column(0)[0].as_py(), 0)
            finally:
                conn.streaming_cancel_query(handle)
                del handle

            handle = conn.send_query_arrow("SELECT number FROM numbers(100000)")
            try:
                reader = conn.streaming_fetch_arrow(handle)
                self.assertGreater(len(reader.read_next_batch()), 0)
                reader.close()
            finally:
                conn.streaming_cancel_query(handle)
                del handle
            self.assertEqual(conn.query("SELECT 1", "CSV").bytes().strip(), b"1")
        finally:
            conn.close()

    def test_parameters_options_and_errors(self):
        conn = chdb._chdb.connect()
        try:
            reader = conn.query_arrow_reader(
                "SELECT {value:Int64} AS n, 'hi' AS s",
                params={"value": 42},
                options={"string_as_string": False},
            )
            table = reader.read_all()
            reader.close()
            self.assertEqual(table.column("n").to_pylist(), [42])
            self.assertEqual(table.schema.field("s").type, pa.binary())

            with self.assertRaisesRegex(ValueError, "Unknown Arrow option"):
                conn.query_arrow_reader("SELECT 1", options={"unknown": True})
            with self.assertRaisesRegex(RuntimeError, "UNKNOWN_TABLE|does not exist"):
                conn.query_arrow_reader("SELECT * FROM no_such_arrow_table")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
