#!python3
"""Streaming INSERT into an S3 object, verified against a local MinIO.

Skipped unless CHDB_S3_TEST=1. Endpoint/credentials default to the values from
ci/jobs/scripts/functional_tests/setup_minio.sh (localhost:11111, clickhouse/clickhouse,
bucket 'test'); override via CHDB_S3_ENDPOINT / CHDB_S3_ACCESS_KEY / CHDB_S3_SECRET_KEY.

Exercises both the happy path (object written, read back, values compared) and the
S3-specific cancel semantics: a multipart upload aborted mid-stream leaves no object.
"""

import os
import unittest
import urllib.error
import urllib.request
import uuid

import chdb


ENDPOINT = os.environ.get("CHDB_S3_ENDPOINT", "http://localhost:11111/test")
ACCESS_KEY = os.environ.get("CHDB_S3_ACCESS_KEY", "clickhouse")
SECRET_KEY = os.environ.get("CHDB_S3_SECRET_KEY", "clickhouse")


def _s3_available():
    """Enabled only when explicitly requested AND the endpoint is reachable, so
    enabling it in CI never produces spurious failures when MinIO is absent."""
    if os.environ.get("CHDB_S3_TEST") != "1":
        return False
    base = ENDPOINT.rsplit("/", 1)[0]  # drop bucket path -> server root
    try:
        urllib.request.urlopen(base, timeout=3)
        return True
    except urllib.error.HTTPError:
        return True  # server answered (e.g. 403/404) => reachable
    except Exception:
        return False


@unittest.skipUnless(_s3_available(), "set CHDB_S3_TEST=1 and run MinIO to enable S3 tests")
class TestStreamingInsertS3(unittest.TestCase):
    def setUp(self):
        self.conn = chdb.connect(":memory:")
        self.key = f"stream_insert_{uuid.uuid4().hex}.csv"
        self.url = f"{ENDPOINT}/{self.key}"

    def tearDown(self):
        if getattr(self, "conn", None) is not None:
            self.conn.close()

    def _s3_fn(self, structure="a UInt64, b String", fmt="CSV"):
        return f"s3('{self.url}', '{ACCESS_KEY}', '{SECRET_KEY}', '{fmt}', '{structure}')"

    def test_insert_into_s3_roundtrip(self):
        target = f"INSERT INTO FUNCTION {self._s3_fn()}"
        with self.conn.send_insert(target, "CSV") as ins:
            ins.append("1,one\n2,two\n")
            res = ins.finish()
        self.assertEqual(res.rows_written, 2)

        out = self.conn.query(
            f"SELECT a, b FROM {self._s3_fn()} ORDER BY a", "CSV"
        ).data()
        self.assertEqual(out, "1,\"one\"\n2,\"two\"\n")

    def test_s3_true_multipart_streaming_roundtrip(self):
        # TRUE streaming volume test: ~12 MB pushed in ~450 mid-size chunks, with
        # small engine blocks so data flows through sendData incrementally, and a
        # 5 MiB S3 part size (MinIO's minimum) + forced multipart so the object is
        # really uploaded via multiple UploadPart calls before Complete. Read back
        # and verify exact count and sum — proves integrity across chunk, block,
        # and upload-part boundaries.
        n = 450_000
        rows_per_chunk = 1000
        target = (
            f"INSERT INTO FUNCTION {self._s3_fn('a UInt64, b String')} "
            "SETTINGS s3_min_upload_part_size = 5242880, "
            "s3_max_single_part_upload_size = 1, "
            "max_insert_block_size = 50000, min_insert_block_size_rows = 50000, "
            "min_insert_block_size_bytes = 0"
        )
        ins = self.conn.send_insert(target, "CSV")
        chunk = []
        for i in range(n):
            chunk.append(f"{i},{'x' * 20}\n")
            if len(chunk) == rows_per_chunk:
                ins.append("".join(chunk))
                chunk = []
        if chunk:
            ins.append("".join(chunk))
        res = ins.finish()
        self.assertEqual(res.rows_written, n)

        out = self.conn.query(
            f"SELECT count(), sum(a) FROM {self._s3_fn('a UInt64, b String')}", "CSV"
        ).data().strip()
        self.assertEqual(out, f"{n},{n * (n - 1) // 2}")

    def test_cancel_multipart_leaves_no_object(self):
        # Force multipart so cancel exercises AbortMultipartUpload, and the
        # object must never materialize.
        target = (
            f"INSERT INTO FUNCTION {self._s3_fn()} "
            "SETTINGS s3_min_upload_part_size = 1, s3_max_single_part_upload_size = 1"
        )
        ins = self.conn.send_insert(target, "CSV")
        for i in range(1000):
            ins.append(f"{i},val{i}\n")
        ins.cancel()

        # Reading the (never-completed) object must fail / find nothing.
        with self.assertRaises(Exception):
            self.conn.query(f"SELECT count() FROM {self._s3_fn()}", "CSV").data()


if __name__ == "__main__":
    unittest.main()
