#!python3

import os
import shutil
import tempfile
import unittest

import chdb

ROWS = 20000
# Small segments, so parallel parsing splits the file into many of them.
SETTINGS = "SETTINGS input_format_parallel_parsing = 1, min_chunk_bytes_for_parallel_parsing = 16384"


class TestIssue334(unittest.TestCase):
    """_row_number stays correct after the first parallel parsing segment when the file has a header."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.mkdtemp()
        cls.csv = cls._write("t.csv", ",", ["id,pad"])
        cls.csv_with_types = cls._write("types.csv", ",", ["id,pad", "UInt64,String"])
        cls.tsv = cls._write("t.tsv", "\t", ["id\tpad"])
        cls.no_header = cls._write("no_header.csv", ",", [])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_dir, ignore_errors=True)

    @classmethod
    def _write(cls, name, sep, header):
        path = os.path.join(cls.tmp_dir, name)
        with open(path, "w") as f:
            for line in header:
                f.write(line + "\n")
            for i in range(ROWS):
                f.write(f"{i}{sep}abcdefghij\n")
        return path

    def _check(self, source, settings=""):
        sql = f"SELECT count(), countIf(_row_number != id) FROM {source} {SETTINGS}{settings}"
        self.assertEqual(chdb.query(sql, "CSV").bytes().decode().strip(), f"{ROWS},0")

    def test_csv_with_names(self):
        self._check(f"file('{self.csv}', 'CSVWithNames')")

    def test_csv_with_names_and_types(self):
        self._check(f"file('{self.csv_with_types}', 'CSVWithNamesAndTypes')")

    def test_csv_detected_header(self):
        self._check(f"file('{self.csv}', 'CSV')")

    def test_csv_skip_first_lines(self):
        self._check(
            f"file('{self.csv}', 'CSV', 'id UInt64, pad String')",
            ", input_format_csv_skip_first_lines = 1",
        )

    def test_tsv_with_names(self):
        self._check(f"file('{self.tsv}', 'TSVWithNames')")

    def test_tsv_skip_first_lines(self):
        self._check(
            f"file('{self.tsv}', 'TSV', 'id UInt64, pad String')",
            ", input_format_tsv_skip_first_lines = 1",
        )

    def test_csv_without_header(self):
        self._check(f"file('{self.no_header}', 'CSV', 'id UInt64, pad String')")

    def test_csv_with_names_max_threads_1(self):
        self._check(f"file('{self.csv}', 'CSVWithNames')", ", max_threads = 1")


if __name__ == "__main__":
    unittest.main()
