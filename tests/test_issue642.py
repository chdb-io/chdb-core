#!python3

import os
import tempfile
import unittest
from pathlib import Path

import chdb


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _row_schema(field_number, imports=""):
    return f'syntax = "proto3"; {imports} message Row {{ uint64 value = {field_number}; }}\n'


class TestIssue642(unittest.TestCase):
    """Relative `format_schema` values resolve against the `format_schema_path` option."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        # Both directories hold the same relative schema path with different field
        # numbers, so a query that reads the wrong schema returns 0 instead of the value.
        self.schemas_a = self.root / "schemas_a"
        self.schemas_b = self.root / "schemas_b"
        _write(self.schemas_a / "nested" / "row.proto", _row_schema(1))
        _write(self.schemas_b / "nested" / "row.proto", _row_schema(2))
        self.data_a = self.root / "a.bin"
        self.data_a.write_bytes(bytes((2, (1 << 3) | 0, 42)))
        self.data_b = self.root / "b.bin"
        self.data_b.write_bytes(bytes((2, (2 << 3) | 0, 7)))
        self.cwd = os.getcwd()
        os.chdir(self.root)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def _query(self, data, options=None):
        sql = (
            f"SELECT value FROM file('{data}', Protobuf, 'value UInt64') "
            "SETTINGS format_schema = 'nested/row.proto:Row'"
        )
        return chdb.query(sql, "CSV", options=options).bytes()

    def test_schema_resolved_from_format_schema_path(self):
        self.assertEqual(self._query(self.data_a, {"format_schema_path": str(self.schemas_a)}), b"42\n")

    def test_each_connection_uses_its_own_format_schema_path(self):
        self.assertEqual(self._query(self.data_a, {"format_schema_path": str(self.schemas_a)}), b"42\n")
        self.assertEqual(self._query(self.data_b, {"format_schema_path": str(self.schemas_b)}), b"7\n")

    def test_without_format_schema_path_follows_working_directory(self):
        os.chdir(self.schemas_a)
        self.assertEqual(self._query(self.data_a), b"42\n")
        os.chdir(self.schemas_b)
        self.assertEqual(self._query(self.data_b), b"7\n")

    def test_imports_resolve_from_google_protos_path(self):
        schemas = self.root / "schemas_import"
        protos = self.root / "protos"
        _write(schemas / "nested" / "row.proto", _row_schema(1, 'import "shared/common.proto";'))
        _write(protos / "shared" / "common.proto", 'syntax = "proto3"; message Common { uint64 x = 1; }\n')
        options = {"format_schema_path": str(schemas), "google_protos_path": str(protos)}
        self.assertEqual(self._query(self.data_a, options), b"42\n")

    def test_missing_format_schema_path_is_reported(self):
        missing = self.root / "missing"
        with self.assertRaisesRegex(Exception, "format_schema_path .* is not an existing directory"):
            self._query(self.data_a, {"format_schema_path": str(missing)})


if __name__ == "__main__":
    unittest.main()
