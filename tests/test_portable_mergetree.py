#!/usr/bin/env python3

import json
import tempfile
import unittest
import uuid
from pathlib import Path

from chdb import session as chs


class TestPortableMergeTree(unittest.TestCase):
    def test_readonly_local_table_disk(self):
        with tempfile.TemporaryDirectory(prefix="chdb-portable-mergetree-") as tmp:
            root = Path(tmp)

            writer = chs.Session(str(root / "writer"))
            try:
                writer.query("CREATE DATABASE source ENGINE = Atomic")
                writer.query(
                    "CREATE TABLE source.result (id UInt64, cohort UInt8) "
                    "ENGINE = MergeTree PARTITION BY cohort ORDER BY id"
                )
                writer.query(
                    "INSERT INTO source.result "
                    "SELECT number, number % 3 FROM numbers(10000)"
                )
                raw = str(
                    writer.query(
                        "SELECT data_paths FROM system.tables "
                        "WHERE database = 'source' AND name = 'result'",
                        "JSONEachRow",
                    )
                ).strip()
                table_path = Path(json.loads(raw)["data_paths"][0])
            finally:
                writer.close()

            reader = chs.Session(str(root / "reader"))
            try:
                reader.query("CREATE DATABASE saved ENGINE = Atomic")
                escaped_path = (str(table_path).rstrip("/") + "/").replace("'", "''")
                with self.assertRaisesRegex(Exception, "must be read-only"):
                    reader.query(
                        f"ATTACH TABLE saved.writable_result UUID '{uuid.uuid4()}' "
                        "(id UInt64, cohort UInt8) "
                        "ENGINE = MergeTree PARTITION BY cohort ORDER BY id "
                        "SETTINGS disk = disk("
                        "type = object_storage, object_storage_type = local, "
                        f"path = '{escaped_path}', metadata_type = plain), "
                        "table_disk = true, table_readonly = true"
                    )

                reader.query(
                    f"ATTACH TABLE saved.result UUID '{uuid.uuid4()}' "
                    "(id UInt64, cohort UInt8) "
                    "ENGINE = MergeTree PARTITION BY cohort ORDER BY id "
                    "SETTINGS disk = disk("
                    "type = object_storage, object_storage_type = local, "
                    f"path = '{escaped_path}', metadata_type = plain, "
                    "readonly = true, read_only = true), "
                    "table_disk = true, table_readonly = true"
                )

                result = str(
                    reader.query(
                        "SELECT cohort, count() AS rows FROM saved.result "
                        "GROUP BY cohort ORDER BY cohort",
                        "JSONEachRow",
                    )
                ).strip().splitlines()
                self.assertEqual(
                    [
                        {"cohort": 0, "rows": 3334},
                        {"cohort": 1, "rows": 3333},
                        {"cohort": 2, "rows": 3333},
                    ],
                    [json.loads(row) for row in result],
                )

                with self.assertRaisesRegex(Exception, "read-only|readonly"):
                    reader.query("INSERT INTO saved.result VALUES (10001, 1)")
            finally:
                reader.close()


if __name__ == "__main__":
    unittest.main()
