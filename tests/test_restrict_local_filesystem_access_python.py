#!python3
"""restrict_local_filesystem_access through the Python connection string.

The C API suite (test_restrict_local_filesystem_access.py) covers the restriction itself; this checks that Python
passes the option through and that Python-only data sources, which hand the engine objects of the host process
rather than files, keep working on a restricted connection.
"""

import os
import shutil
import tempfile
import unittest

from chdb.state.sqlitelike import connect

SECRET = "TOP_SECRET_python_5b7e"


class TestRestrictLocalFilesystemAccessPython(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="chdb_restrict_fs_py_")
        cls.secret = os.path.join(cls.dir, "secret.txt")
        with open(cls.secret, "w") as f:
            f.write(SECRET + "\n")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_connection_string_option(self):
        conn = connect(":memory:?restrict_local_filesystem_access=1")
        try:
            with self.assertRaisesRegex(Exception, "ACCESS_DENIED|Not enough privileges") as cm:
                conn.query(f"SELECT * FROM file('{self.secret}', 'LineAsString')")
            self.assertNotIn(SECRET, str(cm.exception))
            self.assertEqual(str(conn.query("SELECT 1 + 1", "CSV")).strip(), "2")
        finally:
            conn.close()

    def test_python_objects_stay_queryable(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas is not installed")
        df = pd.DataFrame({"a": [1, 2, 3]})  # noqa: F841 - referenced by the query below
        conn = connect(":memory:?restrict_local_filesystem_access=1")
        try:
            self.assertEqual(str(conn.query("SELECT sum(a) FROM Python(df)", "CSV")).strip(), "6")
        finally:
            conn.close()

    def test_without_the_option_files_stay_readable(self):
        conn = connect(":memory:")
        try:
            self.assertIn(SECRET, str(conn.query(f"SELECT * FROM file('{self.secret}', 'LineAsString')", "CSV")))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
