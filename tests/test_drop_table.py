#!python3

import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from chdb import session

test_dir1 = "test_drop_table"

class TestDropTable(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(test_dir1, ignore_errors=True)
        return super().tearDown()

    def test_drop_table(self):
        shutil.rmtree(test_dir1, ignore_errors=True)
        sess = session.Session(test_dir1)

        sess.query('''
            CREATE TABLE test_table_1
            (
                value String
            ) ENGINE = MergeTree()
            ORDER BY value
        ''')
        sess.query("INSERT INTO test_table_1 VALUES ('test')")
        sess.query("DROP TABLE test_table_1")

        sess.close()

        sess = session.Session(test_dir1)

        sess.query('''
            CREATE TABLE test_table_2
            (
                value String
            ) ENGINE = MergeTree()
            ORDER BY value
        ''')
        sess.query("INSERT INTO test_table_2 VALUES ('test')")
        sess.query("DROP TABLE test_table_2 SYNC")

        sess.close()

    def test_drop_table_sync_in_first_persistent_session(self):
        child = textwrap.dedent(
            """
            import sys
            from chdb import session

            sess = session.Session(sys.argv[1])
            sess.query(
                "CREATE TABLE first_session "
                "(value String) ENGINE = MergeTree ORDER BY value"
            )
            sess.query("DROP TABLE first_session SYNC")
            sess.close()
            """
        )

        with tempfile.TemporaryDirectory(prefix="chdb-drop-sync-") as state_path:
            result = subprocess.run(
                [sys.executable, "-c", child, state_path],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

        self.assertEqual(
            result.returncode,
            0,
            f"child stdout:\n{result.stdout}\nchild stderr:\n{result.stderr}",
        )


if __name__ == '__main__':
    unittest.main()
