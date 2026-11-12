#!python3

import unittest
import shutil
import os
import sys
from chdb import session


test_dir1 = ".test_open_session_after_failure"
if sys.platform == "win32":
    # An administrator can write almost anywhere on Windows (the CI runners' "/usr/bin" is a
    # directory they may create), but no one can create a directory below a regular file.
    test_dir2 = os.path.join(sys.executable, "db")
else:
    test_dir2 = "/usr/bin"


class TestStateful(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_dir1, ignore_errors=True)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(test_dir1, ignore_errors=True)
        return super().tearDown()

    def test_path(self):
        # Test that creating session with invalid path (read-only directory) raises exception
        if not os.access(test_dir2, os.W_OK):
            with self.assertRaises(Exception):
                sess = session.Session(test_dir2)

        # Test that creating session with valid path works after failure
        sess = session.Session(test_dir1)

        ret = sess.query("select 'aaaaa'")
        self.assertEqual(str(ret), "\"aaaaa\"\n")

        sess.close()


if __name__ == '__main__':
    unittest.main()
