#!/usr/bin/env python3

import unittest
import shutil
import os
import threading
from chdb import session
from utils import is_musl_linux


test_concurrent_dir = ".tmp_test_session_concurrency"
test_concurrent_dir_2 = ".tmp_test_session_concurrency_different"


class TestSessionConcurrency(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_concurrent_dir, ignore_errors=True)
        shutil.rmtree(test_concurrent_dir_2, ignore_errors=True)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(test_concurrent_dir, ignore_errors=True)
        shutil.rmtree(test_concurrent_dir_2, ignore_errors=True)
        return super().tearDown()

    def test_multiple_sessions_same_path(self):
        sess1 = session.Session(test_concurrent_dir)
        sess1.query("CREATE DATABASE IF NOT EXISTS test_db")
        sess1.query("CREATE TABLE IF NOT EXISTS test_db.data (id Int32, value String) ENGINE = MergeTree() ORDER BY id")
        sess1.query("INSERT INTO test_db.data VALUES (1, 'first')")
        sess2 = session.Session(test_concurrent_dir)
        result1 = sess1.query("SELECT * FROM test_db.data ORDER BY id", "CSV")
        self.assertIn("1", str(result1))
        self.assertIn("first", str(result1))
        result2 = sess2.query("SELECT * FROM test_db.data ORDER BY id", "CSV")
        self.assertIn("1", str(result2))
        self.assertIn("first", str(result2))
        sess2.query("INSERT INTO test_db.data VALUES (2, 'second')")
        result1 = sess1.query("SELECT * FROM test_db.data ORDER BY id", "CSV")
        self.assertIn("1", str(result1))
        self.assertIn("2", str(result1))
        sess1.close()
        sess2.close()

    def test_sessions_are_thread_safe(self):
        sess = session.Session(test_concurrent_dir)
        sess.query("CREATE DATABASE IF NOT EXISTS test_db")
        sess.query("CREATE TABLE IF NOT EXISTS test_db.shared_counter (id Int32, thread_id Int32) ENGINE = MergeTree() ORDER BY id")

        errors = []

        def shared_session_worker(thread_id):
            try:
                for i in range(3):
                    sess.query(f"INSERT INTO test_db.shared_counter VALUES ({i}, {thread_id})")
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = []
        for i in range(3):
            t = threading.Thread(target=shared_session_worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Unexpected errors when sharing session across threads: {errors}")

        result = sess.query("SELECT COUNT(*) FROM test_db.shared_counter")
        self.assertIn("9", str(result))

        sess.close()

    def test_correct_multi_threaded_access(self):
        setup_sess = session.Session(test_concurrent_dir)
        setup_sess.query("CREATE DATABASE IF NOT EXISTS test_db")
        setup_sess.query("CREATE TABLE IF NOT EXISTS test_db.thread_data (thread_id Int32, value Int32) ENGINE = MergeTree() ORDER BY (thread_id, value)")
        setup_sess.close()

        results = []
        errors = []

        def worker(thread_id):
            try:
                thread_sess = session.Session(test_concurrent_dir)
                for i in range(5):
                    thread_sess.query(f"INSERT INTO test_db.thread_data VALUES ({thread_id}, {i})")

                result = thread_sess.query(f"SELECT COUNT(*) FROM test_db.thread_data WHERE thread_id = {thread_id}")
                results.append((thread_id, result))

                thread_sess.close()
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(3):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertEqual(len(results), 3)

        verify_sess = session.Session(test_concurrent_dir)
        final_count = verify_sess.query("SELECT COUNT(*) FROM test_db.thread_data")
        self.assertIn("15", str(final_count))  # 3 threads * 5 inserts each
        verify_sess.close()

    def test_session_reopen_after_close(self):
        sess1 = session.Session(test_concurrent_dir)
        sess1.query("CREATE TABLE IF NOT EXISTS test (id Int32) ENGINE = MergeTree() ORDER BY id")
        sess1.query("INSERT INTO test VALUES (1)")
        sess1.close()

        sess2 = session.Session(test_concurrent_dir)
        result = sess2.query("SELECT * FROM test")
        self.assertIn("1", str(result))
        sess2.close()
        sess3 = session.Session(test_concurrent_dir)
        result = sess3.query("SELECT * FROM test")
        self.assertIn("1", str(result))
        sess3.close()

    @unittest.skipIf(is_musl_linux(), "Skip test on musl systems")
    def test_session_path_consistency(self):
        sess1 = session.Session(test_concurrent_dir)
        sess2 = session.Session(test_concurrent_dir_2)
        try:
            sess1.query("CREATE TABLE items (id Int32, src String) ENGINE = MergeTree() ORDER BY id")
            sess1.query("INSERT INTO items VALUES (1, 'path_one')")

            sess2.query("CREATE TABLE items (id Int32, src String) ENGINE = MergeTree() ORDER BY id")
            sess2.query("INSERT INTO items VALUES (2, 'path_two')")

            res1 = str(sess1.query("SELECT id, src FROM items", "CSV")).strip()
            res2 = str(sess2.query("SELECT id, src FROM items", "CSV")).strip()
            self.assertEqual(res1, '1,"path_one"')
            self.assertEqual(res2, '2,"path_two"')
        finally:
            sess2.close()
            sess1.close()

    def test_session_usage_after_close(self):
        sess = session.Session(test_concurrent_dir)
        sess.query("SELECT 1")
        sess.close()
        try:
            sess.query("SELECT 1")
            self.fail("Should raise error when using closed session")
        except Exception as e:
            error_msg = str(e)
            self.assertIsNotNone(error_msg)

if __name__ == "__main__":
    unittest.main()
