#!python3

"""Regression tests for CREATE NAMED COLLECTION in embedded sessions.

See https://github.com/chdb-io/chdb-core/issues/81

Named collection DDL (CREATE/DROP/ALTER) goes through the process-wide
``NamedCollectionFactory`` singleton. Its ``shutdown()`` used to reset the
metadata storage while leaving the ``loaded`` flag set, so after the global
Context was torn down and recreated (which happens repeatedly inside a single
chDB process) ``loadIfNot()`` became a no-op and the next CREATE NAMED
COLLECTION dereferenced a null metadata storage. That crash was swallowed by
the fault handler, so it looked like the query hung forever.

These tests would hang (and therefore time out and fail) against the buggy
build, and pass once the factory is fully reset on shutdown.
"""

import shutil
import threading
import unittest

from chdb import session


# Guard so a regression manifests as a fast test failure instead of a hang.
QUERY_TIMEOUT_SECONDS = 30


def _run_with_timeout(func):
    """Run ``func`` in a worker thread, failing if it does not finish in time."""
    result = {}

    def target():
        try:
            result["value"] = func()
        except Exception as exc:  # noqa: BLE001 - propagate to the assertion below
            result["error"] = exc

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(QUERY_TIMEOUT_SECONDS)
    return worker.is_alive(), result


class TestNamedCollection(unittest.TestCase):
    test_dir = ".test_named_collection"
    test_dir2 = ".test_named_collection_2"

    def setUp(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)
        shutil.rmtree(self.test_dir2, ignore_errors=True)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)
        shutil.rmtree(self.test_dir2, ignore_errors=True)
        return super().tearDown()

    def test_create_named_collection_returns_and_persists(self):
        """The exact issue #81 repro: CREATE must return promptly and persist."""
        sess = session.Session(self.test_dir)
        try:
            timed_out, result = _run_with_timeout(
                lambda: sess.query("CREATE NAMED COLLECTION nc AS a='1', b='2'")
            )
            self.assertFalse(
                timed_out,
                "CREATE NAMED COLLECTION hung (issue #81 regression)",
            )
            self.assertNotIn("error", result, msg=str(result.get("error")))

            # The collection must actually exist and be queryable.
            names = sess.query(
                "SELECT name FROM system.named_collections ORDER BY name"
            )
            self.assertEqual(str(names), '"nc"\n')

            # Keys are stored (values are hidden in system.named_collections).
            keys = sess.query(
                "SELECT mapKeys(collection) FROM system.named_collections "
                "WHERE name = 'nc'"
            )
            self.assertEqual(str(keys), "\"['a','b']\"\n")
        finally:
            sess.close()

    def test_create_named_collection_after_session_recycle(self):
        """Root cause: a prior session shutdown must not break a later CREATE.

        Opening and closing the first session tears down a global Context (and
        thus calls NamedCollectionFactory::shutdown()). The CREATE in the second
        session is what crashed before the fix.
        """
        first = session.Session(self.test_dir)
        # Touch named collections so the factory is initialized in this Context.
        first.query("SELECT count() FROM system.named_collections")
        first.close()

        second = session.Session(self.test_dir2)
        try:
            timed_out, result = _run_with_timeout(
                lambda: second.query("CREATE NAMED COLLECTION nc2 AS token='x'")
            )
            self.assertFalse(
                timed_out,
                "CREATE NAMED COLLECTION hung after session recycle (issue #81)",
            )
            self.assertNotIn("error", result, msg=str(result.get("error")))

            exists = second.query(
                "SELECT count() FROM system.named_collections WHERE name = 'nc2'"
            )
            self.assertEqual(str(exists), "1\n")
        finally:
            second.close()


class TestNamedCollectionReadonly(unittest.TestCase):
    """Named-collection DDL must respect readonly and allow_ddl.

    See https://github.com/chdb-io/chdb-core/issues/119

    CREATE/ALTER/DROP NAMED COLLECTION used to bypass ``readonly=2`` (and
    ``allow_ddl=0``) because the named-collection access types were missing
    from the readonly/DDL masks in ContextAccess. In embedded chDB those
    settings are the only guard between an untrusted SQL caller and session
    state, so the DDL must be rejected like any other DDL.
    """

    test_dir = ".test_named_collection_readonly"

    def setUp(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)
        return super().tearDown()

    def _collection_names(self, sess):
        return str(sess.query("SELECT name FROM system.named_collections ORDER BY name"))

    def test_named_collection_ddl_rejected_in_readonly_2(self):
        """The exact issue #119 repro: DDL must fail and change nothing."""
        sess = session.Session(self.test_dir)
        try:
            sess.query("CREATE NAMED COLLECTION aws AS url='https://example.com/x.parquet', access_key_id='K'")
            self.assertEqual(self._collection_names(sess), '"aws"\n')

            sess.query("SET readonly=2")

            with self.assertRaisesRegex(Exception, "readonly"):
                sess.query("CREATE NAMED COLLECTION x AS a=1")
            with self.assertRaisesRegex(Exception, "readonly"):
                sess.query("ALTER NAMED COLLECTION aws SET url='https://evil.example.com/y.parquet'")
            with self.assertRaisesRegex(Exception, "readonly"):
                sess.query("DROP NAMED COLLECTION aws")

            # The operator's collection is intact and no new one appeared.
            self.assertEqual(self._collection_names(sess), '"aws"\n')
            keys = sess.query(
                "SELECT mapKeys(collection) FROM system.named_collections WHERE name = 'aws'"
            )
            self.assertEqual(str(keys), "\"['access_key_id','url']\"\n")
        finally:
            sess.close()

    def test_named_collection_ddl_rejected_in_readonly_1(self):
        sess = session.Session(self.test_dir)
        try:
            sess.query("SET readonly=1")
            with self.assertRaisesRegex(Exception, "readonly"):
                sess.query("CREATE NAMED COLLECTION x AS a=1")
            self.assertEqual(self._collection_names(sess), "")
        finally:
            sess.close()

    def test_named_collection_ddl_rejected_with_allow_ddl_0(self):
        sess = session.Session(self.test_dir)
        try:
            sess.query("SET allow_ddl=0")
            with self.assertRaisesRegex(Exception, "DDL queries are prohibited"):
                sess.query("CREATE NAMED COLLECTION x AS a=1")
            self.assertEqual(self._collection_names(sess), "")
        finally:
            sess.close()

    def test_named_collection_ddl_still_works_without_readonly(self):
        """The gate must not break the normal (non-readonly) DDL path."""
        sess = session.Session(self.test_dir)
        try:
            sess.query("CREATE NAMED COLLECTION nc AS a='1'")
            sess.query("ALTER NAMED COLLECTION nc SET a='2'")
            self.assertEqual(self._collection_names(sess), '"nc"\n')
            sess.query("DROP NAMED COLLECTION nc")
            self.assertEqual(self._collection_names(sess), "")
        finally:
            sess.close()


if __name__ == "__main__":
    unittest.main()
