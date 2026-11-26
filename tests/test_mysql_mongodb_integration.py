#!python3
"""Always-on guard tests for the MySQL and MongoDB integrations (issue #82).

These need NO live server and run everywhere CI runs the suite. They prove the
integrations are genuinely *compiled in and wired to their client libraries* --
not merely that a name appears in a system table:

  1. mysql()/mongodb() table functions and the MySQL table/database + MongoDB
     table engines are registered;
  2. invoking each against an unreachable host produces a **driver-level**
     error (mysqlxx::ConnectionFailed / Code 279 for MySQL,
     mongocxx "No suitable servers" / Code 1001 for MongoDB), i.e. the real
     client library is linked and attempts network I/O -- not a
     "not compiled in" error (Code 46/56/336);
  3. the read path (table function + storage engine), the database engine, and
     the write path (INSERT INTO FUNCTION) are all wired;
  4. argument validation fires (Code 42), proving the factory entry exists.

Real data roundtrips against a live MySQL/MongoDB live in
test_mysql_mongodb_federation.py (env-gated, mirroring test_remote_query.py).

Skipped under CHDB_LITE=1 (these libs are intentionally absent in lite). On
Windows, whose wheel is built without network-backed storage, each integration's
tests skip when the build registers none of its table functions and engines; a
Windows build that registers only some of them fails, and so does any other
platform that lacks one.
"""

import os
import sys
import tempfile
import unittest

from chdb import session as chs


# Errors meaning "feature not compiled into this build" -> must NOT appear.
_NOT_COMPILED = (
    "Code: 46",   # UNKNOWN_FUNCTION
    "Code: 56",   # UNKNOWN_STORAGE
    "Code: 336",  # UNKNOWN_DATABASE_ENGINE
    "Unknown table function",
    "Unknown table engine",
    "Unknown database engine",
)
# Signatures proving the actual client driver is linked and attempting I/O.
_MYSQL_DRIVER = ("mysqlxx", "Connections to mysql failed", "Code: 279")
_MONGO_DRIVER = ("mongocxx", "No suitable servers", "Code: 1001")

_UNREACHABLE = "127.0.0.1:1"  # refuses immediately -> fast, deterministic

# What each integration registers. One build option (USE_MYSQL / USE_MONGODB)
# compiles in all of an integration's entries, and the two are independent.
_REGISTERED = {
    "MySQL": (
        ("table_functions", "mysql"),
        ("table_engines", "MySQL"),
        ("database_engines", "MySQL"),
    ),
    "MongoDB": (
        ("table_functions", "mongodb"),
        ("table_engines", "MongoDB"),
    ),
}


class TestMySQLMongoDBCompiledIn(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("CHDB_LITE") == "1":
            raise unittest.SkipTest("MySQL/MongoDB are intentionally absent in chdb-core-lite")

    def setUp(self):
        self._path = tempfile.mkdtemp(prefix="chdb_fed_")
        self.s = chs.Session(self._path)

    def tearDown(self):
        try:
            self.s.close()
        finally:
            import shutil
            shutil.rmtree(self._path, ignore_errors=True)

    def _count(self, sql):
        return int(str(self.s.query(sql, "CSV")).strip())

    def _require(self, integration):
        """On Windows, skip when the build has no `integration` at all: none of its
        entries is in system.table_functions / table_engines / database_engines.
        The Windows wheel is built without either integration; everywhere else
        they are part of the build, and a missing one must fail these tests."""
        if sys.platform != "win32":
            return
        entries = _REGISTERED[integration]
        if not any(self._count(f"SELECT count() FROM system.{table} WHERE name = '{name}'")
                   for table, name in entries):
            self.skipTest(f"the Windows wheel is built without {integration}: none of "
                          + ", ".join(f"system.{table} '{name}'" for table, name in entries)
                          + " is registered")

    def _error_of(self, sql):
        with self.assertRaises(Exception) as ctx:
            self.s.query(sql)
        return str(ctx.exception)

    def _assert_wired(self, sql, driver_markers):
        """Raises a driver-level error (proves linked & doing I/O), and never a
        'not compiled in' error."""
        msg = self._error_of(sql)
        for nc in _NOT_COMPILED:
            self.assertNotIn(nc, msg, f"'{nc}' for {sql!r}: {msg[:200]}")
        self.assertTrue(
            any(m in msg for m in driver_markers),
            f"expected one of {driver_markers} (driver linked) for {sql!r}, got: {msg[:200]}",
        )

    # ---- registration --------------------------------------------------

    def test_table_functions_registered(self):
        with self.subTest("MySQL"):
            self._require("MySQL")
            self.assertEqual(1, self._count(
                "SELECT count() FROM system.table_functions WHERE name = 'mysql'"))
        with self.subTest("MongoDB"):
            self._require("MongoDB")
            self.assertEqual(1, self._count(
                "SELECT count() FROM system.table_functions WHERE name = 'mongodb'"))

    def test_storage_and_database_engines_registered(self):
        with self.subTest("MySQL"):
            self._require("MySQL")
            self.assertEqual(1, self._count(
                "SELECT count() FROM system.table_engines WHERE name = 'MySQL'"))
            self.assertEqual(1, self._count(
                "SELECT count() FROM system.database_engines WHERE name = 'MySQL'"))
        with self.subTest("MongoDB"):
            self._require("MongoDB")
            self.assertEqual(1, self._count(
                "SELECT count() FROM system.table_engines WHERE name = 'MongoDB'"))

    # ---- read path is wired to the real driver ------------------------

    def test_mysql_table_function_uses_real_driver(self):
        self._require("MySQL")
        self._assert_wired(
            f"SELECT * FROM mysql('{_UNREACHABLE}', 'db', 'tbl', 'user', 'pass')",
            _MYSQL_DRIVER,
        )

    def test_mongodb_table_function_uses_real_driver(self):
        self._require("MongoDB")
        self._assert_wired(
            f"SELECT * FROM mongodb('{_UNREACHABLE}', 'db', 'coll', 'user', 'pass', 'id Int32')",
            _MONGO_DRIVER,
        )

    def test_mysql_storage_engine_uses_real_driver(self):
        self._require("MySQL")
        # CREATE with explicit schema is lazy; the SELECT triggers the connection.
        self.s.query(
            f"CREATE TABLE t_mysql (id Int32) ENGINE = MySQL('{_UNREACHABLE}', 'db', 'tbl', 'user', 'pass')"
        )
        self._assert_wired("SELECT * FROM t_mysql", _MYSQL_DRIVER)

    def test_mongodb_storage_engine_uses_real_driver(self):
        self._require("MongoDB")
        self.s.query(
            f"CREATE TABLE t_mongo (id Int32) ENGINE = MongoDB('{_UNREACHABLE}', 'db', 'coll', 'user', 'pass')"
        )
        self._assert_wired("SELECT * FROM t_mongo", _MONGO_DRIVER)

    def test_mysql_database_engine_uses_real_driver(self):
        self._require("MySQL")
        self._assert_wired(
            f"CREATE DATABASE db_mysql ENGINE = MySQL('{_UNREACHABLE}', 'db', 'user', 'pass')",
            _MYSQL_DRIVER,
        )

    # ---- write path is wired ------------------------------------------

    def test_mysql_insert_into_function_uses_real_driver(self):
        self._require("MySQL")
        self._assert_wired(
            f"INSERT INTO FUNCTION mysql('{_UNREACHABLE}', 'db', 'tbl', 'user', 'pass') SELECT 1",
            _MYSQL_DRIVER,
        )

    # ---- argument validation (factory entry exists) -------------------

    def test_mysql_table_function_validates_argument_count(self):
        self._require("MySQL")
        msg = self._error_of("SELECT * FROM mysql('only-one-arg')")
        self.assertIn("Code: 42", msg)                 # NUMBER_OF_ARGUMENTS_DOESNT_MATCH
        for nc in _NOT_COMPILED:
            self.assertNotIn(nc, msg)

    def test_mongodb_table_function_validates_argument_count(self):
        self._require("MongoDB")
        msg = self._error_of("SELECT * FROM mongodb('only-one-arg')")
        self.assertIn("Code: 42", msg)
        for nc in _NOT_COMPILED:
            self.assertNotIn(nc, msg)


if __name__ == "__main__":
    unittest.main()
