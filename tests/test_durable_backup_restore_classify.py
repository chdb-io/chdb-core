#!python3

import os
import shutil
import tempfile
import unittest

from chdb import _chdb
from chdb.state.sqlitelike import connect


READ_ONLY = _chdb.query_class.READ_ONLY
MUTATING = _chdb.query_class.MUTATING
MUTATING_GLOBAL = _chdb.query_class.MUTATING_GLOBAL
CONTROL = _chdb.query_class.CONTROL
UNKNOWN = _chdb.query_class.UNKNOWN

# A database name that a naive f-string would get wrong twice over: the dash
# forces backticks, and the backtick inside has to be doubled.
AWKWARD_DB = "durable-obj`1"
AWKWARD_DB_SQL = "`durable-obj``1`"


class DurablePrimitivesTestCase(unittest.TestCase):
    """One connection with a scratch data directory and a backups directory."""

    def setUp(self):
        self.work = tempfile.mkdtemp(prefix="chdb-durable-")
        # Absolute and already created: a relative backups.allowed_path would
        # resolve against the data directory, and the engine does not create it.
        self.backups = os.path.join(self.work, "backups")
        os.makedirs(self.backups)
        self.conn = connect(
            f"{os.path.join(self.work, 'db')}?backups.allowed_path={self.backups}"
        )

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.work, ignore_errors=True)

    def scalar(self, sql):
        return str(self.conn.query(sql, "CSV")).strip()

    def classify(self, sql):
        query_class, _has_secrets = self.conn._conn.classify_query(sql)
        return query_class

    def has_secrets(self, sql):
        _query_class, has_secrets = self.conn._conn.classify_query(sql)
        return has_secrets

    def backup(self, database, file_path, base_file_path=""):
        self.conn._conn.backup_database(database, file_path, base_file_path)

    def restore(self, database, file_path):
        self.conn._conn.restore_database(database, file_path)

    def seed_awkward_database(self):
        self.conn.query(f"CREATE DATABASE {AWKWARD_DB_SQL}")
        self.conn.query(
            f"CREATE TABLE {AWKWARD_DB_SQL}.t (id UInt32, name String) "
            "ENGINE = MergeTree ORDER BY id"
        )
        self.conn.query(f"INSERT INTO {AWKWARD_DB_SQL}.t VALUES (1,'a'),(2,'b'),(3,'c')")


class TestClassifyQuery(DurablePrimitivesTestCase):
    """classify_query reports a statement's effect using the engine's own
    parser, so a control plane never has to keep a list of SQL prefixes."""

    def test_reads_are_read_only(self):
        for sql in (
            "SELECT 1",
            "SELECT count() FROM system.numbers LIMIT 1",
            "SHOW DATABASES",
            "DESCRIBE TABLE system.one",
            "EXPLAIN SELECT 1",
            "EXISTS TABLE system.one",
        ):
            with self.subTest(sql=sql):
                self.assertEqual(self.classify(sql), READ_ONLY)

    def test_writes_the_backup_captures_are_mutating(self):
        for sql in (
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t (id UInt32) ENGINE = Memory",
            "CREATE DATABASE d",
            "ALTER TABLE t UPDATE id = 2 WHERE id = 1",
            "ALTER TABLE t ADD COLUMN c String",
            "DROP TABLE t",
            "TRUNCATE TABLE t",
            "RENAME TABLE t TO u",
            "OPTIMIZE TABLE t FINAL",
            "DELETE FROM t WHERE id = 1",
            "UNDROP TABLE t",
        ):
            with self.subTest(sql=sql):
                self.assertEqual(self.classify(sql), MUTATING)

    def test_session_and_server_statements_are_control(self):
        for sql in (
            "USE other",
            "SET max_threads = 4",
            "ATTACH TABLE t",
            "DETACH TABLE t",
            "SYSTEM DROP MARK CACHE",
            "BACKUP DATABASE d TO File('x')",
            "RESTORE DATABASE d FROM File('x')",
            "KILL QUERY WHERE query_id = 'x'",
            "BEGIN TRANSACTION",
        ):
            with self.subTest(sql=sql):
                self.assertEqual(self.classify(sql), CONTROL)

    def test_state_a_checkpoint_would_not_carry_is_global(self):
        """These change something real and are worth replaying, but they live
        beside the databases, so `BACKUP DATABASE` does not capture them. A
        caller that filed them with the MUTATING statements would lose them at
        the next checkpoint, without an error."""
        for sql in (
            "CREATE FUNCTION f AS (x) -> x + 1",
            "DROP FUNCTION f",
            "CREATE USER u IDENTIFIED WITH no_password",
            "GRANT SELECT ON *.* TO u",
            "REVOKE SELECT ON *.* FROM u",
            "CREATE ROLE r",
            "CREATE QUOTA q",
            "CREATE ROW POLICY p ON t",
            "CREATE SETTINGS PROFILE sp",
            "DROP USER u",
            "CREATE NAMED COLLECTION nc AS a = 1",
            "DROP NAMED COLLECTION nc",
            "ALTER NAMED COLLECTION nc SET a = 2",
            "CREATE WORKLOAD w",
            "CREATE RESOURCE r (WRITE DISK d)",
            # A WebAssembly module arrives this way; the bytes land in the user
            # scripts directory, which no database backup reaches.
            "INSERT INTO system.wasm_modules VALUES ('m', 'bytes')",
            # Reaching `system` is global whatever the verb is.
            "ALTER TABLE system.t MODIFY COLUMN c String",
            "DROP TABLE system.t",
            "TRUNCATE TABLE system.t",
        ):
            with self.subTest(sql=sql):
                self.assertEqual(self.classify(sql), MUTATING_GLOBAL)

    def test_the_two_mutating_classes_are_told_apart(self):
        """The whole point of the split: same keyword, different fate at the
        next checkpoint."""
        self.assertEqual(self.classify("CREATE TABLE t (a Int32) ENGINE = Memory"), MUTATING)
        self.assertEqual(self.classify("CREATE FUNCTION f AS (x) -> x + 1"), MUTATING_GLOBAL)
        self.assertEqual(self.classify("DROP TABLE t"), MUTATING)
        self.assertEqual(self.classify("DROP FUNCTION f"), MUTATING_GLOBAL)
        self.assertEqual(self.classify("INSERT INTO t VALUES (1)"), MUTATING)
        self.assertEqual(
            self.classify("INSERT INTO system.wasm_modules VALUES ('m','b')"), MUTATING_GLOBAL
        )

    def test_writing_outside_the_database_is_control(self):
        self.assertEqual(self.classify("SELECT 1 INTO OUTFILE 'o.csv'"), CONTROL)
        self.assertEqual(
            self.classify("INSERT INTO FUNCTION file('o.csv') SELECT 1"), CONTROL
        )

    def test_unparseable_sql_is_unknown(self):
        self.assertEqual(self.classify("SELECT FROM WHERE (("), UNKNOWN)
        self.assertEqual(self.classify(""), UNKNOWN)
        self.assertEqual(self.classify("   "), UNKNOWN)
        self.assertEqual(self.classify("-- only a comment"), UNKNOWN)

    def test_batch_takes_the_most_restricted_statement(self):
        self.assertEqual(self.classify("SELECT 1; SELECT 2"), READ_ONLY)
        self.assertEqual(self.classify("SELECT 1; INSERT INTO t VALUES (1)"), MUTATING)
        self.assertEqual(self.classify("INSERT INTO t VALUES (1); USE other"), CONTROL)
        self.assertEqual(self.classify("SELECT 1; SELECT FROM WHERE (("), UNKNOWN)

    def test_trailing_separators_are_not_statements(self):
        self.assertEqual(self.classify("SELECT 1;"), READ_ONLY)
        self.assertEqual(self.classify("SELECT 1; -- done"), READ_ONLY)
        self.assertEqual(self.classify("  SELECT 1  ;  "), READ_ONLY)

    def test_a_statement_cannot_hide_behind_inlined_rows(self):
        """The rows of an INSERT are data, not SQL, but whatever follows them
        is SQL again and has to be classified too."""
        self.assertEqual(self.classify("INSERT INTO t VALUES (1); USE other"), CONTROL)
        self.assertEqual(
            self.classify("INSERT INTO t FORMAT CSV\n1\n\nUSE other"), CONTROL
        )
        self.assertEqual(
            self.classify("EXPLAIN INSERT INTO t VALUES (1); USE other"), CONTROL
        )

    def test_the_classes_are_ordered_so_a_caller_can_fold_a_batch(self):
        """A control plane that classifies statements one at a time needs to
        combine the answers, and the ordering is what lets it use max()."""
        self.assertLess(READ_ONLY, MUTATING)
        self.assertLess(MUTATING, MUTATING_GLOBAL)
        self.assertLess(MUTATING_GLOBAL, CONTROL)
        self.assertLess(CONTROL, UNKNOWN)
        self.assertEqual(max([READ_ONLY, CONTROL, MUTATING]), CONTROL)
        self.assertEqual(max([READ_ONLY, MUTATING_GLOBAL, MUTATING]), MUTATING_GLOBAL)

    def test_secrets_in_the_text_are_reported(self):
        """The engine redacts a credential when it prints a statement back, but
        a caller's log of what it submitted has no such protection -- and a
        durable log outlives the statement by design."""
        for sql in (
            "CREATE NAMED COLLECTION nc AS access_key_id = 'AKIA', secret_access_key = 's3cr3t'",
            "CREATE USER u IDENTIFIED WITH sha256_password BY 'hunter2'",
            "SELECT * FROM s3('https://b/k', 'AKIA', 's3cr3t')",
        ):
            with self.subTest(sql=sql):
                self.assertTrue(self.has_secrets(sql))

        for sql in (
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "CREATE FUNCTION f AS (x) -> x + 1",
            "CREATE USER u IDENTIFIED WITH no_password",
        ):
            with self.subTest(sql=sql):
                self.assertFalse(self.has_secrets(sql))

    def test_one_secret_taints_the_whole_batch(self):
        self.assertTrue(
            self.has_secrets("SELECT 1; CREATE USER u IDENTIFIED WITH sha256_password BY 'p'")
        )

    def test_unparseable_text_claims_no_secret(self):
        """Nothing was proven about text that did not parse, so it must not
        report a clean bill of health it cannot back up -- the class is UNKNOWN,
        which is the refusal; has_secrets stays false rather than guessing."""
        self.assertEqual(self.classify("SELECT FROM WHERE (("), UNKNOWN)
        self.assertFalse(self.has_secrets("SELECT FROM WHERE (("))

    def test_classifying_does_not_execute(self):
        self.conn.query("CREATE TABLE probe (id UInt32) ENGINE = Memory")
        self.assertEqual(self.classify("INSERT INTO probe VALUES (1)"), MUTATING)
        self.assertEqual(self.classify("DROP TABLE probe"), MUTATING)
        self.assertEqual(self.scalar("SELECT count() FROM probe"), "0")

    def test_classifying_does_not_change_the_session(self):
        before = self.scalar("SELECT currentDatabase()")
        self.assertEqual(self.classify("USE system"), CONTROL)
        self.assertEqual(self.scalar("SELECT currentDatabase()"), before)


class TestBackupAndRestore(DurablePrimitivesTestCase):
    """backup_database / restore_database take the database and the path apart
    so the engine can quote each, which is what makes them safe to call with
    names a control plane did not choose."""

    def test_round_trip_of_an_awkward_name_and_path(self):
        self.seed_awkward_database()
        archive = os.path.join(self.backups, "it's a backup.tar.gz")

        self.backup(AWKWARD_DB, archive)
        self.assertTrue(os.path.exists(archive))

        self.conn.query(f"DROP DATABASE {AWKWARD_DB_SQL}")
        self.restore(AWKWARD_DB, archive)

        self.assertEqual(self.scalar(f"SELECT count() FROM {AWKWARD_DB_SQL}.t"), "3")
        self.assertEqual(
            self.scalar(f"SELECT name FROM {AWKWARD_DB_SQL}.t ORDER BY id"),
            '"a"\n"b"\n"c"',
        )

    def test_restore_leaves_the_current_database_alone(self):
        self.seed_awkward_database()
        archive = os.path.join(self.backups, "current-db.tar.gz")
        self.backup(AWKWARD_DB, archive)
        self.conn.query(f"DROP DATABASE {AWKWARD_DB_SQL}")

        before = self.scalar("SELECT currentDatabase()")
        self.restore(AWKWARD_DB, archive)
        self.assertEqual(self.scalar("SELECT currentDatabase()"), before)

    def test_an_existing_destination_is_refused_not_overwritten(self):
        self.seed_awkward_database()
        archive = os.path.join(self.backups, "once.tar.gz")
        self.backup(AWKWARD_DB, archive)
        first_size = os.path.getsize(archive)

        with self.assertRaises(RuntimeError):
            self.backup(AWKWARD_DB, archive)
        self.assertEqual(os.path.getsize(archive), first_size)

    def test_a_path_outside_allowed_path_is_refused(self):
        self.seed_awkward_database()
        outside = os.path.join(self.work, "escape.tar.gz")
        with self.assertRaises(RuntimeError):
            self.backup(AWKWARD_DB, outside)
        self.assertFalse(os.path.exists(outside))

    def test_a_quote_in_the_name_cannot_end_the_statement(self):
        """The classic injection shape: if the name were pasted into the SQL,
        the trailing DROP would run. It has to be read as part of the name,
        which does not exist, so the backup fails and nothing is dropped."""
        self.seed_awkward_database()
        with self.assertRaises(RuntimeError):
            self.backup(
                f"x` TO File('{self.backups}/inj.tar.gz'); DROP DATABASE {AWKWARD_DB_SQL} --",
                os.path.join(self.backups, "injection.tar.gz"),
            )
        self.assertEqual(self.scalar(f"SELECT count() FROM {AWKWARD_DB_SQL}.t"), "3")

    def test_empty_arguments_are_refused(self):
        self.seed_awkward_database()
        with self.assertRaises(RuntimeError):
            self.backup("", os.path.join(self.backups, "empty-db.tar.gz"))
        with self.assertRaises(RuntimeError):
            self.backup(AWKWARD_DB, "")

    def test_a_non_ascii_name_and_path_round_trip(self):
        self.conn.query("CREATE DATABASE `数据库-α`")
        self.conn.query(
            "CREATE TABLE `数据库-α`.`данные` (id UInt32) ENGINE = MergeTree ORDER BY id"
        )
        self.conn.query("INSERT INTO `数据库-α`.`данные` VALUES (10),(20)")

        archive = os.path.join(self.backups, "备份-β.tar.gz")
        self.backup("数据库-α", archive)
        self.conn.query("DROP DATABASE `数据库-α`")
        self.restore("数据库-α", archive)

        self.assertEqual(self.scalar("SELECT sum(id) FROM `数据库-α`.`данные`"), "30")

    def test_restore_brings_back_the_schema(self):
        self.seed_awkward_database()
        archive = os.path.join(self.backups, "schema.tar.gz")
        self.backup(AWKWARD_DB, archive)
        self.conn.query(f"DROP DATABASE {AWKWARD_DB_SQL}")
        self.restore(AWKWARD_DB, archive)

        self.assertEqual(
            self.scalar(
                "SELECT name, type FROM system.columns "
                f"WHERE database = '{AWKWARD_DB}' AND table = 't' ORDER BY position"
            ),
            '"id","UInt32"\n"name","String"',
        )

    def test_a_relative_path_is_refused_rather_than_resolved(self):
        """backups.allowed_path resolves a relative value against the data
        directory and a relative archive path then resolves against that, so a
        relative path lands somewhere the caller did not mean."""
        self.seed_awkward_database()
        with self.assertRaises(RuntimeError):
            self.backup(AWKWARD_DB, "backups/relative.tar.gz")

    def test_a_missing_directory_is_refused(self):
        self.seed_awkward_database()
        with self.assertRaises(RuntimeError):
            self.backup(AWKWARD_DB, os.path.join(self.backups, "no-such-dir", "x.tar.gz"))

    def test_an_incremental_backup_is_smaller_than_its_base(self):
        self.conn.query("CREATE DATABASE big")
        self.conn.query("CREATE TABLE big.t (id UInt32) ENGINE = MergeTree ORDER BY id")
        self.conn.query("INSERT INTO big.t SELECT number FROM numbers(100000)")

        base = os.path.join(self.backups, "base.tar.gz")
        self.backup("big", base)

        self.conn.query("INSERT INTO big.t SELECT number FROM numbers(10)")
        incr = os.path.join(self.backups, "incr.tar.gz")
        self.backup("big", incr, base_file_path=base)

        self.assertLess(os.path.getsize(incr), os.path.getsize(base) // 10)

    def test_an_incremental_backup_needs_its_base_to_exist(self):
        self.seed_awkward_database()
        with self.assertRaises(RuntimeError):
            self.backup(
                AWKWARD_DB,
                os.path.join(self.backups, "orphan.tar.gz"),
                base_file_path=os.path.join(self.backups, "not-a-base.tar.gz"),
            )

    def test_restoring_a_missing_archive_reports_an_error(self):
        with self.assertRaises(RuntimeError):
            self.restore(AWKWARD_DB, os.path.join(self.backups, "not-there.tar.gz"))


if __name__ == "__main__":
    unittest.main()
