#!python3

import os
import tempfile
import unittest

from chdb.state.sqlitelike import connect

QUERY_LOG_CONFIG = """<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </query_log>
    <processors_profile_log>
        <database>system</database>
        <table>processors_profile_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </processors_profile_log>
</clickhouse>
"""

CUSTOM_TABLE_CONFIG = """<clickhouse>
    <query_log>
        <database>system</database>
        <table>my_query_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </query_log>
</clickhouse>
"""

# text_log only receives anything if a real log channel raises the logger level,
# so give it a file to write to.
TEXT_LOG_CONFIG = """<clickhouse>
    <logger>
        <log>{log_path}</log>
        <async>false</async>
    </logger>
    <text_log>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </text_log>
</clickhouse>
"""


def scalar(conn, sql):
    return str(conn.query(sql, "CSV")).strip()


class TestSystemLogs(unittest.TestCase):
    """System logs configured through a config file must be created and populated
    on the embedded engine, and the storage path must stay usable for the next
    engine (chdb-io/chdb-core#222)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "db")
        self.config = self.write_config("logs.xml", QUERY_LOG_CONFIG)

    def tearDown(self):
        self.tmp.cleanup()

    def write_config(self, name, body):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w") as f:
            f.write(body)
        return path

    def write_sentinel(self, *parts):
        directory = os.path.join(self.db_path, *parts)
        os.makedirs(directory)
        path = os.path.join(directory, "sentinel.txt")
        with open(path, "w") as f:
            f.write("keep me")
        return path

    def run_marker_query(self, conn, marker):
        conn.query(f"SELECT '{marker}' AS m", "CSV")
        conn.query("SYSTEM FLUSH LOGS", "CSV")

    def finished_query(self, conn, marker, table="query_log"):
        return scalar(
            conn,
            f"SELECT query FROM system.{table} "
            f"WHERE type = 'QueryFinish' AND query LIKE '%{marker}%'",
        )

    def test_query_log_records_the_query_that_was_run(self):
        conn = connect(f"{self.db_path}?config-file={self.config}")
        try:
            self.run_marker_query(conn, "MARKER_ONE")
            self.assertEqual(
                self.finished_query(conn, "MARKER_ONE"),
                "\"SELECT 'MARKER_ONE' AS m\"",
            )
        finally:
            conn.close()

    def test_processors_profile_log_is_populated(self):
        conn = connect(f"{self.db_path}?config-file={self.config}")
        try:
            conn.query("SELECT count() FROM numbers(1000)", "CSV")
            conn.query("SYSTEM FLUSH LOGS", "CSV")
            self.assertGreater(
                int(scalar(conn, "SELECT count() FROM system.processors_profile_log")),
                0,
            )
        finally:
            conn.close()

    def test_query_log_works_again_for_a_second_engine_on_the_same_path(self):
        """A MergeTree system log table leaves a data directory behind that its
        never-persisted metadata cannot reclaim, so without cleanup the next
        engine on the same path fails to create the table at all."""
        first = connect(f"{self.db_path}?config-file={self.config}")
        try:
            self.run_marker_query(first, "MARKER_FIRST")
            self.assertEqual(
                self.finished_query(first, "MARKER_FIRST"),
                "\"SELECT 'MARKER_FIRST' AS m\"",
            )
        finally:
            first.close()

        second = connect(f"{self.db_path}?config-file={self.config}")
        try:
            self.run_marker_query(second, "MARKER_SECOND")
            self.assertEqual(
                self.finished_query(second, "MARKER_SECOND"),
                "\"SELECT 'MARKER_SECOND' AS m\"",
            )
            # The previous engine's rows are gone with its metadata: system log
            # history does not survive an engine, even on a persistent path.
            self.assertEqual(
                scalar(
                    second,
                    "SELECT count() FROM system.query_log WHERE query LIKE '%MARKER_FIRST%'",
                ),
                "0",
            )
        finally:
            second.close()

    def test_cleanup_only_touches_the_configured_log_table(self):
        """The config names my_query_log, so only that directory may be removed;
        a directory named after the default table must be left alone."""
        config = self.write_config("custom.xml", CUSTOM_TABLE_CONFIG)
        configured = self.write_sentinel("data", "system", "my_query_log")
        unconfigured = self.write_sentinel("data", "system", "query_log")

        conn = connect(f"{self.db_path}?config-file={config}")
        try:
            self.run_marker_query(conn, "MARKER_CUSTOM")
            self.assertEqual(
                self.finished_query(conn, "MARKER_CUSTOM", table="my_query_log"),
                "\"SELECT 'MARKER_CUSTOM' AS m\"",
            )
        finally:
            conn.close()

        self.assertFalse(os.path.exists(configured))
        self.assertTrue(os.path.exists(unconfigured))
        with open(unconfigured) as f:
            self.assertEqual(f.read(), "keep me")

    def test_text_log_works_in_a_second_engine_in_the_same_process(self):
        """text_log's queue is a process-wide static; its shutdown latch and its
        flush bookkeeping used to survive into the next engine, which made
        SYSTEM FLUSH LOGS fail with ABORTED and left the table uncreated."""
        log_path = os.path.join(self.tmp.name, "clickhouse.log")
        config = self.write_config(
            "text.xml", TEXT_LOG_CONFIG.format(log_path=log_path)
        )

        first = connect(f"{self.db_path}?config-file={config}")
        try:
            first.query("SELECT 'MARKER_TEXT_FIRST' AS m", "CSV")
            first.query("SYSTEM FLUSH LOGS", "CSV")
        finally:
            first.close()

        second = connect(f"{self.db_path}?config-file={config}")
        try:
            second.query("SELECT 'MARKER_TEXT_SECOND' AS m", "CSV")
            # Used to raise "Shutdown has been called" (ABORTED).
            second.query("SYSTEM FLUSH LOGS", "CSV")
            self.assertEqual(scalar(second, "EXISTS TABLE system.text_log"), "1")
            # And the new engine's messages really reach the new consumer.
            self.assertGreater(
                int(scalar(second, "SELECT count() FROM system.text_log")), 0
            )
        finally:
            second.close()

    def test_no_system_logs_without_a_config_file(self):
        conn = connect(self.db_path)
        try:
            self.run_marker_query(conn, "MARKER_NONE")
            self.assertEqual(scalar(conn, "EXISTS TABLE system.query_log"), "0")
        finally:
            conn.close()

    def test_config_discovered_from_the_working_directory_does_not_arm_logs(self):
        """An embedded engine must not spawn flush threads because of a config
        file that happens to sit in the host process' working directory."""
        self.write_config("config.xml", QUERY_LOG_CONFIG)
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            conn = connect(self.db_path)
            try:
                # SYSTEM FLUSH LOGS forces table creation when logs are armed, so
                # this assertion can actually fail if the discovery armed them.
                self.run_marker_query(conn, "MARKER_CWD")
                self.assertEqual(scalar(conn, "EXISTS TABLE system.query_log"), "0")
            finally:
                conn.close()
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
