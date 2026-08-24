#!python3

import shutil
import unittest

from chdb.state.sqlitelike import connect


def setting_row(conn, name):
    """Return (value, changed) for a setting as seen by this connection."""
    out = str(
        conn.query(
            f"SELECT value, changed FROM system.settings WHERE name = '{name}'",
            "CSV",
        )
    ).strip()
    value, changed = out.rsplit(",", 1)
    return value.strip('"'), int(changed)


class TestConnectionArgvSettings(unittest.TestCase):
    """Settings passed as connection arguments (--<setting>=<value>) must be
    applied to that connection's session, with clickhouse-local semantics:
    visible in system.settings, effective on query behavior, and isolated
    between connections sharing the same path (chdb-io/chdb-core#191)."""

    def test_format_setting_via_connection_arg_reflected_in_system_settings(self):
        conn = connect(":memory:?output_format_json_quote_denormals=1")
        try:
            self.assertEqual(
                setting_row(conn, "output_format_json_quote_denormals"), ("1", 1)
            )
        finally:
            conn.close()

    def test_format_setting_via_connection_arg_affects_output(self):
        conn = connect(":memory:?output_format_json_quote_denormals=1")
        try:
            out = str(conn.query("SELECT nan AS x", "JSONEachRow")).strip()
            self.assertEqual(out, '{"x":"nan"}')
        finally:
            conn.close()

    def test_connection_without_arg_keeps_default_behavior(self):
        conn = connect(":memory:")
        try:
            self.assertEqual(
                setting_row(conn, "output_format_json_quote_denormals"), ("0", 0)
            )
            out = str(conn.query("SELECT nan AS x", "JSONEachRow")).strip()
            self.assertEqual(out, '{"x":null}')
        finally:
            conn.close()

    def test_two_connections_same_path_have_isolated_settings(self):
        conn_a = connect(":memory:?max_threads=1")
        conn_b = connect(":memory:?max_threads=2")
        try:
            self.assertEqual(setting_row(conn_a, "max_threads"), ("1", 1))
            self.assertEqual(setting_row(conn_b, "max_threads"), ("2", 1))
            # Re-read A after B ran queries: B's value must not have leaked.
            self.assertEqual(setting_row(conn_a, "max_threads"), ("1", 1))
        finally:
            conn_b.close()
            conn_a.close()

    def test_second_connection_does_not_inherit_first_connection_settings(self):
        conn_a = connect(":memory:?output_format_json_quote_denormals=1")
        conn_b = connect(":memory:")
        try:
            self.assertEqual(
                setting_row(conn_b, "output_format_json_quote_denormals"), ("0", 0)
            )
            self.assertEqual(
                str(conn_b.query("SELECT nan AS x", "JSONEachRow")).strip(),
                '{"x":null}',
            )
            self.assertEqual(
                str(conn_a.query("SELECT nan AS x", "JSONEachRow")).strip(),
                '{"x":"nan"}',
            )
        finally:
            conn_b.close()
            conn_a.close()

    def test_settings_do_not_survive_into_new_connection_after_close(self):
        conn_a = connect(":memory:?max_threads=1")
        conn_a.close()
        conn_b = connect(":memory:")
        try:
            _, changed = setting_row(conn_b, "max_threads")
            self.assertEqual(changed, 0)
        finally:
            conn_b.close()

    def test_experimental_setting_via_connection_arg(self):
        conn = connect(":memory:?allow_experimental_nullable_tuple_type=1")
        try:
            self.assertEqual(
                setting_row(conn, "allow_experimental_nullable_tuple_type"), ("1", 1)
            )
        finally:
            conn.close()

    def test_multiple_settings_in_one_connection_string(self):
        conn = connect(":memory:?max_threads=2&output_format_json_quote_denormals=1")
        try:
            self.assertEqual(setting_row(conn, "max_threads"), ("2", 1))
            self.assertEqual(
                str(conn.query("SELECT nan AS x", "JSONEachRow")).strip(),
                '{"x":"nan"}',
            )
        finally:
            conn.close()

    def test_negative_setting_value_via_connection_arg(self):
        conn = connect(":memory:?max_partitions_to_read=-1")
        try:
            self.assertEqual(setting_row(conn, "max_partitions_to_read"), ("-1", 1))
        finally:
            conn.close()

    def test_bare_flag_sets_boolean_setting(self):
        conn = connect(":memory:?final")
        try:
            self.assertEqual(setting_row(conn, "final"), ("1", 1))
        finally:
            conn.close()

    def test_per_query_settings_clause_overrides_connection_arg(self):
        conn = connect(":memory:?output_format_json_quote_denormals=1")
        try:
            out = str(
                conn.query(
                    "SELECT nan AS x SETTINGS output_format_json_quote_denormals = 0",
                    "JSONEachRow",
                )
            ).strip()
            self.assertEqual(out, '{"x":null}')
            # The override is per query; the connection-level value is intact.
            self.assertEqual(
                str(conn.query("SELECT nan AS x", "JSONEachRow")).strip(),
                '{"x":"nan"}',
            )
        finally:
            conn.close()

    def test_set_statement_stays_isolated_between_connections(self):
        conn_a = connect(":memory:")
        conn_b = connect(":memory:")
        try:
            conn_a.query("SET max_threads = 1")
            self.assertEqual(setting_row(conn_a, "max_threads"), ("1", 1))
            _, changed = setting_row(conn_b, "max_threads")
            self.assertEqual(changed, 0)
        finally:
            conn_b.close()
            conn_a.close()

    def test_invalid_setting_value_fails_connection(self):
        with self.assertRaisesRegex(Exception, "max_threads"):
            connect(":memory:?max_threads=not_a_number")

    def test_failed_connection_does_not_pin_engine_instance(self):
        """A failed connect must release its engine reference; otherwise the
        process-wide engine stays alive on the old path forever and any later
        connect with a different path errors with 'already initialized'."""
        with self.assertRaises(Exception):
            connect(":memory:?max_threads=not_a_number")
        test_dir = ".test_connection_argv_settings_leak"
        shutil.rmtree(test_dir, ignore_errors=True)
        conn = connect(f"file:{test_dir}")
        try:
            self.assertEqual(str(conn.query("SELECT 1", "CSV")).strip(), "1")
        finally:
            conn.close()
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_unknown_option_is_still_ignored(self):
        conn = connect(":memory:?definitely_not_a_chdb_setting=42")
        try:
            self.assertEqual(str(conn.query("SELECT 1", "CSV")).strip(), "1")
        finally:
            conn.close()

    def test_mode_ro_enforces_readonly(self):
        test_dir = ".test_connection_argv_settings_ro"
        shutil.rmtree(test_dir, ignore_errors=True)
        rw = connect(f"file:{test_dir}")
        rw.query("CREATE TABLE argv_ro_t (x Int32) ENGINE = MergeTree() ORDER BY x")
        rw.query("INSERT INTO argv_ro_t VALUES (7)")
        rw.close()
        conn = connect(f"file:{test_dir}?mode=ro")
        try:
            self.assertEqual(setting_row(conn, "readonly"), ("2", 1))
            self.assertEqual(str(conn.query("SELECT x FROM argv_ro_t", "CSV")).strip(), "7")
            # readonly=2 (not 1): per-query settings stay usable on a ro connection.
            self.assertEqual(
                str(
                    conn.query(
                        "SELECT x FROM argv_ro_t SETTINGS max_threads = 1", "CSV"
                    )
                ).strip(),
                "7",
            )
            with self.assertRaisesRegex(Exception, "[Rr]eadonly"):
                conn.query("INSERT INTO argv_ro_t VALUES (8)")
        finally:
            conn.close()
            shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
