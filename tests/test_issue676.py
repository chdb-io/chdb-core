#!python3

import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

# Each case runs in its own process: the engine is one instance per process, and
# a connection another test leaves open would decide which options it started with.
_PRELUDE = """
import warnings
import chdb

def server_setting(conn, name):
    return str(conn.query(
        f"SELECT value, changed FROM system.server_settings WHERE name = '{name}'", "CSV")).strip()

def connect_warnings(conn_str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        conn = chdb.connect(conn_str)
    return conn, [str(w.message) for w in caught if issubclass(w.category, RuntimeWarning)]

# A directory of one executable UDF, `function(UInt8)`, that returns `output` for every row.
def udf_dir(function, output):
    import os, tempfile
    directory = tempfile.mkdtemp()
    script = os.path.join(directory, function + ".py")
    with open(script, "w") as f:
        f.write(f"#!/usr/bin/env python3\\nimport sys\\nfor line in sys.stdin:\\n    print({output!r}, flush=True)\\n")
    os.chmod(script, 0o755)
    with open(os.path.join(directory, function + "_function.xml"), "w") as f:
        f.write(
            f"<functions><function><type>executable_pool</type><name>{function}</name>"
            "<return_type>String</return_type><argument><type>UInt8</type></argument>"
            f"<format>TabSeparated</format><command>{function}.py</command></function></functions>")
    return directory
"""


def _run(body):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    # A file rather than -c: chdb_udf reads the function's source.
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as script:
        script.write(_PRELUDE + textwrap.dedent(body))
    try:
        return subprocess.run(
            [sys.executable, script.name], env=env, capture_output=True, text=True, timeout=300,
        )
    finally:
        os.unlink(script.name)


class TestIssue676(unittest.TestCase):
    """A connection that joins a running engine never loses its startup options silently."""

    def _check(self, body):
        proc = _run(body)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout, proc.stdout + proc.stderr)

    def test_differing_server_setting_warns(self):
        self._check("""
            held = chdb.connect(":memory:")
            second, caught = connect_warnings(":memory:?max_server_memory_usage=9876543210")
            assert len(caught) == 1, caught
            assert "max_server_memory_usage=9876543210 (running: 0)" in caught[0], caught
            assert server_setting(second, "max_server_memory_usage").endswith(",0"), server_setting(second, "max_server_memory_usage")
            second.close()
            held.close()
            print("OK")
        """)

    def test_differing_result_path_is_refused(self):
        self._check("""
            import tempfile
            held = chdb.connect(":memory:")
            try:
                chdb.query("SELECT 1", "CSV", options={"format_schema_path": tempfile.gettempdir()})
            except Exception as e:
                assert "format_schema_path=" in str(e) and "Close the other connections first" in str(e), str(e)
            else:
                raise AssertionError("connected")
            held.close()
            print("OK")
        """)

    def test_differing_google_protos_path_is_refused(self):
        self._check("""
            import tempfile
            held = chdb.connect(":memory:")
            try:
                chdb.connect(f":memory:?google_protos_path={tempfile.mkdtemp()}")
            except Exception as e:
                assert "google_protos_path=" in str(e), str(e)
            else:
                raise AssertionError("connected")
            held.close()
            print("OK")
        """)

    def test_failed_connection_leaves_no_warning_behind(self):
        self._check("""
            held = chdb.connect(":memory:")
            try:
                chdb.connect(":memory:?max_server_memory_usage=9876543210&max_threads=notanumber")
            except Exception:
                pass
            else:
                raise AssertionError("connected")
            conn, caught = connect_warnings(":memory:")
            assert caught == [], caught
            conn.close()
            held.close()
            print("OK")
        """)

    def test_other_udf_path_replaces_the_running_one_with_a_warning(self):
        self._check("""
            import tempfile
            first, second = tempfile.mkdtemp(), tempfile.mkdtemp()
            held = chdb.connect(f":memory:?udf_path={first}")
            conn, caught = connect_warnings(f":memory:?udf_path={second}")
            assert len(caught) == 1 and "Replaced the executable UDF paths" in caught[0] and first in caught[0], caught
            conn.close()
            held.close()
            print("OK")
        """)

    def test_server_setting_given_by_its_configuration_path_warns(self):
        self._check("""
            held = chdb.connect(":memory:")
            conn, caught = connect_warnings(":memory:?query_cache.max_entries=1")
            # Not applied as the query setting query_cache_max_entries, whose name it shares.
            assert len(caught) == 1 and caught[0].startswith("Ignoring startup options query_cache_max_entries=1 (running: 1024)"), caught
            conn.close()
            conn, caught = connect_warnings(":memory:?query_cache.max_entries=1024")
            assert caught == [], caught
            conn.close()
            held.close()
            print("OK")
        """)

    def test_failed_connection_keeps_the_running_udfs(self):
        self._check("""
            held = chdb.connect(f":memory:?udf_path={udf_dir('from_first', 'first')}")
            try:
                chdb.connect(f":memory:?udf_path={udf_dir('from_second', 'second')}&max_threads=notanumber")
            except Exception:
                pass
            else:
                raise AssertionError("connected")
            assert held.query("SELECT from_first(1)", "CSV").bytes() == b'"first"\\n'
            try:
                held.query("SELECT from_second(1)", "CSV")
            except Exception as e:
                assert "from_second" in str(e), str(e)
            else:
                raise AssertionError("from_second was loaded")
            held.close()
            print("OK")
        """)

    def test_other_udf_path_runs_its_own_scripts(self):
        # Both directories define the same function, so only the scripts they run differ.
        self._check("""
            held = chdb.connect(f":memory:?udf_path={udf_dir('which_dir', 'first')}")
            assert held.query("SELECT which_dir(1)", "CSV").bytes() == b'"first"\\n'
            conn, caught = connect_warnings(f":memory:?udf_path={udf_dir('which_dir', 'second')}")
            assert conn.query("SELECT which_dir(1)", "CSV").bytes() == b'"second"\\n'
            assert held.query("SELECT which_dir(1)", "CSV").bytes() == b'"second"\\n'
            conn.close()
            held.close()
            print("OK")
        """)

    def test_query_setting_that_is_also_a_server_setting(self):
        self._check("""
            held = chdb.connect(":memory:")
            conn, caught = connect_warnings(":memory:?max_table_size_to_drop=1")
            assert len(caught) == 1 and "apply to this connection's queries" in caught[0], caught
            assert str(conn.query("SELECT getSetting('max_table_size_to_drop')", "CSV")).strip() == "1"
            conn.close()
            held.close()
            print("OK")
        """)

    def test_equal_values_are_accepted_silently(self):
        self._check("""
            import tempfile
            directory = tempfile.mkdtemp()
            held = chdb.connect(f":memory:?max_server_memory_usage=9876543210&format_schema_path={directory}")
            # The same value, the default of a setting the running engine does not set, and the same path
            # written with a trailing slash.
            for conn_str in (
                ":memory:?max_server_memory_usage=9876543210",
                ":memory:?max_concurrent_queries=0",
                f":memory:?format_schema_path={directory}/",
            ):
                conn, caught = connect_warnings(conn_str)
                assert caught == [], (conn_str, caught)
                conn.close()
            held.close()
            print("OK")
        """)

    def test_query_settings_still_apply_per_connection(self):
        self._check("""
            held = chdb.connect(":memory:?max_threads=3")
            second, caught = connect_warnings(":memory:?max_threads=2")
            assert caught == [], caught
            assert str(held.query("SELECT getSetting('max_threads')", "CSV")).strip() == "3"
            assert str(second.query("SELECT getSetting('max_threads')", "CSV")).strip() == "2"
            second.close()
            held.close()
            print("OK")
        """)

    def test_options_apply_once_no_other_connection_is_open(self):
        self._check("""
            held = chdb.connect(":memory:")
            held.close()
            conn, caught = connect_warnings(":memory:?max_server_memory_usage=9876543210")
            assert caught == [], caught
            assert server_setting(conn, "max_server_memory_usage") == '"9876543210",1', server_setting(conn, "max_server_memory_usage")
            conn.close()
            print("OK")
        """)

    def test_udf_defined_while_another_connection_is_open(self):
        self._check("""
            from chdb.udf import chdb_udf
            held = chdb.connect(":memory:")

            @chdb_udf()
            def shout(value):
                return value.upper()

            assert chdb.query("SELECT 1", "CSV").bytes() == b"1\\n"
            assert chdb.query("SELECT shout('ab')", "CSV").bytes() == b'"AB"\\n'
            held.close()
            print("OK")
        """)

    def test_session_then_datastore_style_options(self):
        # The options the DataStore layer passes on every connection, while a Session holds the engine.
        self._check("""
            from chdb import session
            sess = session.Session()
            conn, caught = connect_warnings(
                ":memory:?memory_worker_correct_memory_tracker=1&max_server_memory_usage=0&max_server_memory_usage_to_ram_ratio=0.9")
            assert conn.query("SELECT 1", "CSV").bytes() == b"1\\n"
            # The values equal the running engine's, so nothing is reported.
            assert caught == [], caught
            conn.close()
            sess.close()
            print("OK")
        """)


if __name__ == "__main__":
    unittest.main()
