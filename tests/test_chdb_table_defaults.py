#!python3

import glob
import os
import re
import tempfile
import unittest

from chdb.state.sqlitelike import connect

CLICKHOUSE_DEFAULT = "basic, uniq_v2"

MERGE_TREE_CONFIG = """<clickhouse>
    <merge_tree>
        <auto_statistics_types>basic</auto_statistics_types>
    </merge_tree>
</clickhouse>
"""


def rows(conn, sql):
    out = str(conn.query(sql, "TSVRaw")).rstrip("\n")
    return [line.split("\t") for line in out.split("\n")] if out else []


def part_statistics(conn, table):
    """{column: statistics} materialized in the table's parts after a merge.

    chDB does not build statistics on insert (materialize_statistics_on_insert
    defaults to 0), so merge first: that is where statistics come from."""
    conn.query(f"OPTIMIZE TABLE {table} FINAL")
    return {
        column: stats
        for column, stats in rows(
            conn,
            f"SELECT DISTINCT column, toString(statistics) FROM system.parts_columns "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active ORDER BY column",
        )
    }


def storage_settings(conn, table):
    create = str(conn.query(f"SHOW CREATE TABLE {table}", "TSVRaw"))
    return re.search(r"SETTINGS (.*)$", create.strip(), re.S).group(1)


class TestAutoStatisticsDefault(unittest.TestCase):
    """chDB creates MergeTree tables without implicit statistics and with LZ4; the
    choice is written into the table definition so it does not leak into tables
    created under the ClickHouse defaults."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_new_table_has_no_implicit_statistics(self):
        conn = connect(self.db_path)
        try:
            conn.query("CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
            conn.query("INSERT INTO t SELECT number, toString(number) FROM numbers(1000)")
            self.assertEqual(part_statistics(conn, "t"), {"a": "[]", "b": "[]"})
            self.assertEqual(
                storage_settings(conn, "t"),
                "index_granularity = 8192, auto_statistics_types = '', default_compression_codec = 'LZ4'",
            )
            self.assertEqual(
                rows(conn, "SELECT name, statistics FROM system.columns "
                           "WHERE database = currentDatabase() AND table = 't' ORDER BY position"),
                [["a", ""], ["b", ""]],
            )
        finally:
            conn.close()

    def test_explicit_table_setting_restores_implicit_statistics(self):
        conn = connect(self.db_path)
        try:
            conn.query(
                "CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a "
                f"SETTINGS auto_statistics_types = '{CLICKHOUSE_DEFAULT}'"
            )
            conn.query("INSERT INTO t SELECT number, toString(number) FROM numbers(1000)")
            self.assertEqual(
                part_statistics(conn, "t"),
                {"a": "['Basic','UniqV2']", "b": "['Basic','UniqV2']"},
            )
            self.assertEqual(
                storage_settings(conn, "t"),
                f"auto_statistics_types = '{CLICKHOUSE_DEFAULT}', index_granularity = 8192, default_compression_codec = 'LZ4'",
            )
        finally:
            conn.close()

    def test_explicit_column_statistics_are_built_and_estimated(self):
        conn = connect(self.db_path)
        try:
            conn.query(
                "CREATE TABLE t (a UInt64, b UInt64 STATISTICS(tdigest), c UInt64) "
                "ENGINE = MergeTree ORDER BY a"
            )
            conn.query("INSERT INTO t SELECT number, number % 100, number FROM numbers(10000)")
            self.assertEqual(
                part_statistics(conn, "t"),
                {"a": "[]", "b": "['TDigest']", "c": "[]"},
            )
            conn.query("ALTER TABLE t ADD STATISTICS c TYPE basic")
            conn.query("ALTER TABLE t MATERIALIZE STATISTICS c SETTINGS mutations_sync = 1")
            self.assertEqual(
                part_statistics(conn, "t"),
                {"a": "[]", "b": "['TDigest']", "c": "['Basic']"},
            )
            # The estimator reads the explicit `basic` statistics; the columns
            # without statistics have no estimates.
            self.assertEqual(
                rows(conn, "SELECT column, estimates.min, estimates.max, estimates.default_count "
                           "FROM system.parts_columns WHERE database = currentDatabase() "
                           "AND table = 't' AND active ORDER BY column"),
                [["a", "\\N", "\\N", "\\N"], ["b", "\\N", "\\N", "\\N"], ["c", "0", "9999", "1"]],
            )
        finally:
            conn.close()

    def test_alter_modify_setting_enables_implicit_statistics(self):
        conn = connect(self.db_path)
        try:
            conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
            conn.query("ALTER TABLE t MODIFY SETTING auto_statistics_types = 'basic'")
            conn.query("INSERT INTO t SELECT number FROM numbers(100)")
            self.assertEqual(part_statistics(conn, "t"), {"a": "['Basic']"})
        finally:
            conn.close()

    def test_new_table_keeps_the_default_after_reopen(self):
        conn = connect(self.db_path)
        try:
            conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
        finally:
            conn.close()

        conn = connect(self.db_path)
        try:
            conn.query("INSERT INTO t SELECT number FROM numbers(100)")
            self.assertEqual(part_statistics(conn, "t"), {"a": "[]"})
            self.assertEqual(
                storage_settings(conn, "t"),
                "index_granularity = 8192, auto_statistics_types = '', default_compression_codec = 'LZ4'",
            )
        finally:
            conn.close()

    def test_table_created_before_the_default_keeps_implicit_statistics_on_reopen(self):
        conn = connect(self.db_path)
        try:
            conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
        finally:
            conn.close()

        # Rewrite the stored definition into what an older chDB wrote: no
        # auto_statistics_types, so the table follows the ClickHouse default.
        (metadata,) = glob.glob(os.path.join(self.db_path, "store", "*", "*", "t.sql"))
        with open(metadata) as f:
            definition = f.read()
        chdb_defaults = ", auto_statistics_types = '', default_compression_codec = 'LZ4'"
        self.assertIn(chdb_defaults, definition)
        with open(metadata, "w") as f:
            f.write(definition.replace(chdb_defaults, ""))

        conn = connect(self.db_path)
        try:
            conn.query("INSERT INTO t SELECT number FROM numbers(100)")
            self.assertEqual(part_statistics(conn, "t"), {"a": "['Basic','UniqV2']"})
            self.assertEqual(storage_settings(conn, "t"), "index_granularity = 8192")
        finally:
            conn.close()

    def test_merge_tree_config_value_wins_over_chdb_default(self):
        config = os.path.join(self.tmp.name, "merge_tree.xml")
        with open(config, "w") as f:
            f.write(MERGE_TREE_CONFIG)

        conn = connect(f"{self.db_path}?config-file={config}")
        try:
            conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
            conn.query("INSERT INTO t SELECT number FROM numbers(100)")
            self.assertEqual(part_statistics(conn, "t"), {"a": "['Basic']"})
            self.assertEqual(storage_settings(conn, "t"), "index_granularity = 8192, default_compression_codec = 'LZ4'")
        finally:
            conn.close()

    def test_merge_tree_family_engines_have_no_implicit_statistics(self):
        engines = {
            "replacing": "ReplacingMergeTree",
            "summing": "SummingMergeTree",
            "aggregating": "AggregatingMergeTree",
            "collapsing": "CollapsingMergeTree(s)",
            "versioned": "VersionedCollapsingMergeTree(s, a)",
        }
        conn = connect(self.db_path)
        try:
            for table, engine in engines.items():
                conn.query(
                    f"CREATE TABLE {table} (a UInt64, s Int8) ENGINE = {engine} ORDER BY a"
                )
                conn.query(f"INSERT INTO {table} SELECT number, 1 FROM numbers(100)")
                self.assertEqual(
                    part_statistics(conn, table), {"a": "[]", "s": "[]"}, engine
                )
                self.assertEqual(
                    storage_settings(conn, table),
                    "index_granularity = 8192, auto_statistics_types = '', default_compression_codec = 'LZ4'",
                    engine,
                )

            conn.query("CREATE TABLE ctas ENGINE = MergeTree ORDER BY a AS SELECT number AS a FROM numbers(100)")
            self.assertEqual(part_statistics(conn, "ctas"), {"a": "[]"})
        finally:
            conn.close()


# Two parts of incompressible data whose merge reads more than the 100 MiB above which
# ClickHouse's size-aware default switches merged parts from LZ4 to ZSTD(3).
LARGE_PART_ROWS = 8_000_000

MERGE_TREE_CODEC_CONFIG = """<clickhouse>
    <merge_tree>
        <default_compression_codec>ZSTD(1)</default_compression_codec>
    </merge_tree>
</clickhouse>
"""


class TestCompressionCodecDefault(unittest.TestCase):
    """Merged parts of a new chDB table stay LZ4 however large they get."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "db")

    def tearDown(self):
        self.tmp.cleanup()

    def merged_part_codec(self, conn, settings=""):
        conn.query(f"CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() {settings}")
        conn.query("SYSTEM STOP MERGES t")
        for _ in range(2):
            conn.query(f"INSERT INTO t SELECT rand64() FROM numbers({LARGE_PART_ROWS})")
        conn.query("SYSTEM START MERGES t")
        conn.query("OPTIMIZE TABLE t FINAL")
        return rows(conn, "SELECT default_compression_codec, rows FROM system.parts "
                          "WHERE database = currentDatabase() AND table = 't' AND active")

    def test_large_merge_writes_lz4(self):
        conn = connect(self.db_path)
        try:
            self.assertEqual(self.merged_part_codec(conn), [["LZ4", str(2 * LARGE_PART_ROWS)]])
        finally:
            conn.close()

    def test_explicit_empty_setting_restores_clickhouse_size_aware_default(self):
        conn = connect(self.db_path)
        try:
            self.assertEqual(
                self.merged_part_codec(conn, "SETTINGS default_compression_codec = ''"),
                [["ZSTD(3)", str(2 * LARGE_PART_ROWS)]],
            )
        finally:
            conn.close()

    def test_merge_tree_config_codec_wins_over_chdb_default(self):
        config = os.path.join(self.tmp.name, "codec.xml")
        with open(config, "w") as f:
            f.write(MERGE_TREE_CODEC_CONFIG)
        conn = connect(f"{self.db_path}?config-file={config}")
        try:
            self.assertEqual(self.merged_part_codec(conn), [["ZSTD(1)", str(2 * LARGE_PART_ROWS)]])
            self.assertEqual(
                storage_settings(conn, "t"),
                "index_granularity = 8192, auto_statistics_types = ''",
            )
        finally:
            conn.close()

    def test_column_codec_wins_over_table_default(self):
        conn = connect(self.db_path)
        try:
            conn.query("CREATE TABLE t (a UInt64 CODEC(ZSTD(1)), b UInt64) ENGINE = MergeTree ORDER BY tuple()")
            conn.query("INSERT INTO t SELECT number, number FROM numbers(1000)")
            self.assertEqual(
                rows(conn, "SELECT name, compression_codec FROM system.columns "
                           "WHERE database = currentDatabase() AND table = 't' ORDER BY position"),
                [["a", "CODEC(ZSTD(1))"], ["b", ""]],
            )
        finally:
            conn.close()


class TestMaxInsertThreadsDefault(unittest.TestCase):
    """chDB inserts with one thread unless the query asks for more."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = connect(os.path.join(self.tmp.name, "db"))
        self.conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def sinks(self, settings=""):
        """Number of MergeTreeSink processors in the INSERT pipeline."""
        plan = str(self.conn.query(f"EXPLAIN PIPELINE INSERT INTO t SELECT number FROM numbers_mt(1000000) {settings}", "TSVRaw"))
        return sum(1 for line in plan.splitlines() if "MergeTreeSink" in line)

    def test_default_is_one_thread_and_not_reported_as_user_set(self):
        self.assertEqual(
            rows(self.conn, "SELECT value, changed FROM system.settings WHERE name = 'max_insert_threads'"),
            [["1", "0"]],
        )
        self.assertEqual(self.sinks(), 1)

    def test_query_setting_raises_it(self):
        self.assertEqual(self.sinks("SETTINGS max_insert_threads = 4"), 4)


if __name__ == "__main__":
    unittest.main()
