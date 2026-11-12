#!python3

import importlib.util
import json
import os
import tempfile
import types
import unittest

from chdb.state.sqlitelike import connect

LOGS_CONFIG = """<clickhouse>
    <logger>
        <log>{log_path}</log>
        <level>information</level>
        <async>false</async>
    </logger>
    <part_log>
        <database>system</database>
        <table>part_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </part_log>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </query_log>
</clickhouse>
"""

ROWS = 100000
# A UInt64 column alone serializes to 8 bytes per row before compression.
MIN_COMPRESSED_INPUT = 8 * ROWS


def load_profile_module():
    path = os.path.join(os.path.dirname(__file__), "..", "benchmark", "load_profile.py")
    spec = importlib.util.spec_from_file_location("load_profile", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def scalar(conn, sql):
    return str(conn.query(sql, "TSVRaw")).strip()


class TestMergeInstrumentation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "db")
        self.log_path = os.path.join(self.tmp.name, "chdb.log")
        self.config = os.path.join(self.tmp.name, "config.xml")
        with open(self.config, "w") as f:
            f.write(LOGS_CONFIG.format(log_path=self.log_path))

    def tearDown(self):
        self.tmp.cleanup()

    def connect_with_logs(self):
        return connect(f"{self.db_path}?config-file={self.config}")

    def shutdown_lines(self):
        with open(self.log_path) as f:
            return [line.split("<Information> EmbeddedServer: ", 1)[1].rstrip()
                    for line in f if "EmbeddedServer: Shutting down" in line]

    def test_insert_reports_compression_in_query_log(self):
        conn = self.connect_with_logs()
        try:
            conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
            conn.query(f"INSERT INTO t SELECT number FROM numbers({ROWS}) SETTINGS log_comment = 'compress_insert'")
            conn.query("SYSTEM FLUSH LOGS")
            blocks, compressed_input, has_time = scalar(
                conn,
                "SELECT ProfileEvents['CompressedWriteBufferBlocks'], "
                "ProfileEvents['CompressedWriteBufferBytes'], "
                "mapContains(ProfileEvents, 'CompressedWriteBufferCompressMicroseconds') "
                "FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = 'compress_insert'",
            ).split("\t")
            self.assertGreaterEqual(int(blocks), 1)
            self.assertGreaterEqual(int(compressed_input), MIN_COMPRESSED_INPUT)
            self.assertEqual(has_time, "1")
        finally:
            conn.close()

    def test_merge_reports_compression_in_part_log(self):
        conn = self.connect_with_logs()
        try:
            conn.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a")
            conn.query("SYSTEM STOP MERGES t")
            for i in range(3):
                conn.query(f"INSERT INTO t SELECT number FROM numbers({i * ROWS}, {ROWS})")
            conn.query("SYSTEM START MERGES t")
            conn.query("OPTIMIZE TABLE t FINAL")
            conn.query("SYSTEM FLUSH LOGS")
            inputs, rows, compressed_input = scalar(
                conn,
                "SELECT length(merged_from), rows, ProfileEvents['CompressedWriteBufferBytes'] "
                "FROM system.part_log WHERE event_type = 'MergeParts' AND table = 't'",
            ).split("\t")
            self.assertEqual(inputs, "3")
            self.assertEqual(rows, str(3 * ROWS))
            self.assertGreaterEqual(int(compressed_input), 3 * MIN_COMPRESSED_INPUT)
        finally:
            conn.close()

    def test_shutdown_logs_tables_left_with_unmerged_parts(self):
        conn = self.connect_with_logs()
        try:
            conn.query("CREATE TABLE fragmented (a UInt64) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2")
            conn.query("CREATE TABLE compact (a UInt64) ENGINE = MergeTree ORDER BY a")
            conn.query("SYSTEM STOP MERGES fragmented")
            for i in range(3):
                conn.query(f"INSERT INTO fragmented SELECT number FROM numbers({i * 10}, 10)")
            conn.query("INSERT INTO compact SELECT number FROM numbers(10)")
        finally:
            conn.close()

        lines = self.shutdown_lines()
        self.assertEqual(len(lines), 1, lines)
        self.assertRegex(
            lines[0],
            r"^Shutting down default\.fragmented with 6 active parts in 2 partitions \(\d+(\.\d+)? \w+\), "
            r"0 outdated parts, cancelling 0 running merges over 0 parts \(0\.00 B\)$",
        )

    def test_load_profile_reports_insert_parts_merges_and_final_layout(self):
        create = os.path.join(self.tmp.name, "create.sql")
        with open(create, "w") as f:
            f.write("CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2;")

        profile = load_profile_module()
        report = profile.run(types.SimpleNamespace(
            path=self.db_path,
            create=create,
            load=f"INSERT INTO t SELECT number, toString(number) FROM numbers({4 * 50000})",
            table="t",
            settings="max_threads = 1, max_block_size = 50000, max_insert_block_size = 50000, "
                     "min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0",
            final=True,
            settle_seconds=0,
            overwrite=False,
        ))
        json.dumps(report)

        # Four blocks, each split across both partitions.
        self.assertEqual(report["inserts"]["load"]["parts"], 8)
        self.assertEqual(report["inserts"]["load"]["partitions"], 2)
        self.assertEqual(report["inserts"]["load"]["rows"]["sum"], 4 * 50000)
        self.assertEqual(report["pipeline"]["merge_tree_sinks"], 1)
        # Background merges may already have started on the eight parts.
        self.assertIn(report["stages"]["after_load"]["active_parts"], range(2, 9))
        self.assertEqual(report["stages"]["after_final"]["active_parts"], 2)
        self.assertEqual(sorted(report["stages"]["after_final"]["partitions"]), ["default.t/0", "default.t/1"])
        self.assertEqual(report["stages"]["after_reopen"]["active_parts"], 2)
        self.assertEqual(report["stages"]["after_reopen"]["active_rows"]["sum"], 4 * 50000)

        merges = report["merges"]
        self.assertGreaterEqual(merges["count"], 2)
        self.assertEqual(merges["failed"], 0)
        # Each inserted part ends up inside one of the two final parts.
        self.assertGreaterEqual(merges["input_parts"], 8)
        self.assertGreater(merges["events"]["CompressedWriteBufferBytes"], 0)

        load_events = report["queries"]["load"]["events"]
        self.assertEqual(int(load_events["MergeTreeDataWriterRows"]), 4 * 50000)
        self.assertGreaterEqual(int(load_events["CompressedWriteBufferBytes"]), 8 * 4 * 50000)
        self.assertGreater(report["data_bytes_after_close"], 0)


if __name__ == "__main__":
    unittest.main()
