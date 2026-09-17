#!python3
"""Direct querying of pyarrow.RecordBatch and pyarrow.dataset.Dataset.

Both reach the dedicated PyArrow scan of the Python() table engine (the same
one pyarrow.Table uses): the object is wrapped in / used as a
pyarrow.dataset.Dataset, the projection is pushed into its scanner and the
resulting RecordBatchReader is consumed over the Arrow C stream interface.

Most assertions are mirrors: the same query is run over the RecordBatch /
Dataset and over an equivalent pyarrow.Table, and the two results must agree on
columns, types, values and row order. That pins the new inputs to the behaviour
of the already-supported one instead of to hand-copied literals.
"""

import os
import shutil
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

import chdb
from chdb import session

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

try:
    import pandas as pd
except ImportError:
    pd = None


# Schema/values shared by the mirror tests. Deliberately mixes a nullable string
# column and a negative float so NULL handling and sign survive the C ABI hop.
SAMPLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float64()),
    ]
)
SAMPLE_ROWS = [
    {"id": 1, "name": "Alice", "score": 9.5},
    {"id": 2, "name": "宝贝", "score": 7.25},
    {"id": 3, "name": None, "score": -1.5},
]


def sample_batch():
    return pa.record_batch(
        [
            pa.array([1, 2, 3], pa.int64()),
            pa.array(["Alice", "宝贝", None], pa.string()),
            pa.array([9.5, 7.25, -1.5], pa.float64()),
        ],
        schema=SAMPLE_SCHEMA,
    )


class TestArrowRecordBatchInput(unittest.TestCase):
    def test_record_batch_select_star_matches_source_columns_values_and_order(self):
        rb_star = sample_batch()
        out = chdb.query("SELECT * FROM Python(rb_star) ORDER BY id", "ArrowTable")
        self.assertEqual(out.column_names, ["id", "name", "score"])
        self.assertEqual(out.to_pylist(), SAMPLE_ROWS)
        self.assertEqual(out.to_pylist(), rb_star.to_pylist())

    def test_record_batch_result_identical_to_equivalent_table_result(self):
        rb_mirror = sample_batch()
        tbl_mirror = pa.Table.from_batches([rb_mirror])
        sql = "SELECT id, name, score FROM Python({}) ORDER BY id"
        rb_out = chdb.query(sql.format("rb_mirror"), "ArrowTable")
        tbl_out = chdb.query(sql.format("tbl_mirror"), "ArrowTable")
        self.assertEqual(rb_out.schema, tbl_out.schema)
        self.assertEqual(rb_out.to_pylist(), tbl_out.to_pylist())

    def test_record_batch_describe_identical_to_equivalent_table_describe(self):
        rb_desc = sample_batch()
        tbl_desc = pa.Table.from_batches([rb_desc])
        rb_out = chdb.query("DESCRIBE Python(rb_desc)", "ArrowTable")
        tbl_out = chdb.query("DESCRIBE Python(tbl_desc)", "ArrowTable")
        rb_cols = [(r["name"], r["type"]) for r in rb_out.to_pylist()]
        tbl_cols = [(r["name"], r["type"]) for r in tbl_out.to_pylist()]
        self.assertEqual([n for n, _ in rb_cols], ["id", "name", "score"])
        self.assertEqual(rb_cols, tbl_cols)

    def test_record_batch_projection_subset_returns_requested_columns_in_order(self):
        rb_proj = pa.record_batch(
            [
                pa.array([1, 2], pa.int64()),
                pa.array(["x", "y"], pa.string()),
                pa.array([1.5, 2.5], pa.float64()),
            ],
            names=["a", "b", "c"],
        )
        # Reversed order w.r.t. the schema: a name-keyed projection must not
        # shuffle values between columns.
        out = chdb.query("SELECT c, a FROM Python(rb_proj) ORDER BY a", "ArrowTable")
        self.assertEqual(out.column_names, ["c", "a"])
        self.assertEqual(out.to_pylist(), [{"c": 1.5, "a": 1}, {"c": 2.5, "a": 2}])

    def test_record_batch_aggregation_matches_pyarrow_compute(self):
        rb_agg = pa.record_batch(
            [
                pa.array(["a", "b", "a", "b", "a"], pa.string()),
                pa.array([1, 2, 3, 4, 5], pa.int64()),
            ],
            names=["g", "v"],
        )
        out = chdb.query(
            "SELECT count() AS c, sum(v) AS s, min(v) AS lo, max(v) AS hi"
            " FROM Python(rb_agg)",
            "ArrowTable",
        )
        self.assertEqual(
            out.to_pylist(),
            [
                {
                    "c": rb_agg.num_rows,
                    "s": pc.sum(rb_agg.column("v")).as_py(),
                    "lo": pc.min(rb_agg.column("v")).as_py(),
                    "hi": pc.max(rb_agg.column("v")).as_py(),
                }
            ],
        )

    def test_record_batch_group_by_matches_pyarrow_group_by(self):
        rb_grp = pa.record_batch(
            [
                pa.array(["a", "b", "a", "b", "a"], pa.string()),
                pa.array([1, 2, 3, 4, 5], pa.int64()),
            ],
            names=["g", "v"],
        )
        groups = {}
        for row in rb_grp.to_pylist():
            agg = groups.setdefault(row["g"], {"g": row["g"], "v_sum": 0, "count_all": 0})
            agg["v_sum"] += row["v"]
            agg["count_all"] += 1
        expected = [groups[key] for key in sorted(groups)]
        out = chdb.query(
            "SELECT g, sum(v) AS v_sum, count() AS count_all"
            " FROM Python(rb_grp) GROUP BY g ORDER BY g",
            "ArrowTable",
        )
        self.assertEqual(out.to_pylist(), expected)

    def test_record_batch_where_filter_matches_pyarrow_filter(self):
        rb_filt = sample_batch()
        expected = pa.Table.from_batches(
            [rb_filt.filter(pc.greater(rb_filt.column("score"), 0))]
        )
        out = chdb.query(
            "SELECT * FROM Python(rb_filt) WHERE score > 0 ORDER BY id", "ArrowTable"
        )
        self.assertEqual(out.to_pylist(), expected.to_pylist())

    def test_sliced_record_batch_respects_offset_and_length(self):
        # A sliced RecordBatch keeps the parent buffers and carries a non-zero
        # offset; the scan must honour it instead of re-reading from row 0.
        rb_slice = pa.record_batch(
            [pa.array([10, 20, 30, 40, 50], pa.int64())], names=["v"]
        ).slice(1, 3)
        out = chdb.query("SELECT v FROM Python(rb_slice) ORDER BY v", "ArrowTable")
        self.assertEqual(out.to_pylist(), [{"v": 20}, {"v": 30}, {"v": 40}])
        self.assertEqual(out.to_pylist(), rb_slice.to_pylist())

    def test_empty_record_batch_yields_no_rows_but_keeps_schema(self):
        rb_empty = pa.record_batch(
            [pa.array([], pa.int64()), pa.array([], pa.string())], names=["a", "b"]
        )
        self.assertEqual(
            str(chdb.query("SELECT count() FROM Python(rb_empty)", "CSV")), "0\n"
        )
        desc = chdb.query("DESCRIBE Python(rb_empty)", "ArrowTable")
        self.assertEqual([r["name"] for r in desc.to_pylist()], ["a", "b"])

    def test_record_batch_null_values_survive_as_sql_nulls(self):
        rb_nulls = sample_batch()
        out = chdb.query(
            "SELECT id, isNull(name) AS is_null FROM Python(rb_nulls) ORDER BY id",
            "ArrowTable",
        )
        self.assertEqual(
            out.to_pylist(),
            [
                {"id": r["id"], "is_null": r["name"] is None}
                for r in sorted(SAMPLE_ROWS, key=lambda r: r["id"])
            ],
        )

    def test_record_batch_type_coverage_matches_equivalent_table(self):
        ts = datetime(2026, 7, 22, 1, 2, 3, 456000, tzinfo=timezone.utc)
        schema = pa.schema(
            [
                pa.field("i32", pa.int32()),
                pa.field("i64", pa.int64()),
                pa.field("u16", pa.uint16()),
                pa.field("f32", pa.float32()),
                pa.field("f64", pa.float64()),
                pa.field("flag", pa.bool_()),
                pa.field("s", pa.string()),
                pa.field("d", pa.date32()),
                pa.field("ts", pa.timestamp("us", tz="Asia/Shanghai")),
                pa.field("dec", pa.decimal128(18, 4)),
                pa.field("lst", pa.list_(pa.int64())),
                pa.field("dict_col", pa.dictionary(pa.int32(), pa.string())),
            ]
        )
        rb_types = pa.record_batch(
            [
                pa.array([1, -2], pa.int32()),
                pa.array([10, -20], pa.int64()),
                pa.array([7, 8], pa.uint16()),
                pa.array([1.5, -2.5], pa.float32()),
                pa.array([1.25, -2.75], pa.float64()),
                pa.array([True, False], pa.bool_()),
                pa.array(["ok", None], pa.string()),
                pa.array([date(2026, 1, 2), date(1999, 12, 31)], pa.date32()),
                pa.array([ts, None], pa.timestamp("us", tz="Asia/Shanghai")),
                pa.array([Decimal("123.4567"), Decimal("-0.0001")], pa.decimal128(18, 4)),
                pa.array([[1, 2, 3], []], pa.list_(pa.int64())),
                pa.array(["x", "y"], pa.dictionary(pa.int32(), pa.string())),
            ],
            schema=schema,
        )
        tbl_types = pa.Table.from_batches([rb_types])
        sql = "SELECT * FROM Python({}) ORDER BY i64"
        rb_out = chdb.query(sql.format("rb_types"), "ArrowTable")
        tbl_out = chdb.query(sql.format("tbl_types"), "ArrowTable")
        self.assertEqual(rb_out.schema, tbl_out.schema)
        self.assertEqual(rb_out.to_pylist(), tbl_out.to_pylist())
        # And the values really are the source values, not an empty mirror.
        self.assertEqual(rb_out.num_rows, 2)
        rows = rb_out.to_pylist()
        self.assertEqual([r["i64"] for r in rows], [-20, 10])
        self.assertEqual([r["s"] for r in rows], [None, "ok"])
        self.assertEqual([r["lst"] for r in rows], [[], [1, 2, 3]])
        self.assertEqual([r["dict_col"] for r in rows], ["y", "x"])
        self.assertEqual(
            [r["dec"] for r in rows], [Decimal("-0.0001"), Decimal("123.4567")]
        )

    def test_large_record_batch_is_sliced_into_blocks(self):
        rb_big = pa.record_batch([pa.array(range(1_000_000), pa.int64())], names=["x"])
        out = chdb.query(
            "SELECT count() AS c, sum(x) AS s, min(x) AS lo, max(x) AS hi"
            " FROM Python(rb_big) SETTINGS max_block_size = 65536",
            "CSV",
        )
        self.assertEqual(str(out), f"1000000,{1_000_000 * 999_999 // 2},0,999999\n")

    def test_large_record_batch_under_parallel_scan(self):
        rb_par = pa.record_batch([pa.array(range(500_000), pa.int64())], names=["x"])
        out = chdb.query(
            "SELECT count() AS c, sum(x) AS s FROM Python(rb_par)"
            " SETTINGS max_threads = 8",
            "CSV",
        )
        self.assertEqual(str(out), f"500000,{500_000 * 499_999 // 2}\n")

    def test_record_batch_is_rescannable_across_queries(self):
        # Unlike a pyarrow.RecordBatchReader (single-use stream), a RecordBatch
        # holds its data and must answer every query in full.
        rb_twice = pa.record_batch([pa.array([1, 2, 3], pa.int64())], names=["x"])
        first = chdb.query("SELECT count() AS c, sum(x) AS s FROM Python(rb_twice)", "CSV")
        second = chdb.query("SELECT count() AS c, sum(x) AS s FROM Python(rb_twice)", "CSV")
        self.assertEqual(str(first), "3,6\n")
        self.assertEqual(str(second), str(first))

    def test_record_batch_self_join_rescans_full_data(self):
        rb_join = pa.record_batch([pa.array([1, 2, 3], pa.int64())], names=["x"])
        out = chdb.query(
            "SELECT count() FROM Python(rb_join) AS a JOIN Python(rb_join) AS b"
            " ON a.x = b.x",
            "CSV",
        )
        self.assertEqual(str(out), "3\n")

    def test_record_batch_joined_with_pyarrow_table(self):
        rb_left = pa.record_batch(
            [pa.array([1, 2, 3], pa.int64()), pa.array(["a", "b", "c"], pa.string())],
            names=["id", "tag"],
        )
        tbl_right = pa.table({"id": pa.array([2, 3, 4], pa.int64()), "w": [20, 30, 40]})
        out = chdb.query(
            "SELECT l.id AS id, l.tag AS tag, r.w AS w FROM Python(rb_left) AS l"
            " JOIN Python(tbl_right) AS r ON l.id = r.id ORDER BY id",
            "ArrowTable",
        )
        self.assertEqual(
            out.to_pylist(),
            [{"id": 2, "tag": "b", "w": 20}, {"id": 3, "tag": "c", "w": 30}],
        )

    def test_record_batch_in_stateful_session(self):
        rb_sess = sample_batch()
        sess = session.Session()
        try:
            out = sess.query(
                "SELECT count() AS c, sum(id) AS s FROM Python(rb_sess)", "CSV"
            )
            self.assertEqual(str(out), "3,6\n")
        finally:
            sess.close()

    def test_record_batch_takes_dedicated_pyarrow_dataset_path(self):
        # Falsifiable structural check: the dedicated PyArrow path wraps the
        # batch with pyarrow.dataset.dataset(). If the batch fell through to the
        # generic __arrow_c_stream__ scan (or to the dict/__getitem__ scan), the
        # wrapper below would never be called.
        rb_spy = pa.record_batch([pa.array([1, 2, 3], pa.int64())], names=["x"])
        calls = []
        original = ds.dataset

        def spy(source, *args, **kwargs):
            calls.append(type(source).__name__)
            return original(source, *args, **kwargs)

        ds.dataset = spy
        try:
            out = chdb.query("SELECT sum(x) FROM Python(rb_spy)", "CSV")
        finally:
            ds.dataset = original
        self.assertEqual(str(out), "6\n")
        self.assertTrue(calls, "pyarrow.dataset.dataset() was never called")
        self.assertEqual(set(calls), {"RecordBatch"})


class TestArrowDatasetInput(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="chdb_arrow_ds_")

        # Multi-file parquet dataset: 3 files x 4 rows.
        cls.parquet_dir = os.path.join(cls.tmpdir, "parquet")
        os.makedirs(cls.parquet_dir)
        cls.parquet_rows = []
        for f in range(3):
            ids = [f * 4 + i for i in range(4)]
            tbl = pa.table(
                {
                    "id": pa.array(ids, pa.int64()),
                    "grp": pa.array([f"g{f}"] * 4, pa.string()),
                    "val": pa.array([float(i) / 2 for i in ids], pa.float64()),
                }
            )
            pq.write_table(tbl, os.path.join(cls.parquet_dir, f"part-{f}.parquet"))
            cls.parquet_rows.extend(tbl.to_pylist())

        # Hive-partitioned parquet dataset.
        cls.hive_dir = os.path.join(cls.tmpdir, "hive")
        cls.hive_expected = {"a": [1, 2], "b": [3, 4, 5]}
        for key, vals in cls.hive_expected.items():
            part = os.path.join(cls.hive_dir, f"k={key}")
            os.makedirs(part)
            pq.write_table(
                pa.table({"v": pa.array(vals, pa.int64())}),
                os.path.join(part, "part.parquet"),
            )

        # Single CSV file dataset.
        cls.csv_path = os.path.join(cls.tmpdir, "rows.csv")
        with open(cls.csv_path, "w", encoding="utf-8") as fh:
            fh.write("n,s\n1,one\n2,two\n3,three\n")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_in_memory_dataset_result_identical_to_equivalent_table_result(self):
        tbl_mem = pa.Table.from_batches([sample_batch()])
        ds_mem = ds.dataset(tbl_mem)
        sql = "SELECT id, name, score FROM Python({}) ORDER BY id"
        ds_out = chdb.query(sql.format("ds_mem"), "ArrowTable")
        tbl_out = chdb.query(sql.format("tbl_mem"), "ArrowTable")
        self.assertEqual(ds_out.schema, tbl_out.schema)
        self.assertEqual(ds_out.to_pylist(), tbl_out.to_pylist())
        self.assertEqual(ds_out.to_pylist(), SAMPLE_ROWS)

    def test_in_memory_dataset_describe_identical_to_equivalent_table_describe(self):
        tbl_desc2 = pa.Table.from_batches([sample_batch()])
        ds_desc = ds.dataset(tbl_desc2)
        ds_cols = [
            (r["name"], r["type"])
            for r in chdb.query("DESCRIBE Python(ds_desc)", "ArrowTable").to_pylist()
        ]
        tbl_cols = [
            (r["name"], r["type"])
            for r in chdb.query("DESCRIBE Python(tbl_desc2)", "ArrowTable").to_pylist()
        ]
        self.assertEqual([n for n, _ in ds_cols], ["id", "name", "score"])
        self.assertEqual(ds_cols, tbl_cols)

    def test_dataset_from_record_batch_matches_record_batch_scan(self):
        rb_src = sample_batch()
        ds_from_rb = ds.dataset(rb_src)
        sql = "SELECT id, name, score FROM Python({}) ORDER BY id"
        ds_out = chdb.query(sql.format("ds_from_rb"), "ArrowTable")
        rb_out = chdb.query(sql.format("rb_src"), "ArrowTable")
        self.assertEqual(ds_out.to_pylist(), rb_out.to_pylist())
        self.assertEqual(ds_out.to_pylist(), SAMPLE_ROWS)

    def test_multi_file_parquet_dataset_scans_every_file(self):
        ds_files = ds.dataset(self.parquet_dir, format="parquet")
        expected_ids = sorted(r["id"] for r in self.parquet_rows)
        out = chdb.query(
            "SELECT count() AS c, sum(id) AS s, uniqExact(grp) AS groups"
            " FROM Python(ds_files)",
            "ArrowTable",
        )
        self.assertEqual(
            out.to_pylist(),
            [{"c": len(expected_ids), "s": sum(expected_ids), "groups": 3}],
        )

    def test_multi_file_parquet_dataset_rows_match_pyarrow_scan(self):
        ds_rows = ds.dataset(self.parquet_dir, format="parquet")
        expected = sorted(self.parquet_rows, key=lambda r: r["id"])
        out = chdb.query(
            "SELECT id, grp, val FROM Python(ds_rows) ORDER BY id", "ArrowTable"
        )
        self.assertEqual(out.column_names, ["id", "grp", "val"])
        self.assertEqual(out.to_pylist(), expected)

    def test_single_parquet_file_dataset(self):
        one_file = os.path.join(self.parquet_dir, "part-1.parquet")
        ds_one = ds.dataset(one_file, format="parquet")
        expected = pq.read_table(one_file).to_pylist()
        out = chdb.query("SELECT * FROM Python(ds_one) ORDER BY id", "ArrowTable")
        self.assertEqual(out.to_pylist(), sorted(expected, key=lambda r: r["id"]))

    def test_dataset_projection_subset_returns_requested_columns_in_order(self):
        ds_proj = ds.dataset(self.parquet_dir, format="parquet")
        expected = sorted(self.parquet_rows, key=lambda r: r["id"])
        out = chdb.query(
            "SELECT grp, id FROM Python(ds_proj) ORDER BY id", "ArrowTable"
        )
        self.assertEqual(out.column_names, ["grp", "id"])
        self.assertEqual(
            out.to_pylist(), [{"grp": r["grp"], "id": r["id"]} for r in expected]
        )

    def test_dataset_where_filter_matches_pyarrow_filter(self):
        ds_filt = ds.dataset(self.parquet_dir, format="parquet")
        expected = sorted(
            (r for r in self.parquet_rows if r["id"] >= 5 and r["grp"] != "g2"),
            key=lambda r: r["id"],
        )
        out = chdb.query(
            "SELECT id, grp, val FROM Python(ds_filt)"
            " WHERE id >= 5 AND grp != 'g2' ORDER BY id",
            "ArrowTable",
        )
        self.assertEqual(out.to_pylist(), expected)

    def test_hive_partitioned_dataset_exposes_partition_column(self):
        ds_hive = ds.dataset(self.hive_dir, format="parquet", partitioning="hive")
        out = chdb.query(
            "SELECT k, count() AS c, sum(v) AS s FROM Python(ds_hive)"
            " GROUP BY k ORDER BY k",
            "ArrowTable",
        )
        self.assertEqual(
            out.to_pylist(),
            [
                {"k": key, "c": len(vals), "s": sum(vals)}
                for key, vals in sorted(self.hive_expected.items())
            ],
        )

    def test_hive_partition_column_alone_is_projectable(self):
        # The partition column is not stored in the parquet files; selecting it
        # on its own proves the projection is applied by the dataset scanner.
        ds_hive_k = ds.dataset(self.hive_dir, format="parquet", partitioning="hive")
        out = chdb.query(
            "SELECT k, count() AS c FROM Python(ds_hive_k) GROUP BY k ORDER BY k",
            "ArrowTable",
        )
        self.assertEqual(
            out.to_pylist(),
            [
                {"k": key, "c": len(vals)}
                for key, vals in sorted(self.hive_expected.items())
            ],
        )

    def test_csv_format_dataset(self):
        ds_csv = ds.dataset(self.csv_path, format="csv")
        out = chdb.query("SELECT n, s FROM Python(ds_csv) ORDER BY n", "ArrowTable")
        self.assertEqual(out.column_names, ["n", "s"])
        self.assertEqual(
            out.to_pylist(),
            [{"n": 1, "s": "one"}, {"n": 2, "s": "two"}, {"n": 3, "s": "three"}],
        )

    def test_union_dataset_scans_all_children(self):
        tbl_a = pa.table({"x": pa.array([1, 2], pa.int64())})
        tbl_b = pa.table({"x": pa.array([3, 4, 5], pa.int64())})
        ds_union = ds.dataset([ds.dataset(tbl_a), ds.dataset(tbl_b)])
        self.assertIsInstance(ds_union, ds.UnionDataset)
        out = chdb.query(
            "SELECT count() AS c, sum(x) AS s FROM Python(ds_union)", "ArrowTable"
        )
        self.assertEqual(out.to_pylist(), [{"c": 5, "s": 15}])

    def test_empty_dataset_yields_no_rows_but_keeps_schema(self):
        ds_empty = ds.dataset(
            pa.table(
                {
                    "a": pa.array([], pa.int64()),
                    "b": pa.array([], pa.string()),
                }
            )
        )
        self.assertEqual(
            str(chdb.query("SELECT count() FROM Python(ds_empty)", "CSV")), "0\n"
        )
        desc = chdb.query("DESCRIBE Python(ds_empty)", "ArrowTable")
        self.assertEqual([r["name"] for r in desc.to_pylist()], ["a", "b"])

    def test_dataset_with_empty_fragment_does_not_truncate_scan(self):
        # An empty file yields a zero-row batch mid-stream; it must be skipped
        # without ending the scan early.
        mixed = os.path.join(self.tmpdir, "mixed")
        os.makedirs(mixed, exist_ok=True)
        for name, vals in [("a-full", [1, 2]), ("b-empty", []), ("c-full", [3, 4, 5])]:
            pq.write_table(
                pa.table({"x": pa.array(vals, pa.int64())}),
                os.path.join(mixed, f"{name}.parquet"),
            )
        ds_mixed = ds.dataset(mixed, format="parquet")
        out = chdb.query(
            "SELECT count() AS c, sum(x) AS s FROM Python(ds_mixed)", "ArrowTable"
        )
        self.assertEqual(out.to_pylist(), [{"c": 5, "s": 15}])

    def test_dataset_over_chunked_table_with_empty_chunk(self):
        ds_chunked = ds.dataset(
            pa.table({"x": pa.chunked_array([[], [1, 2], [], [3]], pa.int64())})
        )
        out = chdb.query(
            "SELECT count() AS c, sum(x) AS s FROM Python(ds_chunked)", "ArrowTable"
        )
        self.assertEqual(out.to_pylist(), [{"c": 3, "s": 6}])

    def test_dataset_is_rescannable_across_queries_and_in_self_join(self):
        ds_rescan = ds.dataset(self.parquet_dir, format="parquet")
        expected_count = len(self.parquet_rows)
        first = chdb.query("SELECT count() FROM Python(ds_rescan)", "CSV")
        second = chdb.query("SELECT count() FROM Python(ds_rescan)", "CSV")
        self.assertEqual(str(first), f"{expected_count}\n")
        self.assertEqual(str(second), str(first))
        joined = chdb.query(
            "SELECT count() FROM Python(ds_rescan) AS a JOIN Python(ds_rescan) AS b"
            " ON a.id = b.id",
            "CSV",
        )
        self.assertEqual(str(joined), f"{expected_count}\n")

    def test_dataset_under_parallel_scan(self):
        ds_par = ds.dataset(self.parquet_dir, format="parquet")
        expected_ids = [r["id"] for r in self.parquet_rows]
        out = chdb.query(
            "SELECT count() AS c, sum(id) AS s FROM Python(ds_par)"
            " SETTINGS max_threads = 8",
            "CSV",
        )
        self.assertEqual(str(out), f"{len(expected_ids)},{sum(expected_ids)}\n")

    def test_dataset_in_stateful_session(self):
        ds_sess = ds.dataset(self.parquet_dir, format="parquet")
        sess = session.Session()
        try:
            out = sess.query("SELECT count() FROM Python(ds_sess)", "CSV")
            self.assertEqual(str(out), f"{len(self.parquet_rows)}\n")
        finally:
            sess.close()

    def test_dataset_joined_with_pyarrow_table(self):
        ds_left = ds.dataset(self.parquet_dir, format="parquet")
        tbl_lookup = pa.table(
            {"grp": pa.array(["g0", "g2"], pa.string()), "label": ["zero", "two"]}
        )
        expected = sorted(
            (
                {"id": r["id"], "label": {"g0": "zero", "g2": "two"}[r["grp"]]}
                for r in self.parquet_rows
                if r["grp"] in ("g0", "g2")
            ),
            key=lambda r: r["id"],
        )
        out = chdb.query(
            "SELECT l.id AS id, r.label AS label FROM Python(ds_left) AS l"
            " JOIN Python(tbl_lookup) AS r ON l.grp = r.grp ORDER BY id",
            "ArrowTable",
        )
        self.assertEqual(out.to_pylist(), expected)

    def test_dataset_is_scanned_directly_with_pushed_down_projection(self):
        # Falsifiable structural check for the Dataset branch:
        #  - Dataset.scanner() is called with exactly the projected columns, so
        #    the projection really is pushed into the scanner;
        #  - pyarrow.dataset.dataset() is NOT re-invoked on the Dataset (it only
        #    accepts a *list* of datasets and would raise TypeError).
        class SpyDataset(ds.InMemoryDataset):
            scanner_kwargs = []

            def scanner(self, **kwargs):
                SpyDataset.scanner_kwargs.append(kwargs)
                return super().scanner(**kwargs)

        ds_spy = SpyDataset([pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})])
        wrap_calls = []
        original = ds.dataset

        def spy(source, *args, **kwargs):
            wrap_calls.append(type(source).__name__)
            return original(source, *args, **kwargs)

        ds.dataset = spy
        try:
            out = chdb.query("SELECT b FROM Python(ds_spy) ORDER BY b", "ArrowTable")
        finally:
            ds.dataset = original
        self.assertEqual(out.to_pylist(), [{"b": "x"}, {"b": "y"}, {"b": "z"}])
        self.assertEqual(wrap_calls, [])
        self.assertTrue(SpyDataset.scanner_kwargs, "Dataset.scanner() was never called")
        self.assertEqual(SpyDataset.scanner_kwargs, [{"columns": ["b"]}] * len(SpyDataset.scanner_kwargs))


class TestExistingPythonTableInputsUnaffected(unittest.TestCase):
    """The new type checks must not divert the already-supported inputs."""

    def test_pyarrow_table_still_scanned_with_projection(self):
        tbl_reg = pa.table({"x": [1, 2, 3], "s": ["a", "b", "c"]})
        out = chdb.query(
            "SELECT s FROM Python(tbl_reg) WHERE x > 1 ORDER BY s", "ArrowTable"
        )
        self.assertEqual(out.column_names, ["s"])
        self.assertEqual(out.to_pylist(), [{"s": "b"}, {"s": "c"}])

    @unittest.skipIf(pd is None, "pandas not installed")
    def test_pandas_dataframe_still_takes_pandas_path(self):
        # A pandas subclass whose __arrow_c_stream__ raises proves the pandas
        # scan is still chosen ahead of any Arrow protocol path.
        class GuardedDF(pd.DataFrame):
            def __arrow_c_stream__(self, requested_schema=None):
                raise AssertionError("pandas must not take an Arrow stream path")

        df_reg = GuardedDF({"x": [1, 2, 3], "s": ["a", "b", "c"]})
        out = chdb.query("SELECT sum(x), max(s) FROM Python(df_reg) WHERE x > 1", "CSV")
        self.assertEqual(str(out), '5,"c"\n')

    def test_record_batch_reader_still_single_use_generic_stream(self):
        schema = pa.schema([("x", pa.int64())])
        reader_reg = pa.RecordBatchReader.from_batches(
            schema, [pa.record_batch([pa.array([1, 2, 3], pa.int64())], schema=schema)]
        )
        self.assertEqual(
            str(chdb.query("SELECT count() FROM Python(reader_reg)", "CSV")), "3\n"
        )
        # Still exhausted after the first scan: the reader must not be mistaken
        # for a re-scannable RecordBatch.
        self.assertEqual(
            str(chdb.query("SELECT count() FROM Python(reader_reg)", "CSV")), "0\n"
        )

    def test_python_dict_input_still_works(self):
        dict_reg = {"x": [1, 2, 3], "s": ["a", "b", "c"]}
        out = chdb.query(
            "SELECT s FROM Python(dict_reg) WHERE x > 1 ORDER BY s", "ArrowTable"
        )
        self.assertEqual(out.to_pylist(), [{"s": "b"}, {"s": "c"}])

    def test_pyreader_subclass_input_still_works(self):
        class MyReader(chdb.PyReader):
            def __init__(self, data):
                self.data = data
                self.cursor = 0
                super().__init__(data)

            def read(self, col_names, count):
                if self.cursor >= len(self.data["x"]):
                    return []
                block = [self.data[col] for col in col_names]
                self.cursor += len(block[0])
                return block

        reader_py = MyReader({"x": [1, 2, 3], "s": ["a", "b", "c"]})
        out = chdb.query("SELECT sum(x) AS s FROM Python(reader_py)", "CSV")
        self.assertEqual(str(out), "6\n")

    def test_unsupported_object_still_reports_object_not_found(self):
        not_queryable = object()
        with self.assertRaises(Exception) as ctx:
            chdb.query("SELECT * FROM Python(not_queryable)", "CSV")
        self.assertIn("Python object not found", str(ctx.exception))
        # Engine stays usable after the failure.
        self.assertEqual(str(chdb.query("SELECT 1", "CSV")), "1\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
