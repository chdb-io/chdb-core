#!python3
"""Cluster-organized vector search, and reading only the rows a rerank needs.

The Context Plane builds a search generation once and seals it. For IVF-style search it
needs no index object: the centroids are an ordinary table of the generation, every row
stores the id of its nearest centroid, and the table is sorted by that id. A query picks
the nearest centroids and filters by their ids, so the primary key reads only those
clusters. `hierarchicalKMeans` and `assignCentroid` (ClickHouse#112309) do the training
and the assignment.

The vectors, the nearest centroid of every row and the rows a probe may return are computed
here as well, so the engine is compared with a reference, with and without a scalar filter
bound as a query parameter, across several parts and after a merge.

The second class covers `uncompressed_fixed_array_size` and
`merge_tree_use_one_row_per_block_layout` (the port of ClickHouse#131027).
"""

import json
import random
import shutil
import tempfile
import unittest

import chdb

DIM = 8
K = 16  # trained centroids
NATURAL = 20  # natural clusters in the data
ROWS = 3000
GRANULARITY = 16


def float32(x):
    """The value as stored in a Float32 column."""
    import struct

    return struct.unpack("f", struct.pack("f", x))[0]


def l2(a, b):
    return sum((x - y) * (x - y) for x, y in zip(a, b))


def literal(values):
    return "[" + ",".join(repr(v) for v in values) + "]"


def strings(values):
    return "[" + ",".join("'%s'" % v for v in values) + "]"


class Queries:
    """Helpers of both test classes; `conn` is the connection of the class."""

    @classmethod
    def select(cls, sql, params=None):
        raw = cls.conn.query(sql, "JSONEachRow", params=params or {}).bytes()
        return [json.loads(line) for line in bytes(raw).splitlines() if line.strip()]

    @classmethod
    def insert(cls, table, rows):
        cls.conn.query("INSERT INTO %s FORMAT JSONEachRow\n" % table + "\n".join(json.dumps(r) for r in rows))

    def event(self, name):
        found = self.select("SELECT value FROM system.events WHERE event = '%s'" % name)
        return int(found[0]["value"]) if found else 0

    def assertFails(self, sql, error):
        with self.assertRaises(Exception) as raised:
            self.conn.query(sql)
        self.assertIn(error, str(raised.exception))


class TestClusterLayout(Queries, unittest.TestCase):
    PROBE = (
        "SELECT id FROM layout.vectors"
        " WHERE cid IN (SELECT cid FROM layout.centroids"
        "               ORDER BY L2Distance(vec, {q:Array(Float32)}) LIMIT {nprobe:UInt32})"
        " %s"
        " ORDER BY L2Distance(vec, {q:Array(Float32)}), id LIMIT {k:UInt32}"
    )

    @classmethod
    def setUpClass(cls):
        cls.path = tempfile.mkdtemp(prefix="chdb-layout-")
        cls.conn = chdb.connect(cls.path)
        rng = random.Random(11)
        centers = [[rng.uniform(-10, 10) for _ in range(DIM)] for _ in range(NATURAL)]
        cls.rows = []
        for i in range(ROWS):
            center = centers[rng.randrange(NATURAL)]
            cls.rows.append(
                {
                    "id": i,
                    "required_service_ids": ["s1"] if i % 3 else ["s1", "s2"],
                    "vec": [float32(x + rng.gauss(0, 0.6)) for x in center],
                }
            )
        cls.queries = [[float32(x + rng.gauss(0, 0.6)) for x in centers[rng.randrange(NATURAL)]] for _ in range(12)]
        cls.conn.query("CREATE DATABASE layout ENGINE = Atomic")
        cls.conn.query(
            "CREATE TABLE layout.raw (id UInt32, required_service_ids Array(String), vec Array(Float32))"
            " ENGINE = MergeTree ORDER BY id"
        )
        cls.insert("layout.raw", cls.rows)
        # The centroids are data of the generation: an ordinary table, trained once.
        cls.conn.query(
            "CREATE TABLE layout.centroids (cid UInt32, vec Array(Float32) CODEC(NONE))"
            " ENGINE = MergeTree ORDER BY cid"
        )
        cls.conn.query(
            "INSERT INTO layout.centroids SELECT toUInt32(n - 1), v"
            " FROM (SELECT hierarchicalKMeans(%d)(vec) AS found FROM layout.raw)"
            " ARRAY JOIN found AS v, arrayEnumerate(found) AS n" % K
        )
        cls.centroids = {
            int(r["cid"]): [float32(x) for x in r["vec"]]
            for r in cls.select("SELECT cid, vec FROM layout.centroids ORDER BY cid")
        }
        # The vector column has the one-row-per-block layout, so `cid` is a plain column
        # filled by the insert: a DEFAULT that names `vec` would switch that layout off.
        cls.conn.query(
            """
            CREATE TABLE layout.vectors
            (
                id UInt32,
                cid UInt32,
                required_service_ids Array(String),
                vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = %d)
            )
            ENGINE = MergeTree ORDER BY (cid, id)
            SETTINGS index_granularity = %d, min_bytes_for_wide_part = 0
            """
            % (DIM, GRANULARITY)
        )
        # Four inserts: four parts.
        for part in range(4):
            cls.conn.query(
                "INSERT INTO layout.vectors"
                " SELECT id, assignCentroid(vec, (SELECT groupArray(vec) FROM"
                "     (SELECT vec FROM layout.centroids ORDER BY cid))), required_service_ids, vec"
                " FROM layout.raw WHERE id %% 4 = %d" % part
            )

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        shutil.rmtree(cls.path, ignore_errors=True)

    def nearest_centroids(self, vector, count):
        ranked = sorted(self.centroids, key=lambda cid: (l2(self.centroids[cid], vector), cid))
        return ranked[:count]

    def expected(self, query, nprobe, allowed=None, limit=10):
        """The nearest rows among the clusters of the `nprobe` nearest centroids."""
        probed = set(self.nearest_centroids(query, nprobe))
        rows = [
            r
            for r in self.rows
            if self.nearest_centroids(r["vec"], 1)[0] in probed
            and (allowed is None or set(r["required_service_ids"]) <= set(allowed))
        ]
        rows.sort(key=lambda r: (l2(r["vec"], query), r["id"]))
        return [r["id"] for r in rows[:limit]]

    def search(self, query, nprobe, allowed=None, limit=10, settings=""):
        where = "" if allowed is None else "AND hasAll({allowed:Array(String)}, required_service_ids)"
        params = {"q": literal(query), "nprobe": str(nprobe), "k": str(limit)}
        if allowed is not None:
            params["allowed"] = strings(allowed)
        found = self.select(self.PROBE % where + settings, params)
        return [int(r["id"]) for r in found]

    def check_searches(self):
        recall = []
        for query in self.queries:
            exact = self.expected(query, K)
            for nprobe in (1, 4, K):
                self.assertEqual(self.search(query, nprobe), self.expected(query, nprobe))
                for allowed in (["s1"], ["s1", "s2"]):
                    self.assertEqual(
                        self.search(query, nprobe, allowed), self.expected(query, nprobe, allowed)
                    )
            self.assertEqual(self.search(query, 4, allowed=[]), [])
            recall.append(len(set(self.search(query, 4)) & set(exact)) / float(len(exact)))
        self.assertGreaterEqual(sum(recall) / len(recall), 0.9)

    def test_1_every_row_stores_its_nearest_centroid(self):
        self.assertEqual(len(self.centroids), K)
        stored = {int(r["id"]): int(r["cid"]) for r in self.select("SELECT id, cid FROM layout.vectors")}
        self.assertEqual(len(stored), ROWS)
        for row in self.rows:
            self.assertEqual(stored[row["id"]], self.nearest_centroids(row["vec"], 1)[0])

    def test_2_a_probe_returns_the_nearest_rows_of_its_clusters_across_parts(self):
        parts = self.select("SELECT count() AS n FROM system.parts WHERE database = 'layout' AND table = 'vectors' AND active")
        self.assertGreater(int(parts[0]["n"]), 1)
        self.check_searches()

    def test_3_the_primary_key_reads_only_the_probed_clusters(self):
        self.conn.query("OPTIMIZE TABLE layout.vectors FINAL")
        runs = self.select(
            "SELECT uniqExact(cid) AS clusters, countIf(cid != prev) + 1 AS runs FROM"
            " (SELECT cid, lagInFrame(cid, 1, cid) OVER (ORDER BY _part, _part_offset) AS prev"
            "  FROM layout.vectors)"
        )[0]
        self.assertEqual(int(runs["runs"]), int(runs["clusters"]))  # every cluster is contiguous
        plan = bytes(
            self.conn.query(
                "EXPLAIN indexes = 1 " + self.PROBE % "",
                "TSVRaw",
                params={"q": literal(self.queries[0]), "nprobe": "2", "k": "10"},
            ).bytes()
        ).decode()
        self.assertIn("cid in 2-element set", plan)
        granules = [line.split("Granules:")[1].strip() for line in plan.splitlines() if "Granules:" in line]
        read, total = [int(x) for x in granules[-1].split("/")]
        self.assertLess(read * 3, total)
        self.check_searches()

    def test_4_a_probe_reads_only_its_result_rows_of_the_vector_column(self):
        """The vectors are sorted by distance on quantized-free data here, so the rows are chosen by another
        column to make the vector a lazily read column, as a rerank reads it."""
        before = self.event("MergeTreeOneRowPerBlockReadRows")
        found = self.select(
            "SELECT id, vec FROM layout.vectors WHERE cid IN (0, 1, 2) ORDER BY sipHash64(id) LIMIT 5"
            " SETTINGS merge_tree_use_one_row_per_block_layout = 1,"
            " query_plan_optimize_lazy_materialization = 1, use_uncompressed_cache = 0"
        )
        self.assertEqual(self.event("MergeTreeOneRowPerBlockReadRows") - before, 5)
        by_id = {r["id"]: r["vec"] for r in self.rows}
        for row in found:
            self.assertEqual([float32(x) for x in row["vec"]], by_id[int(row["id"])])


class TestOneRowPerBlock(Queries, unittest.TestCase):
    """`uncompressed_fixed_array_size` and `merge_tree_use_one_row_per_block_layout` (ClickHouse#131027)."""

    @classmethod
    def setUpClass(cls):
        cls.path = tempfile.mkdtemp(prefix="chdb-onerow-")
        cls.conn = chdb.connect(cls.path)
        rng = random.Random(5)
        cls.rows = [{"id": i, "vec": [float32(rng.uniform(-1, 1)) for _ in range(DIM)]} for i in range(600)]
        cls.conn.query("CREATE DATABASE onerow ENGINE = Atomic")
        cls.conn.query(
            """
            CREATE TABLE onerow.docs
            (
                id UInt32,
                vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = %d)
            )
            ENGINE = MergeTree ORDER BY id
            SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64
            """
            % DIM
        )
        for start in (0, 300):
            cls.insert("onerow.docs", cls.rows[start : start + 300])

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        shutil.rmtree(cls.path, ignore_errors=True)

    def lazy_read(self, enabled, table="onerow.docs"):
        """Five rows chosen by another column, with their vectors, which are read after the rows are chosen."""
        before = self.event("MergeTreeOneRowPerBlockReadRows")
        found = self.select(
            "SELECT id, vec FROM " + table + " ORDER BY sipHash64(id) LIMIT 5 "
            "SETTINGS merge_tree_use_one_row_per_block_layout = " + str(enabled)
            + ", query_plan_optimize_lazy_materialization = 1, use_uncompressed_cache = 0"
        )
        return found, self.event("MergeTreeOneRowPerBlockReadRows") - before

    def reattach(self, table, column, settings=""):
        """Detaches the table and attaches its data with a definition in another database, as a reader that
        opens the table of another process does. Returns the name of the attached table."""
        uuid = self.select("SELECT uuid FROM system.tables WHERE database = 'onerow' AND name = '%s'" % table)[0]["uuid"]
        self.conn.query("CREATE DATABASE IF NOT EXISTS onerow_reader ENGINE = Atomic")
        self.conn.query("DETACH TABLE onerow.%s" % table)
        attach = "ATTACH TABLE onerow_reader.%s UUID '%s' (id UInt32, %s) ENGINE = MergeTree ORDER BY id SETTINGS %s"
        usual = "min_bytes_for_wide_part = 0, index_granularity = 64"
        try:
            self.conn.query(attach % (table, uuid, column, usual + settings))
        except Exception:
            self.conn.query("ATTACH TABLE onerow.%s" % table)
            raise
        return "onerow_reader." + table

    def test_1_every_row_is_one_block_and_only_the_chosen_rows_are_read(self):
        sizes = self.select(
            "SELECT sum(rows) AS n, sum(data_compressed_bytes) AS stored FROM system.parts_columns "
            "WHERE database = 'onerow' AND table = 'docs' AND active AND column = 'vec'"
        )
        self.assertEqual(int(sizes[0]["n"]), 600)
        with_setting, rows_read = self.lazy_read(1)
        without_setting, rows_read_usual = self.lazy_read(0)
        self.assertEqual(with_setting, without_setting)
        by_id = {r["id"]: r["vec"] for r in self.rows}
        self.assertEqual(len(with_setting), 5)
        for row in with_setting:
            self.assertEqual([float32(x) for x in row["vec"]], by_id[int(row["id"])])
        self.assertEqual(rows_read, 5)
        self.assertEqual(rows_read_usual, 0)

    def test_2_rows_of_another_length_and_changes_of_the_layout_are_refused(self):
        self.assertFails("INSERT INTO onerow.docs VALUES (1000, [1, 2, 3])", "BAD_ARGUMENTS")
        self.assertFails(
            "ALTER TABLE onerow.docs MODIFY COLUMN vec MODIFY SETTING uncompressed_fixed_array_size = 4", "BAD_ARGUMENTS"
        )
        self.assertFails("ALTER TABLE onerow.docs MODIFY COLUMN vec Array(Float32) CODEC(LZ4)", "BAD_ARGUMENTS")
        self.assertFails(
            "CREATE TABLE onerow.refused (id UInt32, vec Array(Float32) CODEC(LZ4) "
            "SETTINGS (uncompressed_fixed_array_size = 8)) ENGINE = MergeTree ORDER BY id",
            "BAD_ARGUMENTS",
        )
        self.assertEqual(int(self.select("SELECT count() AS n FROM onerow.docs")[0]["n"]), 600)

    def test_3_a_merge_writes_one_block_per_row_as_well(self):
        self.conn.query("OPTIMIZE TABLE onerow.docs FINAL")
        parts = self.select("SELECT count() AS n FROM system.parts WHERE database = 'onerow' AND table = 'docs' AND active")
        self.assertEqual(int(parts[0]["n"]), 1)
        with_setting, rows_read = self.lazy_read(1)
        self.assertEqual(with_setting, self.lazy_read(0)[0])
        self.assertEqual(rows_read, 5)


    def test_4_a_reader_attaches_the_table_with_a_definition_only_read_only(self):
        column = "vec Array(Float32) SETTINGS (uncompressed_fixed_array_size = %d)" % DIM
        expected = self.lazy_read(0)[0]
        # A writable table could go on to write parts for a definition that its parts do not have.
        with self.assertRaises(Exception) as refused:
            self.reattach("docs", column)
        self.assertIn("NOT_IMPLEMENTED", str(refused.exception))
        self.assertEqual(self.lazy_read(1)[0], expected)
        attached = self.reattach("docs", column, ", table_readonly = 1")
        found, rows_read = self.lazy_read(1, attached)
        self.assertEqual(found, expected)
        self.assertEqual(rows_read, 5)
        self.assertFails("INSERT INTO %s VALUES (1000, %s)" % (attached, literal([0.0] * DIM)), "TABLE_IS_PERMANENTLY_READ_ONLY")

    def test_5_parts_written_without_the_setting_are_refused_not_misread(self):
        self.conn.query(
            "CREATE TABLE onerow.plain (id UInt32, vec Array(Float32) CODEC(NONE)) ENGINE = MergeTree ORDER BY id "
            "SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64"
        )
        self.insert("onerow.plain", self.rows)
        expected = self.lazy_read(0, "onerow.plain")[0]
        attached = self.reattach(
            "plain", "vec Array(Float32) CODEC(NONE) SETTINGS (uncompressed_fixed_array_size = %d)" % DIM, ", table_readonly = 1"
        )
        with self.assertRaises(Exception) as refused:
            self.lazy_read(1, attached)
        self.assertIn("CORRUPTED_DATA", str(refused.exception))
        self.assertEqual(self.lazy_read(0, attached)[0], expected)

if __name__ == "__main__":
    unittest.main()
