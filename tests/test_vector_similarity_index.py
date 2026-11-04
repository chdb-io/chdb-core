#!python3
"""Systematic tests for the HNSW vector-similarity (ANN) index (USearch/SimSIMD).

The `vector_similarity` index (ClickHouse's only ANN index) was not compiled
into chdb until ENABLE_USEARCH/ENABLE_SIMSIMD were turned on. These tests go
beyond "is it registered" and exercise it the way ClickHouse's own vector-search
tests do:

  * the index is registered and visible in system.data_skipping_indices,
  * EXPLAIN shows the skip index actually drives the query,
  * ANN results have high recall against an *exact brute-force scan of the same
    engine* (use_skip_indexes=0) -- the brute force is the ground truth, so we
    measure the index's real recall, not a Python re-implementation,
  * all supported distance functions (L2Distance / cosineDistance / dotProduct)
    and quantizations build and answer queries,
  * Array(Float32) and Array(Float64) columns work,
  * the index keeps working after a merge (OPTIMIZE FINAL),
  * malformed usage (dimension mismatch, unsupported method, non-array column)
    is rejected cleanly rather than crashing.

Recall note: HNSW recall is excellent only when each index granule spans a whole
data part (the ClickHouse-recommended layout). We use the default
index_granularity so small/medium parts hold a single granule; this mirrors
real usage and yields ~1.0 recall.

Skipped under CHDB_LITE=1 (USearch is intentionally absent in lite).
"""

import os
import random
import shutil
import tempfile
import unittest

from chdb import session as chs


def _vec_literal(values):
    return "[" + ",".join(repr(float(x)) for x in values) + "]"


class TestVectorSimilarityIndex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("CHDB_LITE") == "1":
            raise unittest.SkipTest("USearch/vector_similarity is intentionally absent in chdb-core-lite")

    def setUp(self):
        self._path = tempfile.mkdtemp(prefix="chdb_vec_")
        self.s = chs.Session(self._path)
        self.s.query("SET allow_experimental_vector_similarity_index = 1")

    def tearDown(self):
        try:
            self.s.close()
        finally:
            shutil.rmtree(self._path, ignore_errors=True)

    # ---- helpers -------------------------------------------------------

    def _scalar(self, sql):
        return str(self.s.query(sql, "CSV")).strip().strip('"')

    def _ids(self, sql):
        out = str(self.s.query(sql, "CSV")).strip()
        return [int(x) for x in out.split("\n") if x != ""]

    def _make_table(self, name, dim, distance="L2Distance", col_type="Float32",
                    index_args=None, granularity=1, index_granularity=None):
        args = index_args if index_args is not None else f"'hnsw', '{distance}', {dim}"
        settings = f" SETTINGS index_granularity={index_granularity}" if index_granularity else ""
        self.s.query(
            f"CREATE TABLE {name} (id UInt32, v Array({col_type}), "
            f"INDEX idx v TYPE vector_similarity({args}) GRANULARITY {granularity}) "
            f"ENGINE = MergeTree ORDER BY id{settings}"
        )

    def _insert(self, name, data, chunk=250):
        for k in range(0, len(data), chunk):
            rows = ",".join(
                f"({i}, {_vec_literal(data[i])})" for i in range(k, min(k + chunk, len(data)))
            )
            self.s.query(f"INSERT INTO {name} VALUES {rows}")

    def _random_data(self, n, dim, seed, nonzero=False):
        rnd = random.Random(seed)
        data = []
        for _ in range(n):
            v = [round(rnd.uniform(-10, 10), 3) for _ in range(dim)]
            if nonzero and not any(v):
                v[0] = 1.0
            data.append(v)
        return data

    def _topk(self, name, ref, k, distance, use_index):
        order = "ASC"
        return self._ids(
            f"SELECT id FROM {name} "
            f"ORDER BY {distance}(v, {_vec_literal(ref)}) {order} LIMIT {k} "
            f"SETTINGS use_skip_indexes={1 if use_index else 0}"
        )

    def _recall(self, name, dim, distance, seed, k=10, queries=20):
        rnd = random.Random(seed * 31 + 5)
        total = 0.0
        for _ in range(queries):
            ref = [round(rnd.uniform(-10, 10), 3) for _ in range(dim)]
            truth = set(self._topk(name, ref, k, distance, use_index=False))
            got = set(self._topk(name, ref, k, distance, use_index=True))
            total += len(truth & got) / k
        return total / queries

    # ---- registration / plan ------------------------------------------

    def test_index_registered_in_system_table(self):
        self._make_table("reg", 4)
        self.assertEqual(
            "vector_similarity",
            self._scalar(
                "SELECT type FROM system.data_skipping_indices "
                "WHERE table = 'reg' AND name = 'idx'"
            ),
        )

    def test_explain_shows_skip_index_drives_query(self):
        # Small index_granularity -> many index granules, so the skip index has
        # something to prune and appears in the plan.
        self._make_table("expl", 8, index_granularity=32)
        self._insert("expl", self._random_data(300, 8, seed=1))
        plan = str(
            self.s.query(
                "EXPLAIN indexes = 1 SELECT id FROM expl "
                f"ORDER BY L2Distance(v, {_vec_literal([1.0] * 8)}) LIMIT 5",
                "TabSeparatedRaw",
            )
        )
        self.assertIn("Skip", plan)
        self.assertIn("vector_similarity", plan)

    # ---- recall (index vs exact brute force, same engine) -------------

    def test_l2_recall_against_bruteforce(self):
        self._make_table("vl2", 16, "L2Distance")
        self._insert("vl2", self._random_data(1000, 16, seed=7))
        recall = self._recall("vl2", 16, "L2Distance", seed=7)
        self.assertGreaterEqual(recall, 0.9, f"L2 recall@10 too low: {recall:.2f}")

    def test_cosine_recall_against_bruteforce(self):
        self._make_table("vcos", 16, "cosineDistance")
        self._insert("vcos", self._random_data(1000, 16, seed=11, nonzero=True))
        recall = self._recall("vcos", 16, "cosineDistance", seed=11)
        self.assertGreaterEqual(recall, 0.9, f"cosine recall@10 too low: {recall:.2f}")

    def test_small_dataset_returns_exact_nearest_neighbours(self):
        # One granule -> the index must return the exact nearest neighbours.
        self._make_table("vexact", 3, "L2Distance")
        self.s.query(
            "INSERT INTO vexact VALUES "
            "(1, [0.0,0.0,0.0]), (2, [1.0,0.0,0.0]), (3, [5.0,5.0,5.0]), "
            "(4, [10.0,10.0,10.0]), (5, [0.5,0.5,0.0])"
        )
        got = self._topk("vexact", [0.1, 0.1, 0.0], 3, "L2Distance", use_index=True)
        truth = self._topk("vexact", [0.1, 0.1, 0.0], 3, "L2Distance", use_index=False)
        self.assertEqual([1, 5, 2], truth)   # sanity: brute force is what we expect
        self.assertEqual(truth, got)         # index agrees exactly

    # ---- distance functions / quantizations / types -------------------

    def test_all_distance_functions_build_and_answer(self):
        for dist in ("L2Distance", "cosineDistance", "dotProduct"):
            with self.subTest(distance=dist):
                name = "d_" + dist.lower()
                self._make_table(name, 8, dist)
                self._insert(name, self._random_data(300, 8, seed=3, nonzero=True))
                rows = self._topk(name, [1.0] * 8, 5, dist, use_index=True)
                self.assertEqual(5, len(rows))

    def test_quantizations_build_and_answer(self):
        # 6-arg form: method, distance, dims, quantization, M, ef_construction.
        for quant in ("f32", "f16", "bf16", "i8"):
            with self.subTest(quantization=quant):
                name = "q_" + quant
                self._make_table(
                    name, 8, index_args=f"'hnsw', 'L2Distance', 8, '{quant}', 16, 64"
                )
                self._insert(name, self._random_data(300, 8, seed=5, nonzero=True))
                # i8 quantization rejects zero-magnitude query vectors.
                rows = self._topk(name, [1.0] * 8, 5, "L2Distance", use_index=True)
                self.assertEqual(5, len(rows))

    def test_float64_column_supported(self):
        self._make_table("vf64", 6, "L2Distance", col_type="Float64")
        self._insert("vf64", self._random_data(400, 6, seed=9))
        recall = self._recall("vf64", 6, "L2Distance", seed=9, k=5, queries=10)
        self.assertGreaterEqual(recall, 0.9, f"Float64 recall@5 too low: {recall:.2f}")

    # ---- robustness after merge ---------------------------------------

    def test_recall_preserved_after_merge(self):
        # The merge rebuilds the index for all 900 rows at once, on several threads, so the
        # HNSW graph - and the recall it gives - depends on thread scheduling. With the default
        # build parameters recall drops on machines with few cores (0.96-0.99 on 2 cores, 0.89
        # once on a macOS x86_64 runner). A denser graph makes it insensitive to that.
        self._make_table("vmerge", 12, "L2Distance",
                         index_args="'hnsw', 'L2Distance', 12, 'f32', 64, 512")
        # several inserts -> several parts, then merge into one.
        self._insert("vmerge", self._random_data(900, 12, seed=13), chunk=150)
        self.assertGreater(int(self._scalar(
            "SELECT count() FROM system.parts WHERE table='vmerge' AND active")), 1)
        self.s.query("OPTIMIZE TABLE vmerge FINAL")
        self.assertEqual("1", self._scalar(
            "SELECT count() FROM system.parts WHERE table='vmerge' AND active"))
        recall = self._recall("vmerge", 12, "L2Distance", seed=13)
        self.assertGreaterEqual(recall, 0.9, f"post-merge recall@10 too low: {recall:.2f}")

    # ---- error handling -----------------------------------------------

    def test_dimension_mismatch_on_insert_is_rejected(self):
        self._make_table("vdim", 4, "L2Distance")
        with self.assertRaises(Exception) as ctx:
            self.s.query("INSERT INTO vdim VALUES (1, [1.0, 2.0, 3.0])")  # 3 != 4
        self.assertNotIn("Unknown Index type", str(ctx.exception))

    def test_unsupported_method_is_rejected_cleanly(self):
        with self.assertRaises(Exception) as ctx:
            self.s.query(
                "CREATE TABLE vbad (id UInt32, v Array(Float32), "
                "INDEX idx v TYPE vector_similarity('ivfflat', 'L2Distance', 4)) "
                "ENGINE = MergeTree ORDER BY id"
            )
        msg = str(ctx.exception)
        self.assertNotIn("Unknown Index type", msg)  # proves the type IS compiled in
        self.assertIn("method", msg.lower())

    def test_non_array_column_is_rejected(self):
        with self.assertRaises(Exception) as ctx:
            self.s.query(
                "CREATE TABLE vscalar (id UInt32, v Float32, "
                "INDEX idx v TYPE vector_similarity('hnsw', 'L2Distance', 1)) "
                "ENGINE = MergeTree ORDER BY id"
            )
        self.assertNotIn("Unknown Index type", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
