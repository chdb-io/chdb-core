#!python3
"""BM25 scoring of the text index (the port of ClickHouse#117519).

A table with a scoring text index and an HNSW index, as the Context Plane builds it. The
scores of `bm25()` must equal BM25 computed here from the same tokens, with and without a
scalar filter bound as a query parameter, across several parts and after a merge.
"""

import json
import math
import random
import shutil
import tempfile
import unittest

import chdb

K1, B = 1.2, 0.75
DIM = 8
WORDS = [
    "orders", "customer_id", "dim_customer_orders", "amount", "ledger", "revenue",
    "account", "region", "invoice", "shipment", "raft", "consensus",
]


def reference(docs, terms, allowed=None):
    """BM25 with the Lucene-smoothed IDF, statistics over every document."""
    n = len(docs)
    avgdl = sum(len(d["tokens"]) for d in docs) / n
    scores = {}
    for term in terms:
        df = sum(1 for d in docs if term in d["tokens"])
        if not df:
            continue
        idf = math.log(1 + (n - df + 0.5) / (df + 0.5))
        for d in docs:
            tf = d["tokens"].count(term)
            if tf and (allowed is None or set(d["services"]) <= set(allowed)):
                dl = len(d["tokens"])
                part = idf * tf * (K1 + 1) / (tf + K1 * (1 - B + B * dl / avgdl))
                scores[d["id"]] = scores.get(d["id"], 0.0) + part
    return scores


class TestBM25Scoring(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = tempfile.mkdtemp(prefix="chdb-bm25-")
        cls.conn = chdb.connect(cls.path)
        rng = random.Random(7)
        cls.docs = []
        for i in range(240):
            tokens = [rng.choice(WORDS) for _ in range(rng.randint(3, 40))]
            services = ["s1"] if i % 3 else ["s1", "s2"]
            cls.docs.append({"id": i, "tokens": tokens, "services": services})
        cls.conn.query("CREATE DATABASE bm25 ENGINE = Atomic")
        cls.conn.query(
            """
            CREATE TABLE bm25.docs
            (
                id UInt32,
                required_service_ids Array(String),
                text String,
                embedding Array(Float32) CODEC(NONE),
                INDEX vector_idx embedding
                    TYPE vector_similarity('hnsw', 'cosineDistance', %d, 'f16', 16, 128),
                INDEX text_idx text TYPE text(
                    tokenizer = splitByString([' ']),
                    posting_list_codec = 'bitpacking',
                    scoring = 'bm25')
            )
            ENGINE = MergeTree ORDER BY id
            SETTINGS allow_experimental_text_index_scoring = 1
            """
            % DIM
        )
        # Four inserts: four parts.
        for start in range(0, len(cls.docs), 60):
            rows = [
                json.dumps(
                    {
                        "id": d["id"],
                        "required_service_ids": d["services"],
                        "text": " ".join(d["tokens"]),
                        "embedding": [rng.random() for _ in range(DIM)],
                    }
                )
                for d in cls.docs[start : start + 60]
            ]
            cls.conn.query("INSERT INTO bm25.docs FORMAT JSONEachRow\n" + "\n".join(rows))

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        shutil.rmtree(cls.path, ignore_errors=True)

    def rows(self, sql, params=None):
        raw = self.conn.query(sql, "JSONEachRow", params=params or {}).bytes()
        return [json.loads(line) for line in bytes(raw).splitlines() if line.strip()]

    def scores(self, terms, allowed=None):
        where = "hasAnyTokens(text, {terms:Array(String)})"
        params = {"terms": "[" + ",".join("'%s'" % t for t in terms) + "]"}
        if allowed is not None:
            where += " AND hasAll({allowed:Array(String)}, required_service_ids)"
            params["allowed"] = "[" + ",".join("'%s'" % s for s in allowed) + "]"
        found = self.rows(
            "SELECT id, bm25() AS score FROM bm25.docs WHERE " + where
            + " SETTINGS allow_experimental_bm25_scoring = 1",
            params,
        )
        return {int(r["id"]): float(r["score"]) for r in found}

    def check(self, terms, allowed=None):
        got, want = self.scores(terms, allowed), reference(self.docs, terms, allowed)
        self.assertEqual(set(got), set(want))
        for doc_id, score in want.items():
            self.assertAlmostEqual(got[doc_id], score, delta=1e-4)
        ranked = lambda s: sorted(s, key=lambda i: (-round(s[i], 4), i))[:10]  # noqa: E731
        self.assertEqual(ranked(got), ranked(want))

    def test_1_scores_equal_the_reference_across_parts(self):
        parts = self.rows("SELECT count() AS n FROM system.parts WHERE database = 'bm25' AND active")
        self.assertGreater(int(parts[0]["n"]), 1)
        self.check(["orders"])
        self.check(["customer_id", "raft", "revenue"])
        self.check(["dim_customer_orders", "no_such_token"])

    def test_2_a_bound_scalar_filter_removes_rows_and_keeps_scores(self):
        self.check(["orders", "ledger"], allowed=["s1"])
        self.check(["orders", "ledger"], allowed=["s1", "s2"])
        self.assertEqual(self.scores(["orders"], allowed=[]), {})

    def test_3_absent_tokens_match_nothing(self):
        self.assertEqual(self.scores(["no_such_token"]), {})

    def test_4_both_indexes_are_read(self):
        lexical = bytes(
            self.conn.query(
                "EXPLAIN indexes = 1 SELECT id, bm25() AS s FROM bm25.docs"
                " WHERE hasAnyTokens(text, ['orders']) ORDER BY s DESC LIMIT 5"
                " SETTINGS allow_experimental_bm25_scoring = 1",
                "TSVRaw",
            ).bytes()
        ).decode()
        self.assertIn("text_idx", lexical)
        self.assertIn("BM25 scoring", lexical)
        vector = "[" + ",".join(["0.5"] * DIM) + "]"
        dense = bytes(
            self.conn.query(
                "EXPLAIN indexes = 1 SELECT id FROM bm25.docs"
                " ORDER BY cosineDistance(embedding, %s) LIMIT 5" % vector,
                "TSVRaw",
            ).bytes()
        ).decode()
        self.assertIn("vector_idx", dense)

    def test_5_scores_equal_the_reference_after_a_merge(self):
        self.conn.query("OPTIMIZE TABLE bm25.docs FINAL")
        parts = self.rows("SELECT count() AS n FROM system.parts WHERE database = 'bm25' AND active")
        self.assertEqual(int(parts[0]["n"]), 1)
        self.check(["customer_id", "raft", "revenue"])
        self.check(["orders", "ledger"], allowed=["s1"])


if __name__ == "__main__":
    unittest.main()
