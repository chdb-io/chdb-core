#!/usr/bin/env python3
"""Correctness tests for the Python(df) read-path optimizations in:

  da7bd879e32  ORDER BY..LIMIT top-N late materialization + arrow string predicate pushdown
  e9abecfdf39  non-null schema inference + PREWHERE pushdown

All four are *execution* optimizations: they must not change results. Each test
runs a query through chdb's Python(df) table function and compares it against the
pandas ground truth, so a wrong-result regression in the optimized path fails:

  * top-N late materialization picking the wrong rows, or gathering the wrong
    row's data for some column (phase-2 gather-by-index is per-type),
  * arrow-direct predicate mis-evaluating LIKE '%x%' / <>'' / ='' / NULLs /
    case-sensitivity, or wrongly firing on a non-substring LIKE pattern,
  * non-null schema inference dropping or mishandling NULLs.

Every non-`id` column is a deterministic function of `id`, so for each returned
row we can recompute and assert every column -- this catches a gather that pulls
the right count of rows but the wrong row's bytes for a given column/type.

Arrow-backed string columns (pandas 3.x `str`, pandas 2.x `string[pyarrow]`)
exercise the new arrow-direct predicate + top-N arrow gather; `object` dtype
exercises the fallback. Runs on both pandas 2.x and 3.x.
"""

import io
import re
import unittest

import numpy as np
import pandas as pd

import chdb

PANDAS_MAJOR = int(pd.__version__.split(".")[0])
ARROW_STR = "string" if PANDAS_MAJOR >= 3 else "string[pyarrow]"

N = 257  # coprime-friendly size; >1 block boundary not needed for correctness


def make_url(i):
    """URL column: matches '%google%' for some, empty for some, other otherwise."""
    if i % 7 == 0:
        return ""
    if i % 3 == 0:
        return f"http://www.google.com/{i}"
    return f"http://other{i}.com/path"


def make_df(str_dtype):
    """Every column is a pure function of id, so any returned row is fully checkable.

    `k` is a bijection of id (gcd(101, N)==1), unique and non-monotonic, so it is a
    meaningful, tie-free ORDER BY key whose order differs from id order.
    """
    ids = np.arange(N, dtype=np.int64)
    return pd.DataFrame(
        {
            "id": ids,
            "k": ((ids * 101 + 7) % N).astype(np.int64),
            "grp": (ids % 4).astype(np.int32),
            "i16": (ids % 100 - 50).astype(np.int16),
            "u32": (ids * 3).astype(np.uint32),
            "f64": ids.astype(np.float64) / 8.0,
            "when": pd.to_datetime("2021-06-01") + pd.to_timedelta(ids, unit="s"),
            "url": pd.array([make_url(i) for i in ids], dtype=str_dtype),
            "s": pd.array([f"v{i}" for i in ids], dtype=str_dtype),
            "pad": (ids % 9).astype(np.int8),  # -> >= 8 output columns so top-N fires on SELECT *
        }
    )


def expected_row(i):
    """Re-derive the canonical (string) representation chdb should emit for row id=i."""
    return {
        "id": str(i),
        "k": str((i * 101 + 7) % N),
        "grp": str(i % 4),
        "i16": str(i % 100 - 50),
        "u32": str(i * 3),
        "when": (pd.to_datetime("2021-06-01") + pd.Timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S"),
        "url": make_url(i),
        "s": f"v{i}",
        "pad": str(i % 9),
    }


def chdb_rows(sql, columns):
    """Run `sql` and return list of dict rows (all values as str), order preserved."""
    res = str(chdb.query(sql, "CSVWithNames"))
    frame = pd.read_csv(io.StringIO(res), dtype=str, keep_default_na=False)
    assert list(frame.columns) == columns, f"columns {list(frame.columns)} != {columns}"
    return frame.to_dict("records")


class _Base(unittest.TestCase):
    def assert_ids_in_order(self, rows, expected_ids):
        got = [int(r["id"]) for r in rows]
        self.assertEqual(got, list(expected_ids), f"id order/contents mismatch\n got={got}\n exp={list(expected_ids)}")

    def assert_full_rows(self, rows, cols):
        """Every returned row's every column must equal the id-derived ground truth."""
        for r in rows:
            i = int(r["id"])
            exp = expected_row(i)
            for c in cols:
                got = r[c]
                if c == "when":
                    got = got.split(".")[0]  # DateTime64 prints whole seconds as ...SS.000000
                self.assertEqual(got, exp[c], f"row id={i} col {c}: chdb={r[c]!r} expected={exp[c]!r}")


class TestArrowStringPredicate(_Base):
    """Optimization 2: arrow-direct <>'' / ='' / LIKE '%substr%' (+ fallbacks)."""

    def _check(self, str_dtype):
        df = make_df(str_dtype)  # noqa: F841 -- referenced by Python(df)

        def ids_where(sql):
            return sorted(int(r["id"]) for r in chdb_rows(f"SELECT id FROM Python(df) WHERE {sql}", ["id"]))

        # notEmpty: excludes '' (no NULLs in url here)
        self.assertEqual(ids_where("url <> ''"), sorted(int(i) for i in range(N) if make_url(i) != ""))
        # empty
        self.assertEqual(ids_where("url = ''"), sorted(int(i) for i in range(N) if make_url(i) == ""))
        # LIKE '%substr%' (arrow-fast sz_find), case-sensitive
        self.assertEqual(ids_where("url LIKE '%google%'"), sorted(i for i in range(N) if "google" in make_url(i)))
        self.assertEqual(ids_where("url LIKE '%GOOGLE%'"), [])  # case-sensitive: no uppercase
        self.assertEqual(ids_where("url LIKE '%%'"), sorted(range(N)))  # empty needle: matches all
        # fallbacks (NOT the '%substr%' fast path -> must route to ClickHouse like, stay correct)
        # prefix pattern (no leading %): whole-string starts-with
        self.assertEqual(ids_where("url LIKE 'http://other2%'"),
                         sorted(i for i in range(N) if make_url(i).startswith("http://other2")))
        # '_' single-char wildcard, fully anchored (no %): s == 'v1' + exactly one char
        self.assertEqual(ids_where("s LIKE 'v1_'"),
                         sorted(i for i in range(N) if re.fullmatch("v1.", f"v{i}")))

    def test_arrow_string(self):
        self._check(ARROW_STR)

    def test_object_string(self):
        self._check(object)

    def test_nulls_excluded(self):
        # notEmpty(NULL)=NULL and empty(NULL)=NULL and like(NULL,..)=NULL -> none selected
        s = pd.array(["google", None, "", "xgoogley", None], dtype=ARROW_STR)
        df = pd.DataFrame({"id": np.arange(5, dtype=np.int64), "s": s})  # noqa: F841

        def ids_where(sql):
            return sorted(int(r["id"]) for r in chdb_rows(f"SELECT id FROM Python(df) WHERE {sql}", ["id"]))

        self.assertEqual(ids_where("s <> ''"), [0, 3])          # NULL and '' excluded
        self.assertEqual(ids_where("s = ''"), [2])              # NULL excluded
        self.assertEqual(ids_where("s LIKE '%google%'"), [0, 3])  # NULL excluded

    def test_non_ascii_substring(self):
        vals = ["日本語google日本", "google×", "ascii", "中文"]
        df = pd.DataFrame({"id": np.arange(len(vals), dtype=np.int64),  # noqa: F841
                           "s": pd.array(vals, dtype=ARROW_STR)})
        got = sorted(int(r["id"]) for r in
                     chdb_rows("SELECT id FROM Python(df) WHERE s LIKE '%日本語%'", ["id"]))
        self.assertEqual(got, [0])
        got = sorted(int(r["id"]) for r in
                     chdb_rows("SELECT id FROM Python(df) WHERE s LIKE '%google%'", ["id"]))
        self.assertEqual(got, [0, 1])


class TestTopNLateMaterialization(_Base):
    """Optimization 1: ORDER BY .. LIMIT top-N pushdown (wide output -> fires)."""

    ALL = ["id", "k", "grp", "i16", "u32", "f64", "when", "url", "s", "pad"]
    # f64 omitted from full-row checks (float CSV formatting); covered by id-order.
    CHECK = ["id", "k", "grp", "i16", "u32", "when", "url", "s", "pad"]

    def _pd_topn(self, df, by, ascending, limit, offset=0, where=None):
        sub = df if where is None else df[where(df)]
        srt = sub.sort_values(by, ascending=ascending, kind="stable")
        return list(srt["id"].iloc[offset:offset + limit])

    def _check(self, str_dtype):
        df = make_df(str_dtype)  # noqa: F841

        # SELECT * ORDER BY k ASC LIMIT (wide output -> top-N pushdown active)
        rows = chdb_rows("SELECT * FROM Python(df) ORDER BY k LIMIT 5", self.ALL)
        self.assert_ids_in_order(rows, self._pd_topn(df, "k", True, 5))
        self.assert_full_rows(rows, self.CHECK)  # gather-by-index pulled the right row's data

        # DESC
        rows = chdb_rows("SELECT * FROM Python(df) ORDER BY k DESC LIMIT 6", self.ALL)
        self.assert_ids_in_order(rows, self._pd_topn(df, "k", False, 6))
        self.assert_full_rows(rows, self.CHECK)

        # LIMIT + OFFSET
        rows = chdb_rows("SELECT * FROM Python(df) ORDER BY k LIMIT 5 OFFSET 4", self.ALL)
        self.assert_ids_in_order(rows, self._pd_topn(df, "k", True, 5, offset=4))
        self.assert_full_rows(rows, self.CHECK)

        # multi-key ORDER BY grp, k
        rows = chdb_rows("SELECT * FROM Python(df) ORDER BY grp, k LIMIT 9", self.ALL)
        self.assert_ids_in_order(rows, self._pd_topn(df, ["grp", "k"], True, 9))
        self.assert_full_rows(rows, self.CHECK)

        # WHERE (arrow predicate) + ORDER BY LIMIT (top-N) combined
        rows = chdb_rows("SELECT * FROM Python(df) WHERE url LIKE '%google%' ORDER BY k LIMIT 5", self.ALL)
        self.assert_ids_in_order(rows, self._pd_topn(df, "k", True, 5,
                                                     where=lambda d: d["url"].str.contains("google", regex=False)))
        self.assert_full_rows(rows, self.CHECK)

    def test_arrow_string(self):
        self._check(ARROW_STR)

    def test_object_string(self):
        self._check(object)

    def test_narrow_output_fallback(self):
        # < 8 output columns -> top-N pushdown gated off; must still be correct
        df = make_df(ARROW_STR)  # noqa: F841
        rows = chdb_rows("SELECT id, s FROM Python(df) WHERE url <> '' ORDER BY k LIMIT 5", ["id", "s"])
        exp = list(df[df["url"].astype("string").str.len() > 0].sort_values("k", kind="stable")["id"].iloc[:5])
        self.assert_ids_in_order(rows, exp)
        for r in rows:
            self.assertEqual(r["s"], f"v{int(r['id'])}")

    def test_with_ties_not_broken(self):
        # WITH TIES disables the pushdown; result must remain correct (>= LIMIT rows, all from
        # the smallest grp groups). grp has ties.
        df = make_df(ARROW_STR)  # noqa: F841
        rows = chdb_rows("SELECT id, grp FROM Python(df) ORDER BY grp LIMIT 3 WITH TIES", ["id", "grp"])
        self.assertGreaterEqual(len(rows), 3)
        # all returned rows share grp == 0 (the only group that can fill/tie the first 3)
        self.assertTrue(all(int(r["grp"]) == 0 for r in rows))
        self.assertEqual(sorted(int(r["id"]) for r in rows), sorted(i for i in range(N) if i % 4 == 0))

    def test_select_star_no_limit_unaffected(self):
        # sanity: plain SELECT * (no ORDER BY/LIMIT) returns every row intact
        df = make_df(ARROW_STR)  # noqa: F841
        rows = chdb_rows("SELECT * FROM Python(df) ORDER BY id", TestTopNLateMaterialization.ALL)
        self.assertEqual(len(rows), N)
        self.assert_full_rows(rows, TestTopNLateMaterialization.CHECK)


class TestNonNullInferenceAndPrewhere(_Base):
    """Optimization (e9abecfdf39): non-null String inference + PREWHERE.

    Datetime columns are kept Nullable (numpy datetime64 has no O(1) null
    metadata), so only the query results below are asserted, not the schema.
    """

    def test_null_free_string_values_intact(self):
        # null-free string column is inferred as non-Nullable String; values must be exact.
        df = pd.DataFrame({"id": np.arange(N, dtype=np.int64),  # noqa: F841
                           "s": pd.array([f"v{i}" for i in range(N)], dtype=ARROW_STR)})
        rows = chdb_rows("SELECT id, s FROM Python(df) WHERE s = 'v42'", ["id", "s"])
        self.assertEqual(rows, [{"id": "42", "s": "v42"}])
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS c FROM Python(df) WHERE s IS NULL", ["c"]),
                         [{"c": "0"}])

    def test_string_with_nulls_preserved(self):
        vals = ["a", None, "b", None, "c"]
        df = pd.DataFrame({"id": np.arange(5, dtype=np.int64),  # noqa: F841
                           "s": pd.array(vals, dtype=ARROW_STR)})
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS t, COUNT(s) AS v FROM Python(df)", ["t", "v"]),
                         [{"t": "5", "v": "3"}])
        got = sorted(int(r["id"]) for r in chdb_rows("SELECT id FROM Python(df) WHERE s IS NULL", ["id"]))
        self.assertEqual(got, [1, 3])

    def test_null_free_datetime_inference(self):
        df = pd.DataFrame({  # noqa: F841
            "id": np.arange(N, dtype=np.int64),
            "when": pd.to_datetime("2021-06-01") + pd.to_timedelta(np.arange(N), unit="s"),
        })
        # filter + project on a null-free datetime column (kept Nullable)
        rows = chdb_rows("SELECT id FROM Python(df) WHERE when = '2021-06-01 00:00:10' ORDER BY id", ["id"])
        self.assertEqual(rows, [{"id": "10"}])
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS c FROM Python(df) WHERE when IS NULL", ["c"]),
                         [{"c": "0"}])

    def test_masked_int_column_is_nullable(self):
        # pandas nullable Int64 (masked extension dtype) with NULLs must infer as
        # Nullable from the dtype alone (no _mask array probe) and preserve NULLs.
        df = pd.DataFrame({  # noqa: F841
            "id": np.arange(5, dtype=np.int64),
            "m": pd.array([10, None, 30, None, 50], dtype="Int64"),
        })
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS t, COUNT(m) AS v FROM Python(df)", ["t", "v"]),
                         [{"t": "5", "v": "3"}])
        got = sorted(int(r["id"]) for r in chdb_rows("SELECT id FROM Python(df) WHERE m IS NULL", ["id"]))
        self.assertEqual(got, [1, 3])
        self.assertEqual(chdb_rows("SELECT m FROM Python(df) WHERE id = 2", ["m"]), [{"m": "30"}])

    def test_plain_int_column_not_nullable(self):
        # plain numpy int64 -> non-Nullable; no NULLs, values intact
        df = pd.DataFrame({  # noqa: F841
            "id": np.arange(N, dtype=np.int64),
            "v": (np.arange(N, dtype=np.int64) * 2),
        })
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS c FROM Python(df) WHERE v IS NULL", ["c"]),
                         [{"c": "0"}])
        self.assertEqual(chdb_rows("SELECT v FROM Python(df) WHERE id = 10", ["v"]), [{"v": "20"}])

    def test_trivial_count_exact(self):
        # SELECT count() with no WHERE is answered from totalRows(); must be exact.
        df = pd.DataFrame({"id": np.arange(N, dtype=np.int64),  # noqa: F841
                           "v": (np.arange(N, dtype=np.int64) * 2)})
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS c FROM Python(df)", ["c"]), [{"c": str(N)}])
        # a WHERE/GROUP BY count takes the normal (scanning) path and must still be right
        self.assertEqual(chdb_rows("SELECT COUNT(*) AS c FROM Python(df) WHERE id < 10", ["c"]),
                         [{"c": "10"}])

    def test_prewhere_multi_condition(self):
        # multi-condition WHERE -> multi-step PREWHERE; rows must match pandas exactly
        df = make_df(ARROW_STR)  # noqa: F841
        rows = chdb_rows(
            "SELECT id FROM Python(df) WHERE url <> '' AND grp = 1 AND i16 > 0 ORDER BY id", ["id"])
        exp = [i for i in range(N) if make_url(i) != "" and i % 4 == 1 and (i % 100 - 50) > 0]
        self.assertEqual([int(r["id"]) for r in rows], exp)


class TestRegexpReplaceAnchoredExtract(_Base):
    """REGEXP_REPLACE anchored capture-then-truncate extraction.

    chdb's port of ClickHouse PR #108270 was dropped with the v26.7 upgrade in favor of the
    upstream implementation. Upstream v26.7 matches a trailing `.*$` across newline tails
    (no newline fallback), so a row whose discarded tail contains '\\n' now also extracts the
    captured group. Expected values are hardcoded v26.7 engine semantics (const and non-const
    pattern paths verified identical).
    """

    Q28 = r"^https?://(?:www\.)?([^/]+)/.*$"

    def _check(self, str_dtype, pattern, repl, cases):
        # cases: list of (input, expected). Build a column so vectorConstantConstant (the optimized
        # path) is exercised, not constant folding of a literal.
        df = pd.DataFrame({  # noqa: F841 -- referenced by Python(df)
            "id": np.arange(len(cases), dtype=np.int64),
            "s": pd.array([c[0] for c in cases], dtype=str_dtype),
        })
        pat = pattern.replace("\\", "\\\\").replace("'", "\\'")
        rep = repl.replace("\\", "\\\\").replace("'", "\\'")
        rows = chdb_rows(
            f"SELECT id, REGEXP_REPLACE(s, '{pat}', '{rep}') AS r FROM Python(df) ORDER BY id",
            ["id", "r"],
        )
        for (inp, exp), row in zip(cases, rows):
            self.assertEqual(row["r"], exp, f"input={inp!r} pat={pattern!r} repl={repl!r}: got {row['r']!r} expected {exp!r}")

    # capturing-group \1 = host; trailing tail stripped; newline tail -> original
    Q28_CASES = [
        ("http://www.example.com/path?q=1", "example.com"),
        ("https://example.org/a", "example.org"),
        ("http://example.com", "http://example.com"),     # no /path -> no match -> original
        ("http://www.x.com/", "x.com"),
        ("http://x/y", "x"),
        ("http://h.com/a\tb", "h.com"),                   # tab in tail (not newline) -> matches
        ("http://x.com/a\nb", "x.com"),                   # newline in tail -> still matches in v26.7
        ("http://h.com/a\n", "h.com"),                    # trailing newline -> still matches in v26.7
        ("not-a-url", "not-a-url"),
        ("", ""),
    ]

    def test_q28_host_arrow(self):
        self._check(ARROW_STR, self.Q28, r"\1", self.Q28_CASES)

    def test_q28_host_object(self):
        self._check(object, self.Q28, r"\1", self.Q28_CASES)

    def test_group2_host(self):
        # \2 = host, \1 = scheme
        cases = [
            ("http://www.example.com/p", "www.example.com"),
            ("https://example.org/a/b", "example.org"),
            ("http://example.com", "http://example.com"),
            ("http://x.com/a\nb", "x.com"),               # newline tail -> still matches in v26.7
        ]
        self._check(ARROW_STR, r"^(https?)://([^/]+)/.*$", r"\2", cases)

    def test_scheme_group1(self):
        cases = [
            ("http://www.x.com/p", "http"),
            ("https://y.org/a", "https"),
            ("ftp://z.com/a", "ftp://z.com/a"),           # ftp -> no match -> original
        ]
        self._check(ARROW_STR, r"^(https?)://(?:www\.)?[^/]+/.*$", r"\1", cases)

    def test_dotall_matches_newline_tail(self):
        # (?s): dot matches newline, so the trailing .* consumes the newline tail -> host returned
        cases = [
            ("http://x.com/a\nb", "x.com"),
            ("http://h.com/p", "h.com"),
        ]
        self._check(ARROW_STR, r"(?s)^https?://(?:www\.)?([^/]+)/.*$", r"\1", cases)

    def test_non_anchored_global_replace_unaffected(self):
        # not anchored capture-then-truncate -> normal global replace, must stay correct
        cases = [("a1b2c3", "aXbXcX"), ("http://a/ http://b/", "[a] [b]")]
        self._check(ARROW_STR, r"[0-9]", "X", [cases[0]])
        self._check(ARROW_STR, r"https?://([^/]+)/", r"[\1]", [cases[1]])


class TestSchemaInferenceCoverage(_Base):
    """Schema inference (3d4b7c13407): per-column-kind ClickHouse type + Nullable-ness.

    Numeric/datetime types are decided from the dtype alone (no Series materialized);
    masked/nullable extension dtypes are detected structurally (BaseMaskedDtype).
    """

    # all pandas masked/nullable extension dtypes
    MASKED = ["Int8", "Int16", "Int32", "Int64",
              "UInt8", "UInt16", "UInt32", "UInt64",
              "Float32", "Float64", "boolean"]

    def _typename(self, df, col):  # noqa: ARG002 -- df referenced by Python(df)
        return chdb_rows(f"SELECT toTypeName({col}) AS t FROM Python(df) LIMIT 1", ["t"])[0]["t"]

    def test_all_masked_dtypes_infer_nullable(self):
        # regression: every masked dtype with a NULL -> Nullable, and the NULL is preserved
        # (not turned into a bogus value). Guards the masked-dtype detection.
        for dt in self.MASKED:
            vals = [True, None, False] if dt == "boolean" else [1, None, 3]
            df = pd.DataFrame({"x": pd.array(vals, dtype=dt)})  # noqa: F841
            tn = self._typename(df, "x")
            self.assertTrue(tn.startswith("Nullable("), f"{dt}: type {tn} is not Nullable")
            self.assertEqual(chdb_rows("SELECT count(*) AS c FROM Python(df) WHERE x IS NULL", ["c"]),
                             [{"c": "1"}], f"{dt}: IS NULL count")
            self.assertEqual(chdb_rows("SELECT count(x) AS c FROM Python(df)", ["c"]),
                             [{"c": "2"}], f"{dt}: non-null count")

    def test_masked_int_values_intact_and_aggregates_skip_null(self):
        df = pd.DataFrame({"id": np.arange(6, dtype=np.int64),  # noqa: F841
                           "m": pd.array([10, None, 30, None, 50, 60], dtype="Int64")})
        self.assertEqual(chdb_rows("SELECT sum(m) AS s, count(m) AS c FROM Python(df)", ["s", "c"]),
                         [{"s": "150", "c": "4"}])  # 10+30+50+60, nulls skipped
        self.assertEqual(chdb_rows("SELECT m FROM Python(df) WHERE id = 4", ["m"]), [{"m": "50"}])
        got = sorted(int(r["id"]) for r in chdb_rows("SELECT id FROM Python(df) WHERE m IS NULL", ["id"]))
        self.assertEqual(got, [1, 3])

    def test_numpy_integers_non_nullable_exact_type(self):
        # plain numpy ints -> exact, non-Nullable CH types (lowercase dtype, no _mask)
        df = pd.DataFrame({  # noqa: F841
            "i8": np.array([-1, 2], dtype=np.int8),
            "u16": np.array([1, 2], dtype=np.uint16),
            "i64": np.array([5, 6], dtype=np.int64),
        })
        self.assertEqual(self._typename(df, "i8"), "Int8")
        self.assertEqual(self._typename(df, "u16"), "UInt16")
        self.assertEqual(self._typename(df, "i64"), "Int64")
        self.assertEqual(chdb_rows("SELECT i8, u16, i64 FROM Python(df) WHERE i64 = 5", ["i8", "u16", "i64"]),
                         [{"i8": "-1", "u16": "1", "i64": "5"}])

    def test_datetime_is_nullable_datetime64(self):
        df = pd.DataFrame({  # noqa: F841
            "id": np.arange(3, dtype=np.int64),
            "ts": pd.to_datetime("2021-06-01") + pd.to_timedelta(np.arange(3), unit="s"),
        })
        self.assertTrue(self._typename(df, "ts").startswith("Nullable(DateTime64"),
                        f"datetime -> {self._typename(df, 'ts')}")
        self.assertEqual(chdb_rows("SELECT count(*) AS c FROM Python(df) WHERE ts IS NULL", ["c"]),
                         [{"c": "0"}])

    def test_arrow_string_nullability_follows_content(self):
        # null-free arrow string -> plain String; with a null -> Nullable(String)
        df = pd.DataFrame({"s": pd.array(["a", "b", "c"], dtype=ARROW_STR)})  # noqa: F841
        self.assertEqual(self._typename(df, "s"), "String")
        df = pd.DataFrame({"s": pd.array(["a", None, "c"], dtype=ARROW_STR)})  # noqa: F841
        self.assertEqual(self._typename(df, "s"), "Nullable(String)")
        self.assertEqual(chdb_rows("SELECT count(s) AS v, count(*) AS t FROM Python(df)", ["v", "t"]),
                         [{"v": "2", "t": "3"}])

    def test_category_is_lowcardinality(self):
        df = pd.DataFrame({"c": pd.Categorical(["x", "y", "x", "z"])})  # noqa: F841
        self.assertIn("LowCardinality", self._typename(df, "c"))
        self.assertEqual(chdb_rows("SELECT count(DISTINCT c) AS d FROM Python(df)", ["d"]), [{"d": "3"}])
        self.assertEqual(chdb_rows("SELECT count(*) AS n FROM Python(df) WHERE c = 'y'", ["n"]), [{"n": "1"}])

    def test_wide_mixed_frame_all_columns(self):
        # a wide frame mixing every kind in one go; types + a concrete row must be exact
        df = make_df(ARROW_STR)  # noqa: F841 -- has int64/int32/int16/uint32/float64/datetime/2 strings/int8
        self.assertEqual(self._typename(df, "grp"), "Int32")
        self.assertEqual(self._typename(df, "u32"), "UInt32")
        self.assertTrue(self._typename(df, "f64").startswith("Nullable(Float64"))  # numpy float -> Nullable
        self.assertTrue(self._typename(df, "when").startswith("Nullable(DateTime64"))
        self.assertEqual(self._typename(df, "url"), "String")  # null-free arrow string
        rows = chdb_rows("SELECT id, grp, u32, url, s, pad FROM Python(df) WHERE id = 12",
                         ["id", "grp", "u32", "url", "s", "pad"])
        self.assertEqual(rows, [{"id": "12", "grp": "0", "u32": "36", "url": make_url(12), "s": "v12", "pad": "3"}])


if __name__ == "__main__":
    unittest.main(verbosity=2)
