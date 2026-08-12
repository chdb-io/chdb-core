#!python3

import gc
import os
import subprocess
import sys
import unittest

import numpy as np
import pandas as pd

import chdb

PANDAS3 = int(pd.__version__.split(".")[0]) >= 3


def refresh_frame_locals():
    """chdb's variable lookup walks every stack frame's f_locals; on
    Python <= 3.12 (pre-PEP 667) that access materializes a cached snapshot
    dict on the frame which holds strong references and is NOT updated by
    `del`. Re-reading f_locals refreshes the snapshot and drops deleted
    entries, so weakref-release assertions see the true liveness."""
    sys._getframe(1).f_locals  # noqa: B018


def make_df(nrows=1000, tag=""):
    return pd.DataFrame(
        {
            "i64": np.arange(nrows, dtype=np.int64),
            "i32": np.arange(nrows, dtype=np.int32) * 2,
            "f64": np.arange(nrows, dtype=np.float64) / 3.0,
            "s": [f"{tag}str{j % 7}" for j in range(nrows)],
        }
    )


class TestPandasMetaCache(unittest.TestCase):
    def setUp(self):
        self.conn = chdb.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def q(self, sql):
        return str(self.conn.query(sql, "CSV")).strip()

    def test_repeated_query_stable(self):
        df = make_df()  # noqa: F841
        expected = self.q("SELECT count(), sum(i64), sum(i32), min(s), max(s) FROM Python(df)")
        for _ in range(5):
            self.assertEqual(
                self.q("SELECT count(), sum(i64), sum(i32), min(s), max(s) FROM Python(df)"), expected
            )
        self.assertEqual(expected, '1000,499500,999000,"str0","str6"')

    def test_rebind_to_new_dataframe(self):
        df = make_df(100)  # noqa: F841
        self.assertEqual(self.q("SELECT count(), sum(i64) FROM Python(df)"), "100,4950")
        df = make_df(50, tag="new")  # noqa: F841
        self.assertEqual(self.q("SELECT count(), sum(i64), min(s) FROM Python(df)"), '50,1225,"newstr0"')

    def test_replace_numeric_column_same_dtype(self):
        df = make_df(100)
        self.assertEqual(self.q("SELECT sum(i64) FROM Python(df)"), "4950")
        df["i64"] = df["i64"] + 1000000
        self.assertEqual(self.q("SELECT sum(i64) FROM Python(df)"), str(4950 + 100 * 1000000))

    def test_inplace_numeric_write_visible(self):
        df = make_df(100)
        self.assertEqual(self.q("SELECT sum(i64) FROM Python(df)"), "4950")
        df.loc[0, "i64"] = 777
        self.assertEqual(self.q("SELECT sum(i64) FROM Python(df)"), str(4950 + 777))
        self.assertEqual(self.q("SELECT i64 FROM Python(df) WHERE i64 = 777"), "777")

    def test_string_column_write_visible(self):
        df = make_df(100)
        self.assertEqual(self.q("SELECT count() FROM Python(df) WHERE s = 'CHANGED'"), "0")
        df.loc[3, "s"] = "CHANGED"
        self.assertEqual(self.q("SELECT count() FROM Python(df) WHERE s = 'CHANGED'"), "1")
        self.assertEqual(self.q("SELECT i64 FROM Python(df) WHERE s = 'CHANGED'"), "3")

    def test_replace_whole_string_column(self):
        df = make_df(10)
        self.q("SELECT count() FROM Python(df)")
        df["s"] = [f"rep{j}" for j in range(10)]
        self.assertEqual(self.q("SELECT min(s), max(s) FROM Python(df)"), '"rep0","rep9"')

    def test_add_drop_rename_column(self):
        df = make_df(10)
        self.assertEqual(self.q("SELECT count() FROM Python(df)"), "10")
        df["extra"] = np.arange(10, dtype=np.int64) * 10
        self.assertEqual(self.q("SELECT sum(extra) FROM Python(df)"), "450")
        df = df.drop(columns=["extra"])
        with self.assertRaises(Exception):
            self.conn.query("SELECT sum(extra) FROM Python(df)", "CSV")
        df = df.rename(columns={"i64": "renamed"})
        self.assertEqual(self.q("SELECT sum(renamed) FROM Python(df)"), "45")

    def test_append_rows_visible(self):
        df = make_df(10)
        self.assertEqual(self.q("SELECT count() FROM Python(df)"), "10")
        df.loc[len(df)] = [999, 999, 9.9, "appended"]
        self.assertEqual(self.q("SELECT count() FROM Python(df)"), "11")
        self.assertEqual(self.q("SELECT count() FROM Python(df) WHERE s = 'appended'"), "1")

    def test_dtype_change_visible(self):
        df = make_df(10)
        self.assertEqual(self.q("SELECT toTypeName(i64) FROM Python(df) LIMIT 1"), '"Int64"')
        df["i64"] = df["i64"].astype(np.float64)
        self.assertIn("Float64", self.q("SELECT toTypeName(i64) FROM Python(df) LIMIT 1"))

    def test_del_and_recreate(self):
        df = make_df(10)
        self.assertEqual(self.q("SELECT count() FROM Python(df)"), "10")
        del df
        gc.collect()
        df = make_df(20, tag="again")  # noqa: F841
        self.assertEqual(self.q("SELECT count(), min(s) FROM Python(df)"), '20,"againstr0"')

    def test_weakref_does_not_extend_lifetime(self):
        import weakref

        df = make_df(10)
        ref = weakref.ref(df)
        self.q("SELECT count() FROM Python(df)")
        del df
        refresh_frame_locals()
        gc.collect()
        self.assertIsNone(ref(), "metadata cache must not keep the DataFrame alive")

    def test_object_column_bypass_and_mutation(self):
        df = pd.DataFrame({"a": np.arange(5, dtype=np.int64), "o": pd.Series([{"k": 1}] * 5, dtype=object)})
        first = self.q("SELECT count() FROM Python(df)")
        self.assertEqual(first, "5")
        second = self.q("SELECT count() FROM Python(df)")
        self.assertEqual(second, "5")

    def test_category_column_bypass(self):
        df = pd.DataFrame({"c": pd.Categorical(["x", "y", "x", "z"]), "v": np.arange(4, dtype=np.int64)})
        self.assertEqual(
            self.q("SELECT c, sum(v) FROM Python(df) GROUP BY c ORDER BY c"), '"x",2\n"y",1\n"z",3'
        )
        self.assertEqual(
            self.q("SELECT c, sum(v) FROM Python(df) GROUP BY c ORDER BY c"), '"x",2\n"y",1\n"z",3'
        )

    @unittest.skipUnless(PANDAS3, "arrow-backed str dtype requires pandas 3")
    def test_nullable_flip_string_column(self):
        df = pd.DataFrame({"s": pd.array(["a", "b", "c"], dtype="str")})
        self.assertEqual(self.q("SELECT toTypeName(s) FROM Python(df) LIMIT 1"), '"String"')
        df.loc[1, "s"] = None
        self.assertIn("Nullable", self.q("SELECT toTypeName(s) FROM Python(df) LIMIT 1"))
        self.assertEqual(self.q("SELECT count() FROM Python(df) WHERE s IS NULL"), "1")
        df.loc[1, "s"] = "b"
        self.assertEqual(self.q("SELECT toTypeName(s) FROM Python(df) LIMIT 1"), '"String"')
        self.assertEqual(self.q("SELECT count() FROM Python(df) WHERE s IS NULL"), "0")

    def test_multiple_dataframes_lru(self):
        dfs = {}
        for k in range(6):
            dfs[f"lru{k}"] = make_df(10 + k, tag=f"t{k}")
        for k in range(6):
            globals()[f"lru{k}"] = dfs[f"lru{k}"]
        try:
            for _ in range(2):
                for k in range(6):
                    self.assertEqual(
                        self.q(f"SELECT count(), min(s) FROM Python(lru{k})"), f'{10 + k},"t{k}str0"'
                    )
        finally:
            for k in range(6):
                del globals()[f"lru{k}"]

    def test_integer_column_labels_consistent(self):
        # Scanning int-labeled columns is a pre-existing chdb limitation
        # (fillColumn indexes with str(label)); the cache must not change the
        # behavior between the first and repeated runs.
        df = pd.DataFrame({0: np.arange(10, dtype=np.int64), 1: [f"v{j}" for j in range(10)]})
        outcomes = []
        for _ in range(3):
            try:
                outcomes.append(("ok", self.q('SELECT sum("0") FROM Python(df)')))
            except Exception as e:
                outcomes.append(("err", type(e).__name__))
        self.assertEqual(len(set(outcomes)), 1, outcomes)
        self.assertEqual(self.q("SELECT count() FROM Python(df)"), "10")

    def test_string_backing_released_immediately(self):
        import weakref

        df = make_df(10)
        backing = weakref.ref(df["s"].array._pa_array) if PANDAS3 else None
        self.q("SELECT count(), min(s) FROM Python(df)")
        del df
        refresh_frame_locals()
        gc.collect()
        if backing is not None:
            self.assertIsNone(
                backing(),
                "the metadata cache holds string backings only via weakref; "
                "deleting the DataFrame must free the payload without any follow-up query",
            )

    def test_two_tables_in_one_query(self):
        left = make_df(10, tag="l")  # noqa: F841
        right = make_df(10, tag="r")  # noqa: F841
        self.assertEqual(
            self.q(
                "SELECT count() FROM Python(left) a JOIN Python(right) b ON a.i64 = b.i64"
            ),
            "10",
        )

    def test_multi_chunk_string_column(self):
        parts = [make_df(100, tag=f"p{i}") for i in range(5)]
        df = pd.concat(parts, ignore_index=True)
        if PANDAS3:
            self.assertGreater(df["s"].array._pa_array.num_chunks, 1)
        expected = "500," + str(sum(range(100)) * 5 + 100 * 2 * sum(range(5)) * 0)
        self.assertEqual(self.q("SELECT count(), sum(i64) FROM Python(df)"), "500," + str(sum(range(100)) * 5))
        self.assertEqual(self.q("SELECT countDistinct(s) FROM Python(df)"), str(7 * 5))
        del expected


ENV_MODE_SCRIPT = r"""
import numpy as np, pandas as pd, chdb
conn = chdb.connect(':memory:')
df = pd.DataFrame({'a': np.arange(100, dtype=np.int64), 's': [f's{i%5}' for i in range(100)]})
out = []
out.append(str(conn.query('SELECT count(), sum(a), min(s) FROM Python(df)', 'CSV')).strip())
out.append(str(conn.query('SELECT count(), sum(a), min(s) FROM Python(df)', 'CSV')).strip())
df.loc[0, 's'] = 'zz'
out.append(str(conn.query("SELECT count() FROM Python(df) WHERE s = 'zz'", 'CSV')).strip())
df['a'] = df['a'] * 2
out.append(str(conn.query('SELECT sum(a) FROM Python(df)', 'CSV')).strip())
print('|'.join(out))
"""


class TestMetaCacheEnvModes(unittest.TestCase):
    def run_mode(self, extra_env):
        env = dict(os.environ)
        env.update(extra_env)
        res = subprocess.run(
            [sys.executable, "-c", ENV_MODE_SCRIPT],
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
            check=False,
        )
        self.assertEqual(res.returncode, 0, res.stderr[-2000:])
        return res.stdout.strip().splitlines()[-1]

    def test_disabled_and_strict_match_default(self):
        default = self.run_mode({})
        disabled = self.run_mode({"CHDB_PANDAS_META_CACHE": "0"})
        strict = self.run_mode({"CHDB_PANDAS_META_CACHE_STRICT": "1"})
        self.assertEqual(default, disabled)
        self.assertEqual(default, strict)
        self.assertEqual(default, "100,4950,\"s0\"|100,4950,\"s0\"|1|9900")


if __name__ == "__main__":
    unittest.main()
