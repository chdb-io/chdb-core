#!python3

import ctypes
import ctypes.util
import gc
import mmap
import os
import subprocess
import sys
import unittest

import numpy as np
import pandas as pd

import chdb

PANDAS3 = int(pd.__version__.split(".")[0]) >= 3


def big_df(nrows=200_000):
    return pd.DataFrame(
        {
            "i64": np.arange(nrows, dtype=np.int64),
            "i32": (np.arange(nrows, dtype=np.int32) % 1000),
            "u16": (np.arange(nrows) % 500).astype(np.uint16),
            "f64": np.arange(nrows, dtype=np.float64) / 7.0,
            "s": [f"payload-{j % 997}-{'x' * (j % 23)}" for j in range(nrows)],
            "t": [f"{j % 13}" for j in range(nrows)],
        }
    )


class TestZeroCopyScan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = chdb.connect(":memory:")
        cls.df = big_df()
        cls.nrows = len(cls.df)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def q(self, sql):
        return str(self.conn.query(sql, "CSV")).strip()

    def test_numeric_aggregates_exact(self):
        df = self.df  # noqa: F841
        n = self.nrows
        self.assertEqual(
            self.q("SELECT count(), sum(i64), sum(i32), sum(u16) FROM Python(df)"),
            f"{n},{n * (n - 1) // 2},{sum(range(1000)) * (n // 1000)},{int(np.sum(np.arange(n) % 500))}",
        )

    def test_string_values_roundtrip(self):
        df = self.df  # noqa: F841
        self.assertEqual(
            self.q("SELECT sum(length(s)) FROM Python(df)"), str(int(self.df["s"].str.len().sum()))
        )
        self.assertEqual(
            self.q("SELECT s FROM Python(df) WHERE i64 = 42"), '"' + self.df["s"][42] + '"'
        )
        self.assertEqual(self.q("SELECT countDistinct(s) FROM Python(df)"), str(self.df["s"].nunique()))

    def test_group_by_on_borrowed_strings(self):
        df = self.df  # noqa: F841
        pd_top = self.df.groupby("t").size().sort_values(ascending=False)
        top_count = int(pd_top.iloc[0])
        self.assertEqual(
            self.q("SELECT count() AS c FROM Python(df) GROUP BY t ORDER BY c DESC LIMIT 1"),
            str(top_count),
        )

    def test_order_by_limit_on_borrowed(self):
        df = self.df  # noqa: F841
        self.assertEqual(
            self.q("SELECT i64, s FROM Python(df) ORDER BY i64 DESC LIMIT 2"),
            f'{self.nrows - 1},"{self.df["s"].iloc[-1]}"\n{self.nrows - 2},"{self.df["s"].iloc[-2]}"',
        )

    def test_self_join_on_borrowed(self):
        df = self.df  # noqa: F841
        self.assertEqual(
            self.q(
                "SELECT count() FROM (SELECT i32 FROM Python(df) WHERE i64 < 1000) a "
                "JOIN (SELECT i32 FROM Python(df) WHERE i64 < 1000) b ON a.i32 = b.i32"
            ),
            "1000",
        )

    def test_filter_where_on_borrowed(self):
        df = self.df  # noqa: F841
        expected = int((self.df["i32"] == 7).sum())
        self.assertEqual(self.q("SELECT count() FROM Python(df) WHERE i32 = 7"), str(expected))
        self.assertEqual(
            self.q("SELECT sum(i64) FROM Python(df) WHERE i32 = 7"),
            str(int(self.df.loc[self.df["i32"] == 7, "i64"].sum())),
        )

    def test_short_circuit_if_on_borrowed(self):
        df = self.df  # noqa: F841
        expected = int(np.where(np.arange(self.nrows) % 2 == 0, np.arange(self.nrows), 0).sum())
        self.assertEqual(
            self.q("SELECT sum(if(i64 % 2 = 0, i64, 0)) FROM Python(df)"), str(expected)
        )

    def test_with_totals_on_borrowed(self):
        df = self.df  # noqa: F841
        out = self.q("SELECT t, sum(i64) FROM Python(df) GROUP BY t WITH TOTALS ORDER BY t LIMIT 3")
        self.assertIn(",", out)

    def test_limit_by_on_borrowed(self):
        df = self.df  # noqa: F841
        self.assertEqual(
            self.q("SELECT count() FROM (SELECT t, i64 FROM Python(df) ORDER BY i64 LIMIT 2 BY t)"),
            str(13 * 2),
        )

    def test_multi_chunk_borrow(self):
        parts = [big_df(60_000) for _ in range(3)]
        mc = pd.concat(parts, ignore_index=True)
        if PANDAS3:
            self.assertGreater(mc["s"].array._pa_array.num_chunks, 1)
        globals()["mc_df"] = mc
        try:
            self.assertEqual(
                self.q("SELECT count(), sum(length(s)) FROM Python(mc_df)"),
                f"{len(mc)},{int(mc['s'].str.len().sum())}",
            )
            self.assertEqual(
                self.q("SELECT sum(i64) FROM Python(mc_df)"), str(int(mc["i64"].sum()))
            )
        finally:
            del globals()["mc_df"]

    def test_nullable_string_fallback(self):
        s = pd.Series([f"v{j}" for j in range(50_000)], dtype="str" if PANDAS3 else object)
        s[10] = None
        dfn = pd.DataFrame({"s": s, "v": np.arange(50_000, dtype=np.int64)})
        globals()["dfn"] = dfn
        try:
            self.assertEqual(self.q("SELECT count() FROM Python(dfn) WHERE s IS NULL"), "1")
            self.assertEqual(
                self.q("SELECT sum(length(s)) FROM Python(dfn)"),
                str(int(dfn["s"].dropna().str.len().sum())),
            )
        finally:
            del globals()["dfn"]

    def test_result_outlives_dataframe(self):
        local_df = big_df(100_000)
        globals()["tmp_owner_df"] = local_df
        try:
            ret = self.conn.query("SELECT i64, s FROM Python(tmp_owner_df) ORDER BY i64 DESC LIMIT 5", "CSV")
        finally:
            del globals()["tmp_owner_df"]
        del local_df
        gc.collect()
        self.q("SELECT 1")
        gc.collect()
        text = str(ret)
        self.assertIn("99999", text)
        self.assertIn(f"payload-{99999 % 997}-", text)

    def test_repeated_queries_no_leak(self):
        df = self.df  # noqa: F841
        for _ in range(30):
            self.q("SELECT sum(i64), max(length(s)) FROM Python(df)")
        gc.collect()

    def test_arrow_direct_predicate_still_works(self):
        df = self.df  # noqa: F841
        expected = int(self.df["s"].str.contains("payload-7-", regex=False).sum())
        self.assertEqual(
            self.q("SELECT count() FROM Python(df) WHERE s LIKE '%payload-7-%'"), str(expected)
        )


PROT_READ_SCRIPT = r"""
import ctypes, mmap, sys
import numpy as np, pandas as pd, pyarrow as pa, chdb

PAGE = 4096
libc = ctypes.CDLL(None, use_errno=True)

def mmap_readonly_arrow_string(values):
    arr = pa.array(values, type=pa.string())
    buffers = arr.buffers()
    offsets_np = np.frombuffer(buffers[1], dtype=np.int32)
    data_bytes = buffers[2].to_pybytes() if buffers[2] is not None else b""

    def alloc_ro(payload, extra_tail=64):
        n = (len(payload) + extra_tail + PAGE - 1) // PAGE * PAGE
        m = mmap.mmap(-1, n)
        m.write(payload)
        addr = ctypes.addressof(ctypes.c_char.from_buffer(m))
        assert libc.mprotect(ctypes.c_void_p(addr), ctypes.c_size_t(n), 1) == 0  # PROT_READ
        return m, addr

    m_off, off_addr = alloc_ro(offsets_np.tobytes())
    m_dat, dat_addr = alloc_ro(data_bytes)
    off_buf = pa.foreign_buffer(off_addr, len(offsets_np) * 4, m_off)
    dat_buf = pa.foreign_buffer(dat_addr, len(data_bytes), m_dat)
    ro = pa.StringArray.from_buffers(len(arr), off_buf, dat_buf)
    assert ro.to_pylist() == values[: len(values)]
    return ro, dat_addr, (m_off, m_dat)

nrows = 100_000
values = [f"guarded-{j % 101}-{'y' * (j % 17)}" for j in range(nrows)]
# Deterministic tail position: keep the payload end well inside a page so the
# borrow-side same-page check passes and the mount actually happens.
total = sum(len(v) for v in values)
pad = (2048 - total) % PAGE
values[-1] = values[-1] + "z" * pad
ro_arr, dat_addr, keep = mmap_readonly_arrow_string(values)
s = pd.arrays.ArrowStringArray(pa.chunked_array([ro_arr]))
df = pd.DataFrame({"s": s, "v": np.arange(nrows, dtype=np.int64)})
# The harness is void if pandas copied the buffers: assert identity.
got_addr = df["s"].array._pa_array.chunk(0).buffers()[2].address
assert got_addr == dat_addr, (got_addr, dat_addr)

conn = chdb.connect(":memory:")
queries = [
    "SELECT sum(length(s)) FROM Python(df)",
    "SELECT countDistinct(s) FROM Python(df)",
    "SELECT count() FROM Python(df) WHERE s LIKE '%guarded-7-%'",
    "SELECT s FROM Python(df) ORDER BY v DESC LIMIT 3",
    "SELECT t.s, count() FROM (SELECT s FROM Python(df)) t GROUP BY t.s ORDER BY count() DESC LIMIT 2",
    "SELECT count() FROM (SELECT s, v FROM Python(df) ORDER BY v LIMIT 1 BY s)",
    "SELECT sum(if(v % 2 = 0, length(s), 0)) FROM Python(df)",
]
expected_len = sum(len(v) for v in values)
r0 = str(conn.query(queries[0], "CSV")).strip()
assert r0 == str(expected_len), (r0, expected_len)
for q in queries[1:]:
    conn.query(q, "CSV")
conn.close()
print("PROT_READ_HARNESS_OK")
"""


class TestZeroCopyProtRead(unittest.TestCase):
    @unittest.skipUnless(PANDAS3 and sys.platform == "linux", "needs pandas 3 + linux mprotect")
    def test_engine_never_writes_borrowed_buffers(self):
        env = dict(os.environ)
        res = subprocess.run(
            [sys.executable, "-c", PROT_READ_SCRIPT],
            capture_output=True,
            text=True,
            env=env,
            timeout=300,
            check=False,
        )
        self.assertEqual(res.returncode, 0, f"stdout={res.stdout[-500:]} stderr={res.stderr[-3000:]}")
        self.assertIn("PROT_READ_HARNESS_OK", res.stdout)


EQUIV_SCRIPT = r"""
import numpy as np, pandas as pd, chdb
conn = chdb.connect(':memory:')
df = pd.DataFrame({
    'i64': np.arange(100_000, dtype=np.int64),
    's': [f'e{j % 321}' for j in range(100_000)],
})
out = []
for q in [
    'SELECT sum(i64), sum(length(s)), countDistinct(s) FROM Python(df)',
    "SELECT count() FROM Python(df) WHERE s = 'e7'",
    'SELECT s FROM Python(df) ORDER BY i64 DESC LIMIT 1',
]:
    out.append(str(conn.query(q, 'CSV')).strip())
print('|'.join(out))
"""


class TestZeroCopyEscapeHatch(unittest.TestCase):
    def run_mode(self, extra_env):
        env = dict(os.environ)
        env.update(extra_env)
        res = subprocess.run(
            [sys.executable, "-c", EQUIV_SCRIPT],
            capture_output=True,
            text=True,
            env=env,
            timeout=300,
            check=False,
        )
        self.assertEqual(res.returncode, 0, res.stderr[-2000:])
        return res.stdout.strip().splitlines()[-1]

    def test_zero_copy_off_equivalence(self):
        on = self.run_mode({})
        off = self.run_mode({"CHDB_ZERO_COPY": "0"})
        self.assertEqual(on, off)


class TestBorrowReleaseWithoutNextQuery(unittest.TestCase):
    def test_dataframe_result_settles_guards_in_call(self):
        import weakref

        conn = chdb.connect(":memory:")
        try:
            df = pd.DataFrame({"x": np.arange(300_000, dtype=np.int64)})
            view = df["x"].to_numpy(copy=False)
            block = view.base if view.base is not None else view
            wr = weakref.ref(block)
            globals()["lifec_df"] = df
            try:
                res = conn.query("SELECT x FROM Python(lifec_df)", "dataframe")
                self.assertEqual(len(res), 300_000)
            finally:
                del globals()["lifec_df"]
            del df, view, block, res
            gc.collect()
            self.assertIsNone(
                wr(),
                "borrowed numeric buffer must be released by the query call itself, "
                "not deferred to the next query",
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
