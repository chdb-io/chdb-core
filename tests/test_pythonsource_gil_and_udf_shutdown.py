#!python3
"""Regression tests for multithreaded Python() non-pandas string/array scans and UDF shutdown safety.

Covers:
1. PythonSource::convert_string_array_to_block and PythonSource::insert_string_from_array
   acquiring the GIL on ClickHouse worker threads when scanning non-pandas dict string
   columns, NumPy object/unicode/bytes ndarray columns, numpy.recarray tables, and
   PyReader array blocks under max_threads > 1.
2. PythonUDFRegistry::removeUDF / clear dropping Python callables outside the registry
   mutex (no self-deadlock when a callable's __del__ re-enters the registry) and
   PythonScalarUDF / PythonUDAFDescriptor / PythonAggregateUDF::destroy guarding
   interpreter shutdown with Py_IsInitialized().
"""

import concurrent.futures
import json
import os
import subprocess
import sys
import unittest
import uuid

import numpy as np

import chdb
from chdb.sqltypes import INT64, STRING

_CHDB_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(chdb.__file__)))

CAFE = "café"
CJK = "中文"
CYR = "привет"
ROCKET = "\U0001f680"


def fresh_str(s: str, idx: int) -> str:
    """Build a fresh PyUnicode object at runtime so its UTF-8 cache is unpopulated."""
    return "".join([s, "_", str(idx)])


class _CustomObj:
    def __init__(self, tag: str):
        self.tag = tag

    def __str__(self) -> str:
        return f"obj<{self.tag}>"


class _NumpyColumnMapping:
    """Non-pandas mapping with .columns / .dtypes / __getitem__ backed by np.ndarray."""

    def __init__(self, columns_map):
        self._cols = dict(columns_map)
        self.columns = list(self._cols.keys())
        self.dtypes = {name: arr.dtype for name, arr in self._cols.items()}

    def __getitem__(self, key):
        return self._cols[key]


class _NdarrayPyReader(chdb.PyReader):
    """PyReader returning np.ndarray chunks to exercise insert_string_from_array."""

    def __init__(self, ids, texts, payloads):
        data = {"id": ids, "text": texts, "payload": payloads}
        super().__init__(data)
        self.data = data
        self.cursor = 0
        self.total = len(ids)

    def get_schema(self):
        return [("id", "int64"), ("text", "str"), ("payload", "str")]

    def read(self, col_names, count):
        if self.cursor >= self.total:
            return []
        end = min(self.cursor + count, self.total)
        chunk = [self.data[name][self.cursor:end] for name in col_names]
        self.cursor = end
        return chunk


_SUBPROCESS_NON_PANDAS_DICT_SCAN = """
import concurrent.futures
import sys
import uuid

class _BlockPandas:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] == "pandas":
            raise ModuleNotFoundError("pandas blocked by test", name=name)

sys.meta_path.insert(0, _BlockPandas())
sys.path.insert(0, {chdb_parent!r})

import chdb

PATTERNS = ["café", "中文", "привет", "\\U0001f680", "ascii"]

def build_fresh_dict(worker_id, n_rows=4096):
    ids = list(range(n_rows))
    texts = []
    expected_bytes = 0
    non_empty = 0
    for i in range(n_rows):
        # The first value has to be a string: the column type is inferred from it.
        if i % 11 == 10:
            texts.append(None)
        else:
            val = "".join([PATTERNS[(i + worker_id) % len(PATTERNS)], "#", str(worker_id), "_", str(i)])
            texts.append(val)
            expected_bytes += len(val.encode("utf-8"))
            non_empty += 1
    tags = [PATTERNS[i % len(PATTERNS)] for i in range(n_rows)]
    return {{"id": ids, "text": texts, "tag": tags}}, expected_bytes, non_empty

def run_worker(worker_id):
    table_dict, expected_bytes, expected_non_empty = build_fresh_dict(worker_id)
    var_name = f"__dict_tbl_{{worker_id}}_{{uuid.uuid4().hex}}"
    globals()[var_name] = table_dict
    conn = chdb.connect(":memory:")
    try:
        sql = (
            f"SELECT count(), sum(id), sum(length(text)), countIf(text != '') "
            f"FROM Python({{var_name}}) "
            f"SETTINGS max_threads = 8, max_block_size = 128"
        )
        for _ in range(3):
            res = str(conn.query(sql, "CSV")).strip()
            expected = f"4096,{{4095 * 4096 // 2}},{{expected_bytes}},{{expected_non_empty}}"
            assert res == expected, f"worker {{worker_id}}: got {{res!r}}, expected {{expected!r}}"

        grp_sql = (
            f"SELECT tag, count() FROM Python({{var_name}}) "
            f"GROUP BY tag ORDER BY tag "
            f"SETTINGS max_threads = 8, max_block_size = 128"
        )
        grp_lines = str(conn.query(grp_sql, "CSV")).strip().splitlines()
        assert len(grp_lines) == len(PATTERNS), grp_lines
    finally:
        conn.close()
        globals().pop(var_name, None)

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    futs = [pool.submit(run_worker, w) for w in range(4)]
    for f in futs:
        f.result()

print("NON_PANDAS_DICT_MULTITHREAD_OK")
"""


_SUBPROCESS_UDF_EXIT_CLEANUP = """
import sys
sys.path.insert(0, {chdb_parent!r})

import chdb
from chdb.sqltypes import INT64, STRING

@chdb.func(return_type=STRING)
def exit_udf_fmt(x: int) -> str:
    return f"val_{{x}}_中文"

class ExitAcc:
    def __init__(self):
        self.acc = 0

    def update(self, v: int) -> None:
        self.acc += v

    def merge(self, other) -> None:
        self.acc += other.acc

    def evaluate(self) -> int:
        return self.acc

chdb.create_aggregate_function("exit_udaf_sum", ExitAcc, arg_types=[INT64], return_type=INT64)

# Keep connection and an aggregate -State table live across interpreter exit so both
# atexit connection teardown and late UDF/UDAF registry & state cleanup are exercised.
conn = chdb.connect(":memory:")
res = str(conn.query(
    "SELECT count(), exit_udaf_sum(toInt64(number)) FROM ("
    "  SELECT number, exit_udf_fmt(toInt64(number)) AS s "
    "  FROM numbers(2000)"
    ") WHERE s LIKE 'val_%' SETTINGS max_threads = 4",
    "CSV",
)).strip()
assert res == "2000,1999000", res

conn.query("CREATE TABLE keep_udaf_state (k UInt64, st AggregateFunction(exit_udaf_sum, Int64)) ENGINE = Memory")
conn.query(
    "INSERT INTO keep_udaf_state "
    "SELECT number % 4 AS k, exit_udaf_sumState(toInt64(number)) "
    "FROM numbers(200) GROUP BY k"
)
merged = str(conn.query("SELECT sum(m) FROM (SELECT exit_udaf_sumMerge(st) AS m FROM keep_udaf_state GROUP BY k)", "CSV")).strip()
assert merged == str(199 * 200 // 2), merged
print("UDF_EXIT_OK")
"""


class TestPythonSourceGilAndUdfShutdown(unittest.TestCase):
    def test_non_pandas_dict_multithreaded_string_scan(self):
        """Python(dict) with pandas absent scans across 8 threads without GIL errors."""
        script = _SUBPROCESS_NON_PANDAS_DICT_SCAN.format(chdb_parent=_CHDB_PARENT_DIR)
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=180,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"subprocess failed (rc={proc.returncode})\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )
        self.assertIn("NON_PANDAS_DICT_MULTITHREAD_OK", proc.stdout)

    def test_numpy_recarray_object_string_multithreaded_scan(self):
        """numpy.recarray with strided object-dtype strings & mixed Python objects under max_threads=8."""
        n_rows = 6000
        patterns = [CAFE, CJK, CYR, ROCKET, "plain_ascii"]
        ids = np.arange(n_rows, dtype=np.int64)

        texts = np.empty(n_rows, dtype=object)
        mixed = np.empty(n_rows, dtype=object)
        expected_text_bytes = 0
        expected_mixed_strings = []

        for i in range(n_rows):
            s = fresh_str(patterns[i % len(patterns)], i)
            texts[i] = s
            expected_text_bytes += len(s.encode("utf-8"))

            mod = i % 6
            if mod == 0:
                mixed[i] = None
                expected_mixed_strings.append("")
            elif mod == 1:
                mixed[i] = float("nan")
                expected_mixed_strings.append("")
            elif mod == 2:
                raw = f"bytes_{i}".encode("utf-8")
                mixed[i] = raw
                expected_mixed_strings.append(f"bytes_{i}")
            elif mod == 3:
                d = {"k": i}
                mixed[i] = d
                expected_mixed_strings.append(json.dumps(d))
            elif mod == 4:
                lst = [i, "x"]
                mixed[i] = lst
                expected_mixed_strings.append(json.dumps(lst))
            else:
                mixed[i] = _CustomObj(str(i))
                expected_mixed_strings.append(f"obj<{i}>")

        rec = np.rec.fromarrays([ids, texts, mixed], names="id,text,mixed")
        self.assertEqual(type(rec).__name__, "recarray")
        # Interleaved record layout produces stride > sizeof(PyObject*) on column views.
        self.assertGreater(rec["text"].strides[0], 8)

        expected_mixed_bytes = sum(len(x.encode("utf-8")) for x in expected_mixed_strings)

        def query_worker(worker_idx: int):
            var_name = f"__rec_obj_{worker_idx}_{uuid.uuid4().hex}"
            globals()[var_name] = rec
            conn = chdb.connect(":memory:")
            try:
                agg_res = str(
                    conn.query(
                        f"SELECT count(), sum(id), sum(length(text)), sum(length(mixed)) "
                        f"FROM Python({var_name}) "
                        f"SETTINGS max_threads = 8, max_block_size = 128",
                        "CSV",
                    )
                ).strip()
                self.assertEqual(
                    agg_res,
                    f"{n_rows},{(n_rows - 1) * n_rows // 2},{expected_text_bytes},{expected_mixed_bytes}",
                )

                sample_res = (
                    str(
                        conn.query(
                            f"SELECT id, text, mixed FROM Python({var_name}) "
                            f"WHERE id < 6 ORDER BY id "
                            f"SETTINGS max_threads = 8, max_block_size = 64",
                            "CSV",
                        )
                    )
                    .strip()
                    .splitlines()
                )
                self.assertEqual(len(sample_res), 6)
                self.assertEqual(sample_res[0], f'0,"{fresh_str(CAFE, 0)}",""')
                self.assertEqual(sample_res[1], f'1,"{fresh_str(CJK, 1)}",""')
                self.assertEqual(sample_res[2], f'2,"{fresh_str(CYR, 2)}","bytes_2"')
                self.assertEqual(sample_res[5], f'5,"{fresh_str(CAFE, 5)}","obj<5>"')
            finally:
                conn.close()
                globals().pop(var_name, None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(query_worker, w) for w in range(4)]
            for fut in futures:
                fut.result()

    def test_numpy_recarray_fixed_unicode_and_bytes_multithreaded_scan(self):
        """numpy.recarray with fixed-width Unicode (<U) and byte-string (S) dtypes under max_threads=8."""
        n_rows = 4096
        ids = np.arange(n_rows, dtype=np.int64)
        uni_vals = [f"{CAFE}_{i}" if i % 2 == 0 else f"{CJK}_{i}" for i in range(n_rows)]
        byte_vals = [f"bin_{i}".encode("ascii") for i in range(n_rows)]

        rec_u = np.rec.fromarrays(
            [ids, np.array(uni_vals, dtype="<U24"), np.array(byte_vals, dtype="S16")],
            names="id,ustr,bstr",
        )
        self.assertEqual(rec_u["ustr"].dtype.kind, "U")
        self.assertEqual(rec_u["bstr"].dtype.kind, "S")

        expected_u_bytes = sum(len(v.encode("utf-8")) for v in uni_vals)
        expected_b_bytes = sum(len(v) for v in byte_vals)

        conn = chdb.connect(":memory:")
        try:
            res = str(
                conn.query(
                    "SELECT count(), sum(id), sum(length(ustr)), sum(length(bstr)) "
                    "FROM Python(rec_u) "
                    "SETTINGS max_threads = 8, max_block_size = 128",
                    "CSV",
                )
            ).strip()
            self.assertEqual(
                res,
                f"{n_rows},{(n_rows - 1) * n_rows // 2},{expected_u_bytes},{expected_b_bytes}",
            )

            top3 = (
                str(
                    conn.query(
                        "SELECT id, ustr, bstr FROM Python(rec_u) WHERE id < 3 ORDER BY id "
                        "SETTINGS max_threads = 8, max_block_size = 64",
                        "CSV",
                    )
                )
                .strip()
                .splitlines()
            )
            self.assertEqual(
                top3,
                [
                    f'0,"{CAFE}_0","bin_0"',
                    f'1,"{CJK}_1","bin_1"',
                    f'2,"{CAFE}_2","bin_2"',
                ],
            )
        finally:
            conn.close()

    def test_custom_ndarray_mapping_and_pyreader_ndarray_scan(self):
        """Non-pandas ndarray column mapping (strided slice) and PyReader ndarray chunks."""
        n_rows = 3000
        base_strs = np.array(
            [fresh_str([CAFE, CJK, CYR, ROCKET][i % 4], i) for i in range(n_rows * 2)],
            dtype=object,
        )
        # Step-2 slice has stride = 16 bytes (2 * sizeof(PyObject*)).
        sliced_strs = base_strs[::2]
        self.assertEqual(len(sliced_strs), n_rows)
        self.assertEqual(sliced_strs.strides[0], 2 * base_strs.strides[0])

        ids = np.arange(n_rows, dtype=np.int64)
        mapping_tbl = _NumpyColumnMapping({"id": ids, "s": sliced_strs})

        expected_bytes = sum(len(s.encode("utf-8")) for s in sliced_strs)

        conn = chdb.connect(":memory:")
        try:
            res = str(
                conn.query(
                    "SELECT count(), sum(id), sum(length(s)) FROM Python(mapping_tbl) "
                    "SETTINGS max_threads = 8, max_block_size = 100",
                    "CSV",
                )
            ).strip()
            self.assertEqual(
                res,
                f"{n_rows},{(n_rows - 1) * n_rows // 2},{expected_bytes}",
            )

            fixed_u = np.array([f"{CYR}_{i}" for i in range(n_rows)], dtype="<U20")
            reader = _NdarrayPyReader(ids, sliced_strs, fixed_u)
            expected_u_bytes = sum(len(f"{CYR}_{i}".encode("utf-8")) for i in range(n_rows))
            reader_res = str(
                conn.query(
                    "SELECT count(), sum(id), sum(length(text)), sum(length(payload)) "
                    "FROM Python(reader) "
                    "SETTINGS max_block_size = 256",
                    "CSV",
                )
            ).strip()
            self.assertEqual(
                reader_res,
                f"{n_rows},{(n_rows - 1) * n_rows // 2},{expected_bytes},{expected_u_bytes}",
            )
        finally:
            conn.close()

    def test_udf_drop_reentrant_destructor_does_not_deadlock(self):
        """Dropping a UDF whose closure's __del__ re-enters PythonUDFRegistry must not deadlock."""
        events = []

        chdb.create_function("helper_udf_to_drop", lambda x: x + 1, arg_types=[INT64], return_type=INT64)

        class _DropGuard:
            def __del__(self):
                # Synchronously invoked from ~PythonScalarUDF() when the closure's
                # refcount hits zero; re-enters PythonUDFRegistry::removeUDF.
                chdb.drop_function("helper_udf_to_drop")
                events.append("del_ran")

        def make_reentrant_udf():
            guard = _DropGuard()

            def _udf(x: int) -> int:
                _ = guard
                return x * 2

            return _udf

        chdb.create_function("reentrant_udf", make_reentrant_udf(), arg_types=[INT64], return_type=INT64)

        conn = chdb.connect(":memory:")
        try:
            val = str(conn.query("SELECT reentrant_udf(21), helper_udf_to_drop(41)", "CSV")).strip()
            self.assertEqual(val, "42,42")
        finally:
            conn.close()

        chdb.drop_function("reentrant_udf")
        self.assertEqual(events, ["del_ran"])

    def test_udf_and_udaf_interpreter_exit_cleanup(self):
        """Live UDFs, UDAFs, and AggregateFunction states exit cleanly without GIL crashes."""
        script = _SUBPROCESS_UDF_EXIT_CLEANUP.format(chdb_parent=_CHDB_PARENT_DIR)
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"subprocess failed (rc={proc.returncode})\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )
        self.assertIn("UDF_EXIT_OK", proc.stdout)
        self.assertNotIn("Fatal Python error", proc.stderr)


if __name__ == "__main__":
    unittest.main()
