#!/usr/bin/env python3
"""Result-set transfer throughput through the ADBC driver, DuckDB-ADBC style
(duckdb.org/2023/08/04/adbc): fetch `SELECT *` over a lineitem-shaped table,
same libchdb build for every transport.

  adbc-stream   streamed Arrow C Data Interface (the driver's path)
  capi-arrow    chdb_query_arrow via ctypes, materialized
  capi-csv      CSV serialization baseline (understates a real text
                consumer's cost: no field parsing included)

Usage: CHDB_LIB_PATH=./libchdb.so python tests/benchmarks/bench_adbc_fetch.py
"""

import ctypes
import os
import statistics
import time

import pyarrow as pa
import adbc_driver_manager.dbapi as dbapi

LIB = os.environ.get("CHDB_LIB_PATH", "./libchdb.so")
N_ROWS = 5_000_000
WARMUP = 1
REPEATS = 3

SETUP = f"""
CREATE TABLE lineitem ENGINE = MergeTree() ORDER BY l_orderkey AS
SELECT
    number                        AS l_orderkey,
    toFloat64(number % 50 + 1)    AS l_quantity,
    toFloat64(number % 100000) / 100 AS l_extendedprice,
    toFloat64(number % 10) / 100  AS l_discount,
    toDate('1994-01-01') + number % 2500 AS l_shipdate,
    concat('comment_', toString(number % 1000)) AS l_comment
FROM numbers({N_ROWS})
"""

QUERY = "SELECT * FROM lineitem"


class ArrowArrayStream(ctypes.Structure):
    _fields_ = [
        ("get_schema", ctypes.c_void_p),
        ("get_next", ctypes.c_void_p),
        ("get_last_error", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


def bench(fn, *args):
    for _ in range(WARMUP):
        fn(*args)
    times = []
    for _ in range(REPEATS):
        t0 = time.perf_counter()
        rows = fn(*args)
        times.append(time.perf_counter() - t0)
        assert rows == N_ROWS, f"row mismatch: {rows}"
    return statistics.median(times)


def main():
    conn = dbapi.connect(
        driver=LIB, entrypoint="chdb_adbc_init", autocommit=True
    )
    cur = conn.cursor()
    cur.execute(SETUP)

    # --- adbc-stream ---
    def adbc_stream():
        cur.execute(QUERY)
        return sum(b.num_rows for b in cur.fetch_record_batch())

    # --- capi paths share the ADBC connection's engine via a second handle ---
    lib = ctypes.CDLL(LIB)
    lib.chdb_connect.restype = ctypes.c_void_p
    lib.chdb_connect.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
    lib.chdb_query.restype = ctypes.c_void_p
    lib.chdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
    lib.chdb_query_arrow.restype = ctypes.c_void_p
    lib.chdb_query_arrow.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_void_p
    ]
    lib.chdb_result_error.restype = ctypes.c_char_p
    lib.chdb_result_error.argtypes = [ctypes.c_void_p]
    lib.chdb_result_length.restype = ctypes.c_size_t
    lib.chdb_result_length.argtypes = [ctypes.c_void_p]
    lib.chdb_result_buffer.restype = ctypes.POINTER(ctypes.c_char)
    lib.chdb_result_buffer.argtypes = [ctypes.c_void_p]
    lib.chdb_destroy_query_result.restype = None
    lib.chdb_destroy_query_result.argtypes = [ctypes.c_void_p]

    argv = (ctypes.c_char_p * 1)(b"clickhouse")
    conn_pp = lib.chdb_connect(1, argv)
    capi_conn = ctypes.c_void_p.from_address(conn_pp).value

    def capi_arrow():
        stream = ArrowArrayStream()
        res = lib.chdb_query_arrow(
            capi_conn, QUERY.encode(), ctypes.byref(stream), None
        )
        err = lib.chdb_result_error(res)
        assert not err, err
        reader = pa.RecordBatchReader._import_from_c(ctypes.addressof(stream))
        rows = sum(b.num_rows for b in reader)
        lib.chdb_destroy_query_result(res)
        return rows

    def capi_csv():
        res = lib.chdb_query(capi_conn, QUERY.encode(), b"CSV")
        err = lib.chdb_result_error(res)
        assert not err, err
        n = lib.chdb_result_length(res)
        buf = ctypes.string_at(lib.chdb_result_buffer(res), n)
        rows = buf.count(b"\n")
        lib.chdb_destroy_query_result(res)
        return rows

    results = {}
    for name, fn in [
        ("adbc-stream", adbc_stream),
        ("capi-arrow", capi_arrow),
        ("capi-csv", capi_csv),
    ]:
        secs = bench(fn)
        results[name] = secs
        print(f"{name:14s} {secs:7.3f} s   {N_ROWS / secs / 1e6:6.1f} M rows/s")

    base = results["capi-csv"]
    for name in ("adbc-stream", "capi-arrow"):
        print(f"{name} vs csv: {base / results[name]:.1f}x faster")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
