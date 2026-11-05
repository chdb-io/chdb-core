#!/usr/bin/env python3
"""
Benchmark: Arrow (vectorized) Python UDFs against the per-row native UDFs.

Every case registers the same function twice, once per type, checks that both
return the same result and times the query with each. A builtin SQL expression
with the same result gives the engine-only baseline.

Run separately from unit tests:
    python tests/benchmarks/bench_udf_arrow.py [--rows N] [--threads T]
"""

import argparse
import math
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

import chdb
from chdb.session import Session

REPEATS = 3


def case(name, arg_types, return_type, native, arrow, source, expr, builtin):
    return {
        "name": name,
        "arg_types": arg_types,
        "return_type": return_type,
        "native": native,
        "arrow": arrow,
        "source": source,
        "expr": expr,
        "builtin": builtin,
    }


CASES = [
    case(
        "int: x * 2 + 1",
        ["Int64"], "Int64",
        lambda x: x * 2 + 1,
        lambda x: pc.add(pc.multiply(x, 2), 1),
        "SELECT toInt64(number) AS x FROM {table}({rows})",
        "sum({f}(x))",
        "sum(x * 2 + 1)",
    ),
    case(
        "float: sqrt(x) * 0.5 (numpy)",
        ["Float64"], "Float64",
        lambda x: math.sqrt(x) * 0.5,
        lambda x: np.sqrt(x.to_numpy()) * 0.5,
        "SELECT toFloat64(number) AS x FROM {table}({rows})",
        "sum({f}(x))",
        "sum(sqrt(x) * 0.5)",
    ),
    case(
        "two args: a * b + a",
        ["Float64", "Float64"], "Float64",
        lambda a, b: a * b + a,
        lambda a, b: pc.add(pc.multiply(a, b), a),
        "SELECT toFloat64(number % 1000) AS a, toFloat64(number % 7) AS b FROM {table}({rows})",
        "sum({f}(a, b))",
        "sum(a * b + a)",
    ),
    case(
        "string: upper(s)",
        ["String"], "String",
        lambda s: s.upper(),
        lambda s: pc.utf8_upper(s),
        "SELECT concat('item_', toString(number % 100000)) AS s FROM {table}({rows})",
        "sum(length({f}(s)))",
        "sum(length(upper(s)))",
    ),
    case(
        "string -> int: length(s)",
        ["String"], "Int64",
        lambda s: len(s),
        lambda s: pc.utf8_length(s),
        "SELECT concat('item_', toString(number % 100000)) AS s FROM {table}({rows})",
        "sum({f}(s))",
        "sum(lengthUTF8(s))",
    ),
    case(
        "nullable (10% NULL, on_null=skip): x + 1",
        ["Int64"], "Int64",
        lambda x: x + 1,
        lambda x: pc.add(x, 1),
        "SELECT if(number % 10 = 0, NULL, toInt64(number)) AS x FROM {table}({rows})",
        "sum({f}(x))",
        "sum(x + 1)",
    ),
    # Per-value Python code inside an Arrow UDF: what it costs depends on how the values
    # leave Arrow. to_pylist() builds a pyarrow scalar per value and is slower than the
    # per-row UDF; to_numpy().tolist() is not.
    case(
        "python loop over to_numpy().tolist()",
        ["Int64"], "Int64",
        lambda x: x * 2 + 1,
        lambda x: [v * 2 + 1 for v in x.to_numpy().tolist()],
        "SELECT toInt64(number) AS x FROM {table}({rows})",
        "sum({f}(x))",
        "sum(x * 2 + 1)",
    ),
    case(
        "python loop over to_pylist()",
        ["Int64"], "Int64",
        lambda x: x * 2 + 1,
        lambda x: [v * 2 + 1 for v in x.to_pylist()],
        "SELECT toInt64(number) AS x FROM {table}({rows})",
        "sum({f}(x))",
        "sum(x * 2 + 1)",
    ),
]


def same(left, right):
    # Parallel float sums add up in no fixed order.
    return left == right or math.isclose(float(left), float(right), rel_tol=1e-9)


def timed(session, sql):
    best = None
    result = None
    for _ in range(REPEATS):
        start = time.perf_counter()
        result = str(session.query(sql, "CSV")).strip()
        elapsed = time.perf_counter() - start
        best = elapsed if best is None else min(best, elapsed)
    return best, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--threads", type=int, default=0, help="max_threads; 0 keeps the default")
    parser.add_argument("--table", default="numbers_mt", choices=["numbers", "numbers_mt"],
                        help="numbers is a single stream, so it runs every UDF on one thread")
    args = parser.parse_args()

    print(f"chdb={chdb.__version__}  pyarrow={pa.__version__}  numpy={np.__version__}")
    print(f"rows={args.rows:,}  table={args.table}  max_threads={args.threads or 'default'}  "
          f"best of {REPEATS}")
    print()

    settings = f" SETTINGS max_threads = {args.threads}" if args.threads else ""
    session = Session()
    header = f"{'case':<44} {'builtin':>9} {'native':>9} {'arrow':>9} {'speedup':>8}"
    print(header)
    print("-" * len(header))

    for i, c in enumerate(CASES):
        native_name, arrow_name = f"bench_native_{i}", f"bench_arrow_{i}"
        chdb.create_function(native_name, c["native"], c["arg_types"], c["return_type"])
        chdb.create_function(arrow_name, c["arrow"], c["arg_types"], c["return_type"], type="arrow")
        try:
            source = c["source"].format(table=args.table, rows=args.rows)

            def sql(expr):
                return f"SELECT {expr} FROM ({source}){settings}"

            builtin_time, builtin_result = timed(session, sql(c["builtin"]))
            native_time, native_result = timed(session, sql(c["expr"].format(f=native_name)))
            arrow_time, arrow_result = timed(session, sql(c["expr"].format(f=arrow_name)))
            if not (same(native_result, builtin_result) and same(arrow_result, builtin_result)):
                raise AssertionError(
                    f"{c['name']}: results differ: builtin={builtin_result} "
                    f"native={native_result} arrow={arrow_result}")

            print(f"{c['name']:<44} {builtin_time:>8.3f}s {native_time:>8.3f}s {arrow_time:>8.3f}s "
                  f"{native_time / arrow_time:>7.1f}x")
        finally:
            chdb.drop_function(native_name)
            chdb.drop_function(arrow_name)

    session.close()


if __name__ == "__main__":
    main()
