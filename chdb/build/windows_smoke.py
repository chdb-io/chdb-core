"""Smoke test of an installed chdb-core wheel on Windows (a Windows runner, or Wine).

Run it from a directory outside the source tree, so that `import chdb` finds the installed
package and not the checkout. Exits non-zero on the first failure.
"""

import os
import sys
import tempfile
import time

started = time.monotonic()


def step(name):
    print(f"[{time.monotonic() - started:6.1f}s] {name}", flush=True)


step(f"Python {sys.version} on {sys.platform}")
import chdb  # noqa: E402

step(f"chdb {chdb.__version__} from {os.path.dirname(chdb.__file__)}")

res = chdb.query("SELECT 1 AS x, 'héllo' AS s, toTypeName(now()) AS t", "CSV")
print(res)
assert str(res).strip() == '1,"héllo","DateTime"', str(res)

step("DataFrame output")
df = chdb.query("SELECT number, toString(number) AS s FROM numbers(5)", "DataFrame")
print(df)
assert list(df["number"]) == [0, 1, 2, 3, 4]

step("Arrow output")
table = chdb.query("SELECT number FROM numbers(3)", "ArrowTable")
assert table.num_rows == 3, table

step("Query a pandas DataFrame")
import pandas as pd  # noqa: E402

src = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
res = chdb.query("SELECT sum(a), max(b) FROM Python(src)", "CSV")
assert str(res).strip() == '6,"z"', str(res)

step("Parquet round trip and an on-disk session")
from chdb import session  # noqa: E402

with tempfile.TemporaryDirectory() as tmp:
    parquet = os.path.join(tmp, "t.parquet").replace("\\", "/")
    chdb.query(f"INSERT INTO FUNCTION file('{parquet}', Parquet) SELECT number AS n FROM numbers(10)")
    res = chdb.query(f"SELECT count(), sum(n) FROM file('{parquet}', Parquet)", "CSV")
    assert str(res).strip() == "10,45", str(res)

    sess = session.Session(os.path.join(tmp, "db"))
    sess.query("CREATE DATABASE IF NOT EXISTS d")
    sess.query("CREATE TABLE d.t (x Int32) ENGINE = MergeTree ORDER BY x")
    sess.query("INSERT INTO d.t VALUES (1), (2), (3)")
    res = sess.query("SELECT sum(x) FROM d.t", "CSV")
    assert str(res).strip() == "6", str(res)
    sess.close()

step("Windows smoke test passed")
