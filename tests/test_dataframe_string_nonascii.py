#!/usr/bin/env python3
"""Regression tests for non-ASCII string values scanned via Python(df).

chdb-core <= 26.5.0 appended a trailing NUL byte to every non-ASCII string
value converted from Python (PyUnicode -> UTF-8), silently corrupting data:
length('中文') returned 7 instead of 6, WHERE s = '中文' matched nothing, and
the corrupted value round-tripped back into query results. ASCII-only values
took a different code path and were unaffected.
"""

import unittest

import pandas as pd

import chdb

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

# Mixed scripts, accents, emoji and an ASCII control value.
SAMPLES = ["中文测试", "école", "日本語", "🎉emoji", "ascii_only"]


class TestDataFrameNonAsciiStrings(unittest.TestCase):
    """Non-ASCII values must survive Python(df) byte-exact."""

    def _expected_rows(self, values):
        # UTF-8 byte order equals codepoint order, so python sorted() agrees
        # with ClickHouse ORDER BY on String.
        return [
            "\t".join(
                (
                    v,
                    str(len(v.encode("utf-8"))),
                    str(len(v)),
                    v.encode("utf-8").hex().upper(),
                )
            )
            for v in sorted(values)
        ]

    def _check_exact_bytes(self, df, values):
        res = str(
            chdb.query(
                "SELECT s, length(s), lengthUTF8(s), hex(s)"
                " FROM Python(df) ORDER BY s",
                "TSV",
            )
        )
        self.assertEqual(res.strip().split("\n"), self._expected_rows(values))

    def test_object_dtype_exact_utf8_bytes(self):
        df = pd.DataFrame({"s": pd.Series(SAMPLES, dtype=object)})
        self._check_exact_bytes(df, SAMPLES)

    def test_default_dtype_exact_utf8_bytes(self):
        # object dtype on pandas 2.x, `str` dtype on pandas 3.x
        df = pd.DataFrame({"s": SAMPLES})
        self._check_exact_bytes(df, SAMPLES)

    @unittest.skipUnless(HAS_PYARROW, "needs pyarrow")
    def test_string_pyarrow_dtype_exact_utf8_bytes(self):
        df = pd.DataFrame({"s": pd.array(SAMPLES, dtype="string[pyarrow]")})
        self._check_exact_bytes(df, SAMPLES)

    def test_where_equality_on_non_ascii(self):
        df = pd.DataFrame({"s": SAMPLES})
        hit = str(
            chdb.query(
                "SELECT count() FROM Python(df) WHERE s = '中文测试'", "CSV"
            )
        )
        self.assertEqual(hit, "1\n")
        # The NUL-corrupted variant must NOT match anything.
        miss = str(
            chdb.query(
                "SELECT count() FROM Python(df) WHERE s = '中文测试\\0'", "CSV"
            )
        )
        self.assertEqual(miss, "0\n")

    def test_group_by_non_ascii(self):
        df = pd.DataFrame({"s": ["北京", "北京", "上海"]})
        res = str(
            chdb.query(
                "SELECT s, count() AS c FROM Python(df) GROUP BY s ORDER BY s",
                "CSV",
            )
        )
        self.assertEqual(res, '"上海",1\n"北京",2\n')

    def test_roundtrip_via_dataframe_output(self):
        df = pd.DataFrame({"s": SAMPLES})
        out = chdb.query("SELECT s FROM Python(df) ORDER BY s", "DataFrame")
        self.assertEqual(list(out["s"]), sorted(SAMPLES))


if __name__ == "__main__":
    unittest.main(verbosity=2)
