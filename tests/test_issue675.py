#!python3

import datetime
import unittest
import uuid
from decimal import Decimal

import numpy as np
import pandas as pd
import chdb


def _row(sql):
    return chdb.query(sql, "DataFrame").iloc[0].to_dict()


class TestIssue675(unittest.TestCase):
    """Python(dict) reads each list with the type rules of a pandas object column."""

    def test_types(self):
        data = {
            "i": [1, 2],
            "big": [1, 2**40],
            "f": [0.5, 1.5],
            "s": ["x", "y"],
            "b": [True, False],
            "d": [Decimal("1.5"), Decimal("2.25")],
            "ts": [datetime.datetime(2026, 1, 1, 12), datetime.datetime(2026, 1, 2)],
            "dt": [datetime.date(2026, 1, 1), datetime.date(2026, 1, 2)],
            "u": [uuid.UUID(int=1), uuid.UUID(int=2)],
            "j": [{"k": 1}, {"k": 2}],
            "n": [1, None],
            "huge": [1, 2**70],
            "raw": [b"ab", b"\xe8\x8b\xb9"],
        }
        types = _row("SELECT " + ", ".join(f"toTypeName({c}) AS {c}" for c in data) + " FROM Python(data) LIMIT 1")
        # pandas turns datetime objects into datetime64 when it builds the frame; the unit depends on its version.
        self.assertTrue(types.pop("ts").startswith("Nullable(DateTime64("))
        self.assertEqual(types, {
            "i": "Nullable(Int32)",
            "big": "Nullable(Int64)",
            "f": "Nullable(Float64)",
            "s": "Nullable(String)",
            "b": "Nullable(Bool)",
            "d": "Nullable(Decimal(3, 2))",
            "dt": "Nullable(String)",
            "u": "Nullable(String)",
            "j": "Nullable(JSON)",
            "n": "Nullable(Int32)",
            "huge": "Nullable(String)",
            "raw": "Nullable(String)",
        })

    def test_values_are_unchanged(self):
        data = {
            "i": [1, 2**40, -3],
            "f": [0.5, None, 2.0],
            "huge": [1, 2**70, 3],
            "raw": [b"ab", b"\xe8\x8b\xb9", b""],
            "d": [Decimal("1.5"), Decimal("2.25"), None],
        }
        ret = chdb.query("SELECT i, f, huge, hex(raw) AS raw, toString(d) AS d FROM Python(data)", "DataFrame")
        self.assertEqual(ret["i"].tolist(), [1, 2**40, -3])
        self.assertEqual(ret["f"].isna().tolist(), [False, True, False])
        self.assertEqual(ret["huge"].tolist(), ["1", str(2**70), "3"])
        self.assertEqual(ret["raw"].tolist(), ["6162", "E88BB9", ""])
        self.assertEqual(ret["d"].tolist()[:2], ["1.5", "2.25"])

    def test_bytes_like_values(self):
        data = {
            "b": [b"ab", b"\x00"],
            "ba": [bytearray(b"ab"), bytearray(b"\xff")],
            "mv": [memoryview(b"ab"), memoryview(b"\xe8\x8b\xb9")],
        }
        ret = chdb.query("SELECT hex(b) AS b, hex(ba) AS ba, hex(mv) AS mv FROM Python(data)", "DataFrame")
        self.assertEqual(ret.to_dict("list"), {"b": ["6162", "00"], "ba": ["6162", "FF"], "mv": ["6162", "E88BB9"]})

    def test_numpy_and_series_values_keep_their_dtype(self):
        data = {"a": np.array([1, 2], dtype=np.int16), "b": pd.Series([0.5, 1.5], dtype="float32")}
        self.assertEqual(_row("SELECT toTypeName(a) AS a, toTypeName(b) AS b FROM Python(data) LIMIT 1"),
                         {"a": "Int16", "b": "Nullable(Float32)"})

    def test_lists_of_lists(self):
        data = {"a": [[1, 2], [3, 4]]}
        self.assertEqual(chdb.query("SELECT a FROM Python(data)", "CSV").bytes(), b'"[1, 2]"\n"[3, 4]"\n')

    def test_the_dict_is_not_modified(self):
        data = {"a": [1, 2]}
        chdb.query("SELECT sum(a) FROM Python(data)", "CSV")
        self.assertEqual(data, {"a": [1, 2]})
        self.assertIsInstance(data["a"], list)


if __name__ == "__main__":
    unittest.main()
