#!python3
"""Every type an Arrow UDF can declare, as an argument and as its result.

For each type: the Arrow type the argument arrives as and the values it holds, extreme values
included; the same values coming back unchanged; NULL under on_null="skip" and "pass"; the result
built from plain Python values instead of an Arrow array; and, for the integers, a result that does
not fit the type.
"""

import datetime
import os
import unittest
from typing import Any, Callable, List, NamedTuple, Optional

import pyarrow as pa
import pyarrow.compute as pc

import chdb
from chdb.session import Session

# chdb-core-lite drops the Int128/256 and UInt128/256 conversion functions the big integer
# cases build their input with; see tests/test_func_udf_types.py.
_LITE = os.environ.get("CHDB_LITE") == "1"

UTC = datetime.timezone.utc


def ns_since_epoch(x):
    # Python datetimes stop at microseconds, so compare nanosecond timestamps as integers.
    return pc.cast(x, pa.int64()).to_pylist()


class Case(NamedTuple):
    declared: Any                   # arg_types / return_type spelling
    sql_type: str                   # the type's ClickHouse name
    literals: List[str]             # SQL expressions of that type, extreme values included
    arrow_type: pa.DataType         # what the argument arrives as
    values: List[Any]               # its values, as `extract` gives them
    returned: Optional[Callable] = None   # builds the result from Python values; defaults to `values`
    too_big: Optional[List[Any]] = None   # a result holding a value outside the type
    extract: Callable = lambda x: x.to_pylist()  # noqa: E731
    big_int: bool = False


CASES = {
    "uint8": Case("UInt8", "UInt8", ["toUInt8(0)", "toUInt8(255)"], pa.uint8(), [0, 255], too_big=[256]),
    "uint16": Case("UInt16", "UInt16", ["toUInt16(0)", "toUInt16(65535)"], pa.uint16(), [0, 65535],
                   too_big=[-1]),
    "uint32": Case("UInt32", "UInt32", ["toUInt32(0)", "toUInt32(4294967295)"], pa.uint32(), [0, 4294967295],
                   too_big=[4294967296]),
    "uint64": Case("UInt64", "UInt64", ["toUInt64(0)", "toUInt64(18446744073709551615)"], pa.uint64(),
                   [0, 18446744073709551615], too_big=["18446744073709551616"]),
    "int8": Case("Int8", "Int8", ["toInt8(-128)", "toInt8(127)"], pa.int8(), [-128, 127], too_big=[128]),
    "int16": Case("Int16", "Int16", ["toInt16(-32768)", "toInt16(32767)"], pa.int16(), [-32768, 32767],
                  too_big=[-32769]),
    "int32": Case("Int32", "Int32", ["toInt32(-2147483648)", "toInt32(2147483647)"], pa.int32(),
                  [-2147483648, 2147483647], too_big=[2147483648]),
    "int64": Case("Int64", "Int64", ["toInt64(-9223372036854775808)", "toInt64(9223372036854775807)"], pa.int64(),
                  [-9223372036854775808, 9223372036854775807], too_big=[9223372036854775808]),
    # Wider than any Arrow integer: they travel as decimal text, and come back from it.
    "uint128": Case("UInt128", "UInt128",
                    ["toUInt128('0')", "toUInt128('340282366920938463463374607431768211455')"], pa.string(),
                    ["0", "340282366920938463463374607431768211455"],
                    too_big=["340282366920938463463374607431768211456"], big_int=True),
    "int128": Case("Int128", "Int128",
                   ["toInt128('-170141183460469231731687303715884105728')",
                    "toInt128('170141183460469231731687303715884105727')"], pa.string(),
                   ["-170141183460469231731687303715884105728", "170141183460469231731687303715884105727"],
                   too_big=["170141183460469231731687303715884105728"], big_int=True),
    "uint256": Case("UInt256", "UInt256",
                    ["toUInt256('0')",
                     "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')"],
                    pa.string(),
                    ["0", "115792089237316195423570985008687907853269984665640564039457584007913129639935"],
                    too_big=["115792089237316195423570985008687907853269984665640564039457584007913129639936"],
                    big_int=True),
    "int256": Case("Int256", "Int256",
                   ["toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968')",
                    "toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967')"],
                   pa.string(),
                   ["-57896044618658097711785492504343953926634992332820282019728792003956564819968",
                    "57896044618658097711785492504343953926634992332820282019728792003956564819967"],
                   too_big=["-57896044618658097711785492504343953926634992332820282019728792003956564819969"],
                   big_int=True),
    "float32": Case("Float32", "Float32",
                    ["toFloat32(-1.5)", "toFloat32('inf')", "toFloat32(3.4028234663852886e38)"], pa.float32(),
                    [-1.5, float("inf"), 3.4028234663852886e38]),
    "float64": Case("Float64", "Float64",
                    ["toFloat64(-1.5)", "toFloat64('-inf')", "toFloat64(1.7976931348623157e308)", "toFloat64(5e-324)"],
                    pa.float64(), [-1.5, float("-inf"), 1.7976931348623157e308, 5e-324]),
    "bool": Case("Bool", "Bool", ["true", "false"], pa.bool_(), [True, False]),
    "string": Case("String", "String", ["''", "'abc'", "'héllo'", "'a\\0b'"], pa.string(),
                   ["", "abc", "héllo", "a\x00b"]),
    "bytes": Case(bytes, "String", ["unhex('ff00')", "''"], pa.binary(), [b"\xff\x00", b""]),
    "date": Case("Date", "Date", ["toDate('1970-01-01')", "toDate('2149-06-06')"], pa.date32(),
                 [datetime.date(1970, 1, 1), datetime.date(2149, 6, 6)]),
    "date32": Case("Date32", "Date32", ["toDate32('1900-01-01')", "toDate32('2299-12-31')"], pa.date32(),
                   [datetime.date(1900, 1, 1), datetime.date(2299, 12, 31)]),
    "datetime": Case("DateTime('UTC')", "DateTime('UTC')",
                     ["toDateTime('1970-01-01 00:00:00', 'UTC')", "toDateTime('2106-02-07 06:28:15', 'UTC')"],
                     pa.timestamp("s", "UTC"),
                     [datetime.datetime(1970, 1, 1, tzinfo=UTC), datetime.datetime(2106, 2, 7, 6, 28, 15, tzinfo=UTC)]),
    "datetime64_0": Case("DateTime64(0, 'UTC')", "DateTime64(0, 'UTC')",
                         ["toDateTime64('1900-01-01 00:00:00', 0, 'UTC')", "toDateTime64('2299-12-31 23:59:59', 0, 'UTC')"],
                         pa.timestamp("s", "UTC"),
                         [datetime.datetime(1900, 1, 1, tzinfo=UTC), datetime.datetime(2299, 12, 31, 23, 59, 59, tzinfo=UTC)]),
    # A scale that is not a multiple of 3 travels in the next finer Arrow unit.
    "datetime64_2": Case("DateTime64(2, 'UTC')", "DateTime64(2, 'UTC')",
                         ["toDateTime64('1900-01-01 00:00:00.01', 2, 'UTC')",
                          "toDateTime64('2299-12-31 23:59:59.99', 2, 'UTC')"],
                         pa.timestamp("ms", "UTC"),
                         [datetime.datetime(1900, 1, 1, 0, 0, 0, 10000, tzinfo=UTC),
                          datetime.datetime(2299, 12, 31, 23, 59, 59, 990000, tzinfo=UTC)]),
    "datetime64_3": Case("DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')",
                         ["toDateTime64('1900-01-01 00:00:00.001', 3, 'UTC')",
                          "toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC')"],
                         pa.timestamp("ms", "UTC"),
                         [datetime.datetime(1900, 1, 1, 0, 0, 0, 1000, tzinfo=UTC),
                          datetime.datetime(2299, 12, 31, 23, 59, 59, 999000, tzinfo=UTC)]),
    "datetime64_6": Case("DateTime64(6, 'UTC')", "DateTime64(6, 'UTC')",
                         ["toDateTime64('1900-01-01 00:00:00.000001', 6, 'UTC')",
                          "toDateTime64('2299-12-31 23:59:59.999999', 6, 'UTC')"],
                         pa.timestamp("us", "UTC"),
                         [datetime.datetime(1900, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
                          datetime.datetime(2299, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)]),
    "datetime64_9": Case("DateTime64(9, 'UTC')", "DateTime64(9, 'UTC')",
                         ["toDateTime64('1900-01-01 00:00:00.000000001', 9, 'UTC')",
                          "toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC')"],
                         pa.timestamp("ns", "UTC"),
                         [-2208988799999999999, 9223372036854775807],
                         returned=lambda values: pa.array(values, pa.timestamp("ns", "UTC")),
                         extract=ns_since_epoch),
}


def recorder(case, seen):
    """The identity, recording the Arrow type and the values each call receives."""

    def identity(x):
        seen.append((x.type, case.extract(x), x.null_count))
        return x

    return identity


def returning(result):
    def fn(x):
        return result

    return fn


class TestArrowUDFTypes(unittest.TestCase):
    def setUp(self):
        self.session = Session()
        self.registered = []

    def tearDown(self):
        for name in self.registered:
            chdb.drop_function(name)
        self.session.close()

    def create(self, name, fn, case, **kwargs):
        chdb.create_function(name, fn, [case.declared], case.declared, type="arrow", **kwargs)
        self.registered.append(name)

    def rows(self, name, literals):
        """Each row as (argument, result IS NULL, result = argument, result type)."""
        ret = self.session.query(
            f"SELECT isNull(r), ifNull(r = v, 0), toTypeName(r) FROM "
            f"(SELECT v, {name}(v) AS r FROM (SELECT arrayJoin([{', '.join(literals)}]) AS v))",
            "CSV")
        rows = []
        for line in str(ret).strip().splitlines():
            is_null, equal, type_name = line.split(",", 2)
            rows.append((is_null == "1", equal == "1", type_name.strip('"')))
        return rows

    def check_round_trip(self, name, case):
        seen = []
        self.create(name, recorder(case, seen), case)
        rows = self.rows(name, case.literals)
        self.assertEqual(seen, [(case.arrow_type, case.values, 0)])
        self.assertEqual(rows, [(False, True, f"Nullable({case.sql_type})")] * len(case.literals))

    def check_skip(self, name, case):
        seen = []
        self.create(name, recorder(case, seen), case)
        rows = self.rows(name, [case.literals[0], "NULL", case.literals[-1]])
        # The NULL row never reaches the function; the others arrive without a NULL.
        self.assertEqual(seen, [(case.arrow_type, [case.values[0], case.values[-1]], 0)])
        self.assertEqual([r[:2] for r in rows], [(False, True), (True, False), (False, True)])

    def check_pass(self, name, case):
        seen = []
        self.create(name, recorder(case, seen), case, on_null="pass")
        rows = self.rows(name, [case.literals[0], "NULL", case.literals[-1]])
        self.assertEqual(seen, [(case.arrow_type, [case.values[0], None, case.values[-1]], 1)])
        self.assertEqual([r[:2] for r in rows], [(False, True), (True, False), (False, True)])

    def check_from_python_values(self, name, case):
        result = case.returned(case.values) if case.returned else list(case.values)
        self.create(name, returning(result), case)
        rows = self.rows(name, case.literals)
        self.assertEqual(rows, [(False, True, f"Nullable({case.sql_type})")] * len(case.literals))

    def check_too_big(self, name, case):
        self.create(name, returning(case.too_big), case)
        with self.assertRaises(RuntimeError):
            self.rows(name, [case.literals[0]])


def _add_tests(case_name, case):
    checks = {
        "round_trip": TestArrowUDFTypes.check_round_trip,
        "skip_null": TestArrowUDFTypes.check_skip,
        "pass_null": TestArrowUDFTypes.check_pass,
        "result_from_python_values": TestArrowUDFTypes.check_from_python_values,
    }
    if case.too_big is not None:
        checks["result_out_of_range"] = TestArrowUDFTypes.check_too_big

    for check_name, check in checks.items():
        def test(self, check=check, udf_name=f"arrow_type_{case_name}_{check_name}"):
            check(self, udf_name, case)

        test.__name__ = f"test_{case_name}_{check_name}"
        if case.big_int:
            test = unittest.skipIf(_LITE, "chdb-core-lite drops Int128/256, UInt128/256 conversions")(test)
        setattr(TestArrowUDFTypes, test.__name__, test)


for _name, _case in CASES.items():
    _add_tests(_name, _case)


if __name__ == "__main__":
    unittest.main()
