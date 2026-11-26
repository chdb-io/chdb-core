#!python3

import unittest
import datetime
import chdb
from chdb.udf import chdb_udf
from chdb import func
from chdb.sqltypes import INT64, FLOAT64, STRING, BOOL, DATE
from chdb.session import Session
from chdb import query, sql

# chdb_udf registers an executable UDF, which the engine runs as an external command.
# The Windows engine cannot start external commands and fails every such call with
# this error, so these tests are skipped there, and only on that error.
NO_EXTERNAL_COMMANDS = "Code: 48. DB::Exception: Running external commands is not implemented on Windows"


def run_executable_udf(test, run, *args):
    try:
        return run(*args)
    except Exception as e:
        if NO_EXTERNAL_COMMANDS in str(e):
            test.skipTest("executable UDFs need external commands, which this build cannot run "
                          "(Running external commands is not implemented on Windows)")
        raise


@chdb_udf()
def sum_udf(lhs, rhs):
    return int(lhs) + int(rhs)


@chdb_udf(return_type="Int32")
def mul_udf(lhs, rhs):
    return int(lhs) * int(rhs)


class TestUDF(unittest.TestCase):
    def test_sum_udf(self):
        ret = run_executable_udf(self, query, "select sum_udf(12,22)", "Debug")
        self.assertEqual(str(ret), '"34"\n')

    def test_return_Int32(self):
        ret = run_executable_udf(self, query, "select mul_udf(12,22) + 1", "Debug")
        self.assertEqual(str(ret), "265\n")

    def test_define_in_function(self):
        @chdb_udf()
        def sum_udf2(lhs, rhs):
            return int(lhs) + int(rhs)

        # sql is a alias for query
        ret = run_executable_udf(self, sql, "select sum_udf2(11, 22)", "Debug")
        self.assertEqual(str(ret), '"33"\n')


class TestUDFinSession(unittest.TestCase):
    def test_sum_udf(self):
        with Session(":memory:?verbose&log-level=test") as session:
            ret = run_executable_udf(self, session.query, "select sum_udf(12,22)")
            self.assertEqual(str(ret), '"34"\n')

    def test_return_Int32(self):
        with Session("file::memory:") as session:
            ret = run_executable_udf(self, session.query, "select mul_udf(12,22) + 1")
            self.assertEqual(str(ret), "265\n")

    def test_define_in_function(self):
        @chdb_udf()
        def sum_udf2(lhs, rhs):
            return int(lhs) + int(rhs)

        with Session() as session:
            # sql is a alias for query
            ret = run_executable_udf(self, session.sql, "select sum_udf2(11, 22)", "CSV")
            self.assertEqual(str(ret), '"33"\n')


if __name__ == "__main__":
    unittest.main()
