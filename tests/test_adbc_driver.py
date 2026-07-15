#!python3
"""
Tests for the ADBC driver entrypoint (chdb_adbc_init) exported by libchdb.

Loads libchdb through the standard ADBC driver manager (the exact path any
external consumer uses):

    adbc_driver_manager.dbapi.connect(
        driver="<path to libchdb.so>", entrypoint="chdb_adbc_init")

Requires libchdb built from a NOT USE_PYTHON configuration with the ADBC
entrypoint, plus the `adbc_driver_manager` and `pyarrow` packages. If any of
those is missing the tests are skipped, mirroring test_arrow_c_data_output.py.
"""

import os
import shutil
import tempfile
import unittest

try:
    import pyarrow as pa
    from adbc_driver_manager import dbapi as adbc_dbapi
except ImportError:  # pragma: no cover - optional test dependency
    pa = None
    adbc_dbapi = None


def _candidate_libchdb_paths():
    here = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(here)
    names = ["libchdb.so", "libchdb.dylib"]
    yield from (os.path.join(project_root, n) for n in names)
    yield from (os.path.join(project_root, "buildlib", n) for n in names)
    on_path = shutil.which("libchdb.so")
    if on_path:
        yield on_path


def _find_libchdb_path():
    for path in _candidate_libchdb_paths():
        if os.path.exists(path):
            return path
    return None


_LIBCHDB_PATH = _find_libchdb_path()


def _has_adbc_entrypoint(path):
    if path is None or adbc_dbapi is None:
        return False
    try:
        conn = adbc_dbapi.connect(
            driver=path, entrypoint="chdb_adbc_init", autocommit=True
        )
        conn.close()
        return True
    except Exception:
        return False


_SKIP_REASON = (
    "adbc_driver_manager/pyarrow not installed"
    if adbc_dbapi is None
    else "libchdb with chdb_adbc_init not found"
)
_ENABLED = _has_adbc_entrypoint(_LIBCHDB_PATH)


def _connect(path=":memory:"):
    # autocommit=True matches the driver's transaction model (autocommit-only)
    # and avoids the DB-API compliance warning from the default connect path.
    return adbc_dbapi.connect(
        driver=_LIBCHDB_PATH,
        entrypoint="chdb_adbc_init",
        db_kwargs={"path": path},
        autocommit=True,
    )


@unittest.skipUnless(_ENABLED, _SKIP_REASON)
class TestAdbcQuery(unittest.TestCase):
    def test_select_arrow_result(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT number AS n, toString(number) AS s FROM numbers(5)"
            )
            table = cur.fetch_arrow_table()
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.column_names, ["n", "s"])
        self.assertEqual(table.column("n").to_pylist(), [0, 1, 2, 3, 4])
        self.assertEqual(table.column("s").to_pylist(), ["0", "1", "2", "3", "4"])

    def test_fetchone_via_dbapi(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 21 * 2 AS answer")
            row = cur.fetchone()
        self.assertEqual(row[0], 42)

    def test_error_reports_message(self):
        with _connect() as conn, conn.cursor() as cur:
            with self.assertRaises(Exception) as ctx:
                cur.execute("SELECT * FROM this_table_does_not_exist_42")
            self.assertIn("this_table_does_not_exist_42", str(ctx.exception))

    def test_execute_update_ddl_and_insert(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS adbc_t")
            cur.execute(
                "CREATE TABLE adbc_t.upd (x Int64) ENGINE = MergeTree() ORDER BY x"
            )
            cur.execute("INSERT INTO adbc_t.upd SELECT number FROM numbers(10)")
            cur.execute("SELECT count() FROM adbc_t.upd")
            self.assertEqual(cur.fetchone()[0], 10)


@unittest.skipUnless(_ENABLED, _SKIP_REASON)
class TestAdbcIngest(unittest.TestCase):
    def _table(self, n=6, offset=0):
        return pa.table(
            {
                "id": pa.array(range(offset, offset + n), pa.int64()),
                "name": pa.array([f"row{i}" for i in range(offset, offset + n)]),
            }
        )

    def test_ingest_create_and_append(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_ca", self._table(6), mode="create")
            cur.execute("SELECT count() FROM ingest_ca")
            self.assertEqual(cur.fetchone()[0], 6)

            cur.adbc_ingest("ingest_ca", self._table(4, offset=6), mode="append")
            cur.execute("SELECT count(), max(id) FROM ingest_ca")
            self.assertEqual(cur.fetchone(), (10, 9))

    def test_ingest_replace(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_rep", self._table(6), mode="create")
            cur.adbc_ingest("ingest_rep", self._table(2), mode="replace")
            cur.execute("SELECT count() FROM ingest_rep")
            self.assertEqual(cur.fetchone()[0], 2)

    def test_ingest_create_append(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_capp", self._table(3), mode="create_append")
            cur.adbc_ingest("ingest_capp", self._table(3), mode="create_append")
            cur.execute("SELECT count() FROM ingest_capp")
            self.assertEqual(cur.fetchone()[0], 6)

    def test_ingest_create_existing_fails(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_dup", self._table(1), mode="create")
            with self.assertRaises(Exception):
                cur.adbc_ingest("ingest_dup", self._table(1), mode="create")

    def test_ingest_roundtrip_values(self):
        table = self._table(5)
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_rt", table, mode="create")
            cur.execute("SELECT id, name FROM ingest_rt ORDER BY id")
            got = cur.fetch_arrow_table()
        self.assertEqual(got.column("id").to_pylist(), table.column("id").to_pylist())
        self.assertEqual(
            got.column("name").to_pylist(), table.column("name").to_pylist()
        )


@unittest.skipUnless(_ENABLED, _SKIP_REASON)
class TestAdbcMetadata(unittest.TestCase):
    def test_get_info(self):
        with _connect() as conn:
            info = conn.adbc_get_info()
        self.assertEqual(info["vendor_name"], "ClickHouse")
        self.assertEqual(info["driver_name"], "ADBC chDB Driver")
        self.assertTrue(info["vendor_version"])

    def test_get_table_schema(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE schema_probe (a Int64, b String) "
                "ENGINE = MergeTree() ORDER BY a"
            )
            schema = conn.adbc_get_table_schema("schema_probe")
        self.assertEqual(schema.names, ["a", "b"])
        self.assertEqual(schema.field("a").type, pa.int64())

    def test_get_table_types(self):
        with _connect() as conn:
            types = conn.adbc_get_table_types()
        self.assertIn("BASE TABLE", types)

    def test_autocommit_only(self):
        with _connect() as conn:
            # dbapi defaults to autocommit for this driver setup; explicit
            # commit on the raw connection must report INVALID_STATE.
            with self.assertRaises(Exception):
                conn.adbc_connection.commit()


@unittest.skipUnless(_ENABLED, _SKIP_REASON)
class TestAdbcPersistence(unittest.TestCase):
    def test_on_disk_path_roundtrip(self):
        # Engine state is process-global with one storage path; this test
        # exercises connect/reconnect against the same on-disk path.
        tmp = tempfile.mkdtemp(prefix="chdb_adbc_")
        try:
            with _connect(tmp) as conn, conn.cursor() as cur:
                cur.adbc_ingest(
                    "persisted",
                    pa.table({"v": pa.array([1, 2, 3], pa.int64())}),
                    mode="create",
                )
            with _connect(tmp) as conn, conn.cursor() as cur:
                cur.execute("SELECT sum(v) FROM persisted")
                self.assertEqual(cur.fetchone()[0], 6)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
