#!/usr/bin/env python3
"""ADBC driver tests: load the driver through the standard driver manager
(driver=<path>, entrypoint="chdb_adbc_init") — the exact path any external
consumer uses. Works against libchdb.so or the Python module's _chdb.abi3.so;
CHDB_LIB_PATH overrides discovery. Skips when dependencies are missing."""

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
    if os.environ.get("CHDB_LIB_PATH"):
        yield os.environ["CHDB_LIB_PATH"]
    here = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(here)
    names = ["libchdb.so", "libchdb.dylib"]
    yield from (os.path.join(project_root, n) for n in names)
    yield from (os.path.join(project_root, "buildlib", n) for n in names)
    on_path = shutil.which("libchdb.so")
    if on_path:
        yield on_path
    # Installed chdb package (wheel CI): the module doubles as the driver.
    try:
        import importlib.util as _ilu

        _spec = _ilu.find_spec("chdb")
        if _spec and _spec.origin:
            yield os.path.join(os.path.dirname(_spec.origin), "_chdb.abi3.so")
    except (ImportError, ValueError):
        pass


def _find_libchdb_path():
    for path in _candidate_libchdb_paths():
        if os.path.exists(path):
            return path
    return None


_LIBCHDB_PATH = _find_libchdb_path()

# The Python module doubles as the driver, but its pybind runtime must be
# initialized by a normal import before the ADBC entrypoint is used — the
# same order the locator package uses (import _chdb, then _chdb.__file__).
if _LIBCHDB_PATH and _LIBCHDB_PATH.endswith(".abi3.so"):
    import importlib.util

    _spec = importlib.util.spec_from_file_location("_chdb", _LIBCHDB_PATH)
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)


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

    def test_empty_stream_outlives_connection(self):
        # A schema-only (empty) streaming result must stay safe to read and
        # release after the statement, connection, and database are closed —
        # it detaches from the connection at wire-up.
        from adbc_driver_manager import AdbcConnection, AdbcDatabase, AdbcStatement

        db = AdbcDatabase(driver=_LIBCHDB_PATH, entrypoint="chdb_adbc_init")
        conn = AdbcConnection(db)
        stmt = AdbcStatement(conn)
        stmt.set_sql_query("SELECT 1 AS x WHERE 0")
        handle, _ = stmt.execute_query()
        reader = pa.RecordBatchReader._import_from_c(handle.address)
        stmt.close()
        conn.close()
        db.close()
        self.assertEqual(reader.read_all().num_rows, 0)
        del reader

    def test_connect_without_entrypoint(self):
        # AdbcDriverInit (the default name driver managers probe) is exported
        # as an alias, so the entrypoint argument is optional.
        conn = adbc_dbapi.connect(
            driver=_LIBCHDB_PATH, db_kwargs={"path": ":memory:"}, autocommit=True
        )
        with conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)

    def test_fetchone_via_dbapi(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 21 * 2 AS answer")
            row = cur.fetchone()
        self.assertEqual(row[0], 42)

    def test_large_result_streams_in_batches(self):
        # ExecuteQuery goes through the streaming Arrow path: a large result
        # arrives as multiple record batches instead of one materialized blob.
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT number FROM numbers(1000000)")
            reader = cur.fetch_record_batch()
            batches = list(reader)
        self.assertGreater(len(batches), 1)
        self.assertEqual(sum(b.num_rows for b in batches), 1000000)

    def test_empty_select_preserves_schema(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT number AS n, toString(number) AS s "
                "FROM numbers(10) WHERE number < 0"
            )
            tbl = cur.fetch_arrow_table()
        self.assertEqual(tbl.num_rows, 0)
        self.assertEqual(tbl.column_names, ["n", "s"])

    def test_different_statement_rejected_while_stream_open(self):
        # ADBC concurrency: chDB runs one statement at a time and cannot buffer
        # a live stream, so a DIFFERENT statement's execute is rejected (not a
        # silent invalidation of the open reader). The first reader stays valid.
        from adbc_driver_manager import AdbcStatusCode

        with _connect() as conn:
            cur1 = conn.cursor()
            cur1.execute("SELECT number FROM numbers(1000000)")
            r1 = iter(cur1.fetch_record_batch())
            b0 = next(r1)  # partially consumed, stream still live

            cur2 = conn.cursor()
            with self.assertRaises(Exception) as ctx:
                cur2.execute("SELECT number + 5000000 FROM numbers(1000000)")
            self.assertEqual(ctx.exception.status_code, AdbcStatusCode.INVALID_STATE)

            total1 = b0.num_rows + sum(b.num_rows for b in r1)  # r1 reads to the end
            self.assertEqual(total1, 1000000)
            cur1.close()
            cur2.close()

    def test_same_statement_reexecute_invalidates_own_stream(self):
        # The spec REQUIRES a statement's next execute to invalidate its own
        # prior result. Re-executing the same cursor works; the old reader
        # fails with a clear message.
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT number FROM numbers(1000000)")
            r1 = iter(cur.fetch_record_batch())
            next(r1)

            cur.execute("SELECT 42")  # same statement reused — allowed, no error
            self.assertEqual(cur.fetchone()[0], 42)

            # The old reader is now dead; the exact message depends on which
            # layer closes it (driver invalidation vs the dbapi cursor's own
            # result teardown), so just require that reading it fails.
            with self.assertRaises(Exception):
                for _ in r1:
                    pass
            cur.close()

    def test_metadata_call_rejected_while_stream_open(self):
        # A metadata call is not a statement reuse, so an open live stream
        # makes it fail with INVALID_STATE rather than being silently killed.
        from adbc_driver_manager import AdbcStatusCode

        with _connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT number FROM numbers(1000000)")
            reader = iter(cur.fetch_record_batch())
            b0 = next(reader)

            with self.assertRaises(Exception) as ctx:
                conn.adbc_get_objects(depth="db_schemas").read_all()
            self.assertEqual(ctx.exception.status_code, AdbcStatusCode.INVALID_STATE)

            total = b0.num_rows + sum(b.num_rows for b in reader)  # stream untouched
            self.assertEqual(total, 1000000)
            cur.close()

    def test_abandoned_stream_leaves_connection_usable(self):
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT number FROM numbers(1000000)")
            reader = iter(cur.fetch_record_batch())
            next(reader)
            del reader  # abandon mid-stream: release() cancels the query
            cur.execute("SELECT 7")
            self.assertEqual(cur.fetchone()[0], 7)
            cur.close()

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
        from adbc_driver_manager import AdbcStatusCode

        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_dup", self._table(1), mode="create")
            with self.assertRaises(Exception) as ctx:
                cur.adbc_ingest("ingest_dup", self._table(1), mode="create")
        self.assertEqual(ctx.exception.status_code, AdbcStatusCode.ALREADY_EXISTS)

    def test_ingest_create_append_schema_mismatch(self):
        from adbc_driver_manager import AdbcStatusCode

        mismatched = pa.table({"id": [1], "extra": ["x"]})
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ingest_mismatch", self._table(2), mode="create_append")
            with self.assertRaises(Exception) as ctx:
                cur.adbc_ingest("ingest_mismatch", mismatched, mode="create_append")
        self.assertEqual(ctx.exception.status_code, AdbcStatusCode.ALREADY_EXISTS)

    def test_ingest_target_db_schema(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS ingest_tgt")
            cur.adbc_ingest(
                "in_schema", self._table(3), mode="create",
                db_schema_name="ingest_tgt",
            )
            cur.execute("SELECT count() FROM ingest_tgt.in_schema")
            self.assertEqual(cur.fetchone()[0], 3)

    def test_ingest_maps_field_nullability(self):
        t = pa.table(
            {"nn": pa.array([1, 2], pa.int64()), "nu": pa.array([1, None], pa.int64())},
            schema=pa.schema(
                [pa.field("nn", pa.int64(), nullable=False),
                 pa.field("nu", pa.int64(), nullable=True)]
            ),
        )
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_ingest("ing_null", t, mode="create")
            cur.execute(
                "SELECT name, type FROM system.columns "
                "WHERE table = 'ing_null' AND database = currentDatabase() ORDER BY name"
            )
            self.assertEqual(
                cur.fetchall(), [("nn", "Int64"), ("nu", "Nullable(Int64)")]
            )

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
class TestAdbcParameters(unittest.TestCase):
    def test_positional_select(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ? + 1 AS v, upper(?) AS s", (41, "abc"))
            self.assertEqual(cur.fetchone(), (42, "ABC"))

    def test_param_types_roundtrip(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT ? AS f, ? AS b, toTypeName(?) AS t",
                (2.5, True, 7),
            )
            row = cur.fetchone()
        self.assertEqual(row[0], 2.5)
        self.assertEqual(row[1], True)
        self.assertEqual(row[2], "Int64")

    def test_null_param(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT isNull(?) AS n", (None,))
            self.assertEqual(cur.fetchone()[0], 1)

    def test_placeholder_in_literal_not_bound(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT '?q' AS lit, ? AS v", (5,))
            self.assertEqual(cur.fetchone(), ("?q", 5))

    def test_executemany_insert(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE param_many (id Int64, name String) "
                "ENGINE = MergeTree() ORDER BY id"
            )
            cur.executemany(
                "INSERT INTO param_many VALUES (?, ?)",
                [(1, "a"), (2, "b"), (3, "c")],
            )
            cur.execute("SELECT count(), sum(id) FROM param_many")
            self.assertEqual(cur.fetchone(), (3, 6))

    def test_multi_row_bind_concatenates_result_sets(self):
        # Binding several parameter rows to a result-returning statement runs
        # it once per row and concatenates the per-execution results.
        params = pa.RecordBatch.from_pydict({"0": pa.array([1, 2, 3, 4], pa.int64())})
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_statement.set_sql_query("SELECT 1 + ? AS v")
            cur.adbc_statement.bind(params)
            handle, _ = cur.adbc_statement.execute_query()
            result = pa.RecordBatchReader._import_from_c(handle.address).read_all()
        self.assertEqual(result.column("v").to_pylist(), [2, 3, 4, 5])

    def test_param_injection_is_inert(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ? AS s", ("'; DROP TABLE x; --",))
            self.assertEqual(cur.fetchone()[0], "'; DROP TABLE x; --")

    def test_param_string_control_chars_roundtrip(self):
        # Parameter values travel as escaped text: tab/newline would end the
        # value mid-parse and a bare backslash-N would read as NULL.
        cases = ["tab\there", "new\nline", "cr\rhere", "back\\slash", "\\N", "mix\t\\N\nend"]
        with _connect() as conn, conn.cursor() as cur:
            for v in cases:
                cur.execute("SELECT ? AS s", (v,))
                self.assertEqual(cur.fetchone()[0], v)

    def test_single_null_parameter_selects_null(self):
        # A lone NULL parameter in a SELECT binds as Nullable(String) with \N,
        # not a bare literal NULL (which types the column as Nothing and has
        # no Arrow output — the stream would fail to fetch).
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ? AS x", (None,))
            self.assertEqual(cur.fetchone(), (None,))

    def test_multi_row_bind_null_and_backslash_n_distinct(self):
        # In concatenated executions a real NULL and the literal string
        # backslash-N must stay distinguishable.
        params = pa.RecordBatch.from_pydict(
            {"0": pa.array(["\\N", None, "x\ty"], pa.string())}
        )
        with _connect() as conn, conn.cursor() as cur:
            cur.adbc_statement.set_sql_query("SELECT ? AS s")
            cur.adbc_statement.bind(params)
            handle, _ = cur.adbc_statement.execute_query()
            result = pa.RecordBatchReader._import_from_c(handle.address).read_all()
        self.assertEqual(result.column("s").to_pylist(), ["\\N", None, "x\ty"])

    def test_param_temporal_and_decimal(self):
        import datetime
        import decimal

        d = datetime.date(2024, 5, 17)
        dt = datetime.datetime(2024, 5, 17, 12, 34, 56, 789000)
        dec = decimal.Decimal("12345.67")
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ? AS d, ? AS dt, ? AS dec", (d, dt, dec))
            row = cur.fetchone()
        self.assertEqual(row[0], d)
        # DateTime64 carries the server timezone; the wall-clock value of a
        # naive datetime parameter is preserved.
        self.assertEqual(row[1].replace(tzinfo=None), dt)
        self.assertEqual(row[2], dec)

    def test_param_binary_is_binary_safe(self):
        blob = b"\x00\x01\xff"
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT hex(?) AS h, length(?) AS l", (blob, blob))
            row = cur.fetchone()
        self.assertEqual(row[0], "0001FF")
        self.assertEqual(row[1], 3)

    def test_param_int64_boundaries(self):
        lo, hi = -(2**63), 2**63 - 1
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ? AS lo, ? AS hi", (lo, hi))
            self.assertEqual(cur.fetchone(), (lo, hi))

    def test_param_unicode(self):
        s = "中文 🚀 '; -- $tag$"
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ? AS s", (s,))
            self.assertEqual(cur.fetchone()[0], s)

    def test_executemany_mixed_types_with_nulls(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE param_mixed (id Int64, v Nullable(Float64)) "
                "ENGINE = MergeTree() ORDER BY id"
            )
            cur.executemany(
                "INSERT INTO param_mixed VALUES (?, ?)",
                [(1, 1.5), (2, None), (3, 2.5)],
            )
            cur.execute(
                "SELECT count(), sum(v), countIf(v IS NULL) FROM param_mixed"
            )
            self.assertEqual(cur.fetchone(), (3, 4.0, 1))

    def test_parameterized_large_result_streams(self):
        # Parameterized SELECTs go through the streaming Arrow path too:
        # a large result arrives as multiple record batches.
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT number + ? FROM numbers(1000000)", (1,))
            reader = cur.fetch_record_batch()
            batches = list(reader)
        self.assertGreater(len(batches), 1)
        self.assertEqual(sum(b.num_rows for b in batches), 1000000)

    def test_rowcount_distinguishes_zero_from_unknown(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE rc (x Int64) ENGINE = Memory")
            self.assertEqual(cur.rowcount, -1)  # DDL: not applicable
            cur.execute("INSERT INTO rc SELECT number FROM numbers(5)")
            self.assertEqual(cur.rowcount, 5)
            cur.execute("INSERT INTO rc SELECT number FROM numbers(9) WHERE 0")
            self.assertEqual(cur.rowcount, 0)   # a known zero, not unknown
            cur.executemany("INSERT INTO rc VALUES (?)", [(7,), (8,)])
            self.assertEqual(cur.rowcount, 2)

    def test_executemany_fallback_zero_rows_is_known(self):
        # INSERT ... SELECT ? WHERE 0 is not the plain-VALUES shape, so it
        # takes the per-row fallback; every execution writes zero rows and
        # the total must be a known 0, not "unknown" (-1).
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE rc_zero (x Int64) ENGINE = Memory")
            cur.executemany(
                "INSERT INTO rc_zero SELECT ? WHERE 0", [(1,), (2,), (3,)]
            )
            self.assertEqual(cur.rowcount, 0)
            cur.execute("SELECT count() FROM rc_zero")
            self.assertEqual(cur.fetchone()[0], 0)

    def test_heredoc_string_with_question_mark(self):
        # Heredoc strings are tokenized by the engine lexer: a `?` inside
        # one is literal text, not a placeholder.
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT $$a?b$$ AS s, ? AS v", (5,))
            self.assertEqual(cur.fetchone(), ("a?b", 5))

    def test_executemany_large_batch(self):
        # The plain INSERT ... VALUES (?, ...) shape streams all bound rows
        # through one statement.
        rows = [(i, float(i) / 2) for i in range(5000)]
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE many_fast (id Int64, v Float64) "
                "ENGINE = MergeTree() ORDER BY id"
            )
            cur.executemany("INSERT INTO many_fast (id, v) VALUES (?, ?)", rows)
            cur.execute("SELECT count(), sum(id) FROM many_fast")
            self.assertEqual(cur.fetchone(), (5000, sum(r[0] for r in rows)))

    def test_executemany_expression_falls_back(self):
        # A constant mixed into VALUES doesn't match the fast-path shape;
        # the per-row path must produce the same result.
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE many_slow (id Int64, tag Int64) "
                "ENGINE = MergeTree() ORDER BY id"
            )
            cur.executemany(
                "INSERT INTO many_slow VALUES (?, 7)", [(1,), (2,), (3,)]
            )
            cur.execute("SELECT count(), sum(tag) FROM many_slow")
            self.assertEqual(cur.fetchone(), (3, 21))

    def test_param_count_mismatch(self):
        with _connect() as conn, conn.cursor() as cur:
            with self.assertRaises(Exception) as ctx:
                cur.execute("SELECT ? + ?", (1,))
            self.assertIn("placeholder", str(ctx.exception))


def _expected_get_objects_schema():
    """The GetObjects result schema mandated by the ADBC spec, transcribed
    independently from the C++ definition so a typo in either side fails
    the equality assertion (adbc.h 'Connection Metadata' section)."""
    usage = pa.struct(
        [
            pa.field("fk_catalog", pa.utf8()),
            pa.field("fk_db_schema", pa.utf8()),
            pa.field("fk_table", pa.utf8(), nullable=False),
            pa.field("fk_column_name", pa.utf8(), nullable=False),
        ]
    )
    constraint = pa.struct(
        [
            pa.field("constraint_name", pa.utf8()),
            pa.field("constraint_type", pa.utf8(), nullable=False),
            pa.field("constraint_column_names", pa.list_(pa.utf8()), nullable=False),
            pa.field("constraint_column_usage", pa.list_(usage)),
        ]
    )
    column = pa.struct(
        [
            pa.field("column_name", pa.utf8(), nullable=False),
            pa.field("ordinal_position", pa.int32()),
            pa.field("remarks", pa.utf8()),
            pa.field("xdbc_data_type", pa.int16()),
            pa.field("xdbc_type_name", pa.utf8()),
            pa.field("xdbc_column_size", pa.int32()),
            pa.field("xdbc_decimal_digits", pa.int16()),
            pa.field("xdbc_num_prec_radix", pa.int16()),
            pa.field("xdbc_nullable", pa.int16()),
            pa.field("xdbc_column_def", pa.utf8()),
            pa.field("xdbc_sql_data_type", pa.int16()),
            pa.field("xdbc_datetime_sub", pa.int16()),
            pa.field("xdbc_char_octet_length", pa.int32()),
            pa.field("xdbc_is_nullable", pa.utf8()),
            pa.field("xdbc_scope_catalog", pa.utf8()),
            pa.field("xdbc_scope_schema", pa.utf8()),
            pa.field("xdbc_scope_table", pa.utf8()),
            pa.field("xdbc_is_autoincrement", pa.bool_()),
            pa.field("xdbc_is_generatedcolumn", pa.bool_()),
        ]
    )
    table = pa.struct(
        [
            pa.field("table_name", pa.utf8(), nullable=False),
            pa.field("table_type", pa.utf8(), nullable=False),
            pa.field("table_columns", pa.list_(column)),
            pa.field("table_constraints", pa.list_(constraint)),
        ]
    )
    db_schema = pa.struct(
        [
            pa.field("db_schema_name", pa.utf8()),
            pa.field("db_schema_tables", pa.list_(table)),
        ]
    )
    return pa.schema(
        [
            pa.field("catalog_name", pa.utf8()),
            pa.field("catalog_db_schemas", pa.list_(db_schema)),
        ]
    )


@unittest.skipUnless(_ENABLED, _SKIP_REASON)
class TestAdbcGetObjects(unittest.TestCase):
    def _objects(self, conn, **kwargs):
        return conn.adbc_get_objects(**kwargs).read_all().to_pylist()

    def test_schema_matches_spec(self):
        with _connect() as conn:
            got = conn.adbc_get_objects(depth="all").read_all().schema
        self.assertEqual(got, _expected_get_objects_schema())

    def test_depth_all_finds_table_and_columns(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE obj_probe (a Int64, b String) "
                "ENGINE = MergeTree() ORDER BY a"
            )
            catalogs = self._objects(conn, depth="all")
        self.assertEqual(len(catalogs), 1)
        schemas = {s["db_schema_name"]: s for s in catalogs[0]["catalog_db_schemas"]}
        self.assertIn("default", schemas)
        self.assertIn("system", schemas)
        tables = {t["table_name"]: t for t in schemas["default"]["db_schema_tables"]}
        self.assertIn("obj_probe", tables)
        self.assertEqual(tables["obj_probe"]["table_type"], "BASE TABLE")
        columns = {
            c["column_name"]: c for c in tables["obj_probe"]["table_columns"]
        }
        self.assertEqual(set(columns), {"a", "b"})
        self.assertEqual(columns["a"]["ordinal_position"], 1)
        self.assertEqual(columns["a"]["xdbc_type_name"], "Int64")

    def test_empty_db_schema_filter_matches_nothing(self):
        # Spec: an empty string selects objects *without* a database schema;
        # every table here has one, so the catalog row carries no schemas.
        with _connect() as conn:
            catalogs = conn.adbc_get_objects(
                depth="all", db_schema_filter=""
            ).read_all().to_pylist()
        self.assertEqual(len(catalogs), 1)
        self.assertEqual(catalogs[0]["catalog_db_schemas"], [])

    def test_nonmatching_catalog_filter_yields_zero_rows(self):
        with _connect() as conn:
            table = conn.adbc_get_objects(
                depth="all", catalog_filter="no_such_catalog"
            ).read_all()
        self.assertEqual(table.num_rows, 0)

    def test_depth_db_schemas_has_null_tables(self):
        with _connect() as conn:
            catalogs = self._objects(conn, depth="db_schemas")
        schemas = catalogs[0]["catalog_db_schemas"]
        self.assertTrue(len(schemas) >= 1)
        self.assertIsNone(schemas[0]["db_schema_tables"])

    def test_table_name_filter(self):
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE filter_me (x Int64) ENGINE = MergeTree() ORDER BY x"
            )
            catalogs = self._objects(
                conn, depth="all", table_name_filter="filter_%"
            )
        schemas = {s["db_schema_name"]: s for s in catalogs[0]["catalog_db_schemas"]}
        names = [t["table_name"] for t in schemas["default"]["db_schema_tables"]]
        self.assertEqual(names, ["filter_me"])

    def test_view_table_type(self):
        # Note: the table-type *filter* (GetObjects' table_type argument) is
        # implemented in the driver but cannot be exercised from Python —
        # adbc_driver_manager's bindings pass NULL for it ("TODO: support
        # table_types" in _lib.pyx). Assert the classification instead.
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE view_base (x Int64) ENGINE = MergeTree() ORDER BY x"
            )
            cur.execute("CREATE VIEW view_probe AS SELECT * FROM view_base")
            catalogs = self._objects(conn, depth="all")
        schemas = {s["db_schema_name"]: s for s in catalogs[0]["catalog_db_schemas"]}
        tables = {t["table_name"]: t for t in schemas["default"]["db_schema_tables"]}
        self.assertEqual(tables["view_probe"]["table_type"], "VIEW")
        self.assertEqual(tables["view_base"]["table_type"], "BASE TABLE")


@unittest.skipUnless(_ENABLED, _SKIP_REASON)
class TestAdbcMetadata(unittest.TestCase):
    def test_get_info(self):
        with _connect() as conn:
            info = conn.adbc_get_info()
        self.assertEqual(info["vendor_name"], "ClickHouse")
        self.assertEqual(info["driver_name"], "ADBC chDB Driver")
        self.assertTrue(info["vendor_version"])
        # driver_version is semver-shaped (at most three components), unlike
        # the four-component vendor_version.
        self.assertRegex(info["driver_version"], r"^\d+(\.\d+){0,2}$")
        self.assertRegex(info["driver_arrow_version"], r"^v\d+")
        # Capability-probe fields for upper layers. The driver manager maps
        # some codes to friendly names and leaves others as the numeric code
        # (3 = VENDOR_SQL, 4 = VENDOR_SUBSTRAIT).
        self.assertRegex(info["vendor_arrow_version"], r"^v\d+")
        self.assertIs(info[3], True)
        self.assertIs(info[4], False)

    def test_current_db_schema_option(self):
        with _connect() as conn:
            self.assertEqual(conn.adbc_current_db_schema, "default")

    def test_get_table_schema_missing_table_is_not_found(self):
        from adbc_driver_manager import AdbcStatusCode

        with _connect() as conn:
            with self.assertRaises(Exception) as ctx:
                conn.adbc_get_table_schema("definitely_missing_table")
        self.assertEqual(ctx.exception.status_code, AdbcStatusCode.NOT_FOUND)

    def test_streaming_query_error_maps_status_code(self):
        # Errors on the streaming SELECT path get the same ADBC status codes as
        # the non-streaming paths (was a blind INTERNAL before): a missing table
        # is NOT_FOUND, a syntax/parse error is INVALID_ARGUMENT.
        from adbc_driver_manager import AdbcStatusCode

        with _connect() as conn:
            for sql, want in [
                ("SELECT * FROM definitely_missing_table", AdbcStatusCode.NOT_FOUND),
                ("SELEC 1", AdbcStatusCode.INVALID_ARGUMENT),
                ("SELECT cast('abc' AS Int64)", AdbcStatusCode.INVALID_ARGUMENT),
            ]:
                with conn.cursor() as cur:
                    with self.assertRaises(Exception) as ctx:
                        cur.execute(sql)
                        cur.fetchall()
                    self.assertEqual(ctx.exception.status_code, want, sql)

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
class TestAdbcUri(unittest.TestCase):
    def _connect_uri(self, uri):
        return adbc_dbapi.connect(
            driver=_LIBCHDB_PATH,
            entrypoint="chdb_adbc_init",
            db_kwargs={"uri": uri},
            autocommit=True,
        )

    def _roundtrip(self, uri, expect_dir):
        with self._connect_uri(uri) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
        self.assertTrue(os.path.isdir(expect_dir), expect_dir)

    def test_file_uri_forms(self):
        base = tempfile.mkdtemp(prefix="chdb_uri_")
        try:
            self._roundtrip(f"file:{base}/u1", f"{base}/u1")
            self._roundtrip(f"file://{base}/u2", f"{base}/u2")
            self._roundtrip(f"file://localhost{base}/u3", f"{base}/u3")
            self._roundtrip(f"file:{base}/my%20db", f"{base}/my db")
        finally:
            shutil.rmtree(base, ignore_errors=True)

    def test_non_file_scheme_rejected(self):
        with self.assertRaises(Exception) as ctx:
            self._connect_uri("http://example.com/db")
        self.assertIn("scheme", str(ctx.exception).lower())


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
