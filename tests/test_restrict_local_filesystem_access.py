#!/usr/bin/env python3
"""The `restrict_local_filesystem_access` connection option (chdb-core #345).

A connection opened with the option must not let SQL read, list, write or execute anything on the local
filesystem, by any spelling, while the engine's own I/O keeps working: table metadata, temporary files for
spilling, and the credential files the S3 client loads for an authorized query. Connections opened without it
behave as before, including in the same process as a restricted one.

Runs against libchdb through ctypes, the way C hosts such as pg_chdb use it. The scenarios that need a fresh
process (a persistent --path, AWS credential environment variables) run this file again as a child.
"""

import ctypes
import hashlib
import http.server
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.parse

OPTION = b"--restrict_local_filesystem_access"
RESTRICTED_USER = "chdb_restricted_local_filesystem"
SECRET = "TOP_SECRET_6f1d2c"
DENIED = r"ACCESS_DENIED|Not enough privileges|not allowed"


def _find_libchdb():
    candidates = []
    if os.environ.get("CHDB_LIB_PATH"):
        candidates.append(os.environ["CHDB_LIB_PATH"])
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    lib_name = "libchdb.dylib" if platform.system() == "Darwin" else "libchdb.so"
    candidates += [os.path.join(repo_root, "buildlib", lib_name), os.path.join(repo_root, "libchdb.so")]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


_LIB_PATH = _find_libchdb()


class LocalResultV2(ctypes.Structure):
    _fields_ = [
        ("buf", ctypes.c_void_p),
        ("len", ctypes.c_size_t),
        ("_vec", ctypes.c_void_p),
        ("elapsed", ctypes.c_double),
        ("rows_read", ctypes.c_uint64),
        ("bytes_read", ctypes.c_uint64),
        ("error_message", ctypes.c_char_p),
    ]


class Chdb:
    """A thin ctypes binding of the chdb C API."""

    def __init__(self, path):
        lib = ctypes.CDLL(path)
        lib.chdb_connect.restype = ctypes.c_void_p
        lib.chdb_connect.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        lib.chdb_close_conn.restype = None
        lib.chdb_close_conn.argtypes = [ctypes.c_void_p]
        lib.chdb_query.restype = ctypes.c_void_p
        lib.chdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        lib.chdb_query_with_params.restype = ctypes.c_void_p
        lib.chdb_query_with_params.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_size_t,
        ]
        lib.chdb_stream_query.restype = ctypes.c_void_p
        lib.chdb_stream_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        lib.chdb_stream_fetch_result.restype = ctypes.c_void_p
        lib.chdb_stream_fetch_result.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        lib.chdb_stream_cancel_query.restype = None
        lib.chdb_stream_cancel_query.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        lib.chdb_result_buffer.restype = ctypes.POINTER(ctypes.c_char)
        lib.chdb_result_buffer.argtypes = [ctypes.c_void_p]
        lib.chdb_result_length.restype = ctypes.c_size_t
        lib.chdb_result_length.argtypes = [ctypes.c_void_p]
        lib.chdb_result_error.restype = ctypes.c_char_p
        lib.chdb_result_error.argtypes = [ctypes.c_void_p]
        lib.chdb_destroy_query_result.restype = None
        lib.chdb_destroy_query_result.argtypes = [ctypes.c_void_p]
        lib.chdb_set.restype = ctypes.c_int
        lib.chdb_set.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        lib.chdb_get.restype = ctypes.c_char_p
        lib.chdb_get.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        lib.query_stable_v2.restype = ctypes.POINTER(LocalResultV2)
        lib.query_stable_v2.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
        lib.free_result_v2.restype = None
        lib.free_result_v2.argtypes = [ctypes.POINTER(LocalResultV2)]
        self.lib = lib

    def connect(self, *args):
        argv = [b"clickhouse"] + list(args)
        c_argv = (ctypes.c_char_p * len(argv))(*argv)
        conn_ptr = self.lib.chdb_connect(len(argv), c_argv)
        return Connection(self, conn_ptr) if conn_ptr else None

    def query_stable_v2(self, *args):
        argv = [b"clickhouse"] + list(args)
        c_argv = (ctypes.c_char_p * len(argv))(*argv)
        res = self.lib.query_stable_v2(len(argv), c_argv)
        try:
            if res.contents.error_message:
                return res.contents.error_message.decode(errors="replace"), ""
            return None, ctypes.string_at(res.contents.buf, res.contents.len).decode(errors="replace")
        finally:
            self.lib.free_result_v2(res)


class Connection:
    def __init__(self, chdb, conn_ptr):
        self.lib = chdb.lib
        self.conn_ptr = conn_ptr
        self.conn = ctypes.c_void_p.from_address(conn_ptr).value

    def close(self):
        if self.conn_ptr:
            self.lib.chdb_close_conn(self.conn_ptr)
            self.conn_ptr = None

    def _take(self, result):
        """(error, body) of a chdb_result, which it destroys."""
        try:
            err = self.lib.chdb_result_error(result)
            if err:
                return err.decode(errors="replace"), ""
            n = self.lib.chdb_result_length(result)
            buf = self.lib.chdb_result_buffer(result)
            return None, ctypes.string_at(buf, n).decode(errors="replace") if buf and n else ""
        finally:
            self.lib.chdb_destroy_query_result(result)

    def query(self, sql, fmt="TSV"):
        return self._take(self.lib.chdb_query(self.conn, sql.encode(), fmt.encode()))

    def query_with_params(self, sql, params, fmt="TSV"):
        names = [k.encode() for k in params]
        values = [v.encode() for v in params.values()]
        c_names = (ctypes.c_char_p * len(names))(*names)
        c_values = (ctypes.c_char_p * len(values))(*values)
        return self._take(
            self.lib.chdb_query_with_params(self.conn, sql.encode(), fmt.encode(), c_names, c_values, len(names))
        )

    def stream_query(self, sql, fmt="TSV"):
        stream = self.lib.chdb_stream_query(self.conn, sql.encode(), fmt.encode())
        err = self.lib.chdb_result_error(stream)
        if err:
            self.lib.chdb_destroy_query_result(stream)
            return err.decode(errors="replace"), ""
        chunks = []
        try:
            while True:
                err, body = self._take(self.lib.chdb_stream_fetch_result(self.conn, stream))
                if err:
                    return err, "".join(chunks)
                if not body:
                    return None, "".join(chunks)
                chunks.append(body)
        finally:
            self.lib.chdb_stream_cancel_query(self.conn, stream)
            self.lib.chdb_destroy_query_result(stream)

    def set(self, key, value):
        return self.lib.chdb_set(self.conn, key.encode(), None if value is None else value.encode())

    def get(self, key):
        value = self.lib.chdb_get(self.conn, key.encode())
        return None if value is None else value.decode()


def _make_tree(root):
    """Files a restricted connection must not reach, and the spellings that lead to them."""
    os.makedirs(os.path.join(root, "sub"))
    secret = os.path.join(root, "secret.txt")
    with open(secret, "w") as f:
        f.write(SECRET + "\n")
    with open(os.path.join(root, "data.csv"), "w") as f:
        f.write("1\n2\n")
    os.symlink(secret, os.path.join(root, "sub", "link.txt"))
    return secret


@unittest.skipIf(_LIB_PATH is None, "libchdb not found; build chdb-core or set CHDB_LIB_PATH")
class TestRestrictLocalFilesystemAccess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.chdb = Chdb(_LIB_PATH)
        cls.dir = os.path.realpath(tempfile.mkdtemp(prefix="chdb_restrict_fs_"))
        cls.secret = _make_tree(cls.dir)
        # One engine per process: both connections share it, as concurrent connections of a host do.
        cls.plain = cls.chdb.connect()
        cls.restricted = cls.chdb.connect(OPTION + b"=1")
        if not cls.plain or not cls.restricted:
            raise RuntimeError("chdb_connect failed")

    @classmethod
    def tearDownClass(cls):
        cls.restricted.close()
        cls.plain.close()
        shutil.rmtree(cls.dir, ignore_errors=True)

    def path(self, *parts):
        return os.path.join(self.dir, *parts)

    def assertDenied(self, sql, pattern=DENIED, conn=None):
        err, body = (conn or self.restricted).query(sql)
        self.assertIsNotNone(err, f"not denied: {sql}\n{body}")
        self.assertRegex(err, pattern, sql)
        self.assertNotIn(SECRET, err + body)
        return err

    def assertAllowed(self, sql, conn=None):
        err, body = (conn or self.restricted).query(sql)
        self.assertIsNone(err, f"{sql}\n{err}")
        return body

    # --- reading, listing and writing files

    def test_file_table_function_spellings_are_denied(self):
        rel_to_cwd = os.path.relpath(self.secret)
        for target in [
            self.secret,
            rel_to_cwd,
            self.path("sub", "..", "secret.txt"),
            self.path("sub", "link.txt"),
            self.path("*.txt"),
            self.path("{secret,data}.*"),
            self.path("**", "*.txt"),
            self.path("missing.txt"),
        ]:
            with self.subTest(target=target):
                self.assertDenied(f"SELECT * FROM file('{target}', 'LineAsString')")
        self.assertDenied(f"SELECT * FROM fileCluster('default', '{self.secret}', 'LineAsString')", DENIED + "|CLUSTER_DOESNT_EXIST")
        self.assertDenied(f"DESCRIBE file('{self.secret}')")
        self.assertDenied(f"SELECT * FROM (SELECT * FROM file('{self.secret}', 'LineAsString'))")
        self.assertDenied(f"WITH t AS (SELECT * FROM file('{self.secret}', 'LineAsString')) SELECT * FROM t")
        self.assertDenied(f"SELECT * FROM view(SELECT * FROM file('{self.secret}', 'LineAsString'))")
        self.assertDenied(f"SELECT * FROM numbers(1) WHERE '{SECRET}' IN (SELECT * FROM file('{self.secret}', 'LineAsString'))")

    def test_scalar_file_function_is_denied(self):
        self.assertDenied(f"SELECT file('{self.secret}')")
        self.assertDenied(f"SELECT file(concat('{self.dir}', '/secret.txt'))")

    def test_url_with_file_scheme_is_denied(self):
        for url in [f"file://{self.secret}", f"FILE://{self.secret}", f"file://{self.path('sub', '..', 'secret.txt')}"]:
            with self.subTest(url=url):
                self.assertDenied(f"SELECT * FROM url('{url}', 'LineAsString')")
        self.assertDenied(f"SELECT * FROM url('secret.txt', 'LineAsString') SETTINGS url_base = 'file://{self.dir}/'")
        for function in ["s3", "url", "azureBlobStorage", "hdfs"]:
            with self.subTest(function=function):
                self.assertDenied(f"SELECT * FROM {function}('file://{self.secret}', 'LineAsString')", ".")
        unknown = DENIED + "|UNKNOWN_FUNCTION|Unknown table function"
        self.assertDenied("SELECT * FROM odbc('DSN=x', 't')", unknown)
        self.assertDenied("SELECT * FROM jdbc('jdbc:sqlite:x.db', 't')", unknown)

    def test_filesystem_listing_is_denied(self):
        self.assertDenied(f"SELECT name FROM filesystem('{self.dir}')")
        self.assertDenied(f"SELECT content FROM filesystem('{self.dir}') WHERE name = 'secret.txt'")
        self.assertDenied("SELECT count() FROM filesystem('/')")
        self.assertDenied(f"CREATE DATABASE fs_db ENGINE = Filesystem('{self.dir}')")

    def test_writes_are_denied_and_create_nothing(self):
        out = self.path("out_insert.csv")
        self.assertDenied(f"INSERT INTO FUNCTION file('{out}', 'CSV', 'x UInt8') SELECT 1")
        self.assertFalse(os.path.exists(out))
        out = self.path("out_into_outfile.csv")
        self.assertDenied(f"SELECT 1 INTO OUTFILE '{out}' FORMAT CSV")
        self.assertDenied(f"SELECT 1 INTO OUTFILE '{out}' APPEND FORMAT CSV")
        self.assertDenied(f"SELECT 1 INTO OUTFILE '{out}' TRUNCATE FORMAT CSV")
        self.assertFalse(os.path.exists(out))
        # An existing file is not overwritten either.
        self.assertDenied(f"INSERT INTO FUNCTION file('{self.secret}', 'LineAsString') SELECT 'overwritten'")
        self.assertDenied(f"SELECT 'overwritten' INTO OUTFILE '{self.secret}' TRUNCATE FORMAT LineAsString")
        with open(self.secret) as f:
            self.assertEqual(f.read(), SECRET + "\n")

    def test_from_infile_is_denied(self):
        self.assertAllowed("CREATE TABLE IF NOT EXISTS infile_target (x UInt8) ENGINE = Memory")
        self.assertDenied(f"INSERT INTO infile_target FROM INFILE '{self.path('data.csv')}' FORMAT CSV")
        self.assertEqual(self.assertAllowed("SELECT count() FROM infile_target"), "0\n")

    def test_file_engines_and_descriptors_are_denied(self):
        self.assertDenied(f"CREATE TABLE file_table (s String) ENGINE = File(LineAsString, '{self.secret}')")
        self.assertDenied("CREATE TABLE fd_table (s String) ENGINE = File(LineAsString, stdin)")
        self.assertDenied("SELECT * FROM file(stdin, 'LineAsString', 's String')")
        self.assertDenied(f"CREATE TABLE url_table (s String) ENGINE = URL('file://{self.secret}', LineAsString)")
        self.assertDenied(f"CREATE TEMPORARY TABLE tmp_file (s String) ENGINE = File(LineAsString, '{self.secret}')")
        self.assertDenied(f"CREATE TABLE as_file AS file('{self.secret}', 'LineAsString')")
        self.assertDenied(f"ATTACH TABLE attached FROM '{self.path('sub')}' (x UInt8) ENGINE = MergeTree ORDER BY x")
        self.assertDenied(
            f"CREATE TABLE rocks (k UInt8, v String) ENGINE = EmbeddedRocksDB(0, '{self.path('rocks')}') PRIMARY KEY k"
        )
        self.assertFalse(os.path.exists(self.path("rocks")))
        self.assertDenied(f"CREATE TABLE file_log (s String) ENGINE = FileLog('{self.dir}', 'LineAsString')")
        self.assertDenied(f"SELECT * FROM file('{self.path('archive.zip')} :: secret.txt', 'LineAsString')")

    def test_sqlite_and_local_data_lakes_are_denied(self):
        self.assertDenied(f"SELECT * FROM sqlite('{self.path('x.db')}', 't')")
        self.assertDenied(f"CREATE DATABASE lite ENGINE = SQLite('{self.path('x.db')}')")
        self.assertFalse(os.path.exists(self.path("x.db")))
        for function in ["iceberg", "hudi", "paimon", "deltaLake"]:
            with self.subTest(function=function):
                self.assertDenied(f"SELECT * FROM {function}('file://{self.dir}')", ".")
        for function in ["deltaLakeLocal", "icebergLocal"]:
            with self.subTest(function=function):
                self.assertDenied(f"SELECT * FROM {function}('{self.dir}')", DENIED + "|Unknown table function")

    def test_schema_and_template_files_are_confined(self):
        prohibited = r"prohibited|shouldn't go outside|ACCESS_DENIED"
        self.assertDenied(
            f"SELECT 1 AS x FORMAT Template SETTINGS format_template_resultset = '{self.secret}'", prohibited
        )
        self.assertDenied(
            "SELECT 1 AS x FORMAT Template SETTINGS format_template_resultset = '../../../../../../../etc/passwd'",
            prohibited,
        )
        self.assertDenied(
            f"SELECT 1 AS x FORMAT Protobuf SETTINGS format_schema = '{self.path('secret.proto')}:M'", prohibited
        )
        self.assertDenied(
            f"SELECT 1 AS x FORMAT Protobuf SETTINGS output_format_schema = '{self.path('generated.proto')}',"
            " format_protobuf_use_autogenerated_schema = 1",
            prohibited,
        )
        self.assertFalse(os.path.exists(self.path("generated.proto")))

    # --- programs, dictionaries, disks, backups

    def test_program_execution_is_denied(self):
        self.assertDenied("SELECT * FROM executable('cat', 'TSV', 'x String', (SELECT 'a'))")
        self.assertDenied("CREATE TABLE exe (x String) ENGINE = Executable('cat', TSV)")
        self.assertDenied("CREATE TABLE exe_pool (x String) ENGINE = ExecutablePool('cat', TSV)")
        self.assertDenied("SYSTEM RELOAD FUNCTIONS")
        self.assertDenied("SYSTEM RELOAD CONFIG", DENIED + "|not supported in clickhouse-local")

    def test_dictionaries_on_files_or_programs_are_denied(self):
        attrs = "(id UInt64, v String) PRIMARY KEY id"
        for i, (source, layout) in enumerate(
            [
                (f"FILE(PATH '{self.secret}' FORMAT 'TSV')", "FLAT()"),
                (f"file(path '{self.secret}' format 'TSV')", "FLAT()"),
                ("EXECUTABLE(COMMAND 'cat' FORMAT 'TSV')", "FLAT()"),
                ("EXECUTABLE_POOL(COMMAND 'cat' FORMAT 'TSV')", "FLAT()"),
                ("ODBC(CONNECTION_STRING 'DSN=x' TABLE 't')", "FLAT()"),
                (f"YAMLRegExpTree(PATH '{self.secret}')", "REGEXP_TREE"),
                ("CLICKHOUSE(QUERY 'SELECT 1 AS id, \\'a\\' AS v')",
                 f"SSD_CACHE(PATH '{self.path('ssd')}' BLOCK_SIZE 4096 FILE_SIZE 16777216)"),
            ]
        ):
            with self.subTest(source=source, layout=layout):
                self.assertDenied(
                    f"CREATE DICTIONARY denied_{i} {attrs} SOURCE({source}) LAYOUT({layout}) LIFETIME(0)",
                    DENIED + "|Unknown dictionary source type|UNKNOWN_ELEMENT_IN_CONFIG",
                )
        self.assertFalse(os.path.exists(self.path("ssd")))

    def test_dictionaries_on_tables_keep_working(self):
        self.assertAllowed("CREATE TABLE IF NOT EXISTS dict_source (id UInt64, v String) ENGINE = MergeTree ORDER BY id")
        self.assertAllowed("INSERT INTO dict_source VALUES (1, 'one')")
        self.assertAllowed(
            "CREATE DICTIONARY IF NOT EXISTS table_dict (id UInt64, v String) PRIMARY KEY id"
            " SOURCE(CLICKHOUSE(TABLE 'dict_source')) LAYOUT(FLAT()) LIFETIME(0)"
        )
        self.assertEqual(self.assertAllowed("SELECT dictGet('table_dict', 'v', toUInt64(1))"), "one\n")

    def test_clickhouse_dictionary_queries_run_as_the_connection(self):
        attrs = "(id UInt64, v String) PRIMARY KEY id"
        for i, query in enumerate(
            [
                f"SELECT 1 AS id, file(''{self.secret}'') AS v",
                f"SELECT 1 AS id, line AS v FROM file(''{self.secret}'', ''LineAsString'', ''line String'')",
            ]
        ):
            with self.subTest(query=query):
                err, _ = self.restricted.query(
                    f"CREATE DICTIONARY query_dict_{i} {attrs} SOURCE(CLICKHOUSE(QUERY '{query}')) LAYOUT(FLAT()) LIFETIME(0)"
                )
                if err is None:
                    self.assertDenied(f"SELECT dictGet('query_dict_{i}', 'v', toUInt64(1))")
        # USER names a remote server's user; an in-process query runs as the connection's user whatever it says.
        self.assertAllowed("CREATE TABLE IF NOT EXISTS dict_source (id UInt64, v String) ENGINE = MergeTree ORDER BY id")
        self.assertAllowed("INSERT INTO dict_source VALUES (2, 'two')")
        self.assertAllowed(
            f"CREATE DICTIONARY user_default_dict {attrs} SOURCE(CLICKHOUSE(TABLE 'dict_source' USER 'default'))"
            " LAYOUT(FLAT()) LIFETIME(0)"
        )
        self.assertEqual(self.assertAllowed("SELECT dictGet('user_default_dict', 'v', toUInt64(2))"), "two\n")
        err, _ = self.restricted.query(
            f"CREATE DICTIONARY user_default_file_dict {attrs}"
            f" SOURCE(CLICKHOUSE(QUERY 'SELECT 1 AS id, file(''{self.secret}'') AS v' USER 'default')) LAYOUT(FLAT()) LIFETIME(0)"
        )
        if err is None:
            self.assertDenied("SELECT dictGet('user_default_file_dict', 'v', toUInt64(1))")
        self.assertDenied(
            f"CREATE DICTIONARY local_user_dict {attrs} SOURCE(CLICKHOUSE(TABLE 'dict_source' LOCAL_USER 'default'))"
            " LAYOUT(FLAT()) LIFETIME(0)"
        )
        # A remote server keeps the user it is given.
        self.assertAllowed(
            f"CREATE DICTIONARY remote_dict {attrs}"
            " SOURCE(CLICKHOUSE(HOST '192.0.2.1' PORT 9000 USER 'remote_reader' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)"
        )
        err, ddl = self.restricted.query("SHOW CREATE DICTIONARY remote_dict", "RawBLOB")
        self.assertIsNone(err, err)
        self.assertIn("USER 'remote_reader'", ddl)
        self.assertIn(f"LOCAL_USER '{RESTRICTED_USER}'", ddl)

    def test_custom_disks_are_denied(self):
        disk = self.path("disk") + "/"
        self.assertDenied(
            "CREATE TABLE on_disk (x UInt8) ENGINE = MergeTree ORDER BY x"
            f" SETTINGS disk = disk(type = 'local', path = '{disk}')"
        )
        self.assertAllowed("CREATE TABLE IF NOT EXISTS plain_disk (x UInt8) ENGINE = MergeTree ORDER BY x")
        self.assertDenied(
            f"ALTER TABLE plain_disk MODIFY SETTING disk = disk(type = 'local', path = '{disk}')", DENIED + "|SETTING"
        )
        self.assertDenied(f"CREATE DATABASE on_disk_db ENGINE = Atomic SETTINGS disk = disk(type = 'local', path = '{disk}')")
        self.assertFalse(os.path.exists(disk))

    def test_local_backups_are_denied(self):
        self.assertAllowed("CREATE TABLE IF NOT EXISTS backup_source (x UInt8) ENGINE = MergeTree ORDER BY x")
        for sql in [
            f"BACKUP TABLE backup_source TO File('{self.path('backup')}')",
            "BACKUP TABLE backup_source TO Disk('default', 'b1')",
            f"RESTORE TABLE backup_source AS restored FROM File('{self.path('backup')}')",
            "RESTORE TABLE backup_source AS restored FROM Disk('default', 'b1')",
            f"CREATE DATABASE from_backup ENGINE = Backup('default', File('{self.path('backup')}'))",
        ]:
            with self.subTest(sql=sql):
                self.assertDenied(sql)
        self.assertFalse(os.path.exists(self.path("backup")))

    # --- the restriction cannot be lifted from SQL

    def test_sql_cannot_lift_the_restriction(self):
        self.assertDenied(f"GRANT READ, WRITE ON FILE TO {RESTRICTED_USER}")
        self.assertDenied("GRANT ALL ON *.* TO default", DENIED + "|ACCESS_MANAGEMENT|GRANT")
        self.assertDenied(f"SELECT * FROM file('{self.secret}', 'LineAsString') SETTINGS readonly = 0")
        self.assertDenied(
            f"CREATE VIEW borrowed DEFINER = default SQL SECURITY DEFINER AS SELECT * FROM file('{self.secret}', 'LineAsString')"
        )
        self.assertAllowed("CREATE TABLE IF NOT EXISTS mv_source (x UInt8) ENGINE = MergeTree ORDER BY x")
        read_secret = f"SELECT s FROM file('{self.secret}', 'LineAsString' , 's String'), mv_source"
        for mv in [
            f"CREATE MATERIALIZED VIEW mv_none ENGINE = Memory SQL SECURITY NONE AS {read_secret}",
            f"CREATE MATERIALIZED VIEW mv_default ENGINE = Memory DEFINER = default SQL SECURITY DEFINER AS {read_secret}",
            f"CREATE MATERIALIZED VIEW mv_own ENGINE = Memory AS {read_secret}",
            f"CREATE MATERIALIZED VIEW mv_refresh REFRESH EVERY 1 SECOND ENGINE = Memory AS {read_secret}",
        ]:
            with self.subTest(mv=mv):
                self.assertDenied(mv)
        self.assertAllowed("SET default_view_definer = 'default'")
        try:
            self.assertDenied(
                f"CREATE VIEW borrowed2 SQL SECURITY DEFINER AS SELECT * FROM file('{self.secret}', 'LineAsString')"
            )
        finally:
            self.assertAllowed("SET default_view_definer = 'CURRENT_USER'")
        self.assertDenied(f"EXECUTE AS default SELECT * FROM file('{self.secret}', 'LineAsString')", DENIED + "|IMPERSONATE")
        self.assertDenied("CREATE USER escalated", DENIED + "|ACCESS_MANAGEMENT")
        for sql in ["SET restrict_local_filesystem_access = 0", "SELECT 1 SETTINGS restrict_local_filesystem_access = 0"]:
            with self.subTest(sql=sql):
                self.assertIsNotNone(self.restricted.query(sql)[0])
        self.assertNotEqual(self.restricted.set("restrict_local_filesystem_access", "0"), 0)
        self.assertEqual(self.restricted.get("restrict_local_filesystem_access"), "1")
        self.assertDenied(f"SELECT file('{self.secret}')")

    def test_views_of_unrestricted_connections_run_as_the_reader(self):
        # SQL SECURITY INVOKER is the default for plain views, so reading through one needs the reader's grants.
        self.assertAllowed(
            f"CREATE VIEW IF NOT EXISTS secret_view AS SELECT * FROM file('{self.secret}', 'LineAsString')", self.plain
        )
        self.assertIn(SECRET, self.assertAllowed("SELECT * FROM secret_view", self.plain))
        self.assertDenied("SELECT * FROM secret_view")

    def test_every_query_entrypoint_is_restricted(self):
        err, body = self.restricted.query_with_params("SELECT * FROM file({p:String}, 'LineAsString')", {"p": self.secret})
        self.assertIsNotNone(err)
        self.assertRegex(err, DENIED)
        self.assertNotIn(SECRET, body)
        err, body = self.restricted.stream_query(f"SELECT * FROM file('{self.secret}', 'LineAsString')")
        self.assertIsNotNone(err)
        self.assertRegex(err, DENIED)
        self.assertNotIn(SECRET, body)
        self.assertEqual(self.restricted.set("param_p", self.secret), 0)
        self.assertDenied("SELECT * FROM file({p:String}, 'LineAsString')")

    # --- what keeps working on a restricted connection

    def test_engine_managed_storage_keeps_working(self):
        self.assertEqual(self.assertAllowed("SELECT currentUser()"), RESTRICTED_USER + "\n")
        self.assertAllowed("CREATE TABLE IF NOT EXISTS kept (x UInt64) ENGINE = MergeTree ORDER BY x")
        self.assertAllowed("INSERT INTO kept SELECT number FROM numbers(100000)")
        self.assertEqual(self.assertAllowed("SELECT count(), sum(x) FROM kept"), "100000\t4999950000\n")
        # Sorting and aggregation spill to engine-managed temporary files.
        self.assertEqual(
            self.assertAllowed(
                "SELECT sum(c) FROM (SELECT x % 50000 AS k, count() AS c FROM kept GROUP BY k ORDER BY k)"
                " SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0,"
                " max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0"
            ),
            "100000\n",
        )
        self.assertAllowed("CREATE TABLE IF NOT EXISTS kept_log (x UInt8) ENGINE = Log")
        self.assertAllowed("INSERT INTO kept_log VALUES (1)")
        self.assertAllowed("CREATE TEMPORARY TABLE IF NOT EXISTS kept_tmp (x UInt8)")
        self.assertAllowed("CREATE FUNCTION IF NOT EXISTS plus_one AS (x) -> x + 1")
        self.assertEqual(self.assertAllowed("SELECT plus_one(1)"), "2\n")
        self.assertEqual(self.assertAllowed("SELECT count() FROM format(CSV, 'x UInt8', '1\n2\n')"), "2\n")
        self.assertAllowed("SELECT count() FROM system.tables")
        self.assertAllowed("CREATE VIEW IF NOT EXISTS own_view AS SELECT 1")
        self.assertAllowed("SELECT * FROM own_view")
        self.assertAllowed("SET max_threads = 2")

    # --- connections without the option are unchanged

    def test_unrestricted_connection_keeps_its_behavior(self):
        self.assertEqual(self.plain.get("restrict_local_filesystem_access"), "0")
        self.assertEqual(self.assertAllowed("SELECT currentUser()", self.plain), "default\n")
        self.assertIn(SECRET, self.assertAllowed(f"SELECT * FROM file('{self.secret}', 'LineAsString')", self.plain))
        self.assertIn(SECRET, self.assertAllowed(f"SELECT file('{self.secret}')", self.plain))
        self.assertIn(SECRET, self.assertAllowed(f"SELECT * FROM url('file://{self.secret}', 'LineAsString')", self.plain))
        self.assertIn("secret.txt", self.assertAllowed(f"SELECT name FROM filesystem('{self.dir}')", self.plain))
        out = self.path("plain_out.csv")
        self.assertAllowed(f"SELECT 7 INTO OUTFILE '{out}' FORMAT CSV", self.plain)
        with open(out) as f:
            self.assertEqual(f.read(), "7\n")
        self.assertAllowed("CREATE TABLE IF NOT EXISTS plain_infile (x UInt8) ENGINE = Memory", self.plain)
        self.assertAllowed(f"INSERT INTO plain_infile FROM INFILE '{self.path('data.csv')}' FORMAT CSV", self.plain)
        self.assertEqual(self.assertAllowed("SELECT sum(x) FROM plain_infile", self.plain), "3\n")
        template = self.path("resultset.tpl")
        with open(template, "w") as f:
            f.write("rows: ${data}")
        with open(self.path("row.tpl"), "w") as f:
            f.write("<${x:Escaped}>")
        self.assertEqual(
            self.assertAllowed(
                "SELECT 1 AS x FORMAT Template SETTINGS"
                f" format_template_resultset = '{template}', format_template_row = '{self.path('row.tpl')}'",
                self.plain,
            ),
            "rows: <1>",
        )
        # The restricted connection is still restricted after the unrestricted one used files.
        self.assertDenied(f"SELECT file('{self.secret}')")

    def test_option_values(self):
        for value in [OPTION, OPTION + b"=true", OPTION + b"=TRUE"]:
            with self.subTest(value=value):
                conn = self.chdb.connect(value)
                self.assertIsNotNone(conn)
                try:
                    self.assertEqual(conn.get("restrict_local_filesystem_access"), "1")
                    self.assertDenied(f"SELECT file('{self.secret}')", conn=conn)
                finally:
                    conn.close()
        for value in [OPTION + b"=0", OPTION + b"=false"]:
            with self.subTest(value=value):
                conn = self.chdb.connect(value)
                self.assertIsNotNone(conn)
                try:
                    self.assertEqual(conn.get("restrict_local_filesystem_access"), "0")
                    self.assertIn(SECRET, self.assertAllowed(f"SELECT file('{self.secret}')", conn))
                finally:
                    conn.close()
        # The last occurrence wins, like for other connection arguments.
        conn = self.chdb.connect(OPTION + b"=0", OPTION + b"=1")
        try:
            self.assertEqual(conn.get("restrict_local_filesystem_access"), "1")
        finally:
            conn.close()
        for value in [OPTION + b"=yes", OPTION + b"=2", OPTION + b"="]:
            with self.subTest(value=value):
                self.assertIsNone(self.chdb.connect(value))

    def test_entrypoints_without_connections_reject_the_option(self):
        err, out = self.chdb.query_stable_v2(OPTION + b"=1", b"--query=SELECT 1")
        self.assertIsNotNone(err, out)


def _child_persistent(lib_path, db_path):
    """A restricted connection to a database on disk: create, close, reload and query it again."""
    chdb = Chdb(lib_path)
    results = {}
    conn = chdb.connect(b"--path=" + db_path.encode(), OPTION)
    results["create"] = conn.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")
    results["insert"] = conn.query("INSERT INTO t SELECT number FROM numbers(1000)")
    results["optimize"] = conn.query("OPTIMIZE TABLE t FINAL")
    conn.close()
    conn = chdb.connect(b"--path=" + db_path.encode(), OPTION)
    results["reload"] = conn.query("SELECT count(), sum(x) FROM t")
    results["file"] = conn.query(f"SELECT * FROM file('{db_path}/metadata/default/t.sql', 'LineAsString')")
    conn.close()
    return results


class _S3Handler(http.server.BaseHTTPRequestHandler):
    """A bucket holding one CSV object, which records how each request was signed."""

    body = b"1\n2\n3\n"
    authorizations = []

    def _reply(self, with_body):
        self.authorizations.append(self.headers.get("Authorization") or "")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(self.body)))
        self.send_header("ETag", '"0123456789abcdef"')
        self.send_header("Last-Modified", "Mon, 02 Nov 2026 00:00:00 GMT")
        self.end_headers()
        if with_body:
            self.wfile.write(self.body)

    def do_HEAD(self):  # noqa: N802
        self._reply(False)

    def do_GET(self):  # noqa: N802
        self._reply(True)

    def log_message(self, *args):
        pass


def _child_s3_credentials(lib_path, credentials_file):
    """pg_chdb's case: an authorized s3() query signs with the host's credential file, which SQL cannot read."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _S3Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}/bucket/data.csv"
    chdb = Chdb(lib_path)
    results = {}
    conn = chdb.connect(OPTION)
    results["s3"] = conn.query(f"SELECT sum(c1) FROM s3('{url}', 'CSV')")
    results["authorizations"] = list(_S3Handler.authorizations)
    results["nosign"] = conn.query(f"SELECT sum(c1) FROM s3('{url}', NOSIGN, 'CSV')")
    results["url"] = conn.query(f"SELECT sum(c1) FROM url('{url}', 'CSV')")
    results["file"] = conn.query(f"SELECT * FROM file('{credentials_file}', 'LineAsString')")
    results["scalar_file"] = conn.query(f"SELECT file('{credentials_file}')")
    conn.close()
    server.shutdown()
    return results


def _child_roles(lib_path, root, secret_file):
    """`default` reads files only through a role and has a row policy: the restricted user gets neither more nor less."""
    users = os.path.join(root, "users.xml")
    with open(users, "w") as f:
        f.write(
            "<clickhouse><profiles><default/></profiles><quotas><default/></quotas>"
            "<roles><file_reader><grants><query>GRANT READ, WRITE ON FILE</query></grants></file_reader></roles>"
            "<users><default><password></password><networks><ip>::/0</ip></networks>"
            "<profile>default</profile><quota>default</quota>"
            "<grants><query>GRANT SELECT, INSERT, CREATE, DROP, SHOW, TABLE ENGINE, SYSTEM, ACCESS MANAGEMENT ON *.*</query>"
            "<query>GRANT file_reader</query></grants>"
            "<databases><default><filtered><filter>x = 1</filter></filtered></default></databases>"
            "</default></users></clickhouse>"
        )
    config = os.path.join(root, "config.xml")
    with open(config, "w") as f:
        f.write(f"<clickhouse><users_config>{users}</users_config></clickhouse>")
    chdb = Chdb(lib_path)
    plain = chdb.connect(b"--config-file=" + config.encode())
    restricted = chdb.connect(b"--config-file=" + config.encode(), OPTION)
    read = f"SELECT * FROM file('{secret_file}', 'LineAsString')"
    results = {
        "plain_file": plain.query(read),
        "restricted_file": restricted.query(read),
        "restricted_roles": restricted.query("SELECT count() FROM system.current_roles"),
        "create": plain.query("CREATE TABLE filtered (x UInt8) ENGINE = MergeTree ORDER BY x"),
        "insert": plain.query("INSERT INTO filtered VALUES (1), (2)"),
        "plain_rows": plain.query("SELECT count() FROM filtered"),
        "restricted_rows": restricted.query("SELECT count() FROM filtered"),
        "set_role": restricted.query("SET ROLE file_reader"),
        "restricted_file_after_set_role": restricted.query(read),
        # A policy created later for everyone but `default` must not reach the restricted user either.
        "wide_policy": plain.query("CREATE ROW POLICY wide ON default.filtered USING 1 AS PERMISSIVE TO ALL EXCEPT default"),
        "restricted_rows_after_wide_policy": restricted.query("SELECT count() FROM filtered"),
        "restricted_execute_as": restricted.query(f"EXECUTE AS default {read}"),
        "restricted_create_policy": restricted.query("CREATE ROW POLICY own ON default.filtered USING 1 TO ALL"),
        "plain_no_role": plain.query("SET ROLE NONE"),
        "plain_outfile": plain.query(f"SELECT 1 INTO OUTFILE '{os.path.join(root, 'out.csv')}' FORMAT CSV"),
    }
    results["outfile_written"] = os.path.exists(os.path.join(root, "out.csv"))
    # Settings profiles bound to `default` and to its login role, with a constraint, bind the restricted user as well.
    results["profiles"] = [
        plain.query("SET ROLE DEFAULT"),
        plain.query("CREATE SETTINGS PROFILE for_default SETTINGS max_threads = 3, max_memory_usage = 1000000000 CONST TO default"),
        plain.query("CREATE SETTINGS PROFILE for_role SETTINGS max_execution_time = 77 TO file_reader"),
    ]
    settings = "SELECT getSetting('max_threads'), getSetting('max_execution_time'), getSetting('max_memory_usage')"
    fresh_plain = chdb.connect(b"--config-file=" + config.encode())
    fresh_restricted = chdb.connect(b"--config-file=" + config.encode(), OPTION)
    results["plain_settings"] = fresh_plain.query(settings)
    results["restricted_settings"] = fresh_restricted.query(settings)
    results["restricted_const"] = fresh_restricted.query("SET max_memory_usage = 5")
    fresh_restricted.close()
    fresh_plain.close()
    restricted.close()
    plain.close()
    return results


def _child_shutdown(lib_path):
    """SYSTEM SHUTDOWN and SYSTEM KILL signal the whole process group, so this runs in a session of its own."""
    chdb = Chdb(lib_path)
    conn = chdb.connect(OPTION)
    results = {sql: conn.query(sql) for sql in ["SYSTEM SHUTDOWN", "SYSTEM KILL", "SELECT 'alive'"]}
    conn.close()
    return results


class _BucketHandler(http.server.BaseHTTPRequestHandler):
    """An in-memory S3 bucket: enough of PUT, GET (with ranges), HEAD, DELETE and ListObjectsV2 for BACKUP/RESTORE."""

    objects = {}
    protocol_version = "HTTP/1.1"

    def _key(self):
        return urllib.parse.urlsplit(self.path).path

    def _send(self, code, body=b"", headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _read_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            data = b""
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    return data
                data += self.rfile.read(size)
                self.rfile.readline()
        return self.rfile.read(int(self.headers.get("Content-Length", 0)))

    def do_PUT(self):  # noqa: N802
        body = self._read_body()
        if self.headers.get("x-amz-content-sha256", "").startswith("STREAMING-"):
            raise RuntimeError("aws-chunked uploads are not supported by this mock")
        self.objects[self._key()] = body
        self._send(200, headers={"ETag": '"%s"' % hashlib.md5(body).hexdigest()})

    def do_HEAD(self):  # noqa: N802
        self.do_GET()

    def do_GET(self):  # noqa: N802
        parts = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parts.query)
        if "list-type" in query or parts.path.count("/") == 1 and not parts.path.endswith(".backup"):
            prefix = query.get("prefix", [""])[0]
            bucket = parts.path.rstrip("/")
            keys = sorted(k for k in self.objects if k.startswith(bucket + "/" + prefix))
            contents = "".join(
                f"<Contents><Key>{k[len(bucket) + 1:]}</Key><Size>{len(self.objects[k])}</Size>"
                f"<ETag>\"x\"</ETag><LastModified>2026-11-02T00:00:00.000Z</LastModified></Contents>"
                for k in keys
            )
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name>'
                f"<Prefix>{prefix}</Prefix><KeyCount>{len(keys)}</KeyCount><MaxKeys>1000</MaxKeys>"
                f"<IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            ).encode()
            return self._send(200, xml, {"Content-Type": "application/xml"})
        body = self.objects.get(parts.path)
        if body is None:
            xml = b'<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>no</Message></Error>'
            return self._send(404, xml, {"Content-Type": "application/xml"})
        headers = {"ETag": '"x"', "Last-Modified": "Mon, 02 Nov 2026 00:00:00 GMT", "Accept-Ranges": "bytes"}
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            start, _, end = rng[6:].partition("-")
            start = int(start)
            end = int(end) if end else len(body) - 1
            headers["Content-Range"] = f"bytes {start}-{end}/{len(body)}"
            return self._send(206, body[start : end + 1], headers)
        self._send(200, body, headers)

    def do_DELETE(self):  # noqa: N802
        self.objects.pop(self._key(), None)
        self._send(204)

    def do_POST(self):  # noqa: N802
        self._read_body()
        xml = b'<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>'
        self._send(200, xml, {"Content-Type": "application/xml"})

    def log_message(self, *args):
        pass


def _child_s3_backup(lib_path, secret_file):
    """BACKUP/RESTORE through S3 work on a restricted connection; a backup cannot bring a table on a local file back."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _BucketHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}/bucket"
    creds = "'test', 'testtest'"
    chdb = Chdb(lib_path)
    plain = chdb.connect()
    restricted = chdb.connect(OPTION)
    results = {}
    results["setup"] = [
        plain.query("CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x"),
        plain.query("INSERT INTO src SELECT number FROM numbers(10)"),
        plain.query(f"CREATE TABLE on_file (s String) ENGINE = File(LineAsString, '{secret_file}')"),
        plain.query(f"BACKUP TABLE default.on_file TO S3('{base}/poisoned', {creds})"),
    ]
    results["backup"] = restricted.query(f"BACKUP TABLE default.src TO S3('{base}/clean', {creds})")
    results["restore"] = restricted.query(f"RESTORE TABLE default.src AS default.src_copy FROM S3('{base}/clean', {creds})")
    results["restored"] = restricted.query("SELECT count(), sum(x) FROM src_copy")
    results["restore_file_table"] = restricted.query(
        f"RESTORE TABLE default.on_file AS default.on_file_copy FROM S3('{base}/poisoned', {creds})"
    )
    results["file_table_exists"] = restricted.query("EXISTS TABLE on_file_copy")
    results["backup_database"] = restricted.query(
        f"CREATE DATABASE poisoned_db ENGINE = Backup('default', S3('{base}/poisoned', {creds}))"
    )
    results["backup_database_read"] = restricted.query("SELECT * FROM poisoned_db.on_file")
    restricted.close()
    plain.close()
    server.shutdown()
    return results


@unittest.skipIf(_LIB_PATH is None, "libchdb not found; build chdb-core or set CHDB_LIB_PATH")
class TestRestrictedConnectionInternalAccess(unittest.TestCase):
    """Scenarios that need a process of their own."""

    def run_child(self, scenario, *args, env=None):
        proc = subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--child", scenario, _LIB_PATH, *args],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
            start_new_session=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr[-4000:])
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_persistent_database_on_a_restricted_connection(self):
        db = tempfile.mkdtemp(prefix="chdb_restrict_fs_db_")
        try:
            results = self.run_child("persistent", db)
        finally:
            shutil.rmtree(db, ignore_errors=True)
        for step in ["create", "insert", "optimize", "reload"]:
            self.assertIsNone(results[step][0], f"{step}: {results[step][0]}")
        self.assertEqual(results["reload"][1], "1000\t499500\n")
        self.assertRegex(results["file"][0] or "", DENIED)

    def test_s3_credentials_file_is_used_but_not_readable(self):
        tmp = tempfile.mkdtemp(prefix="chdb_restrict_fs_aws_")
        try:
            credentials = os.path.join(tmp, "credentials")
            with open(credentials, "w") as f:
                f.write("[restricted-test]\naws_access_key_id = AKIDRESTRICTTEST\naws_secret_access_key = " + SECRET + "\n")
            env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
            env.update(
                AWS_SHARED_CREDENTIALS_FILE=credentials,
                AWS_CONFIG_FILE=os.path.join(tmp, "no-config"),
                AWS_PROFILE="restricted-test",
                AWS_EC2_METADATA_DISABLED="true",
                AWS_REGION="us-east-1",
            )
            results = self.run_child("s3", credentials, env=env)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        self.assertIsNone(results["s3"][0], results["s3"][0])
        self.assertEqual(results["s3"][1], "6\n")
        self.assertTrue(results["authorizations"], "the bucket received no request")
        self.assertTrue(
            all("Credential=AKIDRESTRICTTEST/" in a for a in results["authorizations"]), results["authorizations"]
        )
        self.assertEqual(results["nosign"], [None, "6\n"])
        self.assertEqual(results["url"], [None, "6\n"])
        for key in ["file", "scalar_file"]:
            err, body = results[key]
            self.assertRegex(err or "", DENIED, key)
            self.assertNotIn(SECRET, (err or "") + body)

    def test_roles_and_row_policies_of_default(self):
        tmp = tempfile.mkdtemp(prefix="chdb_restrict_fs_roles_")
        try:
            secret = _make_tree(tmp)
            results = self.run_child("roles", tmp, secret)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        self.assertIn(SECRET, results["plain_file"][1], results["plain_file"])
        self.assertRegex(results["restricted_file"][0] or "", DENIED)
        self.assertEqual(results["restricted_roles"], [None, "0\n"])
        for step in ["create", "insert"]:
            self.assertIsNone(results[step][0], results[step][0])
        self.assertEqual(results["plain_rows"], [None, "1\n"])
        self.assertEqual(results["restricted_rows"], [None, "1\n"])
        self.assertIsNone(results["wide_policy"][0], results["wide_policy"][0])
        self.assertEqual(results["restricted_rows_after_wide_policy"], [None, "1\n"])
        self.assertRegex(results["restricted_execute_as"][0] or "", DENIED + "|IMPERSONATE")
        self.assertRegex(results["restricted_create_policy"][0] or "", DENIED + "|ACCESS_MANAGEMENT")
        self.assertIsNotNone(results["set_role"][0])
        self.assertRegex(results["restricted_file_after_set_role"][0] or "", DENIED)
        # Without the role, `default` lacks the FILE grants, but is not a restricted connection: ClickHouse's own
        # rules apply, and INTO OUTFILE needs no grant.
        self.assertIsNone(results["plain_no_role"][0], results["plain_no_role"][0])
        self.assertIsNone(results["plain_outfile"][0], results["plain_outfile"][0])
        self.assertTrue(results["outfile_written"])
        for err, _ in results["profiles"]:
            self.assertIsNone(err, err)
        self.assertEqual(results["plain_settings"], [None, "3\t77\t1000000000\n"])
        self.assertEqual(results["restricted_settings"], results["plain_settings"])
        self.assertRegex(results["restricted_const"][0] or "", "SETTING_CONSTRAINT_VIOLATION|should not be changed")

    def test_sql_cannot_end_the_host_process(self):
        results = self.run_child("shutdown")
        for sql in ["SYSTEM SHUTDOWN", "SYSTEM KILL"]:
            self.assertRegex(results[sql][0] or "", DENIED, sql)
        self.assertEqual(results["SELECT 'alive'"], [None, "alive\n"])

    def test_s3_backup_and_restore(self):
        tmp = tempfile.mkdtemp(prefix="chdb_restrict_fs_bk_")
        try:
            secret = _make_tree(tmp)
            results = self.run_child("s3_backup", secret)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        for err, _ in results["setup"]:
            self.assertIsNone(err, err)
        for step in ["backup", "restore", "restored"]:
            self.assertIsNone(results[step][0], f"{step}: {results[step][0]}")
        self.assertEqual(results["restored"][1], "10\t45\n")
        self.assertRegex(results["restore_file_table"][0] or "", DENIED)
        self.assertEqual(results["file_table_exists"], [None, "0\n"])
        # A Backup database attaches the tables of its backup again at every start, whatever their engine.
        self.assertRegex(results["backup_database"][0] or "", DENIED)
        err, body = results["backup_database_read"]
        self.assertNotIn(SECRET, (err or "") + body)


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--child":
        scenario, lib_path, *rest = sys.argv[2:]
        handler = {"persistent": _child_persistent, "s3": _child_s3_credentials, "s3_backup": _child_s3_backup, "shutdown": _child_shutdown, "roles": _child_roles}[scenario]
        print(json.dumps(handler(lib_path, *rest)))
        sys.exit(0)
    unittest.main()
