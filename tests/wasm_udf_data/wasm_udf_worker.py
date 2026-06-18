#!/usr/bin/env python3
"""Worker for tests/test_wasm_udf.py — runs the WASM UDF checks in a fresh process.

This MUST run in its own process: chDB's embedded server is a process singleton
whose data path and server settings (including allow_experimental_webassembly_udf)
are fixed by the first connection. Driving it from the shared pytest process would
lock every other test to this path. So the pytest test spawns this script.

Protocol: prints "SKIP: <reason>" and exits 0 if the runtime isn't in this build;
prints "ALL_OK" on success; raises (non-zero exit) on any failed assertion.
"""

import os
import shutil
import sys
import tempfile

from chdb.session import Session

MODULE = "wasm_udf_demo"
FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wasm_udf_demo.wasm")


def main():
    tmp = tempfile.mkdtemp(prefix="chdb_wasm_worker_")
    conn = os.path.join(tmp, "wasm.db") + "?allow_experimental_webassembly_udf=1"
    # CHDB_TEST_WASM_ENGINE lets a developer force a specific engine to exercise the
    # "engine not compiled in" skip path (e.g. =wasmedge on a build with only wasmtime).
    engine = os.environ.get("CHDB_TEST_WASM_ENGINE", "")
    if engine:
        conn += "&webassembly_udf_engine=" + engine
    sess = Session(conn)
    try:
        def scalar(sql):
            return str(sess.query(sql, "CSV")).strip()

        def runtime_absent(msg):
            # The system table + module manager exist whenever the experimental flag is on,
            # but the WASM *engine* is only compiled into the full build. Minimized (lite)
            # and cross-compiled wheels ship without wasmtime (USE_WASMTIME=0), so loading a
            # module or creating a function raises SUPPORT_IS_DISABLED ("... support is
            # disabled" / "WebAssembly support is not enabled"). Treat that as "skip", not a
            # failure — checking the table alone is not enough (it exists even without the engine).
            m = msg.lower()
            return ("support is disabled" in m) or ("support is not enabled" in m)

        abs_fixture = FIXTURE.replace("\\", "\\\\").replace("'", "\\'")

        # Load the module + create the first function. These need the engine, so this is
        # also where we detect whether the runtime is actually present in this build.
        try:
            sess.query("DROP FUNCTION IF EXISTS wasm_add", "CSV")
            sess.query("DROP FUNCTION IF EXISTS count_chars", "CSV")
            sess.query(f"DELETE FROM system.webassembly_modules WHERE name = '{MODULE}'", "CSV")
            sess.query(
                "INSERT INTO system.webassembly_modules (name, code) "
                f"SELECT '{MODULE}', raw_blob FROM file('{abs_fixture}', 'RawBlob')",
                "CSV",
            )
            sess.query(
                "CREATE OR REPLACE FUNCTION wasm_add LANGUAGE WASM ABI ROW_DIRECT "
                f"FROM '{MODULE}' ARGUMENTS (UInt64, UInt64) RETURNS UInt64",
                "CSV",
            )
        except Exception as e:  # noqa: BLE001
            if runtime_absent(str(e)):
                print("SKIP: WASM runtime not in this build:", str(e).strip().splitlines()[-1])
                return 0
            raise

        # Module is registered, with a 64-hex-char (32-byte) SHA256 hash.
        assert scalar(f"SELECT name FROM system.webassembly_modules WHERE name = '{MODULE}'") == f'"{MODULE}"'
        assert scalar(
            f"SELECT length(hex(reinterpretAsFixedString(hash))) FROM system.webassembly_modules WHERE name = '{MODULE}'"
        ) == "64"

        sess.query(
            "CREATE OR REPLACE FUNCTION count_chars LANGUAGE WASM ABI BUFFERED_V1 "
            f"FROM '{MODULE}' ARGUMENTS (s String) RETURNS UInt64 "
            "SETTINGS serialization_format = 'RowBinary'",
            "CSV",
        )

        # ROW_DIRECT scalar path (numeric).
        assert scalar("SELECT wasm_add(2, 3)") == "5"
        assert scalar("SELECT wasm_add(1000000, 2345)") == "1002345"
        assert str(
            sess.query("SELECT wasm_add(number, number) FROM numbers(4) ORDER BY number", "CSV")
        ).strip().split("\n") == ["0", "2", "4", "6"]

        # BUFFERED_V1 String -> UInt64 block path.
        assert scalar("SELECT count_chars('hello world')") == "11"
        assert scalar("SELECT count_chars('')") == "0"
        assert scalar("SELECT count_chars('café')") == "4"  # 4 Unicode scalars, 5 UTF-8 bytes
        assert str(
            sess.query(
                "SELECT count_chars(s) FROM (SELECT arrayJoin(['a','bb','ccc','']) AS s) ORDER BY length(s)",
                "CSV",
            )
        ).strip().split("\n") == ["0", "1", "2", "3"]

        # Wrong argument type must be rejected. Use try/except/else so the
        # AssertionError raised when the call *does* succeed is not silently
        # swallowed by the same broad ``except Exception``.
        try:
            sess.query("SELECT wasm_add('a', 'b')", "CSV")
        except Exception:
            pass  # expected: the runtime should reject the wrong argument type
        else:
            raise AssertionError("wrong-argument-type call was not rejected")

        sess.query("DROP FUNCTION IF EXISTS wasm_add", "CSV")
        sess.query("DROP FUNCTION IF EXISTS count_chars", "CSV")
        sess.query(f"DELETE FROM system.webassembly_modules WHERE name = '{MODULE}'", "CSV")
        print("ALL_OK")
        return 0
    finally:
        # Mirror the test-suite convention (test_func_udf.py, test_stateful.py):
        # always close the session and remove the on-disk temp dir so repeated
        # worker runs don't leak chDB data directories.
        sess.close()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
