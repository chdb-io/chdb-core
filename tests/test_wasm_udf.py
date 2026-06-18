#!python3
"""End-to-end tests for chDB WebAssembly UDFs.

These exercise the ClickHouse WASM UDF runtime (wasmtime) that chDB links in when
built with -DENABLE_RUST=1 -DENABLE_WASMTIME=1 (see chdb/build.sh), gated behind the
experimental server setting `allow_experimental_webassembly_udf`.

IMPORTANT — process isolation: chDB's embedded server is a process singleton. The
first connection in a process fixes its data path and server settings (including the
experimental flag) for the whole process. If this test touched the server in the
shared pytest process, every later test that connects with `:memory:` would fail with
"EmbeddedServer already initialized with path ...". So this module initializes NOTHING
at import time and runs all WASM work in fresh subprocesses (tests/wasm_udf_data/).

The fixture `tests/wasm_udf_data/wasm_udf_demo.wasm` is a tiny dependency-free module
(source in `tests/wasm_udf_data/wasm_src/`). It imports no host functions and exists
only to prove the runtime is wired up, exercising the two core UDF ABIs with one
function each: a ROW_DIRECT scalar `wasm_add(UInt64, UInt64) -> UInt64` (numeric path)
and a BUFFERED_V1 `count_chars(String) -> UInt64` (String/block path). ABI keywords are
the in-tree v26.5 spelling `ROW_DIRECT` / `BUFFERED_V1`.
"""

import os
import subprocess
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
_WORKER = os.path.join(_HERE, "wasm_udf_data", "wasm_udf_worker.py")

# WASM UDF support (wasmtime) is a full-build-only feature. The size-minimized lite
# wheel deliberately excludes the engine, so these tests do not run there at all.
_LITE = os.environ.get("CHDB_LITE") == "1"
_LITE_SKIP = "WASM UDF is a full-build feature; the lite wheel excludes the wasmtime engine"


def _subprocess_env():
    # Propagate the parent's import path so the subprocess imports the SAME chdb
    # (the installed wheel in CI, or the repo checkout locally).
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    return env


def _run(args):
    return subprocess.run(
        [sys.executable, *args],
        cwd=_REPO,
        env=_subprocess_env(),
        capture_output=True,
        text=True,
    )


@unittest.skipIf(_LITE, _LITE_SKIP)
class TestWasmUDF(unittest.TestCase):
    def test_wasm_udf_end_to_end(self):
        """Load a module, create ROW_DIRECT + BUFFERED_V1 functions, and execute them."""
        proc = _run([_WORKER])
        out = proc.stdout + proc.stderr
        if "SKIP:" in out and "ALL_OK" not in out:
            self.skipTest(f"WASM UDF runtime not present in this build: {out.strip()}")
        self.assertEqual(proc.returncode, 0, out)
        self.assertIn("ALL_OK", out, out)


@unittest.skipIf(_LITE, _LITE_SKIP)
class TestWasmUDFDisabled(unittest.TestCase):
    def test_create_function_without_flag_is_disabled(self):
        """Without allow_experimental_webassembly_udf, the runtime refuses WASM UDFs."""
        snippet = (
            "import tempfile, os\n"
            "from chdb.session import Session\n"
            "s = Session(os.path.join(tempfile.mkdtemp(), 'w.db'))\n"  # no flag
            "try:\n"
            "    s.query(\"CREATE FUNCTION nope LANGUAGE WASM ABI ROW_DIRECT \"\n"
            "            \"FROM 'whatever' ARGUMENTS (x UInt64) RETURNS UInt64\", 'CSV')\n"
            "    print('NO_ERROR')\n"
            "except Exception as e:\n"
            "    print('ERR:', e)\n"
        )
        proc = _run(["-c", snippet])
        out = proc.stdout + proc.stderr
        # Verify both that the call was actually rejected (printed "ERR:" via
        # the except branch) and not silently accepted (would have printed
        # "NO_ERROR"). The substring check alone is not enough because any
        # unrelated diagnostic mentioning "not enabled" could let the test
        # false-pass even if the runtime accepted the CREATE FUNCTION.
        self.assertNotIn("NO_ERROR", out, f"CREATE FUNCTION unexpectedly succeeded: {out!r}")
        self.assertIn("ERR:", out, f"expected the CREATE FUNCTION to be rejected: {out!r}")
        self.assertIn("not enabled", out.lower(), f"unexpected output: {out!r}")


if __name__ == "__main__":
    unittest.main()
