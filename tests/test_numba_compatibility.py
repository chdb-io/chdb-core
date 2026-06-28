#!python3
"""Regression test: chdb must import cleanly even after numba (macOS).

On macOS, chdb's pybind11 stub/shim dylibs statically link ClickHouse's bundled
libcxx and used to export libc++ ``std::__1`` methods (e.g. ``basic_string``) as
*weak external* symbols. numba (through llvmlite) bundles its own libc++; when
``import numba`` runs before ``import chdb``, dyld's weak-symbol coalescing made
chdb bind to numba's ABI-incompatible libc++, corrupting memory inside
pybind11's ``EnsureSharedLibraryIsLoaded`` and aborting the interpreter with a
garbled ``dlopen`` path. Hiding ``std::__1`` from those dylibs' export tables
fixes it.

Because the failure aborts the whole process, each import order is exercised in
a subprocess and asserted via its exit code, rather than importing in-process
(which would take down the entire test runner).
"""

import importlib.util
import os
import subprocess
import sys
import unittest

HAS_NUMBA = importlib.util.find_spec("numba") is not None
IS_MACOS = sys.platform == "darwin"
IS_LITE = os.environ.get("CHDB_LITE") == "1"


@unittest.skipUnless(
    HAS_NUMBA and IS_MACOS and not IS_LITE,
    "regression test requires numba on macOS (non-lite build)",
)
class TestNumbaCompatibility(unittest.TestCase):
    """chdb and numba must coexist regardless of import order on macOS."""

    def _run_import(self, code):
        return subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=180,
        )

    def setUp(self):
        # If numba can't even be imported on its own -- e.g. the environment has
        # a numpy that is too new for the installed numba ("Numba needs NumPy
        # X or less") -- there is nothing to test here, so skip instead of
        # failing. This never masks the regression we care about: the crash only
        # happens when chdb is imported *alongside* numba; `import numba` by
        # itself still succeeds, so a genuine regression is still caught below.
        probe = self._run_import("import numba")
        if probe.returncode != 0:
            self.skipTest(
                "numba is not importable in this environment "
                f"(returncode={probe.returncode}); skipping chdb/numba "
                f"coexistence check.\nstderr={probe.stderr!r}"
            )

    def test_import_numba_then_chdb(self):
        # The order that used to abort: numba first, then chdb.
        proc = self._run_import(
            "import numba; import chdb; "
            "print(chdb.query('SELECT 1 AS a', 'CSV'), end='')"
        )
        self.assertEqual(
            proc.returncode,
            0,
            "`import numba; import chdb` aborted "
            f"(returncode={proc.returncode}).\n"
            f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}",
        )
        self.assertIn("1", proc.stdout)

    def test_import_chdb_then_numba(self):
        # Control: the order that always worked.
        proc = self._run_import(
            "import chdb; import numba; "
            "print(chdb.query('SELECT 2 AS a', 'CSV'), end='')"
        )
        self.assertEqual(
            proc.returncode,
            0,
            "`import chdb; import numba` aborted "
            f"(returncode={proc.returncode}).\n"
            f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}",
        )
        self.assertIn("2", proc.stdout)


if __name__ == "__main__":
    unittest.main()
