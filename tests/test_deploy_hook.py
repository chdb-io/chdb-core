#!python3

"""Tests for the chdb.deploy extension hook in chdb/__init__.py.

The chdb wrapper wheel (PyPI package `chdb`) may ship chdb/deploy.py into the
same package directory as chdb-core; the __init__.py hook prefers that
extended decorator when it is importable and falls back to chdb.udf.func
otherwise. Each case runs in a subprocess so the import-time behavior is
exercised from a clean slate.
"""

import subprocess
import sys
import textwrap
import unittest


def _run(code: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    return result.stdout


class TestDeployHook(unittest.TestCase):
    def test_falls_back_to_plain_decorator_without_deploy_module(self):
        out = _run(
            """
            import chdb
            import chdb.udf

            assert chdb.func is chdb.udf.func, (
                "without chdb/deploy.py, chdb.func must be chdb.udf.func"
            )
            print("fallback-ok")
            """
        )
        self.assertIn("fallback-ok", out)

    def test_prefers_deploy_module_when_importable(self):
        # Pre-seeding sys.modules["chdb.deploy"] is equivalent to the wrapper
        # wheel having shipped chdb/deploy.py: the hook's `from .deploy
        # import func` resolves against sys.modules first.
        out = _run(
            """
            import sys
            import types

            stub = types.ModuleType("chdb.deploy")

            def _extended_func(*args, **kwargs):
                raise NotImplementedError

            stub.func = _extended_func
            sys.modules["chdb.deploy"] = stub

            import chdb

            assert chdb.func is stub.func, (
                "with chdb.deploy importable, chdb.func must be its func"
            )
            print("prefers-deploy-ok")
            """
        )
        self.assertIn("prefers-deploy-ok", out)


if __name__ == "__main__":
    unittest.main()
