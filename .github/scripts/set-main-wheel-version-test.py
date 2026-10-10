#!/usr/bin/env python3

import importlib.util
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("set-main-wheel-version.py")
SPEC = importlib.util.spec_from_file_location("set_main_wheel_version", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class MainWheelVersionTest(unittest.TestCase):
    def test_stable_tag(self):
        self.assertEqual(
            MODULE.main_version("v26.9.0", 12, "A1B2C3D4"),
            "26.9.0.post1.dev12+ga1b2c3d4",
        )

    def test_rc_tag(self):
        self.assertEqual(
            MODULE.main_version("v26.9.2-rc.2", 0, "abcdef12"),
            "26.9.2rc2.post1.dev0+gabcdef12",
        )

    def test_beta_tag(self):
        self.assertEqual(
            MODULE.main_version("v4.0.0b6", 3, "123abc"),
            "4.0.0b6.post1.dev3+g123abc",
        )

    def test_rejects_unsupported_tag(self):
        with self.assertRaisesRegex(ValueError, "unsupported release tag"):
            MODULE.main_version("nightly", 1, "abcdef")

    def test_rejects_negative_distance(self):
        with self.assertRaisesRegex(ValueError, "cannot be negative"):
            MODULE.main_version("v26.9.0", -1, "abcdef")

    def test_replace_once_preserves_missing_final_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "pyproject.toml"
            path.write_text('[project]\nversion = "0.0.1b1"')
            MODULE.replace_once(
                path,
                r'^version\s*=\s*"[^"]+"$',
                'version = "26.9.0.post1.dev1+gabcdef"',
            )
            self.assertEqual(
                path.read_text(),
                '[project]\nversion = "26.9.0.post1.dev1+gabcdef"',
            )


if __name__ == "__main__":
    unittest.main()
