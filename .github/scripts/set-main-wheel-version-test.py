#!/usr/bin/env python3

import datetime
import importlib.util
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("set-main-wheel-version.py")
SPEC = importlib.util.spec_from_file_location("set_main_wheel_version", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

# 2026-10-15 16:17:59 UTC
COMMIT_TIME = int(datetime.datetime(2026, 10, 15, 16, 17, 59, tzinfo=datetime.timezone.utc).timestamp())


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

    def test_dev_number_is_the_commit_minute_in_utc(self):
        self.assertEqual(MODULE.dev_number(COMMIT_TIME), 2610151617)

    def test_nightly_has_no_local_version(self):
        self.assertEqual(MODULE.nightly_version("v26.9.0", COMMIT_TIME, (26, 9)), "26.9.0.post1.dev2610151617")

    def test_nightly_names_a_newer_engine_line(self):
        self.assertEqual(MODULE.nightly_version("v26.9.0", COMMIT_TIME, (26, 11)), "26.11.0.dev2610151617")

    def test_nightly_after_rc(self):
        self.assertEqual(
            MODULE.nightly_version("v26.9.2-rc.2", COMMIT_TIME, (26, 9)),
            "26.9.2rc2.post1.dev2610151617",
        )

    def test_engine_line(self):
        self.assertEqual(MODULE.engine_line('#define CHDB_VERSION "26.11.0-dev"\n'), (26, 11))
        self.assertIsNone(MODULE.engine_line("no version here"))

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
