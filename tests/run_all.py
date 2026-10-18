#!python3

import glob
import os
import re
import subprocess
import sys
import time
import unittest
import xml.etree.ElementTree as ET


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    END = '\033[0m'


# Test targets that each get a dedicated subprocess (module, class, or single
# test). The C-ABI tests load the standalone libchdb.so via ctypes, i.e. a
# second engine alongside the Python module _chdb.abi3.so.
#
# The engine serves one storage path at a time, so a test that opens a path
# other than ":memory:" needs the engine to shut down and start again. Give
# each of those its own process: the engine is built once there and goes away
# with the process, instead of being torn down and rebuilt in place.
ISOLATED = [
    "test_adbc_driver.TestAdbcCancel",
    "test_adbc_driver.TestAdbcGetObjects",
    "test_adbc_driver.TestAdbcIngest",
    "test_adbc_driver.TestAdbcMetadata",
    "test_adbc_driver.TestAdbcParameters",
    "test_adbc_driver.TestAdbcQuery",
    "test_adbc_driver.TestAdbcReadOnly",
    "test_adbc_driver.TestAdbcStatementSettings",
    "test_adbc_driver.TestAdbcPersistence.test_on_disk_path_roundtrip",
    "test_adbc_driver.TestAdbcUri.test_chdb_bad_authority_rejected",
    "test_adbc_driver.TestAdbcUri.test_chdb_memory_forms",
    "test_adbc_driver.TestAdbcUri.test_chdb_relative_named_memory_escape_hatch",
    "test_adbc_driver.TestAdbcUri.test_chdb_relative_path",
    "test_adbc_driver.TestAdbcUri.test_chdb_uri_path_forms",
    "test_adbc_driver.TestAdbcUri.test_file_uri_forms",
    "test_adbc_driver.TestAdbcUri.test_non_file_scheme_rejected",
    "test_arrow_c_data_output",
    "test_c_api_query_with_params",
    "test_c_api_stream_insert",
    "test_signal_handler_api",
    "test_system_logs",
]

# Modules covered by the ISOLATED targets above; excluded from the main shards.
# Note the consequence: a class of one of these modules that is NOT named above
# does not run at all. Add new test_adbc_driver classes here.
ISOLATED_MODULES = {t.split(".", 1)[0] for t in ISOLATED}

# Shard the remaining suite across this many subprocesses so that no single
# process accumulates the whole suite's engine create/destroy churn. Starting
# and tearing the embedded engine down on every connection repeatedly can
# corrupt the process allocator and abort under load on macOS; splitting the
# run keeps each process's churn well below that point.
NGROUPS = 4

HERE = os.path.dirname(os.path.abspath(__file__)) or "."
os.chdir(HERE)

# Set CHDB_JUNIT_DIR to also write one JUnit XML file per subprocess there; the
# nightly dashboard reads them. Results and exit codes are unchanged.
JUNIT_DIR = os.environ.get("CHDB_JUNIT_DIR")


class _RecordingResult(unittest.TextTestResult):
    """TextTestResult that also remembers each test's outcome and duration."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []
        self._started = 0.0

    def startTest(self, test):
        self._started = time.monotonic()
        super().startTest(test)

    def _record(self, test, status, detail=""):
        # The id, not the test: a test case kept alive keeps its connection open, and
        # the next test that connects to another path then fails.
        self.records.append((test.id(), status, time.monotonic() - self._started, detail))

    def addSuccess(self, test):
        super().addSuccess(test)
        self._record(test, "passed")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._record(test, "failure", self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self._record(test, "error", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._record(test, "skipped", reason)

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self._record(test, "passed")

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self._record(test, "failure", "unexpected success")

    def addSubTest(self, test, subtest, err):
        # A failing subtest fails its test, which then never reaches addSuccess.
        super().addSubTest(test, subtest, err)
        if err is not None:
            status = "failure" if issubclass(err[0], test.failureException) else "error"
            self._record(test, status, f"{subtest}\n{self._exc_info_to_string(err, test)}")


def _write_junit(name, result, load_errors):
    suite = ET.Element("testsuite", name=name)
    counts = {"failure": 0, "error": 0, "skipped": 0}
    for test_id, status, seconds, detail in result.records:
        classname, _, method = test_id.rpartition(".")
        case = ET.SubElement(suite, "testcase", classname=classname or test_id,
                             name=method or test_id, time=f"{seconds:.3f}")
        if status != "passed":
            counts[status] += 1
            message = detail.strip().splitlines()[-1][:200] if detail.strip() else status
            ET.SubElement(case, status, message=message).text = detail
    for mod in load_errors:
        counts["error"] += 1
        case = ET.SubElement(suite, "testcase", classname=mod, name="<module load>", time="0")
        ET.SubElement(case, "error", message="module failed to load")
    suite.set("tests", str(len(result.records) + len(load_errors)))
    suite.set("failures", str(counts["failure"]))
    suite.set("errors", str(counts["error"]))
    suite.set("skipped", str(counts["skipped"]))
    os.makedirs(JUNIT_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    ET.ElementTree(suite).write(os.path.join(JUNIT_DIR, f"{safe}.xml"), encoding="utf-8", xml_declaration=True)


def _run_modules(mods):
    """Load and run the given test modules in THIS process. Loading by name
    (not discover()) keeps unrelated modules out of the process. Tolerates a
    module-level SkipTest (e.g. missing REMOTE_*/S3 env). Returns True on
    success."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    load_errors = []
    for mod in mods:
        try:
            suite.addTests(loader.loadTestsFromName(mod))
        except unittest.SkipTest:
            pass
        except Exception as exc:  # noqa: BLE001 - surface import failures, don't abort
            print(f"{Colors.RED}[run_all] failed to load {mod}: {exc}{Colors.END}")
            load_errors.append(mod)
    if JUNIT_DIR:
        ret = unittest.TextTestRunner(verbosity=2, resultclass=_RecordingResult).run(suite)
        _write_junit(os.environ.get("CHDB_JUNIT_NAME", "tests"), ret, load_errors)
    else:
        ret = unittest.TextTestRunner(verbosity=2).run(suite)
    if load_errors:
        print(f"{Colors.RED}[run_all] modules that failed to load: "
              f"{', '.join(load_errors)}{Colors.END}")
    return len(ret.failures) == 0 and len(ret.errors) == 0 and not load_errors


# Worker mode: run exactly the modules listed after --only, in this process.
#
# Exit via os._exit once the result is known. A worker that started the engine
# would otherwise run its shutdown (a global thread-pool teardown registered
# with atexit) on the way out, which on macOS occasionally takes minutes. The
# process is about to be replaced anyway, so skip the teardown and let the OS
# reclaim it — after flushing, since os._exit doesn't.
if "--only" in sys.argv:
    _mods = sys.argv[sys.argv.index("--only") + 1:]
    _ok = _run_modules(_mods)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0 if _ok else 1)


# Orchestrator: shard the main modules into NGROUPS subprocesses, then run each
# ISOLATED target in its own. Each shard re-invokes this script with --only.
all_mods = [p[:-3] for p in sorted(glob.glob("test_*.py"))]
main_mods = [m for m in all_mods if m not in ISOLATED_MODULES]
groups = [main_mods[i::NGROUPS] for i in range(NGROUPS)]

failed = []
for gi, grp in enumerate(groups, 1):
    if not grp:
        continue
    print(f"\n{Colors.YELLOW}[run_all] main shard {gi}/{NGROUPS} "
          f"({len(grp)} modules){Colors.END}", flush=True)
    env = dict(os.environ, CHDB_JUNIT_NAME=f"main-shard-{gi}")
    if subprocess.call([sys.executable, __file__, "--only", *grp], env=env) != 0:
        failed.append(f"main-shard-{gi}")

for target in ISOLATED:
    if not os.path.exists(target.split(".", 1)[0] + ".py"):
        continue
    print(f"\n{Colors.YELLOW}[run_all] isolated subprocess: {target}{Colors.END}",
          flush=True)
    env = dict(os.environ, CHDB_JUNIT_NAME=target)
    if subprocess.call([sys.executable, __file__, "--only", target], env=env) != 0:
        failed.append(target)

if not failed:
    print(f"\n{Colors.GREEN}{Colors.BOLD}✓ ALL TESTS PASSED{Colors.END}")
    print(f"{Colors.GREEN}{NGROUPS} main shards + {len(ISOLATED)} isolated targets"
          f"{Colors.END}")
    sys.exit(0)

print(f"\n{Colors.RED}{Colors.BOLD}✖ TEST FAILURES{Colors.END}")
print(f"{Colors.RED}Failed shards: {', '.join(failed)}{Colors.END}")
sys.exit(1)
