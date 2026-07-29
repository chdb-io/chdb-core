#!python3

import glob
import os
import subprocess
import sys
import unittest


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
    "test_adbc_driver.TestAdbcGetObjects",
    "test_adbc_driver.TestAdbcIngest",
    "test_adbc_driver.TestAdbcMetadata",
    "test_adbc_driver.TestAdbcParameters",
    "test_adbc_driver.TestAdbcQuery",
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
]

# Modules covered by the ISOLATED targets above; excluded from the main shards.
ISOLATED_MODULES = {t.split(".", 1)[0] for t in ISOLATED}

# Shard the remaining suite across this many subprocesses so that no single
# process accumulates the whole suite's engine create/destroy churn. Starting
# and tearing the embedded engine down on every connection repeatedly can
# corrupt the process allocator and abort under load on macOS; splitting the
# run keeps each process's churn well below that point.
NGROUPS = 4

HERE = os.path.dirname(os.path.abspath(__file__)) or "."
os.chdir(HERE)


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
    ret = unittest.TextTestRunner(verbosity=2).run(suite)
    if load_errors:
        print(f"{Colors.RED}[run_all] modules that failed to load: "
              f"{', '.join(load_errors)}{Colors.END}")
    return len(ret.failures) == 0 and len(ret.errors) == 0 and not load_errors


# Worker mode: run exactly the modules listed after --only, in this process.
if "--only" in sys.argv:
    _mods = sys.argv[sys.argv.index("--only") + 1:]
    sys.exit(0 if _run_modules(_mods) else 1)


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
    if subprocess.call([sys.executable, __file__, "--only", *grp]) != 0:
        failed.append(f"main-shard-{gi}")

for target in ISOLATED:
    if not os.path.exists(target.split(".", 1)[0] + ".py"):
        continue
    print(f"\n{Colors.YELLOW}[run_all] isolated subprocess: {target}{Colors.END}",
          flush=True)
    if subprocess.call([sys.executable, __file__, "--only", target]) != 0:
        failed.append(target)

if not failed:
    print(f"\n{Colors.GREEN}{Colors.BOLD}✓ ALL TESTS PASSED{Colors.END}")
    print(f"{Colors.GREEN}{NGROUPS} main shards + {len(ISOLATED)} isolated targets"
          f"{Colors.END}")
    sys.exit(0)

print(f"\n{Colors.RED}{Colors.BOLD}✖ TEST FAILURES{Colors.END}")
print(f"{Colors.RED}Failed shards: {', '.join(failed)}{Colors.END}")
sys.exit(1)
