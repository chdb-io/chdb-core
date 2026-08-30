#!/bin/bash
#
# Release gates for the macOS static library (issue #198).
#
# libchdb.so/.dylib gate their exports at link time. libchdb.a cannot: it is handed to a
# linker we do not control, so the gate has to be baked into the objects
# (cmake/bundled_runtime_visibility.cmake). These checks verify that it was.
#
#   Gate 1  no coalescing fixup in the probe can bind to the system C++/unwind runtime
#   Gate 2  no bundled runtime object exports a symbol the system runtime also exports
#   Gate 3a the two checked-in export allow-lists describe the same C API contract
#   Gate 3b every symbol in that contract is still reachable from libchdb.a
#   Gate 4  the linked probe connects and runs a query
#
# The probe is linked with a modern deployment target on purpose. Measured against a
# pre-fix arm64 archive: -mmacosx-version-min=10.15 (what chdb_example.cpp uses) yields no
# coalescing fixups at all, 12.0 and up yield 33102 of them. Anything older than 12.0 hides
# the failure this script exists to catch.
#
# Usage: check_static_lib_hermetic.sh [path/to/libchdb.a] [path/to/chdb.h]

set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJ_DIR="$( cd "${MY_DIR}/../.." >/dev/null 2>&1 && pwd )"

LIBCHDB_A=${1:-${PROJ_DIR}/libchdb.a}
CHDB_H=${2:-${PROJ_DIR}/programs/local/chdb.h}
MIN_USEFUL_TARGET=12.0

if [ "$(uname)" != "Darwin" ]; then
    echo "Error: these gates need a macOS host (dyld_info, system libc++). Run them in the"
    echo "       native macOS job that consumes the cross-built artifact."
    exit 1
fi

# Default to the host's own OS version: it is the newest target whose binaries this machine
# can still run, and gate 4 has to actually run the probe. Override with
# MACOSX_DEPLOYMENT_TARGET, but not below the floor checked next.
DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-$(sw_vers -productVersion | cut -d. -f1).0}

for f in "${LIBCHDB_A}" "${CHDB_H}"; do
    [ -f "${f}" ] || { echo "Error: not found: ${f}"; exit 1; }
done

if [ "$(printf '%s\n%s\n' "${MIN_USEFUL_TARGET}" "${DEPLOYMENT_TARGET}" | sort -V | head -1)" != "${MIN_USEFUL_TARGET}" ]; then
    echo "Error: deployment target ${DEPLOYMENT_TARGET} is older than ${MIN_USEFUL_TARGET}; the"
    echo "       linker would emit no chained fixups and gate 1 would pass vacuously."
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "Static library gates"
echo "  archive:           ${LIBCHDB_A}"
echo "  header:            ${CHDB_H}"
echo "  deployment target: ${DEPLOYMENT_TARGET}"
echo

failures=0
fail () { echo "FAIL: $*"; failures=$((failures + 1)); }

# Exports of the three system runtimes chDB bundles its own copies of. `dyld_info` reads
# these out of the dyld shared cache; the dylibs no longer exist as files on modern macOS.
{ dyld_info -exports /usr/lib/libc++.1.dylib
  dyld_info -exports /usr/lib/libc++abi.dylib
  dyld_info -exports /usr/lib/system/libunwind.dylib; } 2>/dev/null \
    | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^__?[A-Za-z_$]/ && $i !~ /^\[/) { print $i; break } }' \
    | sed 's/^_//' | sort -u > "${WORK_DIR}/system_exports.txt"
[ -s "${WORK_DIR}/system_exports.txt" ] || { echo "Error: dyld_info returned no system runtime exports"; exit 1; }

# --- Gate 3a: the checked-in C API contract ---------------------------------------------
# Runs first: gate 3b consumes its output, and nothing downstream should hard-code a count.
echo "== Gate 3a: export allow-lists agree =="
# Fatal rather than counted: gate 3b builds its link line out of this contract, so there is
# nothing meaningful left to check if the two lists disagree.
if ! python3 "${MY_DIR}/check_export_contract.py" > "${WORK_DIR}/contract.txt"; then
    echo "FAIL: chdb/libchdb_export.map and chdb/libchdb_export_macos.txt disagree"
    exit 1
fi
echo "PASS"
echo

# --- Gate 2: no-link archive check ------------------------------------------------------
# Only the bundled runtime objects are in scope. ClickHouse's own weak/template symbols are
# a different problem and would swamp the signal.
echo "== Gate 2: bundled runtime objects export nothing the system runtime also exports =="
ar t "${LIBCHDB_A}" | grep -E '^lib(cxx|cxxabi|unwind)__' > "${WORK_DIR}/runtime_members.txt" || true
runtime_member_count=$(wc -l < "${WORK_DIR}/runtime_members.txt" | tr -d ' ')
if [ "${runtime_member_count}" -eq 0 ]; then
    fail "no libcxx__/libcxxabi__/libunwind__ members in the archive - has the member naming in create_static_libchdb.py changed?"
else
    mkdir -p "${WORK_DIR}/objs"
    (cd "${WORK_DIR}/objs" && xargs ar x "${LIBCHDB_A}" < "${WORK_DIR}/runtime_members.txt")

    # `nm -m` is needed rather than plain `nm -g`: a hidden symbol is still N_EXT in Mach-O,
    # just flagged private, and only the -m listing spells that out ("private external").
    # Those are the ones that cannot coalesce, so they are what gets filtered out here.
    find "${WORK_DIR}/objs" -name '*.o' -print0 \
        | xargs -0 nm -m -g --defined-only 2>/dev/null \
        | awk '!/private external/ && $NF ~ /^_/ { print substr($NF, 2) }' \
        | sort -u > "${WORK_DIR}/runtime_exports.txt"

    comm -12 "${WORK_DIR}/runtime_exports.txt" "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/overlap.txt"
    overlap=$(wc -l < "${WORK_DIR}/overlap.txt" | tr -d ' ')
    echo "  runtime objects: ${runtime_member_count}, still-external symbols: $(wc -l < "${WORK_DIR}/runtime_exports.txt" | tr -d ' ')"
    if [ "${overlap}" -eq 0 ]; then
        echo "PASS"
    else
        echo "  first 20 of ${overlap} colliding symbols:"
        head -20 "${WORK_DIR}/overlap.txt" | sed 's/^/    /'
        fail "${overlap} bundled runtime symbols can coalesce with the system runtime"
    fi
fi
echo

# --- Build the probe --------------------------------------------------------------------
echo "== Building probe (deployment target ${DEPLOYMENT_TARGET}) =="
cp "${CHDB_H}" "${WORK_DIR}/chdb.h"
cp "${MY_DIR}/static-probe/chdb_static_probe.c" "${WORK_DIR}/"
# Symlinked, not copied: the archive is around a gigabyte.
ln -s "$(cd "$(dirname "${LIBCHDB_A}")" && pwd)/$(basename "${LIBCHDB_A}")" "${WORK_DIR}/libchdb.a"

# -u forces the linker to resolve every symbol in the contract, so a public C API function
# that got hidden or dropped fails the link instead of failing a user months later.
force_flags=()
while read -r symbol; do
    [ -n "${symbol}" ] && force_flags+=("-Wl,-u,_${symbol}")
done < "${WORK_DIR}/contract.txt"

PROBE="${WORK_DIR}/chdb_static_probe"
if (cd "${WORK_DIR}" && MACOSX_DEPLOYMENT_TARGET="${DEPLOYMENT_TARGET}" \
        clang chdb_static_probe.c -o chdb_static_probe \
            -mmacosx-version-min="${DEPLOYMENT_TARGET}" \
            -I. -L. "${force_flags[@]}" -lchdb -liconv \
            -framework CoreFoundation -framework Security \
        > "${WORK_DIR}/link.log" 2>&1); then
    echo "PASS (Gate 3b: all $(wc -l < "${WORK_DIR}/contract.txt" | tr -d ' ') contract symbols resolved)"
else
    sed 's/^/    /' "${WORK_DIR}/link.log" | tail -40
    fail "probe link failed - a contract symbol is hidden or was dropped by the archive minimisation"
    echo
    echo "${failures} gate(s) failed"
    exit 1
fi
echo

# --- Gate 1: chained fixups -------------------------------------------------------------
echo "== Gate 1: no coalescing fixup can bind to the system runtime =="
dyld_info -fixups "${PROBE}" 2>/dev/null | grep '<weak-def-coalesce>' \
    | sed 's|.*<weak-def-coalesce>/_||' | sort -u > "${WORK_DIR}/probe_coalesce.txt" || true
total_coalesce=$(wc -l < "${WORK_DIR}/probe_coalesce.txt" | tr -d ' ')
comm -12 "${WORK_DIR}/probe_coalesce.txt" "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/probe_overlap.txt"
risky=$(wc -l < "${WORK_DIR}/probe_overlap.txt" | tr -d ' ')

# The gate is the intersection with the system runtime, not the raw total. Tens of
# thousands of the raw records are ClickHouse/abseil/magic_enum template instantiations:
# weak by definition, defined nowhere else, so dyld resolves them to this image. Only the
# ones the system libc++/libc++abi also defines can actually be bound to another runtime,
# and those are the ones that hang. Measured on a pre-fix arm64 archive at deployment
# target 26.0: 33102 records, 479 of them system-overlapping, probe hangs on chdb_connect.
# With the bundled runtimes hidden: 31576 records, 0 system-overlapping, probe passes.
echo "  coalescing records: ${total_coalesce} (informational), able to bind to the system runtime: ${risky}"
if [ "${risky}" -eq 0 ]; then
    echo "PASS"
else
    head -20 "${WORK_DIR}/probe_overlap.txt" | sed 's/^/    /'
    fail "${risky} bundled runtime symbols can be bound to the system runtime"
fi
echo

# --- Gate 4: runtime smoke test ---------------------------------------------------------
# The whole point of the exercise: the historical bug built cleanly and hung at run time.
echo "== Gate 4: probe connects and runs a query =="
# A watchdog, not a nicety: the failure this gate exists for is a hang, and macOS has no
# coreutils `timeout`.
(cd "${WORK_DIR}" && ./chdb_static_probe) &
probe_pid=$!
( sleep "${PROBE_TIMEOUT:-120}"; kill -9 "${probe_pid}" 2>/dev/null ) &
watchdog_pid=$!
disown "${watchdog_pid}" 2>/dev/null || true
if wait "${probe_pid}"; then
    echo "PASS"
else
    fail "probe did not complete successfully (killed after ${PROBE_TIMEOUT:-120}s if it hung)"
fi
kill "${watchdog_pid}" 2>/dev/null || true
echo

if [ "${failures}" -ne 0 ]; then
    echo "${failures} gate(s) failed"
    exit 1
fi
echo "All static library gates passed"
