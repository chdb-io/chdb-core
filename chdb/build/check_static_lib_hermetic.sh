#!/bin/bash
#
# Release gates for the static library.
#
# libchdb.so gates its exported surface at link time. libchdb.a is handed to a linker we do
# not control, so the gate has to be baked into the objects
# (cmake/bundled_runtime_visibility.cmake). These checks verify that it was.
#
#   Gate 1  nothing in the probe can bind the bundled runtime to the system runtime
#   Gate 2  no bundled runtime object exports a symbol the system runtime also exports
#   Gate 3a the two checked-in export allow-lists describe the same C API contract
#   Gate 3b every symbol in that contract is still reachable from libchdb.a
#   Gate 4  the linked probe connects and runs a query
#
# The five gates are the same on both platforms; only the tools and the condition that
# exposes the hazard differ.
#
#   Mach-O  weak definitions become <weak-def-coalesce> chained fixups and dyld may bind
#           them to the system libc++/libc++abi. Exposed by a deployment target of 12.0 or
#           newer: measured against a pre-fix arm64 archive, min=10.15 yields no coalescing
#           fixups at all while 12.0 and up yield 33102.
#   ELF     default-visibility definitions reach .dynsym and become interposable. A plain
#           executable link does not expose them, `-rdynamic` does - measured 1 symbol in
#           .dynsym at default visibility versus 0 at hidden - so the probe is linked that
#           way deliberately, as the ELF equivalent of a modern deployment target.
#
# Note both platforms need a visibility-aware tool: a hidden symbol is still a global
# definition in the symbol table on Mach-O and ELF alike, so plain `nm -g` cannot see the
# difference and would report a hardened archive as unprotected.
#
# Usage: check_static_lib_hermetic.sh [path/to/libchdb.a] [path/to/chdb.h]

set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJ_DIR="$( cd "${MY_DIR}/../.." >/dev/null 2>&1 && pwd )"

LIBCHDB_A=${1:-${PROJ_DIR}/libchdb.a}
CHDB_H=${2:-${PROJ_DIR}/programs/local/chdb.h}
MIN_USEFUL_TARGET=12.0

PLATFORM=$(uname)
case "${PLATFORM}" in
    Darwin|Linux) ;;
    *) echo "Error: unsupported platform ${PLATFORM}"; exit 1 ;;
esac

for f in "${LIBCHDB_A}" "${CHDB_H}"; do
    [ -f "${f}" ] || { echo "Error: not found: ${f}"; exit 1; }
done

# Absolute from here on: gate 2 and the probe link both run from a temp directory, where a
# relative path would resolve against the wrong place.
LIBCHDB_A="$(cd "$(dirname "${LIBCHDB_A}")" && pwd)/$(basename "${LIBCHDB_A}")"
CHDB_H="$(cd "$(dirname "${CHDB_H}")" && pwd)/$(basename "${CHDB_H}")"

# Mach-O prefixes every C symbol with an underscore; ELF does not. Everything downstream
# compares bare names, so record the prefix once and apply it only when building link flags.
if [ "${PLATFORM}" = Darwin ]; then
    SYM_PREFIX=_
    # Default to the host's own OS version: it is the newest target whose binaries this
    # machine can still run, and gate 4 has to run the probe. Override with
    # MACOSX_DEPLOYMENT_TARGET, but not below the floor checked next.
    DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-$(sw_vers -productVersion | cut -d. -f1).0}
    if [ "$(printf '%s\n%s\n' "${MIN_USEFUL_TARGET}" "${DEPLOYMENT_TARGET}" | sort -V | head -1)" != "${MIN_USEFUL_TARGET}" ]; then
        echo "Error: deployment target ${DEPLOYMENT_TARGET} is older than ${MIN_USEFUL_TARGET}; the"
        echo "       linker would emit no chained fixups and gate 1 would pass vacuously."
        exit 1
    fi
    EXPOSURE="deployment target ${DEPLOYMENT_TARGET}"
else
    SYM_PREFIX=
    EXPOSURE="-rdynamic"
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "Static library gates (${PLATFORM})"
echo "  archive:  ${LIBCHDB_A}"
echo "  header:   ${CHDB_H}"
echo "  exposure: ${EXPOSURE}"
echo

failures=0
fail () { echo "FAIL: $*"; failures=$((failures + 1)); }

# Symbols a shared library defines and exports, i.e. the ones an image could bind to
# something other than the bundled copy. Used by gates 1 and 2.
so_exports () {
    if [ "${PLATFORM}" = Darwin ]; then
        # dyld_info reads these out of the dyld shared cache; the dylibs no longer exist as
        # files on modern macOS.
        { dyld_info -exports /usr/lib/libc++.1.dylib
          dyld_info -exports /usr/lib/libc++abi.dylib
          dyld_info -exports /usr/lib/system/libunwind.dylib; } 2>/dev/null \
            | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^__?[A-Za-z_$]/ && $i !~ /^\[/) { print $i; break } }' \
            | sed 's/^_//'
    else
        for lib in libstdc++.so.6 libc++.so.1 libc++abi.so.1 libgcc_s.so.1; do
            path=$(ldconfig -p 2>/dev/null | awk -v l="${lib}" '$1 == l { print $NF; exit }')
            [ -n "${path}" ] && [ -f "${path}" ] && readelf --dyn-syms -W "${path}" 2>/dev/null
        done | elf_visible_defined
    fi
}

# Defined symbols that remain visible outside their own object. Hidden ones are excluded:
# they are exactly what cannot be bound elsewhere. Reads a readelf symbol table on stdin.
elf_visible_defined () {
    awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && $6 == "DEFAULT" && ($5 == "GLOBAL" || $5 == "WEAK") {
             n = $8; sub(/@.*/, "", n); if (n != "") print n
         }'
}

# Same idea for the bundled runtime objects extracted from the archive.
archive_visible_exports () {
    local objdir=$1
    if [ "${PLATFORM}" = Darwin ]; then
        # Only the -m listing spells out "private external"; -g alone shows hidden symbols too.
        find "${objdir}" -name '*.o' -print0 \
            | xargs -0 nm -m -g --defined-only 2>/dev/null \
            | awk '!/private external/ && $NF ~ /^_/ { print substr($NF, 2) }'
    else
        find "${objdir}" -name '*.o' -print0 \
            | xargs -0 -n 50 readelf -sW 2>/dev/null \
            | elf_visible_defined
    fi
}

# Symbols in the linked probe that another image could still supply.
probe_bindable () {
    local probe=$1
    if [ "${PLATFORM}" = Darwin ]; then
        dyld_info -fixups "${probe}" 2>/dev/null | grep '<weak-def-coalesce>' \
            | sed 's|.*<weak-def-coalesce>/_||' || true
    else
        readelf --dyn-syms -W "${probe}" 2>/dev/null | elf_visible_defined
    fi
}

so_exports | sort -u > "${WORK_DIR}/system_exports.txt"
[ -s "${WORK_DIR}/system_exports.txt" ] || { echo "Error: found no system runtime exports to compare against"; exit 1; }

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
    archive_visible_exports "${WORK_DIR}/objs" | sort -u > "${WORK_DIR}/runtime_exports.txt"

    comm -12 "${WORK_DIR}/runtime_exports.txt" "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/overlap.txt"
    overlap=$(wc -l < "${WORK_DIR}/overlap.txt" | tr -d ' ')
    echo "  runtime objects: ${runtime_member_count}, still-visible symbols: $(wc -l < "${WORK_DIR}/runtime_exports.txt" | tr -d ' ')"
    if [ "${overlap}" -eq 0 ]; then
        echo "PASS"
    else
        echo "  first 20 of ${overlap} colliding symbols:"
        head -20 "${WORK_DIR}/overlap.txt" | sed 's/^/    /'
        fail "${overlap} bundled runtime symbols can be bound to the system runtime"
    fi
fi
echo

# --- Build the probe --------------------------------------------------------------------
echo "== Building probe (${EXPOSURE}) =="
cp "${CHDB_H}" "${WORK_DIR}/chdb.h"
cp "${MY_DIR}/static-probe/chdb_static_probe.c" "${WORK_DIR}/"
# Symlinked, not copied: the archive is around a gigabyte.
ln -s "${LIBCHDB_A}" "${WORK_DIR}/libchdb.a"

# -u forces the linker to resolve every symbol in the contract, so a public C API function
# that got hidden or dropped fails the link instead of failing a user months later.
force_flags=()
while read -r symbol; do
    [ -n "${symbol}" ] && force_flags+=("-Wl,-u,${SYM_PREFIX}${symbol}")
done < "${WORK_DIR}/contract.txt"

if [ "${PLATFORM}" = Darwin ]; then
    platform_flags=(-mmacosx-version-min="${DEPLOYMENT_TARGET}" -liconv
                    -framework CoreFoundation -framework Security)
    export MACOSX_DEPLOYMENT_TARGET="${DEPLOYMENT_TARGET}"
else
    # -rdynamic is the point, not an accident: it is what puts default-visibility archive
    # symbols into .dynsym, which is what gate 1 then asserts is empty of runtime symbols.
    platform_flags=(-rdynamic -lpthread -ldl -lm -lrt -Wl,--allow-multiple-definition)
fi

PROBE="${WORK_DIR}/chdb_static_probe"
if (cd "${WORK_DIR}" && clang chdb_static_probe.c -o chdb_static_probe \
            -I. -L. "${force_flags[@]}" -lchdb "${platform_flags[@]}" \
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

# --- Gate 1: what the probe still exposes -----------------------------------------------
echo "== Gate 1: nothing in the probe can bind the bundled runtime elsewhere =="
probe_bindable "${PROBE}" | sort -u > "${WORK_DIR}/probe_bindable.txt"
total_bindable=$(wc -l < "${WORK_DIR}/probe_bindable.txt" | tr -d ' ')
comm -12 "${WORK_DIR}/probe_bindable.txt" "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/probe_overlap.txt"
risky=$(wc -l < "${WORK_DIR}/probe_overlap.txt" | tr -d ' ')

# The gate is the intersection with the system runtime, not the raw total. On Mach-O most of
# the raw records are ClickHouse/abseil/magic_enum template instantiations: weak by
# definition, defined nowhere else, so dyld can only bind them to this image. Measured on a
# pre-fix arm64 archive at deployment target 26.0: 33102 records, 479 system-overlapping,
# probe hangs in chdb_connect. With the bundled runtimes hidden: 29692 records, 0
# system-overlapping, probe passes.
echo "  candidate symbols: ${total_bindable} (informational), resolvable from the system runtime: ${risky}"
if [ "${risky}" -eq 0 ]; then
    echo "PASS"
else
    head -20 "${WORK_DIR}/probe_overlap.txt" | sed 's/^/    /'
    fail "${risky} bundled runtime symbols can be bound to the system runtime"
fi
if [ "${PLATFORM}" = Linux ]; then
    # Informational: a dependency here would mean C++ runtime symbols are being satisfied
    # dynamically rather than from the bundled copy.
    echo "  system C++ runtime linked into the probe: $(ldd "${PROBE}" 2>/dev/null | grep -cE 'libstdc\+\+|libc\+\+') library/libraries"
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
