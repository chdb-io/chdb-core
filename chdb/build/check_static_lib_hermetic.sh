#!/bin/bash
#
# Release gates for the static library.
#
# libchdb.so gates its exported surface at link time. libchdb.a is handed to a linker we do
# not control, so the gate has to be baked into the objects
# (cmake/bundled_runtime_visibility.cmake). These checks verify that it was.
#
#   Gate 1  nothing in the probe can bind the bundled runtime to the system runtime
#   Gate 2  the bundled runtime objects export nothing at all
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
#           executable link does not expose them, `-rdynamic` does, so the probe is linked
#           that way deliberately as the ELF equivalent of a modern deployment target.
#
# Note both platforms need a visibility-aware tool: a hidden symbol is still a global
# definition in the symbol table on Mach-O and ELF alike, so plain `nm -g` cannot see the
# difference and would report a hardened archive as unprotected.
#
# Every extraction step below is checked for tool failure separately from its result being
# empty. An empty result is what a clean archive looks like, so conflating the two lets a
# broken `dyld_info` or `readelf` invocation pass the gates vacuously.
#
# Usage: check_static_lib_hermetic.sh [path/to/libchdb.a] [path/to/chdb.h]

set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJ_DIR="$( cd "${MY_DIR}/../.." >/dev/null 2>&1 && pwd )"

LIBCHDB_A=${1:-${PROJ_DIR}/libchdb.a}
CHDB_H=${2:-${PROJ_DIR}/programs/local/chdb.h}
MIN_USEFUL_MAJOR=12

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

# Gate 4 kills a hung probe from a background watchdog. A non-numeric value would make that
# `sleep` fail immediately, the watchdog would exit without killing anything, and `wait`
# would block forever - so reject it here rather than hanging the build.
PROBE_TIMEOUT=${PROBE_TIMEOUT:-120}
if ! printf '%s' "${PROBE_TIMEOUT}" | grep -Eq '^[0-9]+$'; then
    echo "Error: PROBE_TIMEOUT must be a whole number of seconds, got '${PROBE_TIMEOUT}'"
    exit 1
fi

# Mach-O prefixes every C symbol with an underscore; ELF does not. Everything downstream
# compares bare names, so record the prefix once and apply it only when building link flags.
if [ "${PLATFORM}" = Darwin ]; then
    SYM_PREFIX=_
    # The bundled runtimes shadow these three. All are in the dyld shared cache on every
    # supported macOS, so all three must be readable.
    SYS_LIBS_REQUIRED="/usr/lib/libc++.1.dylib /usr/lib/libc++abi.dylib /usr/lib/system/libunwind.dylib"
    SYS_LIBS_OPTIONAL=""
    # Default to the host's own OS version: it is the newest target whose binaries this
    # machine can still run, and gate 4 has to run the probe. Override with
    # MACOSX_DEPLOYMENT_TARGET, but not below the floor checked next.
    DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-$(sw_vers -productVersion | cut -d. -f1).0}
    # Compared as a bare major version rather than with `sort -V` or `sort -t. -k`: both
    # depend on which sort dialect is installed, and the floor only needs the major number
    # (macOS 11 and 10.x are below it, 12 and up are above).
    target_major=${DEPLOYMENT_TARGET%%.*}
    if ! printf '%s' "${target_major}" | grep -Eq '^[0-9]+$'; then
        echo "Error: could not read a major version out of deployment target '${DEPLOYMENT_TARGET}'"
        exit 1
    fi
    if [ "${target_major}" -lt "${MIN_USEFUL_MAJOR}" ]; then
        echo "Error: deployment target ${DEPLOYMENT_TARGET} is older than ${MIN_USEFUL_MAJOR}.0; the"
        echo "       linker would emit no chained fixups and gate 1 would pass vacuously."
        exit 1
    fi
    EXPOSURE="deployment target ${DEPLOYMENT_TARGET}"
else
    SYM_PREFIX=
    # libstdc++ and libgcc_s are what a Linux host always has and are what the bundled
    # runtime actually collides with. libc++ is only present on some distributions, so it is
    # folded in when available rather than required.
    SYS_LIBS_REQUIRED="libstdc++.so.6 libgcc_s.so.1"
    SYS_LIBS_OPTIONAL="libc++.so.1 libc++abi.so.1"
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

# grep exit 1 means "no matches", a legitimate outcome here. Exit 2 and up is a real error
# and must not be swallowed.
grep_optional () { grep "$@" || [ $? -eq 1 ]; }

# Defined symbols that remain visible outside their own object; hidden ones are excluded
# because they are exactly what cannot be bound elsewhere. Reads a readelf symbol table.
elf_visible_defined () {
    awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && $6 == "DEFAULT" && ($5 == "GLOBAL" || $5 == "WEAK") {
             n = $8; sub(/@.*/, "", n); if (n != "") print n
         }'
}

# Exported symbols of one system shared library. Returns non-zero if the library cannot be
# read at all, which the caller distinguishes from "read fine, exports nothing".
dump_so_exports () {
    local lib=$1 raw="${WORK_DIR}/one_so.txt"
    if [ "${PLATFORM}" = Darwin ]; then
        dyld_info -exports "${lib}" > "${raw}" 2>/dev/null || return 1
        [ -s "${raw}" ] || return 1
        awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^__?[A-Za-z_$]/ && $i !~ /^\[/) { print $i; break } }' "${raw}" \
            | sed 's/^_//'
    else
        local path
        path=$(ldconfig -p 2>/dev/null | awk -v l="${lib}" '$1 == l { print $NF; exit }') || true
        [ -n "${path}" ] && [ -f "${path}" ] || return 1
        readelf --dyn-syms -W "${path}" > "${raw}" 2>/dev/null || return 1
        [ -s "${raw}" ] || return 1
        elf_visible_defined < "${raw}"
    fi
}

echo "== System runtime exports (comparison base) =="
: > "${WORK_DIR}/system_exports_raw.txt"
for lib in ${SYS_LIBS_REQUIRED} ${SYS_LIBS_OPTIONAL}; do
    if dump_so_exports "${lib}" > "${WORK_DIR}/lib_syms.txt" && [ -s "${WORK_DIR}/lib_syms.txt" ]; then
        printf '  %-40s %s symbols\n' "${lib}" "$(sort -u "${WORK_DIR}/lib_syms.txt" | wc -l | tr -d ' ')"
        cat "${WORK_DIR}/lib_syms.txt" >> "${WORK_DIR}/system_exports_raw.txt"
    else
        case " ${SYS_LIBS_REQUIRED} " in
            *" ${lib} "*)
                echo "Error: could not read exports from ${lib}. The comparison base would be"
                echo "       incomplete, which would make gates 1 and 2 pass vacuously."
                exit 1 ;;
            *) printf '  %-40s not present (optional)\n' "${lib}" ;;
        esac
    fi
done
sort -u "${WORK_DIR}/system_exports_raw.txt" > "${WORK_DIR}/system_exports.txt"
echo "  total: $(wc -l < "${WORK_DIR}/system_exports.txt" | tr -d ' ') distinct symbols"
echo

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
echo "== Gate 2: bundled runtime objects export nothing =="
if ! ar t "${LIBCHDB_A}" > "${WORK_DIR}/all_members.txt"; then
    fail "could not list the archive members"
else
    grep_optional -E '^lib(cxx|cxxabi|unwind)__' "${WORK_DIR}/all_members.txt" \
        > "${WORK_DIR}/runtime_members.txt"
    runtime_member_count=$(wc -l < "${WORK_DIR}/runtime_members.txt" | tr -d ' ')
    if [ "${runtime_member_count}" -eq 0 ]; then
        fail "no libcxx__/libcxxabi__/libunwind__ members among the $(wc -l < "${WORK_DIR}/all_members.txt" | tr -d ' ') archive members - has the naming in create_static_libchdb.py changed?"
    else
        mkdir -p "${WORK_DIR}/objs"
        (cd "${WORK_DIR}/objs" && xargs ar x "${LIBCHDB_A}" < "${WORK_DIR}/runtime_members.txt")
        extracted=$(find "${WORK_DIR}/objs" -name '*.o' | wc -l | tr -d ' ')
        if [ "${extracted}" -ne "${runtime_member_count}" ]; then
            fail "extracted ${extracted} of ${runtime_member_count} runtime members"
        fi

        if [ "${PLATFORM}" = Darwin ]; then
            # Only the -m listing spells out "private external"; -g alone shows hidden
            # symbols too and would report a hardened archive as unprotected.
            find "${WORK_DIR}/objs" -name '*.o' -print0 \
                | xargs -0 nm -m -g --defined-only > "${WORK_DIR}/nm.txt"
            awk '!/private external/ && $NF ~ /^_/ { print substr($NF, 2) }' "${WORK_DIR}/nm.txt" \
                | sort -u > "${WORK_DIR}/runtime_exports.txt"
        else
            find "${WORK_DIR}/objs" -name '*.o' -print0 \
                | xargs -0 -n 50 readelf -sW > "${WORK_DIR}/readelf.txt"
            elf_visible_defined < "${WORK_DIR}/readelf.txt" | sort -u > "${WORK_DIR}/runtime_exports.txt"
        fi

        # Asserted at zero, not merely "disjoint from this host's system runtime". These
        # three targets are built to have no externally visible definitions at all, and a
        # symbol that is simply absent from the running OS version would otherwise slip
        # through. Measured 0 across 58 archive members on macOS and 83 objects on Linux.
        visible=$(wc -l < "${WORK_DIR}/runtime_exports.txt" | tr -d ' ')
        comm -12 "${WORK_DIR}/runtime_exports.txt" "${WORK_DIR}/system_exports.txt" \
            > "${WORK_DIR}/overlap.txt"
        overlap=$(wc -l < "${WORK_DIR}/overlap.txt" | tr -d ' ')
        echo "  runtime objects: ${runtime_member_count}, externally visible definitions: ${visible}"
        if [ "${visible}" -eq 0 ]; then
            echo "PASS"
        else
            echo "  first 20 (${overlap} of ${visible} are also defined by the system runtime):"
            { [ "${overlap}" -gt 0 ] && cat "${WORK_DIR}/overlap.txt" || cat "${WORK_DIR}/runtime_exports.txt"; } \
                | head -20 | sed 's/^/    /'
            fail "${visible} bundled runtime symbols are still externally visible"
        fi
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
    # symbols into .dynsym, which is what gate 1 then asserts is free of runtime symbols.
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
# Symbols in the linked probe that another image could still supply. Returns non-zero if the
# inspection tool itself failed, so that is not mistaken for "nothing to bind".
probe_candidates () {
    local probe=$1 raw="${WORK_DIR}/probe_raw.txt"
    if [ "${PLATFORM}" = Darwin ]; then
        dyld_info -fixups "${probe}" > "${raw}" 2>/dev/null || return 1
        [ -s "${raw}" ] || return 1
        grep_optional '<weak-def-coalesce>' "${raw}" | sed 's|.*<weak-def-coalesce>/_||'
    else
        readelf --dyn-syms -W "${probe}" > "${raw}" 2>/dev/null || return 1
        [ -s "${raw}" ] || return 1
        elf_visible_defined < "${raw}"
    fi
}

echo "== Gate 1: nothing in the probe can bind the bundled runtime elsewhere =="
if ! probe_candidates "${PROBE}" > "${WORK_DIR}/probe_bindable_unsorted.txt"; then
    fail "could not inspect the probe's fixups/dynamic symbols; gate 1 was not evaluated"
else
    sort -u "${WORK_DIR}/probe_bindable_unsorted.txt" > "${WORK_DIR}/probe_bindable.txt"
    total_bindable=$(wc -l < "${WORK_DIR}/probe_bindable.txt" | tr -d ' ')
    comm -12 "${WORK_DIR}/probe_bindable.txt" "${WORK_DIR}/system_exports.txt" \
        > "${WORK_DIR}/probe_overlap.txt"
    risky=$(wc -l < "${WORK_DIR}/probe_overlap.txt" | tr -d ' ')

    # The gate is the intersection with the system runtime, not the raw total. On Mach-O most
    # of the raw records are ClickHouse/abseil/magic_enum template instantiations: weak by
    # definition, defined nowhere else, so dyld can only bind them to this image. Measured on
    # a pre-fix arm64 archive at deployment target 26.0: 33102 records, 479
    # system-overlapping, probe hangs in chdb_connect. With the bundled runtimes hidden:
    # 29692 records, 0 system-overlapping, probe passes.
    echo "  candidate symbols: ${total_bindable} (informational), resolvable from the system runtime: ${risky}"
    if [ "${total_bindable}" -eq 0 ]; then
        # A real binary always has some: template instantiations on Mach-O, crt and the
        # CHDB_EXPORT C API in .dynsym on ELF. Zero means the extraction produced nothing,
        # so the overlap below would be zero no matter what the archive contains.
        fail "no candidate symbols found at all - extraction is broken, gate 1 would pass vacuously"
    elif [ "${risky}" -eq 0 ]; then
        echo "PASS"
    else
        head -20 "${WORK_DIR}/probe_overlap.txt" | sed 's/^/    /'
        fail "${risky} bundled runtime symbols can be bound to the system runtime"
    fi
fi
if [ "${PLATFORM}" = Linux ]; then
    # Informational: a dependency here would mean C++ runtime symbols are being satisfied
    # dynamically rather than from the bundled copy.
    echo "  system C++ runtime shared libraries linked into the probe: $(ldd "${PROBE}" 2>/dev/null | grep -c 'libstdc++\|libc++' || true)"
fi
echo

# --- Gate 4: runtime smoke test ---------------------------------------------------------
# The whole point of the exercise: the historical bug built cleanly and hung at run time.
echo "== Gate 4: probe connects and runs a query =="
# A watchdog, not a nicety: the failure this gate exists for is a hang, and macOS has no
# coreutils `timeout`.
(cd "${WORK_DIR}" && ./chdb_static_probe) &
probe_pid=$!
( sleep "${PROBE_TIMEOUT}"; kill -9 "${probe_pid}" 2>/dev/null ) &
watchdog_pid=$!
disown "${watchdog_pid}" 2>/dev/null || true
if wait "${probe_pid}"; then
    echo "PASS"
else
    fail "probe did not complete successfully (killed after ${PROBE_TIMEOUT}s if it hung)"
fi
kill "${watchdog_pid}" 2>/dev/null || true
echo

if [ "${failures}" -ne 0 ]; then
    echo "${failures} gate(s) failed"
    exit 1
fi
echo "All static library gates passed"
