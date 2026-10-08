#!/bin/bash
#
# Release gates for the static library.
#
# libchdb.so gates its exported surface at link time. libchdb.a is handed to a linker we do
# not control, so the gate has to be baked into the archive itself. On macOS that is the
# hidden visibility of cmake/bundled_runtime_visibility.cmake; on Linux the archive is also
# prelinked into one object that keeps only the C API global (prelink_static_lib.sh),
# because a hidden definition in an archive member still satisfies the consumer's own
# references in a static link (chdb-io/chdb-rust#53). These checks verify that it was.
#
#   Gate 1  nothing in the probe can bind the bundled runtime to the system runtime
#   Gate 2  macOS: the bundled runtime objects export nothing at all
#           Linux: the archive defines nothing global outside the C API contract
#   Gate 3a the two checked-in export allow-lists describe the same C API contract
#   Gate 3b every symbol in that contract is still reachable from libchdb.a
#   Gate 4  the linked probe connects and runs a query
#   Gate 5  the archive does not override libc's posix_spawn
#   Gate 6  Linux: a consumer's own runtime references still bind to the host runtime, and
#           C, C++ and Rust consumers can unwind, throw and panic after chDB has run
#
# Gates 3 and 4 are the same on both platforms, and gate 1 asks the same question with a
# platform-specific scope; only the tools and the condition that exposes the hazard differ.
# Gate 5 has nothing to find on macOS - base/glibc-compatibility is a Linux-only target -
# but its behavioural half is a real check there too, and it is the half that keeps working
# if the archive is ever assembled a different way. Gate 6 is Linux-only because the macOS
# archive is not prelinked (see prelink_static_lib.sh).
#
#   Mach-O  weak definitions that stay external land in the export trie and dyld may bind
#           them to the system libc++/libc++abi. Exposed by a deployment target of 12.0 or
#           newer, which is when ld starts emitting chained fixups: measured against a
#           pre-fix arm64 archive, min=10.15 produced no coalescing records at all while
#           12.0 and up produced 33102.
#   ELF     default-visibility definitions reach .dynsym and become interposable. A plain
#           executable link does not expose them, `-rdynamic` does, so the probe is linked
#           that way deliberately as the ELF equivalent of a modern deployment target.
#
# Gate 1 deliberately does not parse `dyld_info -fixups`. Its `<weak-def-coalesce>` marker
# text is dyld-version specific - on a macOS 14 runner the same hardened archive that yields
# 32779 records locally yields none, which would have reported a vacuous pass. It compares
# symbol sets instead, using only nm/readelf.
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

sort -u "${WORK_DIR}/contract.txt" > "${WORK_DIR}/contract_sorted.txt"

# --- Gate 2: no-link archive check ------------------------------------------------------
if [ "${PLATFORM}" = Darwin ]; then
    # Only the bundled runtime objects are in scope. ClickHouse's own weak/template symbols
    # are a different problem and would swamp the signal.
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

            # Two sets come out of the same dump. runtime_exports is what is still visible
            # from outside the object, which is what this gate asserts is empty.
            # runtime_defined is every definition regardless of visibility, which gate 1
            # uses to scope itself to the bundled runtime. Only the -m listing spells out
            # "private external"; -g alone shows hidden symbols too and would report a
            # hardened archive as unprotected.
            find "${WORK_DIR}/objs" -name '*.o' -print0 \
                | xargs -0 nm -m -g --defined-only > "${WORK_DIR}/nm.txt"
            awk '!/private external/ && $NF ~ /^_/ { print substr($NF, 2) }' "${WORK_DIR}/nm.txt" \
                | sort -u > "${WORK_DIR}/runtime_exports.txt"
            awk '$NF ~ /^_/ { print substr($NF, 2) }' "${WORK_DIR}/nm.txt" \
                | sort -u > "${WORK_DIR}/runtime_defined.txt"

            # Asserted at zero, not merely "disjoint from this host's system runtime". These
            # three targets are built to have no externally visible definitions at all, and a
            # symbol that is simply absent from the running OS version would otherwise slip
            # through. Measured 0 across 58 archive members.
            visible=$(wc -l < "${WORK_DIR}/runtime_exports.txt" | tr -d ' ')
            comm -12 "${WORK_DIR}/runtime_exports.txt" "${WORK_DIR}/system_exports.txt" \
                > "${WORK_DIR}/overlap.txt"
            overlap=$(wc -l < "${WORK_DIR}/overlap.txt" | tr -d ' ')
            echo "  runtime objects: ${runtime_member_count}, externally visible definitions: ${visible}"
            if [ "${visible}" -eq 0 ]; then
                echo "PASS"
            else
                echo "  first 20 (${overlap} of ${visible} are also defined by the system runtime):"
                if [ "${overlap}" -gt 0 ]; then
                    head -20 "${WORK_DIR}/overlap.txt"
                else
                    head -20 "${WORK_DIR}/runtime_exports.txt"
                fi | sed 's/^/    /'
                fail "${visible} bundled runtime symbols are still externally visible"
            fi
        fi
    fi
else
    # Hidden visibility is not enough on ELF: a hidden definition in an archive member still
    # satisfies a reference from any other object in the consumer's static link, which is
    # how Rust's std ended up calling the bundled libunwind (chdb-io/chdb-rust#53).
    # prelink_static_lib.sh resolves chDB's own references inside one object and localizes
    # everything else, so the contract must be the archive's entire global surface. Checked
    # at every visibility and including COMMON symbols, and against the contract rather than
    # this host's system runtime: whatever else stays global, a consumer can bind to.
    echo "== Gate 2: the archive defines nothing global outside the C API contract =="
    if ! readelf -sW "${LIBCHDB_A}" > "${WORK_DIR}/archive_readelf.txt" 2> "${WORK_DIR}/archive_readelf.err"; then
        sed 's/^/    /' "${WORK_DIR}/archive_readelf.err" | tail -5
        fail "readelf could not read the archive's symbol tables"
    elif ! grep -q '^Symbol table' "${WORK_DIR}/archive_readelf.txt"; then
        fail "readelf printed no symbol table for the archive - gate 2 would pass vacuously"
    else
        awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && ($5 == "GLOBAL" || $5 == "WEAK" || $5 == "UNIQUE") {
                 n = $8; sub(/@.*/, "", n); if (n != "") print n
             }' "${WORK_DIR}/archive_readelf.txt" | sort -u > "${WORK_DIR}/archive_globals.txt"
        # The contract's own symbols must stay linkable from a consumer's shared object too.
        awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && ($5 == "GLOBAL" || $5 == "WEAK") && $6 == "DEFAULT" {
                 n = $8; sub(/@.*/, "", n); if (n != "") print n
             }' "${WORK_DIR}/archive_readelf.txt" | sort -u > "${WORK_DIR}/archive_default_globals.txt"
        comm -23 "${WORK_DIR}/archive_globals.txt" "${WORK_DIR}/contract_sorted.txt" > "${WORK_DIR}/extra_globals.txt"
        comm -13 "${WORK_DIR}/archive_default_globals.txt" "${WORK_DIR}/contract_sorted.txt" > "${WORK_DIR}/missing_globals.txt"
        comm -12 "${WORK_DIR}/extra_globals.txt" "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/overlap.txt"
        extra=$(wc -l < "${WORK_DIR}/extra_globals.txt" | tr -d ' ')
        missing=$(wc -l < "${WORK_DIR}/missing_globals.txt" | tr -d ' ')
        overlap=$(wc -l < "${WORK_DIR}/overlap.txt" | tr -d ' ')

        # A COMMON symbol at any binding is still allocated by the consumer's linker, and a
        # local one breaks the link outright (`ld.lld -r` leaves OpenSSL's hidden
        # OPENSSL_ia32cap_P common; prelink_static_lib.sh uses `ld.bfd -r -d` to allocate it).
        commons=$(awk '$1 ~ /^[0-9]+:$/ && $7 == "COM"' "${WORK_DIR}/archive_readelf.txt" | wc -l | tr -d ' ')

        # The reverse direction: the bundled runtime must be complete, so nothing in the archive
        # may need the system's C++ runtime or unwinder. If a runtime member went missing,
        # chDB's own references would silently bind to libstdc++/libgcc_s instead.
        awk '$1 ~ /^[0-9]+:$/ && $7 == "UND" && $5 != "LOCAL" { n = $8; sub(/@.*/, "", n); if (n != "") print n }' \
            "${WORK_DIR}/archive_readelf.txt" | sort -u > "${WORK_DIR}/archive_undefined.txt"
        comm -12 "${WORK_DIR}/archive_undefined.txt" "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/needs_system_runtime.txt"
        needs_runtime=$(wc -l < "${WORK_DIR}/needs_system_runtime.txt" | tr -d ' ')

        # A COMDAT group whose signature was localized is still matched by name in the
        # consumer's link, so it could be discarded in favour of the host's same-named group.
        # prelink_static_lib.sh dissolves them all.
        if ! readelf -gW "${LIBCHDB_A}" > "${WORK_DIR}/archive_groups.txt" 2>&1; then
            fail "readelf could not list the archive's section groups"
        fi
        groups=$(grep_optional -c 'group section \[' "${WORK_DIR}/archive_groups.txt")

        echo "  global definitions: $(wc -l < "${WORK_DIR}/archive_globals.txt" | tr -d ' '), outside the contract: ${extra} (${overlap} also defined by the system runtime), contract symbols missing: ${missing}"
        echo "  section groups: ${groups}, COMMON symbols: ${commons}, undefined references: $(wc -l < "${WORK_DIR}/archive_undefined.txt" | tr -d ' ') (${needs_runtime} into the system C++ runtime)"
        if [ "${extra}" -ne 0 ]; then
            echo "  first 20:"
            if [ "${overlap}" -gt 0 ]; then
                head -20 "${WORK_DIR}/overlap.txt"
            else
                head -20 "${WORK_DIR}/extra_globals.txt"
            fi | sed 's/^/    /'
            fail "${extra} symbols outside the C API contract are still global - was the archive prelinked by prelink_static_lib.sh?"
        fi
        if [ "${missing}" -ne 0 ]; then
            head -20 "${WORK_DIR}/missing_globals.txt" | sed 's/^/    /'
            fail "${missing} contract symbols are not default-visibility global definitions in the archive"
        fi
        if [ "${groups}" -ne 0 ]; then
            fail "${groups} section groups survived prelinking"
        fi
        if [ "${commons}" -ne 0 ]; then
            fail "${commons} COMMON symbols survived prelinking"
        fi
        if [ "${needs_runtime}" -ne 0 ]; then
            head -20 "${WORK_DIR}/needs_system_runtime.txt" | sed 's/^/    /'
            fail "the archive needs ${needs_runtime} symbols from the system C++ runtime"
        fi
        if [ "${extra}" -eq 0 ] && [ "${missing}" -eq 0 ] && [ "${groups}" -eq 0 ] \
                && [ "${commons}" -eq 0 ] && [ "${needs_runtime}" -eq 0 ]; then
            echo "PASS"
        fi
    fi
fi
echo

# --- Build the probe --------------------------------------------------------------------
echo "== Building probes (${EXPOSURE}) =="
cp "${CHDB_H}" "${WORK_DIR}/chdb.h"
cp "${MY_DIR}/static-probe/chdb_static_probe.c" "${WORK_DIR}/"
cp "${MY_DIR}/static-probe/posix_spawn_probe.c" "${WORK_DIR}/"
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
    # No --allow-multiple-definition: a consumer does not pass it, and it would hide a clash
    # between the probe and the archive. The prelinked archive cannot carry a duplicate
    # definition anyway - `ld -r` rejects one.
    platform_flags=(-rdynamic -lpthread -ldl -lm -lrt)
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

# --- Gate 1: what the linked probe still exposes ---------------------------------------
# Symbols that remain visible from outside the linked image, so another image could supply
# them instead. Returns non-zero if the tool itself failed, so that is never mistaken for
# "nothing is exposed".
probe_visible_symbols () {
    local probe=$1 raw="${WORK_DIR}/probe_raw.txt"
    if [ "${PLATFORM}" = Darwin ]; then
        nm -m -g --defined-only "${probe}" > "${raw}" 2>/dev/null || return 1
        [ -s "${raw}" ] || return 1
        awk '!/private external/ && $NF ~ /^_/ { print substr($NF, 2) }' "${raw}"
    else
        readelf --dyn-syms -W "${probe}" > "${raw}" 2>/dev/null || return 1
        [ -s "${raw}" ] || return 1
        elf_visible_defined < "${raw}"
    fi
}

echo "== Gate 1: no bundled runtime symbol is visible from the linked probe =="
if [ "${PLATFORM}" = Darwin ]; then
    # Scoped to symbols the bundled runtime actually defines. Intersecting the probe's whole
    # visible set with the system runtime instead would flag the replaceable global operator
    # new/delete, which clickhouse_new_delete provides at default visibility on purpose on
    # macOS and which is outside these three targets; gate 2 is what covers the runtime's
    # own copies.
    SCOPE="${WORK_DIR}/runtime_defined.txt"
    SCOPE_NAME="runtime definitions"
else
    # The prelinked archive has no runtime members left to scope by, and gate 2 already
    # requires everything but the contract to be local. What a linked consumer must still not
    # re-export is anything the system runtime itself exports.
    SCOPE="${WORK_DIR}/system_exports.txt"
    SCOPE_NAME="system runtime exports"
fi
if [ ! -s "${SCOPE}" ]; then
    fail "no ${SCOPE_NAME} to scope gate 1 to"
elif ! probe_visible_symbols "${PROBE}" > "${WORK_DIR}/probe_visible_unsorted.txt"; then
    fail "could not read the probe's symbol table; gate 1 was not evaluated"
else
    sort -u "${WORK_DIR}/probe_visible_unsorted.txt" > "${WORK_DIR}/probe_visible.txt"
    probe_visible=$(wc -l < "${WORK_DIR}/probe_visible.txt" | tr -d ' ')
    scope_size=$(wc -l < "${SCOPE}" | tr -d ' ')
    comm -12 "${SCOPE}" "${WORK_DIR}/probe_visible.txt" > "${WORK_DIR}/leaked.txt"
    leaked=$(wc -l < "${WORK_DIR}/leaked.txt" | tr -d ' ')

    echo "  ${SCOPE_NAME}: ${scope_size}, visible from the probe: ${probe_visible}, leaked: ${leaked}"
    if [ "${probe_visible}" -eq 0 ]; then
        # A linked binary always exposes something. Zero means the extraction produced
        # nothing, so the intersection below would be empty whatever the archive contains.
        fail "the probe exposes no symbols at all - extraction is broken, gate 1 would pass vacuously"
    elif [ "${leaked}" -eq 0 ]; then
        echo "PASS"
    else
        head -20 "${WORK_DIR}/leaked.txt" | sed 's/^/    /'
        fail "${leaked} bundled runtime symbols are visible from the linked probe"
    fi
fi
echo

# --- Gate 4: runtime smoke test ---------------------------------------------------------
# The whole point of the exercise: the historical bug built cleanly and hung at run time.
#
# A watchdog, not a nicety: one failure these gates exist for is a hang, and macOS has no
# coreutils `timeout`.
run_probe () {
    local probe=$1 probe_pid watchdog_pid rc=0
    (cd "${WORK_DIR}" && "./${probe}") &
    probe_pid=$!
    ( sleep "${PROBE_TIMEOUT}"; kill -9 "${probe_pid}" 2>/dev/null ) &
    watchdog_pid=$!
    disown "${watchdog_pid}" 2>/dev/null || true
    wait "${probe_pid}" || rc=$?
    kill "${watchdog_pid}" 2>/dev/null || true
    return "${rc}"
}

echo "== Gate 4: probe connects and runs a query =="
if run_probe chdb_static_probe; then
    echo "PASS"
else
    fail "probe did not complete successfully (killed after ${PROBE_TIMEOUT}s if it hung)"
fi
echo

# --- Gate 5: posix_spawn stays libc's ---------------------------------------------------
# base/glibc-compatibility ships a partial posix_spawn that ignores file actions and reports
# success anyway. Inside the ClickHouse binary that is invisible; in an archive handed to a
# linker chdb does not control it displaces libc's definition for the whole program, and a
# consumer's dup2/chdir/close requests disappear without an error - chdb-io/chdb-core#216.
#
# Two halves, because either one alone can be satisfied for the wrong reason. The symbol
# half says the archive defines nothing in the family; the behavioural half links a second
# probe against the archive and checks that the posix_spawn it actually reaches honours a
# dup2 file action, which is the property a consumer depends on however the archive is built.
echo "== Gate 5: the archive does not override libc's posix_spawn =="
if ! nm -g --defined-only "${LIBCHDB_A}" > "${WORK_DIR}/archive_defined_raw.txt" 2>/dev/null; then
    fail "nm could not read the archive; the symbol half of gate 5 was not evaluated"
elif [ ! -s "${WORK_DIR}/archive_defined_raw.txt" ]; then
    fail "nm read no defined symbol out of the archive - the symbol half would pass vacuously"
else
    # A definition line is `<addr> <type> <name>`, or `<type> <name>` where nm has no
    # address to print; keying on the one-letter type field takes both and leaves out the
    # archive member banners and blank lines. The Mach-O underscore is stripped so the
    # match below is one pattern rather than two.
    awk 'NF >= 2 && $(NF - 1) ~ /^[A-Za-z]$/ { n = $NF; sub(/^_/, "", n); print n }' \
        "${WORK_DIR}/archive_defined_raw.txt" \
        | sort -u > "${WORK_DIR}/archive_defined.txt"
    grep_optional -E '^posix_spawn' "${WORK_DIR}/archive_defined.txt" \
        > "${WORK_DIR}/spawn_defined.txt"
    spawn_defined=$(wc -l < "${WORK_DIR}/spawn_defined.txt" | tr -d ' ')
    echo "  archive definitions: $(wc -l < "${WORK_DIR}/archive_defined.txt" | tr -d ' '), in the posix_spawn family: ${spawn_defined}"
    if [ "${spawn_defined}" -eq 0 ]; then
        echo "PASS (symbols)"
    else
        sed 's/^/    /' "${WORK_DIR}/spawn_defined.txt"
        fail "${spawn_defined} posix_spawn symbols are defined by the archive"
    fi
fi

if (cd "${WORK_DIR}" && clang posix_spawn_probe.c -o posix_spawn_probe \
            -L. -lchdb "${platform_flags[@]}" \
        > "${WORK_DIR}/spawn_link.log" 2>&1); then
    if run_probe posix_spawn_probe; then
        echo "PASS (behaviour)"
    else
        fail "the posix_spawn reached through the archive dropped its file actions"
    fi
else
    sed 's/^/    /' "${WORK_DIR}/spawn_link.log" | tail -40
    fail "could not link the posix_spawn probe against the archive"
fi
echo

# --- Gate 6: a consumer's own runtime stays the host's ----------------------------------
# chdb-io/chdb-rust#53. Every probe above uses nothing but the C API, so none of them could
# notice that a consumer's OWN references to the unwinder and the C++ ABI were being bound
# to the copies bundled in libchdb.a. These probes use the host runtime themselves, are
# built with the host toolchain, and are linked the way a consumer links: no -rdynamic, no
# --allow-multiple-definition.
#
# Two halves again. The symbol half takes every reference the probe's own object makes into
# the system runtime and requires the linked binary to import it from the versioned system
# library; a reference bound inside the executable has no versioned .dynsym entry. That
# catches a split binding even where the mix happens not to crash. The behavioural half runs
# the probes: an unwinder walk, C++ throw/catch, and the Rust backtrace and panic of the
# issue.
#
# Linux only: the macOS archive is not prelinked, and its split binding is between two
# copies of LLVM libunwind (see prelink_static_lib.sh).
check_host_bindings () {
    local object=$1 probe=$2 name
    name=$(basename "${probe}")
    if ! nm -u "${object}" > "${WORK_DIR}/${name}_undefined_raw.txt" 2>/dev/null; then
        fail "${name}: nm could not read ${object}; the symbol half was not evaluated"
        return
    fi
    awk 'NF { n = $NF; sub(/@.*/, "", n); print n }' "${WORK_DIR}/${name}_undefined_raw.txt" \
        | sort -u | comm -12 - "${WORK_DIR}/system_exports.txt" > "${WORK_DIR}/${name}_refs.txt"
    if [ ! -s "${WORK_DIR}/${name}_refs.txt" ]; then
        fail "${name}: its object references nothing in the system runtime - the symbol half would pass vacuously"
        return
    fi
    if ! readelf --dyn-syms -W "${probe}" > "${WORK_DIR}/${name}_dynsym.txt" 2>/dev/null; then
        fail "${name}: readelf could not read the linked probe; the symbol half was not evaluated"
        return
    fi
    awk '$1 ~ /^[0-9]+:$/ && $8 ~ /@/ { n = $8; sub(/@.*/, "", n); print n }' "${WORK_DIR}/${name}_dynsym.txt" \
        | sort -u > "${WORK_DIR}/${name}_imported.txt"
    comm -23 "${WORK_DIR}/${name}_refs.txt" "${WORK_DIR}/${name}_imported.txt" \
        > "${WORK_DIR}/${name}_bound_inside.txt"
    local refs inside
    refs=$(wc -l < "${WORK_DIR}/${name}_refs.txt" | tr -d ' ')
    inside=$(wc -l < "${WORK_DIR}/${name}_bound_inside.txt" | tr -d ' ')
    if [ "${inside}" -eq 0 ]; then
        echo "PASS (${name}: all ${refs} references into the system runtime are imported from it)"
    else
        head -20 "${WORK_DIR}/${name}_bound_inside.txt" | sed 's/^/    /'
        fail "${name}: ${inside} of its ${refs} references into the system runtime were bound inside the executable - to libchdb.a"
    fi
}

if [ "${PLATFORM}" = Linux ]; then
    echo "== Gate 6: a consumer's own runtime references stay the host's (chdb-io/chdb-rust#53) =="
    HOST_CC=${HOST_CC:-gcc}
    HOST_CXX=${HOST_CXX:-g++}
    consumer_flags=(-L. -lchdb -lpthread -ldl -lm -lrt)
    cp "${MY_DIR}/static-probe/host_unwind_probe.c" "${MY_DIR}/static-probe/host_cxx_probe.cpp" \
        "${MY_DIR}/static-probe/rust_unwind_probe.rs" "${WORK_DIR}/"
    # The C++ probe goes through chdb.hpp, which ships next to chdb.h in the static tarball.
    cp "$(dirname "${CHDB_H}")/chdb.hpp" "${WORK_DIR}/"

    # Built concurrently: with Ubuntu 24.04's GNU ld every link of this archive takes minutes
    # on aarch64, where binutils walks its whole stub table for every input section.
    (cd "${WORK_DIR}" && { "${HOST_CC}" -I. -c host_unwind_probe.c -o host_unwind_probe.o \
            && "${HOST_CC}" host_unwind_probe.o -o host_unwind_probe "${consumer_flags[@]}"; } \
        > "${WORK_DIR}/host_unwind_link.log" 2>&1) &
    unwind_build=$!
    (cd "${WORK_DIR}" && { "${HOST_CXX}" -std=c++20 -I. -c host_cxx_probe.cpp -o host_cxx_probe.o \
            && "${HOST_CXX}" host_cxx_probe.o -o host_cxx_probe "${consumer_flags[@]}"; } \
        > "${WORK_DIR}/host_cxx_link.log" 2>&1) &
    cxx_build=$!
    # Linked exactly like chdb-rust's `static` feature.
    rust_build=
    if command -v rustc > /dev/null 2>&1; then
        (cd "${WORK_DIR}" && rustc --edition=2021 -C debuginfo=1 rust_unwind_probe.rs -o rust_unwind_probe \
                -L native=. -l static=chdb -l dylib=stdc++ \
            > "${WORK_DIR}/rust_link.log" 2>&1) &
        rust_build=$!
    fi

    if wait "${unwind_build}"; then
        check_host_bindings "${WORK_DIR}/host_unwind_probe.o" "${WORK_DIR}/host_unwind_probe"
        # The archive used to give every GNU ld consumer an executable stack: dozens of its
        # assembly members carry no .note.GNU-stack. prelink_static_lib.sh passes -z noexecstack.
        stack_flags=$(readelf -lW "${WORK_DIR}/host_unwind_probe" | awk '$1 == "GNU_STACK" { print $7 }')
        case "${stack_flags}" in
            "") fail "host_unwind_probe: no GNU_STACK program header found" ;;
            *E*) fail "host_unwind_probe: linking libchdb.a made the consumer's stack executable (${stack_flags})" ;;
            *) echo "PASS (host_unwind_probe: stack is not executable)" ;;
        esac
        if run_probe host_unwind_probe; then
            echo "PASS (host_unwind_probe: behaviour)"
        else
            fail "host_unwind_probe: the host unwinder could not walk the consumer's stack"
        fi
    else
        sed 's/^/    /' "${WORK_DIR}/host_unwind_link.log" | tail -40
        fail "could not build host_unwind_probe with ${HOST_CC}"
    fi

    if wait "${cxx_build}"; then
        check_host_bindings "${WORK_DIR}/host_cxx_probe.o" "${WORK_DIR}/host_cxx_probe"
        if run_probe host_cxx_probe; then
            echo "PASS (host_cxx_probe: behaviour)"
        else
            fail "host_cxx_probe: the consumer's own C++ exceptions broke"
        fi
    else
        sed 's/^/    /' "${WORK_DIR}/host_cxx_link.log" | tail -40
        fail "could not build host_cxx_probe with ${HOST_CXX}"
    fi

    # Its references come from std's rlibs rather than one object, so it only has the
    # behavioural half - which is the issue's reproducer itself. Both Linux CI jobs put rustc
    # on PATH, so there a missing rustc is a failure rather than a skip.
    if [ -n "${rust_build}" ]; then
        if wait "${rust_build}"; then
            if run_probe rust_unwind_probe; then
                echo "PASS (rust_unwind_probe: behaviour)"
            else
                fail "rust_unwind_probe: a Rust backtrace or panic broke"
            fi
        else
            sed 's/^/    /' "${WORK_DIR}/rust_link.log" | tail -40
            fail "could not build rust_unwind_probe with $(command -v rustc)"
        fi
    elif [ -n "${CI:-}" ]; then
        fail "rust_unwind_probe: rustc is not on PATH"
    else
        echo "SKIP (rust_unwind_probe: rustc is not on PATH)"
    fi
    echo
fi

if [ "${failures}" -ne 0 ]; then
    echo "${failures} gate(s) failed"
    exit 1
fi
echo "All static library gates passed"
