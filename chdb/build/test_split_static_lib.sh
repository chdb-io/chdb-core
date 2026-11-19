#!/bin/bash
# Usage: test_split_static_lib.sh
#
# Tests split_static_lib.py on small fixture libraries (split-fixture/), in seconds and without
# a chDB build. check_static_lib_hermetic.sh and check_static_lib_parity.sh check the real
# archive; this checks the transformation itself, on cases built to have each property the
# isolation of chdb-io/chdb-core#261 depends on, across shards and against a host that
# defines the same names:
#
#   - a symbol only one shard uses becomes local; one that several use keeps a private,
#     hidden name in all of them, definitions and references alike; the library's calls reach
#     the library's definition and the host's reach the host's (fixture "fx", shards 0-1);
#   - a weak default and a strong override in other shards: the strong one wins, inside the
#     library, and the host's own definition stays separate (shards 3-5);
#   - an inline function's static counter, instantiated in two shards: one counter inside the
#     library, another in the host, COMDAT signatures private (shards 7-8);
#   - an optional weak hook does not bind to the host's definition, while a weak reference
#     into the C library keeps its name and binds (shard 6);
#   - a constructor, a thread_local, pointers into strings with an addend, and a C++
#     exception unwinding frames, all across shards, then the host's own exception;
#   - every shard's constructor-only registration is linked in by a plain -l through the
#     shard anchors, once, with --gc-sections too, and when the shards are linked directly
#     as Go links .syso files; also with a single shard (fixture "reg").
#
# Every host link is done with GNU ld and with lld, each with and without --gc-sections, and
# never with --allow-multiple-definition. The fixtures check that they really are split as
# intended, and that the checks catch the failures they are for: the unsplit archive does not
# link against the host, a split archive with one reference changed back to its old name is
# rejected by split_static_lib.py --check (although it links, and then calls the host), and
# the unsplit registrations are lost by a plain -l.
set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FIXTURE="${MY_DIR}/split-fixture"
SPLIT="${MY_DIR}/split_static_lib.py"

if [ "$(uname)" != Linux ]; then
    echo "Error: Linux only, as split_static_lib.py"
    exit 1
fi

# The fixtures are compiled as chDB is, with clang; the hosts link with the compiler driver
# under both linkers.
FX_CC=${FX_CC:-clang}
FX_CXX=${FX_CXX:-clang++}
# The newest versioned lld, as split_static_lib.py picks.
newest () { (ls "$@" 2>/dev/null || true) | sort -V | tail -1; }
LLD=${SPLIT_LLD:-$(newest /usr/bin/ld.lld-* /usr/local/bin/ld.lld-*)}
LLD=${LLD:-$(command -v ld.lld || true)}
for tool in "${FX_CC}" "${FX_CXX}" "${LLD}" ld.bfd readelf ar python3; do
    command -v "${tool}" > /dev/null || { echo "Error: ${tool:-ld.lld} not found"; exit 1; }
done

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT
cd "${WORK_DIR}"

failures=0
pass () { echo "PASS ($*)"; }
fail () { echo "FAIL: $*"; failures=$((failures + 1)); }

echo "split_static_lib.py fixture tests"
echo "  compilers: $(${FX_CC} --version | head -1)"
echo "  linkers:   $(ld.bfd --version | head -1); $(${LLD} --version | head -1)"
echo

# --- Building the fixtures ---------------------------------------------------------------

# <name>: the fixture's members, compiled in name order into raw/lib<name>.a.
build_raw () {
    local name=$1 src obj
    mkdir -p "obj_${name}" raw
    for src in "${FIXTURE}/${name}"_*.c "${FIXTURE}/${name}"_*.cpp; do
        [ -f "${src}" ] || continue
        obj="obj_${name}/$(basename "${src%.*}").o"
        case "${src}" in
            *.c) "${FX_CC}" -O2 -fPIC -ffunction-sections -fdata-sections -I"${FIXTURE}" -c "${src}" -o "${obj}" ;;
            *.cpp) "${FX_CXX}" -std=c++17 -O2 -fPIC -ffunction-sections -fdata-sections -I"${FIXTURE}" -c "${src}" -o "${obj}" ;;
        esac
    done
    rm -f "raw/lib${name}.a"
    # shellcheck disable=SC2046
    ar rcs "raw/lib${name}.a" $(ls "obj_${name}"/*.o | sort)
}

# <name> <out dir> <shard MiB>: split raw/lib<name>.a into <out dir>/lib<name>.a, with its
# shards extracted into <out dir>/shards.
split_fixture () {
    local name=$1 out=$2 shard_mb=$3
    mkdir -p "${out}/shards"
    python3 "${SPLIT}" "raw/lib${name}.a" "${out}/lib${name}.a" --contract "${FIXTURE}/${name}.contract" \
        --shard-mb "${shard_mb}" --manifest "${out}/lib${name}.json" --version fixture > "${out}/split.log" 2>&1 \
        || { sed 's/^/    /' "${out}/split.log"; return 1; }
    (cd "${out}/shards" && ar x "../lib${name}.a")
}

shard_count () { python3 -c 'import json, sys; print(len(json.load(open(sys.argv[1]))["parts"]))' "$1"; }
shard () { printf '%s/shards/libchdb_part_%03d.o' "$1" "$2"; }
# <out dir> <name>: the indexes of the shards that define <name>.
defining () {
    local i
    for i in $(seq 0 $(($(shard_count "$1"/*.json) - 1))); do
        if sym "$(shard "$1" "${i}")" "$2" | grep -q DEF; then echo "${i}"; fi
    done
}

# <object> <name>: "<binding> <visibility> <type> <UND|DEF>" of each symbol of that name.
sym () {
    readelf -sW "$1" | awk -v n="$2" '$1 ~ /^[0-9]+:$/ && $NF == n { print $5, $6, $4, ($7 == "UND" ? "UND" : "DEF") }'
}

# <description> <object> <name> <expected sym() line>
expect_sym () {
    local got
    got=$(sym "$2" "$3" | sort -u | tr '\n' ';')
    if [ "${got}" = "$4;" ]; then
        pass "$1"
    else
        fail "$1: $(basename "$2") has ${3} as '${got%;}', expected '$4'"
    fi
}

# <description> <out dir> <name>: no shard has a symbol of that name, defined or not.
expect_absent () {
    local obj found=""
    for obj in "$2"/shards/*.o; do
        [ -n "$(sym "${obj}" "$3")" ] && found="${found} $(basename "${obj}")"
    done
    if [ -z "${found}" ]; then pass "$1"; else fail "$1: ${3} is still in${found}"; fi
}

# <description> <expected exit: 0 | fail> <command...>: link and record.
try_link () {
    local what=$1 expect=$2 rc=0
    shift 2
    "$@" > "${WORK_DIR}/link.log" 2>&1 || rc=$?
    if [ "${expect}" = 0 ] && [ "${rc}" -ne 0 ]; then
        sed 's/^/    /' "${WORK_DIR}/link.log" | tail -20
        fail "${what}: the link failed"
        return 1
    elif [ "${expect}" = fail ] && [ "${rc}" -eq 0 ]; then
        fail "${what}: the link succeeded, but must fail"
        return 1
    fi
    return 0
}

# <description> <expected exit: 0 | fail> <program> [args]: run with a timeout.
run_host () {
    local what=$1 expect=$2 rc=0
    shift 2
    timeout 60 "$@" > "${WORK_DIR}/run.log" 2>&1 || rc=$?
    if [ "${expect}" = 0 ] && [ "${rc}" -eq 0 ]; then
        pass "${what}: $(tail -1 "${WORK_DIR}/run.log")"
    elif [ "${expect}" = fail ] && [ "${rc}" -ne 0 ]; then
        pass "${what}: fails as it must ($(tail -1 "${WORK_DIR}/run.log"))"
    else
        sed 's/^/    /' "${WORK_DIR}/run.log" | tail -20
        fail "${what}: exit ${rc}"
    fi
}

# The four links of a host against a library: GNU ld and lld, each with and without
# --gc-sections. <host object> <out name> <library dir> <library> <extra flags...>
LINKERS=("bfd:-fuse-ld=bfd" "bfd --gc-sections:-fuse-ld=bfd -Wl,--gc-sections"
         "lld:--ld-path=${LLD}" "lld --gc-sections:--ld-path=${LLD} -Wl,--gc-sections")

# --- Fixture "fx": symbols across shards, weak, COMDAT, hooks, TLS, exceptions -----------
echo "== fx: the split =="
build_raw fx
"${FX_CXX}" -std=c++17 -O2 -I"${FIXTURE}" -c "${FIXTURE}/host_fx.cpp" -o host_fx.o
members=$(ar t raw/libfx.a | wc -l)
if split_fixture fx fx 0.0001; then
    shards=$(shard_count fx/libfx.json)
    # One member per shard, or the cross-shard cases below would not be across shards.
    if [ "${shards}" -eq "${members}" ] && [ "${shards}" -ge 3 ]; then
        pass "fx: ${members} members in ${shards} shards"
    else
        fail "fx: ${members} members in ${shards} shards, expected one shard each"
    fi
    if python3 "${SPLIT}" --check fx/libfx.a --contract "${FIXTURE}/fx.contract" > check.log 2>&1; then
        pass "fx: split_static_lib.py --check accepts the split archive"
    else
        sed 's/^/    /' check.log; fail "fx: split_static_lib.py --check rejects the split archive"
    fi
    # On aarch64 every shard's code is renamed, even a shard far below the 64 MB of one run:
    # the consumer would otherwise put the .text.* of all shards into its own .text, past
    # the reach of GNU ld's stubs (chdb-io/chdb-core#343).
    if [[ "$(readelf -h "$(shard fx 0)")" == *AArch64* ]]; then
        code_names () { readelf -SW "$1" | sed -n 's/^ *\[ *[0-9]*\] \([^ ]*\) *PROGBITS .* [A-Z]*AX[A-Z]* .*/\1/p'; }
        bad=0
        for i in $(seq 0 $((shards - 1))); do
            names=$(code_names "$(shard fx "${i}")")
            if [ -z "${names}" ] || grep -qv "^\.chdb\.text\.${i}\.[0-9]*$" <<< "${names}"; then
                echo "    shard ${i}: $(echo ${names})"; bad=$((bad + 1))
            fi
        done
        [ "${bad}" -eq 0 ] && pass "fx, aarch64: every shard's code is in .chdb.text.<shard>.*" \
            || fail "fx, aarch64: ${bad} shards with code outside .chdb.text.<shard>.*"
        OBJCOPY=${SPLIT_OBJCOPY:-$(newest /usr/bin/llvm-objcopy* /usr/local/bin/llvm-objcopy*)}
        mkdir -p samename && cp fx/shards/*.o samename/
        renamed=$(code_names samename/libchdb_part_001.o | awk 'NR == 1')
        "${OBJCOPY}" "--rename-section=${renamed}=.chdb.text.0.0" samename/libchdb_part_001.o
        # shellcheck disable=SC2046
        rm -f samename/libfx.a && ar rcs samename/libfx.a $(ls samename/*.o | sort)
        if python3 "${SPLIT}" --check samename/libfx.a --contract "${FIXTURE}/fx.contract" > check.log 2>&1; then
            fail "fx, aarch64: --check accepts shard 1 code named after shard 0"
        elif grep -q "code sections outside .chdb.text.1" check.log; then
            pass "fx, aarch64: --check rejects shard 1 code named after shard 0"
        else
            sed 's/^/    /' check.log; fail "fx, aarch64: --check rejects shard 1 code, but not for its name"
        fi
    fi

    echo "== fx: symbols =="
    while read -r name; do
        where=$(defining fx "${name}")
        if [ "$(echo "${where}" | wc -w)" -ne 1 ]; then
            fail "C API ${name}: defined in shards '${where}', expected exactly one"
        else
            expect_sym "C API ${name}: global, default visibility" "$(shard fx "${where}")" "${name}" "GLOBAL DEFAULT FUNC DEF"
        fi
    done < "${FIXTURE}/fx.contract"
    expect_sym "a_only (one shard): local" "$(shard fx 0)" a_only "LOCAL DEFAULT FUNC DEF"
    expect_absent "a_only: no global or reference anywhere else" fx a_only.chdb
    expect_absent "helper: the old name is gone from every shard" fx helper
    expect_sym "helper (two shards): hidden private definition" "$(shard fx 0)" helper.chdb "GLOBAL HIDDEN FUNC DEF"
    expect_sym "helper: the other shard references the private name" "$(shard fx 1)" helper.chdb "GLOBAL DEFAULT NOTYPE UND"
    expect_sym "thread_local: hidden private TLS definition" "$(shard fx 0)" fx_tls_counter.chdb "GLOBAL HIDDEN TLS DEF"
    expect_sym "thread_local: referenced from another shard" "$(shard fx 1)" fx_tls_counter.chdb "GLOBAL DEFAULT TLS UND"
    expect_sym "string pointer: hidden private definition" "$(shard fx 0)" fx_beta_pointer.chdb "GLOBAL HIDDEN OBJECT DEF"
    expect_sym "string pointer: referenced from another shard" "$(shard fx 1)" fx_beta_pointer.chdb "GLOBAL DEFAULT NOTYPE UND"
    expect_sym "constructor's state: referenced from the constructor's shard" "$(shard fx 2)" fx_state_value.chdb "GLOBAL DEFAULT NOTYPE UND"
    expect_sym "weak default: stays weak, hidden" "$(shard fx 3)" fx_policy.chdb "WEAK HIDDEN FUNC DEF"
    expect_sym "strong override: stays strong, hidden" "$(shard fx 4)" fx_policy.chdb "GLOBAL HIDDEN FUNC DEF"
    expect_sym "fx_policy: referenced from a third shard" "$(shard fx 5)" fx_policy.chdb "GLOBAL DEFAULT NOTYPE UND"
    expect_absent "fx_policy: the old name is gone from every shard" fx fx_policy
    expect_sym "optional hook: weak reference under the private name" "$(shard fx 6)" fx_hook.chdb "WEAK DEFAULT NOTYPE UND"
    expect_absent "optional hook: the old name is gone from every shard" fx fx_hook
    expect_sym "C library weak reference: keeps its name" "$(shard fx 6)" __cxa_thread_atexit_impl "WEAK DEFAULT NOTYPE UND"
    expect_sym "exception thrower: hidden private definition" "$(shard fx 9)" _Z13fx_throw_deepi.chdb "GLOBAL HIDDEN FUNC DEF"
    expect_sym "exception thrower: referenced from the catching shard" "$(shard fx 10)" _Z13fx_throw_deepi.chdb "GLOBAL DEFAULT NOTYPE UND"
    # The inline counter's static variable: a COMDAT group in each of the two shards, under
    # the private name, so the consumer's linker keeps one - and not the host's.
    for i in 7 8; do
        if readelf -gW "$(shard fx "${i}")" | grep -q "\[_ZZ10fx_countervE7counter\.chdb\]"; then
            pass "inline counter: shard ${i} keeps the counter's COMDAT group under the private signature"
        else
            fail "inline counter: shard ${i} has no COMDAT group _ZZ10fx_countervE7counter.chdb"
        fi
    done
    public_groups=$(for obj in fx/shards/*.o; do readelf -gW "${obj}"; done \
        | sed -n "s/^COMDAT group section \[ *[0-9]*\] \`[^']*' \[\([^]]*\)\].*/\1/p" | grep -vc '\.chdb$' || true)
    if [ "${public_groups}" -eq 0 ]; then
        pass "no COMDAT group under a public signature"
    else
        fail "${public_groups} COMDAT groups under a public signature"
    fi

    echo "== fx: the host, both directions =="
    for linker in "${LINKERS[@]}"; do
        label=${linker%%:*}
        # shellcheck disable=SC2086
        try_link "fx, ${label}" 0 "${FX_CXX}" host_fx.o -o host_fx -pthread ${linker#*:} -Lfx -lfx \
            && run_host "fx, ${label}" 0 ./host_fx
    done
    # The shards linked directly, as Go links .syso files.
    # shellcheck disable=SC2046
    try_link "fx, shards as objects (bfd)" 0 "${FX_CXX}" host_fx.o $(ls fx/shards/*.o | sort) -o host_fx -pthread -fuse-ld=bfd \
        && run_host "fx, shards as objects (bfd)" 0 ./host_fx

    echo "== fx: the checks catch what they are for =="
    # Unsplit, the library's helper(), fx_policy() and fx_hook() are the host's names.
    try_link "fx, unsplit archive against the host" fail "${FX_CXX}" host_fx.o -o host_fx -pthread -fuse-ld=bfd -Lraw -lfx \
        && pass "fx: the unsplit archive does not link against the host ($(grep -m1 -o 'multiple definition of [^;]*' link.log || echo 'link failed'))"
    if python3 "${SPLIT}" --check raw/libfx.a --contract "${FIXTURE}/fx.contract" > check.log 2>&1; then
        fail "fx: split_static_lib.py --check accepts the unsplit archive"
    else
        pass "fx: split_static_lib.py --check rejects the unsplit archive"
    fi
    # One reference changed back to the old name. The host happens to define helper(), so
    # this links - and the library then calls the host. Only the structure check can tell.
    mkdir -p bad/shards
    cp fx/shards/*.o bad/shards/
    llvm_objcopy=$(newest /usr/bin/llvm-objcopy*)
    "${llvm_objcopy:-objcopy}" --redefine-sym helper.chdb=helper "$(shard bad 1)"
    # shellcheck disable=SC2046
    (cd bad/shards && ar rcs ../libfx.a $(ls *.o | sort))
    if python3 "${SPLIT}" --check bad/libfx.a --contract "${FIXTURE}/fx.contract" > check.log 2>&1; then
        fail "fx: split_static_lib.py --check accepts a shard that references helper by its old name"
    elif grep -q "old name" check.log; then
        pass "fx: split_static_lib.py --check rejects a stale reference ($(grep -o 'e.g. .*' check.log | head -1))"
    else
        sed 's/^/    /' check.log; fail "fx: --check rejected the stale reference for another reason"
    fi
    try_link "fx, stale reference" 0 "${FX_CXX}" host_fx.o -o host_fx -pthread -fuse-ld=bfd -Lbad -lfx \
        && run_host "fx, stale reference: links, then reaches the host's helper()" fail ./host_fx
    # A shard that no longer references the next shard's anchor breaks the ring: a plain -l
    # could then leave out a shard that only constructors live in.
    mkdir -p ring/shards
    cp fx/shards/*.o ring/shards/
    "${llvm_objcopy:-objcopy}" --redefine-sym chdb_split_shard_2.chdb=fx_unrelated_reference "$(shard ring 1)"
    # shellcheck disable=SC2046
    (cd ring/shards && ar rcs ../libfx.a $(ls *.o | sort))
    if python3 "${SPLIT}" --check ring/libfx.a --contract "${FIXTURE}/fx.contract" > check.log 2>&1; then
        fail "fx: split_static_lib.py --check accepts a shard that does not reference the next anchor"
    elif grep -q "shard 1 .*does not reference the next shard's anchor chdb_split_shard_2.chdb" check.log; then
        pass "fx: split_static_lib.py --check rejects a broken anchor ring"
    else
        sed 's/^/    /' check.log; fail "fx: --check rejected the broken anchor ring for another reason"
    fi
else
    fail "fx: split_static_lib.py failed"
fi
echo

# --- Fixture "reg": registrations pulled in by the shard anchors ---------------------------
echo "== reg: the split =="
build_raw reg
"${FX_CC}" -O2 -I"${FIXTURE}" -c "${FIXTURE}/host_reg.c" -o host_reg.o
EXPECTED="reg0=1,reg1=1,reg2=1,reg3=1,reg4=1"
if split_fixture reg reg 0.0001; then
    shards=$(shard_count reg/libreg.json)
    entry=$(defining reg reg_report)
    if [ "${shards}" -ge 3 ] && [ "${entry}" = 2 ]; then
        pass "reg: ${shards} shards, the public entry in shard ${entry} only"
    else
        fail "reg: ${shards} shards with the public entry in '${entry}', expected at least 3 and shard 2"
    fi
    # Nothing but the registration: no symbol another shard references.
    for i in 0 1 3 4; do
        defs=$(readelf -sW "$(shard reg "${i}")" | awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && $5 != "LOCAL" { print $8 }' \
            | grep -v '^chdb_split_shard_' || true)
        [ -z "${defs}" ] || fail "reg: shard ${i} defines more than its anchor: ${defs}"
    done

    echo "== reg: links =="
    # The unsplit archive: a plain -l takes the registry's member only.
    try_link "reg, unsplit archive" 0 "${FX_CC}" host_reg.o -o host_reg -fuse-ld=bfd -Lraw -lreg \
        && run_host "reg, unsplit archive, plain -l: registrations are lost" fail ./host_reg "${EXPECTED}"
    trace=()
    for i in $(seq 0 $((shards - 1))); do trace+=("-Wl,-y,chdb_split_shard_${i}.chdb"); done
    for linker in "${LINKERS[@]}"; do
        label=${linker%%:*}
        # shellcheck disable=SC2086
        if try_link "reg, ${label}" 0 "${FX_CC}" host_reg.o -o host_reg ${linker#*:} "${trace[@]}" -Lreg -lreg; then
            pulled=0
            for i in $(seq 0 $((shards - 1))); do
                grep -q "libreg\.a(libchdb_part_00${i}\.o): definition of chdb_split_shard_${i}\.chdb" link.log && pulled=$((pulled + 1))
            done
            if [ "${pulled}" -eq "${shards}" ]; then
                pass "reg, ${label}: the link pulled in all ${shards} shards"
            else
                sed 's/^/    /' link.log | head -20
                fail "reg, ${label}: the link pulled in ${pulled} of ${shards} shards"
            fi
            run_host "reg, ${label}" 0 ./host_reg "${EXPECTED}"
        fi
    done
    # shellcheck disable=SC2046
    try_link "reg, shards as objects (bfd --gc-sections)" 0 "${FX_CC}" host_reg.o $(ls reg/shards/*.o | sort) -o host_reg \
            -fuse-ld=bfd -Wl,--gc-sections \
        && run_host "reg, shards as objects (bfd --gc-sections)" 0 ./host_reg "${EXPECTED}"

    echo "== reg and fx: a single shard, anchored to itself =="
    if split_fixture reg reg1 1000 && split_fixture fx fx1 1000; then
        if [ "$(shard_count reg1/libreg.json)" -eq 1 ] && [ "$(shard_count fx1/libfx.json)" -eq 1 ]; then
            pass "single shard: one member each"
        else
            fail "single shard: expected one member each"
        fi
        # Found by check_static_lib_hermetic.sh in the archive and in the link trace.
        expect_sym "single shard: its anchor stays a hidden global" "$(shard reg1 0)" chdb_split_shard_0.chdb "GLOBAL HIDDEN NOTYPE DEF"
        python3 "${SPLIT}" --check reg1/libreg.a --contract "${FIXTURE}/reg.contract" > check.log 2>&1 \
            && pass "single shard: split_static_lib.py --check accepts it" \
            || { sed 's/^/    /' check.log; fail "single shard: split_static_lib.py --check rejects it"; }
        for linker in "${LINKERS[0]}" "${LINKERS[3]}"; do
            label=${linker%%:*}
            # shellcheck disable=SC2086
            try_link "reg, single shard, ${label}" 0 "${FX_CC}" host_reg.o -o host_reg ${linker#*:} -Lreg1 -lreg \
                && run_host "reg, single shard, ${label}" 0 ./host_reg "${EXPECTED}"
            # shellcheck disable=SC2086
            try_link "fx, single shard, ${label}" 0 "${FX_CXX}" host_fx.o -o host_fx -pthread ${linker#*:} -Lfx1 -lfx \
                && run_host "fx, single shard, ${label}" 0 ./host_fx
        done
    else
        fail "single shard: split_static_lib.py failed"
    fi
else
    fail "reg: split_static_lib.py failed"
fi
echo

if [ "${failures}" -ne 0 ]; then
    echo "${failures} fixture check(s) failed"
    exit 1
fi
echo "All split_static_lib.py fixture checks passed"
