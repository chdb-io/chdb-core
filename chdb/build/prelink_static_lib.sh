#!/bin/bash
#
# Give libchdb.a the link step it otherwise lacks, so that it exports the C API and nothing
# else - the same surface libchdb.so exports (chdb-io/chdb-rust#53).
#
# libchdb.so applies chdb/libchdb_export.map at link time: everything outside the C API,
# including the bundled LLVM libc++, libc++abi and libunwind, becomes local. An archive has
# no link step of its own, so until now every member reached the consumer's linker with all
# of its global symbols intact. cmake/bundled_runtime_visibility.cmake marks the bundled
# runtime hidden, but hidden only keeps a symbol out of .dynsym: a hidden definition in an
# archive member still satisfies an undefined reference from any other object in the same
# static link. So the consumer's own references bound to chDB's copies:
#
#   Rust    std bound _Unwind_GetIP and _Unwind_RaiseException to the bundled libunwind but
#           took _Unwind_Backtrace and _Unwind_GetIPInfo from libgcc_s, which the minimised
#           archive does not define. libgcc handed its own _Unwind_Context to LLVM's
#           accessors: every backtrace crashed and every panic aborted ("failed to initiate
#           panic, error 3").
#   C++     __cxa_throw, __gxx_personality_v0 and the std::exception typeinfo of a
#           libstdc++ host bound to the bundled libc++abi, and the host's own throw/catch
#           corrupted its heap.
#
# Two steps fix that:
#
#   1. `ld -r` merges every member into one relocatable object. All of chDB's references,
#      including those into the bundled runtime, are resolved inside it.
#   2. `llvm-objcopy --keep-global-symbols=<C API contract>` turns every other definition
#      into a local symbol, which a consumer's linker can no longer bind anything to.
#
# The result is shipped as a one-member archive, so consumers keep linking with -lchdb.
#
# Tool choices, each forced by something in the archive:
#
#   the linker        GNU ld when the archive has COMMON symbols, lld otherwise. x86_64 has
#                     one, OPENSSL_ia32cap_P (a hidden .comm in OpenSSL's perlasm): `ld.lld -r`
#                     leaves commons unallocated and ignores -d, and an unallocated common that
#                     is then made local breaks the consumer's link, while `ld.bfd -r -d`
#                     allocates it. aarch64 has none, and there GNU ld 2.42 (Ubuntu 24.04)
#                     cannot be used: its aarch64 backend writes the output in under a minute
#                     and then spends over an hour freeing per-section data (quadratic in the
#                     1.5 million sections; gone in binutils 2.44). `ld.lld -r` takes seconds.
#   --force-group-allocation
#                     Dissolves the COMDAT groups. Their signatures are matched by name in the
#                     consumer's link, so a group that survived with a now-local signature
#                     could still be discarded in favour of a same-named group of the host
#                     (an inline function of the host's libc++, DW.ref.__gxx_personality_v0),
#                     leaving chDB's code pointing at the host's copy. Needs binutils 2.29+.
#   -z noexecstack    Dozens of assembly members (OpenSSL perlasm, glibc-compatibility's
#                     syscall.s, ...) carry no .note.GNU-stack, which makes GNU ld mark the
#                     result - and, until now, every consumer linked with GNU ld - as needing an
#                     executable stack. None of that code does.
#   llvm-objcopy      GNU objcopy gets the same result but takes about twenty times as long
#                     on an object this size (minutes rather than seconds). Picked like STRIP
#                     in vars.sh: the newest versioned one, /usr/bin first. It also drops
#                     .llvm_addrsig: ld -r concatenates those tables without remapping their
#                     symbol indices.
#
# Linux only. macOS is not prelinked: ld64 -r drops MH_SUBSECTIONS_VIA_SYMBOLS when any
# input lacks it (arm64 then fails to place branch islands in the merged __text), and ld64
# looks personalities up by name, so localizing __gxx_personality_v0 does not work there.
# Both sides of the macOS split are LLVM libunwind, so the split binding is not known to
# crash there - see the PR that introduced this script.
#
# Usage: prelink_static_lib.sh <path/to/libchdb.a>    (rewritten in place)

set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ARCHIVE=${1:?usage: prelink_static_lib.sh <path/to/libchdb.a>}
[ -f "${ARCHIVE}" ] || { echo "Error: not found: ${ARCHIVE}"; exit 1; }
[ "$(uname)" = Linux ] || { echo "Error: prelinking is only implemented for Linux"; exit 1; }

# Next to the archive rather than in /tmp: the intermediate objects are over a gigabyte each.
WORK_DIR=$(mktemp -d "$(dirname "${ARCHIVE}")/prelink.XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

# COMMON symbols decide the linker, see above.
nm "${ARCHIVE}" > "${WORK_DIR}/nm.txt" 2> /dev/null || true
if [ ! -s "${WORK_DIR}/nm.txt" ]; then
    echo "Error: nm read no symbols out of ${ARCHIVE}"
    exit 1
fi
commons=$(awk '$2 == "C" { print $3 }' "${WORK_DIR}/nm.txt" | sort -u | tr '\n' ' ')
rm -f "${WORK_DIR}/nm.txt"

if [ -z "${PRELINK_LD:-}" ]; then
    if [ -z "${commons}" ]; then
        PRELINK_LD=$(ls -1 /usr/bin/ld.lld-* 2>/dev/null | sort -V | tail -n 1 || true)
        [ -n "${PRELINK_LD}" ] || PRELINK_LD=$(command -v ld.lld 2>/dev/null || true)
    fi
    if [ -z "${PRELINK_LD:-}" ]; then
        if command -v ld.bfd > /dev/null 2>&1; then
            PRELINK_LD=ld.bfd
        else
            PRELINK_LD=ld
        fi
    fi
fi
# Captured first: `grep -q` stops reading at the first match, and under pipefail the SIGPIPE
# that the still-writing tool then gets would read as "not supported".
ld_version=$("${PRELINK_LD}" --version 2>/dev/null || true)
ld_help=$("${PRELINK_LD}" --help 2>/dev/null || true)
if grep -q '^GNU ld' <<< "${ld_version}"; then
    :
elif grep -q 'LLD' <<< "${ld_version}"; then
    if [ -n "${commons}" ]; then
        echo "Error: the archive has COMMON symbols (${commons}), which ld.lld -r cannot allocate; use GNU ld"
        exit 1
    fi
else
    echo "Error: ${PRELINK_LD} is neither GNU ld nor LLD"
    exit 1
fi
if ! grep -q -- '--force-group-allocation' <<< "${ld_help}"; then
    echo "Error: ${PRELINK_LD} does not support --force-group-allocation (needs binutils 2.29+ or LLD 19+)"
    exit 1
fi

for dir in /usr/bin /usr/local/bin; do
    if [ -z "${PRELINK_OBJCOPY:-}" ]; then
        PRELINK_OBJCOPY=$(ls -1 "${dir}"/llvm-objcopy* 2>/dev/null | sort -V | tail -n 1 || true)
    fi
done
if [ -z "${PRELINK_OBJCOPY}" ]; then
    PRELINK_OBJCOPY=$(command -v llvm-objcopy 2>/dev/null || true)
fi
objcopy_version=$("${PRELINK_OBJCOPY:-false}" --version 2>/dev/null || true)
if ! grep -q 'LLVM' <<< "${objcopy_version}"; then
    echo "Error: llvm-objcopy not found"
    exit 1
fi

echo "Prelinking ${ARCHIVE}"
echo "  COMMON symbols: ${commons:-none}"
echo "  ld:      $(command -v "${PRELINK_LD}") ($(head -n 1 <<< "${ld_version}"))"
echo "  objcopy: ${PRELINK_OBJCOPY} ($(grep -m 1 -i 'version' <<< "${objcopy_version}"))"

python3 "${MY_DIR}/check_export_contract.py" > "${WORK_DIR}/keep.txt"
[ -s "${WORK_DIR}/keep.txt" ] || { echo "Error: the C API contract is empty"; exit 1; }

"${PRELINK_LD}" -r -d -z noexecstack --force-group-allocation \
    --whole-archive "${ARCHIVE}" --no-whole-archive \
    -o "${WORK_DIR}/chdb_prelinked.o"

"${PRELINK_OBJCOPY}" --keep-global-symbols="${WORK_DIR}/keep.txt" --remove-section=.llvm_addrsig \
    "${WORK_DIR}/chdb_prelinked.o" "${WORK_DIR}/object.o"
rm -f "${WORK_DIR}/chdb_prelinked.o"

# Where the object's data starts inside the archive. gold - only gold - slows down
# quadratically on an object this size when that offset is not 8-byte aligned: about 10 s
# aligned against more than an hour misaligned. The offset follows the size of the archive
# symbol table, which changes with the contract, so the member name is chosen to fix it: a
# name longer than 15 characters goes into the archive's name table, and every two more
# characters move the object by two bytes. A tool that repacks the object loses this - rustc
# does when it bundles the archive into an rlib.
object_offset () {
    python3 - "$1" <<'EOF'
import sys
with open(sys.argv[1], "rb") as f:
    if f.read(8) != b"!<arch>\n":
        sys.exit("not an ar archive")
    offset = 8
    while True:
        header = f.read(60)
        if len(header) < 60:
            sys.exit("no object member found")
        name, size = header[:16].decode().strip(), int(header[48:58].decode())
        if name not in ("/", "//"):
            print(offset + 60)
            break
        offset += 60 + size + (size & 1)
        f.seek(offset)
EOF
}

offset=
for member in libchdb.o libchdb_prelinked.o libchdb_prelinked__.o libchdb_prelinked____.o libchdb_prelinked______.o; do
    mv "${WORK_DIR}"/*.o "${WORK_DIR}/${member}"
    rm -f "${WORK_DIR}/libchdb.a"
    ar rcs "${WORK_DIR}/libchdb.a" "${WORK_DIR}/${member}"
    offset=$(object_offset "${WORK_DIR}/libchdb.a")
    [ $((offset % 8)) -eq 0 ] && break
done
if [ $((offset % 8)) -ne 0 ]; then
    echo "Error: could not place the object at an 8-byte aligned offset in the archive (last: ${offset})"
    exit 1
fi
mv -f "${WORK_DIR}/libchdb.a" "${ARCHIVE}"

echo "Prelinked: member ${member} at offset ${offset}, $(du -h "${ARCHIVE}" | cut -f1), keep list of $(wc -l < "${WORK_DIR}/keep.txt" | tr -d ' ') symbols"
