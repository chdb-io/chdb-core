#!/bin/bash
#
# Prints the symbols glibc's shared objects export on this machine, one per line: the C library
# every Linux consumer of libchdb.a links. The archive's weak references into it keep their
# names (prelink_static_lib.sh), and check_static_lib_hermetic.sh checks that no other weak
# reference did.
#
# The dynamic loader exports part of that ABI itself (__rseq_offset, _dl_find_object), and
# libc.so.6 names it as its one dependency. libpthread, libdl and librt are objects of their
# own before glibc 2.34.

set -euo pipefail

so_path () { ldconfig -p 2>/dev/null | awk -v so="$1" '$1 == so { print $NF; exit }'; }

libc=$(so_path libc.so.6)
[ -n "${libc}" ] || { echo "Error: ldconfig does not know libc.so.6" >&2; exit 1; }
loader=$(readelf -dW "${libc}" | sed -n 's/.*(NEEDED).*\[\(.*\)\]$/\1/p')
[ -n "${loader}" ] || { echo "Error: ${libc} names no dynamic loader" >&2; exit 1; }

for so in libc.so.6 "${loader}" libm.so.6 libpthread.so.0 libdl.so.2 librt.so.1; do
    path=$(so_path "${so}")
    if [ -z "${path}" ]; then
        case "${so}" in
            libc.so.6|"${loader}") echo "Error: ldconfig does not know ${so}" >&2; exit 1 ;;
            *) continue ;;
        esac
    fi
    readelf --dyn-syms -W "${path}" \
        | awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && ($5 == "GLOBAL" || $5 == "WEAK") {
                   n = $8; sub(/@.*/, "", n); if (n != "") print n
               }'
done | sort -u
