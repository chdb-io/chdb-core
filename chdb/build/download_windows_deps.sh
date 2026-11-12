#!/bin/bash
# Fetch what the Windows cross-build needs besides the compiler, into CHDB_WINDOWS_DEPS_DIR:
#
#   mingw/usr/x86_64-w64-mingw32ucrt/  mingw-w64 13 headers, CRT and winpthreads for the UCRT
#                                      (python.org CPython is UCRT-based, so the module must be too)
#   python/X.Y/                        Windows Python.h & co. plus python3.lib and python3XY.lib,
#                                      from the python.org NuGet package of each version
#
# Every download is pinned by SHA-256. The script is idempotent: what is already unpacked is kept.
# Usage: download_windows_deps.sh [DEST]   (default: $CHDB_WINDOWS_DEPS_DIR or ~/.cache/chdb-windows-deps)

set -euo pipefail

DEST=${1:-${CHDB_WINDOWS_DEPS_DIR:-${HOME}/.cache/chdb-windows-deps}}
mkdir -p "${DEST}/downloads"

# The Ubuntu 26.04 packages of mingw-w64 13, the same version ClickHouse's own Windows build uses.
# Ubuntu 24.04 ships 11, whose headers hide _iswctype_l, which libc++ needs.
MINGW_DEBS=(
    "mingw-w64-common_13.0.0-2ubuntu1_all.deb da9b2cb14134ca0ef3a31302df50295a2dd6fee7aa4fd888b11c6f72f8ab939e"
    "mingw-w64-ucrt64-dev_13.0.0-2ubuntu1_all.deb d680c0135268b147f4b870a4a69b40cc7450a2c1a7244ebaacd9e7f97bf33986"
)
MINGW_MIRRORS=(
    "http://archive.ubuntu.com/ubuntu/pool/universe/m/mingw-w64"
    "http://ports.ubuntu.com/ubuntu-ports/pool/universe/m/mingw-w64"
)

# The newest Windows build of each Python the wheel supports (3.9.13 is the last 3.9 with one).
PYTHON_NUGETS=(
    "3.9.13 df29f99c9cf508eda80e878e2795c50ec94431db8d5396a5e455381af593daf4"
    "3.10.11 7c6f99b160a36a7e09492dfcff2b0a3a60bb5229ca44cdcc3ecb32871a6144d0"
    "3.11.9 9283876d58c017e0e846f95b490da3bca0fc0a6ee1134b2870677cfb7eec3c67"
    "3.12.10 0eb85c2dfccccf1b17352de4c397f69194035b7d37149eacc16f1147d93de3b8"
    "3.13.17 dce409744b088f94c6e37ba41f8100c384c27e029e843237364c602d30d475e5"
    "3.14.10 f54ba67c9951c3d773ea47d2bb095150c037c399f0423a968bcda22809241784"
)

fetch() {
    local out=$1 sha=$2
    shift 2
    if [ -f "${out}" ] && echo "${sha}  ${out}" | sha256sum -c --quiet - 2>/dev/null; then
        return 0
    fi
    local url
    for url in "$@"; do
        rm -f "${out}.part"
        if curl -fsSL --retry 5 --retry-all-errors -o "${out}.part" "${url}" \
            && echo "${sha}  ${out}.part" | sha256sum -c --quiet -; then
            mv "${out}.part" "${out}"
            return 0
        fi
        echo "download or checksum failed: ${url}" >&2
    done
    rm -f "${out}.part"
    return 1
}

MINGW_ROOT="${DEST}/mingw"
if [ ! -f "${MINGW_ROOT}/usr/x86_64-w64-mingw32ucrt/include/_mingw.h" ]; then
    rm -rf "${MINGW_ROOT}"
    mkdir -p "${MINGW_ROOT}"
    for entry in "${MINGW_DEBS[@]}"; do
        read -r deb sha <<< "${entry}"
        urls=()
        for mirror in "${MINGW_MIRRORS[@]}"; do urls+=("${mirror}/${deb}"); done
        fetch "${DEST}/downloads/${deb}" "${sha}" "${urls[@]}"
        dpkg-deb -x "${DEST}/downloads/${deb}" "${MINGW_ROOT}"
    done
fi
grep -q 'define __MSVCRT_VERSION__ 0xE00' "${MINGW_ROOT}/usr/x86_64-w64-mingw32ucrt/include/_mingw.h"

for entry in "${PYTHON_NUGETS[@]}"; do
    read -r version sha <<< "${entry}"
    xy=${version%.*}
    nodot=${xy//./}
    dir="${DEST}/python/${xy}"
    if [ -f "${dir}/Python.h" ] && [ -f "${dir}/python3.lib" ] && [ -f "${dir}/python${nodot}.lib" ]; then
        continue
    fi
    pkg="${DEST}/downloads/python.${version}.nupkg"
    fetch "${pkg}" "${sha}" \
        "https://api.nuget.org/v3-flatcontainer/python/${version}/python.${version}.nupkg" \
        "https://www.nuget.org/api/v2/package/python/${version}"
    tmp=$(mktemp -d)
    python3 - "${pkg}" "${tmp}" <<'EOF'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    z.extractall(sys.argv[2], [n for n in z.namelist() if n.startswith(('tools/include/', 'tools/libs/'))])
EOF
    rm -rf "${dir}"
    mkdir -p "${dir}"
    cp -a "${tmp}/tools/include/." "${dir}/"
    cp -a "${tmp}/tools/libs/python3.lib" "${tmp}/tools/libs/python${nodot}.lib" "${dir}/"
    rm -rf "${tmp}"
done

echo "Windows build dependencies are in ${DEST}"
