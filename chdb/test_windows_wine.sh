#!/bin/bash
# Smoke-test the Windows wheel on Linux under Wine: install it into python.org's Windows Python
# (the NuGet package chdb/build/download_windows_deps.sh already fetched) and run
# chdb/build/windows_smoke.py. Wine is a quick first check, not Windows: the authoritative run is
# the windows-2025 job of .github/workflows/build_windows_x86_wheels.yml.
#
# Usage: chdb/test_windows_wine.sh <chdb_core-...-win_amd64.whl>
#
# Wine comes from a pinned portable build: Ubuntu 24.04's Wine 9.0 lacks ucrtbase's crealf, which
# NumPy needs, so it cannot even import pandas.

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
WHEEL=$(realpath "$1")
DEPS_DIR=${CHDB_WINDOWS_DEPS_DIR:-${HOME}/.cache/chdb-windows-deps}
PYTHON_VERSION=3.12.10
WINE_VERSION=11.21
WINE_SHA256=33f3c5b56b1deb8f6725539ea006f27b5d781b3b4ae6a8a1f492d8af4fb9dc83

bash "${DIR}/build/download_windows_deps.sh" "${DEPS_DIR}"

WINE_DIR="${DEPS_DIR}/wine-${WINE_VERSION}"
if [ ! -x "${WINE_DIR}/bin/wine" ]; then
    tarball="${DEPS_DIR}/downloads/wine-${WINE_VERSION}-amd64-wow64.tar.xz"
    if ! echo "${WINE_SHA256}  ${tarball}" | sha256sum -c --quiet - 2>/dev/null; then
        curl -fsSL --retry 5 --retry-all-errors -o "${tarball}" \
            "https://github.com/Kron4ek/Wine-Builds/releases/download/${WINE_VERSION}/wine-${WINE_VERSION}-amd64-wow64.tar.xz"
        echo "${WINE_SHA256}  ${tarball}" | sha256sum -c --quiet -
    fi
    rm -rf "${WINE_DIR}" "${DEPS_DIR}/wine-${WINE_VERSION}-amd64-wow64"
    tar -xf "${tarball}" -C "${DEPS_DIR}"
    mv "${DEPS_DIR}/wine-${WINE_VERSION}-amd64-wow64" "${WINE_DIR}"
fi

PYTHON_DIR="${DEPS_DIR}/winpython-${PYTHON_VERSION}"
if [ ! -f "${PYTHON_DIR}/python.exe" ]; then
    rm -rf "${PYTHON_DIR}"
    python3 - "${DEPS_DIR}/downloads/python.${PYTHON_VERSION}.nupkg" "${PYTHON_DIR}" <<'EOF'
import os, shutil, sys, zipfile
dest = sys.argv[2]
with zipfile.ZipFile(sys.argv[1]) as z:
    for name in z.namelist():
        if name.startswith("tools/") and not name.endswith("/"):
            target = os.path.join(dest, name[len("tools/"):])
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with z.open(name) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
EOF
fi

export WINEPREFIX="${CHDB_WINEPREFIX:-${DEPS_DIR}/wineprefix}"
export WINEDEBUG=-all
export PATH="${WINE_DIR}/bin:${PATH}"
if [ ! -d "${WINEPREFIX}" ]; then
    wineboot --init > /dev/null 2>&1 || true
fi

PY="${PYTHON_DIR}/python.exe"
wine "${PY}" -m pip install --quiet --disable-pip-version-check --no-warn-script-location \
    --force-reinstall "$(winepath -w "${WHEEL}")"

# Outside the source tree, so that the installed package is the one imported.
work=$(mktemp -d)
cp "${DIR}/build/windows_smoke.py" "${work}/"
cd "${work}"
timeout 600 wine "${PY}" windows_smoke.py
rm -rf "${work}"
