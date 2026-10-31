#!/bin/bash

set -e

TARGET_DIR="${HOME}/python_include"
TEMP_DIR="${TARGET_DIR}/tmp"

# Format: "full_version:subdir:minor_version"
# For free-threading (e.g. "3.13t"), headers are extracted from PythonT_Framework.pkg
declare -A VERSION_MAP=(
    ["3.9"]="3.9.13:3.9:3.9"
    ["3.10"]="3.10.11:3.10:3.10"
    ["3.11"]="3.11.9:3.11:3.11"
    ["3.12"]="3.12.10:3.12:3.12"
    ["3.13"]="3.13.9:3.13:3.13"
    ["3.14"]="3.14.0:3.14:3.14"
    # 3.15.0 final is due 2026-10-01; bump to it once python.org ships the installer.
    ["3.15"]="3.15.0rc2:3.15:3.15"
    ["3.13t"]="3.13.9:3.13t:3.13"
    ["3.14t"]="3.14.0:3.14t:3.14"
)

if [ "${CHDB_FREE_THREADING}" == "1" ]; then
    if [ -z "${CHDB_FREE_THREADING_PYTHON_VERSION}" ]; then
        echo "ERROR: CHDB_FREE_THREADING=1 requires CHDB_FREE_THREADING_PYTHON_VERSION (e.g. 3.13t)"
        exit 1
    fi
    FT_VER="${CHDB_FREE_THREADING_PYTHON_VERSION}"
    if [ -z "${VERSION_MAP[$FT_VER]}" ]; then
        echo "ERROR: Unknown free-threading version: ${FT_VER}"
        exit 1
    fi
    VERSIONS=("${VERSION_MAP[$FT_VER]}")
else
    VERSIONS=(
        "${VERSION_MAP[3.9]}"
        "${VERSION_MAP[3.10]}"
        "${VERSION_MAP[3.11]}"
        "${VERSION_MAP[3.12]}"
        "${VERSION_MAP[3.13]}"
        "${VERSION_MAP[3.14]}"
        "${VERSION_MAP[3.15]}"
    )
fi

cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$TARGET_DIR"
mkdir -p "$TEMP_DIR"

for entry in "${VERSIONS[@]}"; do
    IFS=':' read -r FULL_VER SUBDIR MINOR_VER <<< "$entry"

    echo "=========================================="
    echo "Processing Python ${FULL_VER}..."
    echo "=========================================="

    DEST_DIR="${TARGET_DIR}/${SUBDIR}"
    if [ -d "$DEST_DIR" ] && [ -f "${DEST_DIR}/Python.h" ]; then
        echo "✓ Python ${FULL_VER} headers already installed at ${DEST_DIR}"
        echo "  Skipping..."
        continue
    fi

    WORK_DIR="${TEMP_DIR}/${SUBDIR}"
    mkdir -p "$WORK_DIR"
    cd "$WORK_DIR"

    # python.org keeps pre-releases under the final version's directory
    # (e.g. .../3.15.0/python-3.15.0rc2-macos11.pkg).
    DIR_VER="$(sed -E 's/(a|b|rc)[0-9]+$//' <<< "$FULL_VER")"
    PKG_URL="https://www.python.org/ftp/python/${DIR_VER}/python-${FULL_VER}-macos11.pkg"

    echo "Downloading: $PKG_URL"
    if wget -q --spider "$PKG_URL" 2>/dev/null; then
        wget -q --show-progress -O python.pkg "$PKG_URL"
    else
        echo "ERROR: Failed to download Python ${FULL_VER}"
        exit 1
    fi

    echo "Extracting pkg with 7z..."
    7z x -y python.pkg > /dev/null

    # Select the correct framework sub-package
    if [[ "$SUBDIR" == *t ]]; then
        # Free-threading: use PythonT_Framework.pkg
        PAYLOAD_DIR="PythonT_Framework.pkg"
    else
        PAYLOAD_DIR="Python_Framework.pkg"
    fi

    if [ ! -f "${PAYLOAD_DIR}/Payload" ]; then
        echo "ERROR: Cannot find ${PAYLOAD_DIR}/Payload for Python ${FULL_VER}"
        exit 1
    fi

    echo "Extracting Payload from ${PAYLOAD_DIR}..."
    cd "$PAYLOAD_DIR"
    7z x -y Payload -so 2>/dev/null | cpio -id 2>/dev/null || true

    HEADER_SRC=""
    if [[ "$SUBDIR" == *t ]]; then
        # PythonT framework: headers in include/python3.Xt/
        for path in \
            "Versions/${MINOR_VER}/include/python${MINOR_VER}t" \
            "include/python${MINOR_VER}t"
        do
            if [ -d "$path" ] && [ -f "$path/Python.h" ]; then
                HEADER_SRC="$path"
                break
            fi
        done
    else
        for path in \
            "Versions/${MINOR_VER}/Headers" \
            "Headers"
        do
            if [ -d "$path" ] && [ -f "$path/Python.h" ]; then
                HEADER_SRC="$path"
                break
            fi
        done
    fi

    if [ -z "$HEADER_SRC" ]; then
        PYTHON_H=$(find . -name "Python.h" -type f | head -1)
        if [ -n "$PYTHON_H" ]; then
            HEADER_SRC=$(dirname "$PYTHON_H")
        fi
    fi

    if [ -z "$HEADER_SRC" ] || [ ! -f "${HEADER_SRC}/Python.h" ]; then
        echo "ERROR: Cannot find headers for Python ${FULL_VER}"
        exit 1
    fi

    mkdir -p "$DEST_DIR"
    cp -r "${HEADER_SRC}/"* "$DEST_DIR/"

    echo "✓ Python ${FULL_VER} headers installed to ${DEST_DIR}"
    echo "  Files: $(ls "$DEST_DIR" | wc -l | tr -d ' ') items"
done

echo ""
echo "=========================================="
echo "Done! Headers installed to: ${TARGET_DIR}"
echo "=========================================="
ls -la "$TARGET_DIR"