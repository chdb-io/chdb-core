# get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJ_DIR="${DIR}/.." # project root directory
BUILD_DIR="$PROJ_DIR/buildlib" # build directory
CHDB_DIR="$PROJ_DIR/chdb" # chdb directory
CHDB_PY_MOD="_chdb"
if [ "${CHDB_FREE_THREADING}" == "1" ]; then
    if [ "${CHDB_CROSSCOMPILING}" == "1" ]; then
        _ft_ver=${CHDB_FREE_THREADING_PYTHON_VERSION:?requires CHDB_FREE_THREADING_PYTHON_VERSION}
        _py_tag=${_ft_ver//./}      # 3.13t → 313t
        EXT_SUFFIX=".cpython-${_py_tag}-darwin.so"
    else
        EXT_SUFFIX=$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')
        if [ -z "$EXT_SUFFIX" ]; then
            echo "Error: failed to get EXT_SUFFIX from free-threading Python"
            exit 1
        fi
    fi
    CHDB_PY_MODULE="${CHDB_PY_MOD}${EXT_SUFFIX}"
else
    CHDB_PY_MODULE="${CHDB_PY_MOD}.abi3.so"
fi
pushd ${PROJ_DIR} > /dev/null
# get_latest_git_tag() returns None for non-three-part tags (e.g. rc tags such
# as v26.5.1-rc.2) and prints "None" while exiting 0, so the `||` fallbacks
# below never fire and CHDB_VERSION becomes the literal "None" — which is what
# `SELECT chdb()` then returns. Capture the value and explicitly reject an
# empty/"None" result before falling back to the raw tag, then "0.0.0".
# On a raise the helper prints its error to stdout before exiting non-zero, so
# reset to empty on failure (|| ...) to keep that text out of CHDB_VERSION and
# let the fallbacks fire.
# An already-set CHDB_VERSION wins, because the caller knows something this cannot:
# which tag is being built. get_latest_git_tag() asks for the newest tag in the
# repository by commit date, which is a different question and answers it wrongly
# whenever the release being built is not the newest tag — cutting v26.7.0 while
# v26.7.1-rc.1 exists would compile and package 26.7.1. The release workflows export
# the tag they are building; everything below is the fallback for a build that has
# no tag to be told about.
if [ -z "${CHDB_VERSION:-}" ]; then
    CHDB_VERSION=$(python3 -c 'import setup; print(setup.get_latest_git_tag())' 2>/dev/null) || CHDB_VERSION=""
    if [ -z "${CHDB_VERSION}" ] || [ "${CHDB_VERSION}" = "None" ]; then
        CHDB_VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
    fi
    if [ -z "${CHDB_VERSION}" ]; then
        CHDB_VERSION="0.0.0"
    fi
fi
popd > /dev/null

# Keep the compile-time CHDB_VERSION constant in the public C header in sync with
# the resolved version, so C API users read the correct version straight from
# chdb.h (no connection needed) and it never drifts from the release tag. The
# committed value in chdb.h is just the last-release default for source-only use.
CHDB_HEADER="${PROJ_DIR}/programs/local/chdb.h"
if [ -n "${CHDB_VERSION}" ] && [ -f "${CHDB_HEADER}" ]; then
    CHDB_HEADER_TMP=$(mktemp)
    # mktemp creates the file 0600 and mv preserves that mode, which would leak
    # a root-only chdb.h into the release tarballs (and onto users' machines
    # via sudo installs). Restore world-readable perms before the rename.
    chmod 0644 "${CHDB_HEADER_TMP}"
    sed -E 's|^#define CHDB_VERSION ".*"|#define CHDB_VERSION "'"${CHDB_VERSION}"'"|' \
        "${CHDB_HEADER}" > "${CHDB_HEADER_TMP}" && mv "${CHDB_HEADER_TMP}" "${CHDB_HEADER}"
fi

if [ "$1" == "cross-compile" ]; then
    return
fi

# try to use largest llvm-strip version
# if none of them are found, use llvm-strip or strip
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /usr/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /usr/local/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
# on macOS Intel
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /usr/local/Cellar/llvm/*/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /usr/local/opt/llvm/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /usr/local/opt/llvm@*/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
# on macOS ARM (Apple Silicon) - Homebrew uses /opt/homebrew
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /opt/homebrew/Cellar/llvm/*/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /opt/homebrew/opt/llvm/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi
if [ -z "$STRIP" ]; then
    STRIP=$(ls -1 /opt/homebrew/opt/llvm@*/bin/llvm-strip* 2>/dev/null | sort -V | tail -n 1)
fi

# if none of them are found, use llvm-strip or strip (which may fail; keep set -e safe)
if [ -z "$STRIP" ]; then
    STRIP=$(command -v llvm-strip 2>/dev/null || true)
fi
if [ -z "$STRIP" ]; then
    STRIP=$(command -v strip 2>/dev/null || true)
fi

echo "STRIP command: $STRIP"
if [ -n "${STRIP}" ]; then
    echo "STRIP location: $(command -v "${STRIP}" 2>/dev/null || echo 'not found')"
else
    echo "STRIP location: not found"
fi

# check current os type, and make ldd command
if [ "$(uname)" == "Darwin" ]; then
    LDD="otool -L"
    if command -v llvm-ar >/dev/null 2>&1; then
        AR="llvm-ar"
    else
        AR="ar"
    fi
    if command -v llvm-nm >/dev/null 2>&1; then
        NM="llvm-nm"
    else
        NM="nm"
    fi
elif [ "$(uname)" == "Linux" ]; then
    LDD="ldd"
    AR="ar"
    NM="nm"
else
    echo "OS not supported"
    exit 1
fi
