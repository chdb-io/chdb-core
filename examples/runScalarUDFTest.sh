#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

NANOARROW_VER="0.8.0"
NANOARROW_DIR="nanoarrow-${NANOARROW_VER}"
NANOARROW_TAR="apache-arrow-nanoarrow-${NANOARROW_VER}.tar.gz"
NANOARROW_URL="https://github.com/apache/arrow-nanoarrow/releases/download/apache-arrow-nanoarrow-${NANOARROW_VER}/${NANOARROW_TAR}"

# ---- Step 1: Download & extract nanoarrow if needed ----
if [ ! -d "$NANOARROW_DIR" ]; then
    echo "Downloading nanoarrow ${NANOARROW_VER}..."
    curl -fSL -o "$NANOARROW_TAR" "$NANOARROW_URL"
    mkdir -p "$NANOARROW_DIR"
    tar xzf "$NANOARROW_TAR" -C "$NANOARROW_DIR" --strip-components=1
    rm -f "$NANOARROW_TAR"
fi

NA_SRC="$NANOARROW_DIR/src"

# ---- Step 2: Generate nanoarrow_config.h from template ----
CONFIG_H="$NA_SRC/nanoarrow/nanoarrow_config.h"
if [ ! -f "$CONFIG_H" ]; then
    echo "Generating nanoarrow_config.h..."
    sed -e 's/@NANOARROW_VERSION_MAJOR@/0/g' \
        -e 's/@NANOARROW_VERSION_MINOR@/8/g' \
        -e 's/@NANOARROW_VERSION_PATCH@/0/g' \
        -e 's/@NANOARROW_VERSION@/0.8.0/g' \
        -e 's/@NANOARROW_NAMESPACE_DEFINE@//g' \
        "$NA_SRC/nanoarrow/nanoarrow_config.h.in" > "$CONFIG_H"
fi

# ---- Step 3: Compile ----
echo "Compiling chdbScalarUDFTest..."

CC="${CC:-cc}"
CFLAGS="-std=c99 -O2 -Wall -Wno-unused-function"

if [ "$(uname)" = "Darwin" ]; then
    LDD="otool -L"
    LIB_PATH_VAR="DYLD_LIBRARY_PATH"
else
    LDD="ldd"
    LIB_PATH_VAR="LD_LIBRARY_PATH"
fi

# The callbacks read their arguments and build their results with
# nanoarrow's array/schema/view helpers; no IPC code is needed.
$CC $CFLAGS \
    -I"$NA_SRC" \
    -I../programs/local/ \
    chdbScalarUDFTest.c \
    "$NA_SRC/nanoarrow/common/array.c" \
    "$NA_SRC/nanoarrow/common/schema.c" \
    "$NA_SRC/nanoarrow/common/utils.c" \
    -L.. -lchdb \
    -o chdbScalarUDFTest

echo "Build OK"
export ${LIB_PATH_VAR}="$DIR/.."
$LDD ./chdbScalarUDFTest

echo ""
echo "=== Run ==="
# The query result cache test needs an on-disk cache, declared in a configuration file.
CACHE_DIR="$(mktemp -d)"
trap 'rm -rf "$CACHE_DIR"' EXIT
cat > "$CACHE_DIR/config.xml" <<EOF
<clickhouse><filesystem_caches><query_results><path>$CACHE_DIR/query_results/</path><max_size>16Mi</max_size><max_file_segment_size>4Mi</max_file_segment_size></query_results></filesystem_caches></clickhouse>
EOF
./chdbScalarUDFTest "$CACHE_DIR/config.xml"
