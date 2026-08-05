#!/bin/bash
# Configure + build chdb for WebAssembly via Emscripten.
# Mirrors chdb/build.sh but targets the OS_WASM platform branch.
#
# Prereqs: source the emsdk env first:
#   source ~/code/emsdk/emsdk_env.sh
#
# Usage: chdb/build-wasm.sh [configure|build]   (default: configure)

set -eo pipefail

PROJ_DIR=$(cd "$(dirname "$0")/.." && pwd)
# BUILD_DIR can be overridden (e.g. a separate dir for the single-threaded build).
BUILD_DIR="${BUILD_DIR:-${PROJ_DIR}/buildwasm}"
STAGE="${1:-configure}"
build_type="${BUILD_TYPE:-MinSizeRel}"
# WASM_THREADS=ON (default): pthreads (Web Workers + SharedArrayBuffer); the page
# must be cross-origin isolated. WASM_THREADS=OFF: single-threaded build with no
# SharedArrayBuffer dependency, runs on non-isolated pages.
wasm_threads="${WASM_THREADS:-ON}"

if ! command -v emcmake >/dev/null 2>&1; then
    echo "emcmake not found. Run: source ~/code/emsdk/emsdk_env.sh" >&2
    exit 1
fi

mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# WASM: single-threaded MVP first (mirror duckdb-wasm's wasm_mvp default),
# no networked storage, no JIT, no Rust, no jemalloc. Most of these are also
# forced off in cmake/target.cmake's OS_WASM block; passing them here keeps the
# configure log explicit.
CMAKE_ARGS="-DCMAKE_BUILD_TYPE=${build_type} \
    -DENABLE_THINLTO=0 -DENABLE_TESTS=0 -DENABLE_XRAY=0 \
    -DENABLE_CLICKHOUSE_SERVER=0 -DENABLE_CLICKHOUSE_CLIENT=0 \
    -DENABLE_CLICKHOUSE_KEEPER=0 -DENABLE_CLICKHOUSE_KEEPER_CONVERTER=0 \
    -DENABLE_CLICKHOUSE_LOCAL=1 -DENABLE_CLICKHOUSE_SU=0 \
    -DENABLE_CLICKHOUSE_BENCHMARK=0 -DENABLE_CLICKHOUSE_COPIER=0 \
    -DENABLE_CLICKHOUSE_DISKS=0 -DENABLE_CLICKHOUSE_FORMAT=0 \
    -DENABLE_CLICKHOUSE_GIT_IMPORT=0 -DENABLE_CLICKHOUSE_OBFUSCATOR=0 \
    -DENABLE_CLICKHOUSE_ODBC_BRIDGE=0 -DENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER=0 \
    -DENABLE_CLICKHOUSE_ALL=0 \
    -DENABLE_UTILS=0 -DENABLE_EXAMPLES=0 -DENABLE_BENCHMARKS=0 \
    -DENABLE_FUZZING=OFF -DENABLE_BUZZHOUSE=OFF -DENABLE_FUZZER_TEST=OFF \
    -DENABLE_LIBRARIES=0 \
    -DCHDB_LITE=OFF \
    -DENABLE_PYTHON=0 \
    -DUSE_STATIC_LIBRARIES=1 -DSPLIT_SHARED_LIBRARIES=0 \
    -DENABLE_JEMALLOC=0 -DENABLE_ICU=0 \
    -DENABLE_AVX=0 -DENABLE_AVX2=0 -DENABLE_AVX512=0 -DENABLE_AVX512_VBMI=0 \
    -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0 \
    -DENABLE_RUST=0 \
    -DWASM_THREADS=${wasm_threads} \
    -DCHDB_VERSION=${CHDB_VERSION:-0.1-wasm} \
    -DCOMPILER_CACHE=${COMPILER_CACHE:-disabled}"

if [ "${STAGE}" = "configure" ] || [ "${STAGE}" = "build" ]; then
    echo "=== emcmake cmake configure (log -> ${BUILD_DIR}/configure.log) ==="
    emcmake cmake ${CMAKE_ARGS} "${PROJ_DIR}" 2>&1 | tee configure.log
fi

if [ "${STAGE}" = "build" ]; then
    echo "=== building chdb_wasm (-> programs/wasm/chdb.mjs + chdb.wasm) ==="
    cmake --build . --target chdb_wasm 2>&1 | tee build.log
fi
