#!/bin/bash
# Configure + build chdb for WebAssembly via Emscripten.
# Mirrors chdb/build.sh but targets the OS_WASM platform branch.
#
# Prereqs: source the emsdk env first:
#   source ~/code/emsdk/emsdk_env.sh
#
# Usage: chdb/build-wasm.sh [configure|build]   (default: configure)
#
# CHDB_WASM_FULL=1 builds the FULL (untrimmed) engine instead of the default
# chdb-core-lite trim set (details on the variable below):
#   CHDB_WASM_FULL=1 chdb/build-wasm.sh build     # -> buildwasm-full/

set -eo pipefail

PROJ_DIR=$(cd "$(dirname "$0")/.." && pwd)
# CHDB_WASM_FULL=1: build with CHDB_LITE=OFF, i.e. the complete function/aggregate
# registry (restores ~70 aggregates the lite trim drops, among them
# quantileExactInclusive and largestTriangleThreeBuckets/lttb). Costs ~54 MB raw /
# ~6 MiB gzip over lite, so it is opt-in; the published npm bundle stays lite.
chdb_wasm_full="${CHDB_WASM_FULL:-0}"
if [ "${chdb_wasm_full}" = "1" ]; then
    default_build_dir="${PROJ_DIR}/buildwasm-full"
    # Lite force-switches Release -> MinSizeRel; with CHDB_LITE=OFF that force is
    # gone, so ask for MinSizeRel (-Os) explicitly or the engine silently becomes
    # a -O3 Release build — bigger and not what the lite bundle is compiled as.
    default_build_type="MinSizeRel"
    default_version="0.1-wasm-full"
else
    default_build_dir="${PROJ_DIR}/buildwasm"
    default_build_type="Release"
    default_version="0.1-wasm"
fi
# BUILD_DIR can be overridden (e.g. a separate dir for the single-threaded build).
BUILD_DIR="${BUILD_DIR:-${default_build_dir}}"
STAGE="${1:-configure}"
build_type="${BUILD_TYPE:-${default_build_type}}"
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
    -DENABLE_PYTHON=0 \
    -DUSE_STATIC_LIBRARIES=1 -DSPLIT_SHARED_LIBRARIES=0 \
    -DENABLE_JEMALLOC=0 -DENABLE_ICU=0 \
    -DENABLE_AVX=0 -DENABLE_AVX2=0 -DENABLE_AVX512=0 -DENABLE_AVX512_VBMI=0 \
    -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0 \
    -DENABLE_RUST=0 \
    -DWASM_THREADS=${wasm_threads} \
    -DCHDB_VERSION=${CHDB_VERSION:-${default_version}} \
    -DCOMPILER_CACHE=${COMPILER_CACHE:-disabled}"

# Variant selection. CHDB_LITE is passed explicitly either way so the value in the
# cache never depends on cmake/target.cmake's OS_WASM default.
if [ "${chdb_wasm_full}" = "1" ]; then
    # WERROR=0: only lite relaxes -Wframe-larger-than to 131072 (cmake/warnings.cmake);
    # non-lite keeps upstream's 65536, which some TUs cross under -Os.
    #
    # The four ENABLE_* opt-ins: this build runs with ENABLE_LIBRARIES=0 and the
    # CHDB_LITE block is what normally opts them back in, so without lite nothing
    # re-enables them and the "full" engine would lack libraries the lite one has.
    CMAKE_ARGS="${CMAKE_ARGS} \
    -DCHDB_LITE=OFF -DWERROR=0 \
    -DENABLE_RAPIDJSON=1 -DENABLE_BROTLI=1 -DENABLE_SIMDJSON=1 -DENABLE_UTF8PROC=1"
else
    CMAKE_ARGS="${CMAKE_ARGS} -DCHDB_LITE=ON"
fi

echo "=== chdb-wasm variant: $([ "${chdb_wasm_full}" = "1" ] && echo 'FULL (untrimmed registry)' || echo 'lite (trimmed)')" \
     "| threads=${wasm_threads} | ${build_type} | ${BUILD_DIR} ==="

if [ "${STAGE}" = "configure" ] || [ "${STAGE}" = "build" ]; then
    echo "=== emcmake cmake configure (log -> ${BUILD_DIR}/configure.log) ==="
    emcmake cmake ${CMAKE_ARGS} "${PROJ_DIR}" 2>&1 | tee configure.log
fi

if [ "${STAGE}" = "build" ]; then
    echo "=== building chdb_wasm (-> programs/wasm/chdb.mjs + chdb.wasm) ==="
    cmake --build . --target chdb_wasm 2>&1 | tee build.log
fi
