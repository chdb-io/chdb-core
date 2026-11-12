#!/bin/bash
# Cross-compile the chDB Python module for Windows x86_64 on a Linux x86_64 host.
#
# Produces, in chdb/:
#   _chdb.pyd                               the module (stable ABI, python3.dll, Python >= 3.9)
#   pybind11nonlimitedapi_stubs.dll         loads the shim that matches the running Python
#   pybind11nonlimitedapi_chdb_3.X.dll      one shim per supported Python
#
# The toolchain is the host clang/lld (version 21, like the Linux build) targeting
# x86_64-w64-windows-gnu, ClickHouse's own libc++/libunwind/compiler-rt from contrib, and the
# mingw-w64 13 UCRT headers and import libraries (chdb/build/download_windows_deps.sh). See
# cmake/windows/toolchain-x86_64.cmake, which comes from ClickHouse PR #112824.
#
# Environment:
#   CHDB_WINDOWS_DEPS_DIR   where the dependencies are unpacked (default ~/.cache/chdb-windows-deps)
#   CHDB_WINDOWS_BUILD_DIR  build directory (default <repo>/build-windows-x86_64)
#   CHDB_VERSION            engine version (default: derived from git tags, see chdb/vars.sh)
#   CC, CXX                 host clang 21 (default clang-21, clang++-21)
#   CHDB_WINDOWS_CONFIGURE_ONLY=1  stop after cmake

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [ "${CHDB_LITE:-0}" = "1" ] || [ "${CHDB_FREE_THREADING:-0}" = "1" ]; then
    echo "Error: the Windows build supports neither CHDB_LITE nor CHDB_FREE_THREADING yet" >&2
    exit 1
fi

# PreLoad.cmake refuses to configure with these set.
unset CFLAGS CXXFLAGS LDFLAGS

# vars.sh expects neither nounset nor pipefail (its git-describe fallback fails without a tag).
set +uo pipefail
. "${DIR}/vars.sh" cross-compile
set -uo pipefail

DEPS_DIR=${CHDB_WINDOWS_DEPS_DIR:-${HOME}/.cache/chdb-windows-deps}
BUILD_DIR=${CHDB_WINDOWS_BUILD_DIR:-${PROJ_DIR}/build-windows-x86_64}
PYTHON_VERSIONS="3.9 3.10 3.11 3.12 3.13 3.14"

# The compilers must be the plain host clang: the toolchain file selects the Windows target, and
# the native host tools the build bootstraps (flatc for Arrow) reuse these compilers for Linux.
export CC=${CC:-clang-21}
export CXX=${CXX:-clang++-21}
for tool in "${CC}" "${CXX}" ld.lld-21 llvm-strip-21 llvm-readobj-21 llvm-objdump-21; do
    command -v "${tool}" >/dev/null || { echo "Error: ${tool} not found" >&2; exit 1; }
done

bash "${DIR}/build/download_windows_deps.sh" "${DEPS_DIR}"
MINGW_SYSROOT="${DEPS_DIR}/mingw/usr/x86_64-w64-mingw32ucrt"

# Windows v1 feature set: the engine with the formats the Python API relies on (Arrow, Parquet,
# JSON), the sqids and ULID functions, and no network-backed storage. ENABLE_LIBRARIES=0 turns
# every optional contrib off; the ones the module needs are enabled explicitly.
CMAKE_ARGS=(
    -G Ninja
    -DCMAKE_TOOLCHAIN_FILE="${PROJ_DIR}/cmake/windows/toolchain-x86_64.cmake"
    -DMINGW_SYSROOT="${MINGW_SYSROOT}"
    -DCMAKE_BUILD_TYPE=Release
    -DWERROR=0
    -DCHDB_LITE=OFF
    -DENABLE_LIBRARIES=0
    -DENABLE_PARQUET=1 -DENABLE_THRIFT=1 -DENABLE_BROTLI=1
    -DENABLE_RAPIDJSON=1 -DENABLE_SIMDJSON=1 -DENABLE_SIMDUTF=1 -DENABLE_UTF8PROC=1
    -DENABLE_SQIDS=1 -DENABLE_ULID=1
    -DENABLE_ARROW_FLIGHT=0 -DENABLE_ICU=0 -DENABLE_PROTOBUF=0 -DENABLE_AVRO=0
    -DENABLE_AWS_S3=0 -DENABLE_AZURE_BLOB_STORAGE=0 -DENABLE_HDFS=0
    -DENABLE_JEMALLOC=0 -DENABLE_RUST=0 -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0
    -DENABLE_LIBFIU=0 -DGLIBC_COMPATIBILITY=0 -DENABLE_CLIENT_AI=0
    -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_EXAMPLES=0 -DENABLE_BENCHMARKS=0
    -DENABLE_FUZZING=0 -DENABLE_BUZZHOUSE=0 -DENABLE_FUZZER_TEST=0
    -DENABLE_THINLTO=0 -DENABLE_XRAY=0 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=0
    -DENABLE_CLICKHOUSE_ALL=0 -DENABLE_CLICKHOUSE_LOCAL=1
    -DENABLE_NLP=0 -DWITH_LIBSTEMMER=0
    -DENABLE_PYTHON=1 -DCHDB_CROSSCOMPILING=1
    -DPYBIND11_NONLIMITEDAPI_PYTHON_HEADERS_VERSION=3.9
    -DCHDB_PYTHON_INCLUDE_DIR_PREFIX="${DEPS_DIR}/python"
    -DCHDB_PYTHON_LIB_DIR_PREFIX="${DEPS_DIR}/python"
    -DCHDB_PYBIND11_SHIM_VERSIONS="${PYTHON_VERSIONS// /;}"
    -DCHDB_VERSION="${CHDB_VERSION}"
)

mkdir -p "${BUILD_DIR}"
cmake -S "${PROJ_DIR}" -B "${BUILD_DIR}" "${CMAKE_ARGS[@]}"
if [ "${CHDB_WINDOWS_CONFIGURE_ONLY:-0}" = "1" ]; then
    exit 0
fi

TARGETS=(chdb_pymodule pybind11_stubs)
for v in ${PYTHON_VERSIONS}; do
    TARGETS+=("pybind11nonlimitedapi_chdb_${v}")
done
ninja -C "${BUILD_DIR}" "${TARGETS[@]}"

# Stage the module and its DLLs next to the Python sources, where setup.py picks them up.
# Only the Windows outputs: a Linux build in the same checkout keeps its .so files, and setup.py
# packages by CHDB_TARGET_OS.
rm -f "${CHDB_DIR}"/*.pyd "${CHDB_DIR}"/*.dll
cp -a "${BUILD_DIR}/_chdb.pyd" "${CHDB_DIR}/_chdb.pyd"
cp -a "${BUILD_DIR}/contrib/pybind11-cmake/pybind11nonlimitedapi_stubs.dll" "${CHDB_DIR}/"
for v in ${PYTHON_VERSIONS}; do
    cp -a "${BUILD_DIR}/contrib/pybind11-cmake/pybind11nonlimitedapi_chdb_${v}.dll" "${CHDB_DIR}/"
done
for f in "${CHDB_DIR}"/_chdb.pyd "${CHDB_DIR}"/*.dll; do
    llvm-strip-21 --strip-all "${f}"
done

python3 "${DIR}/build/check_windows_module.py" "${CHDB_DIR}" ${PYTHON_VERSIONS}
ls -lh "${CHDB_DIR}"/_chdb.pyd "${CHDB_DIR}"/*.dll
