#!/bin/bash

set -e

# Cross-compile chdb for macOS (x86_64 or arm64) on Linux
# Usage: ./build_mac_on_linux_universal.sh [x86_64|arm64] [Release|Debug]

# Parse arguments
TARGET_ARCH=${1:-x86_64}
build_type=${2:-RelWithDebInfo}

# chdb-core-lite has a 50 MiB wheel budget; -g would bloat past it.
if [ "${CHDB_LITE}" = "1" ] && [ "${build_type}" = "RelWithDebInfo" ]; then
    build_type="Release"
fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. ${DIR}/vars.sh cross-compile

# Validate architecture
if [[ "$TARGET_ARCH" != "x86_64" && "$TARGET_ARCH" != "arm64" ]]; then
    echo "Error: Invalid architecture. Use 'x86_64' or 'arm64'"
    echo "Usage: $0 [x86_64|arm64] [Release|Debug]"
    exit 1
fi

# Verify we're running on Linux
if [ "$(uname)" != "Linux" ]; then
    echo "Error: This script must be run on Linux"
    exit 1
fi

echo "Cross-compiling chdb for macOS ${TARGET_ARCH} on Linux..."

# Set architecture-specific variables first
if [ "$TARGET_ARCH" == "x86_64" ]; then
    DARWIN_TRIPLE="x86_64-apple-darwin"
    TOOLCHAIN_FILE="cmake/darwin/toolchain-x86_64.cmake"
    BUILD_DIR_SUFFIX="darwin-x86_64"
    CPU_FEATURES="-DENABLE_AVX=0 -DENABLE_AVX2=0"
    SDK_DIR="darwin-x86_64"
else
    # arm64
    DARWIN_TRIPLE="aarch64-apple-darwin"
    TOOLCHAIN_FILE="cmake/darwin/toolchain-aarch64.cmake"
    BUILD_DIR_SUFFIX="darwin-arm64"
    CPU_FEATURES="-DENABLE_AVX=0 -DENABLE_AVX2=0"
    SDK_DIR="darwin-aarch64"
fi

# Download macOS SDK
# Use a download-then-verify-then-extract flow rather than the original
# `curl -L | tar xJ` pipe: GitHub release-assets CDN occasionally serves a
# short HTML/error body for this asset, which the old pipe would silently
# stream into xz and abort the build. -f makes curl fail on HTTP errors,
# --retry/--retry-all-errors rides out CDN blips, and the size check catches
# any remaining short-payload pathology before extraction.
SDK_PATH="${PROJ_DIR}/cmake/toolchain/${SDK_DIR}"
SDK_URL='https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz'
echo "Downloading macOS SDK to ${SDK_PATH}..."
mkdir -p "${SDK_PATH}"
cd "${SDK_PATH}"
if ! curl -fL --retry 5 --retry-delay 5 --retry-all-errors \
     -o MacOSX11.0.sdk.tar.xz "${SDK_URL}"; then
    echo "Error: Failed to download macOS SDK from ${SDK_URL}"
    exit 1
fi
sdk_size=$(stat -c %s MacOSX11.0.sdk.tar.xz 2>/dev/null || stat -f %z MacOSX11.0.sdk.tar.xz)
if [ "${sdk_size:-0}" -lt 10000000 ]; then
    echo "Error: SDK tarball too small (${sdk_size} bytes) — likely a CDN error body, aborting"
    exit 1
fi
if ! tar xJf MacOSX11.0.sdk.tar.xz --strip-components=1; then
    echo "Error: Failed to extract macOS SDK"
    exit 1
fi
rm -f MacOSX11.0.sdk.tar.xz
echo "macOS SDK downloaded successfully"

# Download Python headers
echo "Downloading Python headers..."
if ! bash "${DIR}/build/download_python_headers.sh"; then
    echo "Error: Failed to download Python headers"
    exit 1
fi

# Install cctools
if ! bash "${DIR}/build/install_cctools.sh" "${TARGET_ARCH}"; then
    echo "Error: Failed to install cctools"
    exit 1
fi
# Set CCTOOLS path after installation
CCTOOLS_INSTALL_DIR="${HOME}/cctools"
CCTOOLS_BIN="${CCTOOLS_INSTALL_DIR}/bin"

# Override tools with cross-compilation versions from cctools
# export STRIP="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-strip"
export STRIP="llvm-strip-21"
export AR="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-ar"
export NM="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-nm"
export LDD="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-otool -L"

echo "Using cross-compilation tools:"
echo "  STRIP: ${STRIP}"
echo "  AR: ${AR}"
echo "  NM: ${NM}"
echo "  LDD: ${LDD}"

BUILD_DIR=${PROJ_DIR}/build-${BUILD_DIR_SUFFIX}

export CC=clang-21
export CXX=clang++-21

RUST_FEATURES="-DENABLE_RUST=0"
GLIBC_COMPATIBILITY="-DGLIBC_COMPATIBILITY=0"
UNWIND="-DUSE_UNWIND=0"
JEMALLOC="-DENABLE_JEMALLOC=0"
PYINIT_ENTRY="-Wl,-exported_symbol,_PyInit_${CHDB_PY_MOD}"
HDFS="-DENABLE_HDFS=0 -DENABLE_GSASL_LIBRARY=0 -DENABLE_KRB5=0"
MYSQL="-DENABLE_MYSQL=1"
ICU="-DENABLE_ICU=0"
SED_INPLACE="sed -i"
LLVM="-DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0"
CMAKE_AR_FILEPATH="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-ar"
CMAKE_INSTALL_NAME_TOOL="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-install_name_tool"
CMAKE_RANLIB_FILEPATH="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-ranlib"
CMAKE_LINKER_NAME="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-ld"

if [ ! -d $BUILD_DIR ]; then
    mkdir $BUILD_DIR
fi

cd ${BUILD_DIR}

if [ "${CHDB_LITE}" = "1" ]; then
    # chdb-core-lite for macOS: minimal arg list; the CHDB_LITE block in
    # CMakeLists.txt fills in all trim flags / ENABLE_*=0 / linker flags.
    # macOS-specific opt-outs (JEMALLOC=0, ICU=0, GLIBC_COMPATIBILITY=0, UNWIND=0)
    # are passed explicitly here; the umbrella's `if (NOT DEFINED ${_flag})` guard
    # preserves them.
    CMAKE_ARGS="-DCMAKE_BUILD_TYPE=${build_type} \
        -DCMAKE_AR:FILEPATH=${CMAKE_AR_FILEPATH} \
        -DCMAKE_INSTALL_NAME_TOOL=${CMAKE_INSTALL_NAME_TOOL} \
        -DCMAKE_RANLIB:FILEPATH=${CMAKE_RANLIB_FILEPATH} \
        -DLINKER_NAME=${CMAKE_LINKER_NAME} \
        -DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE} \
        -DENABLE_THINLTO=0 -DENABLE_TESTS=0 -DCHDB_LITE=ON \
        -DENABLE_CLICKHOUSE_SERVER=0 -DENABLE_CLICKHOUSE_CLIENT=0 \
        -DENABLE_CLICKHOUSE_KEEPER=0 -DENABLE_CLICKHOUSE_KEEPER_CONVERTER=0 \
        -DENABLE_CLICKHOUSE_LOCAL=1 -DENABLE_CLICKHOUSE_SU=0 \
        -DENABLE_CLICKHOUSE_BENCHMARK=0 -DENABLE_CLICKHOUSE_COPIER=0 \
        -DENABLE_CLICKHOUSE_DISKS=0 -DENABLE_CLICKHOUSE_FORMAT=0 \
        -DENABLE_CLICKHOUSE_GIT_IMPORT=0 -DENABLE_CLICKHOUSE_OBFUSCATOR=0 \
        -DENABLE_CLICKHOUSE_ODBC_BRIDGE=0 -DENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER=0 \
        -DENABLE_CLICKHOUSE_ALL=0 \
        -DUSE_STATIC_LIBRARIES=1 -DSPLIT_SHARED_LIBRARIES=0 \
        -DENABLE_UTILS=0 -DENABLE_EXAMPLES=0 -DENABLE_BENCHMARKS=0 \
        -DENABLE_FUZZING=OFF -DENABLE_BUZZHOUSE=OFF -DENABLE_FUZZER_TEST=OFF \
        ${GLIBC_COMPATIBILITY} ${UNWIND} ${JEMALLOC} ${ICU} ${LLVM} \
        ${CPU_FEATURES} \
        -DENABLE_AVX512=0 -DENABLE_AVX512_VBMI=0 \
        -DCHDB_VERSION=${CHDB_VERSION} \
        "
else
    CMAKE_ARGS="-DCMAKE_BUILD_TYPE=${build_type} \
    -DCMAKE_AR:FILEPATH=${CMAKE_AR_FILEPATH} \
    -DCMAKE_INSTALL_NAME_TOOL=${CMAKE_INSTALL_NAME_TOOL} \
    -DCMAKE_RANLIB:FILEPATH=${CMAKE_RANLIB_FILEPATH} \
    -DLINKER_NAME=${CMAKE_LINKER_NAME} \
    -DENABLE_THINLTO=0 -DENABLE_TESTS=0 -DENABLE_CLICKHOUSE_SERVER=0 -DENABLE_CLICKHOUSE_CLIENT=0 \
    -DENABLE_CLICKHOUSE_KEEPER=0 -DENABLE_CLICKHOUSE_KEEPER_CONVERTER=0 -DENABLE_CLICKHOUSE_LOCAL=1 -DENABLE_CLICKHOUSE_SU=0 -DENABLE_CLICKHOUSE_BENCHMARK=0 \
    -DENABLE_AZURE_BLOB_STORAGE=1 -DENABLE_CLICKHOUSE_COPIER=0 -DENABLE_CLICKHOUSE_DISKS=0 -DENABLE_CLICKHOUSE_FORMAT=0 -DENABLE_CLICKHOUSE_GIT_IMPORT=0 \
    -DENABLE_AWS_S3=1 -DENABLE_HIVE=0 -DENABLE_AVRO=1 \
    -DENABLE_CLICKHOUSE_OBFUSCATOR=0 -DENABLE_CLICKHOUSE_ODBC_BRIDGE=0 -DENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER=0 \
    -DENABLE_KAFKA=1 -DENABLE_LIBPQXX=1 -DENABLE_NATS=0 -DENABLE_AMQPCPP=0 -DENABLE_NURAFT=0 \
    -DENABLE_CASSANDRA=0 -DENABLE_ODBC=0 -DENABLE_NLP=0 \
    -DENABLE_LDAP=0 \
    -DENABLE_CLIENT_AI=1 \
    ${MYSQL} \
    -DUSE_MONGODB=1 \
    -DENABLE_USEARCH=1 -DENABLE_SIMSIMD=1 \
    ${HDFS} \
    -DENABLE_LIBRARIES=0 -DENABLE_SQIDS=1 ${RUST_FEATURES} \
    ${GLIBC_COMPATIBILITY} \
    -DENABLE_UTILS=0 ${LLVM} ${UNWIND} \
    ${ICU} -DENABLE_UTF8PROC=1 ${JEMALLOC} \
    -DENABLE_PARQUET=1 -DENABLE_ROCKSDB=1 -DENABLE_SQLITE=1 -DENABLE_VECTORSCAN=1 \
    -DENABLE_PROTOBUF=1 -DENABLE_THRIFT=1 -DENABLE_MSGPACK=1 \
    -DENABLE_BROTLI=1 -DENABLE_H3=1 -DENABLE_CURL=1 \
    -DENABLE_CLICKHOUSE_ALL=0 -DUSE_STATIC_LIBRARIES=1 -DSPLIT_SHARED_LIBRARIES=0 \
    -DENABLE_SIMDJSON=1 -DENABLE_RAPIDJSON=1 \
    ${CPU_FEATURES} \
    -DENABLE_AVX512=0 -DENABLE_AVX512_VBMI=0 \
    -DENABLE_BASE64=1 \
    -DENABLE_LIBFIU=1 \
    -DCHDB_VERSION=${CHDB_VERSION} \
    -DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE} \
    "
fi

LIBCHDB_SO="libchdb.so"
BINARY=${BUILD_DIR}/programs/clickhouse

# chdb-core-lite only ships the Python module (_chdb.abi3.so); skip the
# standalone libchdb.so cross-compile path entirely.
if [ "${CHDB_LITE}" != "1" ]; then
    # Build libchdb.so
    echo "Executing cmake..."
    cmake ${CMAKE_ARGS} -DENABLE_PYTHON=0 ..
    ninja -d keeprsp
    echo -e "\nBINARY: ${BINARY}"
    ls -lh ${BINARY}
    echo -e "\nfile info of ${BINARY}"
    file ${BINARY}
    rm -f ${BINARY}

    cd ${BUILD_DIR}
    ninja -d keeprsp -v > build.log || true
    USING_RESPONSE_FILE=$(grep -m 1 'clang++.*-o programs/clickhouse .*' build.log | grep '@CMakeFiles/clickhouse.rsp' || true)

    if [ ! "${USING_RESPONSE_FILE}" == "" ]; then
        if [ -f CMakeFiles/clickhouse.rsp ]; then
            cp -a CMakeFiles/clickhouse.rsp CMakeFiles/libchdb.rsp
        else
            echo "CMakeFiles/clickhouse.rsp not found"
            exit 1
        fi
    fi

    LIBCHDB_CMD=$(grep -m 1 'clang++.*-o programs/clickhouse .*' build.log \
        | sed "s/-o programs\/clickhouse/-fPIC -shared -o ${LIBCHDB_SO}/" \
        | sed 's/^[^&]*&& //' | sed 's/&&.*//' \
        | sed 's/ -Wl,-undefined,error/ -Wl,-undefined,dynamic_lookup/g' \
        | sed 's/ -Xlinker --no-undefined//g' \
        | sed 's/@CMakeFiles\/clickhouse.rsp/@CMakeFiles\/libchdb.rsp/g' \
         )

    # Generate the command to generate libchdb.so
    LIBCHDB_CMD=$(echo ${LIBCHDB_CMD} | sed 's/ '${CHDB_PY_MODULE}'/ '${LIBCHDB_SO}'/g')

    if [ ! "${USING_RESPONSE_FILE}" == "" ]; then
        ${SED_INPLACE} 's/ '${CHDB_PY_MODULE}'/ '${LIBCHDB_SO}'/g' CMakeFiles/libchdb.rsp
    fi

    # Restrict libchdb.so exports to the C-ABI allow-list, same as the native
    # build (chdb/build.sh). This must hide the bundled abseil/protobuf: if they
    # stay exported, dyld coalesces them with another module's copy (e.g.
    # pyarrow) and libchdb's protobuf static-init deadlocks on a foreign absl
    # mutex. To expose a new C-ABI symbol, add it to libchdb_export_macos.txt
    # (never widen exports here).
    LIBCHDB_CMD="${LIBCHDB_CMD} -Wl,-exported_symbols_list,${CHDB_DIR}/libchdb_export_macos.txt"
    # Relocatable install name — see chdb/build.sh for rationale. Consumers
    # link with: -lchdb -L<dir> -Wl,-rpath,<dir>
    LIBCHDB_CMD="${LIBCHDB_CMD} -Wl,-install_name,@rpath/libchdb.so"

    LIBCHDB_CMD=$(echo ${LIBCHDB_CMD} | sed 's/@CMakeFiles\/clickhouse.rsp/@CMakeFiles\/libchdb.rsp/g')

    # Save the command to a file for debug
    echo ${LIBCHDB_CMD} > libchdb_cmd.sh

    # Build libchdb.so
    echo "Building libchdb.so..."
    ${LIBCHDB_CMD}

    LIBCHDB_DIR=${BUILD_DIR}/
    LIBCHDB=${LIBCHDB_DIR}/${LIBCHDB_SO}
    ls -lh ${LIBCHDB}
fi

# Build chdb python module
CHDB_PYTHON_INCLUDE_DIR_PREFIX="${HOME}/python_include"
FREE_THREADING_CMAKE=""
if [ "${CHDB_FREE_THREADING}" == "1" ]; then
    if [ -z "${CHDB_FREE_THREADING_PYTHON_VERSION}" ]; then
        echo "Error: CHDB_FREE_THREADING=1 requires CHDB_FREE_THREADING_PYTHON_VERSION (e.g. 3.13t)"
        exit 1
    fi
    FT_PY_VERSION="${CHDB_FREE_THREADING_PYTHON_VERSION}"
    FREE_THREADING_CMAKE="-DCHDB_FREE_THREADING=1 -DCHDB_FREE_THREADING_PYTHON_VERSION=${FT_PY_VERSION}"
fi
cmake ${CMAKE_ARGS} ${FREE_THREADING_CMAKE} -DENABLE_PYTHON=1 -DCHDB_CROSSCOMPILING=1 -DCHDB_PYTHON_INCLUDE_DIR_PREFIX=${CHDB_PYTHON_INCLUDE_DIR_PREFIX} -DPYBIND11_NOPYTHON=ON ..
ninja -d keeprsp || true

# Delete the binary and run ninja -v again to capture the command
rm -f ${BINARY}
cd ${BUILD_DIR}
ninja -d keeprsp -v > build.log || true

USING_RESPONSE_FILE=$(grep -m 1 'clang++.*-o programs/clickhouse .*' build.log | grep '@CMakeFiles/clickhouse.rsp' || true)

if [ ! "${USING_RESPONSE_FILE}" == "" ]; then
    if [ -f CMakeFiles/clickhouse.rsp ]; then
        cp -a CMakeFiles/clickhouse.rsp CMakeFiles/pychdb.rsp
    else
        echo "CMakeFiles/clickhouse.rsp not found"
        exit 1
    fi
fi

# Extract the command to generate CHDB_PY_MODULE
PYCHDB_CMD=$(grep -m 1 'clang++.*-o programs/clickhouse .*' build.log \
    | sed "s/-o programs\/clickhouse/-fPIC -Wl,-undefined,dynamic_lookup -shared ${PYINIT_ENTRY} -o ${CHDB_PY_MODULE}/" \
    | sed 's/^[^&]*&& //' | sed 's/&&.*//' \
    | sed 's/ -Wl,-undefined,error/ -Wl,-undefined,dynamic_lookup/g' \
    | sed 's/ -Xlinker --no-undefined//g' \
    | sed 's/@CMakeFiles\/clickhouse.rsp/@CMakeFiles\/pychdb.rsp/g' \
     )

# For macOS, set rpath
PYCHDB_CMD=$(echo ${PYCHDB_CMD} | sed 's|-Wl,-rpath,/[^[:space:]]*/pybind11-cmake|-Wl,-rpath,@loader_path|g')

# Save the command to a file for debug
echo ${PYCHDB_CMD} > pychdb_cmd.sh

echo "Building Python module..."
${PYCHDB_CMD}

ls -lh ${CHDB_PY_MODULE}

## Check all the so files
LIBCHDB_DIR=${BUILD_DIR}/

PYCHDB=${LIBCHDB_DIR}/${CHDB_PY_MODULE}
LIBCHDB=${LIBCHDB_DIR}/${LIBCHDB_SO}

if [ ${build_type} == "Debug" ]; then
    echo -e "\nDebug build, skip strip and debug symbol extraction"
elif [ ${build_type} == "RelWithDebInfo" ] && [ "${CHDB_LITE}" != "1" ]; then
    echo -e "\nExtracting debug symbols before strip..."
    DSYMUTIL=$(which dsymutil-19 2>/dev/null || which llvm-dsymutil-19 2>/dev/null || which dsymutil 2>/dev/null || which llvm-dsymutil 2>/dev/null || which ${CCTOOLS_BIN}/${DARWIN_TRIPLE}-dsymutil 2>/dev/null || true)
    if [ -n "${DSYMUTIL}" ]; then
        ${DSYMUTIL} ${PYCHDB} -o ${PYCHDB}.dSYM
        ${DSYMUTIL} ${LIBCHDB} -o ${LIBCHDB}.dSYM
        echo "Debug symbols extracted:"
        du -sh ${PYCHDB}.dSYM ${LIBCHDB}.dSYM
    else
        echo "ERROR: llvm-dsymutil not found, cannot extract debug symbols"
        exit 1
    fi

    echo -e "\nStrip the binary:"
    ${STRIP} -S -x ${PYCHDB}
    ${STRIP} -S -x ${LIBCHDB}
else
    echo -e "\n${build_type} build, strip without debug symbol extraction"
    ${STRIP} -S -x ${PYCHDB}
    [ "${CHDB_LITE}" != "1" ] && ${STRIP} -S -x ${LIBCHDB}
fi

echo -e "\nPYCHDB: ${PYCHDB}"
ls -lh ${PYCHDB}
echo -e "\nfile info of ${PYCHDB}"
file ${PYCHDB}

if [ "${CHDB_LITE}" != "1" ]; then
    echo -e "\nLIBCHDB: ${LIBCHDB}"
    ls -lh ${LIBCHDB}
    echo -e "\nfile info of ${LIBCHDB}"
    file ${LIBCHDB}
fi

rm -f ${CHDB_DIR}/*.so
cp -a ${PYCHDB} ${CHDB_DIR}/${CHDB_PY_MODULE}
[ "${CHDB_LITE}" != "1" ] && cp -a ${LIBCHDB} ${PROJ_DIR}/${LIBCHDB_SO}

if [ ${build_type} == "RelWithDebInfo" ] && [ "${CHDB_LITE}" != "1" ]; then
    cp -a ${PYCHDB}.dSYM ${PROJ_DIR}/${CHDB_PY_MODULE}.dSYM
    cp -a ${LIBCHDB}.dSYM ${PROJ_DIR}/${LIBCHDB_SO}.dSYM
fi

echo -e "\nSymbols:"
echo -e "\nPyInit in PYCHDB: ${PYCHDB}"
${NM} ${PYCHDB} | grep PyInit || true
echo -e "\nquery_stable in PYCHDB: ${PYCHDB}"
${NM} ${PYCHDB} | grep query_stable || true
if [ "${CHDB_LITE}" != "1" ]; then
    echo -e "\nPyInit in LIBCHDB: ${LIBCHDB}"
    ${NM} ${LIBCHDB} | grep PyInit || echo "PyInit not found in ${LIBCHDB}, it's OK"
    echo -e "\nquery_stable in LIBCHDB: ${LIBCHDB}"
    ${NM} ${LIBCHDB} | grep query_stable || true
fi

echo -e "\nAfter copy:"
cd ${PROJ_DIR} && pwd

ccache -s || true

if [ "${CHDB_FREE_THREADING}" == "1" ]; then
    echo "Free-threading build: skipping pybind11 nonlimitedapi shim libraries (not needed)"
else
    if ! CMAKE_ARGS="${CMAKE_ARGS}" CHDB_PYTHON_INCLUDE_DIR_PREFIX="${HOME}/python_include" bash ${DIR}/build_pybind11.sh --all --cross-compile --build-dir=${BUILD_DIR}; then
        echo "Error: Failed to build pybind11 libraries"
        exit 1
    fi

    if [ ${build_type} != "Debug" ]; then
        echo -e "\nStrip pybind11 stubs library:"
        STUBS_LIB=${CHDB_DIR}/libpybind11nonlimitedapi_stubs.dylib
        [ -f ${STUBS_LIB} ] && ${STRIP} -S -x ${STUBS_LIB}
    fi
fi

# Fix LC_RPATH in module for cross-compiled builds
echo -e "\nFixing LC_RPATH in ${CHDB_PY_MODULE}..."
INSTALL_NAME_TOOL="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-install_name_tool"
OTOOL="${CCTOOLS_BIN}/${DARWIN_TRIPLE}-otool"

echo -e "\nPre library dependencies:"
${OTOOL} -L ${CHDB_DIR}/${CHDB_PY_MODULE}

if [ "${CHDB_FREE_THREADING}" != "1" ]; then
    STUBS_LIB="libpybind11nonlimitedapi_stubs.dylib"
    OLD_STUBS_PATH=$(${OTOOL} -L ${CHDB_DIR}/${CHDB_PY_MODULE} | grep "${STUBS_LIB}" | awk '{print $1}')
    if [ -n "${OLD_STUBS_PATH}" ]; then
        echo "Changing ${STUBS_LIB} reference:"
        echo "  From: ${OLD_STUBS_PATH}"
        echo "  To:   @loader_path/${STUBS_LIB}"
        ${INSTALL_NAME_TOOL} -change "${OLD_STUBS_PATH}" "@loader_path/${STUBS_LIB}" ${CHDB_DIR}/${CHDB_PY_MODULE}
    else
        echo "${STUBS_LIB} not found in dependencies"
    fi
fi

echo -e "\nPost library dependencies:"
${OTOOL} -L ${CHDB_DIR}/${CHDB_PY_MODULE}

echo -e "\nCross-compilation for macOS ${TARGET_ARCH} completed successfully!"
echo -e "Generated files:"
[ "${CHDB_LITE}" != "1" ] && echo -e "  - ${PROJ_DIR}/${LIBCHDB_SO}"
echo -e "  - ${CHDB_DIR}/${CHDB_PY_MODULE}"
echo -e "\nFile sizes:"
[ "${CHDB_LITE}" != "1" ] && ls -lh ${PROJ_DIR}/${LIBCHDB_SO}
ls -lh ${CHDB_DIR}/${CHDB_PY_MODULE}
echo -e "\nBuild directory: ${BUILD_DIR}"
