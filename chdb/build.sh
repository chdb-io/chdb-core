#!/bin/bash

set -e

# default to RelWithDebInfo to preserve debug symbols for crash analysis
build_type=${1:-RelWithDebInfo}

# chdb-core-lite has a 50 MiB wheel budget; -g would bloat past it.
if [ "${CHDB_LITE}" = "1" ] && [ "${build_type}" = "RelWithDebInfo" ]; then
    build_type="Release"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Setup LLVM path BEFORE sourcing vars.sh so that llvm tools can be found
if [ "$(uname)" == "Darwin" ]; then
    if command -v brew >/dev/null 2>&1 && _llvm=$(brew --prefix llvm@21 2>/dev/null) && [ -x "${_llvm}/bin/clang++" ]; then
        export CXX="${_llvm}/bin/clang++"
        export CC="${_llvm}/bin/clang"
        export PATH="${_llvm}/bin:${PATH}"
    else
        export CC=/usr/bin/clang
        export CXX=/usr/bin/clang++
    fi
fi

. ${DIR}/vars.sh

BUILD_DIR=${PROJ_DIR}/buildlib

HDFS="-DENABLE_HDFS=1 -DENABLE_GSASL_LIBRARY=1 -DENABLE_KRB5=1"
MYSQL="-DENABLE_MYSQL=1"
RUST_FEATURES="-DENABLE_RUST=0"
# check current os type
if [ "$(uname)" == "Darwin" ]; then
    GLIBC_COMPATIBILITY="-DGLIBC_COMPATIBILITY=0"
    UNWIND="-DUSE_UNWIND=0"
    JEMALLOC="-DENABLE_JEMALLOC=0"
    HDFS="-DENABLE_HDFS=0 -DENABLE_GSASL_LIBRARY=0 -DENABLE_KRB5=0"
    ICU="-DENABLE_ICU=0"
    SED_INPLACE="sed -i ''"
    # Enable Rust solely to build the WebAssembly UDF runtime (wasmtime). Keep
    # delta-kernel-rs and other Rust crates off to minimize the build surface.
    # ENABLE_WASMTIME must be set explicitly because ENABLE_LIBRARIES=0 below
    # would otherwise leave it OFF. Requires a Rust toolchain (rustup) on PATH.
    RUST_FEATURES="-DENABLE_RUST=1 -DENABLE_WASMTIME=1"
    # if Darwin ARM64 (M1, M2), disable AVX
    if [ "$(uname -m)" == "arm64" ]; then
        CPU_FEATURES="-DENABLE_AVX=0 -DENABLE_AVX2=0"
        LLVM="-DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0"
    else
        LLVM="-DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0"
        # disable AVX on Darwin for macos11
        if [ "$(sw_vers -productVersion | cut -d. -f1)" -le 11 ]; then
            CPU_FEATURES="-DENABLE_AVX=0 -DENABLE_AVX2=0"
        else
            # for M1, M2 using x86_64 emulation, we need to disable AVX and AVX2
            CPU_FEATURES="-DENABLE_AVX=0 -DENABLE_AVX2=0"
            # # If target macos version is 12, we need to test if support AVX2,
            # # because some Mac Pro Late 2013 (MacPro6,1) support AVX but not AVX2
            # # just test it on the github action, hope you don't using Mac Pro Late 2013.
            # # https://everymac.com/mac-answers/macos-12-monterey-faq/macos-monterey-macos-12-compatbility-list-system-requirements.html
            # if [ "$(sysctl -n machdep.cpu.leaf7_features | grep AVX2)" != "" ]; then
            #     CPU_FEATURES="-DENABLE_AVX=1 -DENABLE_AVX2=1"
            # else
            #     CPU_FEATURES="-DENABLE_AVX=1 -DENABLE_AVX2=0"
            # fi
        fi
    fi
elif [ "$(uname)" == "Linux" ]; then
    GLIBC_COMPATIBILITY="-DGLIBC_COMPATIBILITY=1"
    UNWIND="-DUSE_UNWIND=1"
    JEMALLOC="-DENABLE_JEMALLOC=1"
    ICU="-DENABLE_ICU=1"
    SED_INPLACE="sed -i"
    # x86_64: AVX + embedded compiler. aarch64: embedded compiler too — the
    # regexp JIT (min_count_to_compile_regular_expression) and compiled
    # expressions need LLVM, and official ClickHouse aarch64 builds enable it;
    # keeping it off left ARM without the regexp JIT entirely.
    if [ "$(uname -m)" == "x86_64" ]; then
        CPU_FEATURES="-DENABLE_AVX=1 -DENABLE_AVX2=0"
        LLVM="-DENABLE_EMBEDDED_COMPILER=1 -DENABLE_DWARF_PARSER=1"
    else
        CPU_FEATURES="-DENABLE_AVX=0 -DENABLE_AVX2=0"
        if [ "$(uname -m)" == "aarch64" ]; then
            LLVM="-DENABLE_EMBEDDED_COMPILER=1 -DENABLE_DWARF_PARSER=0"
        else
            LLVM="-DENABLE_EMBEDDED_COMPILER=0 -DENABLE_DWARF_PARSER=0"
        fi
    fi
    # -DENABLE_WASMTIME=1 enables the WebAssembly UDF runtime; it must be set
    # explicitly because ENABLE_LIBRARIES=0 would otherwise leave it OFF.
    RUST_FEATURES="-DENABLE_RUST=1 -DENABLE_DELTA_KERNEL_RS=1 -DENABLE_WASMTIME=1"
    # The historical osslconf=OPENSSL_NO_DEPRECATED_3_0 RUSTFLAGS injection into
    # corrosion-cmake is gone: contrib libcrypto ships the deprecated symbols, and
    # v26.7's vendored openssl 0.10.80 fails to compile with that cfg set.
else
    echo "OS not supported"
    exit 1
fi

if [ ! -d $BUILD_DIR ]; then
    mkdir $BUILD_DIR
fi

cd ${BUILD_DIR}

if [ "${CHDB_LITE}" = "1" ]; then
    # chdb-core-lite: keep this argument list minimal. CMakeLists.txt's
    # CHDB_LITE block fills in all ENABLE_*=0 / trim flags / linker flags.
    # MinSizeRel is forced by the CHDB_LITE block too, but we override if user
    # passed a different build_type explicitly.
    CMAKE_ARGS="-DCMAKE_BUILD_TYPE=${build_type} -DENABLE_THINLTO=0 -DENABLE_TESTS=0 -DCHDB_LITE=ON \
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
        ${GLIBC_COMPATIBILITY} ${UNWIND} \
        ${CPU_FEATURES} \
        -DENABLE_AVX512=0 -DENABLE_AVX512_VBMI=0 \
        -DCHDB_VERSION=${CHDB_VERSION} \
        "
else
    # Explicit -DCHDB_LITE=OFF resets the cmake cache when buildlib/ was previously
    # configured for chdb-core-lite (CI runs lite before FT in the same buildlib/).
    CMAKE_ARGS="-DCMAKE_BUILD_TYPE=${build_type} -DCHDB_LITE=OFF -DENABLE_THINLTO=0 -DENABLE_XRAY=0 -DENABLE_TESTS=0 -DENABLE_CLICKHOUSE_SERVER=0 -DENABLE_CLICKHOUSE_CLIENT=0 \
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
    -DENABLE_BROTLI=1 -DENABLE_H3=1 \
    -DENABLE_CLICKHOUSE_ALL=0 -DUSE_STATIC_LIBRARIES=1 -DSPLIT_SHARED_LIBRARIES=0 \
    -DENABLE_SIMDJSON=1 -DENABLE_RAPIDJSON=1 \
    ${CPU_FEATURES} \
    -DENABLE_AVX512=0 -DENABLE_AVX512_VBMI=0 \
    -DENABLE_SIMDUTF=1 \
    -DENABLE_LIBFIU=1 \
    -DCHDB_VERSION=${CHDB_VERSION} \
    "
fi

# Only disable compiler cache when neither ccache nor sccache is available.
# On Linux CI the Docker image provides ccache; preserve it for fast incremental builds.
if ! command -v ccache >/dev/null 2>&1 && ! command -v sccache >/dev/null 2>&1; then
    CMAKE_ARGS="${CMAKE_ARGS} -DCOMPILER_CACHE=disabled"
fi

LIBCHDB_SO="libchdb.so"
BINARY=${BUILD_DIR}/programs/clickhouse
# chdb-core-lite only ships the Python module (_chdb.abi3.so), so skip the
# standalone libchdb.so build entirely. It saves ~half the link time.
if [ "${CHDB_LITE}" != "1" ]; then
    # Build libchdb.so
    cmake ${CMAKE_ARGS} -DENABLE_PYTHON=0 ${PROJ_DIR}
    ninja -d keeprsp
    echo -e "\nBINARY: ${BINARY}"
    ls -lh ${BINARY}
    echo -e "\nldd ${BINARY}"
    ${LDD} ${BINARY}
    rm -f ${BINARY}

    cd ${BUILD_DIR}
    ninja -d keeprsp -v > build.log || true
    USING_RESPONSE_FILE=$(grep -m 1 'clang.*-o programs/clickhouse .*' build.log | grep '@CMakeFiles/clickhouse.rsp' || true)

    if [ ! "${USING_RESPONSE_FILE}" == "" ]; then
        if [ -f CMakeFiles/clickhouse.rsp ]; then
            cp -a CMakeFiles/clickhouse.rsp CMakeFiles/libchdb.rsp
        else
            echo "CMakeFiles/clickhouse.rsp not found"
            exit 1
        fi
    fi

    LIBCHDB_CMD=$(grep -m 1 'clang.*-o programs/clickhouse .*' build.log \
        | sed "s/-o programs\/clickhouse/-fPIC -shared -o ${LIBCHDB_SO}/" \
        | sed 's/^[^&]*&& //' | sed 's/&&.*//' \
        | sed 's/ -Wl,-undefined,error/ -Wl,-undefined,dynamic_lookup/g' \
        | sed 's/ -Xlinker --no-undefined//g' \
        | sed 's/@CMakeFiles\/clickhouse.rsp/@CMakeFiles\/libchdb.rsp/g' \
         )

    #   generate the command to generate libchdb.so
    LIBCHDB_CMD=$(echo ${LIBCHDB_CMD} | sed 's/ '${CHDB_PY_MODULE}'/ '${LIBCHDB_SO}'/g')

    if [ ! "${USING_RESPONSE_FILE}" == "" ]; then
        ${SED_INPLACE} 's/ '${CHDB_PY_MODULE}'/ '${LIBCHDB_SO}'/g' CMakeFiles/libchdb.rsp
    fi

    # Control exported symbols for libchdb.so
    if [ "$(uname)" == "Darwin" ]; then
        # macOS: use exported_symbols_list file
        LIBCHDB_CMD="${LIBCHDB_CMD} -Wl,-exported_symbols_list,${CHDB_DIR}/libchdb_export_macos.txt"
        # Give the dylib a relocatable install name (macOS best practice for
        # tarball-distributed libraries; CMake's default since CMP0042). Without
        # it the id is the bare "libchdb.so", which linked executables record
        # verbatim and dyld then cannot resolve from e.g. /usr/local/lib.
        # Consumers link with: -lchdb -L<dir> -Wl,-rpath,<dir>
        LIBCHDB_CMD="${LIBCHDB_CMD} -Wl,-install_name,@rpath/libchdb.so"
    else
        # Linux: use version script
        LIBCHDB_CMD="${LIBCHDB_CMD} -Wl,--version-script=${CHDB_DIR}/libchdb_export.map"
    fi

    LIBCHDB_CMD=$(echo ${LIBCHDB_CMD} | sed 's/@CMakeFiles\/clickhouse.rsp/@CMakeFiles\/libchdb.rsp/g')

    # Step 4:
    #   save the command to a file for debug
    echo ${LIBCHDB_CMD} > libchdb_cmd.sh

    # Step 5:
    ${LIBCHDB_CMD}

    LIBCHDB_DIR=${BUILD_DIR}/
    LIBCHDB=${LIBCHDB_DIR}/${LIBCHDB_SO}
    ls -lh ${LIBCHDB}
fi

# build chdb python module
if [ "${CHDB_FREE_THREADING}" == "1" ]; then
    # Resolve the Python interpreter once: prefer python3, fall back to python.
    # Some Linux distros (Debian/Ubuntu without python-is-python3) ship only `python3`.
    PYTHON_BIN=$(command -v python3 2>/dev/null || command -v python 2>/dev/null || true)
    if [ -z "${PYTHON_BIN}" ]; then
        echo "Error: CHDB_FREE_THREADING=1 but neither 'python3' nor 'python' is on PATH"
        exit 1
    fi
    py_version=$("${PYTHON_BIN}" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    is_ft=$("${PYTHON_BIN}" -c "import sysconfig; print(sysconfig.get_config_var('Py_GIL_DISABLED') or 0)")
    if [ "$is_ft" != "1" ]; then
        echo "Error: CHDB_FREE_THREADING=1 but current Python (${PYTHON_BIN}) is not a free-threading build"
        exit 1
    fi
    echo "Using free-threading Python ${py_version} at ${PYTHON_BIN}"
    FREE_THREADING_CMAKE="-DCHDB_FREE_THREADING=1"
else
    py_version="3.9"
    current_py_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "unknown")
    if [ "$current_py_version" != "$py_version" ]; then
        echo "Error: Current Python version is $current_py_version, but required version is $py_version"
        echo "Please switch to Python $py_version using: pyenv shell $py_version"
        exit 1
    fi
    echo "Using Python version: $current_py_version"
    FREE_THREADING_CMAKE=""
fi
if [ "${CHDB_FREE_THREADING}" == "1" ]; then
    cmake ${CMAKE_ARGS} ${FREE_THREADING_CMAKE} -DENABLE_PYTHON=1 ${PROJ_DIR}
else
    # Drop Python paths a previous pybind11-shim configure may have cached in this
    # build dir (the shim loop reuses it); stale entries break the exact-3.9 check.
    cmake -U "*Python*" -U "*PYTHON*" . > /dev/null 2>&1 || true
    cmake ${CMAKE_ARGS} -DENABLE_PYTHON=1 -DPYBIND11_NONLIMITEDAPI_PYTHON_HEADERS_VERSION=${py_version} ${PROJ_DIR}
fi
ninja -d keeprsp || true

# del the binary and run ninja -v again to capture the command, then modify it to generate CHDB_PY_MODULE
/bin/rm -f ${BINARY}
cd ${BUILD_DIR}
ninja -d keeprsp -v > build.log || true

USING_RESPONSE_FILE=$(grep -m 1 'clang.*-o programs/clickhouse .*' build.log | grep '@CMakeFiles/clickhouse.rsp' || true)

if [ ! "${USING_RESPONSE_FILE}" == "" ]; then
    if [ -f CMakeFiles/clickhouse.rsp ]; then
        cp -a CMakeFiles/clickhouse.rsp CMakeFiles/pychdb.rsp
    else
        echo "CMakeFiles/clickhouse.rsp not found"
        exit 1
    fi
fi

# extract the command to generate CHDB_PY_MODULE
PYCHDB_CMD=$(grep -m 1 'clang.*-o programs/clickhouse .*' build.log \
    | sed "s/-o programs\/clickhouse/-fPIC -Wl,-undefined,dynamic_lookup -shared -o ${CHDB_PY_MODULE}/" \
    | sed 's/^[^&]*&& //' | sed 's/&&.*//' \
    | sed 's/ -Wl,-undefined,error/ -Wl,-undefined,dynamic_lookup/g' \
    | sed 's/ -Xlinker --no-undefined//g' \
    | sed 's/@CMakeFiles\/clickhouse.rsp/@CMakeFiles\/pychdb.rsp/g' \
     )

if [ "$(uname)" == "Linux" ]; then
    # malloc.cpp now uses direct symbol interposition (defines malloc/free/etc. calling je_* directly).
    # The old -wrap linker approach is no longer needed and caused __wrap_malloc to be undefined at runtime.
    # No modifications needed for Linux jemalloc integration.
    true
fi

if [ "$(uname)" == "Darwin" ]; then
    PYCHDB_CMD=$(echo ${PYCHDB_CMD} | sed 's|-Wl,-rpath,/[^[:space:]]*/pybind11-cmake|-Wl,-rpath,@loader_path|g')
    PYCHDB_CMD="${PYCHDB_CMD} -Wl,-exported_symbols_list,${CHDB_DIR}/pychdb_export_macos.txt"
else
    PYCHDB_CMD=$(echo ${PYCHDB_CMD} | sed 's|-Wl,-rpath,/[^[:space:]]*/pybind11-cmake|-Wl,-rpath,\$ORIGIN|g')
    PYCHDB_CMD="${PYCHDB_CMD} -Wl,--undefined=PyInit__chdb -Wl,--version-script=${CHDB_DIR}/pychdb_export.map"
fi

# save the command to a file for debug
echo ${PYCHDB_CMD} > pychdb_cmd.sh

${PYCHDB_CMD}

ls -lh ${CHDB_PY_MODULE}

## check all the so files
LIBCHDB_DIR=${BUILD_DIR}/

PYCHDB=${LIBCHDB_DIR}/${CHDB_PY_MODULE}
LIBCHDB=${LIBCHDB_DIR}/${LIBCHDB_SO}

if [ ${build_type} == "Debug" ]; then
    echo -e "\nDebug build, skip strip and debug symbol extraction"
elif [ ${build_type} == "RelWithDebInfo" ] && [ "${CHDB_LITE}" != "1" ]; then
    echo -e "\nExtracting debug symbols before strip..."
    if [ "$(uname)" == "Darwin" ]; then
        dsymutil ${PYCHDB} -o ${PYCHDB}.dSYM
        dsymutil ${LIBCHDB} -o ${LIBCHDB}.dSYM
        echo "Debug symbols extracted:"
        du -sh ${PYCHDB}.dSYM ${LIBCHDB}.dSYM
    else
        OBJCOPY=$(which llvm-objcopy-19 2>/dev/null || which llvm-objcopy 2>/dev/null || which objcopy 2>/dev/null)
        if [ -n "${OBJCOPY}" ]; then
            ${OBJCOPY} --only-keep-debug ${PYCHDB} ${PYCHDB}.debug
            ${OBJCOPY} --only-keep-debug ${LIBCHDB} ${LIBCHDB}.debug
            echo "Debug symbols extracted:"
            ls -lh ${PYCHDB}.debug ${LIBCHDB}.debug
        else
            echo "ERROR: objcopy not found, cannot extract debug symbols"
            exit 1
        fi
    fi

    echo -e "\nStrip the binary:"
    if [ "$(uname)" == "Darwin" ]; then
        ${STRIP} -S -x ${PYCHDB}
        ${STRIP} -S -x ${LIBCHDB}
    else
        ${STRIP} --strip-unneeded --remove-section=.comment --remove-section=.note ${PYCHDB}
        ${STRIP} --strip-unneeded --remove-section=.comment --remove-section=.note ${LIBCHDB}
        ${OBJCOPY} --add-gnu-debuglink=${PYCHDB}.debug ${PYCHDB}
        ${OBJCOPY} --add-gnu-debuglink=${LIBCHDB}.debug ${LIBCHDB}
    fi
else
    echo -e "\n${build_type} build, strip without debug symbol extraction"
    if [ "$(uname)" == "Darwin" ]; then
        ${STRIP} -S -x ${PYCHDB}
        [ "${CHDB_LITE}" != "1" ] && ${STRIP} -S -x ${LIBCHDB}
    else
        ${STRIP} --strip-unneeded --remove-section=.comment --remove-section=.note ${PYCHDB}
        [ "${CHDB_LITE}" != "1" ] && ${STRIP} --strip-unneeded --remove-section=.comment --remove-section=.note ${LIBCHDB}
    fi
fi

echo -e "\nPYCHDB: ${PYCHDB}"
ls -lh ${PYCHDB}
echo -e "\nldd ${PYCHDB}"
${LDD} ${PYCHDB}
echo -e "\nfile info of ${PYCHDB}"
file ${PYCHDB}

if [ "${CHDB_LITE}" != "1" ]; then
    echo -e "\nLIBCHDB: ${LIBCHDB}"
    ls -lh ${LIBCHDB}
    echo -e "\nldd ${LIBCHDB}"
    ${LDD} ${LIBCHDB}
    echo -e "\nfile info of ${LIBCHDB}"
    file ${LIBCHDB}
fi

rm -f ${CHDB_DIR}/*.so
cp -a ${PYCHDB} ${CHDB_DIR}/${CHDB_PY_MODULE}
[ "${CHDB_LITE}" != "1" ] && cp -a ${LIBCHDB} ${PROJ_DIR}/${LIBCHDB_SO}

if [ ${build_type} == "RelWithDebInfo" ] && [ "${CHDB_LITE}" != "1" ]; then
    if [ "$(uname)" == "Darwin" ]; then
        cp -a ${PYCHDB}.dSYM ${PROJ_DIR}/${CHDB_PY_MODULE}.dSYM
        cp -a ${LIBCHDB}.dSYM ${PROJ_DIR}/${LIBCHDB_SO}.dSYM
    else
        cp -a ${PYCHDB}.debug ${PROJ_DIR}/${CHDB_PY_MODULE}.debug
        cp -a ${LIBCHDB}.debug ${PROJ_DIR}/${LIBCHDB_SO}.debug
    fi
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
    # Auto-detect Python for pybind11 when pyenv is absent and env vars are not already set
    if ! command -v pyenv >/dev/null 2>&1 && [ -z "${CHDB_PYBIND11_NATIVE_PYTHON_EXECUTABLE:-}" ]; then
        _py=$(command -v python3 2>/dev/null || command -v python 2>/dev/null || true)
        if [ -n "${_py}" ]; then
            _ver=$("${_py}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || true)
            if [ -n "${_ver}" ]; then
                export CHDB_PYBIND11_NATIVE_PYTHON_EXECUTABLE="${_py}"
                export CHDB_PYBIND11_PYTHON_VERSIONS="${_ver}"
                echo "Auto-detected Python ${_ver} at ${_py} for pybind11 build"
            fi
        fi
    fi

    CMAKE_ARGS="${CMAKE_ARGS}" bash ${DIR}/build_pybind11.sh --all

    if [ ${build_type} != "Debug" ]; then
        echo -e "\nStrip pybind11 stubs library:"
        if [ "$(uname)" == "Darwin" ]; then
            STUBS_LIB=${CHDB_DIR}/libpybind11nonlimitedapi_stubs.dylib
            [ -f ${STUBS_LIB} ] && ${STRIP} -S -x ${STUBS_LIB}
        else
            STUBS_LIB=${CHDB_DIR}/libpybind11nonlimitedapi_stubs.so
            [ -f ${STUBS_LIB} ] && ${STRIP} --strip-unneeded --remove-section=.comment --remove-section=.note ${STUBS_LIB}
        fi
    fi
fi
