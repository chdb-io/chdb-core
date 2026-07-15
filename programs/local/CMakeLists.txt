if (OS_WASM)
    # WebAssembly build: Apache Arrow is not compiled for wasm, so the Arrow
    # stream integration (register/scan/storage/table-function) is omitted. Only
    # the core chdb C API + embedded local server are built. chdb-internal.h still
    # references the standalone Arrow C Data Interface header (arrow/c/abi.h),
    # whose include dir is wired below.
    set (CLICKHOUSE_LOCAL_SOURCES
        chdb.cpp
        ChunkCollectorOutputFormat.cpp
        LocalServer.cpp
        EmbeddedServer.cpp
        ChdbClient.cpp
    )
else()
    set (CLICKHOUSE_LOCAL_SOURCES
        chdb.cpp
        ArrowSchema.cpp
        ArrowStreamWrapper.cpp
        ArrowTableReader.cpp
        ChunkCollectorOutputFormat.cpp
        LocalServer.cpp
        EmbeddedServer.cpp
        ChdbClient.cpp
    )

    if (NOT USE_PYTHON)
        set (CHDB_ARROW_SOURCES
            chdb-arrow.cpp
            chdb-arrow-output.cpp
            chdb-adbc.cpp
            ArrowStreamSource.cpp
            StorageArrowStream.cpp
            TableFunctionArrowStream.cpp
        )
        set (CLICKHOUSE_LOCAL_SOURCES ${CLICKHOUSE_LOCAL_SOURCES} ${CHDB_ARROW_SOURCES})
    endif()
endif()

# Add force function references only for static library builds
if (CHDB_STATIC_LIBRARY_BUILD)
    list(APPEND CLICKHOUSE_LOCAL_SOURCES ForceFunctionReferences.cpp)
    add_compile_definitions(CHDB_STATIC_LIBRARY_BUILD)
endif()

if (USE_PYTHON)
    add_compile_definitions(USE_PYTHON=1)

    set (CHDB_SOURCES
        chdb.cpp
        FieldToPython.cpp
        ListScan.cpp
        LocalChdb.cpp
        LocalServer.cpp
        EmbeddedServer.cpp
        ChdbClient.cpp
        NumpyArray.cpp
        NumpyNestedTypes.cpp
        NumpyType.cpp
        ObjectToPython.cpp
        PyDateTimeHelper.cpp
        PandasAnalyzer.cpp
        PandasDataFrame.cpp
        PandasDataFrameBuilder.cpp
        PandasScan.cpp
        AIQueryProcessor.cpp
        PyArrowStreamFactory.cpp
        PyArrowTable.cpp
        PybindWrapper.cpp
        PythonConversion.cpp
        PythonDict.cpp
        ChdbGlobalFunctions.cpp
        ChdbPyType.cpp
        PythonScalarUDF.cpp
        PythonUDFRegistry.cpp
        PythonReader.cpp
        PythonTableCache.cpp
        PythonImportCache.cpp
        PythonImporter.cpp
        PythonSource.cpp
        PythonUtils.cpp
        StoragePython.cpp
        TableFunctionPython.cpp
    )
    set (CLICKHOUSE_LOCAL_SOURCES ${CLICKHOUSE_LOCAL_SOURCES} ${CHDB_SOURCES})

    # Select pybind11 include path based on build mode
    if (CHDB_FREE_THREADING)
        # Free-threading builds use upstream pybind11 (no limited API / stubs needed)
        set(PYBIND11_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/pybind11-upstream/include")
    else()
        # Regular builds use the mrbind-pybind11 fork with stable ABI stubs
        set(PYBIND11_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/pybind11/include")
    endif()

    # include Python.h
    if(CHDB_CROSSCOMPILING)
        if (CHDB_FREE_THREADING)
            if (NOT DEFINED CHDB_FREE_THREADING_PYTHON_VERSION)
                message(FATAL_ERROR "CHDB_FREE_THREADING requires CHDB_FREE_THREADING_PYTHON_VERSION (e.g. 3.13t)")
            endif()
            set(PYTHON_INCLUDE_DIR "${CHDB_PYTHON_INCLUDE_DIR_PREFIX}/${CHDB_FREE_THREADING_PYTHON_VERSION}")
        else()
            set(PYTHON_INCLUDE_DIR "${CHDB_PYTHON_INCLUDE_DIR_PREFIX}/3.9")
        endif()
        message(STATUS "Cross-compiling: Using Python include directory: ${PYTHON_INCLUDE_DIR}")
    else()
        # get_path('include') is often wrong on macOS (e.g. /Library/Python/3.9/include with no headers).
        # Prefer paths that actually contain Python.h: INCLUDEPY, framework Headers, then fallbacks.
        execute_process(COMMAND python3 -c "import os,sys,sysconfig; p=next((x for x in (sysconfig.get_config_var('INCLUDEPY'),os.path.join(sys.prefix,'Headers'),sysconfig.get_config_var('CONFINCLUDEDIR'),sysconfig.get_path('include')) if x and os.path.isfile(os.path.join(x,'Python.h'))),None); import sys as S; (print(p),S.exit(0)) if p else S.exit(1)"
            OUTPUT_VARIABLE PYTHON_INCLUDE_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE
            RESULT_VARIABLE _py_inc_rc
        )
        if (NOT _py_inc_rc EQUAL 0 OR NOT PYTHON_INCLUDE_DIR)
            message(FATAL_ERROR "Could not find Python.h (python3 include discovery failed). Install Python dev headers or fix PYTHON_INCLUDE_DIR.")
        endif()
    endif()

    # remove all warnings, because pybind11 will generate a lot of warnings
    foreach(_file ${CHDB_SOURCES})
        set_source_files_properties(${_file}
            PROPERTIES COMPILE_FLAGS
            "-w"
        )
    endforeach(_file)
endif()

set (CLICKHOUSE_LOCAL_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
)

clickhouse_program_add(local)

if (OS_WASM)
    # chdb-internal.h includes the standalone Arrow C Data Interface header
    # (arrow/c/abi.h, dependency-free apart from <stdint.h>) for the
    # ArrowArrayStream type referenced by the C API surface. Arrow proper is not
    # built for wasm, so expose just this header from the arrow source tree.
    target_include_directories(clickhouse-local-lib PRIVATE
        "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
endif()

if (TARGET ch_contrib::parquet)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::parquet)
endif()

if (USE_PYTHON)
    target_include_directories(clickhouse-local-lib PRIVATE
        ${PYBIND11_INCLUDE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})
    target_include_directories(clickhouse-local-lib SYSTEM PRIVATE ${PYTHON_INCLUDE_DIR})
    # Python headers on Ubuntu/Debian use a multiarch layout: pyconfig.h includes
    # <x86_64-linux-gnu/python3.X/pyconfig.h>.  We symlink that into the sysroot so the
    # cross-compiler can find it without reaching into host /usr/include.
    # Suppress -Wpoison-system-directories for these targets since the Python headers
    # are intentionally pulled from the host (they are header-only, ABI-neutral).
    if (OS_LINUX)
        target_compile_options(clickhouse-local-lib PRIVATE -Wno-poison-system-directories)
    endif()
endif()

if (TARGET ch_rust::skim)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_rust::skim)
endif()
if (TARGET ch_contrib::azure_sdk)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::azure_sdk)
endif()
if (TARGET ch_contrib::utf8proc)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::utf8proc)
endif()
if (TARGET ch_contrib::arrow)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::arrow)
endif()
if (CHDB_FREE_THREADING)
    target_compile_definitions(clickhouse-local-lib PRIVATE Py_GIL_DISABLED=1)
    target_compile_definitions(clickhouse-local-lib PRIVATE CHDB_FREE_THREADING=1)
elseif (TARGET ch_contrib::pybind11_stubs)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::pybind11_stubs)
    target_compile_definitions(clickhouse-local-lib PRIVATE Py_LIMITED_API=0x03090000)
endif()

if (ENABLE_CLIENT_AI AND TARGET ch_contrib::ai-sdk-cpp)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::ai-sdk-cpp)
endif()

# Always use internal readpassphrase
target_link_libraries(clickhouse-local-lib PRIVATE readpassphrase)
