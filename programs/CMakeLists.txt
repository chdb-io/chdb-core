add_compile_options("$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:${COVERAGE_FLAGS}>")

include(${ClickHouse_SOURCE_DIR}/cmake/split_debug_symbols.cmake)

configure_file (config_tools.h.in ${CONFIG_INCLUDE_PATH}/config_tools.h)

macro(clickhouse_target_link_split_lib target name)
    target_link_libraries(${target} PRIVATE clickhouse-${name}-lib)
endmacro()

macro(clickhouse_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(CLICKHOUSE_${name_uc}_SOURCES ${CLICKHOUSE_${name_uc}_SOURCES} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_LINK ${CLICKHOUSE_${name_uc}_LINK} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_INCLUDE ${CLICKHOUSE_${name_uc}_INCLUDE} PARENT_SCOPE)

    add_library(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_SOURCES})

    set(_link ${CLICKHOUSE_${name_uc}_LINK}) # can't use ${} in if()
    if(_link)
        target_link_libraries(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_LINK})
    endif()

    target_include_directories (clickhouse-${name}-lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    set(_include ${CLICKHOUSE_${name_uc}_INCLUDE}) # can't use ${} in if()
    if (_include)
        target_include_directories(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_INCLUDE})
    endif()
endmacro()

macro(clickhouse_program_add name)
    clickhouse_program_add_library(${name})
endmacro()

# chdb_spec
add_subdirectory (local)
add_subdirectory (server)
# chdb_spec

if (OS_WASM)
    add_subdirectory (wasm)
endif ()

clickhouse_add_executable (clickhouse main.cpp)

# A library that prevent usage of several functions from libc.
if (ARCH_AMD64 AND OS_LINUX AND NOT OS_ANDROID)
    set (HARMFUL_LIB harmful)
endif ()

target_link_libraries (clickhouse PRIVATE clickhouse_common_io ${HARMFUL_LIB})
target_include_directories (clickhouse PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Link system iconv library on macOS for libxml2 compatibility
if (APPLE)
    target_link_libraries (clickhouse PRIVATE iconv bz2)
endif()

macro(clickhouse_program_alias alias)
    if (NOT OS_WINDOWS)
        message(STATUS "Adding alias ${alias}")
        add_custom_command (TARGET clickhouse POST_BUILD COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse ${alias})
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/${alias}" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif ()
endmacro()

function(clickhouse_program_install name lib_name)
    clickhouse_target_link_split_lib(clickhouse ${lib_name})
    # On Windows the symlink aliases are skipped: a symlink to bare `clickhouse` does not resolve
    # (the binary is `clickhouse.exe`), and an extensionless file is not executable there anyway.
    # The Windows entry points are `clickhouse.exe <mode>`, or a copy of the binary named
    # `clickhouse-<mode>.exe` - `main.cpp` reads the mode from `argv[0]` with the directory and
    # the `.exe` suffix stripped.
    if (NOT OS_WINDOWS)
        add_custom_command (TARGET clickhouse POST_BUILD COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse ${name})
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/${name}" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif ()

    foreach(alias ${ARGN})
        clickhouse_program_alias(${alias})
    endforeach()
endfunction()

# chdb_spec
clickhouse_program_install(clickhouse-local local chl ch)
if (OS_WINDOWS)
    # The server does not run on Windows (its startup goes through `BaseDaemon`, which needs POSIX
    # signals and `fork`), so only link `clickhouse-server-lib` (`clickhouse-local` needs it) without
    # installing the alias.
    clickhouse_target_link_split_lib(clickhouse server)
else ()
    clickhouse_program_install(clickhouse-server server)
endif ()
# chdb_spec

# chdb_spec: the Python module on Windows. On Linux and macOS chdb/build.sh links _chdb.abi3.so
# from the `clickhouse` executable's own link command; a PE image cannot be produced that way (an
# EXE and a DLL differ in their startup code and in how imports are bound), so here it is a CMake
# module with the same contents as `clickhouse.exe` minus `main.cpp`. clickhouse_new_delete comes
# first so that its operator new/delete win over the libc++ defaults. The .def file is the export
# list; it mirrors chdb/pychdb_export.map.
if (OS_WINDOWS AND USE_PYTHON)
    add_library (chdb_pymodule MODULE
        $<TARGET_OBJECTS:clickhouse_malloc>
        ${ClickHouse_SOURCE_DIR}/chdb/pychdb_export.def)
    target_link_libraries (chdb_pymodule PRIVATE
        clickhouse_new_delete
        clickhouse-local-lib
        clickhouse-server-lib
        clickhouse_common_io)
    set_target_properties (chdb_pymodule PROPERTIES
        PREFIX ""
        OUTPUT_NAME "_chdb"
        SUFFIX ".pyd"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
endif ()
