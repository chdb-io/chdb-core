if (OS_FREEBSD)
    # Right nix/libc requires fspacectl and it had been added only since FreeBSD14.
    # And since sysroot has older libraries you will got undefined reference for clickhouse binary.
    #
    # But likely everything should work without this syscall, however it is not
    # possible right now to gently override libraries versions for dependencies,
    # and forking rust modules is a little bit too much for this thing.
    #
    # You can take a look at the details in the following issue [1].
    #
    #   [1]: https://github.com/rust-lang/cargo/issues/5640
    #
    # Update 2024-04: Now prql also requires getrandom() via std::sys::pal::unix::rand::imp::getrandom_fill_bytes
    message(STATUS "Rust build is disabled for FreeBSD because we use old sysroot files")
    return()
endif()

option (ENABLE_PRQL "Enable prql" ${ENABLE_LIBRARIES})
option (ENABLE_SKIM "Enable skim" ${ENABLE_LIBRARIES})
option (ENABLE_POLYGLOT "Enable polyglot SQL transpiler" ${ENABLE_LIBRARIES})
# chdb: WasmTime backs the WebAssembly UDF runtime and is enabled independently
# of ENABLE_LIBRARIES (see chdb/build.sh). It is built from the same cargo
# workspace, so it must be considered here too — otherwise this early return
# would skip clickhouse_import_crate() and add_subdirectory(wasmtime) below.
option (ENABLE_WASMTIME "Enable WasmTime engine" ${ENABLE_LIBRARIES})

if (NOT ENABLE_PRQL AND NOT ENABLE_SKIM AND NOT ENABLE_POLYGLOT AND NOT ENABLE_WASMTIME)
    message(STATUS "prql, skim, polyglot, and wasmtime are disabled")
    return()
endif()

# Import only the crates that are actually enabled. Without an explicit CRATES
# allowlist, corrosion builds the entire workspace, which forces unwanted crates
# (e.g. skim) to compile even when disabled — and skim fails to build in chdb's
# minimized macOS toolchain. This keeps a wasmtime-only build from dragging them in.
set (_ch_rust_enabled_crates "")
if (ENABLE_PRQL)
    list (APPEND _ch_rust_enabled_crates _ch_rust_prql)
endif()
if (ENABLE_SKIM)
    list (APPEND _ch_rust_enabled_crates _ch_rust_skim_rust)
endif()
if (ENABLE_POLYGLOT)
    list (APPEND _ch_rust_enabled_crates _ch_rust_polyglot)
endif()
if (ENABLE_WASMTIME)
    list (APPEND _ch_rust_enabled_crates _ch_rust_wasmtime)
endif()

clickhouse_import_crate(MANIFEST_PATH Cargo.toml CRATES ${_ch_rust_enabled_crates})

if (ENABLE_PRQL)
    clickhouse_config_crate_flags(_ch_rust_prql)
    target_include_directories(_ch_rust_prql INTERFACE prql/include)
    add_library(ch_rust::prql ALIAS _ch_rust_prql)

    # Strip internal symbols from the PRQL static library after it is built.
    # Only the C FFI functions need to remain visible; all other symbols (67K+)
    # from the chumsky parser combinator are internal and produce enormous names.
    # Skip in tidy builds where CMAKE_AR is a dummy script that doesn't extract archives.
    # Skip in sanitizer builds where instrumentation adds special symbols (e.g.,
    # ___asan_globals_registered) that the partial link + strip process breaks.
    # Skip on Darwin where objects are Mach-O (ld.lld -r only handles ELF).
    # Skip on ppc64le where ld.lld -r produces broken .eh_frame relocations.
    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        # Use ld.lld for partial linking — it's a cross-linker that handles all ELF architectures,
        # unlike the host ld which may not support the target architecture.
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_prql_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_prql.a")
            add_custom_target(strip_prql_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_prql_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    prql_to_sql prql_free_pointer
                DEPENDS cargo-build__ch_rust_prql
                COMMENT "Stripping internal symbols from PRQL library"
                VERBATIM
            )
            add_dependencies(_ch_rust_prql strip_prql_symbols)
        endif ()
    endif ()
endif()

if (ENABLE_POLYGLOT)
    clickhouse_config_crate_flags(_ch_rust_polyglot)
    target_include_directories(_ch_rust_polyglot INTERFACE polyglot/include)
    add_library(ch_rust::polyglot ALIAS _ch_rust_polyglot)

    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_polyglot_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_polyglot.a")
            add_custom_target(strip_polyglot_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_polyglot_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    polyglot_transpile polyglot_free_pointer
                DEPENDS cargo-build__ch_rust_polyglot
                COMMENT "Stripping internal symbols from polyglot library"
                VERBATIM
            )
            add_dependencies(_ch_rust_polyglot strip_polyglot_symbols)
        endif ()
    endif ()
endif()

if (ENABLE_SKIM)
    clickhouse_config_crate_flags(_ch_rust_skim_rust)
    target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
    add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
endif()

add_subdirectory (wasmtime)
