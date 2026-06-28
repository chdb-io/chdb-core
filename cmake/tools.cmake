# Compiler

if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message (FATAL_ERROR "Compiler ${CMAKE_CXX_COMPILER_ID} is not supported. Please switch to Clang")
endif ()

# Print details to output
if (OS_WASM)
    # emcc rejects an empty --target=/--sysroot; it already knows its target.
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version
        OUTPUT_VARIABLE COMPILER_SELF_IDENTIFICATION
        COMMAND_ERROR_IS_FATAL ANY
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else ()
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version --target=${CMAKE_CXX_COMPILER_TARGET} --sysroot=${CMAKE_SYSROOT}
        OUTPUT_VARIABLE COMPILER_SELF_IDENTIFICATION
        COMMAND_ERROR_IS_FATAL ANY
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif ()
message (STATUS "Using compiler:\n${COMPILER_SELF_IDENTIFICATION}")

# Require minimum compiler versions
set (CLANG_MINIMUM_VERSION 21)
if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
    message (FATAL_ERROR "Compilation with Clang version ${CMAKE_CXX_COMPILER_VERSION} is unsupported, the minimum required version is ${CLANG_MINIMUM_VERSION}.")
endif ()

string (REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
list (GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

# Linker
option (LINKER_NAME "Linker name or full path")

if (LINKER_NAME MATCHES "gold")
    message (FATAL_ERROR "Linking with gold is unsupported. Please use lld.")
endif ()

macro(ch_find_program var)
    if (USING_DUMMY_LAUNCHERS)
        set(${var} "${CMAKE_SOURCE_DIR}/cmake/dummy_compiler_linker.sh")
    else()
        unset(${var})
        find_program(${var} ${ARGN})
    endif()
endmacro()

if (NOT LINKER_NAME)
    if (OS_LINUX AND NOT ARCH_S390X)
        ch_find_program (LLD_PATH NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    elseif (OS_DARWIN)
        ch_find_program (LLD_PATH NAMES "ld64.lld-${COMPILER_VERSION_MAJOR}" "ld64.lld" "ld")
        # Duplicate libraries passed to the linker is not a problem.
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-no_warn_duplicate_libraries")
    endif ()
    if (LLD_PATH)
        if (OS_LINUX OR OS_DARWIN)
            # Clang driver simply allows full linker path.
            set (LINKER_NAME ${LLD_PATH})
        endif ()
    endif()
endif()

if (LINKER_NAME)
    ch_find_program (LLD_PATH NAMES ${LINKER_NAME})
    if (NOT LLD_PATH)
        message (FATAL_ERROR "Using linker ${LINKER_NAME} but can't find its path.")
    endif ()
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --ld-path=${LLD_PATH}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --ld-path=${LLD_PATH}")
    set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --ld-path=${LLD_PATH}")
endif ()

if (LINKER_NAME)
    message(STATUS "Using linker: ${LINKER_NAME}")
elseif (OS_WASM)
    # Emscripten drives wasm-ld internally via emcc; do not force a linker.
    message(STATUS "Using linker: <emscripten wasm-ld>")
elseif (NOT ARCH_S390X AND NOT OS_FREEBSD AND NOT OS_SUNOS)
    message (FATAL_ERROR "The only supported linker is LLVM's LLD, but we cannot find it.")
else ()
    message(STATUS "Using linker: <default>")
endif ()

# Archiver
ch_find_program (LLVM_AR_PATH NAMES "llvm-ar-${COMPILER_VERSION_MAJOR}" "llvm-ar")
if (LLVM_AR_PATH)
    set (CMAKE_AR "${LLVM_AR_PATH}")
endif ()
message(STATUS "Using archiver: ${CMAKE_AR}")

# Ranlib
ch_find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib-${COMPILER_VERSION_MAJOR}" "llvm-ranlib")
if (LLVM_RANLIB_PATH)
    set (CMAKE_RANLIB "${LLVM_RANLIB_PATH}")
endif ()
message(STATUS "Using ranlib: ${CMAKE_RANLIB}")

# Install Name Tool
ch_find_program (LLVM_INSTALL_NAME_TOOL_PATH NAMES "llvm-install-name-tool-${COMPILER_VERSION_MAJOR}" "llvm-install-name-tool")
if (LLVM_INSTALL_NAME_TOOL_PATH)
    set (CMAKE_INSTALL_NAME_TOOL "${LLVM_INSTALL_NAME_TOOL_PATH}")
endif ()
message(STATUS "Using install-name-tool: ${CMAKE_INSTALL_NAME_TOOL}")

# Objcopy
ch_find_program (OBJCOPY_PATH NAMES "llvm-objcopy-${COMPILER_VERSION_MAJOR}" "llvm-objcopy" "objcopy")
if (OBJCOPY_PATH)
    message (STATUS "Using objcopy: ${OBJCOPY_PATH}")
else ()
    # objcopy is only needed for split_debug_symbols (keeper), which is disabled in chdb builds.
    # On macOS without Homebrew LLVM, llvm-objcopy is not shipped by Xcode CLT — downgrade to warning.
    message (WARNING "Cannot find objcopy. Debug symbol splitting will be unavailable.")
endif ()

# nm (used by helper scripts that inspect symbol tables, e.g. cmake/localize_rust_c_symbols.sh)
ch_find_program (NM_PATH NAMES "llvm-nm-${COMPILER_VERSION_MAJOR}" "llvm-nm" "nm")
if (NM_PATH)
    message (STATUS "Using nm: ${NM_PATH}")
else ()
    message (FATAL_ERROR "Cannot find nm.")
endif ()

# Strip
ch_find_program (STRIP_PATH NAMES "llvm-strip-${COMPILER_VERSION_MAJOR}" "llvm-strip" "strip")
if (STRIP_PATH)
    message (STATUS "Using strip: ${STRIP_PATH}")
else ()
    message (FATAL_ERROR "Cannot find strip.")
endif ()

if (OS_DARWIN AND NOT CMAKE_TOOLCHAIN_FILE)
    # utils/list-licenses/list-licenses.sh (which generates system table system.licenses) needs the GNU versions of find and grep. These are
    # not available out-of-the-box on Mac. As a special case, Darwin builds in CI are cross-compiled from x86 Linux where the GNU userland is
    # available.
    find_program(GFIND_PATH NAMES "gfind")
    if (NOT GFIND_PATH)
        # gfind/ggrep are only needed for the license-listing utility, not for compilation.
        # Downgrade to warning so native macOS builds without Homebrew GNU userland can proceed.
        message (WARNING "GNU find not found. License listing (utils/list-licenses) will be unavailable. Install with 'brew install findutils'.")
    endif()
    find_program(GGREP_PATH NAMES "ggrep")
    if (NOT GGREP_PATH)
        message (WARNING "GNU grep not found. License listing (utils/list-licenses) will be unavailable. Install with 'brew install grep'.")
    endif()
endif ()
