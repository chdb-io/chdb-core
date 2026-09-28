if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "Android")
    # This is a toy configuration and not in CI, so expect it to be broken.
    # Use cmake flags such as: -DCMAKE_TOOLCHAIN_FILE=~/ch2/android-ndk-r21d/build/cmake/android.toolchain.cmake -DANDROID_ABI=arm64-v8a -DANDROID_PLATFORM=28
    set (OS_ANDROID 1)
    add_definitions(-D OS_ANDROID)
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    set (OS_FREEBSD 1)
    add_definitions(-D OS_FREEBSD)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_DARWIN 1)
    add_definitions(-D OS_DARWIN)
    # For MAP_ANON/MAP_ANONYMOUS
    add_definitions(-D _DARWIN_C_SOURCE)
elseif (CMAKE_SYSTEM_NAME MATCHES "SunOS")
    set (OS_SUNOS 1)
    add_definitions(-D OS_SUNOS)
elseif (CMAKE_SYSTEM_NAME MATCHES "Emscripten")
    # WebAssembly, through the Emscripten toolchain. Configure with `emcmake cmake ...`, which
    # sets CMAKE_SYSTEM_NAME and points CMAKE_TOOLCHAIN_FILE at Emscripten's own toolchain file.
    set (OS_WASM 1)
    add_definitions(-D OS_WASM)
    # Note: unlike the other platforms, no `_GNU_SOURCE`. Emscripten's musl-derived libc declares
    # everything this tree needs without it (`MAP_ANONYMOUS`, for one), and defining it makes
    # OpenSSL select the GNU `strerror_r`, which returns `char *` and which musl does not have.

    # chdb ships WASM as a product, so the three ABI choices below are options rather than
    # fixed settings. They are consumed by the OS_WASM block further down and by programs/wasm.
    #
    # WASM_MEMORY64: 64-bit Memory64 ABI. OFF attempts the (much harder) wasm32 port.
    option (WASM_MEMORY64 "Build the WASM target for the 64-bit Memory64 ABI" ON)
    # WASM_THREADS: real pthreads (Web Workers + SharedArrayBuffer), which needs the page to be
    #   cross-origin isolated. OFF builds single-threaded so it runs on pages that are not:
    #   thread creation fails, the global pool runs jobs inline and the optional background
    #   pools are not started (gated on CHDB_WASM_SINGLE_THREADED).
    option (WASM_THREADS "Build the WASM target with pthreads (requires cross-origin isolation)" ON)
    # WASM_JSPI: JavaScript Promise Integration for the HTTP bridge - the wasm stack suspends on
    #   an async fetch() instead of requiring synchronous XHR. The only transport that works on
    #   Cloudflare Workers; needs a JSPI-enabled engine (Chrome 137+, workerd, Node with
    #   --experimental-wasm-jspi). Adds -sJSPI at link (programs/wasm).
    option (WASM_JSPI "Use JSPI (async fetch) for the WASM HTTP bridge" OFF)
else ()
    message (FATAL_ERROR "Platform ${CMAKE_SYSTEM_NAME} is not supported")
endif ()

if (OS_WASM)
    # ClickHouse assumes a 64-bit `size_t` and 64-bit pointers pervasively - `1e12uz` literals in
    # `Core/Defines.h`, sizeof-equality static_asserts in ProfileEvents, and so on - so build for
    # the 64-bit Memory64 ABI rather than wasm32. Needs a recent engine (Node >= 23, Chrome >= 133).
    if (WASM_MEMORY64)
        add_compile_options (-sMEMORY64=1)
        add_link_options (-sMEMORY64=1)
    endif ()

    # ClickHouse catches exceptions everywhere. Emscripten only emits throws by default and turns
    # every `catch` into a no-op, so enable the native WebAssembly exception-handling proposal.
    # It is an ABI flag: it has to be on for every translation unit and at the link.
    add_compile_options (-fwasm-exceptions)
    add_link_options (-fwasm-exceptions)

    # Emscripten implements pthreads on Web Workers plus SharedArrayBuffer, which needs the page
    # to be cross-origin isolated. Also an ABI flag, so compile and link both.
    if (WASM_THREADS)
        add_compile_options (-pthread)
        add_link_options (-pthread)
    else ()
        add_definitions (-D CHDB_WASM_SINGLE_THREADED)
    endif ()

    # Nothing here can work in a WebAssembly sandbox: there are no raw sockets, no subprocesses,
    # no `dlopen`, no JIT and no architecture-specific code paths.
    set (ENABLE_JEMALLOC OFF CACHE INTERNAL "")
    set (ENABLE_TCMALLOC OFF CACHE INTERNAL "")
    set (ENABLE_GRPC OFF CACHE INTERNAL "")
    # Protobuf needs a `protoc` that runs on the host, and the nested native configure at the
    # bottom of the top-level `CMakeLists.txt` would be handed `emcc` as its host compiler.
    # ORC hard-depends on it and goes with it; chdb keeps Parquet, see the format block below.
    set (ENABLE_PROTOBUF OFF CACHE INTERNAL "")
    # Its `kj` library uses `fallocate` and friends unconditionally in its POSIX branch,
    # which the Emscripten libc does not provide.
    set (ENABLE_CAPNP OFF CACHE INTERNAL "")
    set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
    set (ENABLE_HDFS OFF CACHE INTERNAL "")
    set (ENABLE_MYSQL OFF CACHE INTERNAL "")
    set (ENABLE_LIBPQXX OFF CACHE INTERNAL "")
    set (ENABLE_NURAFT OFF CACHE INTERNAL "")
    set (ENABLE_KAFKA OFF CACHE INTERNAL "")
    set (ENABLE_AMQPCPP OFF CACHE INTERNAL "")
    set (ENABLE_NATS OFF CACHE INTERNAL "")
    set (ENABLE_CASSANDRA OFF CACHE INTERNAL "")
    # Raw sockets like the rest, and its `mlib` has an explicit #error for platforms
    # it does not recognize (`mlib/time_point.h`: "We do not know how to get the
    # current time on this platform").
    set (USE_MONGODB OFF CACHE INTERNAL "")
    set (ENABLE_AZURE_BLOB_STORAGE OFF CACHE INTERNAL "")
    set (ENABLE_AWS_S3 OFF CACHE INTERNAL "")
    set (ENABLE_S3 OFF CACHE INTERNAL "")
    set (ENABLE_HIVE OFF CACHE INTERNAL "")
    set (ENABLE_ODBC OFF CACHE INTERNAL "")
    set (ENABLE_LDAP OFF CACHE INTERNAL "")
    set (ENABLE_KRB5 OFF CACHE INTERNAL "")
    set (ENABLE_GSASL_LIBRARY OFF CACHE INTERNAL "")
    set (ENABLE_CURL OFF CACHE INTERNAL "")
    # `libssh` needs raw sockets, and its config headers are pregenerated per platform.
    set (ENABLE_SSH OFF CACHE INTERNAL "")
    # This also turns off `wasmtime`, the engine behind WebAssembly UDFs: a host WebAssembly
    # runtime inside a WebAssembly sandbox would need to run guest modules from native code,
    # which this target cannot provide.
    set (ENABLE_RUST OFF CACHE INTERNAL "")
    set (ENABLE_DELTA_KERNEL_RS OFF CACHE INTERNAL "")
    set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
    set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
    set (ENABLE_ROCKSDB OFF CACHE INTERNAL "")
    set (ENABLE_VECTORSCAN OFF CACHE INTERNAL "")
    # BLAKE3 pulls in (a subset of) llvm-project; not worth it on WASM. Disabling
    # it keeps the whole llvm-project tree out of the configure, like the LoongArch port.
    set (ENABLE_BLAKE3 OFF CACHE INTERNAL "")
    # Emscripten's sysroot provides math; don't build llvm-libc math.
    set (ENABLE_LLVM_LIBC_MATH OFF CACHE INTERNAL "")
    set (OPENSSL_NO_ASM ON CACHE INTERNAL "")
    set (GLIBC_COMPATIBILITY OFF CACHE INTERNAL "")
    set (ENABLE_LIBFIU OFF CACHE INTERNAL "")
    # No libunwind on WASM; rely on the host engine for stack traces.
    set (USE_UNWIND OFF CACHE INTERNAL "")

    # The bundle is a download, so default to size (-Os) unless -DCMAKE_BUILD_TYPE=Release
    # has been passed in.
    if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
        set (CMAKE_BUILD_TYPE MinSizeRel CACHE STRING "WASM optimizes for download size" FORCE)
    endif ()

    # Fast JSON parser. Not required to build (RapidJSON below is the fallback that
    # FunctionsJSON picks up), but without either one JSONExtract* silently degrades
    # to DummyJSONParser, which fails every parse and returns defaults.
    set (ENABLE_SIMDJSON ON CACHE INTERNAL "")

    # Avro ON: pure C++ (boost::iostreams + snappy, both already built for WASM).
    # It is the gate for Iceberg/Paimon metadata reading and, together with
    # Parquet, for the DataLakeCatalog database engine.
    set (ENABLE_AVRO ON CACHE INTERNAL "")
    # Parquet READ via a slim Arrow build: Parquet + Thrift on, ORC off (ORC is the
    # only consumer of protobuf/protoc, which stay off) and Arrow's curl/HDFS object
    # store paths guarded out in contrib/arrow-cmake.
    set (ENABLE_PARQUET ON CACHE INTERNAL "")
    set (ENABLE_THRIFT ON CACHE INTERNAL "")
    set (ENABLE_ORC OFF CACHE INTERNAL "")
    # Hard requirements of the Parquet/Arrow build above - _arrow links
    # ch_contrib::brotli and _parquet links ch_contrib::rapidjson
    # unconditionally, so with build-wasm.sh's ENABLE_LIBRARIES=0 and these off the
    # configure fails outright with "target was not found".
    set (ENABLE_BROTLI ON CACHE INTERNAL "")
    set (ENABLE_RAPIDJSON ON CACHE INTERNAL "")

    # Emscripten's libc++ is not the chdb-patched libcxx, so the exception ABI
    # has no embedded stack trace. base/src expect this macro to be defined.
    add_definitions (-DSTD_EXCEPTION_HAS_STACK_TRACE=0)

    # Upstream additionally sets -sSTACK_SIZE / -sINITIAL_MEMORY / -sALLOW_MEMORY_GROWTH /
    # -sMAXIMUM_MEMORY / -sPROXY_TO_PTHREAD / -sEXIT_RUNTIME / -g0 globally here, for its own
    # `clickhouse` executable. chdb does not build that target for WASM; it links
    # programs/wasm, which sets the equivalents on the target itself (WASM_STACK_SIZE,
    # WASM_INITIAL_MEMORY, WASM_PTHREAD_POOL_SIZE, ...). Setting them globally too would
    # put two values for each flag on the same link line.
endif ()

# Since we always use toolchain files to generate hermetic builds, cmake will
# always think it's a cross-compilation, See
# https://cmake.org/cmake/help/latest/variable/CMAKE_CROSSCOMPILING.html
#
# This will slow down cmake configuration and compilation. For instance, LLVM
# will try to configure NATIVE LLVM targets with all tests enabled (You'll see
# Building native llvm-tblgen...).
#
# Here, we set it manually by checking the system name and processor.
if (${CMAKE_SYSTEM_NAME} STREQUAL ${CMAKE_HOST_SYSTEM_NAME} AND ${CMAKE_SYSTEM_PROCESSOR} STREQUAL ${CMAKE_HOST_SYSTEM_PROCESSOR})
    set (CMAKE_CROSSCOMPILING 0)
endif ()

if (CMAKE_CROSSCOMPILING)
    if (OS_DARWIN)
        set (ENABLE_FASTOPS OFF CACHE INTERNAL "")
    elseif (OS_LINUX OR OS_ANDROID)
        if (ARCH_PPC64LE)
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
        elseif (ARCH_RISCV64)
            # RISC-V support is preliminary
            set (GLIBC_COMPATIBILITY OFF CACHE INTERNAL "")
            set (ENABLE_LDAP OFF CACHE INTERNAL "")
            set (OPENSSL_NO_ASM ON CACHE INTERNAL "")
            set (ENABLE_JEMALLOC ON CACHE INTERNAL "")
            set (ENABLE_PARQUET OFF CACHE INTERNAL "")
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_HDFS OFF CACHE INTERNAL "")
            set (ENABLE_MYSQL OFF CACHE INTERNAL "")
            # It might be ok, but we need to update 'sysroot'
            set (ENABLE_RUST OFF CACHE INTERNAL "")
        elseif (ARCH_S390X)
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
            set (ENABLE_RUST OFF CACHE INTERNAL "")
    elseif (ARCH_LOONGARCH64)
            set (GLIBC_COMPATIBILITY OFF CACHE INTERNAL "")
            set (ENABLE_LDAP OFF CACHE INTERNAL "")
            set (OPENSSL_NO_ASM ON CACHE INTERNAL "")
            set (ENABLE_JEMALLOC OFF CACHE INTERNAL "")
            set (ENABLE_PARQUET OFF CACHE INTERNAL "")
            set (ENABLE_GRPC OFF CACHE INTERNAL "")
            set (ENABLE_HDFS OFF CACHE INTERNAL "")
            set (ENABLE_MYSQL OFF CACHE INTERNAL "")
            set (ENABLE_RUST OFF CACHE INTERNAL "")
            set (ENABLE_LIBPQXX OFF CACHE INTERNAL "")
            set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
            set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
            set (ENABLE_BLAKE3 OFF CACHE INTERNAL "")
        elseif (ARCH_E2K)
            # added for future use
            # for now, we're compiling it natively.
        endif ()
    elseif (OS_FREEBSD)
        # FIXME: broken dependencies
        set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
        set (ENABLE_DWARF_PARSER OFF CACHE INTERNAL "")
    elseif (OS_WASM)
        # Handled in the OS_WASM block above: it has to run before this one, because the
        # `CMAKE_CROSSCOMPILING` check below it needs `OS_WASM` to already be set.
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILER_TARGET}")
endif ()
