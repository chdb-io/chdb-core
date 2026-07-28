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
    # WebAssembly target (browser / node) via the Emscripten toolchain.
    # Invoke with `emcmake cmake ...`, which sets CMAKE_SYSTEM_NAME=Emscripten
    # and points CMAKE_TOOLCHAIN_FILE at Emscripten.cmake.
    set (OS_WASM 1)
    add_definitions(-D OS_WASM)
    # MAP_ANONYMOUS for the mmap fallback shim.
    add_definitions(-D _GNU_SOURCE)

    # ClickHouse assumes a 64-bit size_t / pointer width pervasively (e.g. `1e12uz`
    # literals in src/Core/Defines.h, sizeof-equality static_asserts in ProfileEvents).
    # The 32-bit wasm32 ABI breaks thousands of these. Build for wasm64 (Memory64)
    # so size_t and pointers are 64-bit, matching the codebase's assumptions.
    # Requires a recent runtime (Node >= 23, Chrome >= 133). Set WASM_MEMORY64=OFF to
    # attempt the (much harder) 32-bit port instead.
    option (WASM_MEMORY64 "Build the WASM target for the 64-bit Memory64 ABI" ON)
    if (WASM_MEMORY64)
        add_compile_options(-sMEMORY64=1)
        add_link_options(-sMEMORY64=1)
    endif ()

    # ClickHouse relies pervasively on C++ exception *catching*. Emscripten
    # disables catching by default (only throwing works), turning every try/catch
    # into a no-op so exceptions escape to JS. Enable native WebAssembly exception
    # handling (supported by Node >= 23 and modern browsers). It must be applied at
    # both compile and link so landing pads are emitted in every translation unit.
    add_compile_options(-fwasm-exceptions)
    add_link_options(-fwasm-exceptions)

    # ClickHouse is pervasively multi-threaded (global thread pool, background
    # schedule pools, the query pipeline).
    # WASM_THREADS=ON (default): real pthreads (Web Workers + SharedArrayBuffer).
    #   -pthread is an ABI flag and must be applied to every translation unit at
    #   compile and link. Needs the page to be cross-origin isolated (COOP/COEP).
    #   The worker pool size is set on the final link target (programs/wasm).
    # WASM_THREADS=OFF: single-threaded build with no -pthread and no
    #   SharedArrayBuffer dependency, so it runs on pages that are NOT cross-origin
    #   isolated. Emscripten without pthreads makes every thread creation fail, so
    #   the global thread pool degrades to running jobs inline and the optional
    #   background pools are not started (gated on CHDB_WASM_SINGLE_THREADED).
    option (WASM_THREADS "Build the WASM target with pthreads (requires cross-origin isolation)" ON)
    # WASM_JSPI: use JavaScript Promise Integration for the HTTP bridge — the
    # wasm stack suspends on an async fetch() instead of requiring synchronous
    # XHR / a subprocess. The only transport that works on Cloudflare Workers;
    # requires a JSPI-enabled engine (Chrome 137+, workerd, Node with
    # --experimental-wasm-jspi). Adds -sJSPI at link (programs/wasm) and
    # switches WasmHTTPBridge.cpp to the async transport.
    option (WASM_JSPI "Use JSPI (async fetch) for the WASM HTTP bridge" OFF)
    if (WASM_THREADS)
        add_compile_options(-pthread)
        add_link_options(-pthread)
    else ()
        add_definitions(-D CHDB_WASM_SINGLE_THREADED)
    endif ()
else ()
    message (FATAL_ERROR "Platform ${CMAKE_SYSTEM_NAME} is not supported")
endif ()

# WebAssembly cannot use threads-by-default, jemalloc, any networked storage,
# the embedded LLVM JIT, Rust, or hardware-specific code paths. Force these off
# unconditionally so the rest of the tree configures consistently for WASM.
if (OS_WASM)
    set (ENABLE_JEMALLOC OFF CACHE INTERNAL "")
    set (ENABLE_TCMALLOC OFF CACHE INTERNAL "")
    set (ENABLE_GRPC OFF CACHE INTERNAL "")
    set (ENABLE_ARROW_FLIGHT OFF CACHE INTERNAL "")
    set (ENABLE_HDFS OFF CACHE INTERNAL "")
    set (ENABLE_MYSQL OFF CACHE INTERNAL "")
    set (ENABLE_LIBPQXX OFF CACHE INTERNAL "")
    set (ENABLE_NURAFT OFF CACHE INTERNAL "")
    set (ENABLE_KAFKA OFF CACHE INTERNAL "")
    set (ENABLE_AMQPCPP OFF CACHE INTERNAL "")
    set (ENABLE_NATS OFF CACHE INTERNAL "")
    set (ENABLE_CASSANDRA OFF CACHE INTERNAL "")
    set (ENABLE_AZURE_BLOB_STORAGE OFF CACHE INTERNAL "")
    set (ENABLE_AWS_S3 OFF CACHE INTERNAL "")
    set (ENABLE_S3 OFF CACHE INTERNAL "")
    set (ENABLE_HIVE OFF CACHE INTERNAL "")
    set (ENABLE_ODBC OFF CACHE INTERNAL "")
    set (ENABLE_LDAP OFF CACHE INTERNAL "")
    set (ENABLE_KRB5 OFF CACHE INTERNAL "")
    set (ENABLE_GSASL_LIBRARY OFF CACHE INTERNAL "")
    set (ENABLE_CURL OFF CACHE INTERNAL "")
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

    # Build the slim chdb-core-lite feature set on WASM (disables ~30 optional
    # libs centrally). Set before the CHDB_LITE option()/block below so it sticks.
    set (CHDB_LITE ON CACHE BOOL "WASM uses the chdb-core-lite trim set" FORCE)

    # Go further than lite: the libs lite still opts-in but that WASM can't use
    # (native protoc bootstrap, networked object stores, heavy columnar formats).
    set (ENABLE_PROTOBUF OFF CACHE INTERNAL "")
    set (ENABLE_CAPNP OFF CACHE INTERNAL "")
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

    # Emscripten's libc++ is not the chdb-patched libcxx, so the exception ABI
    # has no embedded stack trace. base/src expect this macro to be defined.
    add_definitions (-DSTD_EXCEPTION_HAS_STACK_TRACE=0)
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
        # All WASM-specific disables are handled in the OS_WASM block above.
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported system: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    message (STATUS "Cross-compiling for target: ${CMAKE_CXX_COMPILER_TARGET}")
endif ()
