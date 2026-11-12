option(USE_MONGODB "Enable MongoDB support" ${ENABLE_LIBRARIES})
if(NOT USE_MONGODB)
    message(STATUS "Not using libmongoc and libbson")
    return()
endif()

set(libbson_VERSION_MAJOR 2)
set(libbson_VERSION_MINOR 5)
set(libbson_VERSION_PATCH 7)
set(libbson_VERSION 2.5.7)
set(libmongoc_VERSION_MAJOR 2)
set(libmongoc_VERSION_MINOR 5)
set(libmongoc_VERSION_PATCH 7)
set(libmongoc_VERSION 2.5.7)

set(LIBBSON_SOURCES_ROOT "${ClickHouse_SOURCE_DIR}/contrib/mongo-c-driver/src")
set(LIBBSON_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/libbson/src")
file(GLOB_RECURSE LIBBSON_SOURCES "${LIBBSON_SOURCE_DIR}/*.c")

set(LIBBSON_BINARY_ROOT "${ClickHouse_BINARY_DIR}/contrib/mongo-c-driver/src")
set(LIBBSON_BINARY_DIR "${LIBBSON_BINARY_ROOT}/libbson/src")

# Hard-code endianness based on the target architecture (set in cmake/arch.cmake) instead
# of probing the compiler. Of ClickHouse's supported architectures, only s390x is
# big-endian; everything else is little-endian.
if (ARCH_S390X)
    set(BSON_BIG_ENDIAN 1)
    set(BSON_BYTE_ORDER 4321)
else()
    set(BSON_BIG_ENDIAN 0)
    set(BSON_BYTE_ORDER 1234)
endif()

set(BSON_OS 1)
set(BSON_EXTRA_ALIGN 1)
set(BSON_HAVE_SNPRINTF 1)
set(BSON_HAVE_TIMESPEC 1)
set(BSON_HAVE_GMTIME_R 1)
set(BSON_HAVE_RAND_R 1)
set(BSON_HAVE_STRINGS_H 1)
set(BSON_HAVE_STRLCPY 0)
set(BSON_HAVE_STRNLEN 1)
set(BSON_HAVE_ALIGNED_ALLOC 1)
set(BSON_HAVE_STDBOOL_H 1)
set(BSON_HAVE_CLOCK_GETTIME 1)

# chdb_spec: the values above describe a POSIX host. On Windows (mingw-w64) use what upstream's checks in
# src/libbson/CMakeLists.txt give there; they match the bson/config.h of MSYS2's CLANG64 build of this
# driver (clang, libc++, UCRT).
if(OS_WINDOWS)
    # `if (WIN32) set (BSON_OS 2)`: BSON_OS_WIN32, i.e. Winsock and the Win32 thread, time and error APIs.
    set(BSON_OS 2)
    # The CRT has no rand_r or aligned_alloc, and mingw-w64 declares gmtime_r only for _POSIX_C_SOURCE,
    # which the Windows build does not define (cmake/target.cmake).
    set(BSON_HAVE_GMTIME_R 0)
    set(BSON_HAVE_RAND_R 0)
    set(BSON_HAVE_ALIGNED_ALLOC 0)
    # Only gates two includes; the Windows code reads the clocks with QueryPerformanceCounter and
    # GetSystemTimeAsFileTime.
    set(BSON_HAVE_CLOCK_GETTIME 0)
endif()


# common settings
set(MONGOC_TRACE 0)
set(MONGOC_ENABLE_STATIC_BUILD 1)
set(MONGOC_ENABLE_DEBUG_ASSERTIONS 0)
set(MONGOC_ENABLE_MONGODB_AWS_AUTH 0)
set(MONGOC_ENABLE_SASL_CYRUS 0)
set(MONGOC_ENABLE_SASL 0)
set(MONGOC_ENABLE_SASL_SSPI 0)
set(MONGOC_HAVE_SASL_CLIENT_DONE 0)
set(MONGOC_ENABLE_SRV 0)

# DNS
set(MONGOC_HAVE_DNSAPI 0)
set(MONGOC_HAVE_RES_SEARCH 0)
set(MONGOC_HAVE_RES_NSEARCH 0)
set(MONGOC_HAVE_RES_NCLOSE 0)
set(MONGOC_HAVE_RES_NDESTROY 0)

set(MONGOC_ENABLE_COMPRESSION 1)
set(MONGOC_ENABLE_COMPRESSION_ZLIB 0)
set(MONGOC_ENABLE_COMPRESSION_SNAPPY 0)
set(MONGOC_ENABLE_COMPRESSION_ZSTD 1)

# SSL
set(MONGOC_ENABLE_CRYPTO 0)
set(MONGOC_ENABLE_CRYPTO_CNG 0)
set(MONGOC_ENABLE_CRYPTO_COMMON_CRYPTO 0)
set(MONGOC_ENABLE_CRYPTO_SYSTEM_PROFILE 0)
set(MONGOC_ENABLE_SSL 0)
set(MONGOC_ENABLE_SSL_OPENSSL 0)
set(MONGOC_ENABLE_SSL_SECURE_CHANNEL 0)
set(MONGOC_ENABLE_SSL_SECURE_TRANSPORT 0)
set(MONGOC_ENABLE_SSL_LIBRESSL 0)
set(MONGOC_ENABLE_CRYPTO_LIBCRYPTO 0)
set(MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION 0)
set(MONGOC_HAVE_ASN1_STRING_GET0_DATA 0)
set(MONGOC_HAVE_BCRYPT_PBKDF2 1)
if(ENABLE_SSL)
    set(MONGOC_ENABLE_SSL 1)
    set(MONGOC_ENABLE_CRYPTO 1)
    set(MONGOC_ENABLE_SSL_OPENSSL 1)
    set(MONGOC_ENABLE_CRYPTO_LIBCRYPTO 1)
    set(MONGOC_HAVE_ASN1_STRING_GET0_DATA 1)
else()
    message(WARNING "Building mongoc without SSL")
endif()

set(CMAKE_EXTRA_INCLUDE_FILES "sys/socket.h")
set(MONGOC_SOCKET_ARG2 "struct sockaddr")
set(MONGOC_HAVE_SOCKLEN 1)
set(MONGOC_SOCKET_ARG3 "socklen_t")

set(MONGOC_ENABLE_RDTSCP 0)
set(MONGOC_NO_AUTOMATIC_GLOBALS 1)
set(MONGOC_ENABLE_STATIC_INSTALL 0)
set(MONGOC_ENABLE_SHM_COUNTERS 0)
set(MONGOC_HAVE_SCHED_GETCPU 0)
set(MONGOC_HAVE_SS_FAMILY 0)

# chdb_spec: Windows (mingw-w64) keeps the Linux feature set above: TLS and SCRAM through OpenSSL, SRV
# and SASL (so also SSPI) off, zstd compression. The socket types are the same too: upstream finds
# socklen_t in ws2tcpip.h and uses `struct sockaddr` and `socklen_t` on WIN32. MONGOC_HAVE_DNSAPI stays 0
# although upstream sets it on WIN32: it only serves SRV lookups, and this driver version includes
# <WinDNS.h>, which the (lower-case) mingw-w64 headers on a case-sensitive file system do not have.
if(OS_WINDOWS)
    # Upstream probes BCryptDeriveKeyPBKDF2 only for the CNG crypto backend; with OpenSSL it is 0.
    set(MONGOC_HAVE_BCRYPT_PBKDF2 0)
endif()

configure_file(
        ${LIBBSON_SOURCE_DIR}/bson/config.h.in
        ${LIBBSON_BINARY_DIR}/bson/config.h
)
configure_file(
        ${LIBBSON_SOURCE_DIR}/bson/version.h.in
        ${LIBBSON_BINARY_DIR}/bson/version.h
)

set(COMMON_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/common/src")
set(COMMON_BINARY_DIR "${LIBBSON_BINARY_ROOT}/common/src")
file(GLOB_RECURSE COMMON_SOURCES "${COMMON_SOURCE_DIR}/*.c")
configure_file(
        ${COMMON_SOURCE_DIR}/common-config.h.in
        ${COMMON_BINARY_DIR}/common-config.h
)
add_library(_libbson ${LIBBSON_SOURCES} ${COMMON_SOURCES})
add_library(ch_contrib::libbson ALIAS _libbson)
target_include_directories(_libbson SYSTEM PUBLIC ${LIBBSON_SOURCE_DIR} ${LIBBSON_BINARY_DIR} ${COMMON_SOURCE_DIR} ${COMMON_BINARY_DIR})
target_include_directories(_libbson PRIVATE ${COMMON_SOURCE_DIR}/src)
target_compile_definitions(_libbson PRIVATE BSON_COMPILATION)
if(OS_LINUX)
    target_compile_definitions(_libbson PRIVATE -D_GNU_SOURCE -D_POSIX_C_SOURCE=199309L -D_XOPEN_SOURCE=600)
elseif(OS_DARWIN)
    target_compile_definitions(_libbson PRIVATE -D_DARWIN_C_SOURCE)
endif()
# chdb_spec: on Windows (mingw-w64), build the driver the way upstream's build does.
if(OS_WINDOWS)
    # <bson/compat.h> stops with "#error __USE_MINGW_ANSI_STDIO > 0 is required" when a CRT header has
    # already set it to 0, which mingw-w64 does for UCRT unless a POSIX feature macro is defined (upstream
    # defines _XOPEN_SOURCE=700 for the whole build; MSYS2 passes -D__USE_MINGW_ANSI_STDIO=1). Only the
    # drivers' own sources include <bson/bson.h>, not the bsoncxx/mongocxx headers ClickHouse uses, so
    # it stays PRIVATE.
    #
    # The driver's Windows code also expects the full <windows.h>, but the Windows build defines
    # WIN32_LEAN_AND_MEAN globally (cmake/target.cmake). Upstream gets the full header because
    # <bson/compat.h> includes <winsock2.h> before it defines WIN32_LEAN_AND_MEAN itself. That puts
    # <wincrypt.h> ahead of the OpenSSL headers, which #undef its clashing X509_NAME/OCSP_RESPONSE
    # macros (mongoc-openssl.c), and brings NTSTATUS from <bcrypt.h> (mongoc-handshake.c).
    set(MONGO_C_DRIVER_WINDOWS_DEFINITIONS __USE_MINGW_ANSI_STDIO=1)
    set(MONGO_C_DRIVER_WINDOWS_OPTIONS -UWIN32_LEAN_AND_MEAN)
    target_compile_definitions(_libbson PRIVATE ${MONGO_C_DRIVER_WINDOWS_DEFINITIONS})
    target_compile_options(_libbson PRIVATE ${MONGO_C_DRIVER_WINDOWS_OPTIONS})
    # Upstream's platform target links ws2_32 on WIN32 (top-level CMakeLists.txt).
    target_link_libraries(_libbson ws2_32)
endif()


set(LIBMONGOC_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/libmongoc/src")
set(LIBMONGOC_BINARY_DIR "${LIBBSON_BINARY_ROOT}/libmongoc/src")
file(GLOB_RECURSE LIBMONGOC_SOURCES "${LIBMONGOC_SOURCE_DIR}/*.c")
set(UTF8PROC_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/utf8proc-2.11.3")
set(UTF8PROC_SOURCES "${UTF8PROC_SOURCE_DIR}/utf8proc.c")
set(UTHASH_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/uthash")

set(MONGOC_CXX_COMPILER_ID "${CMAKE_CXX_COMPILER_ID}")
set(MONGOC_CXX_COMPILER_VERSION "${CMAKE_CXX_COMPILER_VERSION}")
configure_file(
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-config.h.in
        ${LIBMONGOC_BINARY_DIR}/mongoc/mongoc-config.h
)
configure_file(
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-version.h.in
        ${LIBMONGOC_BINARY_DIR}/mongoc/mongoc-version.h
)
configure_file(
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-config-private.h.in
        ${LIBMONGOC_BINARY_DIR}/mongoc/mongoc-config-private.h
)
add_library(_libmongoc ${LIBMONGOC_SOURCES} ${COMMON_SOURCES} ${UTF8PROC_SOURCES})
add_library(ch_contrib::libmongoc ALIAS _libmongoc)
target_include_directories(_libmongoc SYSTEM PUBLIC ${LIBMONGOC_SOURCE_DIR} ${LIBMONGOC_BINARY_DIR} ${LIBMONGOC_SOURCE_DIR}/mongoc ${LIBMONGOC_BINARY_DIR}/mongoc ${COMMON_SOURCE_DIR} ${UTF8PROC_SOURCE_DIR} ${UTHASH_SOURCE_DIR} )
target_compile_definitions(_libmongoc PRIVATE MONGOC_COMPILATION)
target_link_libraries(_libmongoc ch_contrib::libbson ch_contrib::c-ares ch_contrib::zstd)
if(ENABLE_SSL)
    target_link_libraries(_libmongoc OpenSSL::SSL)
endif()

# chdb_spec: Windows (mingw-w64); see _libbson above for the compile flags.
if(OS_WINDOWS)
    target_compile_definitions(_libmongoc PRIVATE ${MONGO_C_DRIVER_WINDOWS_DEFINITIONS})
    target_compile_options(_libmongoc PRIVATE ${MONGO_C_DRIVER_WINDOWS_OPTIONS})
    # mongoc-socket.c includes <Mstcpip.h>, but the mingw-w64 header is mstcpip.h, which a case-sensitive
    # file system does not match. Upstream lower-cased the name after this version (CDRIVER-6472,
    # mongo-c-driver 5cf0826a4d4d); until the submodule has that, forward the old name.
    set(MONGOC_WINDOWS_SHIM_DIR "${CMAKE_CURRENT_BINARY_DIR}/windows-include")
    file(CONFIGURE OUTPUT "${MONGOC_WINDOWS_SHIM_DIR}/Mstcpip.h" CONTENT "#pragma once\n#include_next <mstcpip.h>\n")
    target_include_directories(_libmongoc PRIVATE "${MONGOC_WINDOWS_SHIM_DIR}")
    # Upstream's Windows link interface for this configuration (src/libmongoc/CMakeLists.txt): advapi32,
    # which mongoc-socket.c uses to read the TCP keepalive settings from the registry, and for the OpenSSL
    # backend crypt32, which mongoc-openssl.c uses to load the Windows root store when no CA file is given.
    # ws2_32 comes from _libbson. Not dnsapi (SRV), secur32/shlwapi (SSPI) or secur32/bcrypt/ncrypt
    # (Secure Channel), which this configuration does not use.
    target_link_libraries(_libmongoc advapi32 crypt32)
endif()
