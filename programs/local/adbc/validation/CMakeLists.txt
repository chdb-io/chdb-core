# Standalone build for the chDB ADBC conformance suite.
#
# This project is deliberately NOT part of the ClickHouse build: the suite
# loads a prebuilt libchdb through the ADBC driver manager at run time, so it
# needs no ClickHouse target and no ClickHouse build flags. Configure it on its
# own, against any libchdb you want to check:
#
#   cmake -S programs/local/adbc/validation -B build/adbc-validation
#   cmake --build build/adbc-validation
#   CHDB_ADBC_DRIVER=/path/to/libchdb.so ./run.sh build/adbc-validation
#
# See README.md.

cmake_minimum_required(VERSION 3.24)
project(chdb_adbc_validation CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# programs/local/adbc/validation -> repository root
get_filename_component(CHDB_CORE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../.." ABSOLUTE)

# The arrow-adbc release the suite is built from. This is independent of the
# header vendored at ../adbc.h: the suite brings its own copy of adbc.h, and
# needs a release whose DriverQuirks exposes the dialect hooks used below
# (QuoteIdentifier, RewriteSql), which landed after the tag that header records.
set(CHDB_ADBC_TAG "apache-arrow-adbc-24" CACHE STRING "arrow-adbc tag to build the suite from")

# The conformance suite itself is upstream's. Fetching it (rather than vendoring
# ~250 KB of test sources) keeps upgrades to a tag bump, and lets upstream's own
# CMake resolve its dependencies: nanoarrow and fmt are vendored inside it, it
# fetches its own GoogleTest, and the driver manager is built alongside. Every
# driver backend stays off, so this builds only the validation objects, the
# driver manager, and their dependencies.
include(FetchContent)
set(ADBC_BUILD_TESTS ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ADBC_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ADBC_USE_CCACHE OFF CACHE BOOL "" FORCE)
# Use the copies of nanoarrow and fmt that ship inside arrow-adbc, so the suite
# pulls in nothing else. These default to ON upstream but are set explicitly
# because the defaults are declared in a macro that a parent project bypasses.
set(ADBC_WITH_VENDORED_NANOARROW ON CACHE BOOL "" FORCE)
set(ADBC_WITH_VENDORED_FMT ON CACHE BOOL "" FORCE)
# The driver manager is what loads libchdb by path at run time. Upstream only
# turns it on implicitly for its own integration tests, so ask for it directly.
set(ADBC_DRIVER_MANAGER ON CACHE BOOL "" FORCE)
FetchContent_Declare(
    arrow_adbc
    GIT_REPOSITORY https://github.com/apache/arrow-adbc.git
    GIT_TAG ${CHDB_ADBC_TAG}
    GIT_SHALLOW TRUE
    SOURCE_SUBDIR c)
FetchContent_MakeAvailable(arrow_adbc)

# add_arrow_lib() names the driver manager per linkage, so pick whichever this
# configuration produced.
if(TARGET adbc_driver_manager_static)
    set(CHDB_ADBC_DRIVER_MANAGER adbc_driver_manager_static)
elseif(TARGET adbc_driver_manager_shared)
    set(CHDB_ADBC_DRIVER_MANAGER adbc_driver_manager_shared)
elseif(TARGET adbc_driver_manager)
    set(CHDB_ADBC_DRIVER_MANAGER adbc_driver_manager)
else()
    message(FATAL_ERROR "arrow-adbc did not produce a driver manager target")
endif()

add_executable(chdb_adbc_validation chdb_validation.cc)
target_link_libraries(chdb_adbc_validation
                      PRIVATE adbc_validation adbc_validation_util
                              ${CHDB_ADBC_DRIVER_MANAGER} GTest::gtest GTest::gmock GTest::gtest_main)
target_include_directories(chdb_adbc_validation
                           PRIVATE "${arrow_adbc_SOURCE_DIR}/c/include"
                                   "${arrow_adbc_SOURCE_DIR}/c/validation"
                                   # adbc_validation_util.h includes "common/utils.h"
                                   "${arrow_adbc_SOURCE_DIR}/c/driver")
