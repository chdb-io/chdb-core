if (NOT ENABLE_LIBFIU)
  message (STATUS "Not using libfiu")
  return ()
endif ()

set(LIBFIU_DIR "${ClickHouse_SOURCE_DIR}/contrib/libfiu/")

set(LIBFIU_SOURCES
    ${LIBFIU_DIR}/libfiu/fiu.c
    ${LIBFIU_DIR}/libfiu/backtrace.c
    ${LIBFIU_DIR}/libfiu/wtable.c
)
# chdb_spec: fiu-rc.c is libfiu's remote control (fiu_rc_fifo), which talks over POSIX named pipes
# (mkfifo). ClickHouse never uses it, so Windows builds the core without it.
if (NOT OS_WINDOWS)
    list(APPEND LIBFIU_SOURCES ${LIBFIU_DIR}/libfiu/fiu-rc.c)
endif ()

set(LIBFIU_HEADERS "${LIBFIU_DIR}/libfiu")

add_library(_libfiu ${LIBFIU_SOURCES})
target_compile_definitions(_libfiu PRIVATE DUMMY_BACKTRACE)
target_compile_definitions(_libfiu PUBLIC FIU_ENABLE)
target_include_directories(_libfiu PUBLIC ${LIBFIU_HEADERS})
add_library(ch_contrib::libfiu ALIAS _libfiu)
