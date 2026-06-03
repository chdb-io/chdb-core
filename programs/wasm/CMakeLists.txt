# WebAssembly entry module for chdb (Emscripten).
#
# Links the chdb C API (clickhouse-local-lib, which pulls in the ClickHouse core
# static libs transitively) and exposes a flat C surface to JS via ccall/cwrap
# (driven from a Web Worker in the npm package). See chdb_wasm.cpp and
# exported_functions.txt.

clickhouse_add_executable (chdb_wasm chdb_wasm.cpp wasm_compat.c)

target_link_libraries (chdb_wasm PRIVATE clickhouse-local-lib)

# --- Tunable knobs (see also WASM_MIN_SIZE in cmake/target.cmake) -------------
# These are link-time settings, so changing them only re-links (cheap).
#
# WASM_STACK_SIZE: main-thread C++ stack. ClickHouse recurses deeply (parser,
#   planner), so it must be generous; worker threads use WASM_PTHREAD_STACK_SIZE.
# WASM_PTHREAD_POOL_SIZE: number of Web Workers pre-spawned at module init. The
#   pool cannot grow under a blocking query in the browser, so it must cover
#   ClickHouse's peak concurrent threads = a query's max_threads (capped to 4 by
#   chdb_wasm.cpp via `SET max_threads`) + its background threads. 16 gives ample
#   headroom; PTHREAD_POOL_SIZE_STRICT=2 turns any overflow into a clean error
#   rather than a deadlock. Lower it for faster browser startup if your queries are
#   light (but keep it well above CHDB_WASM_MAX_THREADS).
# WASM_PTHREAD_STACK_SIZE: per-worker stack (there are WASM_PTHREAD_POOL_SIZE of
#   these, so this dominates baseline memory — keep it as small as queries allow).
set (WASM_STACK_SIZE "8MB" CACHE STRING "chdb-wasm main-thread stack size")
set (WASM_PTHREAD_POOL_SIZE "16" CACHE STRING "chdb-wasm pre-spawned worker pool size")
set (WASM_PTHREAD_STACK_SIZE "2MB" CACHE STRING "chdb-wasm per-worker stack size")
set (WASM_INITIAL_MEMORY "128MB" CACHE STRING "chdb-wasm initial heap size")

# Emit an ES module loadable from Node / the browser: chdb.mjs (+ chdb.wasm).
set_target_properties (chdb_wasm PROPERTIES
    OUTPUT_NAME chdb
    SUFFIX ".mjs"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")

# Note: -sMEMORY64=1, -fwasm-exceptions and -pthread are added globally in
# cmake/target.cmake's OS_WASM block.
target_link_options (chdb_wasm PRIVATE
    -sMODULARIZE=1
    "SHELL:-s EXPORT_NAME=createChdbModule"
    -sALLOW_MEMORY_GROWTH=1
    "SHELL:-s INITIAL_MEMORY=${WASM_INITIAL_MEMORY}"
    "SHELL:-s STACK_SIZE=${WASM_STACK_SIZE}"
    "SHELL:-s DEFAULT_PTHREAD_STACK_SIZE=${WASM_PTHREAD_STACK_SIZE}"
    -sWASM_BIGINT
    "SHELL:-s PTHREAD_POOL_SIZE=${WASM_PTHREAD_POOL_SIZE}"
    # Hard cap: error (not deadlock) if more pthreads are needed than the pool.
    -sPTHREAD_POOL_SIZE_STRICT=2
    -sFORCE_FILESYSTEM=1
    -sALLOW_TABLE_GROWTH=1
    "SHELL:-s EXPORTED_RUNTIME_METHODS=ccall,cwrap,UTF8ToString,getValue,setValue,HEAPU8"
    "SHELL:-s EXPORTED_FUNCTIONS=@${CMAKE_CURRENT_SOURCE_DIR}/exported_functions.txt"
    -sENVIRONMENT=node,web,worker
    -sASSERTIONS=1
    )
