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
#   pool cannot grow under a blocking query in the browser (in Node it grows on
#   demand, which is why a smaller pool passes the Node tests but fails in a
#   browser with EAGAIN -> "Cannot schedule a task"), so it must cover the
#   threaded build's peak concurrent threads. Even with single-threaded queries
#   (max_threads capped to 1 by chdb_wasm.cpp via `SET max_threads`), the threaded
#   build still runs ClickHouse's background machinery (BackgroundSchedulePool,
#   system logs, ...), which needs well more than a handful of workers; 16 is the
#   value verified to work in a browser. PTHREAD_POOL_SIZE_STRICT=2 turns any
#   overflow into a clean error rather than a deadlock. Raise it if you raise
#   CHDB_WASM_MAX_THREADS or hit "Cannot schedule a task" in the browser.
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

# Note: -sMEMORY64=1, -fwasm-exceptions and (when WASM_THREADS=ON) -pthread are
# added globally in cmake/target.cmake's OS_WASM block.
target_link_options (chdb_wasm PRIVATE
    # Size optimization: -Oz runs Binaryen wasm-opt -Oz over the whole module
    # (whole-program DCE + size opts). ASSERTIONS=0 drops Emscripten's runtime
    # sanity checks (production setting; no functional change). Together these
    # shrink the .wasm substantially without affecting behavior.
    -Oz
    -sASSERTIONS=0
    -sMODULARIZE=1
    "SHELL:-s EXPORT_NAME=createChdbModule"
    -sALLOW_MEMORY_GROWTH=1
    "SHELL:-s INITIAL_MEMORY=${WASM_INITIAL_MEMORY}"
    "SHELL:-s STACK_SIZE=${WASM_STACK_SIZE}"
    -sWASM_BIGINT
    -sFORCE_FILESYSTEM=1
    -sALLOW_TABLE_GROWTH=1
    "SHELL:-s EXPORTED_RUNTIME_METHODS=ccall,cwrap,UTF8ToString,getValue,setValue,HEAPU8,FS"
    "SHELL:-s EXPORTED_FUNCTIONS=@${CMAKE_CURRENT_SOURCE_DIR}/exported_functions.txt"
    -sENVIRONMENT=node,web,worker
    )

# Pthread pool options only apply to the threaded build (-pthread). In the
# single-threaded build (WASM_THREADS=OFF) there is no pool, so omit them.
if (WASM_THREADS)
    target_link_options (chdb_wasm PRIVATE
        "SHELL:-s DEFAULT_PTHREAD_STACK_SIZE=${WASM_PTHREAD_STACK_SIZE}"
        "SHELL:-s PTHREAD_POOL_SIZE=${WASM_PTHREAD_POOL_SIZE}"
        # Hard cap: error (not deadlock) if more pthreads are needed than the pool.
        -sPTHREAD_POOL_SIZE_STRICT=2
        )
endif ()
