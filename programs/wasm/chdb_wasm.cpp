// WebAssembly entry/glue for chdb.
//
// Mirrors duckdb-wasm's approach: a stub main() plus a set of C functions
// exported to JS (see programs/wasm/exported_functions.txt). JS calls them via
// ccall/cwrap (or, in the npm package, from inside a Web Worker). We wrap chdb's
// argv-based C API (programs/local/chdb.h) into a flat, JS-friendly surface.
//
// Two layers are exposed:
//   * implicit connection: chdb_wasm_open/close/query — a process-wide :memory:
//     connection created lazily; convenient for one-shot use.
//   * explicit connections: chdb_wasm_connect/close_conn/query_conn[+ streaming]
//     — handle-based, so callers can manage sessions and stream large results.

#include "../local/chdb.h"

#include <emscripten.h>
#if defined(__EMSCRIPTEN_PTHREADS__)
#    include <emscripten/threading.h>
#endif

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>

// Defined in src/Client/LocalConnection.cpp. Forward-declared here instead of including
// <Client/LocalConnection.h>, which transitively pulls Poco/Net networking headers that
// aren't on the wasm include path. The signature MUST match the definition exactly.
namespace DB
{
void setCHDBProgressHook(
    std::function<void(uint64_t read_rows, uint64_t total_rows_to_read, uint64_t read_bytes,
                       uint64_t total_bytes_to_read, uint64_t elapsed_ns)> hook);
void setCHDBCancelCheck(std::function<bool()> check);
}

// chdb-wasm cancel flag: lives in shared wasm linear memory, so ANY query thread reads it
// with a plain atomic load (no per-thread JS globals — the EM_ASM/globalThis approach
// failed because the check sometimes runs on a pool pthread). The page writes it via the
// wasm Memory SharedArrayBuffer at the offset chdb_wasm_cancel_flag_addr() returns.
static std::atomic<int32_t> g_chdb_cancel_flag{0};

// chdb-wasm live progress: the engine writes the latest progress here on every tick — on
// WHATEVER thread runs the pipeline (usually a pool pthread). The page can't be reached
// from a pool pthread, and the worker thread is blocked in the synchronous query ccall, so
// neither can relay it. Instead the PAGE polls this struct directly: on the mt build the
// wasm Memory is a SharedArrayBuffer the page can view (at chdb_wasm_progress_addr()), and
// the page's own event loop stays free during the query. `seq` is bumped last on write
// (release) so a reader that observes a new seq also observes the matching fields; the
// reader re-checks seq to reject a torn read. 6 × int64 (page maps a BigInt64Array).
struct ChdbProgressShared
{
    std::atomic<int64_t> seq{0};
    std::atomic<int64_t> read_rows{0};
    std::atomic<int64_t> total_rows_to_read{0};
    std::atomic<int64_t> read_bytes{0};
    std::atomic<int64_t> total_bytes_to_read{0};
    std::atomic<int64_t> elapsed_ns{0};
};
static ChdbProgressShared g_chdb_progress;

extern "C" {

// Offset of the cancel flag in wasm memory, for the page to write via the Memory SAB.
uintptr_t chdb_wasm_cancel_flag_addr() { return reinterpret_cast<uintptr_t>(&g_chdb_cancel_flag); }

// Offset of the live-progress struct in wasm memory, for the page to poll via the Memory SAB.
uintptr_t chdb_wasm_progress_addr() { return reinterpret_cast<uintptr_t>(&g_chdb_progress); }

// On wasm the pthread pool is fixed-size (PTHREAD_POOL_SIZE) and cannot grow on
// demand: the chdb query runs synchronously in a worker, so the calling thread is
// blocked and can't service new-worker creation. If ClickHouse asks for more
// concurrent threads than the pool (max_threads defaults to the host core count!),
// it deadlocks / "Cannot schedule a task". So we cap query parallelism to a value
// comfortably below the pool (which leaves room for ClickHouse's background
// threads). The command-line --max_threads arg is NOT honored by the embedded
// server, but a session-level `SET max_threads` right after connect is, and it
// persists for the connection's lifetime.
#ifndef CHDB_WASM_MAX_THREADS
#    define CHDB_WASM_MAX_THREADS "1"
#endif
static char arg0[] = "chdb";

// ------------------------------------------------------------------
// Implicit, process-wide :memory: connection (created lazily on first query).
// ------------------------------------------------------------------

static chdb_connection * g_conn = nullptr;

// Register (once) a query-progress hook on the chdb engine. ClickHouse calls it on every
// progress tick, on whatever thread runs the pipeline (usually a pool pthread). We just
// publish the latest values into the shared-memory struct the page polls — no postMessage
// (a pool pthread can't reach the page) and no throttle (the page's poll interval throttles).
static void chdb_wasm_register_progress_hook()
{
    static bool registered = false;
    if (registered)
        return;
    registered = true;
    DB::setCHDBProgressHook([](uint64_t read_rows, uint64_t total_rows_to_read, uint64_t read_bytes,
                               uint64_t total_bytes_to_read, uint64_t elapsed_ns)
    {
        // Write fields first (relaxed), then bump seq last (release) so a reader observing
        // the new seq is guaranteed to see these fields.
        g_chdb_progress.read_rows.store(static_cast<int64_t>(read_rows), std::memory_order_relaxed);
        g_chdb_progress.total_rows_to_read.store(static_cast<int64_t>(total_rows_to_read), std::memory_order_relaxed);
        g_chdb_progress.read_bytes.store(static_cast<int64_t>(read_bytes), std::memory_order_relaxed);
        g_chdb_progress.total_bytes_to_read.store(static_cast<int64_t>(total_bytes_to_read), std::memory_order_relaxed);
        g_chdb_progress.elapsed_ns.store(static_cast<int64_t>(elapsed_ns), std::memory_order_relaxed);
        g_chdb_progress.seq.fetch_add(1, std::memory_order_release);
    });

    // Cancellation (mt only): the engine polls this on the query thread; we read a
    // SharedArrayBuffer flag the page sets via Atomics.store. The single-threaded build has
    // no SAB and never yields mid-query, so it can't be cancelled.
    DB::setCHDBCancelCheck([]() -> bool
    {
        // seq_cst (not relaxed): the query thread must reliably observe the page's
        // cross-thread Atomics.store of the flag.
        return g_chdb_cancel_flag.load(std::memory_order_seq_cst) != 0;
    });
}

// Apply the wasm thread cap on a freshly opened connection (idempotent, cheap).
static void chdb_wasm_apply_settings(chdb_connection conn)
{
    chdb_wasm_register_progress_hook();
    chdb_result * r = chdb_query(conn, "SET max_threads = " CHDB_WASM_MAX_THREADS, "Null");
    if (r)
        chdb_destroy_query_result(r);
#if defined(CHDB_WASM_SINGLE_THREADED)
    // The single-threaded build has no thread pool (no -pthread), so the Parquet (V3)
    // reader's decoder/prefetch pools can't be created ("Cannot schedule a task"). Force
    // its thread-free path: max_parsing_threads=1 makes the decoder run inline (ParquetV3
    // treats 1 as "no pool"), and disabling row-group prefetch avoids the IO thread pool
    // (reads then run synchronously inside IInputFormat::read()). The threaded build keeps
    // both — its pool pthreads can run these tasks.
    r = chdb_query(conn,
        "SET max_parsing_threads = 1, input_format_parquet_enable_row_group_prefetch = 0",
        "Null");
    if (r)
        chdb_destroy_query_result(r);
#endif
}

// Open (or reopen) the implicit :memory: connection. Returns 1 on success.
int chdb_wasm_open()
{
    if (g_conn)
        return 1;
    // argv[0] is the program name; default path is :memory: when --path is absent.
    char * argv[] = {arg0};
    g_conn = chdb_connect(1, argv);
    if (g_conn)
        chdb_wasm_apply_settings(*g_conn);
    return g_conn != nullptr ? 1 : 0;
}

void chdb_wasm_close()
{
    if (g_conn)
    {
        chdb_close_conn(g_conn);
        g_conn = nullptr;
    }
}

// Run a query on the implicit connection and return a chdb_result*. The caller
// reads it with the chdb_wasm_result_* accessors and frees it with
// chdb_wasm_free_result.
chdb_result * chdb_wasm_query(const char * sql, const char * format)
{
    if (!chdb_wasm_open())
        return nullptr;
    return chdb_query(*g_conn, sql, format && *format ? format : "CSV");
}

// ------------------------------------------------------------------
// Explicit connection handles. The returned chdb_connection* is opaque to JS
// (a pointer; a BigInt under the Memory64 ABI). chdb's embedded server is a
// singleton, so all connections share one :memory: instance.
// ------------------------------------------------------------------

// Open a connection. `path` may be null/empty for an in-memory database.
chdb_connection * chdb_wasm_connect(const char * path)
{
    chdb_connection * conn;
    if (path && *path)
    {
        std::string path_arg = std::string("--path=") + path;
        char * argv[] = {arg0, const_cast<char *>(path_arg.c_str())};
        conn = chdb_connect(2, argv);
    }
    else
    {
        char * argv[] = {arg0};
        conn = chdb_connect(1, argv);
    }
    if (conn)
        chdb_wasm_apply_settings(*conn);
    return conn;
}

void chdb_wasm_close_conn(chdb_connection * conn)
{
    if (conn)
        chdb_close_conn(conn);
}

chdb_result * chdb_wasm_query_conn(chdb_connection * conn, const char * sql, const char * format)
{
    if (!conn)
        return nullptr;
    return chdb_query(*conn, sql, format && *format ? format : "CSV");
}

// ------------------------------------------------------------------
// Streaming. chdb_wasm_stream_start opens a stream; chdb_wasm_stream_fetch
// returns the next chunk as a chdb_result* (read it with the result accessors;
// an empty/zero-length chunk signals end-of-stream). Cancel an in-flight stream
// with chdb_wasm_stream_cancel. Free each chunk with chdb_wasm_free_result.
// ------------------------------------------------------------------

chdb_result * chdb_wasm_stream_start(chdb_connection * conn, const char * sql, const char * format)
{
    if (!conn)
        return nullptr;
    return chdb_stream_query(*conn, sql, format && *format ? format : "CSV");
}

chdb_result * chdb_wasm_stream_fetch(chdb_connection * conn, chdb_result * stream)
{
    if (!conn)
        return nullptr;
    return chdb_stream_fetch_result(*conn, stream);
}

void chdb_wasm_stream_cancel(chdb_connection * conn, chdb_result * stream)
{
    if (conn)
        chdb_stream_cancel_query(*conn, stream);
}

// ------------------------------------------------------------------
// Result accessors (work on any chdb_result*).
// ------------------------------------------------------------------

const char * chdb_wasm_result_buffer(chdb_result * r) { return r ? chdb_result_buffer(r) : nullptr; }
size_t       chdb_wasm_result_length(chdb_result * r) { return r ? chdb_result_length(r) : 0; }
const char * chdb_wasm_result_error(chdb_result * r)  { return r ? chdb_result_error(r) : "null result"; }
double       chdb_wasm_result_elapsed(chdb_result * r){ return r ? chdb_result_elapsed(r) : 0.0; }
uint64_t     chdb_wasm_result_rows_read(chdb_result * r)  { return r ? chdb_result_rows_read(r) : 0; }
uint64_t     chdb_wasm_result_bytes_read(chdb_result * r) { return r ? chdb_result_bytes_read(r) : 0; }
// Source rows/bytes SCANNED from storage (distinct from rows_read/bytes_read, which
// are the RESULT size) — for the "Processed N rows, … bytes" footer line.
uint64_t     chdb_wasm_result_scanned_rows(chdb_result * r)  { return r ? chdb_result_storage_rows_read(r) : 0; }
uint64_t     chdb_wasm_result_scanned_bytes(chdb_result * r) { return r ? chdb_result_storage_bytes_read(r) : 0; }
void         chdb_wasm_free_result(chdb_result * r)   { if (r) chdb_destroy_query_result(r); }

} // extern "C"

// Stub main: the module is driven through the exported C functions above.
int main()
{
    return 0;
}
