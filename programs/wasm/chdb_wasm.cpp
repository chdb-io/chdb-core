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

#include <cstdlib>
#include <cstring>
#include <string>

extern "C" {

// ------------------------------------------------------------------
// Implicit, process-wide :memory: connection (created lazily on first query).
// ------------------------------------------------------------------

static chdb_connection * g_conn = nullptr;

// Open (or reopen) the implicit :memory: connection. Returns 1 on success.
int chdb_wasm_open()
{
    if (g_conn)
        return 1;
    // argv[0] is the program name; default path is :memory: when --path is absent.
    static char arg0[] = "chdb";
    char * argv[] = {arg0};
    g_conn = chdb_connect(1, argv);
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
    static char arg0[] = "chdb";
    if (path && *path)
    {
        std::string path_arg = std::string("--path=") + path;
        char * argv[] = {arg0, const_cast<char *>(path_arg.c_str())};
        return chdb_connect(2, argv);
    }
    char * argv[] = {arg0};
    return chdb_connect(1, argv);
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
void         chdb_wasm_free_result(chdb_result * r)   { if (r) chdb_destroy_query_result(r); }

} // extern "C"

// Stub main: the module is driven through the exported C functions above.
int main()
{
    return 0;
}
