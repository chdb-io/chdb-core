#pragma once

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
extern "C" {
#else
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#endif

#define CHDB_EXPORT __attribute__((visibility("default")))

#ifndef CHDB_NO_DEPRECATED
// WARNING: The following structs are deprecated and will be removed in a future version.
struct local_result
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *, for freeing
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
};

#ifdef __cplusplus
struct local_result_v2
{
    char * buf = nullptr;
    size_t len = 0;
    void * _vec = nullptr; // std::vector<char> *, for freeing
    double elapsed = 0.0;
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    char * error_message = nullptr;
};
#else
struct local_result_v2
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *, for freeing
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
    char * error_message;
};
#endif

/**
 * Connection structure for chDB
 * Contains ChdbClient instance and connection state
 */
struct chdb_conn
{
    void * server; /* ChdbClient instance */
    bool connected; /* Connection state flag */
};

typedef struct
{
	void * internal_data;
} chdb_streaming_result;

#endif

// Return state enumeration for chDB API functions
typedef enum chdb_state
{
    CHDBSuccess = 0,
    CHDBError = 1
} chdb_state;

// Opaque handle for query results.
// Internal data structure managed by chDB implementation.
// Users should only interact through API functions.
typedef struct chdb_result_
{
	void * internal_data;
} chdb_result;

// Connection handle wrapping database session state.
// Internal data structure managed by chDB implementation.
// Users should only interact through API functions.
typedef struct chdb_connection_
{
	void * internal_data;
} * chdb_connection;

// Holds an arrow array stream.
typedef struct chdb_arrow_stream_
{
	void * internal_data;
} * chdb_arrow_stream;

// Holds an arrow schema.
typedef struct chdb_arrow_schema_
{
	void * internal_data;
} * chdb_arrow_schema;

// Holds an arrow array.
typedef struct chdb_arrow_array_
{
	void * internal_data;
} * chdb_arrow_array;

#ifndef CHDB_NO_DEPRECATED
// WARNING: The following interfaces are deprecated and will be removed in a future version.
CHDB_EXPORT struct local_result * query_stable(int argc, char ** argv);
CHDB_EXPORT void free_result(struct local_result * result);

CHDB_EXPORT struct local_result_v2 * query_stable_v2(int argc, char ** argv);
CHDB_EXPORT void free_result_v2(struct local_result_v2 * result);

/**
 * Creates a new chDB connection.
 * Only one active connection is allowed per process.
 * Creating a new connection with different path requires closing existing connection.
 *
 * @param argc Number of command-line arguments
 * @param argv Command-line arguments array (--path=<db_path> to specify database location)
 * @return Pointer to connection pointer, or NULL on failure
 * @note Default path is ":memory:" if not specified
 */
CHDB_EXPORT struct chdb_conn ** connect_chdb(int argc, char ** argv);

/**
 * Closes an existing chDB connection and cleans up resources.
 * Thread-safe function that handles connection shutdown and cleanup.
 *
 * @param conn Pointer to connection pointer to close
 */
CHDB_EXPORT void close_conn(struct chdb_conn ** conn);

/**
 * Executes a query on the given connection.
 * Thread-safe function that handles query execution in a separate thread.
 *
 * @param conn Connection to execute query on
 * @param query SQL query string to execute
 * @param format Output format string (e.g., "CSV", default format)
 * @return Query result structure containing output or error message
 * @note Returns error result if connection is invalid or closed
 */
CHDB_EXPORT struct local_result_v2 * query_conn(struct chdb_conn * conn, const char * query, const char * format);

/**
 * Executes a query on the given connection with explicit length parameters.
 * @brief Thread-safe query execution with binary-safe string handling
 * @param conn Connection to execute query on
 * @param query SQL query string to execute (may contain null bytes)
 * @param query_len Length of query string in bytes
 * @param format Output format string (e.g., "CSV", default format)
 * @param format_len Length of format string in bytes
 * @return Query result structure containing output or error message
 * @note Returns error result if connection is invalid or closed
 * @note This function is binary-safe and can handle queries containing null bytes
 */
CHDB_EXPORT struct local_result_v2 *
query_conn_n(struct chdb_conn * conn, const char * query, size_t query_len, const char * format, size_t format_len);

/**
 * Executes a streaming query on the given connection.
 * @brief Initializes streaming query execution and returns result handle
 * @param conn Connection to execute query on
 * @param query SQL query string to execute
 * @param format Output format string (e.g. "CSV", default format)
 * @return Streaming result handle containing query state or error message
 * @note Returns error result if connection is invalid or closed
 */
CHDB_EXPORT chdb_streaming_result * query_conn_streaming(struct chdb_conn * conn, const char * query, const char * format);

/**
 * Executes a streaming query on the given connection with explicit length parameters.
 * @brief Initializes streaming query execution with binary-safe string handling
 * @param conn Connection to execute query on
 * @param query SQL query string to execute (may contain null bytes)
 * @param query_len Length of query string in bytes
 * @param format Output format string (e.g., "CSV", default format)
 * @param format_len Length of format string in bytes
 * @return Streaming result handle containing query state or error message
 * @note Returns error result if connection is invalid or closed
 * @note This function is binary-safe and can handle queries containing null bytes
 * @note Use chdb_streaming_fetch_result() to retrieve data chunks from the streaming query
 */
CHDB_EXPORT chdb_streaming_result *
query_conn_streaming_n(struct chdb_conn * conn, const char * query, size_t query_len, const char * format, size_t format_len);

/**
 * Retrieves error message from streaming result.
 * @brief Gets error message associated with streaming query execution
 * @param result Streaming result handle from query_conn_streaming()
 * @return Null-terminated error message string, or NULL if no error occurred
 */
CHDB_EXPORT const char * chdb_streaming_result_error(chdb_streaming_result * result);

/**
 * Fetches next chunk of streaming results.
 * @brief Iterates through streaming query results
 * @param conn Active connection handle
 * @param result Streaming result handle from query_conn_streaming()
 * @return Materialized result chunk with data
 * @note Returns empty result when stream ends
 */
CHDB_EXPORT struct local_result_v2 * chdb_streaming_fetch_result(struct chdb_conn * conn, chdb_streaming_result * result);

/**
 * Cancels ongoing streaming query.
 * @brief Aborts streaming query execution and cleans up resources
 * @param conn Active connection handle
 * @param result Streaming result handle to cancel
 */
CHDB_EXPORT void chdb_streaming_cancel_query(struct chdb_conn * conn, chdb_streaming_result * result);

/**
 * Releases resources associated with streaming result.
 * @brief Destroys streaming result handle and frees allocated memory
 * @param result Streaming result handle to destroy
 * @warning Must be called even if query was finished or canceled
 */
 CHDB_EXPORT void chdb_destroy_result(chdb_streaming_result * result);

#endif

/**
 * Creates a new chDB connection.
 * Only one active connection is allowed per process.
 * Creating a new connection with different path requires closing existing connection.
 *
 * @param argc Number of command-line arguments
 * @param argv Command-line arguments array (--path=<db_path> to specify database location)
 * @return Pointer to connection pointer, or NULL on failure
 * @note Default path is ":memory:" if not specified
 */
CHDB_EXPORT chdb_connection * chdb_connect(int argc, char ** argv);

/**
 * Closes an existing chDB connection and cleans up resources.
 * Thread-safe function that handles connection shutdown and cleanup.
 *
 * @param conn Pointer to connection pointer to close
 */
 CHDB_EXPORT void chdb_close_conn(chdb_connection * conn);

/**
 * Executes a query on the given connection.
 * Thread-safe function that handles query execution in a separate thread.
 *
 * @param conn Connection to execute query on
 * @param query SQL query string to execute
 * @param format Output format string (e.g., "CSV", default format)
 * @return Query result structure containing output or error message
 * @note Returns error result if connection is invalid or closed
 */
CHDB_EXPORT chdb_result * chdb_query(chdb_connection conn, const char * query, const char * format);

/**
 * Executes a query on the given connection with explicit length parameters.
 * @brief Thread-safe query execution with binary-safe string handling
 * @param conn Connection to execute query on
 * @param query SQL query string to execute (may contain null bytes)
 * @param query_len Length of query string in bytes
 * @param format Output format string (e.g., "CSV", default format)
 * @param format_len Length of format string in bytes
 * @return Query result structure containing output or error message
 * @note Returns error result if connection is invalid or closed
 * @note This function is binary-safe and can handle queries containing null bytes
 * @note Use chdb_result_* functions to access result data and metadata
 */
CHDB_EXPORT chdb_result * chdb_query_n(chdb_connection conn, const char * query, size_t query_len, const char * format, size_t format_len);

/**
 * @brief Execute a query with command-line interface
 * @param argc Argument count (same as main()'s argc)
 * @param argv Argument vector (same as main()'s argv)
 * @return Query result structure containing output or error message
 */
CHDB_EXPORT chdb_result * chdb_query_cmdline(int argc, char ** argv);

/**
 * Executes a streaming query on the given connection.
 * @brief Initializes streaming query execution and returns result handle
 * @param conn Connection to execute query on
 * @param query SQL query string to execute
 * @param format Output format string (e.g. "CSV", default format)
 * @return Streaming result handle containing query state or error message
 * @note Returns error result if connection is invalid or closed
 */
CHDB_EXPORT chdb_result * chdb_stream_query(chdb_connection conn, const char * query, const char * format);

/**
 * Executes a streaming query with explicit string lengths (binary-safe).
 * @brief Initializes streaming query execution with specified buffer lengths
 * @param conn Connection to execute query on
 * @param query SQL query buffer (may contain null bytes)
 * @param query_len Length of query buffer in bytes
 * @param format Output format buffer (may contain null bytes)
 * @param format_len Length of format buffer in bytes
 * @return Streaming result handle containing query state or error message
 * @note Strings do not need to be null-terminated
 * @note Use this function when dealing with queries/formats containing null bytes
 */
CHDB_EXPORT chdb_result *
chdb_stream_query_n(chdb_connection conn, const char * query, size_t query_len, const char * format, size_t format_len);

/**
 * Executes a query with server-side named parameter binding.
 * @brief Binds {name:Type} placeholders before query execution; values are NOT interpolated into SQL.
 * @param conn Connection to execute query on
 * @param query SQL query string (NUL-terminated, e.g. "SELECT {x:Int64} AS v")
 * @param format Output format string (e.g. "CSV", "JSON")
 * @param param_names Array of param_count NUL-terminated parameter names (must match {name:Type} placeholders)
 * @param param_values Array of param_count NUL-terminated parameter values
 * @param param_count Number of name/value pairs in the arrays
 * @return Query result structure containing output or error message
 * @note Parameter values are passed to the engine as strings; the engine resolves the type from the
 *       {name:Type} placeholder. This avoids SQL injection (no string interpolation) and enables
 *       server-side query-plan caching.
 * @note On duplicate parameter names, the last value wins (NameToNameMap semantics).
 * @note Parameters are scoped to this single call and cleared on return (RAII).
 * @note Use chdb_query_with_params_n for binary-safe values containing NUL bytes.
 */
CHDB_EXPORT chdb_result * chdb_query_with_params(
    chdb_connection conn,
    const char * query,
    const char * format,
    const char * const * param_names,
    const char * const * param_values,
    size_t param_count);

/**
 * Executes a query with server-side named parameter binding and explicit string lengths.
 * @brief Binary-safe variant of chdb_query_with_params() — values may contain NUL bytes.
 * @param conn Connection to execute query on
 * @param query SQL query buffer (may contain NUL bytes)
 * @param query_len Length of query buffer in bytes
 * @param format Output format buffer (may contain NUL bytes)
 * @param format_len Length of format buffer in bytes
 * @param param_names Array of param_count parameter names (each name_lens[i] bytes long)
 * @param param_name_lens Array of param_count byte lengths for param_names
 * @param param_values Array of param_count parameter value buffers (each value_lens[i] bytes long)
 * @param param_value_lens Array of param_count byte lengths for param_values
 * @param param_count Number of name/value pairs
 * @return Query result structure containing output or error message
 * @note Strings do not need to be NUL-terminated.
 */
CHDB_EXPORT chdb_result * chdb_query_with_params_n(
    chdb_connection conn,
    const char * query,
    size_t query_len,
    const char * format,
    size_t format_len,
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count);

/**
 * Executes a streaming query with server-side named parameter binding.
 * @brief Initializes streaming query execution with parameter binding.
 * @param conn Connection to execute query on
 * @param query SQL query string (NUL-terminated)
 * @param format Output format string (e.g. "CSV", "JSON")
 * @param param_names Array of param_count NUL-terminated parameter names
 * @param param_values Array of param_count NUL-terminated parameter values
 * @param param_count Number of name/value pairs
 * @return Streaming result handle containing query state or error message
 * @note Parameters are bound on the connection before streaming starts and cleared once the
 *       streaming initialization returns (the engine has already captured the parameter values).
 */
CHDB_EXPORT chdb_result * chdb_stream_query_with_params(
    chdb_connection conn,
    const char * query,
    const char * format,
    const char * const * param_names,
    const char * const * param_values,
    size_t param_count);

/**
 * Executes a streaming query with server-side named parameter binding and explicit string lengths.
 * @brief Binary-safe variant of chdb_stream_query_with_params().
 * @param conn Connection to execute query on
 * @param query SQL query buffer (may contain NUL bytes)
 * @param query_len Length of query buffer in bytes
 * @param format Output format buffer (may contain NUL bytes)
 * @param format_len Length of format buffer in bytes
 * @param param_names Array of param_count parameter names
 * @param param_name_lens Array of param_count byte lengths for param_names
 * @param param_values Array of param_count parameter value buffers
 * @param param_value_lens Array of param_count byte lengths for param_values
 * @param param_count Number of name/value pairs
 * @return Streaming result handle containing query state or error message
 */
CHDB_EXPORT chdb_result * chdb_stream_query_with_params_n(
    chdb_connection conn,
    const char * query,
    size_t query_len,
    const char * format,
    size_t format_len,
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count);

/**
 * Fetches next chunk of streaming results.
 * @brief Iterates through streaming query results
 * @param conn Active connection handle
 * @param result Streaming result handle from query_conn_streaming()
 * @return Materialized result chunk with data
 * @note Returns empty result when stream ends
 */
CHDB_EXPORT chdb_result * chdb_stream_fetch_result(chdb_connection conn, chdb_result * result);

/**
 * Cancels ongoing streaming query.
 * @brief Aborts streaming query execution and cleans up resources
 * @param conn Active connection handle
 * @param result Streaming result handle to cancel
 */
CHDB_EXPORT void chdb_stream_cancel_query(chdb_connection conn, chdb_result * result);

/**
 * Destroys a query result and releases all associated resources
 * @param result The result handle to destroy
 */
CHDB_EXPORT void chdb_destroy_query_result(chdb_result * result);

/**
 * Gets pointer to the result data buffer
 * @param result The query result handle
 * @return Read-only pointer to the result data
 */
CHDB_EXPORT char * chdb_result_buffer(chdb_result * result);

/**
 * Gets the length of the result data
 * @param result The query result handle
 * @return Size of result data in bytes
 */
CHDB_EXPORT size_t chdb_result_length(chdb_result * result);

/**
 * Gets query execution time
 * @param result The query result handle
 * @return Elapsed time in seconds
 */
CHDB_EXPORT double chdb_result_elapsed(chdb_result * result);

/**
 * Gets total rows in query result
 * @param result The query result handle
 * @return Number of rows contained in the result set
 */
CHDB_EXPORT uint64_t chdb_result_rows_read(chdb_result * result);

/**
 * Gets the total bytes occupied by the result set in internal binary format
 * @param result The query result handle
 * @return Number of bytes occupied by the result set in internal binary representation
 */
CHDB_EXPORT uint64_t chdb_result_bytes_read(chdb_result * result);

/**
 * Gets rows read from storage engine
 * @param result The query result handle
 * @return Number of rows read from storage
 */
CHDB_EXPORT uint64_t chdb_result_storage_rows_read(chdb_result * result);

/**
 * Gets bytes read from storage engine
 * @param result The query result handle
 * @return Number of bytes read from storage engine
 */
CHDB_EXPORT uint64_t chdb_result_storage_bytes_read(chdb_result * result);

/**
 * Retrieves error message from query execution
 * @param result The query result handle
 * @return Null-terminated error description, NULL if no error
 */
CHDB_EXPORT const char * chdb_result_error(chdb_result * result);

//===--------------------------------------------------------------------===//
// Arrow Integration
//===--------------------------------------------------------------------===//

/**
 * Options controlling Arrow C Data Interface output (chdb_query_arrow family).
 * Pass NULL to any chdb_query_arrow / chdb_stream_query_arrow variant for
 * the default contract (unsupported_as_binary=0,
 * low_cardinality_as_dictionary=0, string_as_string=1).
 *
 * Type mapping note: DateTime columns export as Arrow uint32 (Unix
 * seconds), matching ClickHouse's format="ArrowStream" behavior — the
 * timezone is not carried on the Arrow type. Callers that want a
 * timezone-tagged Arrow timestamp should request DateTime64 in SQL,
 * e.g. `SELECT toDateTime64(col, 0, 'UTC')`, which the kernel maps to
 * arrow::timestamp(SECOND, 'UTC').
 *
 * - unsupported_as_binary: 0 (default) throws UNKNOWN_TYPE for types with
 *   no faithful Arrow mapping (JSON/Object, Dynamic, AggregateFunction).
 *   1 silently degrades them to arrow::binary() like the engine default.
 * - low_cardinality_as_dictionary: 0 (default) materializes LowCardinality
 *   columns to their base type T. 1 emits an Arrow dictionary array
 *   (requires consumer to handle cross-batch dictionary stability).
 * - string_as_string: 1 (default) emits Arrow utf8 for String columns.
 *   0 emits Arrow binary.
 */
typedef struct chdb_arrow_options
{
    int unsupported_as_binary;
    int low_cardinality_as_dictionary;
    int string_as_string;
} chdb_arrow_options;

/**
 * Executes a query and exports the entire result as an Arrow C Data
 * Interface stream into the caller-allocated out_stream. Zero-copy where
 * possible (no IPC serialization, no lz4 round-trip).
 *
 * Ownership: out_stream is filled and ownership of its release() callback
 * transfers to the caller. The underlying ClickHouse buffers are kept
 * alive by the stream's private_data and are released atomically when the
 * caller invokes out_stream->release(out_stream).
 *
 * @param conn Active connection
 * @param query Null-terminated SQL query
 * @param out_stream Caller-allocated ArrowArrayStream (struct from arrow/c/abi.h)
 * @param options Knobs controlling type mapping; pass NULL for defaults
 * @return chdb_result with elapsed/rows_read/bytes_read/storage_* metrics or
 *         an error (buffer/length are NULL/0 — the data is in out_stream)
 */
CHDB_EXPORT chdb_result * chdb_query_arrow(
    chdb_connection conn, const char * query,
    chdb_arrow_stream out_stream, const chdb_arrow_options * options);

/**
 * Binary-safe variant of chdb_query_arrow with explicit query length.
 */
CHDB_EXPORT chdb_result * chdb_query_arrow_n(
    chdb_connection conn, const char * query, size_t query_len,
    chdb_arrow_stream out_stream, const chdb_arrow_options * options);

/**
 * Initializes a streaming Arrow query and returns a stream handle. Each
 * subsequent chdb_stream_fetch_arrow() call pulls one record batch with a
 * stable schema across the lifetime of the stream. Cancel/destroy via the
 * shared chdb_stream_cancel_query / chdb_destroy_query_result functions.
 */
CHDB_EXPORT chdb_result * chdb_stream_query_arrow(
    chdb_connection conn, const char * query,
    const chdb_arrow_options * options);

/**
 * Binary-safe variant of chdb_stream_query_arrow with explicit query length.
 */
CHDB_EXPORT chdb_result * chdb_stream_query_arrow_n(
    chdb_connection conn, const char * query, size_t query_len,
    const chdb_arrow_options * options);

/**
 * Pulls the next record batch from a streaming Arrow query into the
 * caller-allocated out_batch (a single-batch ArrowArrayStream). When the
 * stream is exhausted out_batch->get_next() will return a released array
 * on first call. Schema and (if enabled) LowCardinality dictionaries stay
 * stable across all fetches because the converter and cached dictionary
 * values are persisted on the stream handle.
 *
 * @param conn Active connection
 * @param stream_result Stream handle from chdb_stream_query_arrow{,_n}
 * @param out_batch Caller-allocated ArrowArrayStream filled with one batch
 * @return CHDBSuccess on success, CHDBError on protocol error
 */
CHDB_EXPORT chdb_state chdb_stream_fetch_arrow(
    chdb_connection conn, chdb_result * stream_result,
    chdb_arrow_stream out_batch);

/**
 * Registers an Arrow stream as an arrow stream table function with the given name
 * @param conn The connection on which to execute the registration
 * @param table_name Name to register for the arrow stream table function
 * @param arrow_stream chdb Arrow stream handle
 * @return CHDBSuccess on success, CHDBError on failure
 */
CHDB_EXPORT chdb_state chdb_arrow_scan(
    chdb_connection conn, const char * table_name,
    chdb_arrow_stream arrow_stream);

/**
 * Registers an Arrow array as an arrow stream table function with the given name
 * @param conn The connection on which to execute the registration
 * @param table_name Name to register for the arrow stream table function
 * @param arrow_schema chdb Arrow schema handle
 * @param arrow_array chdb Arrow array handle
 * @return CHDBSuccess on success, CHDBError on failure
 */
CHDB_EXPORT chdb_state chdb_arrow_array_scan(
    chdb_connection conn, const char * table_name,
    chdb_arrow_schema arrow_schema, chdb_arrow_array arrow_array);

/**
 * Unregisters an arrow stream table function that was previously registered via chdb_arrow_scan
 * @param conn The connection on which to execute the unregister operation
 * @param table_name Name of the arrow stream table function to unregister
 * @return CHDBSuccess on success, CHDBError on failure
 */
CHDB_EXPORT chdb_state chdb_arrow_unregister_table(chdb_connection conn, const char * table_name);

//===--------------------------------------------------------------------===//
// Signal Handler Control
//===--------------------------------------------------------------------===//

/**
 * Controls whether chDB installs process-wide signal handlers.
 * Call BEFORE chdb_connect() or query_stable() to prevent chDB
 * from installing process-wide signal handlers.
 *
 * @param enabled 1 to enable signal handlers (default), 0 to disable them
 */
CHDB_EXPORT void chdb_set_signal_handlers_enabled(int enabled);

/**
 * Resets all signal handlers installed by chDB back to SIG_DFL.
 * Useful when signal handlers were already installed and need to be removed,
 * e.g. to let the embedding process manage its own signal handling.
 */
CHDB_EXPORT void chdb_reset_signal_handlers(void);

#ifdef __cplusplus
}
#endif
