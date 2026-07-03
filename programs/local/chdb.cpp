#include "chdb.h"
#include "chdb-internal.h"
#include "ChunkCollectorOutputFormat.h"
#include "LocalServer.h"

#include <csignal>
#include <cstddef>
#include <cstring>
#include <ChdbClient.h>
#include <EmbeddedServer.h>
#if USE_PYTHON
#    include <PythonTableCache.h>
#endif
#include <Common/MemoryTracker.h>
#include <Common/SignalHandlers.h>
#include <Common/ThreadStatus.h>

#if defined(USE_MUSL) && defined(__aarch64__)
void chdb_musl_compile_stub(int arg)
{
    jmp_buf buf1;
    sigjmp_buf buf2;

    setjmp(buf1);
    sigsetjmp(buf2, arg);
}
#endif

#if USE_JEMALLOC
#    include <Common/memory.h>
#endif

#ifdef CHDB_STATIC_LIBRARY_BUILD
/// Force reference to ensure function registration object is linked
namespace DB
{
    extern void * ForceStaticRegistrationObjects();
}

[[maybe_unused]] void * force_link_function_references = DB::ForceStaticRegistrationObjects();
#endif

namespace CHDB
{

#if !USE_PYTHON && !defined(OS_WASM)
extern "C"
{
    extern chdb_state chdb_arrow_scan(chdb_connection, const char *, chdb_arrow_stream);
    extern chdb_result * chdb_query_arrow(chdb_connection, const char *, chdb_arrow_stream, const chdb_arrow_options *);
    extern chdb_result * chdb_query_arrow_n(chdb_connection, const char *, size_t, chdb_arrow_stream, const chdb_arrow_options *);
    extern chdb_result * chdb_stream_query_arrow(chdb_connection, const char *, const chdb_arrow_options *);
    extern chdb_result * chdb_stream_query_arrow_n(chdb_connection, const char *, size_t, const chdb_arrow_options *);
    extern chdb_state chdb_stream_fetch_arrow(chdb_connection, chdb_result *, chdb_arrow_stream);
}

/// Force-link references: chdb-arrow.cpp.o and chdb-arrow-output.cpp.o each
/// live in their own translation unit and have no internal callers, so the
/// linker would discard them under --gc-sections / .a archive semantics
/// when libchdb.so is linked from main.cpp's perspective. Taking the
/// address of one entry point from each .o keeps the whole .o alive.
[[maybe_unused]] void * force_link_arrow_functions[] = {
    reinterpret_cast<void*>(chdb_arrow_scan),
    reinterpret_cast<void*>(chdb_query_arrow),
    reinterpret_cast<void*>(chdb_query_arrow_n),
    reinterpret_cast<void*>(chdb_stream_query_arrow),
    reinterpret_cast<void*>(chdb_stream_query_arrow_n),
    reinterpret_cast<void*>(chdb_stream_fetch_arrow)
};
#endif

// used only in pyEntryClickHouseLocal
static std::mutex CHDB_MUTEX;

static local_result_v2 * convert2LocalResultV2(QueryResult * query_result)
{
    auto * local_result = new local_result_v2();
    auto * materialized_query_result = static_cast<MaterializedQueryResult *>(query_result);

    if (!materialized_query_result)
    {
        String error = "Query processing failed";
        local_result->error_message = new char[error.size() + 1];
        std::memcpy(local_result->error_message, error.c_str(), error.size() + 1);
    }
    else if (!materialized_query_result->getError().empty())
    {
        const String & error = materialized_query_result->getError();
        local_result->error_message = new char[error.size() + 1];
        std::memcpy(local_result->error_message, error.c_str(), error.size() + 1);
    }
    else if (!materialized_query_result->result_buffer)
    {
        local_result->rows_read = materialized_query_result->rows_read;
        local_result->bytes_read = materialized_query_result->bytes_read;
        local_result->elapsed = materialized_query_result->elapsed;
    }
    else
    {
        local_result->len = materialized_query_result->result_buffer->size();
        local_result->buf = materialized_query_result->result_buffer->data();
        local_result->_vec = materialized_query_result->result_buffer.release();
        local_result->rows_read = materialized_query_result->rows_read;
        local_result->bytes_read = materialized_query_result->bytes_read;
        local_result->elapsed = materialized_query_result->elapsed;
    }

    return local_result;
}

static local_result_v2 * createErrorLocalResultV2(const String & error)
{
    auto * local_result = new local_result_v2();
    local_result->error_message = new char[error.size() + 1];
    std::memcpy(local_result->error_message, error.c_str(), error.size() + 1);
    return local_result;
}

std::unique_ptr<MaterializedQueryResult> pyEntryClickHouseLocal(int argc, char ** argv)
{
    try
    {
        std::lock_guard<std::mutex> lock(CHDB_MUTEX);
        DB::ThreadStatus thread_status;
        DB::LocalServer app;
        app.init(argc, argv);

        /// Install crash-signal handlers so that chDB can produce useful stack traces
        /// on SIGSEGV/SIGILL/etc.  This is a no-op when the caller has opted out via
        /// chdb_set_signal_handlers_enabled(0), and it is safe to call on every query
        /// because addSignalHandler() is idempotent with respect to the OS handler.
        HandledSignals::instance().setupCommonDeadlySignalHandlers();

        int ret = app.run();
        std::unique_ptr<MaterializedQueryResult> result;
        if (ret == 0)
        {
            result = std::make_unique<MaterializedQueryResult>(
                ResultBuffer(app.stealQueryOutputVector()), app.getElapsedTime(), app.getProcessedRows(), app.getProcessedBytes(), 0, 0);
        }
        else
        {
            result = std::make_unique<MaterializedQueryResult>(app.getErrorMsg());
        }

        if (HandledSignals::disable_signal_handlers.load(std::memory_order_relaxed))
            chdb_reset_signal_handlers();

        return result;
    }
    catch (const DB::Exception & e)
    {
        if (HandledSignals::disable_signal_handlers.load(std::memory_order_relaxed))
            chdb_reset_signal_handlers();
        throw std::domain_error(DB::getExceptionMessage(e, false));
    }
    catch (const boost::program_options::error & e)
    {
        if (HandledSignals::disable_signal_handlers.load(std::memory_order_relaxed))
            chdb_reset_signal_handlers();
        throw std::invalid_argument("Bad arguments: " + std::string(e.what()));
    }
    catch (...)
    {
        if (HandledSignals::disable_signal_handlers.load(std::memory_order_relaxed))
            chdb_reset_signal_handlers();
        throw std::domain_error(DB::getCurrentExceptionMessage(true));
    }
}

const static std::string empty_string;
const std::string & chdb_result_error_string(chdb_result * result)
{
    if (!result)
        return empty_string;

    auto * query_result = reinterpret_cast<QueryResult *>(result);
    return query_result->getError();
}

const std::string & chdb_streaming_result_error_string(chdb_streaming_result * result)
{
    if (!result)
        return empty_string;

    auto * stream_query_result = reinterpret_cast<StreamQueryResult *>(result);
    return stream_query_result->getError();
}

chdb_connection * connect_chdb_with_exception(int argc, char ** argv)
{
    try
    {
        DB::ThreadStatus thread_status;
        auto & server = DB::EmbeddedServer::getInstance(argc, argv);
        auto client = DB::ChdbClient::create(server);
        if (!client)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to create ChdbClient");
        }

        auto * conn = new chdb_conn();
        conn->server = client.release();
        conn->connected = true;
        auto ** conn_ptr = new chdb_conn *(conn);

        if (HandledSignals::disable_signal_handlers.load(std::memory_order_relaxed))
            chdb_reset_signal_handlers();

        return reinterpret_cast<chdb_connection *>(conn_ptr);
    }
    catch (const DB::Exception & e)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to create connection: {}", DB::getExceptionMessage(e, false));
    }
    catch (const std::exception & e)
    {
        throw std::domain_error(std::string("Connection failed: ") + e.what());
    }
    catch (...)
    {
        throw std::domain_error(DB::getCurrentExceptionMessage(true));
    }
}

#if USE_PYTHON
void cachePythonTablesFromQuery(chdb_conn * conn, const std::string & query_str)
{
    if (!conn || !conn->server || !conn->connected)
        return;
    auto * client = reinterpret_cast<DB::ChdbClient *>(conn->server);
    client->findQueryableObjFromPyCache(query_str);
}
#endif

} // namespace CHDB

using namespace CHDB;

local_result * query_stable(int argc, char ** argv)
{
    auto query_result = pyEntryClickHouseLocal(argc, argv);
    if (!query_result->getError().empty() || query_result->result_buffer == nullptr)
        return nullptr;

    local_result * res = new local_result;
    res->len = query_result->result_buffer->size();
    res->buf = query_result->result_buffer->data();
    res->_vec = query_result->result_buffer.release();
    res->rows_read = query_result->rows_read;
    res->bytes_read = query_result->bytes_read;
    res->elapsed = query_result->elapsed;
    return res;
}

void free_result(local_result * result)
{
    if (!result)
    {
        return;
    }
    if (result->_vec)
    {
        std::vector<char> * vec = reinterpret_cast<std::vector<char> *>(result->_vec);
        delete vec;
        result->_vec = nullptr;
    }
    delete result;
}

local_result_v2 * query_stable_v2(int argc, char ** argv)
{
    // pyEntryClickHouseLocal may throw some serious exceptions, although it's not likely
    // to happen in the context of clickhouse-local. we catch them here and return an error
    local_result_v2 * res = nullptr;
    try
    {
        auto query_result = pyEntryClickHouseLocal(argc, argv);
        return convert2LocalResultV2(query_result.get());
    }
    catch (const std::exception & e)
    {
        res = new local_result_v2();
        res->error_message = new char[strlen(e.what()) + 1];
        std::strcpy(res->error_message, e.what());
    }
    catch (...)
    {
        res = new local_result_v2();
        const char * unknown_exception_msg = "Unknown exception";
        size_t len = std::strlen(unknown_exception_msg) + 1;
        res->error_message = new char[len];
        std::strcpy(res->error_message, unknown_exception_msg);
    }

    return res;
}

void free_result_v2(local_result_v2 * result)
{
    if (!result)
        return;

    delete reinterpret_cast<std::vector<char> *>(result->_vec);
    delete[] result->error_message;
    delete result;
}

chdb_conn ** connect_chdb(int argc, char ** argv)
{
    auto * connection = chdb_connect(argc, argv);
    if (!connection)
    {
        return nullptr;
    }
    return reinterpret_cast<chdb_conn **>(connection);
}

void close_conn(chdb_conn ** conn)
{
    if (!conn || !*conn)
        return;

    try
    {
        if ((*conn)->connected && (*conn)->server)
        {
            auto * client = static_cast<DB::ChdbClient *>((*conn)->server);
            delete client;
            (*conn)->server = nullptr;
        }

        (*conn)->connected = false;
        delete *conn;
        *conn = nullptr;
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

struct local_result_v2 * query_conn(chdb_conn * conn, const char * query, const char * format)
{
    return query_conn_n(conn, query, query ? std::strlen(query) : 0, format, format ? std::strlen(format) : 0);
}

struct local_result_v2 * query_conn_n(struct chdb_conn * conn, const char * query, size_t query_len, const char * format, size_t format_len)
{
    if (!checkConnectionValidity(conn))
        return createErrorLocalResultV2("Invalid or closed connection");

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(conn->server);
        auto query_result = client->executeMaterializedQuery(query, query_len, format, format_len);
        return convert2LocalResultV2(query_result.get());
    }
    catch (const std::exception & e)
    {
        return createErrorLocalResultV2(std::string("Error: ") + e.what());
    }
    catch (...)
    {
        return createErrorLocalResultV2(DB::getCurrentExceptionMessage(true));
    }
}

chdb_streaming_result * query_conn_streaming(chdb_conn * conn, const char * query, const char * format)
{
    return query_conn_streaming_n(conn, query, query ? std::strlen(query) : 0, format, format ? std::strlen(format) : 0);
}

chdb_streaming_result *
query_conn_streaming_n(struct chdb_conn * conn, const char * query, size_t query_len, const char * format, size_t format_len)
{
    if (!checkConnectionValidity(conn))
    {
        auto * result = new StreamQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_streaming_result *>(result);
    }

    try
    {
        // Use the client from the connection
        auto * client = static_cast<DB::ChdbClient *>(conn->server);
        auto query_result = client->executeStreamingInit(query, query_len, format, format_len);

        if (!query_result)
        {
            auto * result = new StreamQueryResult("Query processing failed");
            return reinterpret_cast<chdb_streaming_result *>(result);
        }

        return reinterpret_cast<chdb_streaming_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        auto * result = new StreamQueryResult(std::string("Error: ") + e.what());
        return reinterpret_cast<chdb_streaming_result *>(result);
    }
    catch (...)
    {
        auto * result = new StreamQueryResult(DB::getCurrentExceptionMessage(true));
        return reinterpret_cast<chdb_streaming_result *>(result);
    }
}

const char * chdb_streaming_result_error(chdb_streaming_result * result)
{
    if (!result)
        return nullptr;

    auto * stream_query_result = reinterpret_cast<StreamQueryResult *>(result);

    const auto & error_message = stream_query_result->getError();
    if (!error_message.empty())
        return error_message.c_str();

    return nullptr;
}

local_result_v2 * chdb_streaming_fetch_result(chdb_conn * conn, chdb_streaming_result * result)
{
    if (!checkConnectionValidity(conn))
        return createErrorLocalResultV2("Invalid or closed connection");

    if (!result)
        return createErrorLocalResultV2("Invalid streaming result");

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(conn->server);
        if (!client->hasStreamingQuery())
            return createErrorLocalResultV2("No active streaming query");
        auto query_result = client->executeStreamingIterate(result, false);
        if (!query_result)
            return createErrorLocalResultV2("Failed to fetch streaming results");
        auto * local_result = convert2LocalResultV2(query_result.release());
        return local_result;
    }
    catch (const std::exception & e)
    {
        return createErrorLocalResultV2(std::string("Error fetching streaming results: ") + e.what());
    }
    catch (...)
    {
        return createErrorLocalResultV2(std::string("Unknown error fetching streaming results: ") + DB::getCurrentExceptionMessage(true));
    }
}

void chdb_streaming_cancel_query(chdb_conn * conn, chdb_streaming_result * result)
{
    if (!checkConnectionValidity(conn))
        return;

    if (!result)
        return;

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(conn->server);
        client->cancelStreamingQuery(result);
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    /// Note: The result object should be freed by chdb_destroy_result(), not here
}

void chdb_destroy_result(chdb_streaming_result * result)
{
    if (!result)
        return;

    auto * stream_query_result = reinterpret_cast<StreamQueryResult *>(result);

    delete stream_query_result;
}

/// ============== New API Implementation ==============

chdb_connection * chdb_connect(int argc, char ** argv)
{
    try
    {
        auto * conn = connect_chdb_with_exception(argc, argv);

        if (HandledSignals::disable_signal_handlers.load(std::memory_order_relaxed))
            chdb_reset_signal_handlers();

        return conn;
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("EmbeddedServer"), "Connection failed with DB::Exception: {}", DB::getExceptionMessage(e, false));
        return nullptr;
    }
    catch (const boost::program_options::error & e)
    {
        LOG_ERROR(&Poco::Logger::get("EmbeddedServer"), "Connection failed with bad arguments: {}", e.what());
        return nullptr;
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("EmbeddedServer"), "Connection failed with Poco::Exception: {}", e.displayText());
        return nullptr;
    }
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("EmbeddedServer"), "Connection failed with unknown exception: {}", DB::getCurrentExceptionMessage(true));
        return nullptr;
    }
}

void chdb_close_conn(chdb_connection * conn)
{
    if (!conn || !*conn)
        return;

    auto * connection = reinterpret_cast<chdb_conn **>(conn);

    close_conn(connection);
}

chdb_result * chdb_query(chdb_connection conn, const char * query, const char * format)
{
    return chdb_query_n(conn, query, query ? std::strlen(query) : 0, format, format ? std::strlen(format) : 0);
}

chdb_result * chdb_query_n(chdb_connection conn, const char * query, size_t query_len, const char * format, size_t format_len)
{
    if (!conn)
    {
        auto * result = new MaterializedQueryResult("Unexpected null connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * result = new MaterializedQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    try
    {
        // Use the client from the connection
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        auto query_result = client->executeMaterializedQuery(query, query_len, format, format_len);

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        auto * result = new MaterializedQueryResult(std::string("Error: ") + e.what());
        return reinterpret_cast<chdb_result *>(result);
    }
    catch (...)
    {
        auto * result = new MaterializedQueryResult(DB::getCurrentExceptionMessage(true));
        return reinterpret_cast<chdb_result *>(result);
    }
}

chdb_result * chdb_query_cmdline(int argc, char ** argv)
{
    MaterializedQueryResult * result = nullptr;
    try
    {
        auto query_result = pyEntryClickHouseLocal(argc, argv);

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        result = new MaterializedQueryResult(e.what());
    }
    catch (...)
    {
        result = new MaterializedQueryResult("Unknown exception");
    }

    return reinterpret_cast<chdb_result *>(result);
}

chdb_result * chdb_stream_query(chdb_connection conn, const char * query, const char * format)
{
    return chdb_stream_query_n(conn, query, query ? std::strlen(query) : 0, format, format ? std::strlen(format) : 0);
}

chdb_result * chdb_stream_query_n(chdb_connection conn, const char * query, size_t query_len, const char * format, size_t format_len)
{
    if (!conn)
    {
        auto * result = new StreamQueryResult("Unexpected null connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * result = new StreamQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        auto query_result = client->executeStreamingInit(query, query_len, format, format_len);

        if (!query_result)
        {
            auto * result = new StreamQueryResult("Query processing failed");
            return reinterpret_cast<chdb_result *>(result);
        }

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        auto * result = new StreamQueryResult(std::string("Error: ") + e.what());
        return reinterpret_cast<chdb_result *>(result);
    }
    catch (...)
    {
        auto * result = new StreamQueryResult(DB::getCurrentExceptionMessage(true));
        return reinterpret_cast<chdb_result *>(result);
    }
}

namespace
{

/// Build a NameToNameMap from parallel C-ABI arrays of parameter names and values.
/// On duplicate names the last value wins (NameToNameMap == std::unordered_map).
DB::NameToNameMap buildParameterMap(
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count)
{
    DB::NameToNameMap params;
    if (param_count == 0)
        return params;

    if (!param_names || !param_values)
        throw std::invalid_argument("chdb_query_with_params: param_names/param_values must be non-null when param_count > 0");

    params.reserve(param_count);
    for (size_t i = 0; i < param_count; ++i)
    {
        const char * name = param_names[i];
        const char * value = param_values[i];
        if (!name || !value)
            throw std::invalid_argument("chdb_query_with_params: parameter name/value pointers must be non-null");

        const size_t name_len = param_name_lens ? param_name_lens[i] : std::strlen(name);
        const size_t value_len = param_value_lens ? param_value_lens[i] : std::strlen(value);

        params.insert_or_assign(std::string(name, name_len), std::string(value, value_len));
    }
    return params;
}

/// RAII guard mirroring the Python binding's QueryParameterGuard (see LocalChdb.cpp): sets named
/// parameters on the client for the duration of one query, then unconditionally clears them.
/// This matches Python `chdb.query(..., params=...)` semantics — including for streaming, where
/// parameters only need to be present during executeStreamingInit (the engine captures values then).
class CApiQueryParameterGuard
{
public:
    CApiQueryParameterGuard(DB::ChdbClient * client_, const DB::NameToNameMap & params) : client(client_)
    {
        if (client && !params.empty())
        {
            client->setQueryParameters(params);
            applied = true;
        }
    }

    ~CApiQueryParameterGuard()
    {
        if (client && applied)
            client->clearQueryParameters();
    }

    CApiQueryParameterGuard(const CApiQueryParameterGuard &) = delete;
    CApiQueryParameterGuard & operator=(const CApiQueryParameterGuard &) = delete;

private:
    DB::ChdbClient * client = nullptr;
    bool applied = false;
};

} // anonymous namespace

chdb_result * chdb_query_with_params(
    chdb_connection conn,
    const char * query,
    const char * format,
    const char * const * param_names,
    const char * const * param_values,
    size_t param_count)
{
    return chdb_query_with_params_n(
        conn,
        query,
        query ? std::strlen(query) : 0,
        format,
        format ? std::strlen(format) : 0,
        param_names,
        /*param_name_lens=*/nullptr,
        param_values,
        /*param_value_lens=*/nullptr,
        param_count);
}

chdb_result * chdb_query_with_params_n(
    chdb_connection conn,
    const char * query,
    size_t query_len,
    const char * format,
    size_t format_len,
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count)
{
    if (!conn)
    {
        auto * result = new MaterializedQueryResult("Unexpected null connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * result = new MaterializedQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        const auto params = buildParameterMap(param_names, param_name_lens, param_values, param_value_lens, param_count);
        CApiQueryParameterGuard guard(client, params);

        auto query_result = client->executeMaterializedQuery(query, query_len, format, format_len);
        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        auto * result = new MaterializedQueryResult(std::string("Error: ") + e.what());
        return reinterpret_cast<chdb_result *>(result);
    }
    catch (...)
    {
        auto * result = new MaterializedQueryResult(DB::getCurrentExceptionMessage(true));
        return reinterpret_cast<chdb_result *>(result);
    }
}

chdb_result * chdb_stream_query_with_params(
    chdb_connection conn,
    const char * query,
    const char * format,
    const char * const * param_names,
    const char * const * param_values,
    size_t param_count)
{
    return chdb_stream_query_with_params_n(
        conn,
        query,
        query ? std::strlen(query) : 0,
        format,
        format ? std::strlen(format) : 0,
        param_names,
        /*param_name_lens=*/nullptr,
        param_values,
        /*param_value_lens=*/nullptr,
        param_count);
}

chdb_result * chdb_stream_query_with_params_n(
    chdb_connection conn,
    const char * query,
    size_t query_len,
    const char * format,
    size_t format_len,
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count)
{
    if (!conn)
    {
        auto * result = new StreamQueryResult("Unexpected null connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * result = new StreamQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        const auto params = buildParameterMap(param_names, param_name_lens, param_values, param_value_lens, param_count);
        CApiQueryParameterGuard guard(client, params);

        auto query_result = client->executeStreamingInit(query, query_len, format, format_len);
        if (!query_result)
        {
            auto * result = new StreamQueryResult("Query processing failed");
            return reinterpret_cast<chdb_result *>(result);
        }

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        auto * result = new StreamQueryResult(std::string("Error: ") + e.what());
        return reinterpret_cast<chdb_result *>(result);
    }
    catch (...)
    {
        auto * result = new StreamQueryResult(DB::getCurrentExceptionMessage(true));
        return reinterpret_cast<chdb_result *>(result);
    }
}

chdb_result * chdb_stream_fetch_result(chdb_connection conn, chdb_result * result)
{
    if (!conn)
    {
        auto * query_result = new MaterializedQueryResult("Unexpected null connection");
        return reinterpret_cast<chdb_result *>(query_result);
    }

    if (!result)
    {
        auto * query_result = new MaterializedQueryResult("Unexpected null result");
        return reinterpret_cast<chdb_result *>(query_result);
    }

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * query_result = new MaterializedQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(query_result);
    }

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        if (!client->hasStreamingQuery())
            return reinterpret_cast<chdb_result *>(new MaterializedQueryResult("No active streaming query"));
        auto * stream_result = reinterpret_cast<StreamQueryResult *>(result);
        auto query_result = client->executeStreamingIterate(stream_result, false);
        if (!query_result)
            return reinterpret_cast<chdb_result *>(new MaterializedQueryResult("Failed to fetch streaming results"));

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        auto * query_result = new MaterializedQueryResult(std::string("Error: ") + e.what());
        return reinterpret_cast<chdb_result *>(query_result);
    }
    catch (...)
    {
        auto * query_result = new MaterializedQueryResult(DB::getCurrentExceptionMessage(true));
        return reinterpret_cast<chdb_result *>(query_result);
    }
}

void chdb_stream_cancel_query(chdb_connection conn, chdb_result * result)
{
    if (!result || !conn)
        return;

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return;

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        auto * stream_result = reinterpret_cast<StreamQueryResult *>(result);
        client->cancelStreamingQuery(stream_result);
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    /// Note: The result object should be freed by chdb_destroy_query_result(), not here
}

void chdb_destroy_query_result(chdb_result * result)
{
    if (!result)
        return;

    auto * query_result = reinterpret_cast<QueryResult *>(result);
    delete query_result;
}

chdb_insert_stream chdb_stream_insert(chdb_connection conn, const char * query, const char * format)
{
    return chdb_stream_insert_n(conn, query, query ? std::strlen(query) : 0, format, format ? std::strlen(format) : 0);
}

chdb_insert_stream chdb_stream_insert_n(
    chdb_connection conn, const char * query, size_t query_len, const char * format, size_t format_len)
{
    if (!conn)
        return reinterpret_cast<chdb_insert_stream>(new InsertStreamResult("Unexpected null connection"));

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return reinterpret_cast<chdb_insert_stream>(new InsertStreamResult("Invalid or closed connection"));

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        auto query_result = client->executeInsertStreamingInit(query, query_len, format, format_len);
        if (!query_result)
            return reinterpret_cast<chdb_insert_stream>(new InsertStreamResult("Insert stream initialization failed"));
        return reinterpret_cast<chdb_insert_stream>(query_result.release());
    }
    catch (const std::exception & e)
    {
        return reinterpret_cast<chdb_insert_stream>(new InsertStreamResult(std::string("Error: ") + e.what()));
    }
    catch (...)
    {
        return reinterpret_cast<chdb_insert_stream>(new InsertStreamResult(DB::getCurrentExceptionMessage(true)));
    }
}

/// True when the stream has already been finalized by done()/cancel() or by
/// the owning connection's teardown. Once set, the handle must not touch its
/// owner pointer (the client may be gone if the connection was closed with
/// the stream still open) — every entry point below checks this first.
static bool insert_stream_finalized(InsertStreamResult * res)
{
    if (!res->context)
        return false;
    auto ctx = std::static_pointer_cast<CHDB::InsertStreamContext>(res->context);
    return ctx->finalized.load(std::memory_order_acquire);
}

chdb_state chdb_stream_append(chdb_insert_stream stream, const void * data, size_t len)
{
    if (!stream)
        return CHDBError;

    auto * res = reinterpret_cast<InsertStreamResult *>(stream);
    if (insert_stream_finalized(res))
        return CHDBError;
    auto * client = static_cast<DB::ChdbClient *>(res->owner);
    if (!client)
        return CHDBError;

    try
    {
        const bool ok = client->executeInsertStreamingAppend(res, static_cast<const char *>(data), len);
        return ok ? CHDBSuccess : CHDBError;
    }
    catch (...)
    {
        res->setError(DB::getCurrentExceptionMessage(true));
        return CHDBError;
    }
}

chdb_result * chdb_stream_done(chdb_insert_stream stream)
{
    if (!stream)
        return reinterpret_cast<chdb_result *>(new MaterializedQueryResult("Unexpected null insert stream"));

    auto * res = reinterpret_cast<InsertStreamResult *>(stream);
    if (insert_stream_finalized(res))
        return reinterpret_cast<chdb_result *>(new MaterializedQueryResult("Insert stream already finalized"));
    auto * client = static_cast<DB::ChdbClient *>(res->owner);
    if (!client)
        return reinterpret_cast<chdb_result *>(new MaterializedQueryResult("Invalid insert stream"));

    try
    {
        auto query_result = client->executeInsertStreamingDone(res);
        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        return reinterpret_cast<chdb_result *>(new MaterializedQueryResult(std::string("Error: ") + e.what()));
    }
    catch (...)
    {
        return reinterpret_cast<chdb_result *>(new MaterializedQueryResult(DB::getCurrentExceptionMessage(true)));
    }
}

void chdb_stream_cancel_insert(chdb_insert_stream stream)
{
    if (!stream)
        return;

    auto * res = reinterpret_cast<InsertStreamResult *>(stream);
    if (insert_stream_finalized(res))
        return;
    auto * client = static_cast<DB::ChdbClient *>(res->owner);
    if (!client)
        return;

    try
    {
        client->cancelInsertStream(res);
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    /// Note: the handle is freed by chdb_destroy_insert_stream(), not here.
}

const char * chdb_stream_insert_error(chdb_insert_stream stream)
{
    if (!stream)
        return nullptr;

    auto * res = reinterpret_cast<InsertStreamResult *>(stream);
    const std::string & err = res->getError();
    if (!err.empty())
        return err.c_str();

    /// The worker records its failure (e.g. a parse error or
    /// MEMORY_LIMIT_EXCEEDED) in the context, not on the handle; surface it
    /// here so a failed append can be diagnosed before done() is called. The
    /// error_set acquire pairs with the worker's release-store, after which
    /// the worker never touches error_message again; the context is kept
    /// alive by the handle's shared_ptr.
    if (res->context)
    {
        auto ctx = std::static_pointer_cast<CHDB::InsertStreamContext>(res->context);
        if (ctx->error_set.load(std::memory_order_acquire) && !ctx->error_message.empty())
            return ctx->error_message.c_str();
    }
    return nullptr;
}

void chdb_destroy_insert_stream(chdb_insert_stream stream)
{
    if (!stream)
        return;

    auto * res = reinterpret_cast<InsertStreamResult *>(stream);
    /// If the stream was never finalized, abort it so the worker thread is
    /// joined and engine resources are released before we free the handle.
    /// Skip when already finalized — including by connection teardown, in
    /// which case res->owner dangles and must not be dereferenced.
    if (res->context && res->owner && !insert_stream_finalized(res))
    {
        try
        {
            static_cast<DB::ChdbClient *>(res->owner)->cancelInsertStream(res);
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    delete res;
}

char * chdb_result_buffer(chdb_result * result)
{
    if (!result)
        return nullptr;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->result_buffer ? materialized_result->result_buffer->data() : nullptr;
    }

    return nullptr;
}

size_t chdb_result_length(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);
    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->result_buffer ? materialized_result->result_buffer->size() : 0;
    }

    return 0;
}

double chdb_result_elapsed(chdb_result * result)
{
    if (!result)
        return 0.0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->elapsed;
    }
    return 0.0;
}

uint64_t chdb_result_rows_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->rows_read;
    }

    return 0;
}

uint64_t chdb_result_bytes_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->bytes_read;
    }

    return 0;
}

uint64_t chdb_result_storage_rows_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->storage_rows_read;
    }

    return 0;
}

uint64_t chdb_result_storage_bytes_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->storage_bytes_read;
    }

    return 0;
}

uint64_t chdb_result_rows_written(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->rows_written;
    }

    return 0;
}

uint64_t chdb_result_bytes_written(chdb_result * result)
{
    if (!result)
        return 0;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto * materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->bytes_written;
    }

    return 0;
}

const char * chdb_result_error(chdb_result * result)
{
    if (!result)
        return nullptr;

    auto * query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getError().empty())
        return nullptr;

    return query_result->getError().c_str();
}

void chdb_set_signal_handlers_enabled(int enabled)
{
    HandledSignals::disable_signal_handlers.store(!enabled, std::memory_order_relaxed);

    if (!enabled)
        chdb_reset_signal_handlers();
}

void chdb_reset_signal_handlers(void)
{
    static const std::vector<int> deadly_signals = {
        SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGTSTP, SIGTRAP
    };

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    for (int sig : deadly_signals)
        sigaction(sig, &sa, nullptr);

    auto & instance = HandledSignals::instance();
    instance.handled_signals.clear();
}
