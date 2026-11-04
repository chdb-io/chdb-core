#include "LocalChdb.h"
#include "PyBorrowGuard.h"

#include <base/scope_guard.h>
#include "chdb-internal.h"
#include "DataFrameQueryResult.h"
#include "PandasDataFrameBuilder.h"
#include "PythonArrowStream.h"
#include "PythonImporter.h"
#include "ChdbPyType.h"
#include "ChdbGlobalFunctions.h"
#include "PythonUDAFRegistry.h"
#include "PythonUDFRegistry.h"
#include "StoragePython.h"
#include "ChdbClient.h"
#include "EmbeddedServer.h"
#include "PyDateTimeHelper.h"
#include "chdb.h"

#ifndef CHDB_FREE_THREADING
#include <pybind11/detail/non_limited_api.h>
#endif
#include <pybind11/pybind11.h>
#include <IO/Progress.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/CurrentThread.h>
#include <Poco/String.h>
#include <Common/logger_useful.h>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <vector>
#if USE_JEMALLOC
#    include <Common/memory.h>
#endif

#if USE_EMBEDDED_COMPILER
#    include <Interpreters/JIT/CompiledExpressionCache.h>
#endif

#if USE_CLIENT_AI
#    include "AIQueryProcessor.h"
#endif

namespace py = pybind11;

extern bool inside_main = true;

namespace
{

/// chdb_spec: Python owns only connections constructed by this module. Keep
/// their registry alive past C++ static teardown; the Python atexit callback
/// drains it while Python and the engine's static dependencies are still live.
struct PythonConnectionRegistry
{
    std::mutex mutex;
    std::condition_variable state_changed;
    bool exiting = false;
    size_t active_stateless_queries = 0;
    std::atomic<bool> exit_complete{false};
    std::vector<std::weak_ptr<connection_wrapper>> connections;

    static PythonConnectionRegistry & instance()
    {
        static auto * registry = new PythonConnectionRegistry;
        return *registry;
    }
};

/// Diagnostic deadline only: the monitor never releases an object that an
/// operation still uses. The embedding process' watchdog owns the hard limit.
class PythonExitWatchdog
{
public:
    PythonExitWatchdog() : monitor([this]
    {
        std::unique_lock lock(mutex);
        if (!finished_changed.wait_for(lock, std::chrono::minutes(5), [this] { return finished; }))
        {
            const char * current = phase.load(std::memory_order_acquire);
            (void)::write(STDERR_FILENO, current, std::strlen(current));
            finished_changed.wait(lock, [this] { return finished; });
        }
    }) {}

    ~PythonExitWatchdog()
    {
        {
            std::lock_guard lock(mutex);
            finished = true;
        }
        finished_changed.notify_all();
        monitor.join();
    }

    void setPhase(const char * value) { phase.store(value, std::memory_order_release); }

private:
    std::mutex mutex;
    std::condition_variable finished_changed;
    bool finished = false;
    std::atomic<const char *> phase{"chDB Python exit: shutdown still pending after 300s\n"};
    std::thread monitor;
};
thread_local connection_wrapper::Operation * current_python_operation = nullptr;
/// chdb_spec: end Python connection exit ownership.

std::string paramValueToString(const py::handle & value)
{
    if (value.is_none())
        return "NULL";
    if (py::isinstance<py::bool_>(value))
        return py::cast<bool>(value) ? "1" : "0";

    if (py::isinstance<py::list>(value) || py::isinstance<py::tuple>(value))
    {
        std::vector<std::string> parts;
        parts.reserve(static_cast<size_t>(py::len(value)));
        for (const auto & item : value)
            parts.emplace_back(paramValueToString(item));

        std::ostringstream out;
        out << '[';
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (i)
                out << ',';
            out << parts[i];
        }
        out << ']';
        return out.str();
    }

    return py::cast<std::string>(py::str(value));
}

DB::NameToNameMap parseParametersDict(const py::dict & params)
{
    DB::NameToNameMap parsed;
    for (const auto & item : params)
    {
        const std::string key = py::cast<std::string>(item.first);
        const auto & value = item.second;
        parsed.emplace(key, paramValueToString(value));
    }
    return parsed;
}

class QueryParameterGuard
{
public:
    QueryParameterGuard(DB::ChdbClient * client_, const DB::NameToNameMap & params) : client(client_)
    {
        if (client && !params.empty())
        {
            client->setQueryParameters(params);
            applied = true;
        }
    }

    ~QueryParameterGuard()
    {
        if (client && applied)
            client->clearQueryParameters();
    }

private:
    DB::ChdbClient * client;
    bool applied = false;
};

DB::ChdbClient * getChdbClient(chdb_connection handle)
{
    auto * connection = reinterpret_cast<chdb_conn *>(handle);
    if (!checkConnectionValidity(connection))
        return nullptr;

    return static_cast<DB::ChdbClient *>(connection->server);
}

}

CHDB::PythonRuntimeOperationGuard::PythonRuntimeOperationGuard()
{
    auto & registry = PythonConnectionRegistry::instance();
    std::lock_guard lock(registry.mutex);
    if (registry.exiting)
        throw std::runtime_error("chDB is shutting down");
    ++registry.active_stateless_queries;
}

CHDB::PythonRuntimeOperationGuard::~PythonRuntimeOperationGuard()
{
    auto & registry = PythonConnectionRegistry::instance();
    std::lock_guard lock(registry.mutex);
    if (!--registry.active_stateless_queries)
        registry.state_changed.notify_all();
}

namespace CHDB
{
extern chdb_connection * connect_chdb_with_exception(int argc, char ** argv);
extern void cachePythonTablesFromQuery(chdb_conn * conn, const std::string & query_str);
}

const static char * CURSOR_DEFAULT_FORMAT = "JSONCompactEachRowWithNamesAndTypes";
const static size_t CURSOR_DEFAULT_FORMAT_LEN = strlen(CURSOR_DEFAULT_FORMAT);

chdb_result * queryToBuffer(
    const std::string & queryStr,
    const std::string & output_format = "CSV",
    const std::string & path = {},
    const std::string & udfPath = {},
    const DB::NameToNameMap & params = {})
{
    std::vector<std::string> argv = {"clickhouse", "--multiquery"};

    // If format is "Debug" or "debug", then we will add `--verbose` and `--log-level=trace` to argv
    if (output_format == "Debug" || output_format == "debug")
    {
        argv.push_back("--verbose");
        argv.push_back("--log-level=test");
        // Add format string
        argv.push_back("--output-format=CSV");
    }
    else
    {
        // Add format string
        argv.push_back("--output-format=" + output_format);
    }

    // If path is not empty, then we will add `--path` to argv. This is used for chdb.Session to support stateful query
    if (!path.empty())
    {
        // Add path string
        argv.push_back("--path=" + path);
    }
    // argv.push_back("--no-system-tables");
    // Add query string
    argv.push_back("--query=" + queryStr);

    // If udfPath is not empty, then we will add `--user_scripts_path` and `--user_defined_executable_functions_config` to argv
    // the path should be a one time thing, so the caller should take care of the temporary files deletion
    if (!udfPath.empty())
    {
        argv.push_back("--");
        argv.push_back("--user_scripts_path=" + udfPath);
        argv.push_back("--user_defined_executable_functions_config=" + udfPath + "/*.xml");
    }

    for (const auto & [key, value] : params)
    {
        argv.push_back("--param_" + key + "=" + value);
    }

    // Convert std::string to char*
    std::vector<char *> argv_char;
    argv_char.reserve(argv.size());
    for (auto & arg : argv)
        argv_char.push_back(const_cast<char *>(arg.c_str()));

    py::gil_scoped_release release;
    return chdb_query_cmdline(argv_char.size(), argv_char.data());
}

// Pybind11 will take over the ownership of the `query_result` object
// using smart ptr will cause early free of the object
query_result * query(
    const std::string & queryStr,
    const std::string & output_format = "CSV",
    const std::string & path = {},
    const std::string & udfPath = {},
    const py::dict & params = py::dict())
{
    CHDB::PythonRuntimeOperationGuard operation;
    return new query_result(queryToBuffer(queryStr, output_format, path, udfPath, parseParametersDict(params)));
}

// The `query_result` and `memoryview_wrapper` will hold `local_result_wrapper` with shared_ptr
memoryview_wrapper * query_result::get_memview()
{
    return new memoryview_wrapper(this->result_wrapper);
}


// Parse SQLite-style connection string
std::pair<std::string, std::map<std::string, std::string>> connection_wrapper::parse_connection_string(const std::string & conn_str)
{
    std::string path;
    std::map<std::string, std::string> params;

    if (conn_str.empty() || conn_str == ":memory:")
    {
        return {":memory:", params};
    }

    std::string working_str = conn_str;

    // Handle file: prefix
    if (working_str.starts_with("file:"))
    {
        working_str = working_str.substr(5);

        // Handle triple slash for absolute paths
        if (working_str.starts_with("///"))
        {
            working_str = working_str.substr(2); // Remove two slashes, keep one
        }
    }

    // Split path and parameters
    auto query_pos = working_str.find('?');
    if (query_pos != std::string::npos)
    {
        path = working_str.substr(0, query_pos);
        std::string params_str = working_str.substr(query_pos + 1);

        // Parse parameters
        std::istringstream params_stream(params_str);
        std::string param;
        while (std::getline(params_stream, param, '&'))
        {
            auto eq_pos = param.find('=');
            if (eq_pos != std::string::npos)
            {
                std::string key = param.substr(0, eq_pos);
                std::string value = param.substr(eq_pos + 1);
                params[key] = value;
            }
            else if (!param.empty())
            {
                // Handle parameters without values
                params[param] = "";
            }
        }
        // Handle udf_path
        // add user_scripts_path and user_defined_executable_functions_config to params
        // these two parameters need "--" as prefix
        if (params.contains("udf_path"))
        {
            std::string udf_path = params["udf_path"];
            if (!udf_path.empty())
            {
                params["--"] = "";
                params["user_scripts_path"] = udf_path;
                params["user_defined_executable_functions_config"] = udf_path + "/*.xml";
            }
            // remove udf_path from params
            params.erase("udf_path");
        }
    }
    else
    {
        path = working_str;
    }

    // Convert relative paths to absolute
    if (!path.empty() && path[0] != '/' && path != ":memory:")
    {
        std::error_code ec;
        path = std::filesystem::absolute(path, ec).string();
        if (ec)
        {
            throw std::runtime_error("Failed to resolve path: " + path);
        }
    }

    return {path, params};
}

std::vector<std::string>
connection_wrapper::build_clickhouse_args(const std::string & path, const std::map<std::string, std::string> & params)
{
    std::vector<std::string> argv = {"clickhouse"};

    if (path != ":memory:")
    {
        argv.push_back("--path=" + path);
    }

    // Map SQLite parameters to ClickHouse arguments
    for (const auto & [key, value] : params)
    {
        if (key == "mode")
        {
            if (value == "ro")
            {
                is_readonly = true;
                /// readonly=2, not 1: writes and DDL are rejected, but the
                /// connection may still tune per-query settings (SET /
                /// SETTINGS clauses), matching what a SQLite-style mode=ro
                /// promises. readonly=1 would reject those too.
                argv.push_back("--readonly=2");
            }
        }
        else if (key == "--")
        {
            // Handle special parameters "--"
            argv.push_back("--");
        }
        else if (value.empty())
        {
            // Handle parameters without values (like ?withoutarg)
            argv.push_back("--" + key);
        }
        else
        {
            argv.push_back("--" + key + "=" + value);
        }
    }

    return argv;
}

connection_wrapper::connection_wrapper(const std::string & conn_str)
{
    is_readonly = false;
    auto [path, params] = parse_connection_string(conn_str);

#if USE_CLIENT_AI
    applyAIParams(params);
#endif

    auto argv = build_clickhouse_args(path, params);
    std::vector<char *> argv_char;
    argv_char.reserve(argv.size());
    for (auto & arg : argv)
    {
        argv_char.push_back(const_cast<char *>(arg.c_str()));
    }

    conn = CHDB::connect_chdb_with_exception(argv_char.size(), argv_char.data());
    db_path = path;
    is_memory_db = (path == ":memory:");
}

std::shared_ptr<connection_wrapper> connection_wrapper::createPython(const std::string & conn_str)
{
    auto & registry = PythonConnectionRegistry::instance();
    std::lock_guard lock(registry.mutex);
    if (registry.exiting)
        throw std::runtime_error("chDB is shutting down");

    auto result = std::make_shared<connection_wrapper>(conn_str);
    if (registry.connections.size() >= 64)
        std::erase_if(registry.connections, [](const auto & item) { return item.expired(); });
    registry.connections.push_back(result);
    return result;
}

connection_wrapper::Operation connection_wrapper::startOperation()
{
    auto & registry = PythonConnectionRegistry::instance();
    std::lock_guard registry_lock(registry.mutex);
    if (registry.exiting)
        throw std::runtime_error("chDB is shutting down");

    std::lock_guard lock(lifecycle_mutex);
    if (closing || !conn)
        throw std::runtime_error("Connection is closed");
    ++active_operations;
    return Operation(shared_from_this(), conn);
}

connection_wrapper::Operation::Operation(std::shared_ptr<connection_wrapper> owner_, chdb_connection * cell)
    : owner(std::move(owner_)), cell_(cell), previous(current_python_operation)
{
    current_python_operation = this;
}

connection_wrapper::Operation::~Operation()
{
    if (owner)
    {
        current_python_operation = previous;
        owner->finishOperation();
    }
}

void connection_wrapper::finishOperation()
{
    std::lock_guard lock(lifecycle_mutex);
    --active_operations;
    if (!active_operations)
        lifecycle_changed.notify_all();
}

bool connection_wrapper::isOpen() const
{
    std::lock_guard lock(lifecycle_mutex);
    return conn && !closing;
}

size_t connection_wrapper::activeOperationCount() const
{
    std::lock_guard lock(lifecycle_mutex);
    return active_operations;
}

bool connection_wrapper::needsExitClose() const
{
    std::lock_guard lock(lifecycle_mutex);
    return !closed;
}

bool connection_wrapper::closePythonConnectionsAtExit()
{
    auto & registry = PythonConnectionRegistry::instance();
    std::vector<std::shared_ptr<connection_wrapper>> live;
    {
        std::lock_guard lock(registry.mutex);
        registry.exiting = true;
        for (auto & entry : registry.connections)
            if (auto connection = entry.lock())
                live.push_back(std::move(connection));
        registry.connections.clear();
    }

    const bool had_live_connection = std::any_of(live.begin(), live.end(), [](const auto & connection)
    {
        return connection->needsExitClose();
    });

    {
        std::unique_lock lock(registry.mutex);
        py::gil_scoped_release release;
        registry.state_changed.wait(lock, [&registry] { return !registry.active_stateless_queries; });
        lock.unlock();
    }

    const size_t open_python_clients = static_cast<size_t>(std::count_if(live.begin(), live.end(), [](const auto & connection)
    {
        return connection->isOpen();
    }));
    {
        py::gil_scoped_release release;
        DB::EmbeddedServer::cancelPythonQueriesIfExclusive(open_python_clients);
    }

    std::exception_ptr first_error;
    for (auto & connection : live)
    {
        try
        {
            connection->close();
        }
        catch (...)
        {
            if (!first_error)
                first_error = std::current_exception();
        }
    }
    if (first_error)
        std::rethrow_exception(first_error);
    return had_live_connection;
}

connection_wrapper::~connection_wrapper()
{
    try
    {
        close();
    }
    catch (...)
    {
        /// C++ destructors must not throw.
    }
}

void connection_wrapper::close()
{
    for (auto * active = current_python_operation; active; active = active->previous)
        if (active->owner.get() == this)
            throw std::runtime_error("Cannot close a connection from its active operation");

    /// Python UDFs and readers may call back on an engine pipeline thread, not
    /// the thread that owns Operation's TLS marker. Waiting there for this
    /// connection's active query would wait for the callback itself to return.
    if (DB::CurrentThread::tryGetQueryContext())
    {
        std::lock_guard lock(lifecycle_mutex);
        if (active_operations)
            throw std::runtime_error("Cannot close a connection with active operations from a query callback");
    }

    chdb_connection * to_close = nullptr;
    {
        std::unique_lock lock(lifecycle_mutex);
        if (closed)
            return;
        if (closing)
        {
            py::gil_scoped_release release;
            lifecycle_changed.wait(lock, [this] { return closed; });
            lock.unlock();
            return;
        }
        closing = true;
        if (active_operations)
        {
            {
                py::gil_scoped_release release;
                lifecycle_changed.wait(lock, [this] { return !active_operations; });
                lock.unlock();
            }
            lock.lock();
        }
        to_close = std::exchange(conn, nullptr);
    }

    SCOPE_EXIT({
        std::lock_guard lock(lifecycle_mutex);
        closed = true;
        closing = false;
        lifecycle_changed.notify_all();
    });

    std::exception_ptr callback_error;
    try
    {
        if (to_close && progress_callback && !progress_callback.is_none())
        {
            if (auto * client = getChdbClient(*to_close))
                client->setProgressValuesCallback(nullptr);
            progress_callback = py::object{};
        }
    }
    catch (...)
    {
        callback_error = std::current_exception();
    }
#if USE_CLIENT_AI
    ai_processor.reset();
#endif
    if (to_close)
    {
        py::gil_scoped_release release;
        chdb_close_conn(to_close);
    }
    if (callback_error)
        std::rethrow_exception(callback_error);
}

void connection_wrapper::set_progress_callback(const py::object & callback)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    auto * client = getChdbClient(*conn);
    if (!client)
        return;

    if (callback.is_none())
    {
        progress_callback = py::none();
        client->setProgressValuesCallback(nullptr);
        return;
    }

    if (!PyCallable_Check(callback.ptr()))
        throw std::runtime_error("progress callback must be callable");

    progress_callback = callback;
    auto * callback_ptr = callback.ptr();
    Py_INCREF(callback_ptr);
    auto callback_holder = std::shared_ptr<void>(
        callback_ptr,
        [](void * ptr)
        {
            py::gil_scoped_acquire acquire;
            Py_DECREF(reinterpret_cast<PyObject *>(ptr));
        });
    client->setProgressValuesCallback(
        [callback_holder](const DB::ProgressValues & values, UInt64 elapsed_ns)
        {
            py::gil_scoped_acquire acquire;
            try
            {
                py::dict payload;
                payload["read_rows"] = py::int_(values.read_rows);
                payload["read_bytes"] = py::int_(values.read_bytes);
                payload["total_rows_to_read"] = py::int_(values.total_rows_to_read);
                payload["total_bytes_to_read"] = py::int_(values.total_bytes_to_read);
                payload["elapsed_ns"] = py::int_(elapsed_ns);
                payload["written_rows"] = py::int_(values.written_rows);
                payload["written_bytes"] = py::int_(values.written_bytes);
                payload["result_rows"] = py::int_(values.result_rows);
                payload["result_bytes"] = py::int_(values.result_bytes);

                py::handle callback_obj(reinterpret_cast<PyObject *>(callback_holder.get()));
                callback_obj(payload);
            }
            catch (const py::error_already_set &)
            {
                PyErr_Print();
            }
        });
}

cursor_wrapper * connection_wrapper::cursor()
{
    return new cursor_wrapper(shared_from_this());
}

void connection_wrapper::commit()
{
    // do nothing
}

query_result * connection_wrapper::query(const std::string & query_str, const std::string & format, const py::dict & params)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    if (Poco::toLower(format) == "dataframe")
        throw std::runtime_error("Unsupported output format dataframe, please use 'query_df' function");

    const auto parsed_params = parseParametersDict(params);
    auto * client = getChdbClient(*conn);
    QueryParameterGuard guard(client, parsed_params);

    CHDB::cachePythonTablesFromQuery(reinterpret_cast<chdb_conn *>(*conn), query_str);
    py::gil_scoped_release release;

    auto * result = chdb_query_n(*conn, query_str.data(), query_str.size(), format.data(), format.size());

    const auto & error_msg = CHDB::chdb_result_error_string(result);
    if (!error_msg.empty())
    {
        std::string msg_copy(error_msg);
        chdb_destroy_query_result(result);
        throw std::runtime_error(msg_copy);
    }

    return new query_result(result, false);
}

py::object connection_wrapper::query_df(const std::string & query_str, const py::dict & params)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    static const std::string format = "dataframe";

    chdb_result * result = nullptr;
    CHDB::ChunkQueryResult * chunk_result = nullptr;

    /// Destroy the result and settle the deferred decrefs on every exit,
    /// including exceptions thrown by the pandas conversion (which would
    /// otherwise leak the chunks and their borrow guards outright). The GIL
    /// is held at scope exit on all paths: the gil_scoped_release block below
    /// is destructed before this guard runs.
    SCOPE_EXIT({
        if (result)
        {
            chdb_destroy_query_result(result);
            CHDB::drainPyBorrowGuardQueue();
        }
    });

    const auto parsed_params = parseParametersDict(params);
    auto * client = getChdbClient(*conn);
    QueryParameterGuard guard(client, parsed_params);

    CHDB::cachePythonTablesFromQuery(reinterpret_cast<chdb_conn *>(*conn), query_str);

    {
        py::gil_scoped_release release;

        result = chdb_query_n(*conn, query_str.data(), query_str.size(), format.data(), format.size());

        const auto & error_msg = CHDB::chdb_result_error_string(result);
        if (!error_msg.empty())
            throw std::runtime_error(std::string(error_msg));

        if (!(chunk_result = dynamic_cast<CHDB::ChunkQueryResult *>(reinterpret_cast<CHDB::QueryResult *>(result))))
            throw std::runtime_error("Expected ChunkQueryResult for dataframe format");
    }

    CHDB::PandasDataFrameBuilder builder(*chunk_result);
    auto df = builder.getDataFrame();

    return df;
}

std::string connection_wrapper::generate_sql(const std::string & prompt)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
#if USE_CLIENT_AI
    if (!ai_config.has_value())
        ai_config = DB::AIConfiguration{};

    try
    {
        if (!ai_processor)
            ai_processor = std::make_unique<AIQueryProcessor>(conn, *ai_config);
        return ai_processor->generateSQL(prompt);
    }
    catch (const std::exception & e)
    {
        throw std::runtime_error(std::string("AI SQL generation failed: ") + e.what());
    }
#else
    (void)prompt;
    throw std::runtime_error("AI SQL generation is not available in this build. Rebuild with USE_CLIENT_AI enabled.");
#endif
}

streaming_query_result * connection_wrapper::send_query(const std::string & query_str, const std::string & format, const py::dict & params)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    const auto parsed_params = parseParametersDict(params);
    auto * client = getChdbClient(*conn);
    QueryParameterGuard guard(client, parsed_params);

    CHDB::cachePythonTablesFromQuery(reinterpret_cast<chdb_conn *>(*conn), query_str);
    py::gil_scoped_release release;
    auto * result = chdb_stream_query_n(*conn, query_str.data(), query_str.size(), format.data(), format.size());
    const auto & error_msg = CHDB::chdb_result_error_string(result);
    if (!error_msg.empty())
    {
        std::string msg_copy(error_msg);
        chdb_destroy_query_result(result);
        throw std::runtime_error(msg_copy);
    }

    return new streaming_query_result(result);
}

query_result * connection_wrapper::streaming_fetch_result(streaming_query_result * streaming_result)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    py::gil_scoped_release release;

    if (!streaming_result || !streaming_result->get_result())
        return nullptr;

    auto * result  = chdb_stream_fetch_result(*conn, streaming_result->get_result());

    const auto & error_msg = CHDB::chdb_result_error_string(result);
    if (!error_msg.empty())
    {
        std::string msg_copy(error_msg);
        chdb_destroy_query_result(result);
        throw std::runtime_error(msg_copy);
    }

    return new query_result(result, false);
}

py::object connection_wrapper::streaming_fetch_df(streaming_query_result * streaming_result)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    if (!streaming_result || !streaming_result->get_result())
        return py::none();

    chdb_result * result = nullptr;
    CHDB::DataFrameQueryResult * chunk_result = nullptr;

    SCOPE_EXIT({
        if (result)
        {
            chdb_destroy_query_result(result);
            CHDB::drainPyBorrowGuardQueue();
        }
    });

    {
        py::gil_scoped_release release;

        result = chdb_stream_fetch_result(*conn, streaming_result->get_result());

        const auto & error_msg = CHDB::chdb_result_error_string(result);
        if (!error_msg.empty())
            throw std::runtime_error(std::string(CHDB::chdb_result_error_string(result)));

        if (!(chunk_result = dynamic_cast<CHDB::DataFrameQueryResult *>(reinterpret_cast<CHDB::QueryResult*>(result))))
            throw std::runtime_error("Expected DataFrameQueryResult for dataframe format");
    }

    py::handle df_handle = chunk_result->dataframe;

    return py::reinterpret_steal<py::object>(df_handle);
}

void connection_wrapper::streaming_cancel_query(streaming_query_result * streaming_result)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    py::gil_scoped_release release;

    if (!streaming_result || !streaming_result->get_result())
        return;

    chdb_stream_cancel_query(*conn, streaming_result->get_result());
}

streaming_insert_result * connection_wrapper::send_insert(const std::string & query_str, const std::string & format)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    py::gil_scoped_release release;
    auto * stream = chdb_stream_insert_n(*conn, query_str.data(), query_str.size(), format.data(), format.size());
    auto * err = chdb_stream_insert_error(stream);
    if (err)
    {
        std::string msg_copy(err);
        chdb_destroy_insert_stream(stream);
        throw std::runtime_error(msg_copy);
    }
    return new streaming_insert_result(stream);
}

void connection_wrapper::insert_append(streaming_insert_result * ins, const py::bytes & data)
{
    auto operation = startOperation();
    if (!ins || !ins->get_stream())
        throw std::runtime_error("Invalid insert stream");

    /// Borrow the buffer under the GIL (py::bytes accessors need it), then
    /// release the GIL for the (potentially blocking, backpressured) append.
    char * buffer = nullptr;
    Py_ssize_t length = 0;
    if (PyBytes_AsStringAndSize(data.ptr(), &buffer, &length) != 0)
        throw std::runtime_error("send_insert append expects bytes");

    chdb_insert_stream stream = ins->get_stream();
    chdb_state state;
    {
        py::gil_scoped_release release;
        state = chdb_stream_append(stream, buffer, static_cast<size_t>(length));
    }
    if (state != CHDBSuccess)
    {
        auto * err = chdb_stream_insert_error(stream);
        throw std::runtime_error(err ? err : "streaming insert append failed");
    }
}

query_result * connection_wrapper::insert_done(streaming_insert_result * ins)
{
    auto operation = startOperation();
    if (!ins || !ins->get_stream())
        throw std::runtime_error("Invalid insert stream");

    chdb_result * result = nullptr;
    {
        py::gil_scoped_release release;
        result = chdb_stream_done(ins->get_stream());
    }

    const auto & error_msg = CHDB::chdb_result_error_string(result);
    if (!error_msg.empty())
    {
        std::string msg_copy(error_msg);
        chdb_destroy_query_result(result);
        throw std::runtime_error(msg_copy);
    }

    return new query_result(result, false);
}

void connection_wrapper::insert_cancel(streaming_insert_result * ins)
{
    auto operation = startOperation();
    py::gil_scoped_release release;

    if (!ins || !ins->get_stream())
        return;

    chdb_stream_cancel_insert(ins->get_stream());
}

namespace
{

/// backup_database and restore_database return nothing, so the result exists
/// only to carry an error. Raise it if there is one, and drop the result
/// either way.
void raiseOnEngineError(chdb_result * result)
{
    const auto & error_msg = CHDB::chdb_result_error_string(result);
    if (error_msg.empty())
    {
        chdb_destroy_query_result(result);
        return;
    }

    std::string msg_copy(error_msg);
    chdb_destroy_query_result(result);
    throw std::runtime_error(msg_copy);
}

}

/// close() sets conn to nullptr, so every entry point has to look before it
/// dereferences -- otherwise using a closed connection segfaults the
/// interpreter instead of raising.
void connection_wrapper::requireOpen() const
{
    if (!isOpen())
        throw std::runtime_error("Connection is closed");
}

void connection_wrapper::backup_database(
    const std::string & database, const std::string & file_path, const std::string & base_file_path)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    py::gil_scoped_release release;
    raiseOnEngineError(chdb_backup_database_n(
        *conn,
        database.data(),
        database.size(),
        file_path.data(),
        file_path.size(),
        base_file_path.empty() ? nullptr : base_file_path.data(),
        base_file_path.size()));
}

void connection_wrapper::restore_database(const std::string & database, const std::string & file_path)
{
    auto operation = startOperation();
    auto * conn = operation.cell();
    py::gil_scoped_release release;
    raiseOnEngineError(
        chdb_restore_database_n(*conn, database.data(), database.size(), file_path.data(), file_path.size()));
}

py::dict connection_wrapper::classify_query(const std::string & sql, const std::string & target_database)
{
    auto operation = startOperation();
    auto * conn = operation.cell();

    chdb_query_analysis_v1 analysis{};
    analysis.struct_size = sizeof(analysis);
    {
        py::gil_scoped_release release;
        if (chdb_classify_query_n(
                *conn,
                sql.data(),
                sql.size(),
                target_database.empty() ? nullptr : target_database.data(),
                target_database.size(),
                &analysis)
            != CHDBSuccess)
            throw std::runtime_error("Invalid or closed connection");
    }

    py::dict out;
    out["query_class"] = static_cast<chdb_query_class>(analysis.query_class);
    out["statement_count"] = analysis.statement_count;
    out["has_secrets"] = (analysis.flags & CHDB_QUERY_HAS_SECRETS) != 0;
    out["writes_only_target_database"] = (analysis.flags & CHDB_QUERY_WRITES_ONLY_TARGET_DATABASE) != 0;
    out["changes_database_lifecycle"] = (analysis.flags & CHDB_QUERY_CHANGES_DATABASE_LIFECYCLE) != 0;
    return out;
}

#if USE_CLIENT_AI
void connection_wrapper::applyAIParams(std::map<std::string, std::string> & params)
{
    DB::AIConfiguration config;

    auto consume_string = [&](const std::string & target, std::string DB::AIConfiguration::* field)
    {
        for (auto it = params.begin(); it != params.end();)
        {
            if (Poco::toLower(it->first) == target)
            {
                if (!it->second.empty())
                {
                    if ((config.*field).empty())
                        config.*field = it->second;
                }
                it = params.erase(it);
            }
            else
            {
                ++it;
            }
        }
    };

    auto consume_double = [&](const std::string & target, std::optional<double> DB::AIConfiguration::* field)
    {
        for (auto it = params.begin(); it != params.end();)
        {
            if (Poco::toLower(it->first) == target)
            {
                if (!it->second.empty())
                {
                    try
                    {
                        config.*field = std::stod(it->second);
                    }
                    catch (...)
                    {
                    }
                }
                it = params.erase(it);
            }
            else
            {
                ++it;
            }
        }
    };

    auto consume_size_t = [&](const std::string & target, size_t DB::AIConfiguration::* field)
    {
        for (auto it = params.begin(); it != params.end();)
        {
            if (Poco::toLower(it->first) == target)
            {
                if (!it->second.empty())
                {
                    try
                    {
                        config.*field = static_cast<size_t>(std::stoul(it->second));
                    }
                    catch (...)
                    {
                    }
                }
                it = params.erase(it);
            }
            else
            {
                ++it;
            }
        }
    };

    auto consume_bool = [&](const std::string & target, bool DB::AIConfiguration::* field)
    {
        for (auto it = params.begin(); it != params.end();)
        {
            if (Poco::toLower(it->first) == target)
            {
                if (!it->second.empty())
                {
                    std::string val = Poco::toLower(it->second);
                    if (val == "1" || val == "true" || val == "yes" || val == "on")
                        config.*field = true;
                    else if (val == "0" || val == "false" || val == "no" || val == "off")
                        config.*field = false;
                }
                it = params.erase(it);
            }
            else
            {
                ++it;
            }
        }
    };

    consume_string("ai_api_key", &DB::AIConfiguration::api_key);
    consume_string("ai_base_url", &DB::AIConfiguration::base_url);
    consume_string("ai_model", &DB::AIConfiguration::model);
    consume_string("ai_provider", &DB::AIConfiguration::provider);
    consume_double("ai_temperature", &DB::AIConfiguration::temperature);
    consume_size_t("ai_max_tokens", &DB::AIConfiguration::max_tokens);
    consume_size_t("ai_timeout_seconds", &DB::AIConfiguration::timeout_seconds);
    consume_string("ai_system_prompt", &DB::AIConfiguration::system_prompt);
    consume_size_t("ai_max_steps", &DB::AIConfiguration::max_steps);
    consume_bool("ai_enable_schema_access", &DB::AIConfiguration::enable_schema_access);

    ai_config = config;
}
#endif

void cursor_wrapper::execute(const std::string & query_str)
{
    auto operation = conn->startOperation();
    release_result();
    CHDB::cachePythonTablesFromQuery(reinterpret_cast<chdb_conn *>(*operation.cell()), query_str);
    // Use JSONCompactEachRowWithNamesAndTypes format for better type support
    py::gil_scoped_release release;
    current_result = chdb_query_n(*operation.cell(), query_str.data(), query_str.size(), CURSOR_DEFAULT_FORMAT, CURSOR_DEFAULT_FORMAT_LEN);
}


/// chdb_spec: Python's atexit callbacks run before C++ static destructors.
/// Close only this extension's live connections while their Python callbacks
/// and the engine's static dependencies are still valid. C hosts retain
/// ownership of their own connections and terminate the engine themselves.
static void chdb_cleanup_at_exit()
{
    auto & registry = PythonConnectionRegistry::instance();
    try
    {
        PythonExitWatchdog watchdog;
        watchdog.setPhase("chDB Python exit: waiting for active operations or connection close after 300s\n");
        const bool closed_live_connection = connection_wrapper::closePythonConnectionsAtExit();
        watchdog.setPhase("chDB Python exit: waiting for engine pool shutdown after 300s\n");
        if (closed_live_connection)
        {
            if (chdb_shutdown() != CHDBSuccess && !DB::EmbeddedServer::hasActiveClients())
                throw std::runtime_error("chDB shutdown did not finish before Python exit");
        }
        else if (!DB::EmbeddedServer::hasActiveClients()
            && (!DB::EmbeddedServer::lastCleanupSucceeded() || DB::DatabaseCatalog::isAvailable()))
            throw std::runtime_error("Context/catalog cleanup did not finish before Python exit");
        registry.exit_complete.store(true, std::memory_order_release);
    }
    catch (const std::exception & e)
    {
        std::fputs("chDB Python exit shutdown incomplete: ", stderr);
        std::fputs(e.what(), stderr);
        std::fputc('\n', stderr);
    }
    catch (...)
    {
        std::fputs("chDB Python exit shutdown incomplete: unknown error\n", stderr);
    }

    /// The JIT cache must be empty before its process-global owners die.
#if USE_EMBEDDED_COMPILER
    try
    {
        if (auto * cache = DB::CompiledExpressionCacheFactory::instance().tryGetCache())
            cache->clear();
    }
    catch (...)
    {
        // Ignore errors during cleanup at exit
    }
#endif
}
/// chdb_spec: end Python pre-static shutdown.

#ifdef CHDB_FREE_THREADING
PYBIND11_MODULE(_chdb, m, py::mod_gil_not_used())
#else
PYBIND11_MODULE(_chdb, m)
#endif
{
    m.doc() = "chDB module for query function";

    /// Register before user code can create a connection. This callback also
    /// clears the JIT cache before its static owners are destroyed.
    py::module_::import("atexit").attr("register")(py::cpp_function(&chdb_cleanup_at_exit));
    m.def("_python_exit_cleanup_complete", []
    {
        return PythonConnectionRegistry::instance().exit_complete.load(std::memory_order_acquire);
    });
    m.def("_python_active_operations", []
    {
        auto & registry = PythonConnectionRegistry::instance();
        std::lock_guard lock(registry.mutex);
        size_t active = 0;
        for (const auto & entry : registry.connections)
            if (auto connection = entry.lock())
                active += connection->activeOperationCount();
        return active;
    });

    /// py::arithmetic() is what makes the documented ordering usable: without
    /// it a pybind enum has no __lt__, and max() over a batch's classes fails.
    py::enum_<chdb_query_class>(
        m,
        "query_class",
        "What a statement does to state that outlives it, as decided by the ClickHouse parser. "
        "The members compare in ascending order of restriction, so a batch of statements "
        "classifies as max() over its members",
        py::arithmetic())
        .value("READ_ONLY", CHDB_QUERY_READ_ONLY, "Leaves no trace: SELECT, SHOW, DESCRIBE, EXPLAIN, EXISTS, CHECK")
        .value(
            "MUTATING",
            CHDB_QUERY_MUTATING,
            "Changes a database, and BACKUP DATABASE captures the change: INSERT, CREATE, ALTER, "
            "DROP, TRUNCATE, RENAME, UPDATE, DELETE, OPTIMIZE")
        .value(
            "MUTATING_GLOBAL",
            CHDB_QUERY_MUTATING_GLOBAL,
            "Persistent and worth replaying, but outside every database a checkpoint could "
            "capture: global UDFs, named collections, workloads, resources, access management, "
            "writes into the system database")
        .value(
            "CONTROL",
            CHDB_QUERY_CONTROL,
            "Session-scoped, so replaying is meaningless, or not something to issue through a "
            "managed connection: USE, SET, ATTACH, DETACH, SYSTEM, BACKUP, RESTORE, KILL, "
            "transactions, and writes outside the engine such as INTO OUTFILE")
        .value("UNKNOWN", CHDB_QUERY_UNKNOWN, "Did not parse, or parsed into a statement this version does not classify");

    py::class_<memoryview_wrapper>(m, "memoryview_wrapper")
        .def(py::init<std::shared_ptr<local_result_wrapper>>(), py::return_value_policy::take_ownership)
        .def("tobytes", &memoryview_wrapper::bytes)
        .def("__len__", &memoryview_wrapper::size)
        .def("size", &memoryview_wrapper::size)
        .def("release", &memoryview_wrapper::release)
        .def("view", &memoryview_wrapper::view);

    py::class_<query_result>(m, "query_result")
        .def(py::init<chdb_result *>(), py::return_value_policy::take_ownership)
        /// Bound to the length-aware accessor on purpose. Binding query_result::data
        /// (a char *) makes pybind11's type_caster<char> call strlen() on a buffer that
        /// is exactly chdb_result_length() bytes and carries no terminator, which reads
        /// out of bounds (AddressSanitizer: container-overflow). Same Python type as
        /// before -- str -- just built from the length the engine reports.
        .def("data", &query_result::str)
        .def("bytes", &query_result::bytes)
        .def("__str__", &query_result::str)
        .def("__len__", &query_result::size)
        .def("__repr__", &query_result::str)
        .def("show", [](query_result & self) { py::print(self); })
        .def("size", &query_result::size)
        .def("rows_read", &query_result::rows_read)
        .def("bytes_read", &query_result::bytes_read)
        .def("storage_rows_read", &query_result::storage_rows_read)
        .def("storage_bytes_read", &query_result::storage_bytes_read)
        .def("elapsed", &query_result::elapsed)
        .def("rows_written", &query_result::rows_written)
        .def("bytes_written", &query_result::bytes_written)
        .def("get_memview", &query_result::get_memview)
        .def("has_error", &query_result::has_error)
        .def("error_message", &query_result::error_message)
        .def(
            "__arrow_c_stream__",
            [](query_result & self, py::object /*requested_schema*/)
            {
                if (self.has_error())
                    throw std::invalid_argument("query result carries an error: " + self.error_message().cast<std::string>());
                return CHDB::exportArrowIPCAsCapsule(
                    self.data(), self.size(), std::static_pointer_cast<void>(self.get_result_wrapper()));
            },
            py::arg("requested_schema") = py::none(),
            "Arrow PyCapsule interface: export the result as an ArrowArrayStream capsule.\n"
            "Requires the query to have produced Arrow IPC output, i.e. output format\n"
            "\"Arrow\" or \"ArrowStream\". The exported record batches reference the\n"
            "result buffer directly (no copy) and keep the result alive until released.\n"
            "Can be called multiple times; each call yields an independent stream.");

    py::class_<streaming_query_result>(m, "streaming_query_result")
        .def(py::init<chdb_result *>(), py::return_value_policy::take_ownership)
        .def("has_error", &streaming_query_result::has_error)
        .def("error_message", &streaming_query_result::error_message);

    py::class_<streaming_insert_result>(m, "streaming_insert_result")
        .def(py::init<chdb_insert_stream>(), py::return_value_policy::take_ownership)
        .def("has_error", &streaming_insert_result::has_error)
        .def("error_message", &streaming_insert_result::error_message);

    py::class_<DB::PyReader, std::shared_ptr<DB::PyReader>>(m, "PyReader")
        .def(
            py::init<const py::object &>(),
            "Initialize the reader with data. The exact type and structure of `data` can vary."
            "you must hold the data with `self.data` in your inherit class\n\n"
            "Args:\n"
            "    data (Any): The data with which to initialize the reader, format and type are not strictly defined.")
        .def(
            "read",
            [](DB::PyReader & self, const std::vector<std::string> & col_names, int count)
            {
                // GIL is held when called from Python code. Release it to avoid deadlock
                py::gil_scoped_release release;
                return std::move(self.read(col_names, count));
            },
            "Read a specified number of rows from the given columns and return a list of objects, "
            "where each object is a sequence of values for a column.\n\n"
            "Args:\n"
            "    col_names (List[str]): List of column names to read.\n"
            "    count (int): Maximum number of rows to read.\n\n"
            "Returns:\n"
            "    List[Any]: List of sequences, one for each column.")
        .def(
            "get_schema",
            &DB::PyReader::getSchema,
            "Return a list of column names and their types.\n\n"
            "Returns:\n"
            "    List[str, str]: List of column name and type pairs.");

    py::class_<cursor_wrapper>(m, "cursor")
        .def(py::init<std::shared_ptr<connection_wrapper>>())
        .def("execute", &cursor_wrapper::execute)
        .def("commit", &cursor_wrapper::commit)
        .def("close", &cursor_wrapper::close)
        .def("get_memview", &cursor_wrapper::get_memview)
        .def("data_size", &cursor_wrapper::data_size)
        .def("rows_read", &cursor_wrapper::rows_read)
        .def("bytes_read", &cursor_wrapper::bytes_read)
        .def("storage_rows_read", &cursor_wrapper::storage_rows_read)
        .def("storage_bytes_read", &cursor_wrapper::storage_bytes_read)
        .def("elapsed", &cursor_wrapper::elapsed)
        .def("has_error", &cursor_wrapper::has_error)
        .def("error_message", &cursor_wrapper::error_message);

    py::class_<connection_wrapper, std::shared_ptr<connection_wrapper>>(m, "connect")
        .def(py::init(&connection_wrapper::createPython), py::arg("path") = ":memory:")
        .def("cursor", &connection_wrapper::cursor)
        .def("execute", &connection_wrapper::query)
        .def("commit", &connection_wrapper::commit)
        .def("close", &connection_wrapper::close)
        .def(
            "set_progress_callback",
            &connection_wrapper::set_progress_callback,
            py::arg("callback") = py::none(),
            "Register a progress callback to receive progress values during query execution")
        .def(
            "query",
            &connection_wrapper::query,
            py::arg("query_str"),
            py::arg("format") = "CSV",
            py::kw_only(),
            py::arg("params") = py::dict(),
            "Execute a query and return a query_result object")
        .def(
            "query_df",
            &connection_wrapper::query_df,
            py::arg("query_str"),
            py::kw_only(),
            py::arg("params") = py::dict(),
            "Execute a query and return a DataFrame")
#if USE_CLIENT_AI
        .def(
            "generate_sql",
            &connection_wrapper::generate_sql,
            py::arg("prompt"),
            "Generate SQL text from a natural language prompt using the configured AI provider")
#endif
        .def(
            "send_query",
            &connection_wrapper::send_query,
            py::arg("query_str"),
            py::arg("format") = "CSV",
            py::kw_only(),
            py::arg("params") = py::dict(),
            "Send a streaming query and return a streaming query result object")
        .def(
            "streaming_fetch_result",
            &connection_wrapper::streaming_fetch_result,
                py::arg("streaming_result"),
                "Fetches a data chunk from the streaming result. This function should be called repeatedly until the result is exhausted")
        .def(
            "streaming_fetch_df",
            &connection_wrapper::streaming_fetch_df,
                py::arg("streaming_result"),
                "Fetches a DataFrame from the streaming result. This function should be called repeatedly until the result is exhausted")
        .def(
            "streaming_cancel_query",
            &connection_wrapper::streaming_cancel_query,
            py::arg("streaming_result"),
            "Cancel a streaming query")
        .def(
            "send_insert",
            &connection_wrapper::send_insert,
            py::arg("query_str"),
            py::arg("format") = "CSV",
            "Begin a streaming INSERT and return a streaming_insert_result handle")
        .def(
            "insert_append",
            &connection_wrapper::insert_append,
            py::arg("insert_result"),
            py::arg("data"),
            "Append a chunk of FORMAT-encoded bytes to a streaming INSERT")
        .def(
            "insert_done",
            &connection_wrapper::insert_done,
            py::arg("insert_result"),
            "Finalize a streaming INSERT and return a query_result with write stats")
        .def(
            "insert_cancel",
            &connection_wrapper::insert_cancel,
            py::arg("insert_result"),
            "Abort a streaming INSERT without committing")
        .def(
            "backup_database",
            &connection_wrapper::backup_database,
            py::arg("database"),
            py::arg("file_path"),
            py::arg("base_file_path") = "",
            "Back a database up into file_path. The engine quotes both arguments, so neither can "
            "alter the statement. file_path must be absolute, its directory must exist and be "
            "covered by backups.allowed_path, and an existing file is never overwritten. Pass "
            "base_file_path to write only what changed since that archive")
        .def(
            "restore_database",
            &connection_wrapper::restore_database,
            py::arg("database"),
            py::arg("file_path"),
            "Restore a database from an archive written by backup_database. Leaves the current "
            "database of the session unchanged")
        .def(
            "classify_query",
            &connection_wrapper::classify_query,
            py::arg("sql"),
            py::arg("target_database") = "",
            "Report what the SQL would do, without running it, as a dict of query_class, "
            "statement_count, has_secrets, writes_only_target_database and "
            "changes_database_lifecycle. A batch takes the most restricted class among its "
            "members; SQL that does not parse reports UNKNOWN with a count of zero and no flags. "
            "target_database is the database the caller owns, and is what "
            "writes_only_target_database is judged against; unqualified names resolve through the "
            "connection's current database first");

    CHDB::ChdbPyType::initialize(m);
    CHDB::PythonUDFRegistry::instance();
    CHDB::PythonUDAFRegistry::instance();
    CHDB::PyDateTimeHelper::initialize();
    CHDB::registerGlobalFunctions(m);

    m.def(
        "query",
        &query,
        py::arg("queryStr"),
        py::arg("output_format") = "CSV",
        py::kw_only(),
        py::arg("path") = "",
        py::arg("udf_path") = "",
        py::arg("params") = py::dict(),
        "Query chDB and return a query_result object or DataFrame");

    m.def(
        "set_signal_handlers_enabled",
        [](int enabled) { chdb_set_signal_handlers_enabled(enabled); },
        py::arg("enabled"),
        "Enable (1) or disable (0) chDB process-wide signal handler installation. "
        "Must be called BEFORE chdb_connect() or query() to take effect.");

    m.def(
        "reset_signal_handlers",
        []() { chdb_reset_signal_handlers(); },
        "Reset all signal handlers installed by chDB back to SIG_DFL.");

    auto destroy_import_cache = []()
    {
        CHDB::PythonUDFRegistry::instance().clear();
        CHDB::PythonUDAFRegistry::instance().clear();
        CHDB::PythonImporter::destroy();
    };
    m.add_object("_destroy_import_cache", py::capsule(destroy_import_cache));
}
