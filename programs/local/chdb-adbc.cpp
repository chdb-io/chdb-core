/// ADBC (Arrow Database Connectivity) driver entrypoint for libchdb.
///
/// Implements the AdbcDatabase / AdbcConnection / AdbcStatement contract on
/// top of the public chdb C API, so that libchdb itself is loadable as an
/// ADBC driver by the standard driver managers (Python adbc_driver_manager,
/// Go drivermgr, R adbcdrivermanager, ...):
///
///   driver     = /path/to/libchdb.so
///   entrypoint = chdb_adbc_init
///
/// Results travel through the Arrow C Data Interface (chdb_query_arrow),
/// bulk ingestion goes through chdb_arrow_scan + INSERT ... SELECT FROM
/// arrowstream(...). See https://github.com/chdb-io/chdb-core/issues/122.
///
/// Scope notes (kept in sync with the issue's open questions):
///   - autocommit-only: Commit/Rollback return INVALID_STATE, disabling
///     autocommit returns NOT_IMPLEMENTED.
///   - one storage path per process: a second AdbcDatabase with a different
///     path fails at ConnectionInit with the engine's error.
///   - GetObjects / partitioned execution / Substrait: NOT_IMPLEMENTED.

#include "chdb.h"

#include <arrow/c/abi.h>

#include "adbc/adbc.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <Common/config_version.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace
{

/// ---------------------------------------------------------------------
/// Error helpers
/// ---------------------------------------------------------------------

void releaseAdbcError(AdbcError * error)
{
    if (!error)
        return;
    delete[] error->message;
    error->message = nullptr;
    error->release = nullptr;
}

AdbcStatusCode setError(AdbcError * error, AdbcStatusCode code, const std::string & message)
{
    if (error)
    {
        if (error->release)
            error->release(error);
        error->message = new char[message.size() + 1];
        std::memcpy(error->message, message.c_str(), message.size() + 1);
        error->vendor_code = 0;
        std::memset(error->sqlstate, 0, sizeof(error->sqlstate));
        error->release = releaseAdbcError;
    }
    return code;
}

AdbcStatusCode notImplemented(AdbcError * error, const std::string & what)
{
    return setError(error, ADBC_STATUS_NOT_IMPLEMENTED, "[chdb] " + what + " is not implemented");
}

/// ---------------------------------------------------------------------
/// Private handle state
/// ---------------------------------------------------------------------

struct DatabaseImpl
{
    /// Database path (":memory:" by default) plus extra engine arguments
    /// collected from "chdb."-prefixed options.
    std::string path = ":memory:";
    std::vector<std::string> extra_args;
};

struct ConnectionImpl
{
    /// Handle returned by chdb_connect(); the engine instance is shared,
    /// ref-counted process state (one storage path per process).
    chdb_connection * conn = nullptr;
    /// Serializes statement execution on this connection.
    std::mutex mutex;
};

struct StatementImpl
{
    ConnectionImpl * connection = nullptr;
    std::string query;
    bool has_query = false;

    /// Bulk ingestion state (ADBC_INGEST_OPTION_*).
    std::string ingest_table;
    std::string ingest_db_schema;
    std::string ingest_mode = ADBC_INGEST_OPTION_MODE_CREATE;

    /// Data bound via StatementBind / StatementBindStream, moved into the
    /// statement (C Data Interface move semantics).
    ArrowArrayStream bound_stream;
    bool has_bound_stream = false;

    void releaseBoundStream()
    {
        if (has_bound_stream && bound_stream.release)
            bound_stream.release(&bound_stream);
        has_bound_stream = false;
        std::memset(&bound_stream, 0, sizeof(bound_stream));
    }
};

std::atomic<uint64_t> ingest_name_counter{0};

/// ---------------------------------------------------------------------
/// Small utilities
/// ---------------------------------------------------------------------

/// Quotes an identifier with backticks, ClickHouse-style.
std::string quoteIdentifier(const std::string & name)
{
    std::string quoted = "`";
    for (char c : name)
    {
        if (c == '`' || c == '\\')
            quoted += '\\';
        quoted += c;
    }
    quoted += '`';
    return quoted;
}

/// Consumes a chdb_result: on error fills the AdbcError and returns a
/// non-OK status; otherwise reports rows written through rows_affected.
AdbcStatusCode consumeResult(chdb_result * result, int64_t * rows_affected, AdbcError * error)
{
    if (!result)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] query returned no result handle");

    const char * err = chdb_result_error(result);
    if (err)
    {
        std::string message(err);
        chdb_destroy_query_result(result);
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + message);
    }

    if (rows_affected)
    {
        uint64_t written = chdb_result_rows_written(result);
        *rows_affected = written > 0 ? static_cast<int64_t>(written) : -1;
    }
    chdb_destroy_query_result(result);
    return ADBC_STATUS_OK;
}

/// Runs a query whose output is discarded (DDL / INSERT ... SELECT).
AdbcStatusCode runUpdateQuery(
    ConnectionImpl * connection, const std::string & sql, int64_t * rows_affected, AdbcError * error)
{
    chdb_result * result = chdb_query_n(*connection->conn, sql.c_str(), sql.size(), "Null", 4);
    return consumeResult(result, rows_affected, error);
}

/// Runs a scalar query and returns the first line of CSV output.
AdbcStatusCode runScalarQuery(
    ConnectionImpl * connection, const std::string & sql, std::string & value, AdbcError * error)
{
    chdb_result * result = chdb_query_n(*connection->conn, sql.c_str(), sql.size(), "CSV", 3);
    if (!result)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] query returned no result handle");
    const char * err = chdb_result_error(result);
    if (err)
    {
        std::string message(err);
        chdb_destroy_query_result(result);
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + message);
    }
    value.assign(chdb_result_buffer(result), chdb_result_length(result));
    while (!value.empty() && (value.back() == '\n' || value.back() == '\r'))
        value.pop_back();
    chdb_destroy_query_result(result);
    return ADBC_STATUS_OK;
}

/// Exports record batches as an ArrowArrayStream via arrow C++.
AdbcStatusCode exportBatch(
    const std::shared_ptr<arrow::Schema> & schema,
    const std::shared_ptr<arrow::RecordBatch> & batch,
    ArrowArrayStream * out,
    AdbcError * error)
{
    auto reader_result = arrow::RecordBatchReader::Make({batch}, schema);
    if (!reader_result.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + reader_result.status().ToString());
    auto status = arrow::ExportRecordBatchReader(reader_result.ValueUnsafe(), out);
    if (!status.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + status.ToString());
    return ADBC_STATUS_OK;
}

/// ---------------------------------------------------------------------
/// Database functions
/// ---------------------------------------------------------------------

AdbcStatusCode chdbDatabaseNew(AdbcDatabase * database, AdbcError * error)
{
    if (!database)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] database is null");
    database->private_data = new DatabaseImpl();
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbDatabaseSetOption(
    AdbcDatabase * database, const char * key, const char * value, AdbcError * error)
{
    if (!database || !database->private_data || !key)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] database is not allocated");
    auto * impl = static_cast<DatabaseImpl *>(database->private_data);

    std::string option(key);
    std::string option_value(value ? value : "");
    if (option == "path" || option == "uri")
    {
        constexpr const char * file_prefix = "file://";
        if (option_value.rfind(file_prefix, 0) == 0)
            option_value = option_value.substr(std::strlen(file_prefix));
        impl->path = option_value.empty() ? ":memory:" : option_value;
        return ADBC_STATUS_OK;
    }
    /// "chdb.<flag>" passes through as an engine argument "--<flag>=<value>".
    constexpr const char * chdb_prefix = "chdb.";
    if (option.rfind(chdb_prefix, 0) == 0)
    {
        impl->extra_args.push_back("--" + option.substr(std::strlen(chdb_prefix)) + "=" + option_value);
        return ADBC_STATUS_OK;
    }
    return notImplemented(error, "database option '" + option + "'");
}

AdbcStatusCode chdbDatabaseInit(AdbcDatabase * database, AdbcError * error)
{
    if (!database || !database->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] database is not allocated");
    /// Engine startup is deferred to ConnectionInit (chdb_connect).
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbDatabaseRelease(AdbcDatabase * database, AdbcError * error)
{
    if (!database || !database->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] database is not allocated");
    delete static_cast<DatabaseImpl *>(database->private_data);
    database->private_data = nullptr;
    return ADBC_STATUS_OK;
}

/// ---------------------------------------------------------------------
/// Connection functions
/// ---------------------------------------------------------------------

AdbcStatusCode chdbConnectionNew(AdbcConnection * connection, AdbcError * error)
{
    if (!connection)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] connection is null");
    connection->private_data = new ConnectionImpl();
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbConnectionSetOption(
    AdbcConnection * connection, const char * key, const char * value, AdbcError * error)
{
    if (!connection || !connection->private_data || !key)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not allocated");
    if (std::strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0)
    {
        if (value && std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0)
            return ADBC_STATUS_OK; /// autocommit is the only supported mode
        return notImplemented(error, "disabling autocommit (ClickHouse has no classic transactions)");
    }
    return notImplemented(error, std::string("connection option '") + key + "'");
}

AdbcStatusCode chdbConnectionInit(
    AdbcConnection * connection, AdbcDatabase * database, AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not allocated");
    if (!database || !database->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] database is not initialized");

    auto * impl = static_cast<ConnectionImpl *>(connection->private_data);
    auto * db = static_cast<DatabaseImpl *>(database->private_data);

    /// ":memory:" is the engine default and must NOT be passed as --path:
    /// an explicit --path value is treated as a directory name literally.
    std::vector<std::string> args = {"chdb"};
    if (db->path != ":memory:")
        args.push_back("--path=" + db->path);
    args.insert(args.end(), db->extra_args.begin(), db->extra_args.end());
    std::vector<char *> argv;
    argv.reserve(args.size());
    for (auto & arg : args)
        argv.push_back(arg.data());

    impl->conn = chdb_connect(static_cast<int>(argv.size()), argv.data());
    if (!impl->conn)
        return setError(
            error,
            ADBC_STATUS_IO,
            "[chdb] chdb_connect failed for path '" + db->path
                + "' (note: all connections in a process must share one storage path)");
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbConnectionRelease(AdbcConnection * connection, AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not allocated");
    auto * impl = static_cast<ConnectionImpl *>(connection->private_data);
    if (impl->conn)
        chdb_close_conn(impl->conn);
    delete impl;
    connection->private_data = nullptr;
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbConnectionCommit(AdbcConnection *, AdbcError * error)
{
    return setError(
        error, ADBC_STATUS_INVALID_STATE, "[chdb] no active transaction: connections are autocommit-only");
}

AdbcStatusCode chdbConnectionRollback(AdbcConnection *, AdbcError * error)
{
    return setError(
        error, ADBC_STATUS_INVALID_STATE, "[chdb] no active transaction: connections are autocommit-only");
}

AdbcStatusCode chdbConnectionGetInfo(
    AdbcConnection * connection,
    const uint32_t * info_codes,
    size_t info_codes_length,
    ArrowArrayStream * out,
    AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (!out)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] out stream is null");

    struct InfoValue
    {
        uint32_t code;
        bool is_string;
        std::string string_value;
        int64_t int_value;
    };
    std::vector<InfoValue> all = {
        {ADBC_INFO_VENDOR_NAME, true, "ClickHouse", 0},
        {ADBC_INFO_VENDOR_VERSION, true, VERSION_STRING, 0},
        {ADBC_INFO_DRIVER_NAME, true, "ADBC chDB Driver", 0},
        {ADBC_INFO_DRIVER_VERSION, true, VERSION_STRING, 0},
        {ADBC_INFO_DRIVER_ADBC_VERSION, false, "", ADBC_VERSION_1_1_0},
    };

    std::vector<InfoValue> selected;
    if (!info_codes)
        selected = all;
    else
        for (size_t i = 0; i < info_codes_length; ++i)
            for (const auto & info : all)
                if (info.code == info_codes[i])
                    selected.push_back(info);

    /// Result schema mandated by the ADBC spec: info_name uint32 not null,
    /// info_value dense_union<string_value, bool_value, int64_value,
    /// int32_bitmask, string_list, int32_to_int32_list_map>.
    arrow::UInt32Builder name_builder;
    auto string_builder = std::make_shared<arrow::StringBuilder>();
    auto bool_builder = std::make_shared<arrow::BooleanBuilder>();
    auto int64_builder = std::make_shared<arrow::Int64Builder>();
    auto bitmask_builder = std::make_shared<arrow::Int32Builder>();
    auto list_builder = std::make_shared<arrow::ListBuilder>(
        arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>());
    auto map_builder = std::make_shared<arrow::MapBuilder>(
        arrow::default_memory_pool(),
        std::make_shared<arrow::Int32Builder>(),
        std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), std::make_shared<arrow::Int32Builder>()));

    arrow::DenseUnionBuilder value_builder(arrow::default_memory_pool());
    const int8_t string_code = value_builder.AppendChild(string_builder, "string_value");
    value_builder.AppendChild(bool_builder, "bool_value");
    const int8_t int64_code = value_builder.AppendChild(int64_builder, "int64_value");
    value_builder.AppendChild(bitmask_builder, "int32_bitmask");
    value_builder.AppendChild(list_builder, "string_list");
    value_builder.AppendChild(map_builder, "int32_to_int32_list_map");

    auto check = [&](const arrow::Status & status) { return status.ok(); };
    for (const auto & info : selected)
    {
        if (!check(name_builder.Append(info.code)))
            return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetInfo result");
        if (info.is_string)
        {
            if (!check(value_builder.Append(string_code)) || !check(string_builder->Append(info.string_value)))
                return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetInfo result");
        }
        else
        {
            if (!check(value_builder.Append(int64_code)) || !check(int64_builder->Append(info.int_value)))
                return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetInfo result");
        }
    }

    std::shared_ptr<arrow::Array> names;
    std::shared_ptr<arrow::Array> values;
    if (!check(name_builder.Finish(&names)) || !check(value_builder.Finish(&values)))
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetInfo result");

    auto schema = arrow::schema(
        {arrow::field("info_name", arrow::uint32(), /*nullable=*/false),
         arrow::field("info_value", values->type())});
    auto batch = arrow::RecordBatch::Make(schema, names->length(), {names, values});
    return exportBatch(schema, batch, out, error);
}

AdbcStatusCode chdbConnectionGetObjects(
    AdbcConnection *, int, const char *, const char *, const char *, const char **, const char *,
    ArrowArrayStream *, AdbcError * error)
{
    return notImplemented(error, "ConnectionGetObjects");
}

AdbcStatusCode chdbConnectionGetTableSchema(
    AdbcConnection * connection,
    const char * catalog,
    const char * db_schema,
    const char * table_name,
    ArrowSchema * schema,
    AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    auto * impl = static_cast<ConnectionImpl *>(connection->private_data);
    if (!impl->conn)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (!table_name || !schema)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] table_name/schema is null");
    if (catalog && *catalog)
        return notImplemented(error, "catalogs (ClickHouse has databases only)");

    std::string qualified = quoteIdentifier(table_name);
    if (db_schema && *db_schema)
        qualified = quoteIdentifier(db_schema) + "." + qualified;
    std::string sql = "SELECT * FROM " + qualified + " WHERE 0";

    std::lock_guard lock(impl->mutex);
    ArrowArrayStream stream;
    std::memset(&stream, 0, sizeof(stream));
    chdb_result * result = chdb_query_arrow_n(
        *impl->conn, sql.c_str(), sql.size(), reinterpret_cast<chdb_arrow_stream>(&stream), nullptr);
    AdbcStatusCode status = consumeResult(result, nullptr, error);
    if (status != ADBC_STATUS_OK)
        return status;

    if (!stream.release || stream.get_schema(&stream, schema) != 0)
    {
        if (stream.release)
            stream.release(&stream);
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to read schema of " + qualified);
    }
    stream.release(&stream);
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbConnectionGetTableTypes(
    AdbcConnection * connection, ArrowArrayStream * out, AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (!out)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] out stream is null");

    arrow::StringBuilder builder;
    std::shared_ptr<arrow::Array> types;
    if (!builder.Append("BASE TABLE").ok() || !builder.Append("VIEW").ok() || !builder.Finish(&types).ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetTableTypes result");

    auto schema = arrow::schema({arrow::field("table_type", arrow::utf8(), /*nullable=*/false)});
    auto batch = arrow::RecordBatch::Make(schema, types->length(), {types});
    return exportBatch(schema, batch, out, error);
}

AdbcStatusCode chdbConnectionReadPartition(
    AdbcConnection *, const uint8_t *, size_t, ArrowArrayStream *, AdbcError * error)
{
    return notImplemented(error, "ConnectionReadPartition");
}

/// ---------------------------------------------------------------------
/// Statement functions
/// ---------------------------------------------------------------------

AdbcStatusCode chdbStatementNew(
    AdbcConnection * connection, AdbcStatement * statement, AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (!statement)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] statement is null");
    auto * impl = new StatementImpl();
    impl->connection = static_cast<ConnectionImpl *>(connection->private_data);
    std::memset(&impl->bound_stream, 0, sizeof(impl->bound_stream));
    statement->private_data = impl;
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbStatementRelease(AdbcStatement * statement, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    impl->releaseBoundStream();
    delete impl;
    statement->private_data = nullptr;
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbStatementSetSqlQuery(
    AdbcStatement * statement, const char * query, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    if (!query)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] query is null");
    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    impl->query = query;
    impl->has_query = true;
    impl->ingest_table.clear();
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbStatementSetOption(
    AdbcStatement * statement, const char * key, const char * value, AdbcError * error)
{
    if (!statement || !statement->private_data || !key)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    auto * impl = static_cast<StatementImpl *>(statement->private_data);

    if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0)
    {
        impl->ingest_table = value ? value : "";
        impl->has_query = false;
        return ADBC_STATUS_OK;
    }
    if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) == 0)
    {
        impl->ingest_db_schema = value ? value : "";
        return ADBC_STATUS_OK;
    }
    if (std::strcmp(key, ADBC_INGEST_OPTION_MODE) == 0)
    {
        std::string mode(value ? value : "");
        if (mode != ADBC_INGEST_OPTION_MODE_CREATE && mode != ADBC_INGEST_OPTION_MODE_APPEND
            && mode != ADBC_INGEST_OPTION_MODE_REPLACE && mode != ADBC_INGEST_OPTION_MODE_CREATE_APPEND)
            return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] unknown ingest mode '" + mode + "'");
        impl->ingest_mode = mode;
        return ADBC_STATUS_OK;
    }
    return notImplemented(error, std::string("statement option '") + key + "'");
}

AdbcStatusCode chdbStatementPrepare(AdbcStatement * statement, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    /// Queries execute in one shot; there is no server-side prepared state.
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbStatementBind(
    AdbcStatement * statement, ArrowArray * values, ArrowSchema * schema, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    if (!values || !schema)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] values/schema is null");

    /// Import the single batch (taking ownership, C Data move semantics)
    /// and re-export it as a one-batch stream.
    auto imported_schema = arrow::ImportSchema(schema);
    if (!imported_schema.ok())
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] " + imported_schema.status().ToString());
    auto batch = arrow::ImportRecordBatch(values, imported_schema.ValueUnsafe());
    if (!batch.ok())
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] " + batch.status().ToString());
    auto reader = arrow::RecordBatchReader::Make({batch.ValueUnsafe()}, imported_schema.ValueUnsafe());
    if (!reader.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + reader.status().ToString());

    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    impl->releaseBoundStream();
    auto status = arrow::ExportRecordBatchReader(reader.ValueUnsafe(), &impl->bound_stream);
    if (!status.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + status.ToString());
    impl->has_bound_stream = true;
    return ADBC_STATUS_OK;
}

AdbcStatusCode chdbStatementBindStream(
    AdbcStatement * statement, ArrowArrayStream * stream, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    if (!stream || !stream->release)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] stream is null or released");
    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    impl->releaseBoundStream();
    impl->bound_stream = *stream;
    stream->release = nullptr; /// moved
    impl->has_bound_stream = true;
    return ADBC_STATUS_OK;
}

AdbcStatusCode executeIngest(StatementImpl * impl, int64_t * rows_affected, AdbcError * error)
{
    if (!impl->has_bound_stream)
        return setError(
            error, ADBC_STATUS_INVALID_STATE, "[chdb] bulk ingestion requires bound data (Bind/BindStream)");

    std::string qualified = quoteIdentifier(impl->ingest_table);
    if (!impl->ingest_db_schema.empty())
        qualified = quoteIdentifier(impl->ingest_db_schema) + "." + qualified;

    const std::string reg_name = "adbc_ingest_" + std::to_string(ingest_name_counter.fetch_add(1));
    if (chdb_arrow_scan(
            *impl->connection->conn,
            reg_name.c_str(),
            reinterpret_cast<chdb_arrow_stream>(&impl->bound_stream))
        != CHDBSuccess)
    {
        impl->releaseBoundStream();
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to register bound Arrow stream");
    }

    const std::string create_sql = "CREATE TABLE " + qualified
        + " ENGINE = MergeTree() ORDER BY tuple() AS SELECT * FROM arrowstream(" + reg_name + ")";
    const std::string insert_sql = "INSERT INTO " + qualified + " SELECT * FROM arrowstream(" + reg_name + ")";

    AdbcStatusCode status = ADBC_STATUS_OK;
    if (impl->ingest_mode == ADBC_INGEST_OPTION_MODE_CREATE)
    {
        status = runUpdateQuery(impl->connection, create_sql, rows_affected, error);
    }
    else if (impl->ingest_mode == ADBC_INGEST_OPTION_MODE_REPLACE)
    {
        status = runUpdateQuery(impl->connection, "DROP TABLE IF EXISTS " + qualified, nullptr, error);
        if (status == ADBC_STATUS_OK)
            status = runUpdateQuery(impl->connection, create_sql, rows_affected, error);
    }
    else if (impl->ingest_mode == ADBC_INGEST_OPTION_MODE_APPEND)
    {
        status = runUpdateQuery(impl->connection, insert_sql, rows_affected, error);
    }
    else /// create_append
    {
        std::string exists;
        status = runScalarQuery(impl->connection, "EXISTS TABLE " + qualified, exists, error);
        if (status == ADBC_STATUS_OK)
            status = runUpdateQuery(
                impl->connection, exists == "1" ? insert_sql : create_sql, rows_affected, error);
    }

    chdb_arrow_unregister_table(*impl->connection->conn, reg_name.c_str());
    impl->releaseBoundStream();
    return status;
}

AdbcStatusCode chdbStatementExecuteQuery(
    AdbcStatement * statement, ArrowArrayStream * out, int64_t * rows_affected, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    if (!impl->connection || !impl->connection->conn)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (rows_affected)
        *rows_affected = -1;

    std::lock_guard lock(impl->connection->mutex);

    if (!impl->ingest_table.empty())
    {
        if (out)
            return setError(
                error,
                ADBC_STATUS_INVALID_ARGUMENT,
                "[chdb] bulk ingestion returns no result set; use ExecuteUpdate semantics (out must be null)");
        return executeIngest(impl, rows_affected, error);
    }

    if (!impl->has_query)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] no query set (SetSqlQuery)");
    if (impl->has_bound_stream)
        return notImplemented(
            error,
            "binding parameters to SQL queries (chdb supports server-side named parameters; "
            "ADBC positional bind mapping is tracked in chdb-io/chdb-core#122)");

    if (!out)
        return runUpdateQuery(impl->connection, impl->query, rows_affected, error);

    chdb_result * result = chdb_query_arrow_n(
        *impl->connection->conn,
        impl->query.c_str(),
        impl->query.size(),
        reinterpret_cast<chdb_arrow_stream>(out),
        nullptr);

    /// Statements without a result set (DDL, SET, ...) execute successfully
    /// but leave the chunk collector without a header; chdb_query_arrow_n
    /// reports that as an error (chdb-arrow-output.cpp). ADBC consumers
    /// (e.g. DB-API cursor.execute) still expect a stream, so hand back an
    /// empty zero-column result instead.
    if (result)
    {
        const char * err = chdb_result_error(result);
        if (err && std::strcmp(err, "Missing result header for Arrow output") == 0)
        {
            chdb_destroy_query_result(result);
            auto schema = arrow::schema(arrow::FieldVector{});
            auto batch = arrow::RecordBatch::Make(schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
            return exportBatch(schema, batch, out, error);
        }
    }
    return consumeResult(result, nullptr, error);
}

AdbcStatusCode chdbStatementExecutePartitions(
    AdbcStatement *, ArrowSchema *, AdbcPartitions *, int64_t *, AdbcError * error)
{
    return notImplemented(error, "StatementExecutePartitions");
}

AdbcStatusCode chdbStatementGetParameterSchema(
    AdbcStatement *, ArrowSchema *, AdbcError * error)
{
    return notImplemented(error, "StatementGetParameterSchema");
}

AdbcStatusCode chdbStatementSetSubstraitPlan(
    AdbcStatement *, const uint8_t *, size_t, AdbcError * error)
{
    return notImplemented(error, "StatementSetSubstraitPlan");
}

/// ---------------------------------------------------------------------
/// Driver init
/// ---------------------------------------------------------------------

AdbcStatusCode chdbDriverRelease(AdbcDriver * driver, AdbcError * error)
{
    if (!driver)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] driver is null");
    driver->private_data = nullptr;
    return ADBC_STATUS_OK;
}

} // anonymous namespace

extern "C" CHDB_EXPORT AdbcStatusCode chdb_adbc_init(int version, void * raw_driver, AdbcError * error);

extern "C" AdbcStatusCode chdb_adbc_init(int version, void * raw_driver, AdbcError * error)
{
    if (version != ADBC_VERSION_1_0_0 && version != ADBC_VERSION_1_1_0)
        return setError(
            error,
            ADBC_STATUS_NOT_IMPLEMENTED,
            "[chdb] only ADBC 1.0.0 / 1.1.0 are supported (got " + std::to_string(version) + ")");
    if (!raw_driver)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] driver is null");

    auto * driver = static_cast<AdbcDriver *>(raw_driver);
    std::memset(driver, 0, version == ADBC_VERSION_1_0_0 ? ADBC_DRIVER_1_0_0_SIZE : ADBC_DRIVER_1_1_0_SIZE);

    driver->release = chdbDriverRelease;

    driver->DatabaseNew = chdbDatabaseNew;
    driver->DatabaseSetOption = chdbDatabaseSetOption;
    driver->DatabaseInit = chdbDatabaseInit;
    driver->DatabaseRelease = chdbDatabaseRelease;

    driver->ConnectionNew = chdbConnectionNew;
    driver->ConnectionSetOption = chdbConnectionSetOption;
    driver->ConnectionInit = chdbConnectionInit;
    driver->ConnectionRelease = chdbConnectionRelease;
    driver->ConnectionCommit = chdbConnectionCommit;
    driver->ConnectionRollback = chdbConnectionRollback;
    driver->ConnectionGetInfo = chdbConnectionGetInfo;
    driver->ConnectionGetObjects = chdbConnectionGetObjects;
    driver->ConnectionGetTableSchema = chdbConnectionGetTableSchema;
    driver->ConnectionGetTableTypes = chdbConnectionGetTableTypes;
    driver->ConnectionReadPartition = chdbConnectionReadPartition;

    driver->StatementNew = chdbStatementNew;
    driver->StatementRelease = chdbStatementRelease;
    driver->StatementSetSqlQuery = chdbStatementSetSqlQuery;
    driver->StatementSetOption = chdbStatementSetOption;
    driver->StatementPrepare = chdbStatementPrepare;
    driver->StatementBind = chdbStatementBind;
    driver->StatementBindStream = chdbStatementBindStream;
    driver->StatementExecuteQuery = chdbStatementExecuteQuery;
    driver->StatementExecutePartitions = chdbStatementExecutePartitions;
    driver->StatementGetParameterSchema = chdbStatementGetParameterSchema;
    driver->StatementSetSubstraitPlan = chdbStatementSetSubstraitPlan;

    /// ADBC 1.1.0 additions stay zeroed: the driver manager backfills
    /// unset entries with NOT_IMPLEMENTED stubs.

    return ADBC_STATUS_OK;
}
