#include "chdb.h"
#include "chdb-internal.h"
#include "ChdbClient.h"
#include "QueryResult.h"

#include <Common/Exception.h>

#include <arrow/c/abi.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <string>

using namespace DB;

namespace
{

std::atomic<uint64_t> g_insert_stream_seq{0};

String nextInternalStreamName()
{
    return "__chdb_ins_" + std::to_string(g_insert_stream_seq.fetch_add(1, std::memory_order_relaxed));
}

String escapeStreamNameForSql(const String & name)
{
    String escaped;
    escaped.reserve(name.size());
    for (char c : name)
    {
        if (c == '\'')
            escaped += "''";
        else
            escaped += c;
    }
    return escaped;
}

String quoteIdent(const char * name)
{
    String out = "`";
    for (const char * p = name; *p; ++p)
    {
        if (*p == '`')
            out += "``";
        else
            out += *p;
    }
    out += '`';
    return out;
}

/// `(col1, col2, ...)` from an Arrow struct schema so INSERT maps by name.
/// Empty when any child is unnamed — callers then fall back to position mapping.
String columnListFromArrowSchema(const ArrowSchema * schema)
{
    if (!schema || schema->n_children <= 0)
        return {};

    String cols;
    for (int64_t i = 0; i < schema->n_children; ++i)
    {
        const ArrowSchema * child = schema->children[static_cast<size_t>(i)];
        if (!child || !child->name || child->name[0] == '\0')
            return {};
        if (i)
            cols += ", ";
        cols += quoteIdent(child->name);
    }
    return " (" + cols + ")";
}

String columnListFromStream(chdb_arrow_stream arrow_stream)
{
    auto * stream = reinterpret_cast<ArrowArrayStream *>(arrow_stream);
    if (!stream || !stream->get_schema)
        return {};

    ArrowSchema schema;
    std::memset(&schema, 0, sizeof(schema));
    if (stream->get_schema(stream, &schema) != 0)
        return {};

    String cols = columnListFromArrowSchema(&schema);
    if (schema.release)
        schema.release(&schema);
    return cols;
}

chdb_result * makeErrorResult(String message)
{
    return reinterpret_cast<chdb_result *>(new CHDB::MaterializedQueryResult(std::move(message)));
}

struct UnregisterGuard
{
    chdb_connection conn;
    String name;

    ~UnregisterGuard()
    {
        if (!name.empty())
            chdb_arrow_unregister_table(conn, name.c_str());
    }
};

chdb_result * runArrowInsert(
    chdb_connection conn,
    const char * dest_table,
    const String & stream_name,
    const String & column_list,
    const chdb_arrow_insert_options * options)
{
    if (!dest_table || dest_table[0] == '\0')
        return makeErrorResult("Unexpected null dest_table");

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return makeErrorResult("Invalid or closed connection");

    String sql = "INSERT INTO ";
    sql += dest_table;
    sql += column_list;
    sql += " SELECT * FROM ArrowStream('";
    sql += escapeStreamNameForSql(stream_name);
    sql += "')";

    if (options && options->settings && options->settings[0] != '\0')
    {
        sql += " SETTINGS ";
        sql += options->settings;
    }

    auto * client = static_cast<ChdbClient *>(connection->server);
    auto query_result = client->executeMaterializedQuery(
        sql.data(), sql.size(),
        "Null", std::strlen("Null"));

    if (!query_result)
        return makeErrorResult("Insert query processing failed");

    if (!query_result->getError().empty())
        return makeErrorResult(query_result->getError());

    return reinterpret_cast<chdb_result *>(query_result.release());
}

} // namespace

extern "C" {

chdb_result * chdb_insert_arrow_array(
    chdb_connection conn,
    const char * dest_table,
    chdb_arrow_schema arrow_schema,
    chdb_arrow_array arrow_array,
    const chdb_arrow_insert_options * options)
{
    if (!conn)
        return makeErrorResult("Unexpected null connection");
    if (!arrow_schema || !arrow_array)
        return makeErrorResult("Unexpected null Arrow schema or array");

    try
    {
        String stream_name = nextInternalStreamName();
        String column_list = columnListFromArrowSchema(reinterpret_cast<const ArrowSchema *>(arrow_schema));
        if (chdb_arrow_array_scan(conn, stream_name.c_str(), arrow_schema, arrow_array) != CHDBSuccess)
            return makeErrorResult("Failed to register Arrow array for insert");

        UnregisterGuard guard{conn, stream_name};
        return runArrowInsert(conn, dest_table, stream_name, column_list, options);
    }
    catch (const Exception & e)
    {
        return makeErrorResult(getExceptionMessage(e, false));
    }
    catch (const std::exception & e)
    {
        return makeErrorResult(e.what());
    }
    catch (...)
    {
        return makeErrorResult(DB::getCurrentExceptionMessage(true));
    }
}

chdb_result * chdb_insert_arrow_stream(
    chdb_connection conn,
    const char * dest_table,
    chdb_arrow_stream arrow_stream,
    const chdb_arrow_insert_options * options)
{
    if (!conn)
        return makeErrorResult("Unexpected null connection");
    if (!arrow_stream)
        return makeErrorResult("Unexpected null Arrow stream");

    try
    {
        String stream_name = nextInternalStreamName();
        String column_list = columnListFromStream(arrow_stream);
        if (chdb_arrow_scan(conn, stream_name.c_str(), arrow_stream) != CHDBSuccess)
            return makeErrorResult("Failed to register Arrow stream for insert");

        UnregisterGuard guard{conn, stream_name};
        return runArrowInsert(conn, dest_table, stream_name, column_list, options);
    }
    catch (const Exception & e)
    {
        return makeErrorResult(getExceptionMessage(e, false));
    }
    catch (const std::exception & e)
    {
        return makeErrorResult(e.what());
    }
    catch (...)
    {
        return makeErrorResult(DB::getCurrentExceptionMessage(true));
    }
}

} // extern "C"
