#include "chdb.h"
#include "chdb-internal.h"
#include "ArrowStreamRegistry.h"

#include <arrow/c/abi.h>
#include <base/defines.h>

#include <memory>

namespace CHDB
{

struct PrivateData
{
    ArrowSchema * schema;
    ArrowArray * array;
    bool done = false;
};

static void EmptySchemaRelease(ArrowSchema * schema)
{
    schema->release = nullptr;
}

static void EmptyArrayRelease(ArrowArray * array)
{
    array->release = nullptr;
}

static int GetSchema(struct ArrowArrayStream * stream, struct ArrowSchema * out)
{
	auto * private_data = static_cast<PrivateData *>((stream->private_data));
	if (private_data->schema == nullptr)
		return CHDBError;

	*out = *private_data->schema;
	out->release = EmptySchemaRelease;
	return CHDBSuccess;
}

static int GetNext(struct ArrowArrayStream * stream, struct ArrowArray * out)
{
	auto * private_data = static_cast<PrivateData *>((stream->private_data));
	*out = *private_data->array;
	if (private_data->done)
    {
		out->release = nullptr;
    }
	else
    {
		out->release = EmptyArrayRelease;
    }

	private_data->done = true;
	return CHDBSuccess;
}

static const char * GetLastError(struct ArrowArrayStream * /*stream*/)
{
	return nullptr;
}

static void Release(struct ArrowArrayStream * stream)
{
	if (stream->private_data != nullptr)
		delete reinterpret_cast<PrivateData *>(stream->private_data);

	stream->private_data = nullptr;
	stream->release = nullptr;
}

void chdb_destroy_arrow_stream(ArrowArrayStream * arrow_stream)
{
    if (!arrow_stream)
        return;

    if (arrow_stream->release)
        arrow_stream->release(arrow_stream);
    chassert(!arrow_stream->release);

    delete arrow_stream;
}

} // namespace CHDB

static chdb_state chdb_inner_arrow_scan(
    chdb_connection conn, const char * table_name,
    chdb_arrow_stream arrow_stream, bool is_owner)
{
    if (!table_name || !arrow_stream)
        return CHDBError;

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return CHDBError;

    auto * stream = reinterpret_cast<ArrowArrayStream *>(arrow_stream);
    if (!stream->release || !stream->get_schema)
        return CHDBError;

    /// Only validates the stream; the schema is fetched again when the table is read.
    /// get_schema hands us a schema we own, so release it here.
    ArrowSchema schema{};
    if (stream->get_schema(stream, &schema) != 0)
        return CHDBError;
    if (schema.release)
        schema.release(&schema);

    try
    {
        return CHDB::ArrowStreamRegistry::instance().registerArrowStream(String(table_name), stream, is_owner)
            ? CHDBSuccess : CHDBError;
    }
    catch (...)
    {
        return CHDBError;
    }
}

chdb_state chdb_arrow_scan(
    chdb_connection conn, const char * table_name,
    chdb_arrow_stream arrow_stream)
{
    return chdb_inner_arrow_scan(conn, table_name, arrow_stream, false);
}

chdb_state chdb_arrow_array_scan(
    chdb_connection conn, const char * table_name,
    chdb_arrow_schema arrow_schema, chdb_arrow_array arrow_array)
{
    if (!arrow_schema || !arrow_array)
        return CHDBError;

    ArrowArrayStream * stream = nullptr;
    try
    {
        auto private_data = std::make_unique<CHDB::PrivateData>();
        private_data->schema = reinterpret_cast<ArrowSchema *>(arrow_schema);
        private_data->array = reinterpret_cast<ArrowArray *>(arrow_array);

        stream = new ArrowArrayStream();
        stream->get_schema = CHDB::GetSchema;
        stream->get_next = CHDB::GetNext;
        stream->get_last_error = CHDB::GetLastError;
        stream->release = CHDB::Release;
        stream->private_data = private_data.release();
    }
    catch (...)
    {
        return CHDBError;
    }

    /// The registry takes the wrapper only on success; otherwise it is ours to free.
    chdb_state state = chdb_inner_arrow_scan(conn, table_name, reinterpret_cast<chdb_arrow_stream>(stream), true);
    if (state != CHDBSuccess)
        CHDB::chdb_destroy_arrow_stream(stream);
    return state;
}

chdb_state chdb_arrow_unregister_table(chdb_connection conn, const char * table_name)
{
    if (!table_name)
        return CHDBError;

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return CHDBError;

    try
    {
        CHDB::ArrowStreamRegistry::instance().unregisterArrowStream(String(table_name));
        return CHDBSuccess;
    }
    catch (...)
    {
        return CHDBError;
    }
}
