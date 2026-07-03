#pragma once

#include <memory>
#include <vector>
#include <utility>

#include "config.h"
#include <base/types.h>

#include <Processors/Chunk.h>
namespace DB
{
    class Block;
}

namespace CHDB
{

enum class QueryResultType : uint8_t
{
    RESULT_TYPE_MATERIALIZED = 0,
    RESULT_TYPE_STREAMING = 1,
    RESULT_TYPE_CHUNK = 2,
    RESULT_TYPE_DATAFRAME = 3,
    RESULT_TYPE_ARROW = 4,
    RESULT_TYPE_NONE = 5
};

class QueryResult
{
public:
    explicit QueryResult(QueryResultType type, String error_message_ = "")
        : result_type(type), error_message(std::move(error_message_))
    {}

    virtual ~QueryResult() = default;

    QueryResultType getType() const { return result_type; }
    const String & getError() const { return error_message; }
    virtual bool isEmpty() const = 0;

protected:
    QueryResultType result_type;
    String error_message;
};

class StreamQueryResult : public QueryResult
{
public:
    explicit StreamQueryResult(String error_message_ = "")
        : QueryResult(QueryResultType::RESULT_TYPE_STREAMING, std::move(error_message_))
    {}

    bool isEmpty() const override
    {
        return false;
    }

    /// Opaque per-stream state slot used by streaming-output binding code
    /// (currently the Arrow C Data Interface output path) to persist a
    /// converter, schema, and dictionary cache across fetches. The shared
    /// pointer carries its own deleter so the StreamQueryResult destructor
    /// releases the state without knowing its concrete type.
    std::shared_ptr<void> private_data;
};

/// Handle for a streaming INSERT (chdb_stream_insert). Returned to the C layer
/// reinterpret_cast as a chdb_insert_stream, mirroring how StreamQueryResult
/// backs the read-side streaming handle. `context` is a
/// CHDB::InsertStreamContext (opaque here to keep this header light — the
/// thread/queue machinery lives in StreamingInsert.h / ChdbClient). On an init
/// failure `context` is null and `error_message` carries the reason.
class InsertStreamResult : public QueryResult
{
public:
    explicit InsertStreamResult(String error_message_ = "")
        : QueryResult(QueryResultType::RESULT_TYPE_STREAMING, std::move(error_message_))
    {}

    bool isEmpty() const override { return false; }

    /// Updated as append()/done() observe engine-side errors so that
    /// chdb_stream_insert_error() reflects the latest failure.
    void setError(String message) { error_message = std::move(message); }

    std::shared_ptr<void> context;

    /// Back-pointer to the owning DB::ChdbClient (opaque here). The C ABI
    /// append/done/cancel functions take only the stream handle (no conn), so
    /// the handle must carry the client to route calls. If the connection is
    /// closed while the stream is open, teardown cancels the stream and marks
    /// its context finalized; the C ABI checks that flag before dereferencing
    /// this pointer, so a stale handle degrades to error returns and a safe
    /// destroy instead of undefined behavior.
    void * owner = nullptr;
};

using ResultBuffer = std::unique_ptr<std::vector<char>>;

class MaterializedQueryResult : public QueryResult
{
public:
    explicit MaterializedQueryResult(
        ResultBuffer result_buffer_,
        double elapsed_,
        uint64_t rows_read_,
        uint64_t bytes_read_,
        uint64_t storage_rows_read_,
        uint64_t storage_bytes_read_,
        uint64_t rows_written_ = 0,
        uint64_t bytes_written_ = 0)
        : QueryResult(QueryResultType::RESULT_TYPE_MATERIALIZED),
        result_buffer(std::move(result_buffer_)),
        elapsed(elapsed_),
        rows_read(rows_read_),
        bytes_read(bytes_read_),
        storage_rows_read(storage_rows_read_),
        storage_bytes_read(storage_bytes_read_),
        rows_written(rows_written_),
        bytes_written(bytes_written_)
    {}

    explicit MaterializedQueryResult(String error_message_)
        : QueryResult(QueryResultType::RESULT_TYPE_MATERIALIZED, std::move(error_message_))
    {}

    bool isEmpty() const override
    {
        return rows_read == 0;
    }

    String string()
    {
        if (!result_buffer)
            return {};

        return String(result_buffer->begin(), result_buffer->end());
    }

public:
    /// Default member initializers matter for the error-message constructor,
    /// which sets none of these: accessors like chdb_result_rows_written()
    /// are callable on error results and must read zeros, not garbage.
    ResultBuffer result_buffer;
    double elapsed = 0.0;
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    uint64_t storage_rows_read = 0;
    uint64_t storage_bytes_read = 0;
    /// Write progress of INSERT queries, accumulated from the engine's
    /// CountingTransform progress callbacks. Includes rows/bytes written by
    /// cascaded materialized views — same semantics as the HTTP interface's
    /// X-ClickHouse-Summary.written_rows.
    uint64_t rows_written = 0;
    uint64_t bytes_written = 0;
};

/// Raw Chunk-bag query result. Produced by ChunkCollectorOutputFormat-backed
/// runs, consumed by both the USE_PYTHON DataFrame builder and the libchdb
/// Arrow C Data Interface output path. Available in both builds.
class ChunkQueryResult : public QueryResult
{
public:
    explicit ChunkQueryResult(
        std::vector<DB::Chunk> chunks_,
        std::shared_ptr<const DB::Block> header_,
        double elapsed_,
        uint64_t rows_read_,
        uint64_t bytes_read_,
        uint64_t storage_rows_read_,
        uint64_t storage_bytes_read_,
        uint64_t rows_written_ = 0,
        uint64_t bytes_written_ = 0)
        : QueryResult(QueryResultType::RESULT_TYPE_CHUNK),
        chunks(std::move(chunks_)),
        header(header_),
        elapsed(elapsed_),
        rows_read(rows_read_),
        bytes_read(bytes_read_),
        storage_rows_read(storage_rows_read_),
        storage_bytes_read(storage_bytes_read_),
        rows_written(rows_written_),
        bytes_written(bytes_written_)
    {}

    explicit ChunkQueryResult(String error_message_)
        : QueryResult(QueryResultType::RESULT_TYPE_CHUNK, std::move(error_message_))
    {}

    bool isEmpty() const override
    {
        return rows_read == 0;
    }

public:
    /// Same rationale as MaterializedQueryResult: the error-message
    /// constructor leaves these untouched, so they must default to zero.
    std::vector<DB::Chunk> chunks;
    std::shared_ptr<const DB::Block> header;
    double elapsed = 0.0;
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    uint64_t storage_rows_read = 0;
    uint64_t storage_bytes_read = 0;
    uint64_t rows_written = 0;
    uint64_t bytes_written = 0;
};

using QueryResultPtr = std::unique_ptr<QueryResult>;
using MaterializedQueryResultPtr = std::unique_ptr<MaterializedQueryResult>;
using StreamQueryResultPtr = std::unique_ptr<StreamQueryResult>;
using ChunkQueryResultPtr = std::unique_ptr<ChunkQueryResult>;
using InsertStreamResultPtr = std::unique_ptr<InsertStreamResult>;

} // namespace CHDB
