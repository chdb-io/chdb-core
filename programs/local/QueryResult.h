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
    ResultBuffer result_buffer;
    double elapsed = 0.0;
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    uint64_t storage_rows_read = 0;
    uint64_t storage_bytes_read = 0;
    /// Write progress of the query (e.g. INSERT). Includes rows/bytes
    /// written by cascaded materialized views — same semantics as
    /// X-ClickHouse-Summary.written_rows over the HTTP interface.
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
    std::vector<DB::Chunk> chunks;
    std::shared_ptr<const DB::Block> header;
    double elapsed = 0.0;
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    uint64_t storage_rows_read = 0;
    uint64_t storage_bytes_read = 0;
    /// See MaterializedQueryResult::rows_written.
    uint64_t rows_written = 0;
    uint64_t bytes_written = 0;
};

using QueryResultPtr = std::unique_ptr<QueryResult>;
using MaterializedQueryResultPtr = std::unique_ptr<MaterializedQueryResult>;
using StreamQueryResultPtr = std::unique_ptr<StreamQueryResult>;
using ChunkQueryResultPtr = std::unique_ptr<ChunkQueryResult>;

} // namespace CHDB
