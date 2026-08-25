#include "chdb.h"
#include "chdb-internal.h"
#include "ChdbClient.h"
#include "QueryResult.h"

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>

#include <Common/Exception.h>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

using namespace DB;

namespace
{

/// Format name we pass to ChdbClient/ClientBase to divert query output
/// into chunk-collection mode. Single source of truth lives in
/// src/Client/ClientBase.h as DB::CHUNK_COLLECT_FORMAT_NAME.
constexpr const char * kArrowConverterFormatName = "Arrow";

CHColumnToArrowColumn::Settings buildConverterSettings(
    const chdb_arrow_options * options,
    bool output_uuid_as_fixed_byte_array = false,
    bool output_variant_as_string = false)
{
    CHColumnToArrowColumn::Settings settings;
    /// Defaults align with the Layer-1 type contract (string_as_string=1,
    /// low_cardinality_as_dictionary=0, unsupported_as_binary=0). DateTime
    /// is left untouched on purpose: we keep the engine's native
    /// DateTime -> arrow::uint32(seconds, tz-dropped) mapping so the new C
    /// ABI behaves identically to format="ArrowStream" — bindings that
    /// already consume ArrowStream don't need a second code path.
    /// Callers that want timestamp(SECOND, tz) semantics can express that
    /// in SQL via toDateTime64(col, 0, 'UTC').
    settings.output_string_as_string = true;
    settings.output_fixed_string_as_fixed_byte_array = true;
    settings.low_cardinality_as_dictionary = false;
    settings.use_signed_indexes_for_dictionary = false;
    settings.use_64_bit_indexes_for_dictionary = false;
    settings.output_date_as_uint16 = false;
    settings.output_uuid_as_fixed_byte_array = output_uuid_as_fixed_byte_array;
    settings.output_variant_as_string = output_variant_as_string;
    settings.output_unsupported_types_as_binary = false;

    if (options)
    {
        settings.output_string_as_string = options->string_as_string != 0;
        settings.low_cardinality_as_dictionary = options->low_cardinality_as_dictionary != 0;
        settings.output_unsupported_types_as_binary = options->unsupported_as_binary != 0;
    }
    return settings;
}

/// Build a chdb_result that exposes only metrics + error (no buffer payload).
/// Out-of-band data is delivered through the Arrow stream the caller owns.
chdb_result * makeMetricsResult(
    double elapsed,
    uint64_t rows_read,
    uint64_t bytes_read,
    uint64_t storage_rows_read,
    uint64_t storage_bytes_read)
{
    auto * res = new CHDB::MaterializedQueryResult(
        CHDB::ResultBuffer(),
        elapsed,
        rows_read,
        bytes_read,
        storage_rows_read,
        storage_bytes_read);
    return reinterpret_cast<chdb_result *>(res);
}

chdb_result * makeErrorResult(String message)
{
    return reinterpret_cast<chdb_result *>(new CHDB::MaterializedQueryResult(std::move(message)));
}

/// Releases for an ArrowArrayStream we synthesize for the one-batch fetch path.
struct OneBatchPrivate
{
    std::shared_ptr<arrow::Schema> schema;
    ArrowArray array{};
    bool array_consumed = false;
    String last_error;
    std::shared_ptr<void> data_keepalive; /// Holds arrow::RecordBatch alive
};

void OneBatchRelease(ArrowArrayStream * stream)
{
    if (!stream)
        return;
    auto * priv = static_cast<OneBatchPrivate *>(stream->private_data);
    if (priv)
    {
        if (priv->array.release)
            priv->array.release(&priv->array);
        delete priv;
    }
    stream->private_data = nullptr;
    stream->release = nullptr;
}

int OneBatchGetSchema(ArrowArrayStream * stream, ArrowSchema * out)
{
    auto * priv = static_cast<OneBatchPrivate *>(stream->private_data);
    if (!priv || !priv->schema)
        return EINVAL;
    auto status = arrow::ExportSchema(*priv->schema, out);
    if (!status.ok())
    {
        priv->last_error = status.ToString();
        return EIO;
    }
    return 0;
}

int OneBatchGetNext(ArrowArrayStream * stream, ArrowArray * out)
{
    auto * priv = static_cast<OneBatchPrivate *>(stream->private_data);
    if (!priv)
        return EINVAL;
    if (priv->array_consumed || !priv->array.release)
    {
        std::memset(out, 0, sizeof(*out));
        out->release = nullptr;
        return 0;
    }
    /// Hand off ownership of the cached ArrowArray to the caller.
    *out = priv->array;
    std::memset(&priv->array, 0, sizeof(priv->array));
    priv->array_consumed = true;
    return 0;
}

const char * OneBatchGetLastError(ArrowArrayStream * stream)
{
    auto * priv = static_cast<OneBatchPrivate *>(stream->private_data);
    return (priv && !priv->last_error.empty()) ? priv->last_error.c_str() : nullptr;
}

void initOneBatchStream(ArrowArrayStream * stream, std::unique_ptr<OneBatchPrivate> priv)
{
    stream->get_schema = OneBatchGetSchema;
    stream->get_next = OneBatchGetNext;
    stream->get_last_error = OneBatchGetLastError;
    stream->release = OneBatchRelease;
    stream->private_data = priv.release();
}

/// Streaming context held by StreamQueryResult::private_data to persist
/// the converter, schema, and dictionary cache across chdb_stream_fetch_arrow
/// calls. Required for schema/dict stability per Arrow C Data Interface.
struct ArrowStreamingState
{
    chdb_arrow_options options;
    bool output_uuid_as_fixed_byte_array = false;
    bool output_variant_as_string = false;
    std::unique_ptr<CHColumnToArrowColumn> converter;
    std::shared_ptr<arrow::Schema> schema;
    ColumnsWithTypeAndName header_columns;
    bool header_initialized = false;
    /// Cached dictionary values shared across batches. Only used when
    /// low_cardinality_as_dictionary=1; required for cross-chunk stability.
    std::unordered_map<std::string, MutableColumnPtr> cached_dictionary_values;
    std::mutex mutex;
};

void ensureConverterInitialized(
    ArrowStreamingState & state,
    const ColumnsWithTypeAndName & header,
    const Chunk * first_chunk)
{
    if (state.header_initialized)
        return;

    state.header_columns = header;
    auto settings = buildConverterSettings(
        &state.options, state.output_uuid_as_fixed_byte_array, state.output_variant_as_string);
    state.converter = std::make_unique<CHColumnToArrowColumn>(
        state.header_columns,
        kArrowConverterFormatName,
        settings);
    state.converter->initializeArrowSchema(first_chunk);
    state.schema = state.converter->getArrowSchema();
    state.header_initialized = true;
}

/// Materialized path: collect all chunks, build a single arrow::Table, and
/// export it as a RecordBatchReader-backed ArrowArrayStream.
chdb_result * runMaterializedArrowQuery(
    chdb_conn * connection,
    const char * query,
    size_t query_len,
    ArrowArrayStream * out_stream,
    const chdb_arrow_options * options,
    bool output_uuid_as_fixed_byte_array = false,
    bool output_variant_as_string = false)
{
    if (!out_stream)
        return makeErrorResult("Unexpected null out_stream");
    if (!query)
        return makeErrorResult("Unexpected null query");

    /// Ensure caller-supplied stream is zeroed; on error we leave it released.
    std::memset(out_stream, 0, sizeof(*out_stream));

    auto * client = static_cast<DB::ChdbClient *>(connection->server);
    auto query_result = client->executeMaterializedQuery(
        query, query_len,
        CHUNK_COLLECT_FORMAT_NAME, std::strlen(CHUNK_COLLECT_FORMAT_NAME));

    if (!query_result)
        return makeErrorResult("Query processing failed");

    if (!query_result->getError().empty())
        return makeErrorResult(query_result->getError());

    if (query_result->getType() != CHDB::QueryResultType::RESULT_TYPE_CHUNK)
        return makeErrorResult("Expected chunk-collecting result for Arrow output");

    auto * chunk_result = static_cast<CHDB::ChunkQueryResult *>(query_result.get());
    if (!chunk_result->header)
        return makeErrorResult(CHDB::kErrorMissingResultHeader);

    const auto & header_columns = chunk_result->header->getColumnsWithTypeAndName();

    auto settings = buildConverterSettings(options, output_uuid_as_fixed_byte_array, output_variant_as_string);
    auto converter = std::make_unique<CHColumnToArrowColumn>(
        header_columns,
        kArrowConverterFormatName,
        settings);

    /// Chunks pass through untouched — no DateTime widening, no
    /// per-chunk castColumn allocation. Engine column types map 1:1
    /// onto the CHColumnToArrowColumn kernel just like format="ArrowStream".
    std::vector<Chunk> chunks_for_export = std::move(chunk_result->chunks);

    std::shared_ptr<arrow::Table> table;
    try
    {
        if (chunks_for_export.empty())
        {
            /// Empty result: pass a single zero-row probe chunk so the
            /// converter can derive the arrow schema, then use Table::MakeEmpty
            /// to synthesize a properly-shaped zero-row table.
            MutableColumns empty_cols;
            empty_cols.reserve(header_columns.size());
            for (const auto & h : header_columns)
                empty_cols.emplace_back(h.type->createColumn());
            std::vector<Chunk> probe;
            probe.emplace_back(std::move(empty_cols), 0);
            converter->initializeArrowSchema(&probe.front());
            auto schema = converter->getArrowSchema();
            auto empty_table = arrow::Table::MakeEmpty(schema);
            if (!empty_table.ok())
                return makeErrorResult(empty_table.status().ToString());
            table = std::move(empty_table).ValueOrDie();
        }
        else
        {
            converter->chChunkToArrowTable(table, chunks_for_export, header_columns.size());
        }
    }
    catch (const Exception & e)
    {
        return makeErrorResult(getExceptionMessage(e, false));
    }
    catch (const std::exception & e)
    {
        return makeErrorResult(e.what());
    }

    if (!table)
        return makeErrorResult("CHColumnToArrowColumn returned null table");

    auto reader = std::make_shared<arrow::TableBatchReader>(table);

    auto status = arrow::ExportRecordBatchReader(reader, out_stream);
    if (!status.ok())
        return makeErrorResult(status.ToString());

    return makeMetricsResult(
        chunk_result->elapsed,
        chunk_result->rows_read,
        chunk_result->bytes_read,
        chunk_result->storage_rows_read,
        chunk_result->storage_bytes_read);
}

} // namespace

namespace CHDB
{
void chdb_stream_result_set_arrow_uuid_as_fixed(chdb_result * result, bool enabled)
{
    if (!result)
        return;
    auto * generic_result = reinterpret_cast<CHDB::QueryResult *>(result);
    if (generic_result->getType() != CHDB::QueryResultType::RESULT_TYPE_STREAMING)
        return;
    auto * stream = static_cast<CHDB::StreamQueryResult *>(generic_result);
    auto state = std::static_pointer_cast<ArrowStreamingState>(stream->private_data);
    if (!state)
        return;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->output_uuid_as_fixed_byte_array = enabled;
}

void chdb_stream_result_set_arrow_variant_as_string(chdb_result * result, bool enabled)
{
    if (!result)
        return;
    auto * generic_result = reinterpret_cast<CHDB::QueryResult *>(result);
    if (generic_result->getType() != CHDB::QueryResultType::RESULT_TYPE_STREAMING)
        return;
    auto * stream = static_cast<CHDB::StreamQueryResult *>(generic_result);
    auto state = std::static_pointer_cast<ArrowStreamingState>(stream->private_data);
    if (!state)
        return;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->output_variant_as_string = enabled;
}

chdb_result * chdb_query_arrow_with_settings_n(
    chdb_connection conn,
    const char * query,
    size_t query_len,
    chdb_arrow_stream out_stream,
    const chdb_arrow_options * options,
    bool output_uuid_as_fixed_byte_array,
    bool output_variant_as_string,
    const DB::NameToNameMap & params)
{
    if (!conn)
        return makeErrorResult("Unexpected null connection");

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return makeErrorResult("Invalid or closed connection");

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        CApiQueryParameterGuard guard(client, params);
        auto * raw_stream = reinterpret_cast<ArrowArrayStream *>(out_stream);
        return runMaterializedArrowQuery(
            connection,
            query,
            query_len,
            raw_stream,
            options,
            output_uuid_as_fixed_byte_array,
            output_variant_as_string);
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
}

extern "C" {

chdb_result * chdb_query_arrow(
    chdb_connection conn, const char * query,
    chdb_arrow_stream out_stream, const chdb_arrow_options * options)
{
    return chdb_query_arrow_n(conn, query, query ? std::strlen(query) : 0, out_stream, options);
}

chdb_result * chdb_query_arrow_n(
    chdb_connection conn, const char * query, size_t query_len,
    chdb_arrow_stream out_stream, const chdb_arrow_options * options)
{
    if (!conn)
        return makeErrorResult("Unexpected null connection");

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return makeErrorResult("Invalid or closed connection");

    try
    {
        auto * raw_stream = reinterpret_cast<ArrowArrayStream *>(out_stream);
        return runMaterializedArrowQuery(connection, query, query_len, raw_stream, options);
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

chdb_result * chdb_stream_query_arrow(
    chdb_connection conn, const char * query,
    const chdb_arrow_options * options)
{
    return chdb_stream_query_arrow_n(conn, query, query ? std::strlen(query) : 0, options);
}

chdb_result * chdb_stream_query_arrow_n(
    chdb_connection conn, const char * query, size_t query_len,
    const chdb_arrow_options * options)
{
    if (!conn)
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Unexpected null connection"));

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Invalid or closed connection"));

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        auto query_result = client->executeStreamingInit(
            query, query_len,
            CHUNK_COLLECT_FORMAT_NAME, std::strlen(CHUNK_COLLECT_FORMAT_NAME),
            /*dataframe_over_chunks=*/false);
        if (!query_result)
            return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Streaming init failed"));

        if (query_result->getType() != CHDB::QueryResultType::RESULT_TYPE_STREAMING)
            return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Unexpected non-streaming result for Arrow streaming"));

        auto state = std::make_shared<ArrowStreamingState>();
        if (options)
            state->options = *options;
        else
            state->options = chdb_arrow_options{0, 0, 1};

        auto * stream_result = static_cast<CHDB::StreamQueryResult *>(query_result.get());
        stream_result->private_data = std::static_pointer_cast<void>(state);

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const Exception & e)
    {
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult(getExceptionMessage(e, false)));
    }
    catch (const std::exception & e)
    {
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult(e.what()));
    }
    catch (...)
    {
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult(DB::getCurrentExceptionMessage(true)));
    }
}

chdb_result * chdb_stream_query_arrow_with_params(
    chdb_connection conn, const char * query,
    const chdb_arrow_options * options,
    const char * const * param_names,
    const char * const * param_values,
    size_t param_count)
{
    return chdb_stream_query_arrow_with_params_n(
        conn,
        query,
        query ? std::strlen(query) : 0,
        options,
        param_names,
        /*param_name_lens=*/nullptr,
        param_values,
        /*param_value_lens=*/nullptr,
        param_count);
}

chdb_result * chdb_stream_query_arrow_with_params_n(
    chdb_connection conn, const char * query, size_t query_len,
    const chdb_arrow_options * options,
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count)
{
    if (!conn)
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Unexpected null connection"));

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Invalid or closed connection"));

    try
    {
        auto * client = static_cast<DB::ChdbClient *>(connection->server);
        /// Parameters only need to be present while the statement is parsed
        /// during init — the engine captures the values then (same contract
        /// as chdb_stream_query_with_params).
        const auto params
            = CHDB::buildParameterMap(param_names, param_name_lens, param_values, param_value_lens, param_count);
        CHDB::CApiQueryParameterGuard guard(client, params);

        auto query_result = client->executeStreamingInit(
            query, query_len,
            CHUNK_COLLECT_FORMAT_NAME, std::strlen(CHUNK_COLLECT_FORMAT_NAME),
            /*dataframe_over_chunks=*/false);
        if (!query_result)
            return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Streaming init failed"));

        if (query_result->getType() != CHDB::QueryResultType::RESULT_TYPE_STREAMING)
            return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult("Unexpected non-streaming result for Arrow streaming"));

        auto state = std::make_shared<ArrowStreamingState>();
        if (options)
            state->options = *options;
        else
            state->options = chdb_arrow_options{0, 0, 1};

        auto * stream_result = static_cast<CHDB::StreamQueryResult *>(query_result.get());
        stream_result->private_data = std::static_pointer_cast<void>(state);

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const Exception & e)
    {
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult(getExceptionMessage(e, false)));
    }
    catch (const std::exception & e)
    {
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult(e.what()));
    }
    catch (...)
    {
        return reinterpret_cast<chdb_result *>(new CHDB::StreamQueryResult(DB::getCurrentExceptionMessage(true)));
    }
}

chdb_state chdb_stream_fetch_arrow(
    chdb_connection conn, chdb_result * stream_result, chdb_arrow_stream out_batch)
{
    if (!conn || !stream_result || !out_batch)
        return CHDBError;

    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return CHDBError;

    auto * raw_out = reinterpret_cast<ArrowArrayStream *>(out_batch);
    std::memset(raw_out, 0, sizeof(*raw_out));

    auto * generic_result = reinterpret_cast<CHDB::QueryResult *>(stream_result);
    if (generic_result->getType() != CHDB::QueryResultType::RESULT_TYPE_STREAMING)
        return CHDBError;

    auto * stream = static_cast<CHDB::StreamQueryResult *>(generic_result);
    auto state = std::static_pointer_cast<ArrowStreamingState>(stream->private_data);
    if (!state)
        return CHDBError;

    auto * client = static_cast<DB::ChdbClient *>(connection->server);
    if (!client->hasStreamingQuery())
        return CHDBError;

    try
    {
        std::lock_guard<std::mutex> lock(state->mutex);

        auto iter_result = client->executeStreamingIterate(stream, false);
        if (!iter_result)
        {
            stream->setError("Streaming iterate returned no result");
            return CHDBError;
        }

        if (!iter_result->getError().empty())
        {
            /// Surface via chdb_result_error(stream_result).
            stream->setError(iter_result->getError());
            return CHDBError;
        }

        if (iter_result->getType() != CHDB::QueryResultType::RESULT_TYPE_CHUNK)
        {
            /// End of stream: emit an empty one-batch stream. If a schema
            /// has been observed already, expose it via get_schema so the
            /// consumer can still inspect column metadata after EOS.
            auto priv = std::make_unique<OneBatchPrivate>();
            priv->schema = state->schema;
            initOneBatchStream(raw_out, std::move(priv));
            return CHDBSuccess;
        }

        auto * chunk_iter = static_cast<CHDB::ChunkQueryResult *>(iter_result.get());
        if (!chunk_iter->header || chunk_iter->chunks.empty())
        {
            /// Zero-row results reach EOS before the converter initializes;
            /// derive the schema from a zero-row probe chunk (mirrors the
            /// materialized empty-result path) so column metadata survives.
            if (chunk_iter->header && !state->header_initialized)
            {
                const auto & src_header = chunk_iter->header->getColumnsWithTypeAndName();
                MutableColumns probe_cols;
                probe_cols.reserve(src_header.size());
                for (const auto & h : src_header)
                    probe_cols.emplace_back(h.type->createColumn());
                std::vector<Chunk> probe;
                probe.emplace_back(std::move(probe_cols), 0);
                ensureConverterInitialized(*state, src_header, &probe.front());
            }
            auto priv = std::make_unique<OneBatchPrivate>();
            priv->schema = state->schema;
            initOneBatchStream(raw_out, std::move(priv));
            return CHDBSuccess;
        }

        const auto & src_header = chunk_iter->header->getColumnsWithTypeAndName();
        ensureConverterInitialized(*state, src_header, &chunk_iter->chunks.front());

        std::vector<Chunk> batch_chunks = std::move(chunk_iter->chunks);

        std::shared_ptr<arrow::Table> table;
        state->converter->chChunkToArrowTable(table, batch_chunks, state->header_columns.size());
        if (!table)
        {
            stream->setError("Arrow conversion returned no table");
            return CHDBError;
        }

        /// Combine into a single record batch for one fetch == one batch.
        auto batches_result = table->CombineChunksToBatch();
        if (!batches_result.ok())
        {
            stream->setError(batches_result.status().ToString());
            return CHDBError;
        }
        auto batch = std::move(batches_result).ValueOrDie();

        auto priv = std::make_unique<OneBatchPrivate>();
        priv->schema = state->schema;
        auto status = arrow::ExportRecordBatch(*batch, &priv->array, nullptr);
        if (!status.ok())
        {
            stream->setError(status.ToString());
            return CHDBError;
        }
        /// Keep the batch alive so the exported buffers stay valid until the
        /// caller releases the one-batch stream. ExportRecordBatch already
        /// keeps the underlying buffers alive via the array release callback,
        /// but holding the RecordBatch shared_ptr is a defensive belt-and-braces.
        priv->data_keepalive = batch;

        initOneBatchStream(raw_out, std::move(priv));
        return CHDBSuccess;
    }
    catch (const Exception & e)
    {
        stream->setError(getExceptionMessage(e, false));
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        return CHDBError;
    }
    catch (const std::exception & e)
    {
        stream->setError(e.what());
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        return CHDBError;
    }
    catch (...)
    {
        stream->setError(DB::getCurrentExceptionMessage(true));
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        return CHDBError;
    }
}

} // extern "C"
