#include <memory>
#include <mutex>
#include <ChdbClient.h>
#include <EmbeddedServer.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <base/getFQDNOrHostName.h>
#include <base/scope_guard.h>
#include <chdb-internal.h>
#include <Poco/Net/SocketAddress.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#if USE_PYTHON
#include <DataFrameQueryResult.h>
#include <PythonTableCache.h>
#include <PandasDataFrameBuilder.h>
#include <pybind11/pybind11.h>
namespace py = pybind11;
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
}

ChdbClient::ChdbClient(EmbeddedServer & server_ref)
    : ClientBase()
    , server(server_ref)
{
    query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    configuration = ConfigHelper::createEmpty();
    layered_configuration = new Poco::Util::LayeredConfiguration();
    layered_configuration->addWriteable(configuration, 0);
    if (server.config().has("progress"))
        configuration->setString("progress", server.config().getString("progress"));
    if (server.config().has("progress-table"))
        configuration->setString("progress-table", server.config().getString("progress-table"));
    if (server.config().has("enable-progress-table-toggle"))
        configuration->setBool("enable-progress-table-toggle", server.config().getBool("enable-progress-table-toggle"));
    if (!configuration->has("enable-progress-table-toggle"))
        configuration->setBool("enable-progress-table-toggle", false);
    session = std::make_unique<Session>(server.getGlobalContext(), ClientInfo::Interface::LOCAL);
#if USE_PYTHON
    python_table_cache = std::make_shared<CHDB::PythonTableCache>();
    session->setPythonTableCache(python_table_cache);
#endif
    session->authenticate("default", "", Poco::Net::SocketAddress{});
    global_context = session->makeSessionContext();
    global_context->setCurrentDatabase("default");
    global_context->setApplicationType(Context::ApplicationType::LOCAL);
    initClientContext(global_context);
    server_display_name = "chDB-embedded";
    query_processing_stage = QueryProcessingStage::Enum::Complete;
    is_interactive = false;
    ignore_error = false;
    echo_queries = false;
    print_stack_trace = false;
    initTTYBuffer(toProgressOption(getClientConfiguration().getString("progress", "default")),
        toProgressOption(getClientConfiguration().getString("progress-table", "default")));
    initKeystrokeInterceptor();
}

std::unique_ptr<ChdbClient> ChdbClient::create(EmbeddedServer & server_ref)
{
    return std::make_unique<ChdbClient>(server_ref);
}

ChdbClient::~ChdbClient()
{
    {
        std::lock_guard<std::mutex> lock(client_mutex);
        cleanup();
        resetQueryOutputVector();
    }
    EmbeddedServer::releaseInstance();
}

void ChdbClient::cleanup()
{
    try
    {
        if (streaming_query_context && streaming_query_context->streaming_result)
            cancelStreamingQueryWithoutLock(streaming_query_context->streaming_result);
        streaming_query_context.reset();

        /// Tear down any in-flight streaming insert BEFORE freeing connection/
        /// client_context: the worker thread captured `this` and holds references
        /// to them, so it must be cancelled and joined first to avoid a
        /// use-after-free. The worker does not take client_mutex, so joining while
        /// holding it (we are called under the lock) cannot deadlock.
        if (streaming_insert_context)
        {
            auto ctx = streaming_insert_context;
            ctx->cancelled = true;
            if (connection)
            {
                try { connection->sendCancel(); } catch (...) { tryLogCurrentException(__PRETTY_FUNCTION__); }
            }
            if (ctx->queue_buf)
                ctx->queue_buf->finish();
            if (ctx->worker.joinable())
                ctx->worker.join();
            /// Mark the stream finalized so a handle that outlives this
            /// connection becomes inert: the C ABI checks this flag before
            /// dereferencing its (now dangling) owner pointer, making
            /// append/done/cancel/destroy after close safe error paths.
            ctx->finalized.store(true, std::memory_order_release);
            /// Keep teardown symmetric with done()/cancel(): restore the
            /// settings snapshot (connection/client_context are still alive
            /// here) so no stale saved-settings state is left behind.
            restoreSettingsAfterInsertStream();
            streaming_insert_context.reset();
        }

        connection.reset();
        client_context.reset();
        global_context.reset();
        session.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ChdbClient::connect()
{
    initTTYBuffer(toProgressOption(getClientConfiguration().getString("progress", "default")),
        toProgressOption(getClientConfiguration().getString("progress-table", "default")));
    initKeystrokeInterceptor();
    const bool send_progress = need_render_progress || need_render_progress_table || hasProgressCallback() || hasProgressValuesCallback();
    const bool send_profile_events = need_render_progress_table;
    connection_parameters = ConnectionParameters::createForEmbedded(
        session->sessionContext()->getUserName(),
        "default");
    connection = LocalConnection::createConnection(
        connection_parameters,
        std::move(session),
        std_in.get(),
        send_progress,
        send_profile_events,
        server_display_name);
        connection->setDefaultDatabase("default");
#if defined(OS_WASM)
    /// chdb-wasm: enable ClickHouse's interactive-cancel path (polled frequently between
    /// chunks by the executor) so a query can be cancelled from JS. chdbWasmCancelRequested()
    /// reads the shared-memory cancel flag. Wasm-only — native chdb's path is unaffected.
    connection->setCancelCallback([] { return chdbWasmCancelRequested(); });
#endif
}

Poco::Util::LayeredConfiguration & ChdbClient::getClientConfiguration()
{
    chassert(layered_configuration);
    return *layered_configuration;
}

void ChdbClient::processError(std::string_view) const
{
    if (server_exception)
        server_exception->rethrow();
    if (client_exception)
        client_exception->rethrow();
}

bool ChdbClient::hasStreamingQuery() const
{
    std::lock_guard<std::mutex> lock(client_mutex);
    return streaming_query_context != nullptr;
}

size_t ChdbClient::getStorageRowsRead() const
{
    if (connection)
    {
        auto * local_connection = static_cast<LocalConnection *>(connection.get());
        return local_connection->getCHDBProgress().read_rows;
    }
    return 0;
}

size_t ChdbClient::getStorageBytesRead() const
{
    if (connection)
    {
        auto * local_connection = static_cast<LocalConnection *>(connection.get());
        return local_connection->getCHDBProgress().read_bytes;
    }
    return 0;
}

void ChdbClient::setQueryParameters(const NameToNameMap & params)
{
    std::lock_guard<std::mutex> lock(client_mutex);
    query_parameters = params;
    if (client_context)
        client_context->setQueryParameters(query_parameters);
}

void ChdbClient::clearQueryParameters()
{
    std::lock_guard<std::mutex> lock(client_mutex);
    query_parameters.clear();
    if (client_context)
        client_context->setQueryParameters(query_parameters);
}

#if USE_PYTHON
void ChdbClient::findQueryableObjFromPyCache(const String & query_str) const
{
    python_table_cache->findQueryableObjFromQuery(query_str);
}
#endif

#if USE_PYTHON
static bool isJSONSupported(const char * format, size_t format_len)
{
    if (format)
    {
        String lower_format{format, format_len};
        std::transform(lower_format.begin(), lower_format.end(), lower_format.begin(), ::tolower);

        return !(
            lower_format == "arrow" || lower_format == "parquet" || lower_format == "arrowstream" || lower_format == "protobuf"
            || lower_format == "protobuflist" || lower_format == "protobufsingle");
    }

    return true;
}
#endif

bool ChdbClient::parseQueryTextWithOutputFormat(const String & query, const String & format)
{
    if (!format.empty())
    {
        client_context->setDefaultFormat(format);
        setDefaultFormat(format);
    }

    if (!connection || !connection->checkConnected(connection_parameters.timeouts))
        connect();
#if USE_PYTHON
    (static_cast<DB::LocalConnection *>(connection.get()))->getSession().setJSONSupport(isJSONSupported(format.c_str(), format.size()));
#endif
    return processQueryText(query);
}

CHDB::QueryResultPtr ChdbClient::executeMaterializedQuery(
    const char * query, size_t query_len,
    const char * format, size_t format_len)
{
    std::lock_guard<std::mutex> lock(client_mutex);

    String query_str(query, query_len);
    String format_str(format, format_len);

    if (streaming_insert_context)
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "Cannot run a query while a streaming insert is active on this connection");

    try
    {
        DB::ThreadStatus thread_status;
        if (!parseQueryTextWithOutputFormat(query_str, format_str))
        {
            return std::make_unique<CHDB::MaterializedQueryResult>(getErrorMsg());
        }
        auto * local_connection = static_cast<LocalConnection *>(connection.get());
        size_t storage_rows_read = local_connection->getCHDBProgress().read_rows;
        size_t storage_bytes_read = local_connection->getCHDBProgress().read_bytes;
        size_t rows_written = local_connection->getCHDBProgress().written_rows;
        size_t bytes_written = local_connection->getCHDBProgress().written_bytes;

        if (format_str == CHUNK_COLLECT_FORMAT_NAME)
        {
            auto res = std::make_unique<CHDB::ChunkQueryResult>(
                std::move(collected_chunks),
                std::move(collected_chunks_header),
                getElapsedTime(),
                getProcessedRows(),
                getProcessedBytes(),
                storage_rows_read,
                storage_bytes_read,
                rows_written,
                bytes_written);
#if USE_PYTHON
            python_table_cache->clear();
#endif
            return res;
        }

        auto res = std::make_unique<CHDB::MaterializedQueryResult>(
            CHDB::ResultBuffer(stealQueryOutputVector()),
            getElapsedTime(),
            getProcessedRows(),
            getProcessedBytes(),
            storage_rows_read,
            storage_bytes_read,
            rows_written,
            bytes_written);
#if USE_PYTHON
        python_table_cache->clear();
#endif
        return res;
    }
    catch (const Exception & e)
    {
#if USE_PYTHON
        python_table_cache->clear();
#endif
        return std::make_unique<CHDB::MaterializedQueryResult>(getExceptionMessage(e, false));
    }
    catch (...)
    {
#if USE_PYTHON
        python_table_cache->clear();
#endif
        return std::make_unique<CHDB::MaterializedQueryResult>(getCurrentExceptionMessage(true));
    }
}

CHDB::QueryResultPtr ChdbClient::executeStreamingInit(
    const char * query, size_t query_len,
    const char * format, size_t format_len, bool dataframe_over_chunks)
{
    std::lock_guard<std::mutex> lock(client_mutex);

    String query_str(query, query_len);
    String format_str(format, format_len);

    if (streaming_insert_context)
        return std::make_unique<CHDB::StreamQueryResult>(
            "Cannot start a streaming query while a streaming insert is active on this connection");

    try
    {
        DB::ThreadStatus thread_status;

        streaming_query_context = std::make_shared<StreamingQueryContext>();
        if (!parseQueryTextWithOutputFormat(query_str, format_str))
        {
            streaming_query_context.reset();
            return std::make_unique<CHDB::StreamQueryResult>(getErrorMsg());
        }
        streaming_query_context->thread_group = DB::CurrentThread::getGroup();
        streaming_query_context->dataframe_over_chunks = dataframe_over_chunks;
        auto result = std::make_unique<CHDB::StreamQueryResult>();
        streaming_query_context->streaming_result = result.get();
        return result;
    }
    catch (const Exception & e)
    {
        streaming_query_context.reset();
        return std::make_unique<CHDB::StreamQueryResult>(getExceptionMessage(e, false));
    }
    catch (...)
    {
        streaming_query_context.reset();
        return std::make_unique<CHDB::StreamQueryResult>(getCurrentExceptionMessage(true));
    }
}

CHDB::QueryResultPtr ChdbClient::executeStreamingIterate(void * streaming_result, bool is_canceled)
{
    std::lock_guard<std::mutex> lock(client_mutex);

    if (!streaming_query_context)
        return std::make_unique<CHDB::MaterializedQueryResult>("No active streaming query");

    try
    {
        DB::ThreadStatus thread_status;

        if (streaming_query_context->thread_group)
        {
            DB::CurrentThread::attachToGroupIfDetached(streaming_query_context->thread_group);
        }
        auto * local_connection = static_cast<LocalConnection *>(connection.get());
        const auto old_processed_rows = getProcessedRows();
        const auto old_processed_bytes = getProcessedBytes();
        size_t old_storage_rows_read = local_connection->getCHDBProgress().read_rows;
        size_t old_storage_bytes_read = local_connection->getCHDBProgress().read_bytes;
        size_t old_rows_written = local_connection->getCHDBProgress().written_rows;
        size_t old_bytes_written = local_connection->getCHDBProgress().written_bytes;
        const auto old_elapsed_time = getElapsedTime();

        CHDB::QueryResultPtr res;
        if (!processStreamingQuery(streaming_result, is_canceled))
        {
            res = std::make_unique<CHDB::MaterializedQueryResult>(getErrorMsg());
        }
        else
        {
            const auto processed_rows = getProcessedRows();
            const auto processed_bytes = getProcessedBytes();
            size_t storage_rows_read = local_connection->getCHDBProgress().read_rows;
            size_t storage_bytes_read = local_connection->getCHDBProgress().read_bytes;
            size_t rows_written = local_connection->getCHDBProgress().written_rows;
            size_t bytes_written = local_connection->getCHDBProgress().written_bytes;
            const auto elapsed_time = getElapsedTime();

            if (Poco::toLower(default_output_format) == CHUNK_COLLECT_FORMAT_NAME)
            {
                auto rows_read = processed_rows - old_processed_rows;
                auto chunk_result = std::make_unique<CHDB::ChunkQueryResult>(
                    std::move(collected_chunks),
                    std::move(collected_chunks_header),
                    elapsed_time - old_elapsed_time,
                    rows_read,
                    processed_bytes - old_processed_bytes,
                    storage_rows_read - old_storage_rows_read,
                    storage_bytes_read - old_storage_bytes_read,
                    rows_written - old_rows_written,
                    bytes_written - old_bytes_written);

#if USE_PYTHON
                if (streaming_query_context->dataframe_over_chunks)
                {
                    py::gil_scoped_acquire acquire;
                    CHDB::PandasDataFrameBuilder builder(*chunk_result);
                    py::handle df = builder.getDataFrame().release();

                    res = std::make_unique<CHDB::DataFrameQueryResult>(df, rows_read);
                }
                else
                {
                    /// Arrow C Data / ADBC consumers want raw chunks.
                    res = std::move(chunk_result);
                }
#else
                res = std::move(chunk_result);
#endif
            }
            else
            {
                auto * output_vec = stealQueryOutputVector();
                bool has_output_data = output_vec && !output_vec->empty();
                if (has_output_data)
                {
                    res = std::make_unique<CHDB::MaterializedQueryResult>(
                        CHDB::ResultBuffer(output_vec),
                        elapsed_time - old_elapsed_time,
                        processed_rows - old_processed_rows,
                        processed_bytes - old_processed_bytes,
                        storage_rows_read - old_storage_rows_read,
                        storage_bytes_read - old_storage_bytes_read,
                        rows_written - old_rows_written,
                        bytes_written - old_bytes_written);
                }
                else
                {
                    delete output_vec;
                    res = std::make_unique<CHDB::MaterializedQueryResult>(nullptr, 0.0, 0, 0, 0, 0);
                }
            }
        }

        /// Check if query should end based on result type
        bool is_end = !res->getError().empty() || is_canceled || res->isEmpty();
        if (is_end)
        {
            // End of stream reached or cancelled, cleanup
            streaming_query_context.reset();
#if USE_PYTHON
            if (connection)
            {
                auto * local_connection = static_cast<LocalConnection *>(connection.get());
                local_connection->resetQueryContext();
                local_connection->getSession().getPythonTableCache()->clear();
            }
#endif
        }
        return res;
    }
    catch (const Exception & e)
    {
        streaming_query_context.reset();
#if USE_PYTHON
        if (connection)
        {
            auto * local_connection = static_cast<LocalConnection *>(connection.get());
            local_connection->resetQueryContext();
        }
        python_table_cache->clear();
#endif
        return std::make_unique<CHDB::MaterializedQueryResult>(getExceptionMessage(e, false));
    }
    catch (...)
    {
        streaming_query_context.reset();
#if USE_PYTHON
        if (connection)
        {
            auto * local_connection = static_cast<LocalConnection *>(connection.get());
            local_connection->resetQueryContext();
        }
        python_table_cache->clear();
#endif
        return std::make_unique<CHDB::MaterializedQueryResult>(getCurrentExceptionMessage(true));
    }
}

void ChdbClient::cancelStreamingQuery(void * streaming_result)
{
    std::lock_guard<std::mutex> lock(client_mutex);

    cancelStreamingQueryWithoutLock(streaming_result);
}

void ChdbClient::cancelStreamingQueryWithoutLock(void * streaming_result)
{
    if (streaming_query_context)
    {
        try
        {
            /// Process the cancellation through ClientBase's streaming query method
            processStreamingQuery(streaming_result, true);
        }
        catch (...)
        {
            /// Ignore errors during cancellation
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        /// Ensure cleanup happens
        streaming_query_context.reset();
#if USE_PYTHON
        if (connection)
        {
            auto * local_connection = static_cast<LocalConnection *>(connection.get());
            local_connection->resetQueryContext();
        }
        python_table_cache->clear();
#endif
    }
}

CHDB::QueryResultPtr ChdbClient::executeInsertStreamingInit(
    const char * query, size_t query_len, const char * format, size_t format_len)
{
    std::lock_guard<std::mutex> lock(client_mutex);

    String query_str(query, query_len);
    String format_str(format, format_len);

    /// Single-active-statement: the connection has one state/input buffer.
    if (streaming_query_context || streaming_insert_context)
        return std::make_unique<CHDB::InsertStreamResult>(
            "Another streaming query or insert is already active on this connection");

    try
    {
        DB::ThreadStatus thread_status;

        if (!connection || !connection->checkConnected(connection_parameters.timeouts))
            connect();

        /// Keep (query, format) split in the public API, but assemble the full
        /// statement so the engine knows the input format and treats the data as
        /// external (streamed) rather than inline.
        String full_query = query_str;
        if (!format_str.empty())
            full_query += " FORMAT " + format_str;

        const auto & settings = client_context->getSettingsRef();
        const char * begin = full_query.data();
        const char * pos = begin;
        const char * end = begin + full_query.size();
        ASTPtr parsed_query = parseQuery(pos, end, settings, /*allow_multi_statements*/ false);
        if (!parsed_query)
            return std::make_unique<CHDB::InsertStreamResult>("Failed to parse INSERT query");

        auto * insert_ast = parsed_query->as<ASTInsertQuery>();
        if (!insert_ast)
            return std::make_unique<CHDB::InsertStreamResult>("send_insert requires an INSERT query");
        if (insert_ast->select)
            return std::make_unique<CHDB::InsertStreamResult>(
                "send_insert does not support INSERT ... SELECT (use a normal query)");

        /// Snapshot the connection settings before applying the INSERT's
        /// SETTINGS clause; restored by restoreSettingsAfterInsertStream() when
        /// the stream ends (mirrors ClientBase's old_settings SCOPE_EXIT, so
        /// e.g. `SETTINGS s3_max_single_part_upload_size=...` on one streaming
        /// insert never leaks into later statements on this connection).
        insert_stream_saved_settings = std::make_unique<Settings>(client_context->getSettingsRef());
        DB::InterpreterSetQuery::applySettingsFromQuery(parsed_query, client_context);
        connection->setFormatSettings(getFormatSettings(client_context));

        client_context->setCurrentQueryId("");

        auto ctx = std::make_shared<CHDB::InsertStreamContext>();
        ctx->queue_buf = std::make_shared<CHDB::QueueReadBuffer>();
        ctx->parsed_query = parsed_query;
        ctx->full_query = full_query;
        ctx->format = format_str.empty() ? String("Values") : format_str;
        ctx->thread_group = DB::CurrentThread::getGroup();

        /// Force non-parallel parsing so the engine builds a pushing pipeline
        /// (awaiting our sendData blocks) rather than a self-reading completed
        /// pipeline. Undone by the settings restore in done()/cancel().
        client_context->setSetting("input_format_parallel_parsing", false);

        streaming_insert_context = ctx;

        /// All connection I/O (sendQuery, receiveSampleBlock, sendData, finalize)
        /// happens on the worker thread, so the pushing pipeline is driven from a
        /// single thread. We block here until the worker reports setup status.
        auto init_future = ctx->init_promise.get_future();
        ctx->worker = ThreadFromGlobalPool([this, ctx]() { runInsertStreamWorker(ctx); });

        String init_error = init_future.get();
        if (!init_error.empty())
        {
            if (ctx->worker.joinable())
                ctx->worker.join();
            restoreSettingsAfterInsertStream();
            streaming_insert_context.reset();
            return std::make_unique<CHDB::InsertStreamResult>(init_error);
        }

        auto result = std::make_unique<CHDB::InsertStreamResult>();
        result->context = ctx;
        result->owner = this;
        return result;
    }
    catch (const Exception & e)
    {
        /// Restore any settings the INSERT already applied (the non-exception
        /// failure path does the same); otherwise they leak on client_context
        /// for all subsequent statements.
        restoreSettingsAfterInsertStream();
        streaming_insert_context.reset();
        return std::make_unique<CHDB::InsertStreamResult>(getExceptionMessage(e, false));
    }
    catch (...)
    {
        restoreSettingsAfterInsertStream();
        streaming_insert_context.reset();
        return std::make_unique<CHDB::InsertStreamResult>(getCurrentExceptionMessage(true));
    }
}

void ChdbClient::restoreSettingsAfterInsertStream()
{
    if (insert_stream_saved_settings)
    {
        client_context->setSettings(*insert_stream_saved_settings);
        insert_stream_saved_settings.reset();
        if (connection)
            connection->setFormatSettings(getFormatSettings(client_context));
    }
}

void ChdbClient::runInsertStreamWorker(const CHDB::InsertStreamContextPtr & ctx)
{
    auto signal_init = [&](const String & err)
    {
        if (!ctx->init_signaled)
        {
            ctx->init_signaled = true;
            ctx->init_promise.set_value(err);
        }
    };

    try
    {
        DB::ThreadStatus thread_status;
        if (ctx->thread_group)
            DB::CurrentThread::attachToGroupIfDetached(ctx->thread_group);

        auto * local_connection = static_cast<LocalConnection *>(connection.get());
        auto * insert_ast = ctx->parsed_query->as<ASTInsertQuery>();

        /// Send the INSERT (without data); the engine sets up a pushing pipeline.
        connection->sendQuery(
            connection_parameters.timeouts,
            ctx->full_query,
            query_parameters,
            client_context->getCurrentQueryId(),
            query_processing_stage,
            &client_context->getSettingsRef(),
            &client_context->getClientInfo(),
            /*with_pending_data*/ true,
            /*external_roles*/ {},
            [](const Progress &) {});

        Block sample;
        ColumnsDescription columns;
        if (!receiveSampleBlock(sample, columns, ctx->parsed_query))
        {
            String msg = getErrorMsg();
            signal_init(msg.empty() ? String("Failed to receive table structure") : msg);
            ctx->worker_done = true;
            return;
        }

        setInsertionTable(*insert_ast);
        ctx->sample = sample;
        ctx->columns = columns;
        ctx->rows_written = local_connection->getCHDBProgress().written_rows;
        ctx->bytes_written = local_connection->getCHDBProgress().written_bytes;

        const auto & settings = client_context->getSettingsRef();
        auto source = client_context->getInputFormat(
            ctx->format,
            *ctx->queue_buf,
            ctx->sample,
            settings[Setting::max_insert_block_size],
            std::nullopt,
            settings[Setting::max_insert_block_size_bytes],
            settings[Setting::min_insert_block_size_rows],
            settings[Setting::min_insert_block_size_bytes]);

        Pipe pipe(source);
        if (ctx->columns.hasDefaults())
        {
            pipe.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<AddingDefaultsTransform>(header, ctx->columns, *source, client_context);
            });
        }

        QueryPipeline pipeline(std::move(pipe));
        pipeline.setConcurrencyControl(false);
        PullingPipelineExecutor executor(pipeline);

        /// Setup succeeded; unblock executeInsertStreamingInit().
        signal_init("");

        Block block;
        while (executor.pull(block))
        {
            if (ctx->cancelled)
            {
                executor.cancel();
                break;
            }
            if (!block.empty())
                connection->sendData(block, "", false);
        }

        if (!ctx->cancelled)
        {
            connection->sendData({}, "", false);
            receiveEndOfQueryForInsert();
        }
    }
    catch (...)
    {
        ctx->error = std::current_exception();
        try
        {
            ctx->error_message = getCurrentExceptionMessage(true);
        }
        catch (...)
        {
        }
        /// Release-store after writing error/error_message so a reader that
        /// observes error_set (acquire) sees them fully written.
        ctx->error_set.store(true, std::memory_order_release);
        signal_init(ctx->error_message.empty() ? String("Streaming insert failed") : ctx->error_message);
    }
    /// Close the queue on the way out (idempotent). Critical on the error
    /// path: the producer may be blocked in a full-queue push, and with the
    /// worker gone nobody would ever pop — without this, a worker failure
    /// (e.g. MEMORY_LIMIT_EXCEEDED) deadlocks the appending thread. After
    /// finish(), a blocked push returns false and append reports the error.
    ctx->queue_buf->finish();
    ctx->worker_done = true;
}

bool ChdbClient::executeInsertStreamingAppend(void * insert_stream, const char * data, size_t len)
{
    CHDB::InsertStreamContextPtr ctx;
    {
        std::lock_guard<std::mutex> lock(client_mutex);
        auto * res = reinterpret_cast<CHDB::InsertStreamResult *>(insert_stream);
        if (!res || !res->context)
            return false;
        ctx = std::static_pointer_cast<CHDB::InsertStreamContext>(res->context);
        /// Only the atomic flag is safe to read concurrently with the worker;
        /// ctx->error/error_message are non-atomic and must not be touched here.
        if (ctx->error_set.load(std::memory_order_acquire) || ctx->worker_done.load())
            return false;
    }
    /// Push outside the lock: append() may block on backpressure while the
    /// worker concurrently drains the queue.
    if (!ctx->queue_buf->append(data, len))
        return false;
    return !ctx->error_set.load(std::memory_order_acquire);
}

CHDB::QueryResultPtr ChdbClient::executeInsertStreamingDone(void * insert_stream)
{
    auto * res = reinterpret_cast<CHDB::InsertStreamResult *>(insert_stream);
    CHDB::InsertStreamContextPtr ctx =
        (res && res->context) ? std::static_pointer_cast<CHDB::InsertStreamContext>(res->context) : nullptr;
    if (!ctx)
        return std::make_unique<CHDB::MaterializedQueryResult>(res ? res->getError() : "Invalid insert stream");

    if (ctx->finalized.exchange(true))
        return std::make_unique<CHDB::MaterializedQueryResult>(
            res->getError().empty() ? String("Insert stream already finalized") : res->getError());

    /// Signal EOF and let the worker drain + finalize. Do not hold client_mutex
    /// across the join.
    ctx->queue_buf->finish();
    if (ctx->worker.joinable())
        ctx->worker.join();

    std::lock_guard<std::mutex> lock(client_mutex);

    /// Guard the connection like the USE_PYTHON block below: if teardown reset
    /// it before we acquired the lock, report zero progress instead of crashing.
    auto * local_connection = static_cast<LocalConnection *>(connection.get());
    uint64_t rows_written = 0;
    uint64_t bytes_written = 0;
    if (local_connection)
    {
        rows_written = local_connection->getCHDBProgress().written_rows - ctx->rows_written;
        bytes_written = local_connection->getCHDBProgress().written_bytes - ctx->bytes_written;
    }
    double elapsed = getElapsedTime();

    /// The worker has been joined (happens-before), so reading error/error_message
    /// is safe here. Treat a set exception_ptr (or error_set) as failure even when
    /// error_message ended up empty (e.g. getCurrentExceptionMessage threw).
    const bool failed = ctx->error_set.load(std::memory_order_acquire) || static_cast<bool>(ctx->error);
    String err = ctx->error_message;
    if (failed && err.empty())
        err = "Streaming insert failed";

    restoreSettingsAfterInsertStream();
    streaming_insert_context.reset();
#if USE_PYTHON
    if (connection)
    {
        local_connection->resetQueryContext();
        local_connection->getSession().getPythonTableCache()->clear();
    }
#endif

    if (failed)
    {
        res->setError(err);
        return std::make_unique<CHDB::MaterializedQueryResult>(err);
    }

    return std::make_unique<CHDB::MaterializedQueryResult>(
        nullptr, elapsed, 0, 0, 0, 0, rows_written, bytes_written);
}

void ChdbClient::cancelInsertStream(void * insert_stream)
{
    auto * res = reinterpret_cast<CHDB::InsertStreamResult *>(insert_stream);
    CHDB::InsertStreamContextPtr ctx =
        (res && res->context) ? std::static_pointer_cast<CHDB::InsertStreamContext>(res->context) : nullptr;
    if (!ctx)
        return;

    if (ctx->finalized.exchange(true))
        return;

    ctx->cancelled = true;
    if (connection)
    {
        try
        {
            connection->sendCancel();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    ctx->queue_buf->finish();
    if (ctx->worker.joinable())
        ctx->worker.join();

    std::lock_guard<std::mutex> lock(client_mutex);
    restoreSettingsAfterInsertStream();
    cancelInsertStreamWithoutLock();
}

void ChdbClient::cancelInsertStreamWithoutLock()
{
    if (streaming_insert_context)
    {
        streaming_insert_context.reset();
#if USE_PYTHON
        if (connection)
        {
            auto * local_connection = static_cast<LocalConnection *>(connection.get());
            local_connection->resetQueryContext();
        }
#endif
    }
}


} // namespace DB
