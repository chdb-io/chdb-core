#pragma once

#include <Client/ClientBase.h>
#include <Client/LocalConnection.h>
#include <Interpreters/Session.h>
#include <Common/Config/ConfigProcessor.h>
#include <Core/Names.h>
#include "QueryResult.h"
#include "StreamingInsert.h"

#include <memory>
#include <mutex>

namespace DB
{
class EmbeddedServer;

/**
 * ChdbClient - Client for executing queries in chDB
 *
 * Designed for chDB's embedded use case and inherits from ClientBase
 * to reuse all query execution logic.
 * Each client has its own LocalConnection.
 * Holds a reference to EmbeddedServer (lifecycle managed by getInstance/releaseInstance).
 */
class ChdbClient : public ClientBase
{
public:
    static std::unique_ptr<ChdbClient> create(EmbeddedServer & server_ref);

    explicit ChdbClient(EmbeddedServer & server_ref);
    ~ChdbClient() override;

    CHDB::QueryResultPtr executeMaterializedQuery(const char * query, size_t query_len, const char * format, size_t format_len);

    CHDB::QueryResultPtr executeStreamingInit(const char * query, size_t query_len, const char * format, size_t format_len);

    CHDB::QueryResultPtr executeStreamingIterate(void * streaming_result, bool is_canceled = false);

    void cancelStreamingQuery(void * streaming_result);

    bool hasStreamingQuery() const;

    /// Streaming INSERT (write side, mirror of the streaming-read methods above).
    /// Init sends the INSERT query, captures the target structure, and spawns a
    /// worker thread that pulls parsed blocks from a caller-fed QueueReadBuffer
    /// and pushes them into the engine. Returns an InsertStreamResult handle
    /// (never null; carries an error message on init failure).
    CHDB::QueryResultPtr executeInsertStreamingInit(
        const char * query, size_t query_len, const char * format, size_t format_len);

    /// Enqueue a chunk of raw, FORMAT-encoded bytes. Returns false on error
    /// (e.g. the worker already failed); the message is available via
    /// getInsertStreamError().
    bool executeInsertStreamingAppend(void * insert_stream, const char * data, size_t len);

    /// Signal end-of-input, join the worker, and return a MaterializedQueryResult
    /// carrying rows_written/bytes_written/elapsed (or the engine error).
    CHDB::QueryResultPtr executeInsertStreamingDone(void * insert_stream);

    /// Abort the INSERT (CH-default semantics: no special rollback) and join.
    void cancelInsertStream(void * insert_stream);

    /// Latest error message for the stream, or empty.
    const char * getInsertStreamError(void * insert_stream) const;

    bool hasInsertStream() const;

    size_t getStorageRowsRead() const;
    size_t getStorageBytesRead() const;

    /// Set named query parameters ({name:Type} placeholders) on the underlying client context.
    /// Used by both the Python binding (params=...) and the C ABI (chdb_query_with_params).
    void setQueryParameters(const NameToNameMap & params);
    void clearQueryParameters();

#if USE_PYTHON
    void findQueryableObjFromPyCache(const String & query_str) const;
#endif

protected:
    void connect() override;
    Poco::Util::LayeredConfiguration & getClientConfiguration() override;
    void processError(std::string_view query) const override;
    String getName() const override { return "chdb"; }
    bool isEmbeeddedClient() const override { return false; }

    void printHelpMessage(const OptionsDescription &) override {}
    void addExtraOptions(OptionsDescription &) override {}
    void processOptions(const OptionsDescription &, const CommandLineOptions &,
                       const std::vector<Arguments> &, const std::vector<Arguments> &) override {}
    void processConfig() override {}
    void setupSignalHandler() override {}

private:
    void cleanup();
    bool parseQueryTextWithOutputFormat(const String & query, const String & format);
    void cancelStreamingQueryWithoutLock(void * streaming_result);

    /// Runs on the worker thread: drives the INSERT pipeline to completion,
    /// pulling from ctx->queue_buf and pushing blocks into the connection.
    void runInsertStreamWorker(const CHDB::InsertStreamContextPtr & ctx);
    void cancelInsertStreamWithoutLock();

    EmbeddedServer & server;
    std::unique_ptr<Session> session;
    ConfigurationPtr configuration;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> layered_configuration;
    std::unique_ptr<ReadBufferFromFile> input;
#if USE_PYTHON
    std::shared_ptr<CHDB::PythonTableCache> python_table_cache;
#endif
    CHDB::InsertStreamContextPtr streaming_insert_context;
    mutable std::mutex client_mutex;
};

} // namespace DB
