#pragma once

#include <Interpreters/Session.h>
#include <Common/Stopwatch.h>
#include <Core/Names.h>
#include <IO/Progress.h>
#include "chdb.h"
#include "QueryResult.h"
#include "StreamingInsert.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

namespace DB
{
class EmbeddedDatabase;
class EmbeddedServer;
class QueryStatus;
using EmbeddedDatabasePtr = std::shared_ptr<EmbeddedDatabase>;
}

namespace CHDB
{
class ArrowStreamRegistry;
class PythonTableCache;

/// Per-query cancellation anchor shared between the running query/stream and
/// `InterruptController`. Kept separate from `EmbeddedSession::session_mutex`
/// so `chdb_interrupt(conn)` can signal `QueryStatus::cancelQuery` from any
/// watchdog, signal-handler relay, or concurrent thread without blocking.
struct ActiveQuerySlot
{
    std::atomic<bool> interrupt_requested{false};
    std::mutex status_mutex;
    std::weak_ptr<DB::QueryStatus> query_status;
    std::shared_ptr<QueueReadBuffer> queue_buf;
    std::atomic<bool> * external_cancel_flag = nullptr;

    void bindQueryStatus(const std::shared_ptr<DB::QueryStatus> & status) noexcept;
    void cancel() noexcept;
};

class InterruptController
{
public:
    void registerSlot(const std::shared_ptr<ActiveQuerySlot> & slot);
    void unregisterSlot(const ActiveQuerySlot * slot) noexcept;
    void interruptAll() noexcept;

private:
    std::mutex mutex;
    std::vector<std::weak_ptr<ActiveQuerySlot>> slots;
};
} // namespace CHDB

namespace DB
{

struct StreamExecutionState;

/**
 * EmbeddedSession - Lightweight session-backed query runner for chDB.
 *
 * Drives `DB::Session` + `DB::executeQuery` directly without inheriting from
 * interactive `ClientBase` or emulating packets through `LocalConnection`:
 * - Parses each SQL statement once inside `DB::executeQuery`.
 * - Creates one `query_context` per statement via `Session::makeQueryContext()`,
 *   with zero extra `DB::Settings` full-struct snapshots/rollbacks per query or
 *   per streamed chunk.
 * - Guards session state with `std::shared_mutex` (shared lock during microsecond
 *   `makeQueryContext()`, exclusive lock only for session-mutating statements)
 *   and stores streaming pipeline state on `StreamQueryResult`, enabling
 *   concurrent read queries and multiple simultaneous streaming cursors on a
 *   single connection.
 */
class EmbeddedSession
{
public:
    using ProgressValuesCallback = std::function<void(const ProgressValues &, UInt64)>;

    static std::unique_ptr<EmbeddedSession> create(
        EmbeddedDatabasePtr database_, int argc = 0, char ** argv = nullptr, bool force_read_only = false);

    explicit EmbeddedSession(
        EmbeddedDatabasePtr database_, int argc = 0, char ** argv = nullptr, bool force_read_only = false);
    ~EmbeddedSession();

    EmbeddedSession(const EmbeddedSession &) = delete;
    EmbeddedSession & operator=(const EmbeddedSession &) = delete;

    CHDB::QueryResultPtr executeMaterializedQuery(
        const char * query, size_t query_len, const char * format, size_t format_len);

    CHDB::QueryResultPtr executeStreamingInit(
        const char * query, size_t query_len, const char * format, size_t format_len, bool dataframe_over_chunks = true);

    CHDB::QueryResultPtr executeStreamingIterate(void * streaming_result, bool is_canceled = false);

    void cancelStreamingQuery(void * streaming_result);

    CHDB::QueryResultPtr executeInsertStreamingInit(
        const char * query, size_t query_len, const char * format, size_t format_len);

    bool executeInsertStreamingAppend(void * insert_stream, const char * data, size_t len);

    CHDB::QueryResultPtr executeInsertStreamingDone(void * insert_stream);

    void cancelInsertStream(void * insert_stream);

    void analyzeQuery(
        const char * sql, size_t sql_len, std::string_view target_database, chdb_query_analysis_v1 & out);

    void setConnectionSetting(const String & key, const String * value);

    bool getConnectionSetting(const String & key, String & value) const;

    void setProgressValuesCallback(ProgressValuesCallback callback);

    void interrupt() noexcept;
    const std::shared_ptr<CHDB::InterruptController> & getInterruptController() const noexcept { return interrupt_controller; }
    const EmbeddedDatabasePtr & getDatabase() const noexcept { return database; }

    CHDB::ArrowStreamRegistry * getArrowStreamRegistry() const { return arrow_stream_registry.get(); }
#if USE_PYTHON
    CHDB::PythonTableCache * getPythonTableCache() const { return python_table_cache.get(); }
#endif

    /// True when the calling thread is already inside a callback of this session, or is a
    /// pipeline worker thread executing a query belonging to this session.
    bool isActiveOnCurrentThreadOrQuery() const;

    using CallQueryParameters = std::pair<const EmbeddedSession *, const NameToNameMap *>;
    static CallQueryParameters setCallQueryParameters(CallQueryParameters call);

#if USE_PYTHON
    void findQueryableObjFromPyCache(const String & query_str) const;
    void bindPythonTablesForQuery(const String & query_str, const void * explicit_tables_pyobj) const;
#endif

private:
    void cleanup() noexcept;
    void applyCmdSettings(int argc, char ** argv);
    void checkNotReentrant() const;
    const NameToNameMap * currentCallParameters() const noexcept;

    ContextMutablePtr makeQueryContextLocked(
        const NameToNameMap * call_params,
        const char * format,
        size_t format_len,
        bool is_chunk_collect) const;

    void attachProgressAndCancelCallbacks(
        const ContextMutablePtr & query_context,
        const std::shared_ptr<Progress> & query_progress,
        const std::shared_ptr<Stopwatch> & query_watch,
        const std::shared_ptr<CHDB::ActiveQuerySlot> & interrupt_slot) const;

    void syncSessionParametersFromSetQuery(const IAST & ast, bool already_holds_unique_lock);

    void registerActiveStream(const std::shared_ptr<StreamExecutionState> & state);
    void cancelAllActiveStreams() noexcept;

    void runInsertStreamWorker(const CHDB::InsertStreamContextPtr & ctx);

    EmbeddedDatabasePtr database;
    EmbeddedServer & server;
    std::unique_ptr<Session> session;
    ContextMutablePtr session_context;
    const Context * session_context_raw = nullptr;
    NameToNameMap query_parameters;
    bool print_stack_trace = false;

#if USE_PYTHON
    std::shared_ptr<CHDB::PythonTableCache> python_table_cache;
#endif
    std::shared_ptr<CHDB::ArrowStreamRegistry> arrow_stream_registry;
    std::shared_ptr<CHDB::InterruptController> interrupt_controller;

    mutable std::mutex progress_callback_mutex;
    ProgressValuesCallback progress_values_callback;

    mutable std::mutex active_streams_mutex;
    std::vector<std::weak_ptr<StreamExecutionState>> active_streams;

    CHDB::InsertStreamContextPtr streaming_insert_context;
    mutable std::shared_mutex session_mutex;
};

} // namespace DB
