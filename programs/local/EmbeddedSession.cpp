#include <EmbeddedSession.h>
#include <EmbeddedServer.h>
#include <QueryClassifier.h>
#include <chdb-internal.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/QueryScope.h>
#include <Common/SettingsChanges.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils.h>
#include <Common/ThreadStatus.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/CompressionMethod.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ForkWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFromJSON.h>
#include <Parsers/Kusto/parseKQLQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/LogsQL/ParserLogsQLQuery.h>
#include <Parsers/LogsQL/parseLogsQLQuery.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/Polyglot/ParserPolyglotQuery.h>
#include <Parsers/Prometheus/ParserPrometheusQuery.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/Trino/ParserTrinoQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/String.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFile.h>
#include <base/scope_guard.h>

#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <utility>

#if defined(OS_WASM)
namespace DB
{
bool chdbWasmCancelRequested();
void notifyCHDBWasmProgress(
    uint64_t read_rows, uint64_t total_rows_to_read, uint64_t read_bytes,
    uint64_t total_bytes_to_read, uint64_t elapsed_ns);
}
#endif

#if !defined(OS_WASM)
#    include <ArrowStreamRegistry.h>
#endif
#if USE_PYTHON
#    include <PybindWrapper.h>
#    include <PythonTableCache.h>
#endif

namespace ProfileEvents
{
extern const Event FileProgressCallbackInvocations;
}

namespace CHDB
{

void ActiveQuerySlot::bindQueryStatus(const std::shared_ptr<DB::QueryStatus> & status) noexcept
{
    if (!status)
        return;

    bool already_interrupted = false;
    {
        std::lock_guard<std::mutex> lock(status_mutex);
        query_status = status;
        already_interrupted = interrupt_requested.load(std::memory_order_acquire);
    }

    if (already_interrupted)
    {
        try
        {
            status->cancelQuery(DB::CancelReason::CANCELLED_BY_USER);
        }
        catch (...)
        {
        }
    }
}

void ActiveQuerySlot::cancel() noexcept
{
    interrupt_requested.store(true, std::memory_order_release);
    if (external_cancel_flag)
        external_cancel_flag->store(true, std::memory_order_release);

    std::shared_ptr<DB::QueryStatus> status;
    {
        std::lock_guard<std::mutex> lock(status_mutex);
        status = query_status.lock();
    }

    if (status)
    {
        try
        {
            status->cancelQuery(DB::CancelReason::CANCELLED_BY_USER);
        }
        catch (...)
        {
        }
    }

    if (queue_buf)
    {
        try
        {
            queue_buf->finish();
        }
        catch (...)
        {
        }
    }
}

void InterruptController::registerSlot(const std::shared_ptr<ActiveQuerySlot> & slot)
{
    if (!slot)
        return;

    std::lock_guard<std::mutex> lock(mutex);
    if (slots.size() >= 16)
        std::erase_if(slots, [](const auto & w) { return w.expired(); });
    slots.push_back(slot);
}

void InterruptController::unregisterSlot(const ActiveQuerySlot * slot) noexcept
{
    if (!slot)
        return;

    std::lock_guard<std::mutex> lock(mutex);
    std::erase_if(slots, [slot](const auto & w)
    {
        auto locked = w.lock();
        return !locked || locked.get() == slot;
    });
}

void InterruptController::interruptAll() noexcept
{
    std::vector<std::shared_ptr<ActiveQuerySlot>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex);
        snapshot.reserve(slots.size());
        for (auto it = slots.begin(); it != slots.end();)
        {
            if (auto locked = it->lock())
            {
                snapshot.push_back(std::move(locked));
                ++it;
            }
            else
            {
                it = slots.erase(it);
            }
        }
    }

    for (const auto & slot : snapshot)
        slot->cancel();
}

} // namespace CHDB

namespace DB
{

namespace fs = std::filesystem;

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_APPEND_TO_FILE;
extern const int CANNOT_CREATE_DIRECTORY;
extern const int CANNOT_WRITE_TO_FILE;
extern const int FILE_ALREADY_EXISTS;
extern const int INVALID_USAGE_OF_INPUT;
extern const int LOGICAL_ERROR;
extern const int NO_DATA_TO_INSERT;
extern const int QUERY_WAS_CANCELLED;
extern const int SUPPORT_IS_DISABLED;
extern const int SYNTAX_ERROR;
extern const int UNKNOWN_EXCEPTION;
}

namespace Setting
{
extern const SettingsBool allow_settings_after_format_in_insert;
extern const SettingsString default_format;
extern const SettingsDialect dialect;
extern const SettingsBool enable_json_ast_dialect;
extern const SettingsBool enable_logsql_dialect;
extern const SettingsBool enable_polyglot_dialect;
extern const SettingsBool enable_trino_dialect;
extern const SettingsString format;
extern const SettingsBool ignore_format_null_for_explain;
extern const SettingsBool implicit_select;
extern const SettingsString input_format;
extern const SettingsBool input_format_defaults_for_omitted_fields;
extern const SettingsBool into_outfile_create_parent_directories;
extern const SettingsString logsql_database;
extern const SettingsString logsql_message_column;
extern const SettingsString logsql_table;
extern const SettingsString logsql_time_column;
extern const SettingsUInt64 max_ast_depth;
extern const SettingsUInt64 max_ast_elements;
extern const SettingsNonZeroUInt64 max_block_size;
extern const SettingsNonZeroUInt64 max_insert_block_size;
extern const SettingsUInt64 max_insert_block_size_bytes;
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_query_size;
extern const SettingsUInt64 min_insert_block_size_bytes;
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsString output_format;
extern const SettingsString polyglot_dialect;
extern const SettingsString promql_database;
extern const SettingsDoubleAuto promql_evaluation_time;
extern const SettingsString promql_table;
extern const SettingsSnappyMode snappy_mode;
extern const SettingsBool throw_if_no_data_to_insert;
}

namespace
{

struct ActiveSessionFrame
{
    const EmbeddedSession * session;
    ActiveSessionFrame * prev;
};

thread_local ActiveSessionFrame * active_session_stack = nullptr;

class ScopedActiveSessionFrame
{
public:
    explicit ScopedActiveSessionFrame(const EmbeddedSession * self) noexcept
        : frame{self, active_session_stack}
    {
        active_session_stack = &frame;
    }

    ~ScopedActiveSessionFrame()
    {
        active_session_stack = frame.prev;
    }

    ScopedActiveSessionFrame(const ScopedActiveSessionFrame &) = delete;
    ScopedActiveSessionFrame & operator=(const ScopedActiveSessionFrame &) = delete;

private:
    ActiveSessionFrame frame;
};

/// Ensures `CurrentThread` is initialized on the calling thread without double-constructing
/// `ThreadStatus` when invoked from an engine worker thread, and restores any outer `ThreadGroup`.
class ScopedSessionThreadStatus
{
public:
    ScopedSessionThreadStatus()
    {
        if (!CurrentThread::isInitialized())
        {
            owned_status.emplace();
        }
        else if ((saved_group = CurrentThread::getGroup()) != nullptr)
        {
            CurrentThread::detachFromGroupIfNotDetached();
        }
    }

    ~ScopedSessionThreadStatus()
    {
        if (CurrentThread::isInitialized() && CurrentThread::getGroup() != nullptr)
            CurrentThread::detachFromGroupIfNotDetached();
        if (saved_group)
            CurrentThread::attachToGroupIfDetached(saved_group);
    }

    ScopedSessionThreadStatus(const ScopedSessionThreadStatus &) = delete;
    ScopedSessionThreadStatus & operator=(const ScopedSessionThreadStatus &) = delete;

private:
    std::optional<ThreadStatus> owned_status;
    ThreadGroupPtr saved_group;
};

thread_local EmbeddedSession::CallQueryParameters call_query_parameters{nullptr, nullptr};

#if USE_PYTHON
thread_local UInt64 python_tables_bind_token = 0;

UInt64 takePythonTablesBindToken() noexcept
{
    return std::exchange(python_tables_bind_token, 0);
}

bool isJSONSupportedForFormat(const char * format, size_t format_len)
{
    if (!format || format_len == 0)
        return true;

    String lower_format{format, format_len};
    std::transform(lower_format.begin(), lower_format.end(), lower_format.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    return !(lower_format == "arrow" || lower_format == "parquet" || lower_format == "arrowstream"
             || lower_format == "protobuf" || lower_format == "protobuflist" || lower_format == "protobufsingle");
}
#endif

std::string_view connectionParameterName(std::string_view key) noexcept
{
    constexpr std::string_view prefix = "param_";
    return key.starts_with(prefix) ? key.substr(prefix.size()) : std::string_view{};
}

bool tokenEqualsCI(const Token & tok, std::string_view word) noexcept
{
    if (tok.size() != word.size())
        return false;
    for (size_t i = 0; i < word.size(); ++i)
    {
        if (std::tolower(static_cast<unsigned char>(tok.begin[i]))
            != std::tolower(static_cast<unsigned char>(word[i])))
            return false;
    }
    return true;
}

std::string unquoteTokenName(const Token & tok)
{
    if (tok.size() >= 2 && (tok.begin[0] == '`' || tok.begin[0] == '"' || tok.begin[0] == '\''))
        return std::string(tok.begin + 1, tok.end - 1);
    return std::string(tok.begin, tok.end);
}

struct StatementSlice
{
    std::string_view text;
    bool mutates_session = false;
};

/// Fast zero-AST lexical statement splitter. Respects SQL comments, string literals,
/// quoted identifiers, bracket/paren/brace nesting, and inline INSERT payloads
/// (`VALUES (...)` terminated by `;`, other `FORMAT <fmt>` payloads terminated by `\n\n`).
std::vector<StatementSlice> splitMultiQueryStatements(
    const char * all_begin,
    const char * all_end,
    bool allow_settings_after_format_in_insert)
{
    std::vector<StatementSlice> slices;
    const char * cursor = all_begin;

    while (cursor < all_end)
    {
        Lexer lexer(cursor, all_end);
        Token tok = lexer.nextToken();
        while (!tok.isEnd() && (!tok.isSignificant() || tok.type == TokenType::Semicolon))
            tok = lexer.nextToken();

        if (tok.isEnd())
            break;

        const char * stmt_begin = tok.begin;
        bool mutates_session = false;

        if (tok.type == TokenType::BareWord)
        {
            if (tokenEqualsCI(tok, "SET") || tokenEqualsCI(tok, "USE")
                || tokenEqualsCI(tok, "BEGIN") || tokenEqualsCI(tok, "START")
                || tokenEqualsCI(tok, "COMMIT") || tokenEqualsCI(tok, "ROLLBACK"))
            {
                mutates_session = true;
            }
        }

        bool is_insert = (tok.type == TokenType::BareWord && tokenEqualsCI(tok, "INSERT"));
        bool is_explain = (tok.type == TokenType::BareWord && tokenEqualsCI(tok, "EXPLAIN"));
        bool is_ddl_temp_candidate = (tok.type == TokenType::BareWord
            && (tokenEqualsCI(tok, "CREATE") || tokenEqualsCI(tok, "DROP")
                || tokenEqualsCI(tok, "ALTER") || tokenEqualsCI(tok, "RENAME")
                || tokenEqualsCI(tok, "TRUNCATE")));

        bool has_infile = false;
        bool has_select = false;
        bool saw_input_fn = false;
        bool prev_was_from = false;
        bool prev_was_input = false;

        int paren_depth = 0;
        int bracket_depth = 0;
        int curly_depth = 0;

        const char * stmt_end = all_end;
        const char * next_cursor = all_end;

        while (!tok.isEnd())
        {
            if (tok.isError())
            {
                /// Hand the unsplit remainder to DB::executeQuery so the engine parser
                /// reports its canonical syntax error.
                stmt_end = all_end;
                next_cursor = all_end;
                break;
            }

            if (tok.isSignificant())
            {
                if (prev_was_input && tok.type == TokenType::OpeningRoundBracket)
                    saw_input_fn = true;
                prev_was_input = false;

                if (tok.type == TokenType::OpeningRoundBracket)
                    ++paren_depth;
                else if (tok.type == TokenType::ClosingRoundBracket && paren_depth > 0)
                    --paren_depth;
                else if (tok.type == TokenType::OpeningSquareBracket)
                    ++bracket_depth;
                else if (tok.type == TokenType::ClosingSquareBracket && bracket_depth > 0)
                    --bracket_depth;
                else if (tok.type == TokenType::OpeningCurlyBrace)
                    ++curly_depth;
                else if (tok.type == TokenType::ClosingCurlyBrace && curly_depth > 0)
                    --curly_depth;
                else if (paren_depth == 0 && bracket_depth == 0 && curly_depth == 0)
                {
                    if (tok.type == TokenType::Semicolon)
                    {
                        stmt_end = tok.begin;
                        next_cursor = tok.end;
                        break;
                    }

                    if (tok.type == TokenType::BareWord)
                    {
                        if (is_ddl_temp_candidate
                            && (tokenEqualsCI(tok, "TEMPORARY") || tokenEqualsCI(tok, "TEMP")))
                        {
                            mutates_session = true;
                        }

                        if ((is_explain || !is_insert) && tokenEqualsCI(tok, "INSERT"))
                            is_insert = true;

                        if (is_insert)
                        {
                            if (prev_was_from && tokenEqualsCI(tok, "INFILE"))
                                has_infile = true;
                            prev_was_from = tokenEqualsCI(tok, "FROM");

                            if (tokenEqualsCI(tok, "SELECT"))
                                has_select = true;

                            if (tokenEqualsCI(tok, "FORMAT") && !has_infile && (!has_select || saw_input_fn))
                            {
                                Token fmt_tok = lexer.nextToken();
                                while (!fmt_tok.isEnd() && !fmt_tok.isSignificant())
                                    fmt_tok = lexer.nextToken();

                                if (!fmt_tok.isEnd() && !fmt_tok.isError()
                                    && (fmt_tok.type == TokenType::BareWord || fmt_tok.type == TokenType::QuotedIdentifier))
                                {
                                    const std::string fmt_name = unquoteTokenName(fmt_tok);
                                    const char * after_fmt = fmt_tok.end;

                                    if (allow_settings_after_format_in_insert)
                                    {
                                        Lexer lookahead(after_fmt, all_end);
                                        Token maybe_settings = lookahead.nextToken();
                                        while (!maybe_settings.isEnd() && !maybe_settings.isSignificant())
                                            maybe_settings = lookahead.nextToken();
                                        if (maybe_settings.type == TokenType::BareWord
                                            && tokenEqualsCI(maybe_settings, "SETTINGS"))
                                        {
                                            /// Advance past `key = expr, ...` up to newline or `;`.
                                            const char * nl = static_cast<const char *>(
                                                std::memchr(maybe_settings.end, '\n', static_cast<size_t>(all_end - maybe_settings.end)));
                                            if (nl)
                                                after_fmt = nl;
                                        }
                                    }

                                    if (!boost::iequals(fmt_name, "Values"))
                                    {
                                        const char * data_pos = after_fmt;
                                        if (data_pos < all_end && *data_pos != ';')
                                        {
                                            while (data_pos < all_end && (*data_pos == ' ' || *data_pos == '\t' || *data_pos == '\f'))
                                                ++data_pos;
                                            if (data_pos < all_end && *data_pos == '\r')
                                                ++data_pos;
                                            if (data_pos < all_end && *data_pos == '\n')
                                                ++data_pos;

                                            const std::string_view rem(data_pos, static_cast<size_t>(all_end - data_pos));
                                            const size_t blank = rem.find("\n\n");
                                            if (blank != std::string_view::npos)
                                            {
                                                stmt_end = data_pos + blank;
                                                next_cursor = stmt_end + 2;
                                            }
                                            else
                                            {
                                                stmt_end = all_end;
                                                next_cursor = all_end;
                                            }
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        /// Values format: use ClickHouse's Values row skipper so trailing comments
                                        /// and semicolons inside tuples are handled identically to ClientBase.
                                        const char * data_pos = after_fmt;
                                        ReadBufferFromMemory data_in(data_pos, static_cast<size_t>(all_end - data_pos));
                                        skipBOMIfExists(data_in);
                                        do
                                        {
                                            skipWhitespaceAndSQLComments(data_in);
                                            if (data_in.eof() || *data_in.position() == ';')
                                                break;
                                        } while (ValuesBlockInputFormat::skipToNextRow(&data_in, 1, 0));

                                        stmt_end = data_pos + data_in.count();
                                        next_cursor = (stmt_end < all_end && *stmt_end == ';') ? stmt_end + 1 : stmt_end;
                                        break;
                                    }
                                }
                            }
                            else if (tokenEqualsCI(tok, "VALUES") && !has_infile && !has_select)
                            {
                                const char * data_pos = tok.end;
                                ReadBufferFromMemory data_in(data_pos, static_cast<size_t>(all_end - data_pos));
                                skipBOMIfExists(data_in);
                                do
                                {
                                    skipWhitespaceAndSQLComments(data_in);
                                    if (data_in.eof() || *data_in.position() == ';')
                                        break;
                                } while (ValuesBlockInputFormat::skipToNextRow(&data_in, 1, 0));

                                stmt_end = data_pos + data_in.count();
                                next_cursor = (stmt_end < all_end && *stmt_end == ';') ? stmt_end + 1 : stmt_end;
                                break;
                            }
                        }
                    }
                    else
                    {
                        prev_was_from = false;
                    }
                }

                if (tok.type == TokenType::BareWord && tokenEqualsCI(tok, "input"))
                    prev_was_input = true;
            }

            tok = lexer.nextToken();
        }

        if (stmt_end > stmt_begin)
            slices.push_back(StatementSlice{std::string_view(stmt_begin, static_cast<size_t>(stmt_end - stmt_begin)), mutates_session});

        cursor = next_cursor;
    }

    return slices;
}

/// Checks whether a single statement's leading keyword can be a streamable SELECT/WITH/FROM/(SELECT ...) query.
bool isPotentiallyStreamableLeadingToken(std::string_view stmt, Dialect dialect) noexcept
{
    if (dialect != Dialect::clickhouse && dialect != Dialect::trino && dialect != Dialect::polyglot)
        return true;

    Lexer lexer(stmt.data(), stmt.data() + stmt.size());
    Token tok = lexer.nextToken();
    while (!tok.isEnd() && (!tok.isSignificant() || tok.type == TokenType::OpeningRoundBracket))
        tok = lexer.nextToken();

    if (tok.isEnd() || tok.isError())
        return true; /// Let executeQuery produce the syntax error.

    if (tok.type != TokenType::BareWord)
        return true;

    return tokenEqualsCI(tok, "SELECT") || tokenEqualsCI(tok, "WITH")
        || tokenEqualsCI(tok, "FROM") || tokenEqualsCI(tok, "VALUES");
}

bool isChunkCollectFormat(std::string_view format) noexcept
{
    return boost::iequals(format, CHDB::kChunkCollectFormatName);
}

void fillResultSchemaFromHeader(CHDB::QueryResult & res, const Block & header)
{
    res.column_names.clear();
    res.column_types.clear();
    res.column_names.reserve(header.columns());
    res.column_types.reserve(header.columns());
    for (const auto & col : header)
    {
        res.column_names.push_back(col.name);
        res.column_types.push_back(col.type->getName());
    }
}

QueryPipeline createPipeFromInsertInfile(
    const ASTInsertQuery & insert,
    const String & format,
    const ColumnsDescription & columns_description,
    const Block & sample,
    ContextPtr context)
{
    const auto in_file = insert.infile->as<ASTLiteral &>().value.safeGet<String>();

    String compression_method;
    if (insert.compression)
        compression_method = insert.compression->as<ASTLiteral &>().value.safeGet<String>();

    ColumnsDescription columns_for_storage_file;
    for (const auto & column_in_sample : sample)
    {
        ColumnDescription column = columns_description.has(column_in_sample.name)
            ? columns_description.get(column_in_sample.name)
            : ColumnDescription(column_in_sample.name, column_in_sample.type);
        column.type = column_in_sample.type;
        column.default_desc.kind = ColumnDefaultKind::Default;
        columns_for_storage_file.add(std::move(column));
    }

    StorageFile::CommonArguments args{
        WithContext(context),
        insert.table_id,
        format,
        getFormatSettings(context),
        compression_method,
        columns_for_storage_file,
        ConstraintsDescription{},
        String{},
        {},
    };
    StoragePtr storage = std::make_shared<StorageFile>(StorageFile::FileSource::parse(in_file, context), args);
    storage->startup();

    auto metadata = storage->getInMemoryMetadataPtr(context, false);
    SelectQueryInfo query_info;
    QueryPlan plan;
    storage->read(
        plan,
        sample.getNames(),
        storage->getStorageSnapshot(metadata, context),
        query_info,
        context,
        {},
        context->getSettingsRef()[Setting::max_block_size],
        getNumberOfCPUCoresToUse());

    auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    QueryPlanResourceHolder resources;
    QueryPipeline pipeline(QueryPipelineBuilder::getPipe(std::move(*builder), resources));
    pipeline.addResources(std::move(resources));
    pipeline.addStorageHolder(storage);
    return pipeline;
}

String resolveEffectiveOutputFormat(
    const ContextPtr & query_context,
    const ASTPtr & parsed_query,
    const String & caller_format,
    bool outfile_has_ext_format,
    const String & outfile_ext_format)
{
    const bool has_explicit_caller_format = !caller_format.empty();
    String current_format = has_explicit_caller_format ? caller_format : String("TabSeparated");
    bool has_format_clause = false;

    if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get()))
    {
        if (query_with_output->format_ast != nullptr)
        {
            has_format_clause = true;
            current_format = getIdentifierName(query_with_output->format_ast);
            const bool ignore_null_for_explain = query_context->getSettingsRef()[Setting::ignore_format_null_for_explain];
            if (boost::iequals(current_format, "Null") && parsed_query->as<ASTExplainQuery>() && ignore_null_for_explain)
                current_format = has_explicit_caller_format ? caller_format : String("TabSeparated");
        }
        else if (outfile_has_ext_format)
        {
            current_format = outfile_ext_format;
        }
    }

    const auto & settings = query_context->getSettingsRef();
    if (!settings[Setting::output_format].value.empty())
        current_format = settings[Setting::output_format];
    else if (!settings[Setting::format].value.empty())
        current_format = settings[Setting::format];
    else if (!has_explicit_caller_format && !has_format_clause && !outfile_has_ext_format
             && !settings[Setting::default_format].value.empty())
        current_format = settings[Setting::default_format];

    return current_format;
}

struct OutfileTargetState
{
    std::unique_ptr<WriteBuffer> file_buf;
    std::unique_ptr<ForkWriteBuffer> fork_buf;
    String actual_write_path;
    String atomic_rename_dest;
    String non_empty_append_path;
    String ext_format;
    bool has_ext_format = false;
    bool into_file = false;
    bool into_file_and_stdout = false;

    void cleanupTempOnError() noexcept
    {
        if (!atomic_rename_dest.empty() && !actual_write_path.empty())
        {
            std::error_code ec;
            fs::remove(actual_write_path, ec);
        }
    }

    void finalizeAndCommit()
    {
        if (fork_buf)
            fork_buf->finalize();
        else if (file_buf)
            file_buf->finalize();

        if (!atomic_rename_dest.empty())
        {
            try
            {
                fs::rename(actual_write_path, atomic_rename_dest);
            }
            catch (const fs::filesystem_error & e)
            {
                std::error_code ec;
                fs::remove(actual_write_path, ec);
                throw Exception(
                    ErrorCodes::CANNOT_WRITE_TO_FILE,
                    "Cannot rename temporary file {} to {}: {}",
                    actual_write_path,
                    atomic_rename_dest,
                    e.what());
            }
        }
    }
};

OutfileTargetState prepareOutfileIfAny(
    const ASTPtr & parsed_query,
    const ContextPtr & query_context,
    WriteBuffer * memory_buf)
{
    OutfileTargetState out;
    const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get());
    if (!query_with_output || !query_with_output->out_file)
        return out;

    out.into_file = true;
    out.into_file_and_stdout = query_with_output->isIntoOutfileWithStdout();

    String out_file = query_with_output->out_file->as<ASTLiteral &>().value.safeGet<String>();
    if (query_with_output->isOutfileAppend() && query_with_output->isOutfileTruncate())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot use INTO OUTFILE with APPEND and TRUNCATE simultaneously.");

    if (auto fmt = FormatFactory::instance().tryGetFormatFromFileName(out_file))
    {
        out.ext_format = *fmt;
        out.has_ext_format = true;
    }

    String compression_method_str;
    if (query_with_output->compression)
        compression_method_str = query_with_output->compression->as<ASTLiteral &>().value.safeGet<String>();

    CompressionMethod compression_method = chooseCompressionMethod(out_file, compression_method_str);
    UInt64 compression_level = 3;

    if (query_with_output->isOutfileAppend() && compression_method != CompressionMethod::None)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot append to compressed file. Please use uncompressed file or remove APPEND keyword.");

    if (query_with_output->compression_level)
    {
        const auto & level_node = query_with_output->compression_level->as<ASTLiteral &>();
        bool ok = level_node.value.tryGet<UInt64>(compression_level);
        auto range = getCompressionLevelRange(compression_method);
        if (!ok || compression_level < range.first || compression_level > range.second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid compression level, must be positive integer in range {}-{}",
                range.first,
                range.second);
    }

    if (query_context->getSettingsRef()[Setting::into_outfile_create_parent_directories])
    {
        fs::path parent_dir = fs::path(out_file).parent_path();
        if (!parent_dir.empty() && !fs::exists(parent_dir))
        {
            try
            {
                fs::create_directories(parent_dir);
            }
            catch (const fs::filesystem_error & e)
            {
                throw Exception(
                    ErrorCodes::CANNOT_CREATE_DIRECTORY,
                    "Cannot create parent directories for INTO OUTFILE '{}': {}",
                    parent_dir.string(),
                    e.what());
            }
        }
    }

    const bool exists = fs::exists(out_file);
    if (exists && !query_with_output->isOutfileAppend() && !query_with_output->isOutfileTruncate())
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File {} exists, consider using APPEND or TRUNCATE.", out_file);

    String write_path = out_file;
    int flags = O_WRONLY | O_EXCL | O_CREAT;
    if (exists && query_with_output->isOutfileTruncate() && fs::is_regular_file(out_file))
    {
        out.atomic_rename_dest = out_file;
        fs::path p(out_file);
        write_path = (p.parent_path() / fmt::format("tmp_{}.{}", UUIDHelpers::generateV4(), p.filename().string())).string();
    }
    else if (exists && query_with_output->isOutfileAppend())
    {
        flags = O_WRONLY | O_APPEND;
    }
    else if (exists && query_with_output->isOutfileTruncate())
    {
        flags = O_WRONLY | O_TRUNC;
    }

    out.actual_write_path = write_path;
    auto raw_file = std::make_unique<WriteBufferFromFile>(write_path, DBMS_DEFAULT_BUFFER_SIZE, flags);
    if ((flags & O_APPEND) && raw_file->size() > 0)
        out.non_empty_append_path = out_file;

    out.file_buf = wrapWriteBufferWithCompressionMethod(
        std::move(raw_file),
        compression_method,
        static_cast<int>(compression_level),
        /*zstd_window_log=*/0,
        query_context->getSettingsRef()[Setting::snappy_mode]);

    if (out.into_file_and_stdout && memory_buf)
    {
        /// Wrap non-owning WriteBuffer reference alongside owned file_buf? ForkWriteBuffer takes shared_ptrs;
        /// pass a no-op deleter for memory_buf since the caller finalizes memory_buf separately.
        std::shared_ptr<WriteBuffer> file_shared(out.file_buf.release());
        std::shared_ptr<WriteBuffer> mem_shared(memory_buf, [](WriteBuffer *) {});
        out.fork_buf = std::make_unique<ForkWriteBuffer>(ForkWriteBuffer::WriteBufferPtrs{file_shared, mem_shared});
    }

    return out;
}

/// Parses one statement for `analyzeQuery` (`chdb_classify_query_n`), honoring the session's active dialect.
ASTPtr parseStatementForClassification(
    const char *& pos,
    const char * end,
    const Settings & settings)
{
    const Dialect dialect = settings[Setting::dialect];
    const size_t max_query_size = settings[Setting::max_query_size];
    const uint32_t max_depth = static_cast<uint32_t>(settings[Setting::max_parser_depth]);
    const uint32_t max_backtracks = static_cast<uint32_t>(settings[Setting::max_parser_backtracks]);

    const bool is_set_escape = dialect != Dialect::clickhouse && dialect != Dialect::trino && dialect != Dialect::logsql
        && isClickHouseJSONSetEscape(pos, end, max_query_size, max_depth, max_backtracks);

    if (dialect == Dialect::clickhouse_json && !is_set_escape)
    {
        if (!settings[Setting::enable_json_ast_dialect])
            return nullptr;

        const char * p = pos;
        while (p < end && isWhitespaceASCII(*p))
            ++p;
        if (p >= end || *p != '{')
            return nullptr;

        int depth = 0;
        bool in_string = false;
        bool escaped = false;
        const char * q = p;
        for (; q < end; ++q)
        {
            const char ch = *q;
            if (in_string)
            {
                if (escaped)
                    escaped = false;
                else if (ch == '\\')
                    escaped = true;
                else if (ch == '"')
                    in_string = false;
                continue;
            }
            if (ch == '"')
                in_string = true;
            else if (ch == '{')
                ++depth;
            else if (ch == '}')
            {
                --depth;
                if (depth == 0)
                {
                    ++q;
                    break;
                }
            }
        }
        if (depth != 0)
            return nullptr;

        ASTPtr res = IAST::createFromJSON(String(p, q), settings[Setting::max_ast_depth], settings[Setting::max_ast_elements]);
        if (settings[Setting::max_ast_depth])
            res->checkDepth(settings[Setting::max_ast_depth]);
        if (settings[Setting::max_ast_elements])
            res->checkSize(settings[Setting::max_ast_elements]);
        pos = q;
        return res;
    }

    if (dialect == Dialect::kusto && !is_set_escape)
        return parseKQLQuery(pos, end, /*allow_multi_statements=*/true, /*max_length=*/0, max_depth, max_backtracks);

    std::unique_ptr<IParserBase> parser;
    if (is_set_escape)
        parser = std::make_unique<ParserQuery>(end, settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);
    else if (dialect == Dialect::prql)
        parser = std::make_unique<ParserPRQLQuery>(0, max_depth, max_backtracks);
    else if (dialect == Dialect::promql)
        parser = std::make_unique<ParserPrometheusQuery>(
            settings[Setting::promql_database], settings[Setting::promql_table], Field{settings[Setting::promql_evaluation_time]});
    else if (dialect == Dialect::polyglot)
        parser = std::make_unique<ParserPolyglotQuery>(
            0, max_depth, max_backtracks, settings[Setting::polyglot_dialect], end, settings[Setting::enable_polyglot_dialect]);
    else if (dialect == Dialect::logsql)
        parser = std::make_unique<ParserLogsQLQuery>(
            settings[Setting::logsql_database], settings[Setting::logsql_table],
            settings[Setting::logsql_time_column], settings[Setting::logsql_message_column],
            pos, end, settings[Setting::enable_logsql_dialect], max_depth, max_query_size);
    else if (dialect == Dialect::trino)
        parser = std::make_unique<ParserTrinoQuery>(
            0, max_depth, max_backtracks, end, settings[Setting::enable_trino_dialect],
            settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);
    else
        parser = std::make_unique<ParserQuery>(end, settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);

    if (dialect == Dialect::logsql && !is_set_escape)
        return parseLogsQLQueryAndMovePosition(*parser, pos, end, /*allow_multi_statements=*/true, 0, max_depth, max_backtracks);

    return parseQueryAndMovePosition(*parser, pos, end, "", /*allow_multi_statements=*/true, 0, max_depth, max_backtracks);
}

const char * findStatementEndForClassification(const IAST & ast, const char * parsed_end, const char * all_queries_end)
{
    const auto * insert = ast.as<ASTInsertQuery>();
    if (!insert)
    {
        if (const auto * explain = ast.as<ASTExplainQuery>(); explain && explain->getExplainedQuery())
            insert = explain->getExplainedQuery()->as<ASTInsertQuery>();
    }
    if (!insert || !insert->data)
        return parsed_end;

    if (insert->format == "Values")
    {
        ReadBufferFromMemory data_in(insert->data, static_cast<size_t>(all_queries_end - insert->data));
        skipBOMIfExists(data_in);
        do
        {
            skipWhitespaceAndSQLComments(data_in);
            if (data_in.eof() || *data_in.position() == ';')
                break;
        } while (ValuesBlockInputFormat::skipToNextRow(&data_in, 1, 0));
        return insert->data + data_in.count();
    }

    const auto blank_line = std::string_view(insert->data, static_cast<size_t>(all_queries_end - insert->data)).find("\n\n");
    return blank_line == std::string_view::npos ? all_queries_end : insert->data + blank_line;
}

} // namespace

struct StreamExecutionState
{
    std::mutex mutex;
    ContextMutablePtr query_context;
    ASTPtr parsed_query;
    BlockIO io;
    SharedHeader header;
    String caller_format;
    bool is_chunk_collect = false;
    bool print_stack_trace = false;

    std::unique_ptr<PullingPipelineExecutor> sync_executor;
    std::unique_ptr<PullingAsyncPipelineExecutor> async_executor;

    ThreadGroupPtr thread_group;
    std::shared_ptr<Progress> progress;
    std::shared_ptr<Stopwatch> watch;
    ProgressValues prev_progress{};
    double prev_elapsed = 0.0;

    std::shared_ptr<CHDB::ActiveQuerySlot> interrupt_slot;
    std::shared_ptr<CHDB::InterruptController> interrupt_controller;

#if USE_PYTHON
    std::shared_ptr<CHDB::PythonTableCache> python_table_cache;
    UInt64 python_tables_bind_token = 0;
#endif

    String deferred_error_message;
    int deferred_error_code = 0;
    bool cancelled = false;
    bool finished = false;
    bool had_error = false;

    void releaseBindingsAndContextLocked() noexcept
    {
        /// Drop the query's process list entry now rather than with the stream handle: a Python handle can be
        /// collected at interpreter exit, after the engine is gone, and ~ProcessListEntry dereferences the ProcessList.
        try { io = BlockIO(); } catch (...) {}

        if (interrupt_controller && interrupt_slot)
            interrupt_controller->unregisterSlot(interrupt_slot.get());
#if USE_PYTHON
        if (python_table_cache)
        {
            if (query_context)
                python_table_cache->detachQueryContext(query_context.get());
            if (python_tables_bind_token != 0)
            {
                python_table_cache->releaseQueryableObjs(python_tables_bind_token);
                python_tables_bind_token = 0;
            }
        }
#endif
        parsed_query.reset();
        query_context.reset();
        thread_group.reset();
    }

    void cancelAndTeardown() noexcept
    {
        if (interrupt_slot)
            interrupt_slot->cancel();

        std::lock_guard<std::mutex> lock(mutex);
        if (finished)
            return;

        cancelled = true;
        finished = true;

        try
        {
            ScopedSessionThreadStatus thread_status;
            if (thread_group)
                CurrentThread::attachToGroupIfDetached(thread_group);

            if (sync_executor)
            {
                try { sync_executor->cancel(); } catch (...) {}
                sync_executor.reset();
            }
            if (async_executor)
            {
                try { async_executor->cancel(); } catch (...) {}
                async_executor.reset();
            }

            try { io.onCancelOrConnectionLoss(); } catch (...) {}
        }
        catch (...)
        {
        }

        releaseBindingsAndContextLocked();
    }
};

namespace
{
void cancelStreamExecutionStateHook(void * raw_state) noexcept
{
    if (auto * state = static_cast<StreamExecutionState *>(raw_state))
        state->cancelAndTeardown();
}
} // namespace

std::unique_ptr<EmbeddedSession> EmbeddedSession::create(
    EmbeddedDatabasePtr database_, int argc, char ** argv, bool force_read_only)
{
    return std::make_unique<EmbeddedSession>(std::move(database_), argc, argv, force_read_only);
}

EmbeddedSession::EmbeddedSession(
    EmbeddedDatabasePtr database_, int argc, char ** argv, bool force_read_only)
    : database(std::move(database_))
    , server(database->getServer())
    , interrupt_controller(std::make_shared<CHDB::InterruptController>())
{
    ChdbNamespaceScope ns_scope(database ? database->getNamespace() : nullptr);
    session = std::make_unique<Session>(database->getNamespaceContext(), ClientInfo::Interface::LOCAL);
#if USE_PYTHON
    python_table_cache = std::make_shared<CHDB::PythonTableCache>();
    session->setPythonTableCache(python_table_cache);
#endif
#if !defined(OS_WASM)
    arrow_stream_registry = std::make_shared<CHDB::ArrowStreamRegistry>();
    session->setArrowStreamRegistry(arrow_stream_registry);
#endif

    session->authenticate("default", "", Poco::Net::SocketAddress{});
    session_context = session->makeSessionContext();
#if !defined(OS_WASM)
    session_context->setArrowStreamRegistry(arrow_stream_registry);
#endif
#if USE_PYTHON
    session_context->setPythonTableCache(python_table_cache);
#endif
    session_context_raw = session_context.get();
    session_context->setCurrentDatabase(database->getDefaultDatabaseName());

    print_stack_trace = server.config().getBool("stacktrace", false);
    if (database->isReadOnly() || force_read_only)
        session_context->applySettingChange(SettingChange("readonly", Field(UInt64(2))));
    applyCmdSettings(argc, argv);
    EmbeddedServer::registerActiveSession();
}

EmbeddedSession::~EmbeddedSession()
{
    EmbeddedServer::unregisterActiveSession();
    cleanup();
    database.reset();
}

void EmbeddedSession::applyCmdSettings(int argc, char ** argv)
{
    if (argc <= 1 || !argv)
        return;

    for (const auto & [key, value] : EmbeddedServer::parseOptions(argc, argv))
    {
        if (key == "stacktrace")
        {
            print_stack_trace = (value == "1" || boost::iequals(value, "true") || boost::iequals(value, "yes") || boost::iequals(value, "on"));
        }
        else if (auto param_name = connectionParameterName(key); !param_name.empty())
        {
            query_parameters.insert_or_assign(String(param_name), value);
        }
    }

    if (!query_parameters.empty())
        session_context->setQueryParameters(query_parameters);

    auto changes = EmbeddedServer::parseQuerySettingChanges(argc, argv);
    if (!changes.empty())
        session_context->applySettingsChanges(changes);
}

void EmbeddedSession::cleanup() noexcept
{
    try
    {
        if (interrupt_controller)
            interrupt_controller->interruptAll();

        cancelAllActiveStreams();

        CHDB::InsertStreamContextPtr insert_ctx;
        {
            std::unique_lock<std::shared_mutex> lock(session_mutex);
            insert_ctx = std::move(streaming_insert_context);
        }

        if (insert_ctx)
        {
            insert_ctx->cancelled.store(true, std::memory_order_release);
            if (insert_ctx->interrupt_slot)
                std::static_pointer_cast<CHDB::ActiveQuerySlot>(insert_ctx->interrupt_slot)->cancel();
            if (insert_ctx->queue_buf)
                insert_ctx->queue_buf->finish();
            if (insert_ctx->worker.joinable())
                insert_ctx->worker.join();
            insert_ctx->finalized.store(true, std::memory_order_release);
        }

        std::unique_lock<std::shared_mutex> lock(session_mutex);
#if !defined(OS_WASM)
        if (arrow_stream_registry)
            arrow_stream_registry->clear();
#endif
        session_context.reset();
        session.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void EmbeddedSession::interrupt() noexcept
{
    if (interrupt_controller)
        interrupt_controller->interruptAll();
}

void EmbeddedSession::setProgressValuesCallback(ProgressValuesCallback callback)
{
    std::lock_guard<std::mutex> lock(progress_callback_mutex);
    progress_values_callback = std::move(callback);
}

bool EmbeddedSession::isActiveOnCurrentThreadOrQuery() const
{
    for (const auto * frame = active_session_stack; frame; frame = frame->prev)
    {
        if (frame->session == this)
            return true;
    }
    if (auto current_query_ctx = CurrentThread::tryGetQueryContext())
    {
        if (current_query_ctx->hasSessionContext()
            && current_query_ctx->getSessionContext().get() == session_context_raw)
            return true;
    }
    return false;
}

void EmbeddedSession::checkNotReentrant() const
{
    if (isActiveOnCurrentThreadOrQuery())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot execute an operation on a connection from its own active callback");
}

EmbeddedSession::CallQueryParameters EmbeddedSession::setCallQueryParameters(CallQueryParameters call)
{
    return std::exchange(call_query_parameters, call);
}

const NameToNameMap * EmbeddedSession::currentCallParameters() const noexcept
{
    const auto [target, params] = call_query_parameters;
    return (target == this) ? params : nullptr;
}

#if USE_PYTHON
void EmbeddedSession::findQueryableObjFromPyCache(const String & query_str) const
{
    python_table_cache->releaseQueryableObjs(std::exchange(python_tables_bind_token, 0));
    python_tables_bind_token = python_table_cache->findQueryableObjFromQuery(query_str);
}

void EmbeddedSession::bindPythonTablesForQuery(const String & query_str, const void * explicit_tables_pyobj) const
{
    python_table_cache->releaseQueryableObjs(std::exchange(python_tables_bind_token, 0));
    if (explicit_tables_pyobj)
    {
        const auto & tables_obj = *static_cast<const pybind11::object *>(explicit_tables_pyobj);
        python_tables_bind_token = python_table_cache->findQueryableObjFromQuery(query_str, tables_obj);
    }
    else
    {
        python_tables_bind_token = python_table_cache->findQueryableObjFromQuery(query_str);
    }
}
#endif

ContextMutablePtr EmbeddedSession::makeQueryContextLocked(
    const NameToNameMap * call_params,
    const char * format,
    size_t format_len,
    bool is_chunk_collect) const
{
    ChdbNamespaceScope ns_scope(database ? database->getNamespace() : nullptr);
    ContextMutablePtr query_context = session->makeQueryContext();
    query_context->setClientInterface(ClientInfo::Interface::LOCAL);
    query_context->setCurrentQueryId("");
    query_context->setSetting("serialize_query_plan", false);

    if (format && format_len > 0 && !is_chunk_collect)
        query_context->setDefaultFormat(String(format, format_len));

#if USE_PYTHON
    query_context->setJSONSupport(isJSONSupportedForFormat(format, format_len));
    query_context->setPythonTableCache(python_table_cache);
#endif
#if !defined(OS_WASM)
    query_context->setArrowStreamRegistry(arrow_stream_registry);
#endif

    /// A per-call parameter overrides the connection's value of the same name for this query only.
    if (call_params && !call_params->empty())
        query_context->addQueryParameters(*call_params);

    return query_context;
}

void EmbeddedSession::attachProgressAndCancelCallbacks(
    const ContextMutablePtr & query_context,
    const std::shared_ptr<Progress> & query_progress,
    const std::shared_ptr<Stopwatch> & query_watch,
    const std::shared_ptr<CHDB::ActiveQuerySlot> & interrupt_slot) const
{
    Context * raw_ctx = query_context.get();

    auto handle_progress = [this, raw_ctx, query_progress, query_watch, interrupt_slot](const Progress & delta)
    {
        query_progress->incrementPiecewiseAtomically(delta);
        const ProgressValues accumulated = query_progress->getValues();
        const UInt64 elapsed_ns = query_watch->elapsedNanoseconds();

        if (auto elem = raw_ctx->getProcessListElement())
            interrupt_slot->bindQueryStatus(elem);

#if defined(OS_WASM)
        notifyCHDBWasmProgress(
            accumulated.read_rows,
            accumulated.total_rows_to_read,
            accumulated.read_bytes,
            accumulated.total_bytes_to_read,
            elapsed_ns);
        if (chdbWasmCancelRequested())
            interrupt_slot->cancel();
#endif

        if (interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
        {
            if (auto elem = raw_ctx->getProcessListElement())
                elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
        }

        if (delta.read_rows || delta.read_bytes || delta.written_rows || delta.written_bytes || delta.total_rows_to_read)
        {
            ProgressValuesCallback cb;
            {
                std::lock_guard<std::mutex> lock(progress_callback_mutex);
                cb = progress_values_callback;
            }
            if (cb)
            {
                ScopedActiveSessionFrame active_frame(this);
                cb(accumulated, elapsed_ns);
            }
        }
    };

    query_context->setProgressCallback(handle_progress);
    query_context->setFileProgressCallback([handle_progress](const FileProgress & fp)
    {
        ProfileEvents::increment(ProfileEvents::FileProgressCallbackInvocations);
        handle_progress(Progress(fp));
    });

    query_context->setInteractiveCancelCallback([raw_ctx, interrupt_slot]() -> bool
    {
        if (auto elem = raw_ctx->getProcessListElement())
            interrupt_slot->bindQueryStatus(elem);
#if defined(OS_WASM)
        if (chdbWasmCancelRequested())
            interrupt_slot->cancel();
#endif
        if (!interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
            return false;
        if (auto elem = raw_ctx->getProcessListElement())
            elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
        return true;
    });
}

void EmbeddedSession::syncSessionParametersFromSetQuery(const IAST & ast, bool already_holds_unique_lock)
{
    const auto * set_query = ast.as<ASTSetQuery>();
    if (!set_query || set_query->query_parameters.empty())
        return;

    auto apply = [&]()
    {
        for (const auto & [name, val] : set_query->query_parameters)
            query_parameters.insert_or_assign(name, val);
    };

    if (already_holds_unique_lock)
    {
        apply();
    }
    else
    {
        std::unique_lock<std::shared_mutex> lock(session_mutex);
        apply();
    }
}

void EmbeddedSession::registerActiveStream(const std::shared_ptr<StreamExecutionState> & state)
{
    std::lock_guard<std::mutex> lock(active_streams_mutex);
    if (active_streams.size() >= 16)
        std::erase_if(active_streams, [](const auto & w) { return w.expired(); });
    active_streams.push_back(state);
}

void EmbeddedSession::cancelAllActiveStreams() noexcept
{
    std::vector<std::shared_ptr<StreamExecutionState>> snapshot;
    {
        std::lock_guard<std::mutex> lock(active_streams_mutex);
        snapshot.reserve(active_streams.size());
        for (const auto & w : active_streams)
        {
            if (auto s = w.lock())
                snapshot.push_back(std::move(s));
        }
        active_streams.clear();
    }

    for (const auto & s : snapshot)
        s->cancelAndTeardown();
}

void EmbeddedSession::setConnectionSetting(const String & key, const String * value)
{
    checkNotReentrant();
    ScopedActiveSessionFrame active_frame(this);
    ChdbNamespaceScope ns_scope(database ? database->getNamespace() : nullptr);
    std::unique_lock<std::shared_mutex> lock(session_mutex);

    if (streaming_insert_context)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot change a setting while a streaming insert is active on this connection");

    if (auto name = connectionParameterName(key); !name.empty())
    {
        if (value)
            query_parameters.insert_or_assign(String(name), *value);
        else
            query_parameters.erase(String(name));
        session_context->setQueryParameters(query_parameters);
        return;
    }

    if (value)
    {
        SettingChange change(key, Field(*value));
        session_context->checkSettingsConstraints(change, SettingSource::QUERY);
        session_context->applySettingChange(change);
    }
    else
    {
        if (!session_context->getSettingsRef().has(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting '{}'", key);
        session_context->checkSettingsConstraintsForSettingsReset({key}, SettingSource::QUERY);
        session_context->resetSettingsToDefaultValue({key});
    }
}

bool EmbeddedSession::getConnectionSetting(const String & key, String & value) const
{
    checkNotReentrant();
    ChdbNamespaceScope ns_scope(database ? database->getNamespace() : nullptr);
    std::shared_lock<std::shared_mutex> lock(session_mutex);

    if (auto name = connectionParameterName(key); !name.empty())
    {
        auto it = query_parameters.find(String(name));
        if (it == query_parameters.end())
            return false;
        value = it->second;
        return true;
    }

    Field field;
    if (!session_context->getSettingsRef().tryGet(key, field))
        return false;
    if (Settings::hasBuiltin(key))
        value = Settings::valueToStringUtil(key, field);
    else if (field.getType() == Field::Types::String)
        value = field.safeGet<String>();
    else
        value = field.dump();
    return true;
}

CHDB::QueryResultPtr EmbeddedSession::executeMaterializedQuery(
    const char * query, size_t query_len,
    const char * format, size_t format_len)
{
#if USE_PYTHON
    const UInt64 py_tables_token = takePythonTablesBindToken();
    SCOPE_EXIT({
        if (py_tables_token != 0 && python_table_cache)
            python_table_cache->releaseQueryableObjs(py_tables_token);
    });
#endif

    if (isActiveOnCurrentThreadOrQuery())
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "Cannot execute a query on a connection from its own active callback",
            ErrorCodes::BAD_ARGUMENTS);
    }

    const NameToNameMap * call_params = currentCallParameters();
    const std::string_view format_view(format ? format : "", format ? format_len : 0);
    const bool is_chunk_collect = isChunkCollectFormat(format_view);
    const String format_str(format_view);

    bool allow_settings_after_format = false;
    {
        std::shared_lock<std::shared_mutex> lock(session_mutex);
        if (streaming_insert_context)
        {
            return std::make_unique<CHDB::MaterializedQueryResult>(
                "Cannot run a query while a streaming insert is active on this connection",
                ErrorCodes::BAD_ARGUMENTS);
        }
        allow_settings_after_format = session_context->getSettingsRef()[Setting::allow_settings_after_format_in_insert];
    }

    auto interrupt_slot = std::make_shared<CHDB::ActiveQuerySlot>();
    interrupt_controller->registerSlot(interrupt_slot);
    SCOPE_EXIT({ interrupt_controller->unregisterSlot(interrupt_slot.get()); });

    try
    {
        ScopedSessionThreadStatus thread_status;
        auto total_watch = std::make_shared<Stopwatch>(CLOCK_MONOTONIC);
        auto total_progress = std::make_shared<Progress>();

        const char * const all_begin = query ? query : "";
        const char * const all_end = all_begin + (query ? query_len : 0);
        const auto statements = splitMultiQueryStatements(all_begin, all_end, allow_settings_after_format);

        std::unique_ptr<std::vector<char>> output_vec;
        std::vector<Chunk> collected_chunks;
        std::shared_ptr<const Block> collected_chunks_header;
        std::shared_ptr<const Block> last_result_header;
        uint64_t total_rows_read = 0;
        uint64_t total_bytes_read = 0;

        for (const auto & stmt : statements)
        {
            if (interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

            const ProgressValues progress_before_statement = total_progress->getValues();

            std::unique_lock<std::shared_mutex> exclusive_session_lock(session_mutex, std::defer_lock);
            ContextMutablePtr query_context;
            if (stmt.mutates_session)
            {
                exclusive_session_lock.lock();
                if (streaming_insert_context)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot run a query while a streaming insert is active on this connection");
                query_context = makeQueryContextLocked(call_params, format, format_len, is_chunk_collect);
            }
            else
            {
                std::shared_lock<std::shared_mutex> shared_session_lock(session_mutex);
                if (streaming_insert_context)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot run a query while a streaming insert is active on this connection");
                query_context = makeQueryContextLocked(call_params, format, format_len, is_chunk_collect);
            }

#if USE_PYTHON
            if (py_tables_token != 0 && python_table_cache)
                python_table_cache->attachQueryContext(query_context.get(), py_tables_token);
            SCOPE_EXIT({
                if (py_tables_token != 0 && python_table_cache && query_context)
                    python_table_cache->detachQueryContext(query_context.get());
            });
#endif

            attachProgressAndCancelCallbacks(query_context, total_progress, total_watch, interrupt_slot);

            /// Wire `input()` table function to an isolated empty buffer (or `FROM INFILE`) so
            /// `input()` never touches the host process's fd 0 (#239).
            struct InputState
            {
                std::unique_ptr<QueryPipeline> pipeline;
                std::unique_ptr<PullingPipelineExecutor> executor;
            };
            auto input_state = std::make_shared<InputState>();
            const String stmt_copy(stmt.text);

            query_context->setInputInitializer(
                [input_state, stmt_copy](ContextPtr ctx, const ColumnsDescription & columns_description, const Block & sample)
                {
                    const auto & s = ctx->getSettingsRef();
                    const char * begin = stmt_copy.data();
                    const char * end = begin + stmt_copy.size();
                    ParserQuery parser(end, s[Setting::allow_settings_after_format_in_insert], s[Setting::implicit_select]);
                    ASTPtr parsed = parseQuery(parser, begin, end, "", s[Setting::max_query_size], s[Setting::max_parser_depth], s[Setting::max_parser_backtracks]);
                    const auto * ins = parsed ? parsed->as<ASTInsertQuery>() : nullptr;

                    String fmt = ins ? ins->format : String{};
                    if (fmt.empty() && ins && ins->infile)
                        fmt = FormatFactory::instance().getFormatFromFileName(ins->infile->as<ASTLiteral &>().value.safeGet<String>());
                    if (!s[Setting::input_format].value.empty())
                        fmt = s[Setting::input_format];
                    else if (!s[Setting::format].value.empty())
                        fmt = s[Setting::format];

                    if (fmt.empty())
                        throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "FORMAT must be specified for function input()");

                    if (ins && ins->infile)
                    {
                        input_state->pipeline = std::make_unique<QueryPipeline>(
                            createPipeFromInsertInfile(*ins, fmt, columns_description, sample, ctx));
                        input_state->executor = std::make_unique<PullingPipelineExecutor>(*input_state->pipeline);
                    }
                });

            query_context->setInputBlocksReaderCallback([input_state](ContextPtr) -> Block
            {
                Block block;
                if (input_state->executor)
                    input_state->executor->pull(block);
                return block;
            });

            QueryScope query_scope = QueryScope::create(query_context);

            auto [ast, io] = executeQuery(stmt.text, query_context, QueryFlags{}, QueryProcessingStage::Complete);

            if (auto elem = query_context->getProcessListElement())
                interrupt_slot->bindQueryStatus(elem);

            if (interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
            {
                io.onCancelOrConnectionLoss();
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
            }

            try
            {
                if (io.pipeline.pushing())
                {
                    const auto * insert_ast = ast ? ast->as<ASTInsertQuery>() : nullptr;
                    if (!insert_ast || (!insert_ast->infile && !insert_ast->hasInlinedData()))
                    {
                        io.resetPipeline(/*cancel=*/true);
                        if (query_context->getSettingsRef()[Setting::throw_if_no_data_to_insert])
                            throw Exception(ErrorCodes::NO_DATA_TO_INSERT, "No data to insert");
                    }
                    else if (insert_ast->infile)
                    {
                        String in_format = insert_ast->format;
                        if (in_format.empty())
                            in_format = FormatFactory::instance().getFormatFromFileName(
                                insert_ast->infile->as<ASTLiteral &>().value.safeGet<String>());
                        const auto & s = query_context->getSettingsRef();
                        if (!s[Setting::input_format].value.empty())
                            in_format = s[Setting::input_format];
                        else if (!s[Setting::format].value.empty())
                            in_format = s[Setting::format];

                        ColumnsDescription cols = query_context->hasInsertionTableColumnsDescription()
                            ? *query_context->getInsertionTableColumnsDescription()
                            : ColumnsDescription(io.pipeline.getHeader().getNamesAndTypesList());
                        auto infile_pipeline = createPipeFromInsertInfile(*insert_ast, in_format, cols, io.pipeline.getHeader(), query_context);
                        PullingPipelineExecutor in_exec(infile_pipeline);

                        if (io.pipeline.getNumThreads() > 1)
                        {
                            PushingAsyncPipelineExecutor push_exec(io.pipeline);
                            push_exec.start();
                            Block block;
                            while (in_exec.pull(block))
                            {
                                if (!block.empty())
                                    push_exec.push(std::move(block));
                            }
                            push_exec.finish();
                        }
                        else
                        {
                            PushingPipelineExecutor push_exec(io.pipeline);
                            push_exec.start();
                            Block block;
                            while (in_exec.pull(block))
                            {
                                if (!block.empty())
                                    push_exec.push(std::move(block));
                            }
                            push_exec.finish();
                        }
                    }
                }
                else if (io.pipeline.pulling())
                {
                    const Block header = io.pipeline.getHeader();
                    last_result_header = std::make_shared<const Block>(header.cloneEmpty());

                    if (is_chunk_collect)
                    {
                        collected_chunks_header = std::make_shared<const Block>(header);
                        collected_chunks.clear();
                        uint64_t stmt_rows = 0;
                        uint64_t stmt_bytes = 0;

                        const bool use_sync_pull = (io.pipeline.getNumThreads() <= 1);
                        io.pipeline.setConcurrencyControl(false);
                        if (use_sync_pull)
                        {
                            PullingPipelineExecutor executor(io.pipeline);
                            Chunk chunk;
                            while (executor.pull(chunk))
                            {
                                if (chunk.hasRows())
                                {
                                    materializeChunk(chunk);
                                    if (!io.null_format)
                                    {
                                        stmt_rows += chunk.getNumRows();
                                        stmt_bytes += chunk.bytes();
                                        collected_chunks.emplace_back(std::move(chunk));
                                    }
                                }
                            }
                            if (Chunk totals = executor.getTotals(); totals.hasRows() && !io.null_format)
                            {
                                materializeChunk(totals);
                                collected_chunks.emplace_back(std::move(totals));
                            }
                            if (Chunk extremes = executor.getExtremes(); extremes.hasRows() && !io.null_format)
                            {
                                materializeChunk(extremes);
                                collected_chunks.emplace_back(std::move(extremes));
                            }
                        }
                        else
                        {
                            PullingAsyncPipelineExecutor executor(io.pipeline);
                            Chunk chunk;
                            while (executor.pull(chunk))
                            {
                                if (chunk.hasRows())
                                {
                                    materializeChunk(chunk);
                                    if (!io.null_format)
                                    {
                                        stmt_rows += chunk.getNumRows();
                                        stmt_bytes += chunk.bytes();
                                        collected_chunks.emplace_back(std::move(chunk));
                                    }
                                }
                            }
                            if (Chunk totals = executor.getTotals(); totals.hasRows() && !io.null_format)
                            {
                                materializeChunk(totals);
                                collected_chunks.emplace_back(std::move(totals));
                            }
                            if (Chunk extremes = executor.getExtremes(); extremes.hasRows() && !io.null_format)
                            {
                                materializeChunk(extremes);
                                collected_chunks.emplace_back(std::move(extremes));
                            }
                        }

                        total_rows_read += stmt_rows;
                        total_bytes_read += stmt_bytes;
                    }
                    else
                    {
                        std::unique_ptr<WriteBufferFromVectorMTB<std::vector<char>>> mem_buf;
                        if (output_vec)
                        {
                            mem_buf = std::make_unique<WriteBufferFromVectorMTB<std::vector<char>>>(*output_vec, AppendModeTag{});
                        }
                        else
                        {
                            {
                                MemoryTrackerBlockerInThread blocker;
                                output_vec = std::make_unique<std::vector<char>>(4096);
                            }
                            mem_buf = std::make_unique<WriteBufferFromVectorMTB<std::vector<char>>>(*output_vec);
                        }

                        OutfileTargetState outfile = prepareOutfileIfAny(ast, query_context, mem_buf.get());
                        SCOPE_EXIT({
                            if (std::uncaught_exceptions() > 0)
                                outfile.cleanupTempOnError();
                        });

                        const String effective_format = resolveEffectiveOutputFormat(
                            query_context, ast, format_str, outfile.has_ext_format, outfile.ext_format);

                        WriteBuffer * target_buf = mem_buf.get();
                        if (outfile.fork_buf)
                            target_buf = outfile.fork_buf.get();
                        else if (outfile.file_buf)
                            target_buf = outfile.file_buf.get();

                        auto fmt_settings = getFormatSettings(query_context);
                        if (!outfile.non_empty_append_path.empty()
                            && !FormatFactory::instance().checkIfFormatSupportAppend(effective_format, query_context, fmt_settings))
                        {
                            throw Exception(
                                ErrorCodes::CANNOT_APPEND_TO_FILE,
                                "Data cannot be appended to file {} because the {} format doesn't support appends. "
                                "Use TRUNCATE instead of APPEND, or write into a new file",
                                outfile.non_empty_append_path,
                                effective_format);
                        }

                        OutputFormatPtr output_format = query_context->getOutputFormatParallelIfPossible(
                            effective_format, *target_buf, materializeBlock(header), fmt_settings);
                        if (!outfile.non_empty_append_path.empty())
                            output_format->doNotWritePrefix();

                        const bool use_sync_pull = (io.pipeline.getNumThreads() <= 1);
                        io.pipeline.setConcurrencyControl(false);

                        auto feed_blocks = [&](auto & executor)
                        {
                            Block block;
                            while (executor.pull(block))
                            {
                                if (block.rows() > 0 && !io.null_format)
                                {
                                    total_rows_read += block.rows();
                                    total_bytes_read += block.bytes();
                                    output_format->write(materializeBlock(block));
                                }
                            }

                            const auto & profile_info = executor.getProfileInfo();
                            if (profile_info.hasAppliedLimit())
                                output_format->setRowsBeforeLimit(profile_info.getRowsBeforeLimit());
                            if (profile_info.hasAppliedAggregation())
                                output_format->setRowsBeforeAggregation(profile_info.getRowsBeforeAggregation());

                            if (Block totals = executor.getTotalsBlock(); !totals.empty() && !io.null_format)
                                output_format->setTotals(materializeBlock(totals));
                            if (Block extremes = executor.getExtremesBlock(); !extremes.empty() && !io.null_format)
                                output_format->setExtremes(materializeBlock(extremes));
                        };

                        if (use_sync_pull)
                        {
                            PullingPipelineExecutor executor(io.pipeline);
                            feed_blocks(executor);
                        }
                        else
                        {
                            PullingAsyncPipelineExecutor executor(io.pipeline);
                            feed_blocks(executor);
                        }

                        /// Formats like JSON report the statement's rows_read and bytes_read in their statistics.
                        const ProgressValues progress_after_statement = total_progress->getValues();
                        output_format->onProgress(Progress(
                            progress_after_statement.read_rows - progress_before_statement.read_rows,
                            progress_after_statement.read_bytes - progress_before_statement.read_bytes));
                        output_format->finalize();
                        output_format.reset();
                        outfile.finalizeAndCommit();
                        mem_buf->finalize();
                        mem_buf.reset();

                        /// If the statement wrote exclusively to an OUTFILE (without AND STDOUT),
                        /// drop the untouched memory buffer so no empty buffer is returned.
                        if (outfile.into_file && !outfile.into_file_and_stdout)
                            output_vec.reset();
                    }
                }
                else if (io.pipeline.completed())
                {
                    CompletedPipelineExecutor executor(io.pipeline);
                    executor.execute();
                }

                if (ast)
                    syncSessionParametersFromSetQuery(*ast, exclusive_session_lock.owns_lock());
            }
            catch (...)
            {
                io.onException();
                throw;
            }

            io.onFinish();
        }

        const ProgressValues final_progress = total_progress->getValues();
        const double elapsed_sec = total_watch->elapsedSeconds();

        if (is_chunk_collect)
        {
            auto res = std::make_unique<CHDB::ChunkQueryResult>(
                std::move(collected_chunks),
                std::move(collected_chunks_header),
                elapsed_sec,
                total_rows_read,
                total_bytes_read,
                final_progress.read_rows,
                final_progress.read_bytes,
                final_progress.written_rows,
                final_progress.written_bytes);
            if (last_result_header)
                fillResultSchemaFromHeader(*res, *last_result_header);
            return res;
        }

        auto res = std::make_unique<CHDB::MaterializedQueryResult>(
            CHDB::ResultBuffer(std::move(output_vec)),
            elapsed_sec,
            total_rows_read,
            total_bytes_read,
            final_progress.read_rows,
            final_progress.read_bytes,
            final_progress.written_rows,
            final_progress.written_bytes);
        if (last_result_header)
            fillResultSchemaFromHeader(*res, *last_result_header);
        return res;
    }
    catch (const Exception & e)
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            getExceptionMessage(e, print_stack_trace), e.code());
    }
    catch (...)
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            getCurrentExceptionMessage(print_stack_trace), getCurrentExceptionCode());
    }
}

CHDB::QueryResultPtr EmbeddedSession::executeStreamingInit(
    const char * query, size_t query_len,
    const char * format, size_t format_len, bool dataframe_over_chunks)
{
    (void)dataframe_over_chunks;
#if USE_PYTHON
    UInt64 py_tables_token = takePythonTablesBindToken();
    SCOPE_EXIT({
        if (py_tables_token != 0 && python_table_cache)
            python_table_cache->releaseQueryableObjs(py_tables_token);
    });
#endif

    if (isActiveOnCurrentThreadOrQuery())
    {
        return std::make_unique<CHDB::StreamQueryResult>(
            "Cannot start a streaming query on a connection from its own active callback",
            ErrorCodes::BAD_ARGUMENTS);
    }

    const NameToNameMap * call_params = currentCallParameters();
    const std::string_view format_view(format ? format : "", format ? format_len : 0);
    const bool is_chunk_collect = isChunkCollectFormat(format_view);
    const String query_str(query ? query : "", query ? query_len : 0);

    ContextMutablePtr query_context;
    Dialect current_dialect = Dialect::clickhouse;
    bool allow_settings_after_format = false;
    {
        std::shared_lock<std::shared_mutex> shared_session_lock(session_mutex);
        if (streaming_insert_context)
        {
            return std::make_unique<CHDB::StreamQueryResult>(
                "Cannot start a streaming query while a streaming insert is active on this connection",
                ErrorCodes::BAD_ARGUMENTS);
        }
        current_dialect = session_context->getSettingsRef()[Setting::dialect];
        allow_settings_after_format = session_context->getSettingsRef()[Setting::allow_settings_after_format_in_insert];
        query_context = makeQueryContextLocked(call_params, format, format_len, is_chunk_collect);
    }

    /// Reject multi-statement and non-SELECT queries BEFORE calling executeQuery so
    /// DDL/DML side effects never run on a rejected streaming call (ADBC relies on
    /// falling back to materialized execution when this sentinel error is returned).
    const auto statements = splitMultiQueryStatements(
        query_str.data(), query_str.data() + query_str.size(), allow_settings_after_format);
    if (statements.size() != 1 || !isPotentiallyStreamableLeadingToken(statements.front().text, current_dialect))
    {
        return std::make_unique<CHDB::StreamQueryResult>(
            fmt::format("{} for query: {}", CHDB::kErrorStreamingNotSupportedPrefix, query_str),
            ErrorCodes::BAD_ARGUMENTS);
    }

    auto state = std::make_shared<StreamExecutionState>();
    state->caller_format = String(format_view);
    state->is_chunk_collect = is_chunk_collect;
    state->print_stack_trace = print_stack_trace;
    state->progress = std::make_shared<Progress>();
    state->watch = std::make_shared<Stopwatch>(CLOCK_MONOTONIC);
    state->interrupt_slot = std::make_shared<CHDB::ActiveQuerySlot>();
    state->interrupt_controller = interrupt_controller;
#if USE_PYTHON
    state->python_table_cache = python_table_cache;
#endif

    interrupt_controller->registerSlot(state->interrupt_slot);

    try
    {
        ScopedSessionThreadStatus thread_status;

#if USE_PYTHON
        if (py_tables_token != 0 && python_table_cache)
            python_table_cache->attachQueryContext(query_context.get(), py_tables_token);
#endif
        attachProgressAndCancelCallbacks(query_context, state->progress, state->watch, state->interrupt_slot);

        QueryScope query_scope = QueryScope::create(query_context);
        query_scope.log_peak_memory_usage_in_destructor = false;

        try
        {
            auto [ast, io] = executeQuery(statements.front().text, query_context, QueryFlags{}, QueryProcessingStage::Complete);

            if (auto elem = query_context->getProcessListElement())
                state->interrupt_slot->bindQueryStatus(elem);

            if (!ast || !ast->as<ASTSelectWithUnionQuery>() || !io.pipeline.pulling())
            {
                io.onCancelOrConnectionLoss();
                interrupt_controller->unregisterSlot(state->interrupt_slot.get());
#if USE_PYTHON
                if (py_tables_token != 0 && python_table_cache)
                    python_table_cache->detachQueryContext(query_context.get());
#endif
                return std::make_unique<CHDB::StreamQueryResult>(
                    fmt::format("{} for query: {}", CHDB::kErrorStreamingNotSupportedPrefix, query_str),
                    ErrorCodes::BAD_ARGUMENTS);
            }

            state->query_context = query_context;
            state->parsed_query = std::move(ast);
            state->io = std::move(io);
            state->header = std::make_shared<const Block>(state->io.pipeline.getHeader());
            state->thread_group = CurrentThread::getGroup();
            state->io.pipeline.setConcurrencyControl(false);

#if defined(CHDB_WASM_SINGLE_THREADED)
            state->sync_executor = std::make_unique<PullingPipelineExecutor>(state->io.pipeline);
#else
            if (state->io.pipeline.getNumThreads() <= 1)
                state->sync_executor = std::make_unique<PullingPipelineExecutor>(state->io.pipeline);
            else
                state->async_executor = std::make_unique<PullingAsyncPipelineExecutor>(state->io.pipeline);
#endif

#if USE_PYTHON
            state->python_tables_bind_token = std::exchange(py_tables_token, 0);
#endif
        }
        catch (const Exception & e)
        {
#if USE_PYTHON
            if (py_tables_token != 0 && python_table_cache)
                python_table_cache->detachQueryContext(query_context.get());
#endif
            interrupt_controller->unregisterSlot(state->interrupt_slot.get());

            /// Syntax errors fail immediately at init time; semantic/analysis errors
            /// (e.g. UNKNOWN_TABLE, missing/bad query parameter) defer to the first
            /// fetch(), preserving exact C ABI and Python send_query error timing.
            if (e.code() == ErrorCodes::SYNTAX_ERROR)
            {
                return std::make_unique<CHDB::StreamQueryResult>(
                    getExceptionMessage(e, print_stack_trace), e.code());
            }

            state->deferred_error_message = getExceptionMessage(e, print_stack_trace);
            state->deferred_error_code = e.code();
        }

        registerActiveStream(state);

        auto result = std::make_unique<CHDB::StreamQueryResult>();
        result->stream_state = state;
        result->cancel_fn = &cancelStreamExecutionStateHook;
        return result;
    }
    catch (const Exception & e)
    {
        interrupt_controller->unregisterSlot(state->interrupt_slot.get());
        return std::make_unique<CHDB::StreamQueryResult>(
            getExceptionMessage(e, print_stack_trace), e.code());
    }
    catch (...)
    {
        interrupt_controller->unregisterSlot(state->interrupt_slot.get());
        return std::make_unique<CHDB::StreamQueryResult>(
            getCurrentExceptionMessage(print_stack_trace), getCurrentExceptionCode());
    }
}

CHDB::QueryResultPtr EmbeddedSession::executeStreamingIterate(void * streaming_result, bool is_canceled)
{
    if (isActiveOnCurrentThreadOrQuery())
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "Cannot fetch streaming results on a connection from its own active callback",
            ErrorCodes::BAD_ARGUMENTS);
    }

    auto * handle = reinterpret_cast<CHDB::StreamQueryResult *>(streaming_result);
    if (!handle || !handle->stream_state)
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "No active streaming query", ErrorCodes::BAD_ARGUMENTS);
    }

    auto state = std::static_pointer_cast<StreamExecutionState>(handle->stream_state);
    if (is_canceled)
    {
        state->cancelAndTeardown();
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
    }

    std::lock_guard<std::mutex> stream_lock(state->mutex);

    if (!state->deferred_error_message.empty())
    {
        String msg = std::move(state->deferred_error_message);
        int code = state->deferred_error_code;
        state->had_error = true;
        state->finished = true;
        state->releaseBindingsAndContextLocked();
        return std::make_unique<CHDB::MaterializedQueryResult>(std::move(msg), code);
    }

    if (state->cancelled)
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
    }

    if (state->finished)
    {
        if (state->had_error)
            return std::make_unique<CHDB::MaterializedQueryResult>(
                "No active streaming query", ErrorCodes::BAD_ARGUMENTS);
        if (state->is_chunk_collect)
            return std::make_unique<CHDB::ChunkQueryResult>(
                std::vector<Chunk>{}, state->header, 0.0, 0, 0, 0, 0);
        return std::make_unique<CHDB::MaterializedQueryResult>(nullptr, 0.0, 0, 0, 0, 0);
    }

    try
    {
        ScopedSessionThreadStatus thread_status;
        if (state->thread_group)
            CurrentThread::attachToGroupIfDetached(state->thread_group);

        if (state->interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

        auto finish_stream_cleanly = [&]()
        {
            state->sync_executor.reset();
            state->async_executor.reset();
            state->io.onFinish();
            if (state->thread_group)
                state->thread_group->memory_tracker.logPeakMemoryUsage();
            state->finished = true;
            state->releaseBindingsAndContextLocked();
        };

        if (state->is_chunk_collect)
        {
            Chunk chunk;
            bool got_chunk = false;
            while (state->sync_executor ? state->sync_executor->pull(chunk) : state->async_executor->pull(chunk))
            {
                if (state->interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
                if (chunk.hasRows())
                {
                    got_chunk = true;
                    break;
                }
            }

            if (state->interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

            const ProgressValues cur_prog = state->progress->getValues();
            const double cur_elapsed = state->watch->elapsedSeconds();
            const uint64_t delta_storage_rows = cur_prog.read_rows - state->prev_progress.read_rows;
            const uint64_t delta_storage_bytes = cur_prog.read_bytes - state->prev_progress.read_bytes;
            const uint64_t delta_written_rows = cur_prog.written_rows - state->prev_progress.written_rows;
            const uint64_t delta_written_bytes = cur_prog.written_bytes - state->prev_progress.written_bytes;
            const double delta_elapsed = std::max(0.0, cur_elapsed - state->prev_elapsed);
            state->prev_progress = cur_prog;
            state->prev_elapsed = cur_elapsed;

            if (!got_chunk || state->io.null_format)
            {
                SharedHeader saved_header = state->header;
                finish_stream_cleanly();
                return std::make_unique<CHDB::ChunkQueryResult>(
                    std::vector<Chunk>{}, std::move(saved_header), 0.0, 0, 0, 0, 0);
            }

            materializeChunk(chunk);
            const uint64_t rows = chunk.getNumRows();
            const uint64_t bytes = chunk.bytes();
            std::vector<Chunk> batch;
            batch.emplace_back(std::move(chunk));

            auto res = std::make_unique<CHDB::ChunkQueryResult>(
                std::move(batch),
                state->header,
                delta_elapsed,
                rows,
                bytes,
                delta_storage_rows,
                delta_storage_bytes,
                delta_written_rows,
                delta_written_bytes);
            if (state->header)
                fillResultSchemaFromHeader(*res, *state->header);
            return res;
        }

        Block block;
        bool got_block = false;
        while (state->sync_executor ? state->sync_executor->pull(block) : state->async_executor->pull(block))
        {
            if (state->interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
            if (block.rows() > 0)
            {
                got_block = true;
                break;
            }
        }

        if (state->interrupt_slot->interrupt_requested.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

        const ProgressValues cur_prog = state->progress->getValues();
        const double cur_elapsed = state->watch->elapsedSeconds();
        const uint64_t delta_storage_rows = cur_prog.read_rows - state->prev_progress.read_rows;
        const uint64_t delta_storage_bytes = cur_prog.read_bytes - state->prev_progress.read_bytes;
        const uint64_t delta_written_rows = cur_prog.written_rows - state->prev_progress.written_rows;
        const uint64_t delta_written_bytes = cur_prog.written_bytes - state->prev_progress.written_bytes;
        const double delta_elapsed = std::max(0.0, cur_elapsed - state->prev_elapsed);
        state->prev_progress = cur_prog;
        state->prev_elapsed = cur_elapsed;

        if (!got_block || state->io.null_format)
        {
            finish_stream_cleanly();
            return std::make_unique<CHDB::MaterializedQueryResult>(nullptr, 0.0, 0, 0, 0, 0);
        }

        const uint64_t rows = block.rows();
        const uint64_t bytes = block.bytes();

        std::unique_ptr<std::vector<char>> output_vec;
        {
            MemoryTrackerBlockerInThread blocker;
            output_vec = std::make_unique<std::vector<char>>(4096);
        }
        {
            WriteBufferFromVectorMTB<std::vector<char>> mem_buf(*output_vec);
            const String effective_format = resolveEffectiveOutputFormat(
                state->query_context, state->parsed_query, state->caller_format, false, {});
            auto fmt_settings = getFormatSettings(state->query_context);
            OutputFormatPtr output_format = state->query_context->getOutputFormatParallelIfPossible(
                effective_format, mem_buf, materializeBlock(*state->header), fmt_settings);
            output_format->write(materializeBlock(block));
            output_format->finalize();
            mem_buf.finalize();
        }

        if (output_vec->empty())
            output_vec.reset();

        auto res = std::make_unique<CHDB::MaterializedQueryResult>(
            CHDB::ResultBuffer(std::move(output_vec)),
            delta_elapsed,
            rows,
            bytes,
            delta_storage_rows,
            delta_storage_bytes,
            delta_written_rows,
            delta_written_bytes);
        if (state->header)
            fillResultSchemaFromHeader(*res, *state->header);
        return res;
    }
    catch (const Exception & e)
    {
        state->had_error = true;
        state->finished = true;
        state->sync_executor.reset();
        state->async_executor.reset();
        try { state->io.onException(); } catch (...) {}
        state->releaseBindingsAndContextLocked();
        return std::make_unique<CHDB::MaterializedQueryResult>(
            getExceptionMessage(e, state->print_stack_trace), e.code());
    }
    catch (...)
    {
        const int code = getCurrentExceptionCode();
        const String msg = getCurrentExceptionMessage(state->print_stack_trace);
        state->had_error = true;
        state->finished = true;
        state->sync_executor.reset();
        state->async_executor.reset();
        try { state->io.onException(); } catch (...) {}
        state->releaseBindingsAndContextLocked();
        return std::make_unique<CHDB::MaterializedQueryResult>(msg, code);
    }
}

void EmbeddedSession::cancelStreamingQuery(void * streaming_result)
{
    checkNotReentrant();
    auto * handle = reinterpret_cast<CHDB::StreamQueryResult *>(streaming_result);
    if (!handle)
        return;
    handle->cancelStream();
}

CHDB::QueryResultPtr EmbeddedSession::executeInsertStreamingInit(
    const char * query, size_t query_len, const char * format, size_t format_len)
{
    if (isActiveOnCurrentThreadOrQuery())
    {
        return std::make_unique<CHDB::InsertStreamResult>(
            "Cannot start a streaming insert on a connection from its own active callback",
            ErrorCodes::BAD_ARGUMENTS);
    }

    const NameToNameMap * call_params = currentCallParameters();
    const String query_str(query ? query : "", query ? query_len : 0);
    const String format_str(format ? format : "", format ? format_len : 0);

    auto ctx = std::make_shared<CHDB::InsertStreamContext>();
    ctx->queue_buf = std::make_shared<CHDB::QueueReadBuffer>();
    ctx->format = format_str.empty() ? String("Values") : format_str;
    ctx->full_query = format_str.empty() ? query_str : (query_str + " FORMAT " + format_str);

    auto interrupt_slot = std::make_shared<CHDB::ActiveQuerySlot>();
    interrupt_slot->queue_buf = ctx->queue_buf;
    interrupt_slot->external_cancel_flag = &ctx->cancelled;
    ctx->interrupt_slot = interrupt_slot;

    {
        std::unique_lock<std::shared_mutex> lock(session_mutex);
        if (streaming_insert_context)
        {
            return std::make_unique<CHDB::InsertStreamResult>(
                "Another streaming query or insert is already active on this connection",
                ErrorCodes::BAD_ARGUMENTS);
        }

        ctx->query_context = makeQueryContextLocked(call_params, nullptr, 0, false);
        ctx->query_context->setSetting("input_format_parallel_parsing", false);
        streaming_insert_context = ctx;
    }

    interrupt_controller->registerSlot(interrupt_slot);

    try
    {
        const auto & s = ctx->query_context->getSettingsRef();
        const char * begin = ctx->full_query.data();
        const char * end = begin + ctx->full_query.size();
        ParserQuery parser(end, s[Setting::allow_settings_after_format_in_insert], s[Setting::implicit_select]);
        ASTPtr parsed = parseQuery(
            parser, begin, end, "", s[Setting::max_query_size], s[Setting::max_parser_depth], s[Setting::max_parser_backtracks]);
        if (!parsed)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to parse INSERT query");

        const auto * insert_ast = parsed->as<ASTInsertQuery>();
        if (!insert_ast)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "send_insert requires an INSERT query");
        if (insert_ast->select)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "send_insert does not support INSERT ... SELECT (use a normal query)");

        ctx->parsed_query = std::move(parsed);

        auto init_future = ctx->init_promise.get_future();
        ctx->worker = ThreadFromGlobalPool([this, ctx]() { runInsertStreamWorker(ctx); });

        String init_error = init_future.get();
        if (!init_error.empty())
        {
            if (ctx->worker.joinable())
                ctx->worker.join();
            interrupt_controller->unregisterSlot(interrupt_slot.get());
            {
                std::unique_lock<std::shared_mutex> lock(session_mutex);
                if (streaming_insert_context == ctx)
                    streaming_insert_context.reset();
            }
            return std::make_unique<CHDB::InsertStreamResult>(init_error, ctx->init_error_code);
        }

        auto result = std::make_unique<CHDB::InsertStreamResult>();
        result->context = ctx;
        result->owner = this;
        return result;
    }
    catch (const Exception & e)
    {
        interrupt_controller->unregisterSlot(interrupt_slot.get());
        {
            std::unique_lock<std::shared_mutex> lock(session_mutex);
            if (streaming_insert_context == ctx)
                streaming_insert_context.reset();
        }
        return std::make_unique<CHDB::InsertStreamResult>(
            getExceptionMessage(e, print_stack_trace), e.code());
    }
    catch (...)
    {
        interrupt_controller->unregisterSlot(interrupt_slot.get());
        {
            std::unique_lock<std::shared_mutex> lock(session_mutex);
            if (streaming_insert_context == ctx)
                streaming_insert_context.reset();
        }
        return std::make_unique<CHDB::InsertStreamResult>(
            getCurrentExceptionMessage(print_stack_trace), getCurrentExceptionCode());
    }
}

void EmbeddedSession::runInsertStreamWorker(const CHDB::InsertStreamContextPtr & ctx)
{
    auto signal_init = [&](const String & err, int code = 0)
    {
        if (!ctx->init_signaled)
        {
            ctx->init_signaled = true;
            ctx->init_error_code = code;
            ctx->init_promise.set_value(err);
        }
    };

    auto interrupt_slot = std::static_pointer_cast<CHDB::ActiveQuerySlot>(ctx->interrupt_slot);
    auto watch = std::make_shared<Stopwatch>(CLOCK_MONOTONIC);
    auto progress_ptr = std::shared_ptr<Progress>(&ctx->progress, [](Progress *) {});

    try
    {
        /// ThreadFromGlobalPool already owns a ThreadStatus; detach any outer group cleanly
        /// so QueryScope attaches this insert's own ThreadGroup without colliding.
        ThreadGroupPtr saved_group = CurrentThread::getGroup();
        if (saved_group)
            CurrentThread::detachFromGroupIfNotDetached();
        SCOPE_EXIT({
            if (CurrentThread::getGroup() != nullptr)
                CurrentThread::detachFromGroupIfNotDetached();
            if (saved_group)
                CurrentThread::attachToGroupIfDetached(saved_group);
        });

        attachProgressAndCancelCallbacks(ctx->query_context, progress_ptr, watch, interrupt_slot);
        QueryScope query_scope = QueryScope::create(ctx->query_context);

        auto [ast, io] = executeQuery(ctx->full_query, ctx->query_context, QueryFlags{}, QueryProcessingStage::Complete);

        if (auto elem = ctx->query_context->getProcessListElement())
            interrupt_slot->bindQueryStatus(elem);

        if (!io.pipeline.pushing())
        {
            io.onCancelOrConnectionLoss();
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected pushing pipeline for streaming INSERT");
        }

        try
        {
            std::unique_ptr<PushingPipelineExecutor> sync_push;
            std::unique_ptr<PushingAsyncPipelineExecutor> async_push;
            if (io.pipeline.getNumThreads() > 1)
            {
                async_push = std::make_unique<PushingAsyncPipelineExecutor>(io.pipeline);
                async_push->start();
                ctx->sample = async_push->getHeader();
            }
            else
            {
                sync_push = std::make_unique<PushingPipelineExecutor>(io.pipeline);
                sync_push->start();
                ctx->sample = sync_push->getHeader();
            }

            if (ctx->query_context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields]
                && ctx->query_context->hasInsertionTableColumnsDescription())
            {
                ctx->columns = *ctx->query_context->getInsertionTableColumnsDescription();
            }

            const auto & settings = ctx->query_context->getSettingsRef();
            auto source = ctx->query_context->getInputFormat(
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
                    return std::make_shared<AddingDefaultsTransform>(header, ctx->columns, *source, ctx->query_context);
                });
            }

            QueryPipeline input_pipeline(std::move(pipe));
            input_pipeline.setConcurrencyControl(false);
            PullingPipelineExecutor input_executor(input_pipeline);

            signal_init("");

            Block block;
            while (input_executor.pull(block))
            {
                if (ctx->cancelled.load(std::memory_order_acquire))
                {
                    input_executor.cancel();
                    break;
                }
                if (!block.empty())
                {
                    if (async_push)
                        async_push->push(std::move(block));
                    else
                        sync_push->push(std::move(block));
                }
            }

            if (!ctx->cancelled.load(std::memory_order_acquire))
            {
                if (async_push)
                    async_push->finish();
                else
                    sync_push->finish();

                async_push.reset();
                sync_push.reset();
                io.onFinish();

                const ProgressValues pv = ctx->progress.getValues();
                ctx->rows_written = pv.written_rows;
                ctx->bytes_written = pv.written_bytes;
                ctx->elapsed = watch->elapsedSeconds();
            }
            else
            {
                if (async_push)
                    async_push->cancel();
                if (sync_push)
                    sync_push->cancel();
                async_push.reset();
                sync_push.reset();
                io.onCancelOrConnectionLoss();
            }
        }
        catch (...)
        {
            io.onException();
            throw;
        }
    }
    catch (const Exception & e)
    {
        ctx->error = std::current_exception();
        ctx->error_code = e.code();
        try { ctx->error_message = getExceptionMessage(e, print_stack_trace); } catch (...) {}
        ctx->error_set.store(true, std::memory_order_release);
        signal_init(ctx->error_message.empty() ? String("Streaming insert failed") : ctx->error_message, e.code());
    }
    catch (...)
    {
        ctx->error = std::current_exception();
        ctx->error_code = getCurrentExceptionCode();
        try { ctx->error_message = getCurrentExceptionMessage(print_stack_trace); } catch (...) {}
        ctx->error_set.store(true, std::memory_order_release);
        signal_init(ctx->error_message.empty() ? String("Streaming insert failed") : ctx->error_message, ctx->error_code);
    }

    ctx->queue_buf->finish();
    ctx->query_context.reset();
    ctx->worker_done.store(true, std::memory_order_release);
}

bool EmbeddedSession::executeInsertStreamingAppend(void * insert_stream, const char * data, size_t len)
{
    checkNotReentrant();
    auto * res = reinterpret_cast<CHDB::InsertStreamResult *>(insert_stream);
    if (!res || !res->context)
        return false;

    auto ctx = std::static_pointer_cast<CHDB::InsertStreamContext>(res->context);
    if (ctx->error_set.load(std::memory_order_acquire) || ctx->worker_done.load(std::memory_order_acquire))
        return false;

    if (!ctx->queue_buf->append(data, len))
        return false;
    return !ctx->error_set.load(std::memory_order_acquire);
}

CHDB::QueryResultPtr EmbeddedSession::executeInsertStreamingDone(void * insert_stream)
{
    if (isActiveOnCurrentThreadOrQuery())
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            "Cannot finalize a streaming insert on a connection from its own active callback",
            ErrorCodes::BAD_ARGUMENTS);
    }

    auto * res = reinterpret_cast<CHDB::InsertStreamResult *>(insert_stream);
    CHDB::InsertStreamContextPtr ctx =
        (res && res->context) ? std::static_pointer_cast<CHDB::InsertStreamContext>(res->context) : nullptr;
    if (!ctx)
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            res ? res->getError() : "Invalid insert stream",
            res ? res->getErrorCode() : ErrorCodes::BAD_ARGUMENTS);
    }

    if (ctx->finalized.exchange(true))
    {
        return std::make_unique<CHDB::MaterializedQueryResult>(
            res->getError().empty() ? String("Insert stream already finalized") : res->getError(),
            res->getErrorCode() != 0 ? res->getErrorCode() : ErrorCodes::BAD_ARGUMENTS);
    }

    ctx->queue_buf->finish();
    if (ctx->worker.joinable())
        ctx->worker.join();

    if (ctx->interrupt_slot)
        interrupt_controller->unregisterSlot(static_cast<CHDB::ActiveQuerySlot *>(ctx->interrupt_slot.get()));

    {
        std::unique_lock<std::shared_mutex> lock(session_mutex);
        if (streaming_insert_context == ctx)
            streaming_insert_context.reset();
    }

    const bool failed = ctx->error_set.load(std::memory_order_acquire) || static_cast<bool>(ctx->error);
    if (failed)
    {
        String err = ctx->error_message.empty() ? String("Streaming insert failed") : ctx->error_message;
        const int code = ctx->error_code != 0 ? ctx->error_code : ErrorCodes::UNKNOWN_EXCEPTION;
        res->setError(err, code);
        return std::make_unique<CHDB::MaterializedQueryResult>(err, code);
    }

    return std::make_unique<CHDB::MaterializedQueryResult>(
        nullptr, ctx->elapsed, 0, 0, 0, 0, ctx->rows_written, ctx->bytes_written);
}

void EmbeddedSession::cancelInsertStream(void * insert_stream)
{
    checkNotReentrant();
    auto * res = reinterpret_cast<CHDB::InsertStreamResult *>(insert_stream);
    CHDB::InsertStreamContextPtr ctx =
        (res && res->context) ? std::static_pointer_cast<CHDB::InsertStreamContext>(res->context) : nullptr;
    if (!ctx)
        return;

    if (ctx->finalized.exchange(true))
        return;

    ctx->cancelled.store(true, std::memory_order_release);
    if (ctx->interrupt_slot)
        std::static_pointer_cast<CHDB::ActiveQuerySlot>(ctx->interrupt_slot)->cancel();
    ctx->queue_buf->finish();
    if (ctx->worker.joinable())
        ctx->worker.join();

    if (ctx->interrupt_slot)
        interrupt_controller->unregisterSlot(static_cast<CHDB::ActiveQuerySlot *>(ctx->interrupt_slot.get()));

    std::unique_lock<std::shared_mutex> lock(session_mutex);
    if (streaming_insert_context == ctx)
        streaming_insert_context.reset();
}

void EmbeddedSession::analyzeQuery(
    const char * sql, size_t sql_len, std::string_view target_database, chdb_query_analysis_v1 & out)
{
    out.statement_count = 0;
    out.flags = 0;
    out.query_class = CHDB_QUERY_UNKNOWN;

    if (!sql || sql_len == 0)
        return;

    checkNotReentrant();
    ChdbNamespaceScope ns_scope(database ? database->getNamespace() : nullptr);
    std::shared_lock<std::shared_mutex> lock(session_mutex);

    const String text(sql, sql_len);
    const char * pos = text.data();
    const char * const end = pos + text.size();

    auto result = CHDB_QUERY_READ_ONLY;
    size_t statements = 0;
    bool has_secrets = false;
    bool writes_only_target = true;
    bool changes_lifecycle = false;

    try
    {
        const Settings & settings = session_context->getSettingsRef();
        const auto max_parser_depth = static_cast<uint32_t>(settings[Setting::max_parser_depth]);
        const auto max_parser_backtracks = static_cast<uint32_t>(settings[Setting::max_parser_backtracks]);
        const String current_database = session_context->getCurrentDatabase();
        const String target(target_database);

        while (pos < end)
        {
            Tokens separators(pos, end);
            IParser::Pos separator_iterator(separators, max_parser_depth, max_parser_backtracks);
            while (separator_iterator.isValid() && separator_iterator->type == TokenType::Semicolon)
                ++separator_iterator;

            pos = separator_iterator->begin;
            if (pos >= end)
                break;

            ASTPtr ast = parseStatementForClassification(pos, end, settings);
            if (!ast)
                return;

            statements += CHDB::countExecutableStatements(*ast);
            result = CHDB::combineQueryClass(result, CHDB::classifyStatement(*ast, current_database));
            has_secrets |= CHDB::statementHasSecrets(*ast);
            changes_lifecycle |= CHDB::changesDatabaseLifecycle(*ast);
            writes_only_target &= CHDB::writesOnlyToDatabase(*ast, target, current_database);

            pos = findStatementEndForClassification(*ast, pos, end);
        }
    }
    catch (...)
    {
        return;
    }

    if (statements == 0)
        return;

    out.statement_count = static_cast<uint32_t>(statements);
    out.query_class = static_cast<uint32_t>(result);
    if (has_secrets)
        out.flags |= CHDB_QUERY_HAS_SECRETS;
    if (changes_lifecycle)
        out.flags |= CHDB_QUERY_CHANGES_DATABASE_LIFECYCLE;
    if (writes_only_target && !target_database.empty())
        out.flags |= CHDB_QUERY_WRITES_ONLY_TARGET_DATABASE;
}

} // namespace DB
