/// ADBC driver entrypoint for libchdb: implements the AdbcDatabase /
/// AdbcConnection / AdbcStatement contract on top of the public chdb C API,
/// so the library itself is loadable by the standard driver managers
/// (driver=<libchdb path>, entrypoint=chdb_adbc_init).
///
/// Results travel through the Arrow C Data Interface; bulk ingestion goes
/// through chdb_arrow_scan + INSERT ... SELECT FROM arrowstream(...).
///
/// Result streams: the engine runs one statement at a time per connection.
/// A statement's next Execute invalidates its own prior stream (spec-required);
/// a different statement or a metadata call that hits a still-live stream is
/// rejected with INVALID_STATE rather than silently invalidating it (see
/// reclaimActiveStream). Independent concurrent readers work over separate
/// connections; note that session state (SET, temporary tables) is
/// per-connection.

#include "chdb.h"
#include "chdb-internal.h"

#include <Parsers/Lexer.h>

#include <arrow/c/abi.h>

#include "adbc/adbc.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/config.h>

#include <Common/config_version.h>

#include <algorithm>
#include <atomic>
#include <cctype>
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

/// Engine errors name their ClickHouse error class, e.g. "... (UNKNOWN_TABLE)".
/// The classes with a direct ADBC counterpart map to it so callers can act on
/// the status code; everything else stays INTERNAL.
AdbcStatusCode statusForEngineError(const std::string & message)
{
    /// ClickHouse appends its error class as a trailing "(CLASS_NAME)" token,
    /// e.g. "... (UNKNOWN_TABLE)", optionally followed by " (version ...)".
    /// Match only that terminal token so a class name echoed earlier in the
    /// message (e.g. inside reflected SQL) can't be mistaken for the class.
    /// The names are stable enum identifiers; matching a substring of the
    /// token still lets a family prefix (e.g. CANNOT_PARSE*) map together.
    std::string cls;
    for (size_t close = message.find_last_of(')'); close != std::string::npos;)
    {
        size_t open = message.rfind('(', close);
        if (open == std::string::npos)
            break;
        std::string tok = message.substr(open + 1, close - open - 1);
        if (tok.compare(0, 8, "version ") != 0) // skip a trailing "(version ...)"
        {
            cls = tok;
            break;
        }
        close = open == 0 ? std::string::npos : message.rfind(')', open - 1);
    }
    auto has = [&](const char * name) { return cls.find(name) != std::string::npos; };

    if (has("UNKNOWN_TABLE") || has("UNKNOWN_DATABASE") || has("UNKNOWN_IDENTIFIER")
        || has("UNKNOWN_FUNCTION") || has("UNKNOWN_SETTING") || has("FILE_DOESNT_EXIST"))
        return ADBC_STATUS_NOT_FOUND;
    if (has("TABLE_ALREADY_EXISTS") || has("DATABASE_ALREADY_EXISTS"))
        return ADBC_STATUS_ALREADY_EXISTS;
    if (has("SYNTAX_ERROR") || has("CANNOT_PARSE") || has("TYPE_MISMATCH")
        || has("ILLEGAL_TYPE_OF_ARGUMENT") || has("NUMBER_OF_ARGUMENTS_DOESNT_MATCH")
        || has("BAD_ARGUMENTS") || has("BAD_QUERY_PARAMETER") || has("VALUE_IS_OUT_OF_RANGE")
        || has("ARGUMENT_OUT_OF_BOUND") || has("UNKNOWN_TYPE") || has("NO_COMMON_TYPE"))
        return ADBC_STATUS_INVALID_ARGUMENT;
    if (has("CANNOT_OPEN_FILE") || has("CANNOT_READ") || has("CANNOT_WRITE")
        || has("NETWORK_ERROR") || has("SOCKET_TIMEOUT") || has("CANNOT_READ_FROM_SOCKET")
        || has("CANNOT_WRITE_TO_SOCKET") || has("FILE_ALREADY_EXISTS"))
        return ADBC_STATUS_IO;
    if (has("NOT_IMPLEMENTED") || has("SUPPORT_IS_DISABLED"))
        return ADBC_STATUS_NOT_IMPLEMENTED;
    return ADBC_STATUS_INTERNAL;
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

struct StreamingResultState;

struct ConnectionImpl
{
    /// Handle returned by chdb_connect(); the engine instance is shared,
    /// ref-counted process state (one storage path per process).
    chdb_connection * conn = nullptr;
    /// Serializes statement execution on this connection.
    std::mutex mutex;
    /// The connection's outstanding streamed result, if any. The engine runs
    /// one statement at a time per connection; a new operation resolves this
    /// via reclaimActiveStream (same statement re-execute invalidates it,
    /// a different statement or metadata call is rejected while it is live).
    std::mutex stream_mutex;
    StreamingResultState * active_stream = nullptr;
};

std::atomic<uint64_t> statement_id_counter{1};

struct StatementImpl
{
    ConnectionImpl * connection = nullptr;
    /// Stable identity for stream-ownership checks. A raw StatementImpl* is
    /// unsafe here: a released statement's address can be reused by a new
    /// statement while the old stream is still live, causing a false match.
    /// A monotonic id never collides. 0 is reserved for "not a statement".
    const uint64_t id = statement_id_counter.fetch_add(1);
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

/// Streaming result adapter: exposes chdb's pull-one-batch streaming
/// (chdb_stream_query_arrow + chdb_stream_fetch_arrow, where each fetch
/// yields a single-batch ArrowArrayStream) as one continuous
/// ArrowArrayStream, so large results never materialize in full.
struct StreamingResultState
{
    ConnectionImpl * owner = nullptr;
    /// Identity of the statement that produced this stream (compared, never
    /// dereferenced). The spec lets the SAME statement's next Execute
    /// invalidate its own prior result, but a DIFFERENT statement (or a
    /// metadata call) must not silently invalidate it — see reclaimActiveStream.
    /// Stored as the owning statement's monotonic id (0 = none), never a
    /// pointer, so a reused statement address cannot cause a false match.
    uint64_t owner_statement = 0;
    chdb_connection conn = nullptr;
    chdb_result * stream_result = nullptr;
    std::shared_ptr<arrow::Schema> schema;
    ArrowArray pending{};
    bool has_pending = false;
    bool exhausted = false;
    /// Set when a subsequent statement on the same connection (or closing
    /// the connection) takes over: the engine runs one statement at a time
    /// per connection, so the older stream must stop touching it.
    bool invalidated = false;
    std::string last_error;
    /// Guards all mutable members: get_next runs on the consumer's thread
    /// while invalidation comes from the thread executing the next statement.
    std::mutex mutex;

    /// Caller holds `mutex`.
    void releaseEngineStream()
    {
        if (stream_result)
        {
            if (!exhausted)
                chdb_stream_cancel_query(conn, stream_result);
            chdb_destroy_query_result(stream_result);
            stream_result = nullptr;
        }
    }

    ~StreamingResultState()
    {
        if (owner)
        {
            std::lock_guard reg(owner->stream_mutex);
            if (owner->active_stream == this)
                owner->active_stream = nullptr;
        }
        std::lock_guard guard(mutex);
        if (has_pending && pending.release)
            pending.release(&pending);
        releaseEngineStream();
    }
};

/// A new operation wants the connection, which runs one statement at a time.
/// The ADBC concurrency spec offers three ways to handle an outstanding
/// result stream: buffer it, execute concurrently, or error. chDB can do
/// neither of the first two for a live stream, so it takes the error option —
/// EXCEPT the spec also *requires* the same statement's next Execute to
/// invalidate its own prior result. owner_for_reuse identifies the caller
/// when it is that statement's Execute (nullptr for metadata calls and other
/// statements, which may never reuse and so always get the error).
///
/// A stream that is already exhausted or invalidated is not "live": it is
/// silently detached so ordinary sequential use is unaffected.
[[nodiscard]] AdbcStatusCode reclaimActiveStream(
    ConnectionImpl * connection, uint64_t owner_for_reuse, const char * reason, AdbcError * error)
{
    std::lock_guard reg(connection->stream_mutex);
    auto * old = connection->active_stream;
    if (!old)
        return ADBC_STATUS_OK;
    std::lock_guard guard(old->mutex);
    const bool consumed = old->exhausted || old->invalidated;
    if (!consumed && !(owner_for_reuse != 0 && old->owner_statement == owner_for_reuse))
        return setError(
            error,
            ADBC_STATUS_INVALID_STATE,
            "[chdb] another result stream is still open on this connection; release it "
            "before running new operations (chDB executes one statement at a time per "
            "connection)");
    old->invalidated = true;
    old->last_error = reason;
    old->releaseEngineStream();
    connection->active_stream = nullptr;
    return ADBC_STATUS_OK;
}

constexpr const char * kStreamInvalidatedBySuccessor
    = "[chdb] result stream invalidated by a subsequent statement on this connection";
constexpr const char * kStreamInvalidatedByClose
    = "[chdb] result stream invalidated: connection closed";

/// ---------------------------------------------------------------------
/// Small utilities
/// ---------------------------------------------------------------------

/// Decodes %XX escapes (RFC 3986); malformed escapes are kept verbatim.
std::string percentDecode(const std::string & in)
{
    std::string out;
    out.reserve(in.size());
    for (size_t i = 0; i < in.size(); ++i)
    {
        if (in[i] == '%' && i + 2 < in.size()
            && std::isxdigit(static_cast<unsigned char>(in[i + 1]))
            && std::isxdigit(static_cast<unsigned char>(in[i + 2])))
        {
            out += static_cast<char>(std::stoi(in.substr(i + 1, 2), nullptr, 16));
            i += 2;
        }
        else
            out += in[i];
    }
    return out;
}

/// Parses a file:/chdb:-style URI tail (everything after "<scheme>:") per
/// RFC 8089: <scheme>:name, <scheme>:/abs, <scheme>:///abs and
/// <scheme>://localhost/abs, with %XX percent-decoding. Only an empty or
/// "localhost" authority is accepted. Writes the decoded path into `out`;
/// on a bad authority fills `error` and returns a non-OK status.
AdbcStatusCode parseFileLikeUri(
    const char * scheme, const std::string & after_scheme, std::string & out, AdbcError * error)
{
    std::string rest = after_scheme;
    if (rest.rfind("//", 0) == 0)
    {
        rest = rest.substr(2);
        const auto slash = rest.find('/');
        const std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
        if (!authority.empty() && authority != "localhost")
            return setError(
                error, ADBC_STATUS_INVALID_ARGUMENT,
                std::string("[chdb] unsupported ") + scheme + " URI authority '" + authority + "'");
        rest = slash == std::string::npos ? "" : rest.substr(slash);
    }
    out = percentDecode(rest);
    return ADBC_STATUS_OK;
}

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
/// True when the first significant token is INSERT — the statement kind for
/// which a rows_written of zero is a known count, not "not applicable".
bool statementIsInsert(const std::string & sql)
{
    DB::Lexer lexer(sql.data(), sql.data() + sql.size());
    auto t = lexer.nextToken();
    while (t.type == DB::TokenType::Whitespace || t.type == DB::TokenType::Comment)
        t = lexer.nextToken();
    static constexpr char kw[] = "INSERT";
    if (t.type != DB::TokenType::BareWord || t.end - t.begin != 6)
        return false;
    for (int i = 0; i < 6; ++i)
        if (std::toupper(t.begin[i]) != kw[i])
            return false;
    return true;
}

/// zero_rows_known distinguishes a known count of zero (write statements)
/// from "not applicable" (-1, e.g. DDL).
AdbcStatusCode consumeResult(
    chdb_result * result, int64_t * rows_affected, AdbcError * error, bool zero_rows_known = false)
{
    if (!result)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] query returned no result handle");

    const char * err = chdb_result_error(result);
    if (err)
    {
        std::string message(err);
        chdb_destroy_query_result(result);
        return setError(error, statusForEngineError(message), "[chdb] " + message);
    }

    if (rows_affected)
    {
        uint64_t written = chdb_result_rows_written(result);
        *rows_affected = (written > 0 || zero_rows_known) ? static_cast<int64_t>(written) : -1;
    }
    chdb_destroy_query_result(result);
    return ADBC_STATUS_OK;
}

/// Runs a query whose output is discarded (DDL / INSERT ... SELECT).
/// zero_rows_known_override marks callers that know the statement writes
/// (bulk ingestion) even when it doesn't start with INSERT.
AdbcStatusCode runUpdateQuery(
    ConnectionImpl * connection,
    const std::string & sql,
    int64_t * rows_affected,
    AdbcError * error,
    bool zero_rows_known_override = false)
{
    chdb_result * result = chdb_query_n(*connection->conn, sql.c_str(), sql.size(), "Null", 4);
    return consumeResult(result, rows_affected, error, zero_rows_known_override || statementIsInsert(sql));
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
        return setError(error, statusForEngineError(message), "[chdb] " + message);
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

/// Quotes a string as a ClickHouse SQL string literal.
std::string quoteStringLiteral(const std::string & value)
{
    std::string quoted = "'";
    for (char c : value)
    {
        if (c == '\'' || c == '\\')
            quoted += '\\';
        quoted += c;
    }
    quoted += '\'';
    return quoted;
}

/// Finds `?` placeholders through the engine's own lexer, so every literal
/// form the dialect knows (strings, quoted identifiers, comments, heredocs)
/// is handled by construction and stays in step with the dialect.
std::vector<size_t> findPlaceholders(const std::string & sql)
{
    std::vector<size_t> positions;
    DB::Lexer lexer(sql.data(), sql.data() + sql.size());
    for (auto token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        if (token.isError())
            break; /// the engine reports malformed SQL at execution
        if (token.type == DB::TokenType::QuestionMark)
            positions.push_back(static_cast<size_t>(token.begin - sql.data()));
    }
    return positions;
}

/// Matches the exact shape `INSERT INTO [db.]table [(col, ...)] VALUES
/// (?, ...)` on the engine's token stream. On match, head_out is the SQL
/// up to (excluding) VALUES — quoting preserved verbatim — and
/// placeholder_count the arity, so executemany can stream all bound rows
/// through one INSERT ... SELECT instead of one parse/execute per row.
bool matchPlainInsertShape(const std::string & sql, std::string & head_out, size_t & placeholder_count)
{
    DB::Lexer lexer(sql.data(), sql.data() + sql.size());
    auto next = [&]() -> DB::Token
    {
        auto t = lexer.nextToken();
        while (t.type == DB::TokenType::Whitespace || t.type == DB::TokenType::Comment)
            t = lexer.nextToken();
        return t;
    };
    auto is_keyword = [&](const DB::Token & t, const char * kw)
    {
        if (t.type != DB::TokenType::BareWord)
            return false;
        const size_t len = std::strlen(kw);
        if (static_cast<size_t>(t.end - t.begin) != len)
            return false;
        for (size_t i = 0; i < len; ++i)
            if (std::toupper(t.begin[i]) != kw[i])
                return false;
        return true;
    };
    auto is_ident = [](const DB::Token & t)
    { return t.type == DB::TokenType::BareWord || t.type == DB::TokenType::QuotedIdentifier; };

    if (!is_keyword(next(), "INSERT") || !is_keyword(next(), "INTO"))
        return false;
    if (!is_ident(next()))
        return false;
    auto t = next();
    if (t.type == DB::TokenType::Dot)
    {
        if (!is_ident(next()))
            return false;
        t = next();
    }
    if (t.type == DB::TokenType::OpeningRoundBracket)
    {
        /// column list: idents separated by commas
        for (;;)
        {
            if (!is_ident(next()))
                return false;
            auto sep = next();
            if (sep.type == DB::TokenType::ClosingRoundBracket)
                break;
            if (sep.type != DB::TokenType::Comma)
                return false;
        }
        t = next();
    }
    if (!is_keyword(t, "VALUES"))
        return false;
    head_out.assign(sql.data(), static_cast<size_t>(t.begin - sql.data()));
    if (next().type != DB::TokenType::OpeningRoundBracket)
        return false;
    placeholder_count = 0;
    for (;;)
    {
        if (next().type != DB::TokenType::QuestionMark)
            return false;
        ++placeholder_count;
        auto sep = next();
        if (sep.type == DB::TokenType::ClosingRoundBracket)
            break;
        if (sep.type != DB::TokenType::Comma)
            return false;
    }
    t = next();
    if (t.type == DB::TokenType::Semicolon)
        t = next();
    return t.isEnd();
}

/// Maps an Arrow type to the ClickHouse type name used in a {name:Type}
/// placeholder.
AdbcStatusCode clickhouseTypeFor(
    const std::shared_ptr<arrow::DataType> & type, std::string & out, AdbcError * error)
{
    switch (type->id())
    {
        case arrow::Type::BOOL: out = "Bool"; return ADBC_STATUS_OK;
        case arrow::Type::INT8: out = "Int8"; return ADBC_STATUS_OK;
        case arrow::Type::INT16: out = "Int16"; return ADBC_STATUS_OK;
        case arrow::Type::INT32: out = "Int32"; return ADBC_STATUS_OK;
        case arrow::Type::INT64: out = "Int64"; return ADBC_STATUS_OK;
        case arrow::Type::UINT8: out = "UInt8"; return ADBC_STATUS_OK;
        case arrow::Type::UINT16: out = "UInt16"; return ADBC_STATUS_OK;
        case arrow::Type::UINT32: out = "UInt32"; return ADBC_STATUS_OK;
        case arrow::Type::UINT64: out = "UInt64"; return ADBC_STATUS_OK;
        case arrow::Type::FLOAT: out = "Float32"; return ADBC_STATUS_OK;
        case arrow::Type::DOUBLE: out = "Float64"; return ADBC_STATUS_OK;
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY: out = "String"; return ADBC_STATUS_OK;
        /// All-null columns arrive as Arrow's null type; String is the base
        /// placeholder — the null_count>0 rule wraps it in Nullable (avoiding
        /// a double Nullable(Nullable(...))) and the values bind as \N.
        case arrow::Type::NA: out = "String"; return ADBC_STATUS_OK;
        case arrow::Type::DATE32: out = "Date32"; return ADBC_STATUS_OK;
        case arrow::Type::DATE64: out = "DateTime64(3)"; return ADBC_STATUS_OK;
        case arrow::Type::TIMESTAMP:
        {
            const auto & ts = static_cast<const arrow::TimestampType &>(*type);
            int scale = 0;
            switch (ts.unit())
            {
                case arrow::TimeUnit::SECOND: scale = 0; break;
                case arrow::TimeUnit::MILLI: scale = 3; break;
                case arrow::TimeUnit::MICRO: scale = 6; break;
                case arrow::TimeUnit::NANO: scale = 9; break;
            }
            out = "DateTime64(" + std::to_string(scale);
            if (!ts.timezone().empty())
                out += ", " + quoteStringLiteral(ts.timezone());
            out += ")";
            return ADBC_STATUS_OK;
        }
        case arrow::Type::DECIMAL128:
        {
            const auto & dec = static_cast<const arrow::Decimal128Type &>(*type);
            out = "Decimal(" + std::to_string(dec.precision()) + ", " + std::to_string(dec.scale()) + ")";
            return ADBC_STATUS_OK;
        }
        default:
            return setError(
                error,
                ADBC_STATUS_NOT_IMPLEMENTED,
                "[chdb] binding parameters of Arrow type " + type->ToString() + " is not implemented");
    }
}

/// Textual parameter value for a non-null Arrow scalar, as consumed by the
/// engine's {name:Type} parser. The engine reads parameter values as escaped
/// text: an unescaped tab or newline terminates the value mid-parse, and a
/// bare \N reads as NULL — so string/binary payloads must be escaped or they
/// error out (or worse, silently change value).
std::string scalarToParamString(const std::shared_ptr<arrow::Scalar> & scalar)
{
    switch (scalar->type->id())
    {
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
        {
            const auto & binary = static_cast<const arrow::BaseBinaryScalar &>(*scalar);
            const std::string raw = binary.value->ToString();
            std::string escaped;
            escaped.reserve(raw.size());
            for (char c : raw)
            {
                switch (c)
                {
                    case '\\':
                        escaped += "\\\\";
                        break;
                    case '\t':
                        escaped += "\\t";
                        break;
                    case '\n':
                        escaped += "\\n";
                        break;
                    case '\r':
                        escaped += "\\r";
                        break;
                    case '\0':
                        escaped += "\\0";
                        break;
                    default:
                        escaped += c;
                }
            }
            return escaped;
        }
        default:
            return scalar->ToString();
    }
}

/// Wraps a chdb_result buffer as an arrow::Buffer, tying the result's
/// lifetime to the buffer (used for zero-copy IPC import).
class ChdbResultBuffer : public arrow::Buffer
{
public:
    explicit ChdbResultBuffer(chdb_result * result)
        : arrow::Buffer(
              reinterpret_cast<const uint8_t *>(chdb_result_buffer(result)),
              static_cast<int64_t>(chdb_result_length(result)))
        , result_(result)
    {
    }
    ~ChdbResultBuffer() override { chdb_destroy_query_result(result_); }

private:
    chdb_result * result_;
};

/// Exposes an ArrowStream-format (IPC) chdb_result through the caller's
/// ArrowArrayStream. Takes ownership of the result.
AdbcStatusCode exportIpcResult(chdb_result * result, ArrowArrayStream * out, AdbcError * error)
{
    if (!result)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] query returned no result handle");
    const char * err = chdb_result_error(result);
    if (err)
    {
        std::string message(err);
        chdb_destroy_query_result(result);
        return setError(error, statusForEngineError(message), "[chdb] " + message);
    }
    if (chdb_result_length(result) == 0)
    {
        /// No result bytes: a statement without a result set (DDL, ...) —
        /// hand back an empty zero-column stream instead of failing to
        /// parse an empty IPC payload.
        chdb_destroy_query_result(result);
        auto schema = arrow::schema(arrow::FieldVector{});
        auto batch = arrow::RecordBatch::Make(schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
        return exportBatch(schema, batch, out, error);
    }
    auto buffer = std::make_shared<ChdbResultBuffer>(result);
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    if (!reader.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + reader.status().ToString());
    auto status = arrow::ExportRecordBatchReader(reader.ValueUnsafe(), out);
    if (!status.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + status.ToString());
    return ADBC_STATUS_OK;
}

/// Materializes an IPC result's batches (the batches keep the underlying
/// chdb buffer alive through their shared_ptr chain). A result without bytes
/// contributes nothing: the statement executed but had no result set.
AdbcStatusCode ipcResultToBatches(
    chdb_result * result,
    std::shared_ptr<arrow::Schema> & schema,
    std::vector<std::shared_ptr<arrow::RecordBatch>> & batches,
    AdbcError * error)
{
    if (!result)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] query returned no result handle");
    const char * err = chdb_result_error(result);
    if (err)
    {
        std::string message(err);
        chdb_destroy_query_result(result);
        return setError(error, statusForEngineError(message), "[chdb] " + message);
    }
    if (chdb_result_length(result) == 0)
    {
        chdb_destroy_query_result(result);
        return ADBC_STATUS_OK;
    }
    auto buffer = std::make_shared<ChdbResultBuffer>(result);
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    if (!reader.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + reader.status().ToString());
    if (!schema)
        schema = reader.ValueUnsafe()->schema();
    while (true)
    {
        auto next = reader.ValueUnsafe()->Next();
        if (!next.ok())
            return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + next.status().ToString());
        if (!next.ValueUnsafe())
            break;
        batches.push_back(next.ValueUnsafe());
    }
    return ADBC_STATUS_OK;
}

/// Runs a query through the Arrow C Data output path and materializes the
/// result as a single-chunk arrow::Table.
AdbcStatusCode queryToTable(
    ConnectionImpl * connection, const std::string & sql, std::shared_ptr<arrow::Table> & table, AdbcError * error)
{
    /// Metadata helper: never a statement reuse, so a live stream is rejected.
    if (AdbcStatusCode s = reclaimActiveStream(connection, /*owner_for_reuse=*/0, kStreamInvalidatedBySuccessor, error);
        s != ADBC_STATUS_OK)
        return s;
    ArrowArrayStream stream;
    std::memset(&stream, 0, sizeof(stream));
    chdb_result * result = chdb_query_arrow_n(
        *connection->conn, sql.c_str(), sql.size(), reinterpret_cast<chdb_arrow_stream>(&stream), nullptr);
    AdbcStatusCode status = consumeResult(result, nullptr, error);
    if (status != ADBC_STATUS_OK)
    {
        if (stream.release)
            stream.release(&stream);
        return status;
    }
    auto reader = arrow::ImportRecordBatchReader(&stream);
    if (!reader.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + reader.status().ToString());
    auto table_result = reader.ValueUnsafe()->ToTable();
    if (!table_result.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + table_result.status().ToString());
    auto combined = table_result.ValueUnsafe()->CombineChunks();
    if (!combined.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + combined.status().ToString());
    table = combined.ValueUnsafe();
    return ADBC_STATUS_OK;
}

/// Name-based, type-checked column access: drift in a metadata SELECT list
/// or in the engine's Arrow output surfaces as INTERNAL instead of undefined
/// behavior from a blind positional cast.
template <typename ArrayType>
AdbcStatusCode getTypedColumn(
    const std::shared_ptr<arrow::Table> & table,
    const std::string & name,
    arrow::Type::type expected,
    std::shared_ptr<ArrayType> & out,
    AdbcError * error)
{
    auto column = table->GetColumnByName(name);
    if (!column || column->num_chunks() != 1)
        return setError(
            error, ADBC_STATUS_INTERNAL, "[chdb] metadata query result is missing column '" + name + "'");
    if (column->chunk(0)->type_id() != expected)
        return setError(
            error,
            ADBC_STATUS_INTERNAL,
            "[chdb] metadata column '" + name + "' has unexpected Arrow type "
                + column->chunk(0)->type()->ToString());
    out = std::static_pointer_cast<ArrayType>(column->chunk(0));
    return ADBC_STATUS_OK;
}

/// Resolves a struct child builder by field name with a type check, so the
/// population code cannot silently misalign if the schema definition above
/// is reordered or extended.
template <typename BuilderType>
AdbcStatusCode getFieldBuilder(
    arrow::StructBuilder * parent,
    const char * field_name,
    arrow::Type::type expected,
    BuilderType *& out,
    int * index_out,
    AdbcError * error)
{
    auto struct_type = std::static_pointer_cast<arrow::StructType>(parent->type());
    const int index = struct_type->GetFieldIndex(field_name);
    if (index < 0 || parent->field_builder(index)->type()->id() != expected)
        return setError(
            error,
            ADBC_STATUS_INTERNAL,
            std::string("[chdb] GetObjects result schema is missing field '") + field_name + "'");
    out = static_cast<BuilderType *>(parent->field_builder(index));
    if (index_out)
        *index_out = index;
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
    /// "path" and "uri" are aliases; the last one set wins. The uri form
    /// accepts the file scheme per RFC 8089: file:name, file:/abs,
    /// file:///abs and file://localhost/abs, with %XX percent-decoding.
    /// chDB's own "chdb:" scheme mirrors file: for paths but additionally
    /// recognizes in-memory sentinels (chdb:, chdb:memory, chdb::memory:,
    /// chdb://:memory:, chdb://memory) that all resolve to ":memory:".
    if (option == "path" || option == "uri")
    {
        if (option_value.rfind("file:", 0) == 0)
        {
            std::string parsed;
            if (AdbcStatusCode s = parseFileLikeUri("file", option_value.substr(5), parsed, error);
                s != ADBC_STATUS_OK)
                return s;
            option_value = parsed;
        }
        else if (option_value.rfind("chdb:", 0) == 0)
        {
            const std::string after = option_value.substr(5);

            /// Sentinel in the authority position: chdb://:memory: / chdb://memory.
            /// Only in-memory when no /path follows the authority. Decode first
            /// so a percent-encoded sentinel matches, same as the path position.
            if (after.rfind("//", 0) == 0)
            {
                const std::string auth_rest = percentDecode(after.substr(2));
                if (auth_rest.find('/') == std::string::npos
                    && (auth_rest == ":memory:" || auth_rest == "memory"))
                {
                    impl->path = ":memory:";
                    return ADBC_STATUS_OK;
                }
            }

            /// Sentinel in the path position: chdb: / chdb:memory / chdb::memory:.
            /// Decode first so a percent-encoded ":memory:" still matches.
            const std::string tail = percentDecode(after);
            if (tail.empty() || tail == "memory" || tail == ":memory:")
            {
                impl->path = ":memory:";
                return ADBC_STATUS_OK;
            }

            /// Otherwise a real path, parsed with the shared file: logic.
            std::string parsed;
            if (AdbcStatusCode s = parseFileLikeUri("chdb", after, parsed, error);
                s != ADBC_STATUS_OK)
                return s;
            option_value = parsed;
        }
        else if (option == "uri" && option_value.find("://") != std::string::npos)
            return notImplemented(error, "URI scheme other than file/chdb");
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
    /// Defensive: an outstanding stream must release the engine before the
    /// connection closes and stop pointing at the impl we delete below.
    {
        std::lock_guard reg(impl->stream_mutex);
        if (auto * s = impl->active_stream)
        {
            std::lock_guard guard(s->mutex);
            s->invalidated = true;
            s->last_error = kStreamInvalidatedByClose;
            s->releaseEngineStream();
            s->owner = nullptr;
            impl->active_stream = nullptr;
        }
    }
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

AdbcStatusCode chdbConnectionGetOption(
    AdbcConnection * connection, const char * key, char * value, size_t * length, AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (!key || !length)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] key/length is null");
    auto * impl = static_cast<ConnectionImpl *>(connection->private_data);
    if (!impl->conn)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");

    if (std::strcmp(key, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0)
    {
        std::string current;
        {
            std::lock_guard lock(impl->mutex);
            /// Metadata read: reject (don't silently kill) a live stream.
            if (AdbcStatusCode s = reclaimActiveStream(impl, /*owner_for_reuse=*/0, kStreamInvalidatedBySuccessor, error);
                s != ADBC_STATUS_OK)
                return s;
            AdbcStatusCode status = runScalarQuery(impl, "SELECT currentDatabase()", current, error);
            if (status != ADBC_STATUS_OK)
                return status;
        }
        /// The scalar helper hands back a CSV cell: strip the quoting.
        if (current.size() >= 2 && current.front() == '"' && current.back() == '"')
        {
            current = current.substr(1, current.size() - 2);
            size_t pos = 0;
            while ((pos = current.find("\"\"", pos)) != std::string::npos)
            {
                current.erase(pos, 1);
                ++pos;
            }
        }
        /// Standard get-option contract: report the required size (with NUL)
        /// and copy only when the caller's buffer already fits it.
        const size_t needed = current.size() + 1;
        if (value && *length >= needed)
            std::memcpy(value, current.c_str(), needed);
        *length = needed;
        return ADBC_STATUS_OK;
    }
    return setError(error, ADBC_STATUS_NOT_FOUND, "[chdb] unknown connection option '" + std::string(key) + "'");
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

    enum class InfoKind
    {
        String,
        Int64,
        Bool
    };
    struct InfoValue
    {
        uint32_t code;
        InfoKind kind;
        std::string string_value;
        int64_t int_value;
        bool bool_value;
    };
    /// The driver ships with the engine, so its version is the engine's
    /// major.minor.patch (a semver-shaped string, unlike the four-component
    /// VERSION_STRING reported as the vendor version).
    const std::string driver_version = std::to_string(VERSION_MAJOR) + "." + std::to_string(VERSION_MINOR) + "."
        + std::to_string(VERSION_PATCH);
    std::vector<InfoValue> all = {
        {ADBC_INFO_VENDOR_NAME, InfoKind::String, "ClickHouse", 0, false},
        {ADBC_INFO_VENDOR_VERSION, InfoKind::String, VERSION_STRING, 0, false},
        {ADBC_INFO_VENDOR_ARROW_VERSION, InfoKind::String, "v" ARROW_VERSION_STRING, 0, false},
        {ADBC_INFO_VENDOR_SQL, InfoKind::Bool, "", 0, true},
        {ADBC_INFO_VENDOR_SUBSTRAIT, InfoKind::Bool, "", 0, false},
        {ADBC_INFO_DRIVER_NAME, InfoKind::String, "ADBC chDB Driver", 0, false},
        {ADBC_INFO_DRIVER_VERSION, InfoKind::String, driver_version, 0, false},
        {ADBC_INFO_DRIVER_ARROW_VERSION, InfoKind::String, "v" ARROW_VERSION_STRING, 0, false},
        {ADBC_INFO_DRIVER_ADBC_VERSION, InfoKind::Int64, "", ADBC_VERSION_1_1_0, false},
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
    const int8_t bool_code = value_builder.AppendChild(bool_builder, "bool_value");
    const int8_t int64_code = value_builder.AppendChild(int64_builder, "int64_value");
    value_builder.AppendChild(bitmask_builder, "int32_bitmask");
    value_builder.AppendChild(list_builder, "string_list");
    value_builder.AppendChild(map_builder, "int32_to_int32_list_map");

    auto check = [&](const arrow::Status & status) { return status.ok(); };
    for (const auto & info : selected)
    {
        if (!check(name_builder.Append(info.code)))
            return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetInfo result");
        bool ok = false;
        switch (info.kind)
        {
            case InfoKind::String:
                ok = check(value_builder.Append(string_code)) && check(string_builder->Append(info.string_value));
                break;
            case InfoKind::Bool:
                ok = check(value_builder.Append(bool_code)) && check(bool_builder->Append(info.bool_value));
                break;
            case InfoKind::Int64:
                ok = check(value_builder.Append(int64_code)) && check(int64_builder->Append(info.int_value));
                break;
        }
        if (!ok)
            return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetInfo result");
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

/// Spec-mandated nested schema for GetObjects results. chDB has no catalog
/// concept, so everything lives under a single NULL catalog with ClickHouse
/// databases as db_schemas (matching GetTableSchema's db_schema semantics).
std::shared_ptr<arrow::DataType> getObjectsDbSchemaType()
{
    auto usage_type = arrow::struct_(
        {arrow::field("fk_catalog", arrow::utf8()),
         arrow::field("fk_db_schema", arrow::utf8()),
         arrow::field("fk_table", arrow::utf8(), /*nullable=*/false),
         arrow::field("fk_column_name", arrow::utf8(), /*nullable=*/false)});
    auto constraint_type = arrow::struct_(
        {arrow::field("constraint_name", arrow::utf8()),
         arrow::field("constraint_type", arrow::utf8(), /*nullable=*/false),
         arrow::field("constraint_column_names", arrow::list(arrow::utf8()), /*nullable=*/false),
         arrow::field("constraint_column_usage", arrow::list(usage_type))});
    auto column_type = arrow::struct_(
        {arrow::field("column_name", arrow::utf8(), /*nullable=*/false),
         arrow::field("ordinal_position", arrow::int32()),
         arrow::field("remarks", arrow::utf8()),
         arrow::field("xdbc_data_type", arrow::int16()),
         arrow::field("xdbc_type_name", arrow::utf8()),
         arrow::field("xdbc_column_size", arrow::int32()),
         arrow::field("xdbc_decimal_digits", arrow::int16()),
         arrow::field("xdbc_num_prec_radix", arrow::int16()),
         arrow::field("xdbc_nullable", arrow::int16()),
         arrow::field("xdbc_column_def", arrow::utf8()),
         arrow::field("xdbc_sql_data_type", arrow::int16()),
         arrow::field("xdbc_datetime_sub", arrow::int16()),
         arrow::field("xdbc_char_octet_length", arrow::int32()),
         arrow::field("xdbc_is_nullable", arrow::utf8()),
         arrow::field("xdbc_scope_catalog", arrow::utf8()),
         arrow::field("xdbc_scope_schema", arrow::utf8()),
         arrow::field("xdbc_scope_table", arrow::utf8()),
         arrow::field("xdbc_is_autoincrement", arrow::boolean()),
         arrow::field("xdbc_is_generatedcolumn", arrow::boolean())});
    auto table_type = arrow::struct_(
        {arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
         arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
         arrow::field("table_columns", arrow::list(column_type)),
         arrow::field("table_constraints", arrow::list(constraint_type))});
    return arrow::struct_(
        {arrow::field("db_schema_name", arrow::utf8()),
         arrow::field("db_schema_tables", arrow::list(table_type))});
}

AdbcStatusCode chdbConnectionGetObjects(
    AdbcConnection * connection,
    int depth,
    const char * catalog,
    const char * db_schema,
    const char * table_name,
    const char ** table_type,
    const char * column_name,
    ArrowArrayStream * out,
    AdbcError * error)
{
    if (!connection || !connection->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    auto * impl = static_cast<ConnectionImpl *>(connection->private_data);
    if (!impl->conn)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] connection is not initialized");
    if (!out)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] out stream is null");

    /// Requested table types ("BASE TABLE" / "VIEW"); NULL means all.
    bool want_base = true;
    bool want_view = true;
    if (table_type)
    {
        want_base = want_view = false;
        for (const char ** t = table_type; *t; ++t)
        {
            if (std::strcmp(*t, "BASE TABLE") == 0)
                want_base = true;
            else if (std::strcmp(*t, "VIEW") == 0)
                want_view = true;
        }
    }

    /// Everything lives under one NULL catalog: a non-empty catalog filter
    /// other than the match-all pattern selects nothing.
    /// Our objects carry no catalog: NULL, "" ("objects without a catalog",
    /// per spec) and the match-all pattern select them; anything else
    /// selects nothing — zero result rows. An empty db_schema filter means
    /// "objects without a database schema", which never matches here.
    const bool catalog_matches = !catalog || !*catalog || std::strcmp(catalog, "%") == 0;
    const bool schema_filter_excludes_all = db_schema && !*db_schema;

    struct ColumnInfo
    {
        std::string name;
        int32_t position;
        std::string type;
        std::string comment;
    };
    struct TableInfo
    {
        std::string name;
        std::string type;
        std::vector<ColumnInfo> columns;
    };
    /// databases in deterministic order + per-database tables
    std::vector<std::string> databases;
    std::vector<std::vector<TableInfo>> tables_per_db;

    std::lock_guard lock(impl->mutex);
    if (catalog_matches && !schema_filter_excludes_all)
    {
        std::string db_where;
        if (db_schema && *db_schema)
            db_where = " WHERE name LIKE " + quoteStringLiteral(db_schema);
        std::shared_ptr<arrow::Table> dbs;
        AdbcStatusCode status
            = queryToTable(impl, "SELECT name FROM system.databases" + db_where + " ORDER BY name", dbs, error);
        if (status != ADBC_STATUS_OK)
            return status;
        if (dbs->num_rows() > 0)
        {
            std::shared_ptr<arrow::StringArray> names;
            AdbcStatusCode col_status = getTypedColumn(dbs, "name", arrow::Type::STRING, names, error);
            if (col_status != ADBC_STATUS_OK)
                return col_status;
            for (int64_t i = 0; i < names->length(); ++i)
                databases.push_back(names->GetString(i));
        }
        tables_per_db.resize(databases.size());

        if (depth == ADBC_OBJECT_DEPTH_ALL || depth >= ADBC_OBJECT_DEPTH_TABLES)
        {
            std::string where = db_schema && *db_schema
                ? " WHERE database LIKE " + quoteStringLiteral(db_schema)
                : " WHERE 1";
            if (table_name && *table_name)
                where += " AND name LIKE " + quoteStringLiteral(table_name);
            const char * type_expr
                = "if(engine IN ('View', 'MaterializedView', 'LiveView', 'WindowView'), 'VIEW', 'BASE TABLE')";
            if (!want_base && !want_view)
                where += " AND 0";
            else if (!want_base)
                where += std::string(" AND ") + type_expr + " = 'VIEW'";
            else if (!want_view)
                where += std::string(" AND ") + type_expr + " = 'BASE TABLE'";

            std::shared_ptr<arrow::Table> tbls;
            AdbcStatusCode status2 = queryToTable(
                impl,
                std::string("SELECT database, name, ") + type_expr
                    + " AS ttype FROM system.tables" + where + " ORDER BY database, name",
                tbls,
                error);
            if (status2 != ADBC_STATUS_OK)
                return status2;

            std::shared_ptr<arrow::Table> cols;
            if (depth == ADBC_OBJECT_DEPTH_ALL)
            {
                /// system.columns has no `engine` column, so the table-type
                /// filter is applied via the table lookup below instead.
                std::string col_where = db_schema && *db_schema
                    ? " WHERE database LIKE " + quoteStringLiteral(db_schema)
                    : " WHERE 1";
                if (table_name && *table_name)
                    col_where += " AND table LIKE " + quoteStringLiteral(table_name);
                if (column_name && *column_name)
                    col_where += " AND name LIKE " + quoteStringLiteral(column_name);
                AdbcStatusCode status3 = queryToTable(
                    impl,
                    "SELECT database, table, name, toInt32(position) AS pos, type, comment FROM system.columns"
                        + col_where + " ORDER BY database, table, pos",
                    cols,
                    error);
                if (status3 != ADBC_STATUS_OK)
                    return status3;
            }

            /// Index tables by (database, name) and attach columns.
            auto findDb = [&](const std::string & name) -> int64_t
            {
                for (size_t i = 0; i < databases.size(); ++i)
                    if (databases[i] == name)
                        return static_cast<int64_t>(i);
                return -1;
            };

            if (tbls->num_rows() > 0)
            {
                std::shared_ptr<arrow::StringArray> t_db;
                std::shared_ptr<arrow::StringArray> t_name;
                std::shared_ptr<arrow::StringArray> t_type;
                AdbcStatusCode col_status = getTypedColumn(tbls, "database", arrow::Type::STRING, t_db, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(tbls, "name", arrow::Type::STRING, t_name, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(tbls, "ttype", arrow::Type::STRING, t_type, error);
                if (col_status != ADBC_STATUS_OK)
                    return col_status;
                for (int64_t i = 0; i < tbls->num_rows(); ++i)
                {
                    int64_t db_idx = findDb(t_db->GetString(i));
                    if (db_idx < 0)
                        continue;
                    tables_per_db[static_cast<size_t>(db_idx)].push_back(
                        TableInfo{t_name->GetString(i), t_type->GetString(i), {}});
                }
            }
            if (cols && cols->num_rows() > 0)
            {
                std::shared_ptr<arrow::StringArray> c_db;
                std::shared_ptr<arrow::StringArray> c_table;
                std::shared_ptr<arrow::StringArray> c_name;
                std::shared_ptr<arrow::Int32Array> c_pos;
                std::shared_ptr<arrow::StringArray> c_type;
                std::shared_ptr<arrow::StringArray> c_comment;
                AdbcStatusCode col_status = getTypedColumn(cols, "database", arrow::Type::STRING, c_db, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(cols, "table", arrow::Type::STRING, c_table, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(cols, "name", arrow::Type::STRING, c_name, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(cols, "pos", arrow::Type::INT32, c_pos, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(cols, "type", arrow::Type::STRING, c_type, error);
                if (col_status == ADBC_STATUS_OK)
                    col_status = getTypedColumn(cols, "comment", arrow::Type::STRING, c_comment, error);
                if (col_status != ADBC_STATUS_OK)
                    return col_status;
                for (int64_t i = 0; i < cols->num_rows(); ++i)
                {
                    int64_t db_idx = findDb(c_db->GetString(i));
                    if (db_idx < 0)
                        continue;
                    auto & tables = tables_per_db[static_cast<size_t>(db_idx)];
                    const std::string tname = c_table->GetString(i);
                    for (auto & t : tables)
                        if (t.name == tname)
                        {
                            t.columns.push_back(ColumnInfo{
                                c_name->GetString(i), c_pos->Value(i), c_type->GetString(i), c_comment->GetString(i)});
                            break;
                        }
                }
            }
        }
    }

    /// Assemble the nested result (single NULL-catalog row).
    auto db_schema_type = getObjectsDbSchemaType();
    std::unique_ptr<arrow::ArrayBuilder> top_builder;
    auto make_status
        = arrow::MakeBuilder(arrow::default_memory_pool(), arrow::list(db_schema_type), &top_builder);
    if (!make_status.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + make_status.ToString());

    /// Child builders resolved by field name (never by position), so the
    /// population below cannot silently misalign against the schema
    /// definition in getObjectsDbSchemaType().
    auto * schemas_lb = static_cast<arrow::ListBuilder *>(top_builder.get());
    auto * schema_sb = static_cast<arrow::StructBuilder *>(schemas_lb->value_builder());
    arrow::StringBuilder * schema_name_b = nullptr;
    arrow::ListBuilder * tables_lb = nullptr;
    AdbcStatusCode nav_status
        = getFieldBuilder(schema_sb, "db_schema_name", arrow::Type::STRING, schema_name_b, nullptr, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status = getFieldBuilder(schema_sb, "db_schema_tables", arrow::Type::LIST, tables_lb, nullptr, error);
    if (nav_status != ADBC_STATUS_OK)
        return nav_status;

    auto * table_sb = static_cast<arrow::StructBuilder *>(tables_lb->value_builder());
    arrow::StringBuilder * table_name_b = nullptr;
    arrow::StringBuilder * table_type_b = nullptr;
    arrow::ListBuilder * columns_lb = nullptr;
    arrow::ListBuilder * constraints_lb = nullptr;
    nav_status = getFieldBuilder(table_sb, "table_name", arrow::Type::STRING, table_name_b, nullptr, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status = getFieldBuilder(table_sb, "table_type", arrow::Type::STRING, table_type_b, nullptr, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status = getFieldBuilder(table_sb, "table_columns", arrow::Type::LIST, columns_lb, nullptr, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status = getFieldBuilder(table_sb, "table_constraints", arrow::Type::LIST, constraints_lb, nullptr, error);
    if (nav_status != ADBC_STATUS_OK)
        return nav_status;

    auto * column_sb = static_cast<arrow::StructBuilder *>(columns_lb->value_builder());
    arrow::StringBuilder * col_name_b = nullptr;
    arrow::Int32Builder * col_pos_b = nullptr;
    arrow::StringBuilder * col_remarks_b = nullptr;
    arrow::StringBuilder * col_type_name_b = nullptr;
    int idx_col_name = -1;
    int idx_col_pos = -1;
    int idx_col_remarks = -1;
    int idx_col_type_name = -1;
    nav_status = getFieldBuilder(column_sb, "column_name", arrow::Type::STRING, col_name_b, &idx_col_name, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status
            = getFieldBuilder(column_sb, "ordinal_position", arrow::Type::INT32, col_pos_b, &idx_col_pos, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status = getFieldBuilder(column_sb, "remarks", arrow::Type::STRING, col_remarks_b, &idx_col_remarks, error);
    if (nav_status == ADBC_STATUS_OK)
        nav_status = getFieldBuilder(
            column_sb, "xdbc_type_name", arrow::Type::STRING, col_type_name_b, &idx_col_type_name, error);
    if (nav_status != ADBC_STATUS_OK)
        return nav_status;

    bool builder_ok = true;
    auto ok = [&](const arrow::Status & s) { builder_ok = builder_ok && s.ok(); };

    if (catalog_matches && depth == ADBC_OBJECT_DEPTH_CATALOGS)
    {
        ok(schemas_lb->AppendNull());
    }
    else if (catalog_matches)
    {
        ok(schemas_lb->Append());
        for (size_t db = 0; db < databases.size(); ++db)
        {
            ok(schema_sb->Append());
            ok(schema_name_b->Append(databases[db]));
            if (depth == ADBC_OBJECT_DEPTH_DB_SCHEMAS)
            {
                ok(tables_lb->AppendNull());
                continue;
            }
            ok(tables_lb->Append());
            for (const auto & t : tables_per_db[db])
            {
                ok(table_sb->Append());
                ok(table_name_b->Append(t.name));
                ok(table_type_b->Append(t.type));
                if (depth != ADBC_OBJECT_DEPTH_ALL)
                {
                    ok(columns_lb->AppendNull());
                    ok(constraints_lb->AppendNull());
                    continue;
                }
                ok(columns_lb->Append());
                for (const auto & c : t.columns)
                {
                    ok(column_sb->Append());
                    ok(col_name_b->Append(c.name));
                    ok(col_pos_b->Append(c.position));
                    if (c.comment.empty())
                        ok(col_remarks_b->AppendNull());
                    else
                        ok(col_remarks_b->Append(c.comment));
                    ok(col_type_name_b->Append(c.type));
                    /// remaining xdbc_* fields are not provided
                    for (int f = 0; f < column_sb->num_fields(); ++f)
                        if (f != idx_col_name && f != idx_col_pos && f != idx_col_remarks
                            && f != idx_col_type_name)
                            ok(column_sb->field_builder(f)->AppendNull());
                }
                /// ClickHouse has no SQL constraints to report
                ok(constraints_lb->Append());
            }
        }
    }

    arrow::StringBuilder catalog_name_b;
    if (catalog_matches)
        ok(catalog_name_b.AppendNull());
    std::shared_ptr<arrow::Array> catalog_names;
    std::shared_ptr<arrow::Array> catalog_schemas;
    ok(catalog_name_b.Finish(&catalog_names));
    ok(schemas_lb->Finish(&catalog_schemas));
    if (!builder_ok)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to build GetObjects result");

    auto result_schema = arrow::schema(
        {arrow::field("catalog_name", arrow::utf8()),
         arrow::field("catalog_db_schemas", arrow::list(db_schema_type))});
    auto batch = arrow::RecordBatch::Make(
        result_schema, catalog_matches ? 1 : 0, {catalog_names, catalog_schemas});
    return exportBatch(result_schema, batch, out, error);
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
    /// Metadata read: reject (don't silently kill) a live stream.
    if (AdbcStatusCode s = reclaimActiveStream(impl, /*owner_for_reuse=*/0, kStreamInvalidatedBySuccessor, error);
        s != ADBC_STATUS_OK)
        return s;
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
    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    if (impl->query.empty() && impl->ingest_table.empty())
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] no query to prepare (SetSqlQuery first)");
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

/// The projection for an INSERT ... SELECT out of a registered Arrow stream.
///
/// The Arrow reader turns fixed_size_binary into FixedString(n), and inserting
/// that into a String column drops the trailing NUL bytes, because that is what
/// ClickHouse's FixedString-to-String conversion does. Concatenating keeps them.
/// Every other column, and every column when there is no fixed_size_binary,
/// passes through as "*".
///
/// CREATE ... AS SELECT needs none of this: it makes the column FixedString(n),
/// so there is no conversion to lose bytes in.
std::string streamInsertProjection(ArrowArrayStream & stream)
{
    ArrowSchema schema;
    std::memset(&schema, 0, sizeof(schema));
    if (stream.get_schema(&stream, &schema) != 0 || !schema.release)
        return "*";

    std::string projection;
    bool rewritten = false;
    for (int64_t i = 0; i < schema.n_children; ++i)
    {
        const ArrowSchema * child = schema.children[i];
        const std::string name = child->name ? child->name : "";
        const std::string format = child->format ? child->format : "";
        /// "w:<byte width>" is the Arrow format string for fixed_size_binary.
        const bool fixed_size_binary = format.size() > 2 && format[0] == 'w' && format[1] == ':';

        if (i)
            projection += ", ";
        projection += fixed_size_binary ? "concat(" + quoteIdentifier(name) + ", '')" : quoteIdentifier(name);
        rewritten = rewritten || fixed_size_binary;
    }
    schema.release(&schema);
    return rewritten ? projection : "*";
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
    const std::string insert_sql = "INSERT INTO " + qualified + " SELECT "
        + streamInsertProjection(impl->bound_stream) + " FROM arrowstream(" + reg_name + ")";

    AdbcStatusCode status = ADBC_STATUS_OK;
    if (impl->ingest_mode == ADBC_INGEST_OPTION_MODE_CREATE)
    {
        status = runUpdateQuery(impl->connection, create_sql, rows_affected, error, /*zero_rows_known_override=*/true);
    }
    else if (impl->ingest_mode == ADBC_INGEST_OPTION_MODE_REPLACE)
    {
        /// Single-statement swap: the old table stays intact if this fails.
        status = runUpdateQuery(
            impl->connection,
            "CREATE OR REPLACE TABLE " + qualified
                + " ENGINE = MergeTree() ORDER BY tuple() AS SELECT * FROM arrowstream(" + reg_name + ")",
            rows_affected,
            error,
            /*zero_rows_known_override=*/true);
    }
    else if (impl->ingest_mode == ADBC_INGEST_OPTION_MODE_APPEND)
    {
        status = runUpdateQuery(impl->connection, insert_sql, rows_affected, error);
    }
    else /// create_append
    {
        std::string exists;
        status = runScalarQuery(impl->connection, "EXISTS TABLE " + qualified, exists, error);
        if (status == ADBC_STATUS_OK && exists == "1")
        {
            /// Appending goes through a positional INSERT ... SELECT *, so the
            /// bound columns must line up with the table's. A mismatch is the
            /// spec's "table exists with an incompatible schema" case, which
            /// callers expect as ALREADY_EXISTS rather than an engine error.
            ArrowSchema bound_schema;
            std::memset(&bound_schema, 0, sizeof(bound_schema));
            if (impl->bound_stream.get_schema(&impl->bound_stream, &bound_schema) == 0 && bound_schema.release)
            {
                std::string bound_names = "[";
                for (int64_t i = 0; i < bound_schema.n_children; ++i)
                    bound_names += std::string(i ? "," : "")
                        + quoteStringLiteral(bound_schema.children[i]->name ? bound_schema.children[i]->name : "");
                bound_names += "]";
                bound_schema.release(&bound_schema);

                const std::string db_literal = impl->ingest_db_schema.empty()
                    ? std::string("currentDatabase()")
                    : quoteStringLiteral(impl->ingest_db_schema);
                std::string matches;
                status = runScalarQuery(
                    impl->connection,
                    "SELECT (SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = "
                        + db_literal + " AND table = " + quoteStringLiteral(impl->ingest_table)
                        + " ORDER BY position)) = " + bound_names,
                    matches,
                    error);
                if (status == ADBC_STATUS_OK && matches != "1")
                    status = setError(
                        error,
                        ADBC_STATUS_ALREADY_EXISTS,
                        "[chdb] table " + qualified + " already exists with an incompatible schema");
            }
        }
        if (status == ADBC_STATUS_OK)
            status = runUpdateQuery(
                impl->connection, exists == "1" ? insert_sql : create_sql, rows_affected, error,
                /*zero_rows_known_override=*/true);
    }

    chdb_arrow_unregister_table(*impl->connection->conn, reg_name.c_str());
    impl->releaseBoundStream();
    return status;
}

AdbcStatusCode wireStreamingResult(
    ConnectionImpl * connection,
    uint64_t owner_statement,
    chdb_result * stream_result,
    ArrowArrayStream * out,
    AdbcError * error);

/// executemany fast path for a plain INSERT ... VALUES (?, ...): registers all
/// bound rows as an Arrow stream and inserts them with one INSERT ... SELECT
/// (the bulk-ingestion channel), instead of one execute per row. Non-INSERT
/// or non-plain shapes fall back to the per-row path in executeBoundQuery.
AdbcStatusCode executeBoundInsertBatch(
    StatementImpl * impl, const std::string & insert_head, int64_t * rows_affected, AdbcError * error)
{
    const std::string reg_name = "adbc_ingest_" + std::to_string(ingest_name_counter.fetch_add(1));
    if (chdb_arrow_scan(
            *impl->connection->conn, reg_name.c_str(), reinterpret_cast<chdb_arrow_stream>(&impl->bound_stream))
        != CHDBSuccess)
    {
        impl->releaseBoundStream();
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to register bound Arrow stream");
    }
    AdbcStatusCode status = runUpdateQuery(
        impl->connection,
        insert_head + " SELECT " + streamInsertProjection(impl->bound_stream) + " FROM arrowstream(" + reg_name + ")",
        rows_affected,
        error);
    chdb_arrow_unregister_table(*impl->connection->conn, reg_name.c_str());
    impl->releaseBoundStream();
    return status;
}

AdbcStatusCode executeBoundQuery(
    StatementImpl * impl, ArrowArrayStream * out, int64_t * rows_affected, AdbcError * error)
{
    /// Fast path: a plain INSERT ... VALUES (?, ...) with update semantics
    /// streams all bound rows through one INSERT ... SELECT — the same
    /// engine channel as bulk ingestion — instead of one parse/execute per
    /// row. Anything else falls through to the per-row path below.
    if (!out)
    {
        std::string insert_head;
        size_t shape_arity = 0;
        if (matchPlainInsertShape(impl->query, insert_head, shape_arity))
        {
            ArrowSchema bound_schema;
            std::memset(&bound_schema, 0, sizeof(bound_schema));
            if (impl->bound_stream.get_schema(&impl->bound_stream, &bound_schema) == 0 && bound_schema.release)
            {
                const auto n_cols = static_cast<size_t>(bound_schema.n_children);
                /// The bound stream is registered as a named table, so every
                /// column needs a distinct non-empty name; otherwise fall back
                /// to the per-row path, which never materializes columns.
                std::vector<std::string> names;
                names.reserve(n_cols);
                bool names_usable = true;
                for (size_t i = 0; i < n_cols && names_usable; ++i)
                {
                    const char * cname = bound_schema.children[i]->name;
                    if (!cname || !*cname)
                        names_usable = false;
                    else
                        names.emplace_back(cname);
                }
                std::sort(names.begin(), names.end());
                names_usable = names_usable && std::adjacent_find(names.begin(), names.end()) == names.end();
                bound_schema.release(&bound_schema);
                if (n_cols == shape_arity && names_usable)
                    return executeBoundInsertBatch(impl, insert_head, rows_affected, error);
            }
        }
    }

    auto reader = arrow::ImportRecordBatchReader(&impl->bound_stream);
    impl->has_bound_stream = false;
    std::memset(&impl->bound_stream, 0, sizeof(impl->bound_stream));
    if (!reader.ok())
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] " + reader.status().ToString());

    auto table_result = reader.ValueUnsafe()->ToTable();
    if (!table_result.ok())
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] " + table_result.status().ToString());
    auto combined = table_result.ValueUnsafe()->CombineChunks();
    if (!combined.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + combined.status().ToString());
    auto table = combined.ValueUnsafe();

    const std::string & sql = impl->query;
    const auto positions = findPlaceholders(sql);
    if (static_cast<int>(positions.size()) != table->num_columns())
        return setError(
            error,
            ADBC_STATUS_INVALID_ARGUMENT,
            "[chdb] query has " + std::to_string(positions.size()) + " '?' placeholders but "
                + std::to_string(table->num_columns()) + " parameter columns are bound");
    if (table->num_rows() == 0)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] bound parameter data has no rows");
    /// Multiple bound rows with a result set: the statement runs once per
    /// row and the per-execution results are concatenated into one stream.
    const bool concat_rows = out && table->num_rows() > 1;
    std::shared_ptr<arrow::Schema> concat_schema;
    std::vector<std::shared_ptr<arrow::RecordBatch>> concat_batches;

    std::vector<std::string> ch_types(static_cast<size_t>(table->num_columns()));
    for (int col = 0; col < table->num_columns(); ++col)
    {
        AdbcStatusCode status = clickhouseTypeFor(table->field(col)->type(), ch_types[static_cast<size_t>(col)], error);
        if (status != ADBC_STATUS_OK)
            return status;
        /// A column carrying NULLs binds as Nullable with the engine's \N
        /// value, never a bare literal NULL: a literal NULL types the column
        /// as Nothing, which has no Arrow output (SELECT) and breaks a
        /// concatenated stream's schema.
        if (table->column(col)->null_count() > 0)
            ch_types[static_cast<size_t>(col)] = "Nullable(" + ch_types[static_cast<size_t>(col)] + ")";
    }

    int64_t written_total = 0;
    bool any_written = false;
    /// For INSERTs a per-row count of zero is a known result, not "not
    /// applicable" — same distinction runUpdateQuery makes.
    const bool is_insert = statementIsInsert(sql);
    for (int64_t row = 0; row < table->num_rows(); ++row)
    {
        /// Assemble the rewritten query plus name/value pairs for this row.
        std::string rewritten;
        std::vector<std::string> names;
        std::vector<std::string> values;
        size_t prev = 0;
        for (size_t p = 0; p < positions.size(); ++p)
        {
            rewritten.append(sql, prev, positions[p] - prev);
            prev = positions[p] + 1;

            auto scalar = table->column(static_cast<int>(p))->chunk(0)->GetScalar(row);
            if (!scalar.ok())
                return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + scalar.status().ToString());
            const std::string name = "__adbc_p" + std::to_string(p);
            rewritten += "{" + name + ":" + ch_types[p] + "}";
            names.push_back(name);
            values.push_back(scalar.ValueUnsafe()->is_valid ? scalarToParamString(scalar.ValueUnsafe()) : "\\N");
        }
        rewritten.append(sql, prev, sql.size() - prev);

        std::vector<const char *> name_ptrs;
        std::vector<size_t> name_lens;
        std::vector<const char *> value_ptrs;
        std::vector<size_t> value_lens;
        for (size_t p = 0; p < names.size(); ++p)
        {
            name_ptrs.push_back(names[p].c_str());
            name_lens.push_back(names[p].size());
            value_ptrs.push_back(values[p].c_str());
            value_lens.push_back(values[p].size());
        }

        if (concat_rows)
        {
            chdb_result * result = chdb_query_with_params_n(
                *impl->connection->conn,
                rewritten.c_str(),
                rewritten.size(),
                "ArrowStream",
                11,
                name_ptrs.data(),
                name_lens.data(),
                value_ptrs.data(),
                value_lens.data(),
                names.size());
            AdbcStatusCode status = ipcResultToBatches(result, concat_schema, concat_batches, error);
            if (status != ADBC_STATUS_OK)
                return status;
            continue;
        }

        if (out)
        {
            /// Parameterized SELECT: server-side binding through the
            /// streaming Arrow path, same adapter as the plain path.
            chdb_result * stream_result = chdb_stream_query_arrow_with_params_n(
                *impl->connection->conn,
                rewritten.c_str(),
                rewritten.size(),
                nullptr,
                name_ptrs.data(),
                name_lens.data(),
                value_ptrs.data(),
                value_lens.data(),
                names.size());
            if (!stream_result)
                return setError(error, ADBC_STATUS_INTERNAL, "[chdb] streaming init returned no handle");
            if (const char * err = chdb_result_error(stream_result))
            {
                std::string message(err);
                chdb_destroy_query_result(stream_result);
                if (message.find(CHDB::kErrorStreamingNotSupportedPrefix) == std::string::npos)
                    return setError(error, statusForEngineError(message), "[chdb] " + message);
                /// Statement the engine refuses to stream (rejected before
                /// executing): run it materialized over the IPC format.
                chdb_result * result = chdb_query_with_params_n(
                    *impl->connection->conn,
                    rewritten.c_str(),
                    rewritten.size(),
                    "ArrowStream",
                    11,
                    name_ptrs.data(),
                    name_lens.data(),
                    value_ptrs.data(),
                    value_lens.data(),
                    names.size());
                return exportIpcResult(result, out, error);
            }
            return wireStreamingResult(impl->connection, /*owner_statement=*/impl->id, stream_result, out, error);
        }

        chdb_result * result = chdb_query_with_params_n(
            *impl->connection->conn,
            rewritten.c_str(),
            rewritten.size(),
            "Null",
            4,
            name_ptrs.data(),
            name_lens.data(),
            value_ptrs.data(),
            value_lens.data(),
            names.size());

        int64_t row_written = -1;
        AdbcStatusCode status = consumeResult(result, &row_written, error, is_insert);
        if (status != ADBC_STATUS_OK)
            return status;
        if (row_written >= 0)
        {
            written_total += row_written;
            any_written = true;
        }
    }
    if (concat_rows)
    {
        if (!concat_schema)
            concat_schema = arrow::schema(arrow::FieldVector{});
        auto reader_result = arrow::RecordBatchReader::Make(std::move(concat_batches), concat_schema);
        if (!reader_result.ok())
            return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + reader_result.status().ToString());
        auto export_status = arrow::ExportRecordBatchReader(reader_result.ValueUnsafe(), out);
        if (!export_status.ok())
            return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + export_status.ToString());
    }
    if (rows_affected)
        *rows_affected = any_written ? written_total : -1;
    return ADBC_STATUS_OK;
}

/// Pulls the next batch out of the engine. Returns 0 and a released *out on
/// end-of-stream, per the Arrow C stream contract.
int streamingGetNextImpl(StreamingResultState & state, ArrowArray * out)
{
    std::lock_guard guard(state.mutex);
    if (state.invalidated)
        return EIO; /// last_error carries the reason
    if (state.has_pending)
    {
        *out = state.pending;
        std::memset(&state.pending, 0, sizeof(state.pending));
        state.has_pending = false;
        return 0;
    }
    if (state.exhausted)
    {
        out->release = nullptr;
        return 0;
    }

    ArrowArrayStream one_batch;
    std::memset(&one_batch, 0, sizeof(one_batch));
    if (chdb_stream_fetch_arrow(
            state.conn, state.stream_result, reinterpret_cast<chdb_arrow_stream>(&one_batch))
        != CHDBSuccess)
    {
        const char * err = chdb_result_error(state.stream_result);
        state.last_error = err ? err : "chdb_stream_fetch_arrow failed";
        return EIO;
    }
    int rc = one_batch.get_next(&one_batch, out);
    if (one_batch.release)
        one_batch.release(&one_batch);
    if (rc != 0)
    {
        state.last_error = "failed to read batch from stream";
        return rc;
    }
    if (!out->release)
        state.exhausted = true;
    return 0;
}

/// Materialized fallback for statements the engine refuses to stream (DDL):
/// a missing result header means "executed fine, no result set" — hand
/// DB-API consumers an empty zero-column stream.
AdbcStatusCode executeMaterializedSelect(
    StatementImpl * impl, ArrowArrayStream * out, int64_t * rows_affected, AdbcError * error)
{
    chdb_result * result = chdb_query_arrow_n(
        *impl->connection->conn,
        impl->query.c_str(),
        impl->query.size(),
        reinterpret_cast<chdb_arrow_stream>(out),
        nullptr);
    if (result)
    {
        const char * err = chdb_result_error(result);
        if (err && std::strcmp(err, CHDB::kErrorMissingResultHeader) == 0)
        {
            chdb_destroy_query_result(result);
            auto schema = arrow::schema(arrow::FieldVector{});
            auto batch = arrow::RecordBatch::Make(schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
            return exportBatch(schema, batch, out, error);
        }
    }
    return consumeResult(result, rows_affected, error, statementIsInsert(impl->query));
}

/// Takes ownership of a freshly initialized streaming query handle and wires
/// the adapter into the caller's ArrowArrayStream. Shared by the plain and
/// parameterized execution paths.
AdbcStatusCode wireStreamingResult(
    ConnectionImpl * connection,
    uint64_t owner_statement,
    chdb_result * stream_result,
    ArrowArrayStream * out,
    AdbcError * error)
{
    chdb_connection conn = *connection->conn;
    auto state = std::make_unique<StreamingResultState>();
    state->owner = connection;
    state->owner_statement = owner_statement;
    state->conn = conn;
    state->stream_result = stream_result;

    /// Fetch the first batch eagerly: it drives statements without a result
    /// set to completion and tells us whether there is a result schema.
    ArrowArrayStream first;
    std::memset(&first, 0, sizeof(first));
    if (chdb_stream_fetch_arrow(conn, stream_result, reinterpret_cast<chdb_arrow_stream>(&first))
        != CHDBSuccess)
    {
        const char * err = chdb_result_error(stream_result);
        const std::string message = err ? err : "first fetch failed";
        return setError(error, err ? statusForEngineError(message) : ADBC_STATUS_INTERNAL, "[chdb] " + message);
    }

    ArrowSchema schema_c;
    std::memset(&schema_c, 0, sizeof(schema_c));
    if (first.get_schema(&first, &schema_c) != 0 || !schema_c.release)
    {
        /// Healthy streams always have a schema (no-result statements are
        /// rejected at init). Fail loudly; `state` cancels the engine stream
        /// on destruction so the connection recovers.
        if (first.release)
            first.release(&first);
        return setError(
            error,
            ADBC_STATUS_INTERNAL,
            "[chdb] streaming query was accepted but produced no result schema — "
            "engine streaming state is inconsistent");
    }
    auto imported = arrow::ImportSchema(&schema_c);
    if (!imported.ok())
    {
        if (first.release)
            first.release(&first);
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + imported.status().ToString());
    }
    state->schema = imported.ValueUnsafe();

    int rc = first.get_next(&first, &state->pending);
    if (first.release)
        first.release(&first);
    if (rc != 0)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] failed to read first batch");
    if (state->pending.release)
        state->has_pending = true;
    else
        state->exhausted = true; /// empty result — schema-only stream

    out->private_data = state.release();
    out->get_schema = [](ArrowArrayStream * self, ArrowSchema * schema_out) -> int
    {
        auto * s = static_cast<StreamingResultState *>(self->private_data);
        auto status = arrow::ExportSchema(*s->schema, schema_out);
        if (!status.ok())
        {
            s->last_error = status.ToString();
            return EIO;
        }
        return 0;
    };
    out->get_next = [](ArrowArrayStream * self, ArrowArray * array_out) -> int
    {
        auto * s = static_cast<StreamingResultState *>(self->private_data);
        return streamingGetNextImpl(*s, array_out);
    };
    out->get_last_error = [](ArrowArrayStream * self) -> const char *
    {
        auto * s = static_cast<StreamingResultState *>(self->private_data);
        if (!s)
            return nullptr;
        /// Locked: the invalidation thread writes last_error concurrently.
        std::lock_guard guard(s->mutex);
        return s->last_error.empty() ? nullptr : s->last_error.c_str();
    };
    out->release = [](ArrowArrayStream * self)
    {
        delete static_cast<StreamingResultState *>(self->private_data);
        self->private_data = nullptr;
        self->release = nullptr;
    };

    /// Register as the connection's outstanding stream; exhausted
    /// (schema-only) streams have nothing left in the engine to invalidate.
    auto * registered = static_cast<StreamingResultState *>(out->private_data);
    if (!registered->exhausted)
    {
        std::lock_guard reg(connection->stream_mutex);
        connection->active_stream = registered;
    }
    else
    {
        /// Unregistered, so connection close would never clear these: the
        /// stream must not point back at a connection that may be released
        /// before it, and the completed engine handle can be dropped now.
        registered->owner = nullptr;
        chdb_destroy_query_result(registered->stream_result);
        registered->stream_result = nullptr;
    }
    return ADBC_STATUS_OK;
}

/// Executes a (non-parameterized) query through the streaming Arrow path.
AdbcStatusCode executeStreamingSelect(
    StatementImpl * impl, ArrowArrayStream * out, int64_t * rows_affected, AdbcError * error)
{
    chdb_connection conn = *impl->connection->conn;
    chdb_result * stream_result
        = chdb_stream_query_arrow_n(conn, impl->query.c_str(), impl->query.size(), nullptr);
    if (!stream_result)
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] streaming init returned no handle");
    if (const char * err = chdb_result_error(stream_result))
    {
        std::string message(err);
        chdb_destroy_query_result(stream_result);
        /// The engine rejects non-SELECT statements on the streaming path
        /// before executing them, so falling back re-executes nothing.
        if (message.find(CHDB::kErrorStreamingNotSupportedPrefix) != std::string::npos)
            return executeMaterializedSelect(impl, out, rows_affected, error);
        /// Map the engine error class (UNKNOWN_TABLE, SYNTAX_ERROR, …) to the
        /// ADBC status code, same as the non-streaming paths.
        return setError(error, statusForEngineError(message), "[chdb] " + message);
    }
    return wireStreamingResult(impl->connection, /*owner_statement=*/impl->id, stream_result, out, error);
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
    /// This statement's own prior stream is invalidated (spec-required); a
    /// live stream from a DIFFERENT statement makes this Execute fail with
    /// INVALID_STATE instead of silently killing the other reader.
    if (AdbcStatusCode s
        = reclaimActiveStream(impl->connection, /*owner_for_reuse=*/impl->id, kStreamInvalidatedBySuccessor, error);
        s != ADBC_STATUS_OK)
        return s;

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
        return executeBoundQuery(impl, out, rows_affected, error);

    if (!out)
        return runUpdateQuery(impl->connection, impl->query, rows_affected, error);

    /// INSERT produces no result set: run it as an update — keeping the real
    /// row count — and hand consumers the empty stream they expect, instead
    /// of bouncing off the streaming path's rejection.
    if (statementIsInsert(impl->query))
    {
        AdbcStatusCode status = runUpdateQuery(impl->connection, impl->query, rows_affected, error);
        if (status != ADBC_STATUS_OK)
            return status;
        auto schema = arrow::schema(arrow::FieldVector{});
        auto batch = arrow::RecordBatch::Make(schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
        return exportBatch(schema, batch, out, error);
    }

    return executeStreamingSelect(impl, out, rows_affected, error);
}

AdbcStatusCode chdbStatementExecutePartitions(
    AdbcStatement *, ArrowSchema *, AdbcPartitions *, int64_t *, AdbcError * error)
{
    return notImplemented(error, "StatementExecutePartitions");
}

AdbcStatusCode chdbStatementGetParameterSchema(
    AdbcStatement * statement, ArrowSchema * schema, AdbcError * error)
{
    if (!statement || !statement->private_data)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] statement is not allocated");
    auto * impl = static_cast<StatementImpl *>(statement->private_data);
    if (!impl->has_query)
        return setError(error, ADBC_STATUS_INVALID_STATE, "[chdb] no query set (SetSqlQuery)");
    if (!schema)
        return setError(error, ADBC_STATUS_INVALID_ARGUMENT, "[chdb] schema is null");

    /// Placeholder types are resolved from the bound Arrow data at execute
    /// time, so the parameter schema only conveys the count.
    const auto positions = findPlaceholders(impl->query);
    arrow::FieldVector fields;
    fields.reserve(positions.size());
    for (size_t i = 0; i < positions.size(); ++i)
        fields.push_back(arrow::field(std::to_string(i), arrow::null()));
    auto status = arrow::ExportSchema(*arrow::schema(fields), schema);
    if (!status.ok())
        return setError(error, ADBC_STATUS_INTERNAL, "[chdb] " + status.ToString());
    return ADBC_STATUS_OK;
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

    /// ADBC 1.1.0 additions beyond these stay zeroed: the driver manager
    /// backfills unset entries with NOT_IMPLEMENTED stubs.
    if (version == ADBC_VERSION_1_1_0)
        driver->ConnectionGetOption = chdbConnectionGetOption;

    return ADBC_STATUS_OK;
}

/// Default entrypoint name probed by driver managers when none is given, so
/// `connect(driver=<libchdb path>)` works without an explicit entrypoint.
extern "C" CHDB_EXPORT AdbcStatusCode AdbcDriverInit(int version, void * raw_driver, AdbcError * error);

extern "C" CHDB_EXPORT AdbcStatusCode AdbcDriverInit(int version, void * raw_driver, AdbcError * error)
{
    return chdb_adbc_init(version, raw_driver, error);
}
