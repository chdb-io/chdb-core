#pragma once

#include "chdb.h"
#include "QueryResult.h"

#include <memory>
#include <string>
#include <arrow/c/abi.h>

#include <Core/Names.h>

namespace DB
{
    class EmbeddedSession;
}

/// Connection validity check function
inline bool checkConnectionValidity(chdb_conn * connection)
{
    return connection && connection->connected;
}

namespace CHDB
{

struct ChdbConnectionState;

/// Internal format name used by Python DataFrame and Arrow C Data output paths
/// to collect raw `DB::Chunk`s directly from the pipeline without serialization.
inline constexpr const char * kChunkCollectFormatName = "dataframe";

/// RAII active-operation guard for C ABI `chdb_conn *` / `chdb_connection` handles.
/// Looks up the handle in the live-connection registry before dereferencing it, holds
/// an active-operation count until destruction, and makes concurrent `chdb_close_conn`
/// wait cleanly instead of freeing `DB::EmbeddedSession` under an in-flight call.
class ChdbConnectionLease
{
public:
    ChdbConnectionLease() = default;
    ChdbConnectionLease(std::shared_ptr<ChdbConnectionState> state_, DB::EmbeddedSession * client_) noexcept;
    ~ChdbConnectionLease();

    ChdbConnectionLease(const ChdbConnectionLease &) = delete;
    ChdbConnectionLease & operator=(const ChdbConnectionLease &) = delete;
    ChdbConnectionLease(ChdbConnectionLease && other) noexcept;
    ChdbConnectionLease & operator=(ChdbConnectionLease && other) noexcept;

    explicit operator bool() const noexcept { return client != nullptr; }
    DB::EmbeddedSession * getClient() const noexcept { return client; }

private:
    std::shared_ptr<ChdbConnectionState> state;
    DB::EmbeddedSession * client = nullptr;
};

ChdbConnectionLease acquireConnectionLease(const chdb_conn * connection);
inline ChdbConnectionLease acquireConnectionLease(chdb_connection connection)
{
    return acquireConnectionLease(reinterpret_cast<const chdb_conn *>(connection));
}
ChdbConnectionLease acquireConnectionLeaseByClient(const void * client_ptr);

std::unique_ptr<MaterializedQueryResult> pyEntryClickHouseLocal(int argc, char ** argv);

const std::string & chdb_result_error_string(chdb_result * result);

const std::string & chdb_streaming_result_error_string(chdb_streaming_result * result);

void chdb_destroy_arrow_stream(ArrowArrayStream * arrow_stream);

void chdb_stream_result_set_arrow_uuid_as_fixed(chdb_result * result, bool enabled);
void chdb_stream_result_set_arrow_variant_as_string(chdb_result * result, bool enabled);
chdb_result * chdb_query_arrow_with_settings_n(
    chdb_connection conn,
    const char * query,
    size_t query_len,
    chdb_arrow_stream out_stream,
    const chdb_arrow_options * options,
    bool output_uuid_as_fixed_byte_array,
    bool output_variant_as_string,
    const DB::NameToNameMap & params);

/// Sentinel error texts shared by producers and consumers so fallback
/// detection can't drift with a reword.
/// Producer: chdb-arrow-output.cpp (statement yields no result header).
inline constexpr const char * kErrorMissingResultHeader = "Missing result header for Arrow output";
/// Producer: ClientBase.cpp, which keeps its literal (upstream-diff file).
inline constexpr const char * kErrorStreamingNotSupportedPrefix = "Streaming query is not supported";

/// Build a NameToNameMap from parallel C-ABI arrays of parameter names and values.
/// On duplicate names the last value wins (NameToNameMap == std::unordered_map).
/// Shared by every *_with_params C entry point (chdb.cpp, chdb-arrow-output.cpp).
DB::NameToNameMap buildParameterMap(
    const char * const * param_names,
    const size_t * param_name_lens,
    const char * const * param_values,
    const size_t * param_value_lens,
    size_t param_count);

/// Applies named parameters for one query and restores the previous thread-local binding afterwards.
/// Streaming only needs them during init.
class CApiQueryParameterGuard
{
public:
    CApiQueryParameterGuard(DB::EmbeddedSession * client, const DB::NameToNameMap & params);
    ~CApiQueryParameterGuard();

    CApiQueryParameterGuard(const CApiQueryParameterGuard &) = delete;
    CApiQueryParameterGuard & operator=(const CApiQueryParameterGuard &) = delete;

private:
    const DB::EmbeddedSession * previous_client = nullptr;
    const DB::NameToNameMap * previous_params = nullptr;
    bool applied = false;
};
}
