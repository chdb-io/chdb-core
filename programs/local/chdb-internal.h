#pragma once

#include "chdb.h"
#include "QueryResult.h"

#include <memory>
#include <string>
#include <arrow/c/abi.h>

#include <Core/Names.h>

namespace DB
{
    class ChdbClient;
}

/// Connection validity check function
inline bool checkConnectionValidity(chdb_conn * connection)
{
    return connection && connection->connected;
}

namespace CHDB
{

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

/// RAII guard mirroring the Python binding's QueryParameterGuard (see LocalChdb.cpp): sets named
/// parameters on the client for the duration of one query, then unconditionally clears them.
/// This matches Python `chdb.query(..., params=...)` semantics — including for streaming, where
/// parameters only need to be present during the streaming init (the engine captures values then).
class CApiQueryParameterGuard
{
public:
    CApiQueryParameterGuard(DB::ChdbClient * client_, const DB::NameToNameMap & params);
    ~CApiQueryParameterGuard();

    CApiQueryParameterGuard(const CApiQueryParameterGuard &) = delete;
    CApiQueryParameterGuard & operator=(const CApiQueryParameterGuard &) = delete;

private:
    DB::ChdbClient * client = nullptr;
    bool applied = false;
};
}
