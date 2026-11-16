#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <filesystem>
#include <string_view>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// Points the global context at the directories of one chDB instance: the ones kept under `path`, and the ones
/// the configuration can move elsewhere. clickhouse-local (`LocalServer`) and the in-process engine behind the
/// language bindings (`EmbeddedServer`) both start through here, so a path setting honored by one is honored by
/// the other.
/// The configuration keys that setupInstancePaths and the loading of executable UDFs read. Like server settings,
/// they take effect only when the instance starts.
inline constexpr std::string_view instance_path_options[] = {
    "user_scripts_path",
    "dynamic_user_defined_executable_functions_path",
    "user_defined_executable_functions_config",
    "filesystem_caches_path",
    "top_level_domains_path",
    "format_schema_path",
    "google_protos_path",
};

void setupInstancePaths(
    ContextMutablePtr global_context,
    const Poco::Util::AbstractConfiguration & config,
    const std::filesystem::path & path,
    UInt64 max_temporary_data_on_disk_size);

}
