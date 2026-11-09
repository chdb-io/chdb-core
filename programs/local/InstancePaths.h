#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <filesystem>

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
void setupInstancePaths(
    ContextMutablePtr global_context,
    const Poco::Util::AbstractConfiguration & config,
    const std::filesystem::path & path,
    UInt64 max_temporary_data_on_disk_size);

}
