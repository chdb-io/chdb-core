#include "InstancePaths.h"

#include <Common/Exception.h>
#include <Common/TLDListsHolder.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void setupInstancePaths(
    ContextMutablePtr global_context,
    const Poco::Util::AbstractConfiguration & config,
    const fs::path & path,
    UInt64 max_temporary_data_on_disk_size)
{
    global_context->setPath(path / "");
    DatabaseCatalog::instance().fixPath(global_context->getPath());

    global_context->setTemporaryStoragePath(path / "tmp" / "", max_temporary_data_on_disk_size);
    global_context->setFlagsPath(path / "flags" / "");

    global_context->setUserFilesPath(""); /// user's files are everywhere

    global_context->setUserScriptsPath(config.getString("user_scripts_path", path / "user_scripts" / ""));
    global_context->setDynamicUserDefinedExecutableFunctionsPath(
        config.getString("dynamic_user_defined_executable_functions_path", path / "dynamic_user_defined_executable_functions" / ""));

    /// Set path for filesystem caches
    String filesystem_caches_path(config.getString("filesystem_caches_path", path / "cache" / ""));
    if (!filesystem_caches_path.empty())
        global_context->setFilesystemCachesPath(filesystem_caches_path);

    /// top_level_domains_lists
    const std::string & top_level_domains_path = config.getString("top_level_domains_path", path / "top_level_domains/");
    if (!top_level_domains_path.empty())
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", config);

    /// Without a configured directory, a relative `format_schema` resolves against the process working directory.
    if (config.has("format_schema_path"))
    {
        fs::path format_schema_path = fs::weakly_canonical(config.getString("format_schema_path"));
        if (!fs::is_directory(format_schema_path))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The format_schema_path {} is not an existing directory", format_schema_path.string());
        global_context->setFormatSchemaPath(format_schema_path / "");
    }

    if (config.has("google_protos_path"))
        global_context->setGoogleProtosPath(fs::weakly_canonical(config.getString("google_protos_path")));
}

}
