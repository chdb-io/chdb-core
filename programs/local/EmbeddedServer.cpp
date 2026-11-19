#include "EmbeddedServer.h"
#include "InstancePaths.h"
#include <Core/Settings.h>
#include "ChdbSettingsOverrides.h"

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
}
}

#include "ChunkCollectorOutputFormat.h"
#if USE_PYTHON
#include "TableFunctionPython.h"
#endif
#if !defined(OS_WASM)
#include "StorageArrowStream.h"
#include "TableFunctionArrowStream.h"
#endif
#include <Formats/FormatFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageFactory.h>

#include <algorithm>
#include <utility>
#include <Common/FieldVisitorToString.h>
#include <Poco/Util/MapConfiguration.h>
#include <unordered_set>
#include <fmt/ranges.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <Access/AccessControl.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/UUID.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DatabaseFilesystem.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOverlay.h>
#include <Databases/registerDatabases.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsDiskStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/SharedThreadPools.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SecretArgumentsRegistry.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/registerInterpreters.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/OwnSplitChannel.h>
#include <Parsers/registerParsers.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Storages/MergeTree/ColumnsCache.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <base/argsToConfig.h>
#include <base/getMemoryAmount.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/program_options/options_description.hpp>
#include <sys/resource.h>
#include <unistd.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Poco/SimpleFileChannel.h>
#include <Poco/String.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/getLocalConfigPath.h>
#include <Common/CurrentMetrics.h>
#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#endif
#include <Common/ErrorHandlers.h>
#include <Common/EventNotifier.h>
#include <Common/Exception.h>
#include <Common/IMemoryReleasableCache.h>
#include <Common/Macros.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/PoolId.h>
#include <Common/TLDListsHolder.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/filesystemHelpers.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/SignalHandlers.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/internal/xml_wrapper.hpp>
#endif

std::atomic<bool> chdb_embedded_server_initialized{false};
extern std::atomic<bool> g_memory_tracking_disabled;

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric MemoryTracking;
}

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_introspection_functions;
extern const SettingsBool implicit_select;
extern const SettingsLocalFSReadMethod storage_file_read_method;
}

namespace ServerSetting
{
extern const ServerSettingsUInt32 allow_feature_tier;
extern const ServerSettingsDouble cache_size_to_ram_max_ratio;
extern const ServerSettingsString columns_cache_policy;
extern const ServerSettingsDouble columns_cache_free_memory_ratio;
extern const ServerSettingsUInt64 columns_cache_history_window_ms;
extern const ServerSettingsDouble columns_cache_size_ratio;
extern const ServerSettingsDouble columns_cache_size_to_ram_ratio;
extern const ServerSettingsUInt64 compiled_expression_cache_elements_size;
extern const ServerSettingsUInt64 compiled_expression_cache_size;
extern const ServerSettingsUInt64 database_catalog_drop_table_concurrency;
extern const ServerSettingsString default_database;
extern const ServerSettingsBool enable_gpu;
extern const ServerSettingsString index_mark_cache_policy;
extern const ServerSettingsUInt64 index_mark_cache_size;
extern const ServerSettingsDouble index_mark_cache_size_ratio;
extern const ServerSettingsString index_uncompressed_cache_policy;
extern const ServerSettingsUInt64 index_uncompressed_cache_size;
extern const ServerSettingsDouble index_uncompressed_cache_size_ratio;
extern const ServerSettingsString vector_similarity_index_cache_policy;
extern const ServerSettingsUInt64 vector_similarity_index_cache_size;
extern const ServerSettingsUInt64 vector_similarity_index_cache_max_entries;
extern const ServerSettingsDouble vector_similarity_index_cache_size_ratio;
extern const ServerSettingsString text_index_tokens_cache_policy;
extern const ServerSettingsUInt64 text_index_tokens_cache_size;
extern const ServerSettingsUInt64 text_index_tokens_cache_max_entries;
extern const ServerSettingsDouble text_index_tokens_cache_size_ratio;
extern const ServerSettingsString text_index_header_cache_policy;
extern const ServerSettingsUInt64 text_index_header_cache_size;
extern const ServerSettingsUInt64 text_index_header_cache_max_entries;
extern const ServerSettingsDouble text_index_header_cache_size_ratio;
extern const ServerSettingsString text_index_postings_cache_policy;
extern const ServerSettingsUInt64 text_index_postings_cache_size;
extern const ServerSettingsUInt64 text_index_postings_cache_max_entries;
extern const ServerSettingsDouble text_index_postings_cache_size_ratio;
extern const ServerSettingsUInt64 io_thread_pool_queue_size;
extern const ServerSettingsUInt64 backups_io_thread_pool_queue_size;
extern const ServerSettingsUInt64 max_backups_io_thread_pool_free_size;
extern const ServerSettingsUInt64 max_backups_io_thread_pool_size;
extern const ServerSettingsString mark_cache_policy;
extern const ServerSettingsUInt64 mark_cache_size;
extern const ServerSettingsDouble mark_cache_size_ratio;
extern const ServerSettingsString iceberg_metadata_files_cache_policy;
extern const ServerSettingsUInt64 iceberg_metadata_files_cache_size;
extern const ServerSettingsUInt64 iceberg_metadata_files_cache_max_entries;
extern const ServerSettingsDouble iceberg_metadata_files_cache_size_ratio;
extern const ServerSettingsString parquet_metadata_cache_policy;
extern const ServerSettingsUInt64 parquet_metadata_cache_size;
extern const ServerSettingsUInt64 parquet_metadata_cache_max_entries;
extern const ServerSettingsDouble parquet_metadata_cache_size_ratio;
extern const ServerSettingsUInt64 max_active_parts_loading_thread_pool_size;
extern const ServerSettingsUInt64 max_io_thread_pool_free_size;
extern const ServerSettingsUInt64 max_io_thread_pool_size;
extern const ServerSettingsUInt64 max_outdated_parts_loading_thread_pool_size;
extern const ServerSettingsUInt64 max_parts_cleaning_thread_pool_size;
extern const ServerSettingsUInt64 max_server_memory_usage;
extern const ServerSettingsDouble max_server_memory_usage_to_ram_ratio;
extern const ServerSettingsUInt64 max_thread_pool_free_size;
extern const ServerSettingsUInt64 max_thread_pool_size;
extern const ServerSettingsUInt64 max_unexpected_parts_loading_thread_pool_size;
extern const ServerSettingsBool jemalloc_enable_background_threads;
extern const ServerSettingsUInt64 jemalloc_max_background_threads_num;
extern const ServerSettingsUInt64 jemalloc_merge_tree_arenas;
extern const ServerSettingsUInt64 mmap_cache_size;
extern const ServerSettingsBool show_addresses_in_stack_traces;
extern const ServerSettingsUInt64 thread_pool_queue_size;
extern const ServerSettingsString uncompressed_cache_policy;
extern const ServerSettingsUInt64 uncompressed_cache_size;
extern const ServerSettingsDouble uncompressed_cache_size_ratio;
extern const ServerSettingsString primary_index_cache_policy;
extern const ServerSettingsUInt64 primary_index_cache_size;
extern const ServerSettingsDouble primary_index_cache_size_ratio;
extern const ServerSettingsString statistics_cache_policy;
extern const ServerSettingsUInt64 statistics_cache_size;
extern const ServerSettingsDouble statistics_cache_size_ratio;
extern const ServerSettingsUInt64 query_cache_max_entry_size_in_bytes;
extern const ServerSettingsUInt64 query_cache_max_entry_size_in_rows;
extern const ServerSettingsString query_condition_cache_policy;
extern const ServerSettingsString unique_key_bitmap_cache_policy;
extern const ServerSettingsUInt64 query_condition_cache_size;
extern const ServerSettingsDouble query_condition_cache_size_ratio;
extern const ServerSettingsUInt64 max_prefixes_deserialization_thread_pool_size;
extern const ServerSettingsUInt64 max_prefixes_deserialization_thread_pool_free_size;
extern const ServerSettingsUInt64 prefixes_deserialization_thread_pool_thread_pool_queue_size;
extern const ServerSettingsUInt64 max_format_parsing_thread_pool_size;
extern const ServerSettingsUInt64 max_format_parsing_thread_pool_free_size;
extern const ServerSettingsUInt64 format_parsing_thread_pool_queue_size;
extern const ServerSettingsNonZeroUInt64 ai_function_max_thread_pool_size;
extern const ServerSettingsUInt64 ai_function_max_thread_pool_free_size;
extern const ServerSettingsUInt64 ai_function_thread_pool_queue_size;
extern const ServerSettingsUInt64 memory_worker_period_ms;
extern const ServerSettingsDouble memory_worker_purge_dirty_pages_threshold_ratio;
extern const ServerSettingsDouble memory_worker_purge_total_memory_threshold_ratio;
extern const ServerSettingsBool memory_worker_correct_memory_tracker;
extern const ServerSettingsUInt64 memory_worker_decay_adjustment_period_ms;
extern const ServerSettingsBool memory_worker_use_cgroup;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_LOAD_CONFIG;
extern const int FILE_ALREADY_EXISTS;
extern const int SUPPORT_IS_DISABLED;
extern const int UNKNOWN_FORMAT;
}

static void applySettingsOverridesForLocal(ContextMutablePtr context)
{
    Settings settings = context->getSettingsCopy();

    settings[Setting::allow_introspection_functions] = true;
    settings[Setting::storage_file_read_method] = LocalFSReadMethod::mmap;
    settings[Setting::implicit_select] = true;

    context->setSettings(settings);
}

EmbeddedServer::~EmbeddedServer()
{
    cleanup();
}

void EmbeddedServer::initialize(Poco::Util::Application & self)
{
    setShuttingDown(false);

    Poco::Util::Application::initialize(self);

    const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (home_path_cstr)
        home_path = home_path_cstr;

    /// Load config files if exists
    std::string config_path;
    /// Decide this before the file layer is merged in: afterwards a <config-file> element inside
    /// an auto-discovered config would be indistinguishable from one the caller passed.
    config_file_passed_explicitly = config().has("config-file");
    if (config_file_passed_explicitly)
        config_path = config().getString("config-file");
    else if (config_path.empty() && fs::exists("config.xml"))
        config_path = "config.xml";
    else if (config_path.empty())
        config_path = getLocalConfigPath(home_path).value_or("");

    if (fs::exists(config_path))
    {
        ConfigProcessor config_processor(config_path);
        ConfigProcessor::setConfigPath(fs::path(config_path).parent_path());
        auto loaded_config = config_processor.loadConfig();
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    server_settings.loadSettingsFromConfig(config());

#if USE_JEMALLOC
    /// The compiled-in malloc_conf keeps jemalloc background threads OFF
    /// (spawning them while the module is being dlopen'd crashes; see the
    /// Linux chdb_spec in contrib/jemalloc-cmake). Enable them at runtime
    /// instead, mirroring BaseDaemon for clickhouse-server: without them,
    /// dirty-page purging runs synchronously on query threads and serializes
    /// allocation-heavy aggregations on many-core machines.
    Jemalloc::setBackgroundThreads(server_settings[ServerSetting::jemalloc_enable_background_threads]);
    if (auto max_background_threads = server_settings[ServerSetting::jemalloc_max_background_threads_num])
        Jemalloc::setMaxBackgroundThreads(max_background_threads);

    /// Create the dedicated MergeTree metadata arena pool before any parts are loaded, same as
    /// clickhouse-server and clickhouse-local. Without this the `jemalloc_merge_tree_arenas`
    /// server setting is silently ignored in the embedded server.
    JemallocMergeTreeArena::initialize(server_settings[ServerSetting::jemalloc_merge_tree_arenas]);
#endif

    GlobalThreadPool::initialize(
        server_settings[ServerSetting::max_thread_pool_size],
        server_settings[ServerSetting::max_thread_pool_free_size],
        server_settings[ServerSetting::thread_pool_queue_size]);

    static std::once_flag static_thread_pools_constructed;
    std::call_once(static_thread_pools_constructed, []
    {
        /// chdb_spec: construct the static thread pools before the exit teardown below is first
        /// registered: a function-local static constructed after std::atexit() is destroyed
        /// before the handler runs, and the server it destroys still uses them. The
        /// ShutdownTablesThreadPool is otherwise first constructed when tables are shut down,
        /// which with a connection open at exit made the teardown shut them down on a destroyed
        /// pool ("The ShutdownTablesThreadPool is not initialized").
#define CONSTRUCT_STATIC_THREAD_POOL(SUFFIX, NAME, METRIC) (void)get##SUFFIX##ThreadPool();
        APPLY_FOR_STATIC_THREAD_POOLS(CONSTRUCT_STATIC_THREAD_POOL)
#undef CONSTRUCT_STATIC_THREAD_POOL

        /// The same for the singletons ContextSharedPart::shutdown() shuts down, which the start
        /// constructs later (processConfig) or only a query does. With the static library, a
        /// process that returned from main with a connection open crashed in the teardown, in
        /// NamedCollectionFactory::shutdown() on the already destroyed factory.
        (void)NamedCollectionFactory::instance();
        (void)SQLDefinedHandlersFactory::instance();
        (void)FileCacheFactory::instance();
        (void)StreamingStorageRegistry::instance();

#if USE_AZURE_BLOB_STORAGE
        /// See the explanation near the same line in Server.cpp
        GlobalThreadPool::instance().addOnDestroyCallback([] { Azure::Storage::_internal::XmlGlobalDeinitialize(); });
#endif
    });

    /// chdb_spec: register the exit teardown on every engine start, not only the first. A
    /// function-local static constructed after std::atexit() is destroyed before the handler
    /// runs, so with the first start's registration alone, an engine started later and still
    /// open at exit was torn down on statics that the earlier engines had constructed while they
    /// ran and shut down, already destroyed ("mutex lock failed: Invalid argument" on macOS, a
    /// crash at exit after a few restarts on Linux). Every start registers at this early point,
    /// where the first one always did: the deadly-signal handlers the first start installs below
    /// are then still removed before its handler runs, so a crash in that teardown ends the
    /// process at once instead of waiting minutes for a signal listener that cannot answer.
    /// Running the handler more than once is harmless.
    (void)std::atexit([]
    {
        g_memory_tracking_disabled.store(true, std::memory_order_relaxed);
        /// If a connection is still open at interpreter exit, destroy the server
        /// here, before the global pool teardown, while logging and the statics
        /// its shutdown touches are still alive. Waiting for global_instance's
        /// static destructor is too late: it interleaves with unrelated static
        /// teardown (cross-TU destruction order is unspecified) while worker
        /// threads are still live - the residual SIGSEGV window seen as the
        /// DataStore-suite subprocess crashes on v26.7.
        /// Take ownership under the lock but destroy outside it: the
        /// teardown must not run under instance_mutex, or a teardown
        /// path taking the mutex would self-deadlock and a concurrent
        /// close would block for the whole teardown.
        std::unique_ptr<EmbeddedServer> instance_to_destroy;
        {
            std::lock_guard<std::mutex> exit_lock(instance_mutex);
            client_ref_count = 0;
            instance_to_destroy = std::move(global_instance);
        }
        instance_to_destroy.reset();
        GlobalThreadPool::shutdown();
#if USE_JEMALLOC
        /// The background purge threads the start turned on, as chdb_shutdown() does: this also
        /// runs when the host unloads the library, and they must not outlive its code.
        Jemalloc::setBackgroundThreads(false);
#endif
    });

    getIOThreadPool().initialize(
        server_settings[ServerSetting::max_io_thread_pool_size],
        server_settings[ServerSetting::max_io_thread_pool_free_size],
        server_settings[ServerSetting::io_thread_pool_queue_size]);

    getBackupsIOThreadPool().initialize(
        server_settings[ServerSetting::max_backups_io_thread_pool_size],
        server_settings[ServerSetting::max_backups_io_thread_pool_free_size],
        server_settings[ServerSetting::backups_io_thread_pool_queue_size]);

    const size_t active_parts_loading_threads = server_settings[ServerSetting::max_active_parts_loading_thread_pool_size];
    getActivePartsLoadingThreadPool().initialize(
        active_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        active_parts_loading_threads);

    const size_t outdated_parts_loading_threads = server_settings[ServerSetting::max_outdated_parts_loading_thread_pool_size];
    getOutdatedPartsLoadingThreadPool().initialize(
        outdated_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        outdated_parts_loading_threads);

    getOutdatedPartsLoadingThreadPool().setMaxTurboThreads(active_parts_loading_threads);

    const size_t unexpected_parts_loading_threads = server_settings[ServerSetting::max_unexpected_parts_loading_thread_pool_size];
    getUnexpectedPartsLoadingThreadPool().initialize(
        unexpected_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        unexpected_parts_loading_threads);

    getUnexpectedPartsLoadingThreadPool().setMaxTurboThreads(active_parts_loading_threads);

    const size_t cleanup_threads = server_settings[ServerSetting::max_parts_cleaning_thread_pool_size];
    getPartsCleaningThreadPool().initialize(
        cleanup_threads,
        0, // We don't need any threads one all the parts will be deleted
        cleanup_threads);

    getDatabaseCatalogDropTablesThreadPool().initialize(
        server_settings[ServerSetting::database_catalog_drop_table_concurrency],
        0, // We don't need any threads if there are no DROP queries.
        server_settings[ServerSetting::database_catalog_drop_table_concurrency]);

    getMergeTreePrefixesDeserializationThreadPool().initialize(
        server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_size],
        server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_free_size],
        server_settings[ServerSetting::prefixes_deserialization_thread_pool_thread_pool_queue_size]);

    getFormatParsingThreadPool().initialize(
        server_settings[ServerSetting::max_format_parsing_thread_pool_size],
        server_settings[ServerSetting::max_format_parsing_thread_pool_free_size],
        server_settings[ServerSetting::format_parsing_thread_pool_queue_size]);

    /// As LocalServer does: `AIRequestExecutor` uses the pool without initializing it. (The Iceberg
    /// manifest decode pool initialized here before is gone: manifests are read on the IO pool.)
    getAIRequestThreadPool().initialize(
        server_settings[ServerSetting::ai_function_max_thread_pool_size],
        server_settings[ServerSetting::ai_function_max_thread_pool_free_size],
        server_settings[ServerSetting::ai_function_thread_pool_queue_size]);
}

static DatabasePtr createMemoryDatabaseIfNotExists(ContextPtr context, const String & database_name)
{
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>(database_name, context);
        /// Lock the UUID before attaching the database to avoid assertion failure in addUUIDMapping
        if (UUID uuid = system_database->getUUID(); uuid != UUIDHelpers::Nil)
            DatabaseCatalog::instance().addUUIDMapping(uuid);
        DatabaseCatalog::instance().attachDatabase(database_name, system_database);
    }
    return system_database;
}

static void deferDatabaseTables(IDatabase & database, std::function<void(IDatabase &)> populate)
{
    dynamic_cast<DatabaseWithOwnTablesBase &>(database).setDeferredPopulation(std::move(populate));
}

static void createMemoryDatabaseWithDeferredTables(ContextPtr context, const String & database_name, std::function<void(IDatabase &)> populate)
{
    deferDatabaseTables(*createMemoryDatabaseIfNotExists(context, database_name), std::move(populate));
}

static void deferSystemDatabaseTables(ContextPtr context, IDatabase & system_database)
{
    validateSystemUserQueryLog(context, system_database);

    deferDatabaseTables(
        system_database,
        [context](IDatabase & database) { attachSystemTablesServerExceptOne(context, database, false, false); });

    attachSystemTableOne(context, system_database);
}

namespace
{

class UserDefinedSQLObjectsMemoryStorage final : public UserDefinedSQLObjectsStorageBase
{
public:
    explicit UserDefinedSQLObjectsMemoryStorage(const ContextPtr & global_context_)
        : UserDefinedSQLObjectsStorageBase(global_context_)
    {
    }

    void loadObjects() override {}
    void reloadObjects() override {}
    void reloadObject(UserDefinedSQLObjectType, const String &) override {}

protected:
    bool storeObjectImpl(
        const ContextPtr &,
        UserDefinedSQLObjectType,
        const String &,
        ASTPtr,
        bool,
        bool,
        const Settings &) override
    {
        return true;
    }

    bool removeObjectImpl(
        const ContextPtr &,
        UserDefinedSQLObjectType,
        const String &,
        bool) override
    {
        return true;
    }
};

UUID tryReadAtomicDatabaseUUIDFromDisk(const String & name_, const DiskPtr & db_disk, const ContextPtr & context)
{
    const auto metadata_file = DatabaseCatalog::getMetadataFilePath(name_);
    if (!db_disk || !db_disk->existsFile(metadata_file))
        return UUIDHelpers::Nil;

    try
    {
        const String query = readMetadataFile(db_disk, metadata_file);
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(
            parser,
            query.data(),
            query.data() + query.size(),
            "in file " + metadata_file.string(),
            0,
            context->getSettingsRef()[Setting::max_parser_depth],
            context->getSettingsRef()[Setting::max_parser_backtracks]);
        if (const auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr)
            return create->uuid;
    }
    catch (...)
    {
    }
    return UUIDHelpers::Nil;
}

DatabasePtr createPersistentDatabaseOverlay(const String & name_, const ContextPtr & ns_context)
{
    auto overlay = std::make_shared<DatabaseOverlay>(name_, ns_context);
    auto db_disk = ns_context->getDatabaseDisk();

    UUID default_database_uuid = UUIDHelpers::Nil;
    const fs::path existing_path_symlink = fs::path(ns_context->getPath()) / DatabaseCatalog::getMetadataDirPath(name_);
    if (FS::isSymlinkNoThrow(existing_path_symlink))
    {
        auto symlink_path = FS::readSymlink(existing_path_symlink);
        if (!symlink_path.has_filename() && symlink_path.has_parent_path())
            symlink_path = symlink_path.parent_path();
        tryParse(default_database_uuid, symlink_path.filename().string());
    }
    if (default_database_uuid == UUIDHelpers::Nil)
        default_database_uuid = tryReadAtomicDatabaseUUIDFromDisk(name_, db_disk, ns_context);
    if (default_database_uuid == UUIDHelpers::Nil)
        default_database_uuid = UUIDHelpers::generateV4();

    const String default_database_metadata_path = DatabaseCatalog::getStoreDirPath(default_database_uuid);
    overlay->registerNextDatabase(
        std::make_shared<DatabaseAtomic>(name_, default_database_metadata_path, default_database_uuid, ns_context));
    overlay->registerNextDatabase(std::make_shared<DatabaseFilesystem>(
        name_,
        "",
        ns_context,
        /*is_internal_metadata_replay=*/false));
    return overlay;
}

DatabasePtr createMemoryDatabaseOverlay(const String & name_, const ContextPtr & ns_context)
{
    auto overlay = std::make_shared<DatabaseOverlay>(name_, ns_context);
    overlay->registerNextDatabase(std::make_shared<DatabaseMemory>(name_, ns_context));
    overlay->registerNextDatabase(std::make_shared<DatabaseFilesystem>(
        name_,
        "",
        ns_context,
        /*is_internal_metadata_replay=*/false));
    return overlay;
}

} // namespace

void EmbeddedServer::tryInitPath()
{
    boot_explicit_path.clear();
    if (config().has("path"))
    {
        boot_explicit_path = config().getString("path");
        Poco::trimInPlace(boot_explicit_path);

        if (boot_explicit_path.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot work with empty storage path that is explicitly specified"
                " by the --path option. Please check the program options and"
                " correct the --path.");
        }
    }

    /// Reserve a lazy scratch path name without creating any directory on disk:
    /// `:memory:` namespaces use `DatabaseMemory` and never materialize `/tmp/clickhouse-embedded-*`
    /// unless an explicit external spill or `:memory:` MergeTree table writes to the fallback disk.
    LoggerRawPtr log = &logger();
    std::filesystem::path parent_folder;
    try
    {
        parent_folder = std::filesystem::temp_directory_path();
    }
    catch (const fs::filesystem_error & e)
    {
        LOG_DEBUG(log, "Can not get temporary folder: {}", e.what());
        std::error_code cwd_ec;
        parent_folder = std::filesystem::current_path(cwd_ec);
        if (cwd_ec || parent_folder.empty())
            parent_folder = "/tmp";
    }

    std::filesystem::path default_path = parent_folder / fmt::format("clickhouse-embedded-{}", UUIDHelpers::generateV4());
    if (fs::exists(default_path))
        throw Exception(
            ErrorCodes::FILE_ALREADY_EXISTS,
            "Unsuccessful attempt to set up the working directory: {} already exists.",
            default_path.string());

    temporary_directory_to_delete = default_path;

    std::string engine_root_path = default_path.string();
    if (!boot_explicit_path.empty() && config_file_passed_explicitly && hasAnySystemLogConfigured(config()))
    {
        fs::create_directories(boot_explicit_path);
        engine_root_path = boot_explicit_path;
    }
    else
    {
        config().setString("path", engine_root_path);
    }

    /// No limit on temporary data here, unlike clickhouse-local: the engine runs inside the caller's process.
    /// `default_path` is NOT created here — zero `/tmp/clickhouse-embedded-*` directories for `:memory:`.
    setupInstancePaths(global_context, config(), engine_root_path, 0);
}


/// Shutdown cancels running merges and drops the ones not yet scheduled, so record per table what
/// layout work is being abandoned. Must run before setShuttingDown(), which silences logging.
void EmbeddedServer::logMergeTreeStateAtShutdown() const
{
    if (!global_context)
        return;

    auto log = getLogger("EmbeddedServer");
    if (!log->is(Poco::Message::PRIO_INFORMATION))
        return;

    struct RunningMerges
    {
        size_t merges = 0;
        size_t source_parts = 0;
        UInt64 source_bytes = 0;
    };
    std::map<std::pair<String, String>, RunningMerges> running;
    for (const auto & merge : global_context->getMergeList().get())
    {
        auto & entry = running[{merge.database, merge.table}];
        ++entry.merges;
        entry.source_parts += merge.num_parts;
        entry.source_bytes += merge.total_size_bytes_compressed;
    }

    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        if (database->isExternal() || database_name == DatabaseCatalog::SYSTEM_DATABASE)
            continue;

        for (auto it = database->getTablesIterator(global_context, {}, true); it->isValid(); it->next())
        {
            const auto * data = dynamic_cast<const MergeTreeData *>(it->table().get());
            if (!data)
                continue;

            const auto parts = data->getDataPartsVectorForInternalUsage();
            std::set<String> partitions;
            for (const auto & part : parts)
                partitions.insert(part->info.getPartitionId());

            const auto merges_it = running.find({database_name, it->name()});
            const RunningMerges merges = merges_it == running.end() ? RunningMerges{} : merges_it->second;
            if (parts.size() <= partitions.size() && merges.merges == 0)
                continue;

            LOG_INFO(
                log,
                "Shutting down {}.{} with {} active parts in {} partitions ({}), {} outdated parts, "
                "cancelling {} running merges over {} parts ({})",
                backQuoteIfNeed(database_name), backQuoteIfNeed(it->name()),
                parts.size(), partitions.size(), ReadableSize(data->getTotalActiveSizeInBytes()),
                data->getOutdatedPartsCount(),
                merges.merges, merges.source_parts, ReadableSize(merges.source_bytes));
        }
    }
}

void EmbeddedServer::cleanup()
{
    bool context_cleaned = !global_context;
    try
    {
        logMergeTreeStateAtShutdown();
    }
    catch (...) {}

    /// Mark that we're shutting down to prevent logging operations from
    /// crashing when Poco::Logger's internal data structures are destroyed
    /// (which can happen during Python interpreter exit).
    setShuttingDown();

    /// Clear JIT cache BEFORE shutting down context to avoid use-after-free.
#if USE_EMBEDDED_COMPILER
    try
    {
        if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
            cache->clear();
    }
    catch (const std::exception & e)
    {
        std::cerr << "Exception clearing JIT cache: " << e.what() << std::endl;
    }
    catch (...) {}
#endif

    try
    {
        EventNotifier::shutdown();
    }
    catch (...) {}

    /// The columns cache goes away with the context; stop resizing it, and stop the
    /// MemoryWorker before shutting down Context/SharedContextHolder so its background
    /// thread never races cache or tracker teardown.
    setMemoryReleasableCache(nullptr);
    if (memory_worker)
    {
        memory_worker->setReleasableCache(nullptr);
        memory_worker.reset();
    }

    try
    {
        if (global_context)
        {
            global_context->shutdown();
            /// On normal last-connection close every Session is already gone so
            /// global_context has a single owner here and ContextSharedPart can be
            /// destroyed cleanly. At process exit with an unclosed C/C++ connection,
            /// its Session still holds a ContextPtr referencing shared_context; release
            /// ownership instead of freeing ContextSharedPart beneath late destructors.
            const bool sole_owner = (global_context.use_count() == 1);
            global_context.reset();
            if (sole_owner)
                shared_context.reset();
            else
                (void)shared_context.release();
            context_cleaned = true;
        }
        else
        {
            shared_context.reset();
        }
    }
    catch (const std::exception & e)
    {
        /// If Context/DatabaseCatalog shutdown threw, keep ContextSharedPart alive
        /// rather than freeing it out from under a still-live DatabaseCatalog.
        (void)shared_context.release();
        /// chdb_spec: A failed Context shutdown can leave the catalog alive.
        /// Never hide it as a benign mutex error before the pool teardown.
        /// Use raw stderr: the C++ logger/iostream statics might be gone.
        static constexpr char prefix[] = "Exception in global_context->shutdown(): ";
        (void)::write(STDERR_FILENO, prefix, sizeof(prefix) - 1);
        (void)::write(STDERR_FILENO, e.what(), std::strlen(e.what()));
        (void)::write(STDERR_FILENO, "\n", 1);
        /// chdb_spec: end embedded shutdown failure reporting.
    }
    catch (...)
    {
        (void)shared_context.release();
        static constexpr char message[] = "Unknown exception in global_context->shutdown()\n";
        (void)::write(STDERR_FILENO, message, sizeof(message) - 1);
    }
    last_cleanup_succeeded.store(context_cleaned, std::memory_order_release);

    // Delete the temporary directory if it was lazily materialized.
    try
    {
        if (temporary_directory_to_delete)
        {
            std::error_code ec;
            if (fs::exists(*temporary_directory_to_delete, ec))
                fs::remove_all(*temporary_directory_to_delete, ec);
            temporary_directory_to_delete.reset();
        }
    }
    catch (...) {}
}

static ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}}; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

/// The built-in users XML is a compile-time constant, so parse it once per process rather
/// than once per engine start. chdb starts and stops the engine repeatedly inside one
/// process, and each parse left its Poco name pool behind -- 40,736 bytes per start, the
/// largest single per-start leak LeakSanitizer reports. Sharing one configuration is safe:
/// Context::setUsersConfig() only stores the pointer and hands a const reference to
/// AccessControl, and nothing outside Context reads it back.
static ConfigurationPtr getDefaultUsersConfiguration(const char * xml_data)
{
    static ConfigurationPtr default_users_config = getConfigurationFromXMLString(xml_data);
    return default_users_config;
}


void EmbeddedServer::setupUsers()
{
    static const char * minimal_default_user_xml = "<clickhouse>"
                                                   "    <profiles>"
                                                   "        <default></default>"
                                                   "    </profiles>"
                                                   "    <users>"
                                                   "        <default>"
                                                   "            <password></password>"
                                                   "            <networks>"
                                                   "                <ip>::/0</ip>"
                                                   "            </networks>"
                                                   "            <profile>default</profile>"
                                                   "            <quota>default</quota>"
                                                   "            <named_collection_control>1</named_collection_control>"
                                                   "        </default>"
                                                   "    </users>"
                                                   "    <quotas>"
                                                   "        <default></default>"
                                                   "    </quotas>"
                                                   "</clickhouse>";

    ConfigurationPtr users_config;
    auto & access_control = global_context->getAccessControl();
    access_control.setNoPasswordAllowed(config().getBool("allow_no_password", true));
    access_control.setPlaintextPasswordAllowed(config().getBool("allow_plaintext_password", true));

    /// Before the users config is loaded: its settings profiles may set custom settings.
    /// Mirrors LocalServer::setupUsers (ClickHouse 6c99880936b7).
    if (config().has("custom_settings_prefixes"))
        access_control.setCustomSettingsPrefixes(config().getString("custom_settings_prefixes"));

    if (config().has("config-file") || fs::exists("config.xml"))
    {
        String config_path = config().getString("config-file", "");
        bool has_user_directories = config().has("user_directories");
        const auto config_dir = fs::path{config_path}.remove_filename().string();
        String users_config_path = config().getString("users_config", "");

        if (users_config_path.empty() && has_user_directories)
        {
            users_config_path = config().getString("user_directories.users_xml.path");
            if (fs::path(users_config_path).is_relative() && fs::exists(fs::path(config_dir) / users_config_path))
                users_config_path = fs::path(config_dir) / users_config_path;
        }

        if (users_config_path.empty())
            users_config = getDefaultUsersConfiguration(minimal_default_user_xml);
        else
        {
            ConfigProcessor config_processor(users_config_path);
            const auto loaded_config = config_processor.loadConfig();
            users_config = loaded_config.configuration;
        }
    }
    else
        users_config = getDefaultUsersConfiguration(minimal_default_user_xml);
    if (users_config)
    {
        global_context->setUsersConfig(users_config);
        // NamedCollectionUtils::loadIfNot();
    }
    else
        throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Can't load config for users");
}

int EmbeddedServer::main(const std::vector<std::string> & /*args*/)
try
{
    StackTrace::setShowAddresses(server_settings[ServerSetting::show_addresses_in_stack_traces]);
    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    /// Re-install ClickHouse signal handlers (SA_SIGINFO) after Poco::Application::initialize()
    /// which overwrites them with its own non-SA_SIGINFO handlers.
    HandledSignals::instance().setupCommonDeadlySignalHandlers();

    /// Try to increase limit on number of open files.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur < rlim.rlim_max)
        {
            rlim.rlim_cur = config().getUInt("max_open_files", static_cast<unsigned>(rlim.rlim_max));
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                std::cerr << fmt::format(
                    "Cannot set max number of file descriptors to {}. Try to specify max_open_files according to your system limits. "
                    "error: {}",
                    rlim.rlim_cur,
                    errnoToString())
                          << '\n';
        }
    }

    std::call_once(
        global_register_once_flag,
        []()
        {
            chdb_embedded_server_initialized.store(true, std::memory_order_relaxed);

            registerInterpreters();
            registerParsers();
            /// Don't initialize DateLUT
            registerFunctions();
            registerAggregateFunctions();

            registerTableFunctions();
            auto & table_function_factory = TableFunctionFactory::instance();
#if USE_PYTHON
            registerTableFunctionPython(table_function_factory);
#endif
#if !defined(OS_WASM)
            /// Also in Python builds: the ADBC driver's ingest path uses it.
            registerTableFunctionArrowStream(table_function_factory);
#endif

            registerDatabases();
            registerStorages();
            CHDB::registerDataFrameOutputFormat();
#if !defined(OS_WASM)
            auto & storage_factory = StorageFactory::instance();
            registerStorageArrowStream(storage_factory);
#endif

            registerDictionaries();
            /// Without a finder, formatting a query that may carry secrets (an s3() argument, a
            /// dictionary SOURCE, ...) throws LOGICAL_ERROR, as upstream LocalServer installs it too.
            setSecretArgumentsFinder(&SecretArgumentsRegistry::instance());
            registerDisks(/* global_skip_access_check= */ true);
            registerFormats();
            QueryPlanStepRegistry::registerPlanSteps();
        });

    processConfig();
    /// try to load user defined executable functions, throw on error and die
    try
    {
        global_context->loadOrReloadUserDefinedExecutableFunctions(config());
    }
    catch (...)
    {
        tryLogCurrentException(&logger(), "Caught exception while loading user defined executable functions.");
        throw;
    }

#if USE_FUZZING_MODE
    runLibFuzzer();
#endif

    return Application::EXIT_OK;
}
catch (DB::Exception & e)
{
    bool need_print_stack_trace = config().getBool("stacktrace", false);
    auto message = getExceptionMessageForLogging(e, need_print_stack_trace, true);
    std::cerr << message << std::endl;
    error_message_oss << message << '\n';
    auto code = DB::getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : 1;
}
catch (...)
{
    error_message_oss << DB::getCurrentExceptionMessage(true) << '\n';
    auto code = DB::getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : 1;
}

void EmbeddedServer::processConfig()
{

    auto logging
        = (config().has("logger.console") || config().has("logger.level")
           || config().has("log-level") || config().has("logger.log"));

    /// An explicit log-level wins, then the <logger><level> of a config file, which used to be
    /// overwritten with "trace".
    auto level = config().getString(
        "log-level", config().getString("logger.level", config().getString("send_logs_level", "trace")));
    config().setString("logger", "logger");
    config().setString("logger.level", logging ? level : "fatal");
    buildLoggers(config(), logger(), "clickhouse-embedded");

#if defined(OS_LINUX)
    /// Not in initialize(): buildLoggers() replaces the channel of every existing logger without
    /// synchronization, so a MemoryWorker thread already logging through the old channel could
    /// use it after it is freed. Its constructor also logs, which must respect the configured level.
    MemoryWorkerConfig memory_worker_config{
        .rss_update_period_ms = server_settings[ServerSetting::memory_worker_period_ms],
        .purge_dirty_pages_threshold_ratio = server_settings[ServerSetting::memory_worker_purge_dirty_pages_threshold_ratio],
        .purge_total_memory_threshold_ratio = server_settings[ServerSetting::memory_worker_purge_total_memory_threshold_ratio],
        .correct_tracker = server_settings[ServerSetting::memory_worker_correct_memory_tracker],
        .decay_adjustment_period_ms = server_settings[ServerSetting::memory_worker_decay_adjustment_period_ms],
        .use_cgroup = server_settings[ServerSetting::memory_worker_use_cgroup],
    };
    memory_worker = std::make_unique<MemoryWorker>(memory_worker_config, nullptr);
    memory_worker->start();
#endif

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

    tryInitPath();

    LoggerRawPtr log = &logger();

    /// Maybe useless
    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    /// Sets external authenticators config (LDAP, Kerberos).
    global_context->setExternalAuthenticatorsConfig(config());

    setupUsers();

    /// Limit on total number of concurrently executing queries.
    /// There is no need for concurrent queries, override max_concurrent_queries.
    global_context->getProcessList().setMaxSize(0);

    size_t max_server_memory_usage = server_settings[ServerSetting::max_server_memory_usage];
    const double max_server_memory_usage_to_ram_ratio = server_settings[ServerSetting::max_server_memory_usage_to_ram_ratio];
    const size_t physical_server_memory = getMemoryAmount();
    const size_t default_max_server_memory_usage = static_cast<size_t>(static_cast<double>(physical_server_memory) * max_server_memory_usage_to_ram_ratio);

    if (max_server_memory_usage == 0)
    {
        max_server_memory_usage = default_max_server_memory_usage;
        LOG_INFO(
            log,
            "Changed setting 'max_server_memory_usage' to {}"
            " ({} available memory * {:.2f} max_server_memory_usage_to_ram_ratio)",
            formatReadableSizeWithBinarySuffix(max_server_memory_usage),
            formatReadableSizeWithBinarySuffix(physical_server_memory),
            max_server_memory_usage_to_ram_ratio);
    }
    else if (max_server_memory_usage > default_max_server_memory_usage)
    {
        max_server_memory_usage = default_max_server_memory_usage;
        LOG_INFO(
            log,
            "Lowered setting 'max_server_memory_usage' to {}"
            " because the system has little few memory. The new value was"
            " calculated as {} available memory * {:.2f} max_server_memory_usage_to_ram_ratio",
            formatReadableSizeWithBinarySuffix(max_server_memory_usage),
            formatReadableSizeWithBinarySuffix(physical_server_memory),
            max_server_memory_usage_to_ram_ratio);
    }

    total_memory_tracker.setHardLimit(max_server_memory_usage);
    total_memory_tracker.setDescription("(total)");
    total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);

    const double cache_size_to_ram_max_ratio = server_settings[ServerSetting::cache_size_to_ram_max_ratio];
    const size_t max_cache_size = static_cast<size_t>(static_cast<double>(physical_server_memory) * cache_size_to_ram_max_ratio);

    String uncompressed_cache_policy = server_settings[ServerSetting::uncompressed_cache_policy];
    size_t uncompressed_cache_size = server_settings[ServerSetting::uncompressed_cache_size];
    double uncompressed_cache_size_ratio = server_settings[ServerSetting::uncompressed_cache_size_ratio];
    if (uncompressed_cache_size > max_cache_size)
    {
        uncompressed_cache_size = max_cache_size;
        LOG_DEBUG(
            log,
            "Lowered uncompressed cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setUncompressedCache(uncompressed_cache_policy, uncompressed_cache_size, uncompressed_cache_size_ratio);

    if (server_settings[ServerSetting::enable_gpu])
    {
#if USE_GPU
        global_context->setGPU(server_settings);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server setting `enable_gpu` is set, but the server is built without GPU support");
#endif
    }

    String mark_cache_policy = server_settings[ServerSetting::mark_cache_policy];
    size_t mark_cache_size = server_settings[ServerSetting::mark_cache_size];
    double mark_cache_size_ratio = server_settings[ServerSetting::mark_cache_size_ratio];
    if (!mark_cache_size)
        LOG_ERROR(log, "Too low mark cache size will lead to severe performance degradation.");
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_DEBUG(
            log, "Lowered mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_policy, mark_cache_size, mark_cache_size_ratio);

    /// The columns cache is only filled by queries with `use_columns_cache` enabled; set it up as
    /// clickhouse-local does, or that setting is silently ignored.
    String columns_cache_policy = server_settings[ServerSetting::columns_cache_policy];
    /// Unless configured explicitly, the columns cache is sized relative to the memory of the server.
    size_t columns_cache_size = config().getUInt64("columns_cache_size",
        getDefaultColumnsCacheSize(physical_server_memory, server_settings[ServerSetting::columns_cache_size_to_ram_ratio]));
    double columns_cache_size_ratio = server_settings[ServerSetting::columns_cache_size_ratio];
    if (columns_cache_size > max_cache_size)
    {
        columns_cache_size = max_cache_size;
        LOG_DEBUG(log, "Lowered columns cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(columns_cache_size));
    }
    global_context->setColumnsCache(columns_cache_policy, columns_cache_size, columns_cache_size_ratio);
    /// The cache counts against `max_server_memory_usage`, so it has to be able to give that memory back.
    /// Unregistered in cleanup(), before the context that owns the cache goes away.
    if (auto columns_cache = global_context->getColumnsCache())
    {
        columns_cache->setAutoResizeSettings(
            server_settings[ServerSetting::columns_cache_free_memory_ratio],
            server_settings[ServerSetting::columns_cache_history_window_ms]);
        setMemoryReleasableCache(columns_cache.get());
        if (memory_worker)
            memory_worker->setReleasableCache(columns_cache);
    }

    String index_uncompressed_cache_policy = server_settings[ServerSetting::index_uncompressed_cache_policy];
    size_t index_uncompressed_cache_size = server_settings[ServerSetting::index_uncompressed_cache_size];
    double index_uncompressed_cache_size_ratio = server_settings[ServerSetting::index_uncompressed_cache_size_ratio];
    if (index_uncompressed_cache_size > max_cache_size)
    {
        index_uncompressed_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered index uncompressed cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(index_uncompressed_cache_size));
    }
    global_context->setIndexUncompressedCache(
        index_uncompressed_cache_policy, index_uncompressed_cache_size, index_uncompressed_cache_size_ratio);

    String index_mark_cache_policy = server_settings[ServerSetting::index_mark_cache_policy];
    size_t index_mark_cache_size = server_settings[ServerSetting::index_mark_cache_size];
    double index_mark_cache_size_ratio = server_settings[ServerSetting::index_mark_cache_size_ratio];
    if (index_mark_cache_size > max_cache_size)
    {
        index_mark_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered index mark cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(index_mark_cache_size));
    }
    global_context->setIndexMarkCache(index_mark_cache_policy, index_mark_cache_size, index_mark_cache_size_ratio);

    String primary_index_cache_policy = server_settings[ServerSetting::primary_index_cache_policy];
    size_t primary_index_cache_size = server_settings[ServerSetting::primary_index_cache_size];
    double primary_index_cache_size_ratio = server_settings[ServerSetting::primary_index_cache_size_ratio];
    if (primary_index_cache_size > max_cache_size)
    {
        primary_index_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered primary index cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(primary_index_cache_size));
    }
    global_context->setPrimaryIndexCache(primary_index_cache_policy, primary_index_cache_size, primary_index_cache_size_ratio);

    /// MergeTree reads column statistics through this cache, and system.server_settings
    /// dereferences it, so it has to exist as it does in clickhouse-local.
    String statistics_cache_policy = server_settings[ServerSetting::statistics_cache_policy];
    size_t statistics_cache_size = server_settings[ServerSetting::statistics_cache_size];
    double statistics_cache_size_ratio = server_settings[ServerSetting::statistics_cache_size_ratio];
    if (statistics_cache_size > max_cache_size)
    {
        statistics_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered statistics cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(statistics_cache_size));
    }
    global_context->setStatisticsCache(statistics_cache_policy, statistics_cache_size, statistics_cache_size_ratio);

    String vector_similarity_index_cache_policy = server_settings[ServerSetting::vector_similarity_index_cache_policy];
    size_t vector_similarity_index_cache_size = server_settings[ServerSetting::vector_similarity_index_cache_size];
    size_t vector_similarity_index_cache_max_count = server_settings[ServerSetting::vector_similarity_index_cache_max_entries];
    double vector_similarity_index_cache_size_ratio = server_settings[ServerSetting::vector_similarity_index_cache_size_ratio];
    if (vector_similarity_index_cache_size > max_cache_size)
    {
        vector_similarity_index_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered vector similarity index cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(vector_similarity_index_cache_size));
    }
    global_context->setVectorSimilarityIndexCache(
        vector_similarity_index_cache_policy,
        vector_similarity_index_cache_size,
        vector_similarity_index_cache_max_count,
        vector_similarity_index_cache_size_ratio);

    String text_index_tokens_cache_policy = server_settings[ServerSetting::text_index_tokens_cache_policy];
    size_t text_index_tokens_cache_size = server_settings[ServerSetting::text_index_tokens_cache_size];
    size_t text_index_tokens_cache_max_count = server_settings[ServerSetting::text_index_tokens_cache_max_entries];
    double text_index_tokens_cache_size_ratio = server_settings[ServerSetting::text_index_tokens_cache_size_ratio];
    if (text_index_tokens_cache_size > max_cache_size)
    {
        text_index_tokens_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered text index tokens cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(text_index_tokens_cache_size));
    }
    global_context->setTextIndexTokensCache(text_index_tokens_cache_policy, text_index_tokens_cache_size, text_index_tokens_cache_max_count, text_index_tokens_cache_size_ratio);

    String text_index_header_cache_policy = server_settings[ServerSetting::text_index_header_cache_policy];
    size_t text_index_header_cache_size = server_settings[ServerSetting::text_index_header_cache_size];
    size_t text_index_header_cache_max_count = server_settings[ServerSetting::text_index_header_cache_max_entries];
    double text_index_header_cache_size_ratio = server_settings[ServerSetting::text_index_header_cache_size_ratio];
    if (text_index_header_cache_size > max_cache_size)
    {
        text_index_header_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered text index header cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(text_index_header_cache_size));
    }
    global_context->setTextIndexHeaderCache(text_index_header_cache_policy, text_index_header_cache_size, text_index_header_cache_max_count, text_index_header_cache_size_ratio);

    String text_index_postings_cache_policy = server_settings[ServerSetting::text_index_postings_cache_policy];
    size_t text_index_postings_cache_size = server_settings[ServerSetting::text_index_postings_cache_size];
    size_t text_index_postings_cache_max_count = server_settings[ServerSetting::text_index_postings_cache_max_entries];
    double text_index_postings_cache_size_ratio = server_settings[ServerSetting::text_index_postings_cache_size_ratio];
    if (text_index_postings_cache_size > max_cache_size)
    {
        text_index_postings_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered text index posting list cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(text_index_postings_cache_size));
    }
    global_context->setTextIndexPostingsCache(text_index_postings_cache_policy, text_index_postings_cache_size, text_index_postings_cache_max_count, text_index_postings_cache_size_ratio);

    size_t mmap_cache_size = server_settings[ServerSetting::mmap_cache_size];
    if (mmap_cache_size > max_cache_size)
    {
        mmap_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered mmap file cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(mmap_cache_size));
    }
    global_context->setMMappedFileCache(mmap_cache_size);

#if USE_AVRO
    String iceberg_metadata_files_cache_policy = server_settings[ServerSetting::iceberg_metadata_files_cache_policy];
    size_t iceberg_metadata_files_cache_size = server_settings[ServerSetting::iceberg_metadata_files_cache_size];
    size_t iceberg_metadata_files_cache_max_entries = server_settings[ServerSetting::iceberg_metadata_files_cache_max_entries];
    double iceberg_metadata_files_cache_size_ratio = server_settings[ServerSetting::iceberg_metadata_files_cache_size_ratio];
    if (iceberg_metadata_files_cache_size > max_cache_size)
    {
        iceberg_metadata_files_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered Iceberg metadata cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(iceberg_metadata_files_cache_size));
    }
    global_context->setIcebergMetadataFilesCache(
        iceberg_metadata_files_cache_policy,
        iceberg_metadata_files_cache_size,
        iceberg_metadata_files_cache_max_entries,
        iceberg_metadata_files_cache_size_ratio);
#endif

#if USE_PARQUET
    /// Initialize Parquet metadata cache (required by input_format_parquet_use_native_reader_v3
    /// which defaults to true in ClickHouse 26.3+; getParquetMetadataCache() throws LOGICAL_ERROR
    /// if this is not initialized before any parquet-over-S3/deltaLake query is executed).
    String parquet_metadata_cache_policy = server_settings[ServerSetting::parquet_metadata_cache_policy];
    size_t parquet_metadata_cache_size = server_settings[ServerSetting::parquet_metadata_cache_size];
    size_t parquet_metadata_cache_max_entries = server_settings[ServerSetting::parquet_metadata_cache_max_entries];
    double parquet_metadata_cache_size_ratio = server_settings[ServerSetting::parquet_metadata_cache_size_ratio];
    if (parquet_metadata_cache_size > max_cache_size)
    {
        parquet_metadata_cache_size = max_cache_size;
        LOG_INFO(
            log,
            "Lowered Parquet metadata cache size to {} because the system has limited RAM",
            formatReadableSizeWithBinarySuffix(parquet_metadata_cache_size));
    }
    global_context->setParquetMetadataCache(parquet_metadata_cache_policy, parquet_metadata_cache_size, parquet_metadata_cache_max_entries, parquet_metadata_cache_size_ratio);
#endif

    String query_condition_cache_policy = server_settings[ServerSetting::query_condition_cache_policy];
    size_t query_condition_cache_size = server_settings[ServerSetting::query_condition_cache_size];
    double query_condition_cache_size_ratio = server_settings[ServerSetting::query_condition_cache_size_ratio];
    query_condition_cache_size = std::min(query_condition_cache_size, max_cache_size);
    global_context->setQueryConditionCache(query_condition_cache_policy, query_condition_cache_size, query_condition_cache_size_ratio);

    /// Initialize a query result cache which stores nothing in memory. The maximum entry sizes are configured as in the server: they
    /// apply to the query result cache on disk as well, as in clickhouse-local.
    global_context->setQueryResultCache(
        0, 0, server_settings[ServerSetting::query_cache_max_entry_size_in_bytes], server_settings[ServerSetting::query_cache_max_entry_size_in_rows]);

    /// Initialize a dummy UNIQUE KEY delete-bitmap cache.
    global_context->setDeleteBitmapCache(server_settings[ServerSetting::unique_key_bitmap_cache_policy], 0, 0);

    /// Initialize a dummy encryption header cache (0 size disables it; still needed because
    /// system.server_settings dereferences its getter without a null check).
    global_context->setEncryptionHeaderCache(DEFAULT_ENCRYPTION_HEADER_CACHE_POLICY, 0, 0);

#if USE_AVRO
    /// Initialize a dummy Paimon metadata files cache.
    global_context->setPaimonMetadataFilesCache(DEFAULT_PAIMON_METADATA_CACHE_POLICY, 0, 0, 0);
#endif

    /// Initialize allowed tiers
    global_context->getAccessControl().setAllowTierSettings(server_settings[ServerSetting::allow_feature_tier]);

#if USE_EMBEDDED_COMPILER
    size_t compiled_expression_cache_max_size_in_bytes = server_settings[ServerSetting::compiled_expression_cache_size];
    size_t compiled_expression_cache_max_elements = server_settings[ServerSetting::compiled_expression_cache_elements_size];
    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_max_size_in_bytes, compiled_expression_cache_max_elements);
#endif

    NamedCollectionFactory::instance().loadIfNot();
    FileCacheFactory::instance().loadDefaultCaches(config(), global_context);
    applySettingsOverridesForLocal(global_context);
    CHDB::applySettingsOverridesForChdb(global_context);
    applyCmdOptions(global_context);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());
    /// We load temporary database first, because projections need it.
    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    std::string server_default_database = server_settings[ServerSetting::default_database];
    if (server_default_database.empty())
        server_default_database = DatabaseCatalog::DEFAULT_DATABASE;

    /// Attach a zero-disk internal `DatabaseMemory` placeholder on the un-namespaced global catalog
    /// for engine-internal tasks; every user `chdb_database` / `chdb_connection` mounts its own
    /// isolated `DatabaseMemory` or path-backed `DatabaseAtomic` namespace via `openDatabase()`.
    createMemoryDatabaseIfNotExists(global_context, server_default_database);
    global_context->setCurrentDatabase(server_default_database);

    if (!config().has("no-system-tables"))
    {
        deferSystemDatabaseTables(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE));
        createMemoryDatabaseWithDeferredTables(global_context, DatabaseCatalog::INFORMATION_SCHEMA,
            [context = global_context](IDatabase & database) { attachInformationSchema(context, database); });
        createMemoryDatabaseWithDeferredTables(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE,
            [context = global_context](IDatabase & database) { attachInformationSchema(context, database); });
    }

    if (!config().has("only-system-tables"))
    {
        DatabaseCatalog::instance().createBackgroundTasks();
        DatabaseCatalog::instance().startupBackgroundTasks();
    }

    setupSystemLogs();
}

void EmbeddedServer::setupSystemLogs()
{
    /// clickhouse-local initializes system logs only when the config asks for them
    /// (LocalServer::processConfig). EmbeddedServer never initialized them at all, so a
    /// <query_log> section was accepted and silently ignored on the embedded path: the table
    /// never appeared and SYSTEM FLUSH LOGS reported success without doing anything.
    ///
    /// Keep LocalServer's conditions and add one more: only a config the caller passed explicitly
    /// arms system logs. EmbeddedServer also picks up ./config.xml, ~/.clickhouse-local/config.*
    /// and /etc/clickhouse-local/config.*, and spawning a background flush thread per log because
    /// a config file happens to sit in the host process' working directory is not a reasonable
    /// thing for an embedded engine to do.
    if (config().has("no-system-tables") || config().has("only-system-tables"))
        return;

    if (!config_file_passed_explicitly || !hasAnySystemLogConfigured(config()))
        return;

    removeOrphanedSystemLogData(global_context, config());

    global_context->initializeSystemLogs();
}

void EmbeddedServer::applyCmdOptions(ContextMutablePtr context)
{
    context->setDefaultFormat(
        config().getString(
            "output-format", config().getString("format",  "TSV")));
}

std::unique_ptr<EmbeddedServer> EmbeddedServer::global_instance;
std::mutex EmbeddedServer::instance_mutex;
std::condition_variable EmbeddedServer::instance_cv;
size_t EmbeddedServer::client_ref_count = 0;
std::atomic<size_t> EmbeddedServer::active_session_count{0};
bool EmbeddedServer::local_server_active = false;
bool EmbeddedServer::engine_stopped = false;
std::atomic<bool> EmbeddedServer::last_cleanup_succeeded{true};
std::atomic<uint64_t> EmbeddedServer::next_namespace_id{1};

std::vector<std::pair<std::string, std::string>> EmbeddedServer::parseOptions(int argc, char ** argv)
{
    if (argc <= 1 || !argv)
        return {};

    std::vector<std::pair<std::string, std::string>> options;
    for (int i = 1; i < argc; ++i)
    {
        if (!argv[i])
            continue;
        std::string_view arg = argv[i];
        if (!arg.starts_with("--") || arg.size() <= 2)
            continue;
        arg.remove_prefix(2);

        String key;
        String value;
        if (auto pos = arg.find('='); pos != std::string_view::npos)
        {
            key = String(arg.substr(0, pos));
            value = String(arg.substr(pos + 1));
        }
        else
        {
            key = String(arg);
            /// Space-separated value; do not advance i — the value token is
            /// harmless to re-scan since it does not start with "--".
            /// A dash-leading token counts as the value only when it is a
            /// negative number (e.g. --max_partitions_to_read -1); any other
            /// dash-leading token is the next option, making this key a bare
            /// boolean flag.
            if (i + 1 < argc && argv[i + 1]
                && (argv[i + 1][0] != '-'
                    || (argv[i + 1][1] >= '0' && argv[i + 1][1] <= '9')
                    || argv[i + 1][1] == '.'))
                value = argv[i + 1];
            else
                value = "1";
        }

        if (!key.empty())
            options.emplace_back(std::move(key), std::move(value));
    }
    return options;
}

SettingsChanges EmbeddedServer::parseQuerySettingChanges(int argc, char ** argv)
{
    SettingsChanges changes;
    for (const auto & [key, value] : parseOptions(argc, argv))
        if (Settings::hasBuiltin(key))
            changes.emplace_back(key, value);
    return changes;
}

namespace
{

thread_local std::string connect_warning;

/// The paths of executable UDFs: a connection that names other ones gets them, reloaded into the running instance.
bool isFunctionPathOption(std::string_view key)
{
    return key == "user_scripts_path" || key == "dynamic_user_defined_executable_functions_path"
        || key == "user_defined_executable_functions_config";
}

/// Paths that decide what a query reads (schema files, domain lists): a connection whose value the running instance
/// would ignore is refused, since its queries would silently read other files.
bool isResultPathOption(std::string_view key)
{
    return key == "format_schema_path" || key == "google_protos_path" || key == "top_level_domains_path";
}

String normalizePath(const String & value)
{
    if (value.empty())
        return value;
    std::error_code ec;
    auto path = fs::weakly_canonical(value, ec);
    String result = ec ? fs::path(value).lexically_normal().string() : path.lexically_normal().string();
    while (result.size() > 1 && result.ends_with('/'))
        result.pop_back();
    return result;
}

}

std::string EmbeddedServer::takeConnectWarning()
{
    return std::exchange(connect_warning, {});
}

void EmbeddedServer::reconcileStartupOptions(int argc, char ** argv)
{
    auto running_path = [&](const std::string & key) -> String
    {
        if (key == "user_scripts_path")
            return global_context->getUserScriptsPath();
        if (key == "dynamic_user_defined_executable_functions_path")
            return global_context->getDynamicUserDefinedExecutableFunctionsPath();
        if (key == "filesystem_caches_path")
            return global_context->getFilesystemCachesPath();
        if (key == "format_schema_path")
            return global_context->getFormatSchemaPath();
        if (key == "google_protos_path")
            return global_context->getGoogleProtosPath();
        if (key == "top_level_domains_path")
            return config().getString(key, fs::path(global_context->getPath()) / "top_level_domains/");
        return config().getString(key, "");
    };

    std::vector<std::string> refused;
    std::vector<std::string> ignored;
    std::vector<std::string> server_level_ignored;
    std::vector<std::string> replaced;
    std::vector<std::pair<std::string, std::string>> function_paths;
    Poco::AutoPtr<Poco::Util::MapConfiguration> requested_config = new Poco::Util::MapConfiguration;

    for (const auto & [key, value] : parseOptions(argc, argv))
    {
        if (key == "path" || key == "backups.allowed_path" || key == "shared" || key == "isolated" || key == "database")
            continue;

        const bool is_path = std::ranges::find(instance_path_options, key) != std::end(instance_path_options);
        if (is_path)
        {
            String running = running_path(key);
            if (normalizePath(value) == normalizePath(running))
                continue;
            if (isFunctionPathOption(key))
            {
                function_paths.emplace_back(key, value);
                /// The executable UDFs of a path an earlier connection named are replaced, not added to.
                if (config().has(key))
                    replaced.push_back(fmt::format("{}={} (was: {})", key, value, running));
            }
            else
                (isResultPathOption(key) ? refused : ignored).push_back(fmt::format("{}={} (running: {})", key, value, running));
            continue;
        }

        requested_config->setString(key, value);
    }

    /// Read over the running values the way the instance read its own configuration, so a setting is found under its
    /// name, its alias or its configuration path (query_cache.max_entries), and equal values compare equal however
    /// they are written.
    ServerSettings requested(server_settings);
    requested.loadSettingsFromConfig(*requested_config);
    std::unordered_set<std::string> query_setting_names;
    for (const auto & change : parseQuerySettingChanges(argc, argv))
        query_setting_names.insert(change.name);
    for (auto name : server_settings.getAllRegisteredNames())
    {
        Field running = server_settings.get(name);
        Field value = requested.get(name);
        if (value == running)
            continue;
        auto entry = fmt::format(
            "{}={} (running: {})", name, applyVisitor(FieldVisitorToString(), value), applyVisitor(FieldVisitorToString(), running));
        /// An option given under a query setting's name applies to this connection's queries
        /// (EmbeddedSession::applyCmdSettings); only its server-level use keeps the running value.
        (query_setting_names.contains(String(name)) ? server_level_ignored : ignored).push_back(std::move(entry));
    }

    if (!refused.empty())
        throw DB::Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot connect with {}: another connection to this process's chDB engine is open, and the engine reads these "
            "options only when it starts. Close the other connections first, or connect with the same values",
            fmt::join(refused, ", "));

    if (!function_paths.empty())
    {
        /// The functions are shared by every connection: a connection that is going to fail must not replace them.
        if (auto changes = parseQuerySettingChanges(argc, argv); !changes.empty())
            global_context->getSettingsCopy().applyChanges(changes);

        for (const auto & [key, value] : function_paths)
            config().setString(key, value);
        global_context->setUserScriptsPath(config().getString("user_scripts_path", global_context->getUserScriptsPath()));
        global_context->setDynamicUserDefinedExecutableFunctionsPath(config().getString(
            "dynamic_user_defined_executable_functions_path", global_context->getDynamicUserDefinedExecutableFunctionsPath()));
        global_context->loadOrReloadUserDefinedExecutableFunctions(config());
        /// A function whose configuration did not change keeps its process pool, which runs the command it was
        /// created with, so one from the previous scripts directory would still answer.
        global_context->getExternalUserDefinedExecutableFunctionsLoader().reloadAllTriedToLoad();
    }

    std::vector<std::string> warnings;
    if (!ignored.empty())
        warnings.push_back(fmt::format(
            "Ignoring startup options {}: another connection to this process's chDB engine is open, and the engine reads them "
            "only when it starts. Close the other connections first for them to take effect",
            fmt::join(ignored, ", ")));
    if (!server_level_ignored.empty())
        warnings.push_back(fmt::format(
            "Startup options {} apply to this connection's queries, but their server-level values stay as the running engine "
            "started with them",
            fmt::join(server_level_ignored, ", ")));
    if (!replaced.empty())
        warnings.push_back(fmt::format(
            "Replaced the executable UDF paths of the running engine: {}. The functions loaded from the previous paths are no "
            "longer available to any connection",
            fmt::join(replaced, ", ")));
    if (!warnings.empty())
    {
        connect_warning = fmt::format("{}", fmt::join(warnings, ". "));
        LOG_WARNING(&logger(), "{}", connect_warning);
    }
}

EmbeddedServer::LocalServerGuard::LocalServerGuard()
{
    std::lock_guard<std::mutex> lock(instance_mutex);
    if (engine_stopped)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "chDB has been shut down and cannot be used again in this process");
    if (client_ref_count != 0 || global_instance != nullptr || local_server_active || DatabaseCatalog::isAvailable())
        throw DB::Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot run one-shot LocalServer query while a chDB connection is open; "
            "execute the query on a connection or close all open connections first");
    local_server_active = true;
}

EmbeddedServer::LocalServerGuard::~LocalServerGuard()
{
    {
        std::lock_guard<std::mutex> lock(instance_mutex);
        local_server_active = false;
    }
    instance_cv.notify_all();
}

EmbeddedDatabase::EmbeddedDatabase(
    EmbeddedServer & server_,
    ChdbNamespacePtr ns_,
    ContextMutablePtr ns_context_,
    String default_database_name_,
    String logical_path_,
    bool default_read_only_,
    std::unique_ptr<StatusFile> status_file_)
    : server(server_)
    , ns(std::move(ns_))
    , ns_context(std::move(ns_context_))
    , default_database_name(std::move(default_database_name_))
    , logical_path(std::move(logical_path_))
    , default_read_only(default_read_only_)
    , status_file(std::move(status_file_))
{
}

EmbeddedDatabase::~EmbeddedDatabase()
{
    unmount();
    EmbeddedServer::releaseInstance();
}

void EmbeddedDatabase::addBackupsAllowedPaths(const std::vector<String> & paths)
{
    if (paths.empty() || !ns)
        return;
    std::lock_guard<std::mutex> lock(metadata_mutex);
    for (const auto & p : paths)
    {
        if (std::ranges::find(ns->backups_allowed_paths, p) == ns->backups_allowed_paths.end())
            ns->backups_allowed_paths.push_back(p);
    }
}

void EmbeddedDatabase::unmount() noexcept
{
    try
    {
        if (!isMemory() && !logical_path.empty())
            server.onDatabaseUnmounted(logical_path);

        if (ns && DatabaseCatalog::isAvailable())
            DatabaseCatalog::instance().cleanupChdbNamespace(ns, ns_context);

        if (ns && (!ns->disk_name.empty() || !ns->policy_name.empty()) && server.getGlobalContext())
            server.getGlobalContext()->removeChdbDiskAndStoragePolicy(ns->disk_name, ns->policy_name);

        if (ns)
        {
            ns->sql_objects_storage.reset();
            ns->storage_policy.reset();
            ns->disk.reset();
        }
        ns_context.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    try
    {
        status_file.reset();
    }
    catch (...)
    {
    }
}

void EmbeddedServer::onDatabaseUnmounted(const String & canonical_path) noexcept
{
    try
    {
        std::lock_guard<std::mutex> lock(mounts_mutex);
        auto it = mounted_paths.find(canonical_path);
        if (it != mounted_paths.end() && it->second.expired())
            mounted_paths.erase(it);
    }
    catch (...)
    {
    }
}

EmbeddedDatabasePtr EmbeddedServer::createMemoryDatabase(
    const String & default_db_name,
    bool default_read_only,
    const std::vector<String> & backups_allowed_paths)
{
    const uint64_t id = next_namespace_id.fetch_add(1, std::memory_order_relaxed);
    auto ns = std::make_shared<ChdbNamespace>();
    ns->catalog_prefix = fmt::format("__chdb_ns_{}__", id);
    ns->is_memory = true;
    ns->backups_allowed_paths = backups_allowed_paths;

    ContextMutablePtr ns_context = Context::createCopy(global_context);
    ns_context->setChdbNamespace(ns);
    ns_context->markAsNamespaceGlobalContext();
    ns->sql_objects_storage = std::make_shared<UserDefinedSQLObjectsMemoryStorage>(ns_context);

    String server_default_db = server_settings[ServerSetting::default_database];
    if (server_default_db.empty())
        server_default_db = DatabaseCatalog::DEFAULT_DATABASE;
    const String effective_default_db = default_db_name.empty() ? server_default_db : default_db_name;

    bool mounted_ok = false;
    SCOPE_EXIT({
        if (!mounted_ok && DatabaseCatalog::isAvailable())
            DatabaseCatalog::instance().cleanupChdbNamespace(ns, ns_context);
    });

    {
        ChdbNamespaceScope ns_scope(ns);
        DatabasePtr default_db = createMemoryDatabaseOverlay(server_default_db, ns_context);
        if (UUID u = default_db->getUUID(); u != UUIDHelpers::Nil && !DatabaseCatalog::instance().hasUUIDMapping(u))
            DatabaseCatalog::instance().addUUIDMapping(u);
        DatabaseCatalog::instance().attachDatabase(server_default_db, default_db);
        ns_context->setCurrentDatabase(effective_default_db);
    }

    mounted_ok = true;
    return std::make_shared<EmbeddedDatabase>(
        *this, std::move(ns), std::move(ns_context), effective_default_db, ":memory:", default_read_only, nullptr);
}

EmbeddedDatabasePtr EmbeddedServer::mountPersistentDatabase(
    const String & canonical_path,
    const String & default_db_name,
    bool default_read_only,
    bool allow_readonly_os_dir,
    const std::vector<String> & backups_allowed_paths)
{
    fs::create_directories(fs::path(canonical_path));

    /// Acquire `<path>/status` flock BEFORE touching any files inside `<path>`, so a second
    /// process attempting to open a locked directory fails immediately without disturbing spill
    /// or metadata files owned by the lock holder.
    std::unique_ptr<StatusFile> path_status;
    if (!(default_read_only || allow_readonly_os_dir) || FS::canWrite(canonical_path, /*allow_throw=*/false))
        path_status = std::make_unique<StatusFile>(fs::path(canonical_path) / "status", StatusFile::write_full_info);

    const uint64_t id = next_namespace_id.fetch_add(1, std::memory_order_relaxed);
    auto ns = std::make_shared<ChdbNamespace>();
    ns->catalog_prefix = fmt::format("__chdb_ns_{}__", id);
    ns->data_path = (fs::path(canonical_path) / "").string();
    ns->disk_name = fmt::format("__chdb_disk_{}", id);
    ns->policy_name = fmt::format("__chdb_policy_{}", id);
    ns->is_memory = false;
    ns->backups_allowed_paths = backups_allowed_paths;

    ContextMutablePtr ns_context;
    bool mounted_ok = false;
    SCOPE_EXIT({
        if (!mounted_ok)
        {
            if (DatabaseCatalog::isAvailable())
                DatabaseCatalog::instance().cleanupChdbNamespace(ns, ns_context);
            if (global_context)
                global_context->removeChdbDiskAndStoragePolicy(ns->disk_name, ns->policy_name);
        }
    });

    DiskPtr disk = global_context->getOrCreateDisk(ns->disk_name, [&](const DisksMap &) -> DiskPtr
    {
        auto d = std::make_shared<DiskLocal>(
            ns->disk_name, ns->data_path, 0, global_context, config(), "storage_configuration.disks");
        d->startup(/*skip_access_check=*/false);
        return d;
    });
    ns->disk = disk;

    StoragePolicyPtr policy = global_context->getOrCreateStoragePolicy(ns->policy_name, [&](const StoragePoliciesMap &) -> StoragePolicyPtr
    {
        VolumePtr vol = std::make_shared<SingleDiskVolume>("_volume_" + ns->disk_name, disk);
        return std::make_shared<StoragePolicy>(ns->policy_name, Volumes{vol}, 0.0);
    });
    ns->storage_policy = policy;

    ns_context = Context::createCopy(global_context);
    ns_context->setChdbNamespace(ns);
    ns_context->markAsNamespaceGlobalContext();

    auto udf_storage = std::make_shared<UserDefinedSQLObjectsDiskStorage>(
        ns_context, (fs::path(canonical_path) / "user_defined" / "").string());
    ns->sql_objects_storage = udf_storage;

    String server_default_db = server_settings[ServerSetting::default_database];
    if (server_default_db.empty())
        server_default_db = DatabaseCatalog::DEFAULT_DATABASE;
    const String effective_default_db = default_db_name.empty() ? server_default_db : default_db_name;

    {
        ChdbNamespaceScope ns_scope(ns);
        DatabasePtr default_db = createPersistentDatabaseOverlay(server_default_db, ns_context);
        if (UUID u = default_db->getUUID(); u != UUIDHelpers::Nil && !DatabaseCatalog::instance().hasUUIDMapping(u))
            DatabaseCatalog::instance().addUUIDMapping(u);
        DatabaseCatalog::instance().attachDatabase(server_default_db, default_db);

        if (fs::exists(fs::path(canonical_path) / "metadata") && !config().has("only-system-tables"))
        {
            DatabaseCatalog::instance().loadMarkedAsDroppedTablesForDisk(disk, ns_context);
            waitLoad(loadMetadata(ns_context));
        }

        if (fs::exists(fs::path(canonical_path) / "user_defined"))
            udf_storage->loadObjects();

        ns_context->setCurrentDatabase(effective_default_db);
    }

    mounted_ok = true;
    return std::make_shared<EmbeddedDatabase>(
        *this, std::move(ns), std::move(ns_context), effective_default_db, canonical_path, default_read_only, std::move(path_status));
}

EmbeddedServer & EmbeddedServer::getInstance(int argc, char ** argv)
{
    std::unique_lock<std::mutex> lock(instance_mutex);
    /// Wait for any in-flight one-shot LocalServer call (pyEntryClickHouseLocal) to finish
    /// tearing down its global Context and DatabaseCatalog before booting EmbeddedServer.
    instance_cv.wait(lock, [] { return !local_server_active; });

    /// A warning belongs to the connection that is being made, not to an earlier one that failed after leaving it.
    connect_warning.clear();

    if (engine_stopped)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "chDB has been shut down and cannot be used again in this process");

    if (global_instance)
    {
        if (argc > 0 && argv)
            global_instance->reconcileStartupOptions(argc, argv);
        ++client_ref_count;
        return *global_instance;
    }

    last_cleanup_succeeded.store(false, std::memory_order_release);
    global_instance = std::make_unique<EmbeddedServer>();
    try
    {
        if (argc == 0 || !argv)
        {
            const char * default_argv[] = {"chdb"};
            global_instance->initializeWithArgs(1, const_cast<char **>(default_argv));
        }
        else
        {
            global_instance->initializeWithArgs(argc, argv);
        }
    }
    catch (...)
    {
        global_instance.reset();
        throw;
    }

    client_ref_count = 1;
    return *global_instance;
}

EmbeddedDatabasePtr EmbeddedServer::openDatabase(
    const std::string & path,
    int argc,
    char ** argv,
    bool isolated_memory,
    bool default_read_only,
    bool explicit_path_arg,
    bool allow_readonly_os_dir)
{
    String trimmed_path = path;
    Poco::trimInPlace(trimmed_path);
    if (explicit_path_arg && trimmed_path.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot work with empty storage path that is explicitly specified"
            " by the --path option. Please check the program options and"
            " correct the --path.");
    }

    String requested_default_db;
    std::vector<String> backups_allowed_paths;
    for (const auto & [key, value] : parseOptions(argc, argv))
    {
        if (key == "database")
            requested_default_db = value;
        else if (key == "backups.allowed_path" && !value.empty())
            backups_allowed_paths.push_back(value);
        else if (key == "shared")
            isolated_memory = (value == "0" || boost::iequals(value, "false") || boost::iequals(value, "no"));
        else if (key == "isolated")
            isolated_memory = !(value == "0" || boost::iequals(value, "false") || boost::iequals(value, "no"));
        else if (key == "readonly" && (value == "1" || value == "2"))
            allow_readonly_os_dir = true;
    }

    EmbeddedServer & srv = getInstance(argc, argv);
    bool pin_transferred = false;
    SCOPE_EXIT({
        if (!pin_transferred)
            releaseInstance();
    });

    const bool is_memory = trimmed_path.empty() || trimmed_path == ":memory:";
    if (is_memory)
    {
        if (isolated_memory)
        {
            auto db = srv.createMemoryDatabase(requested_default_db, default_read_only, backups_allowed_paths);
            pin_transferred = true;
            return db;
        }

        std::lock_guard<std::mutex> mount_lock(srv.mounts_mutex);
        if (auto existing = srv.shared_memory_db.lock())
        {
            existing->addBackupsAllowedPaths(backups_allowed_paths);
            return existing;
        }

        auto db = srv.createMemoryDatabase(requested_default_db, /*default_read_only=*/false, backups_allowed_paths);
        srv.shared_memory_db = db;
        pin_transferred = true;
        return db;
    }

    fs::path resolved_fs_path(trimmed_path);
    if (!resolved_fs_path.is_absolute())
    {
        std::error_code abs_ec;
        auto abs_p = fs::absolute(resolved_fs_path, abs_ec);
        if (abs_ec)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to resolve database path '{}': {}", trimmed_path, abs_ec.message());
        resolved_fs_path = std::move(abs_p);
    }
    const String canonical_path = normalizePath(resolved_fs_path.string());

    std::lock_guard<std::mutex> mount_lock(srv.mounts_mutex);
    if (auto it = srv.mounted_paths.find(canonical_path); it != srv.mounted_paths.end())
    {
        if (auto existing = it->second.lock())
        {
            existing->addBackupsAllowedPaths(backups_allowed_paths);
            return existing;
        }
        srv.mounted_paths.erase(it);
    }

    auto db = srv.mountPersistentDatabase(
        canonical_path, requested_default_db, default_read_only, allow_readonly_os_dir, backups_allowed_paths);
    srv.mounted_paths[canonical_path] = db;
    pin_transferred = true;
    return db;
}

EmbeddedDatabasePtr EmbeddedServer::openDatabaseFromArgs(int argc, char ** argv)
{
    std::string path = ":memory:";
    bool explicit_path = false;
    bool allow_ro_dir = false;
    for (const auto & [key, value] : parseOptions(argc, argv))
    {
        if (key == "path")
        {
            path = value;
            explicit_path = true;
        }
        else if (key == "readonly" && (value == "1" || value == "2"))
        {
            allow_ro_dir = true;
        }
    }
    return openDatabase(
        path,
        argc,
        argv,
        /*isolated_memory=*/false,
        /*default_read_only=*/false,
        explicit_path,
        allow_ro_dir);
}

void EmbeddedServer::registerActiveSession() noexcept
{
    active_session_count.fetch_add(1, std::memory_order_relaxed);
}

void EmbeddedServer::unregisterActiveSession() noexcept
{
    active_session_count.fetch_sub(1, std::memory_order_relaxed);
}

void EmbeddedServer::releaseInstance()
{
    std::lock_guard<std::mutex> lock(instance_mutex);

    if (client_ref_count == 0)
        return;

    --client_ref_count;
    if (client_ref_count == 0)
    {
        global_instance.reset();
    }
}

bool EmbeddedServer::beginShutdown()
{
    std::lock_guard<std::mutex> lock(instance_mutex);

    if (client_ref_count != 0 || local_server_active)
        return false;

    engine_stopped = true;
    return true;
}

bool EmbeddedServer::hasActiveClients()
{
    std::lock_guard<std::mutex> lock(instance_mutex);
    return client_ref_count != 0;
}

bool EmbeddedServer::lastCleanupSucceeded()
{
    return last_cleanup_succeeded.load(std::memory_order_acquire);
}

void EmbeddedServer::cancelPythonQueriesIfExclusive(size_t python_clients)
{
    std::lock_guard<std::mutex> lock(instance_mutex);
    /// chdb_spec: Do not cancel a C/Go host's query when its connection shares
    /// this embedded server with a finalizing Python interpreter.
    const size_t current_sessions = active_session_count.load(std::memory_order_acquire);
    if (python_clients && current_sessions == python_clients && global_instance && global_instance->global_context)
        global_instance->global_context->getProcessList().killAllQueries();
    /// chdb_spec: end Python-only exit cancellation.
}

void EmbeddedServer::initializeWithArgs(int argc, char ** argv)
{
    db_path = ":memory:"; // Default path
    for (int i = 1; i < argc; i++)
    {
        if (strncmp(argv[i], "--path=", 7) == 0)
        {
            db_path = argv[i] + 7;
            break;
        }
    }

    try
    {
        std::vector<std::string> args;
        for (int i = 0; i < argc; ++i)
        {
            args.push_back(argv[i]);
        }

        Poco::Util::Application::ArgVec arg_vec;
        for (const auto & arg : args)
        {
            arg_vec.push_back(arg);
        }
        argsToConfig(arg_vec, config(), 100);
        /// argsToConfig drops a value-less flag that appears last, so capture the
        /// boolean `--stacktrace` flag explicitly to make it position-independent
        /// (bare `--stacktrace` works like in clickhouse-local). Only fall back
        /// when argsToConfig did not capture a usable value, so explicit forms
        /// like `--stacktrace=0` or `--stacktrace 0` keep the user's value.
        if (config().getString("stacktrace", "").empty()
            && std::ranges::any_of(args, [](const auto & arg) { return arg == "--stacktrace"; }))
            config().setBool("stacktrace", true);

        initialize(*this);
        int ret = main(args);
        if (ret != 0)
        {
            auto err_msg = getErrorMsg();
            LOG_ERROR(&logger(), "Error initializing EmbeddedServer: {}", err_msg);
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Error initializing EmbeddedServer: {}", err_msg);
        }
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("EmbeddedServer"), "Failed to initialize EmbeddedServer: {}", e.what());
        throw;
    }
}
} // namespace DB
