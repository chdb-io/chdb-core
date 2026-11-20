#pragma once

#include <Client/ClientApplicationBase.h>
#include <Client/LocalConnection.h>

#include <Core/ServerSettings.h>
#include <Interpreters/ChdbNamespace.h>
#include <Interpreters/Context_fwd.h>
#include <Loggers/Loggers.h>
#include <Common/MemoryWorker.h>
#include <Common/SettingsChanges.h>
#include <Common/StatusFile.h>

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{

class EmbeddedServer;

/// Refcounted database mount on the single process-wide `EmbeddedServer`.
/// Owns either an isolated zero-disk `:memory:` `DatabaseMemory` namespace or a
/// dynamically mounted persistent directory (`StatusFile` lock + `DiskLocal` +
/// `StoragePolicy` + `DatabaseAtomic` catalog namespace).
class EmbeddedDatabase
{
public:
    EmbeddedDatabase(
        EmbeddedServer & server_,
        ChdbNamespacePtr ns_,
        ContextMutablePtr ns_context_,
        String default_database_name_,
        String logical_path_,
        bool default_read_only_,
        std::unique_ptr<StatusFile> status_file_);

    ~EmbeddedDatabase();

    EmbeddedDatabase(const EmbeddedDatabase &) = delete;
    EmbeddedDatabase & operator=(const EmbeddedDatabase &) = delete;

    EmbeddedServer & getServer() const noexcept { return server; }
    const ChdbNamespacePtr & getNamespace() const noexcept { return ns; }
    const ContextMutablePtr & getNamespaceContext() const noexcept { return ns_context; }
    const String & getDefaultDatabaseName() const noexcept { return default_database_name; }
    const String & getPath() const noexcept { return logical_path; }
    bool isMemory() const noexcept { return ns ? ns->is_memory : true; }
    bool isReadOnly() const noexcept { return default_read_only; }

    void addBackupsAllowedPaths(const std::vector<String> & paths);

private:
    void unmount() noexcept;

    EmbeddedServer & server;
    ChdbNamespacePtr ns;
    ContextMutablePtr ns_context;
    String default_database_name;
    String logical_path;
    bool default_read_only = false;
    std::unique_ptr<StatusFile> status_file;
    mutable std::mutex metadata_mutex;
};

using EmbeddedDatabasePtr = std::shared_ptr<EmbeddedDatabase>;

/// Lightweight Application for embedded server.
/// Hosts a single shared `ContextSharedPart`, `DatabaseCatalog`, and thread-pool runtime per
/// process while supporting multiple isolated `:memory:` namespaces and dynamically mounted
/// persistent directory paths simultaneously.
class EmbeddedServer : public Poco::Util::Application, public IHints<2>, public Loggers
{
public:
    EmbeddedServer() = default;

    ~EmbeddedServer() override;

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & /*args*/) override;

    VectorWithMemoryTracking<String> getAllRegisteredNames() const override { return {}; }

    ContextMutablePtr getGlobalContext() { return global_context; }

    std::string getErrorMsg() const { return error_message_oss.str(); }

    static EmbeddedServer & getInstance(int argc = 0, char ** argv = nullptr);

    /// Opens or joins a database instance on the process-wide EmbeddedServer.
    /// - `path` empty or `":memory:"`: creates a private zero-disk `DatabaseMemory` namespace when
    ///   `isolated_memory == true`, or joins the shared legacy `:memory:` namespace when `false`.
    /// - `path` naming a filesystem directory: mounts (or joins an already-mounted) persistent path.
    static EmbeddedDatabasePtr openDatabase(
        const std::string & path,
        int argc = 0,
        char ** argv = nullptr,
        bool isolated_memory = false,
        bool default_read_only = false,
        bool explicit_path_arg = false,
        bool allow_readonly_os_dir = false);

    /// Legacy CLI-style open helper used by `chdb_connect(argc, argv)`: extracts `--path=...`
    /// (and `--shared=0` / `--isolated=1`) from `argv`.
    static EmbeddedDatabasePtr openDatabaseFromArgs(int argc, char ** argv);

    /// The --key=value, --key value and bare --key (meaning "1") options of a connection, in order, as
    /// clickhouse-local reads them. A dash-leading token is a value only when it is a negative number.
    static std::vector<std::pair<std::string, std::string>> parseOptions(int argc, char ** argv);

    /// The options of a connection that are query settings, applied to each of its sessions.
    static SettingsChanges parseQuerySettingChanges(int argc, char ** argv);

    /// The warning of the last getInstance()/openDatabase() on this thread that joined a running instance whose
    /// startup options differed from the caller's, and clears it. Empty when there was none.
    static std::string takeConnectWarning();

    static void releaseInstance();
    static void registerActiveSession() noexcept;
    static void unregisterActiveSession() noexcept;

    /// Claims the engine for teardown: succeeds only when no database/client holds the
    /// instance, and then refuses every later openDatabase()/getInstance() for the lifetime of
    /// the process.
    static bool beginShutdown();
    static bool hasActiveClients();
    static bool lastCleanupSucceeded();
    static void cancelPythonQueriesIfExclusive(size_t python_clients);

    /// Reserves exclusive process-global catalog/application state for a one-shot
    /// LocalServer invocation (pyEntryClickHouseLocal).
    class LocalServerGuard
    {
    public:
        LocalServerGuard();
        ~LocalServerGuard();
        LocalServerGuard(const LocalServerGuard &) = delete;
        LocalServerGuard & operator=(const LocalServerGuard &) = delete;
    };

    std::string getPath() const { return db_path; }

private:
    friend class EmbeddedDatabase;

    EmbeddedDatabasePtr createMemoryDatabase(
        const String & default_db_name,
        bool default_read_only,
        const std::vector<String> & backups_allowed_paths);

    EmbeddedDatabasePtr mountPersistentDatabase(
        const String & canonical_path,
        const String & default_db_name,
        bool default_read_only,
        bool allow_readonly_os_dir,
        const std::vector<String> & backups_allowed_paths);

    void onDatabaseUnmounted(const String & canonical_path) noexcept;

    void tryInitPath();
    void setupUsers();
    void cleanup();
    void logMergeTreeStateAtShutdown() const;
    void processConfig();
    void setupSystemLogs();
    void applyCmdOptions(ContextMutablePtr context);
    void initializeWithArgs(int argc, char ** argv);
    void reconcileStartupOptions(int argc, char ** argv);

    static std::unique_ptr<EmbeddedServer> global_instance;
    static std::mutex instance_mutex;
    static std::condition_variable instance_cv;
    static size_t client_ref_count;
    static std::atomic<size_t> active_session_count;
    static bool local_server_active;
    /// Set by beginShutdown(); guarded by instance_mutex.
    static bool engine_stopped;
    static std::atomic<bool> last_cleanup_succeeded;
    static std::atomic<uint64_t> next_namespace_id;

    mutable std::mutex mounts_mutex;
    std::weak_ptr<EmbeddedDatabase> shared_memory_db;
    std::unordered_map<std::string, std::weak_ptr<EmbeddedDatabase>> mounted_paths;

    /// Whether --config-file was passed by the caller, as opposed to a config found next to the
    /// process. Only an explicit one may arm system logs.
    bool config_file_passed_explicitly = false;
    std::string db_path;
    std::string boot_explicit_path;
    ServerSettings server_settings;
    std::optional<std::filesystem::path> temporary_directory_to_delete;
    /// Declared before global_context and memory_worker so reverse member destruction
    /// destroys memory_worker -> global_context -> shared_context.
    SharedContextHolder shared_context;
    ContextMutablePtr global_context;
    std::unique_ptr<MemoryWorker> memory_worker;
    String home_path;
    std::stringstream error_message_oss;
};
} // namespace DB
