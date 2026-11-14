#pragma once

#include <Client/ClientApplicationBase.h>
#include <Client/LocalConnection.h>

#include <Core/ServerSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Loggers/Loggers.h>
#include <Common/MemoryWorker.h>
#include <Common/SettingsChanges.h>
#include <Common/StatusFile.h>

#include <filesystem>
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace DB
{

/// Lightweight Application for embeeded server
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
///
/// EmbeddedServer lifecycle is managed exclusively by getInstance/releaseInstance.
/// Only one EmbeddedServer instance can exist globally at a time.
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

    /// The --key=value, --key value and bare --key (meaning "1") options of a connection, in order, as
    /// clickhouse-local reads them. A dash-leading token is a value only when it is a negative number.
    static std::vector<std::pair<std::string, std::string>> parseOptions(int argc, char ** argv);

    /// The options of a connection that are query settings, applied to each of its sessions.
    static SettingsChanges parseQuerySettingChanges(int argc, char ** argv);

    /// The warning of the last getInstance() on this thread that joined a running instance whose startup options
    /// differed from the connection's, and clears it. Empty when there was none.
    static std::string takeConnectWarning();

    static void releaseInstance();

    /// Claims the engine for teardown: succeeds only when no client holds the
    /// instance, and then refuses every later getInstance() for the lifetime of
    /// the process. Deciding and claiming under one lock is what keeps a
    /// concurrent connect from slipping in behind the check.
    static bool beginShutdown();
    static bool hasActiveClients();
    static bool lastCleanupSucceeded();
    static void cancelPythonQueriesIfExclusive(size_t python_clients);

    std::string getPath() const { return db_path; }

private:
    void tryInitPath();
    void setupUsers();
    void cleanup();
    void logMergeTreeStateAtShutdown() const;
    void processConfig();
    void setupSystemLogs();
    void applyCmdOptions(ContextMutablePtr context);
    void initializeWithArgs(int argc, char ** argv);
    /// Startup options of a connection that joins this running instance: refuses the ones that would make its queries
    /// read other files, loads other executable UDF paths, and leaves a warning for the rest.
    void reconcileStartupOptions(int argc, char ** argv);

    static std::unique_ptr<EmbeddedServer> global_instance;
    static std::mutex instance_mutex;
    static size_t client_ref_count;
    /// Set by beginShutdown(); guarded by instance_mutex.
    static bool engine_stopped;
    static std::atomic<bool> last_cleanup_succeeded;
    /// Whether --config-file was passed by the caller, as opposed to a config found next to the
    /// process. Only an explicit one may arm system logs.
    bool config_file_passed_explicitly = false;
    std::string db_path;
    ServerSettings server_settings;
    std::optional<StatusFile> status;
    std::optional<std::filesystem::path> temporary_directory_to_delete;
    std::unique_ptr<MemoryWorker> memory_worker;
    ContextMutablePtr global_context;
    String home_path;
    std::stringstream error_message_oss;
    SharedPtrContextHolder shared_context;
};
} // namespace DB
