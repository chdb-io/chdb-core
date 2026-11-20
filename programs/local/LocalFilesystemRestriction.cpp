#include "LocalFilesystemRestriction.h"

#include "EmbeddedServer.h"

#include <Access/AccessControl.h>
#include <Access/LocalFilesystemAccess.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Access/UsersConfigParser.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <Databases/DatabaseFactory.h>
#include <Common/quoteString.h>
#include <Storages/StorageFactory.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/parseQuery.h>

#include <boost/algorithm/string/case_conv.hpp>

#include <algorithm>
#include <mutex>
#include <set>
#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace CHDB
{

using namespace DB;

namespace
{

constexpr auto STORAGE_NAME = "chdb_restricted_local_filesystem";

/// Grants of the `default` user that reach the local filesystem or run programs. Revoking a grant the user does not
/// hold is a no-op, so a `default` user that is already narrower stays as narrow.
constexpr const char * REVOKED_GRANTS[] = {
    /// file(), url('file://...'), file descriptors, filesystem(), local data lakes, ATTACH ... FROM '<path>',
    /// EmbeddedRocksDB with a path, File backups and Backup databases on files.
    "REVOKE READ, WRITE ON FILE",
    "REVOKE READ, WRITE ON SQLITE",
    /// ODBC and JDBC go through a bridge program, and an ODBC connection string can name a driver library to load.
    "REVOKE READ, WRITE ON ODBC",
    "REVOKE READ, WRITE ON JDBC",
    /// Disk backups.
    "REVOKE READ, WRITE ON DISK",
    /// A view with SQL SECURITY DEFINER runs with its definer's grants; naming another user would borrow theirs.
    "REVOKE SET DEFINER ON *",
    /// Reload configuration and executable functions from disk, or write heap profiles to files.
    "REVOKE SYSTEM RELOAD CONFIG, SYSTEM RELOAD USERS, SYSTEM RELOAD FUNCTION, SYSTEM JEMALLOC ON *.*",
    /// SYSTEM SHUTDOWN and SYSTEM KILL signal the whole process group of the host.
    "REVOKE SYSTEM SHUTDOWN ON *.*",
    /// A `default` with access management could otherwise run EXECUTE AS default, change users or grant roles.
    "REVOKE ACCESS MANAGEMENT ON *.*",
};

/// Sources revoked above. CREATE TABLE and RESTORE check an engine through the TABLE ENGINE grant, which ALL
/// includes for every engine, so the engines reading these sources are revoked one by one.
constexpr AccessTypeObjects::Source REVOKED_SOURCES[] = {
    AccessTypeObjects::Source::FILE,
    AccessTypeObjects::Source::SQLITE,
    AccessTypeObjects::Source::ODBC,
    AccessTypeObjects::Source::JDBC,
    AccessTypeObjects::Source::DISK,
};

bool isRevokedSource(const std::optional<AccessTypeObjects::Source> & source)
{
    return source && std::ranges::find(REVOKED_SOURCES, *source) != std::end(REVOKED_SOURCES);
}

void revoke(User & user, const String & revoke_query)
{
    ParserGrantQuery parser;
    parser.setParseWithoutGrantees();
    auto ast = parseQuery(parser, revoke_query, "", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    const auto & query = ast->as<const ASTGrantQuery &>();
    if (!query.is_revoke)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not a REVOKE query: {}", revoke_query);
    user.access.revoke(query.access_rights_elements);
}

void revokeGrants(User & user)
{
    for (const char * revoke_query : REVOKED_GRANTS)
        revoke(user, revoke_query);

    std::set<String> engines;
    for (const auto & [name, creator] : StorageFactory::instance().getAllStorages())
        if (isRevokedSource(creator.features.source_access_type))
            engines.insert(name);
    for (const auto & [name, creator] : DatabaseFactory::instance().getDatabaseEngines())
        if (isRevokedSource(creator.features.source_access_type))
            engines.insert(name);
    for (const auto & engine : engines)
        revoke(user, fmt::format("REVOKE TABLE ENGINE ON {}", backQuoteIfNeed(engine)));
}

/// Grants of roles count towards a user's access and cannot be revoked from the user, so the restricted user gets
/// the grants and settings of the roles `default` logs in with, and the roles those include, and holds no role
/// itself. Returns those roles, whose row policies and quotas apply to it along with those of `default` (see
/// RestrictedLocalFilesystemUser).
boost::container::flat_set<UUID> mergeRoles(User & user, const AccessControl & access_control)
{
    boost::container::flat_set<UUID> roles;
    std::vector<UUID> pending = user.granted_roles.findGranted(user.default_roles);
    while (!pending.empty())
    {
        const UUID role_id = pending.back();
        pending.pop_back();
        if (!roles.insert(role_id).second)
            continue;
        auto role = access_control.tryRead<Role>(role_id);
        if (!role)
            continue;
        user.access.makeUnion(role->access);
        user.settings.merge(role->settings);
        pending.insert(pending.end(), role->granted_roles.getGranted().begin(), role->granted_roles.getGranted().end());
    }
    user.granted_roles = {};
    user.default_roles = RolesOrUsersSet{};
    return roles;
}

std::mutex restricted_user_mutex;
std::optional<UUID> restricted_user_id;

}

void addRestrictedLocalFilesystemUser(AccessControl & access_control)
{
    std::lock_guard lock(restricted_user_mutex);
    restricted_user_id.reset();
    setRestrictedLocalFilesystemUser(std::nullopt);

    auto log = getLogger("LocalFilesystemRestriction");
    try
    {
        const auto default_id = access_control.find<User>("default");
        auto default_user = default_id ? access_control.tryRead<User>(*default_id) : nullptr;
        if (!default_user)
        {
            LOG_INFO(log, "No `default` user: connections cannot restrict local filesystem access");
            return;
        }

        const String user_name(RESTRICTED_LOCAL_FILESYSTEM_USER);
        auto storage = access_control.findStorageByName(STORAGE_NAME);
        if (!storage)
        {
            storage = std::make_shared<MemoryAccessStorage>(STORAGE_NAME, access_control.getChangesNotifier(), /*allow_backup_=*/false);
            access_control.addStorage(storage);
        }

        /// A user of this name from the host's configuration is not ours to run restricted connections as.
        if (auto existing = access_control.find<User>(user_name); existing && access_control.findStorage(*existing) != storage)
        {
            LOG_WARNING(log, "User `{}` already exists: connections cannot restrict local filesystem access", user_name);
            return;
        }

        auto user = typeid_cast<std::shared_ptr<User>>(default_user->clone());
        user->setName(user_name);
        const auto default_roles = mergeRoles(*user, access_control);
        revokeGrants(*user);

        const UUID id = UsersConfigParser::generateID(AccessEntityType::USER, user_name);
        /// Registered before the user exists, so no context of it ever resolves row policies or quotas as itself.
        setRestrictedLocalFilesystemUser(RestrictedLocalFilesystemUser{
            .id = id, .policy_user_id = *default_id, .policy_user_name = default_user->getName(), .policy_roles = default_roles});
        storage->insert(id, user, /*replace_if_exists=*/true, /*throw_if_exists=*/false);
        restricted_user_id = id;
    }
    catch (...)
    {
        setRestrictedLocalFilesystemUser(std::nullopt);
        tryLogCurrentException(log, "Cannot create the user for connections that restrict local filesystem access");
    }
}

std::optional<UUID> getRestrictedLocalFilesystemUserID()
{
    std::lock_guard lock(restricted_user_mutex);
    return restricted_user_id;
}

bool parseRestrictLocalFilesystemAccess(int argc, char ** argv)
{
    bool restrict = false;
    for (const auto & [key, value] : DB::EmbeddedServer::parseOptions(argc, argv))
    {
        if (key != RESTRICT_LOCAL_FILESYSTEM_ACCESS_OPTION)
            continue;
        const String lower = boost::algorithm::to_lower_copy(value);
        if (lower == "1" || lower == "true")
            restrict = true;
        else if (lower == "0" || lower == "false")
            restrict = false;
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Invalid value for {}: '{}', expected 0, 1, false or true", key, value);
    }
    return restrict;
}

}
