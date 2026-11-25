#pragma once

#include <Core/UUID.h>

#include <optional>
#include <string_view>

namespace DB
{
class AccessControl;
}

namespace CHDB
{

/// The `restrict_local_filesystem_access` connection option.
///
/// A restricted connection runs its queries as a copy of the `default` user that lacks the grants for local files:
/// READ and WRITE on the FILE, SQLITE, DISK, ODBC and JDBC sources and the engines reading them, SET DEFINER, and
/// the SYSTEM commands that load configuration or write files. It also lacks SYSTEM SHUTDOWN, as SYSTEM SHUTDOWN and
/// SYSTEM KILL end the host process. ClickHouse checks these grants wherever SQL reaches the corresponding
/// capability, so they also hold for subqueries, views, query parameters, streaming queries and RESTORE, and `SET`
/// cannot lift them. `checkLocalFilesystemAccess()` covers what is not behind such a grant, e.g. `INTO OUTFILE`,
/// custom disks, programs and dictionaries on files. The engine's own I/O (metadata, temporary files, caches,
/// credential files) runs without a user and is not affected.
inline constexpr std::string_view RESTRICT_LOCAL_FILESYSTEM_ACCESS_OPTION = "restrict_local_filesystem_access";

/// The user restricted connections run as.
inline constexpr std::string_view RESTRICTED_LOCAL_FILESYSTEM_USER = "chdb_restricted_local_filesystem";

/// Adds the restricted user, made from the `default` user, to `access_control`. Call after the users configuration
/// is loaded. When it cannot be made (no `default` user, or a user with its name already exists), restricted
/// connections are refused rather than run unrestricted.
void addRestrictedLocalFilesystemUser(DB::AccessControl & access_control);

/// The id of the user added by the last addRestrictedLocalFilesystemUser(), if it succeeded.
std::optional<DB::UUID> getRestrictedLocalFilesystemUserID();

/// Whether `argv` asks for a restricted connection. Accepts --restrict_local_filesystem_access[=<bool>]; the last
/// occurrence wins. Throws BAD_ARGUMENTS for a value that is not a boolean.
bool parseRestrictLocalFilesystemAccess(int argc, char ** argv);

}
