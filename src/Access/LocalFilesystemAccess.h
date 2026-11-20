#pragma once

#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>

#include <optional>
#include <string_view>

namespace DB
{

class ASTFunctionWithKeyValueArguments;

/// chDB's `restrict_local_filesystem_access` connection option runs a connection as a dedicated user that lacks the
/// grants for local files. ClickHouse checks those grants for `file()`, the `File` engine and similar sources; the
/// functions below cover what is not behind such a grant, such as `INTO OUTFILE` or custom disks. They apply only to
/// that user, so other users, including ones that merely lack the grants, keep ClickHouse's behavior.

/// Registers the user of restricted connections, or clears it with nullopt.
void setRestrictedLocalFilesystemUserID(std::optional<UUID> user_id);

/// Whether queries running in `context` belong to a restricted connection. The engine's own contexts have no user.
bool isLocalFilesystemAccessRestricted(const ContextPtr & context);

/// Throws ACCESS_DENIED for `operation` if `isLocalFilesystemAccessRestricted(context)`.
void checkLocalFilesystemAccess(const ContextPtr & context, std::string_view operation);

/// Throws ACCESS_DENIED on a restricted connection for a dictionary whose source reads local files or runs a program
/// (FILE, EXECUTABLE, EXECUTABLE_POOL, LIBRARY, ODBC, JDBC, YAMLREGEXPTREE) or whose layout stores data at a path
/// (SSD_CACHE). Names are matched case-insensitively.
void checkDictionaryLocalFilesystemAccess(const ContextPtr & context, std::string_view source, std::string_view layout);

/// A CLICKHOUSE dictionary source in the same process runs its query as the USER it names, `default` unless given.
/// On a restricted connection it must run as the connection's own user: a missing USER is set to it, another one is
/// refused.
void restrictClickHouseDictionarySourceUser(const ContextPtr & context, ASTFunctionWithKeyValueArguments & source);

}
