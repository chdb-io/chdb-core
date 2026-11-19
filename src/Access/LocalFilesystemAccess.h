#pragma once

#include <Interpreters/Context_fwd.h>

#include <string_view>

namespace DB
{

/// Whether queries running in `context` may read and write the local filesystem: true when the user holds both
/// READ and WRITE on the FILE source. The engine's own contexts have no user and always pass.
///
/// chDB connections opened with `restrict_local_filesystem_access` run as a user without these grants. ClickHouse
/// already checks them for `file()`, the `File` engine and similar sources; the call sites of this function cover
/// the file access that is not reached through such a source, such as `INTO OUTFILE` or format schema files.
bool hasLocalFilesystemAccess(const ContextPtr & context);

/// Throws ACCESS_DENIED for `operation` unless `hasLocalFilesystemAccess(context)`.
void checkLocalFilesystemAccess(const ContextPtr & context, std::string_view operation);

/// Throws ACCESS_DENIED for a dictionary whose source reads local files or runs a program (FILE, EXECUTABLE,
/// EXECUTABLE_POOL, LIBRARY, ODBC, JDBC) or whose layout stores data at a path (SSD_CACHE), unless
/// `hasLocalFilesystemAccess(context)`. Names are matched case-insensitively.
void checkDictionaryLocalFilesystemAccess(const ContextPtr & context, std::string_view source, std::string_view layout);

}
