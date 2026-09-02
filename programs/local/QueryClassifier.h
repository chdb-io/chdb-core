#pragma once

#include "chdb.h"

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace CHDB
{

/// What one parsed statement does to state that outlives it. See the
/// chdb_query_class docs in chdb.h for the rule the mapping follows.
chdb_query_class classifyStatement(const DB::IAST & ast, const String & current_database);

/// Whether the statement carries a credential in its text -- a named
/// collection's key, a user's password, a table function's access key.
/// ClickHouse already tracks this so that SHOW CREATE can print `[HIDDEN]`;
/// a caller logging statements needs the same answer for the opposite
/// reason, to keep the text out of durable storage.
bool statementHasSecrets(const DB::IAST & ast);

/// Whether the statement acts on a database as a whole -- creating, dropping
/// or renaming the container rather than working inside it.
bool changesDatabaseLifecycle(const DB::IAST & ast);

/// Whether every persistent write the statement performs lands in
/// `target_database`. Unqualified names resolve through `current_database`
/// first, the way execution would resolve them.
///
/// False as soon as one write cannot be placed there, and false for writes
/// that leave the engine entirely (a table function, an outfile). A statement
/// that writes nothing is vacuously true.
bool writesOnlyToDatabase(const DB::IAST & ast, const String & target_database, const String & current_database);

/// How many executable statements one parsed top-level AST stands for. One,
/// except for `a PARALLEL WITH b`, where both arms run.
size_t countExecutableStatements(const DB::IAST & ast);

/// The class of a batch is the class of its most restricted statement, which
/// the chdb_query_class ordering makes a plain maximum.
inline chdb_query_class combineQueryClass(chdb_query_class lhs, chdb_query_class rhs)
{
    return lhs > rhs ? lhs : rhs;
}

}
