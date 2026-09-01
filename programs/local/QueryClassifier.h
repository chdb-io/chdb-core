#pragma once

#include "chdb.h"

#include <Parsers/IAST_fwd.h>

namespace CHDB
{

/// What one parsed statement does to state that outlives it. See the
/// chdb_query_class docs in chdb.h for the rule the mapping follows.
chdb_query_class classifyStatement(const DB::IAST & ast);

/// Whether the statement carries a credential in its text -- a named
/// collection's key, a user's password, a table function's access key.
/// ClickHouse already tracks this so that SHOW CREATE can print `[HIDDEN]`;
/// a caller logging statements needs the same answer for the opposite
/// reason, to keep the text out of durable storage.
bool statementHasSecrets(const DB::IAST & ast);

/// The class of a batch is the class of its most restricted statement, which
/// the chdb_query_class ordering makes a plain maximum.
inline chdb_query_class combineQueryClass(chdb_query_class lhs, chdb_query_class rhs)
{
    return lhs > rhs ? lhs : rhs;
}

}
