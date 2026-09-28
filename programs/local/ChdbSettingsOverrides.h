#pragma once

#include <Interpreters/Context_fwd.h>

namespace CHDB
{

/// chdb-owned adjustments to the settings ClickHouse runs local queries with.
///
/// Upstream's applySettingsOverridesForLocal() (programs/local/LocalServer.cpp)
/// encodes what clickhouse-local wants; this encodes what an *embedded* engine
/// wants, which is not always the same thing: chdb has no administrator to tune
/// a server, and its callers frequently run one query per process, so defaults
/// that a resident server amortizes are paid on every query here.
///
/// It lives in a chdb-owned translation unit for a maintenance reason: every
/// chdb-specific line added to LocalServer.cpp - a file that tracks upstream -
/// has to be re-resolved on each version sync. Keeping the logic here leaves
/// that file with a one-line diff, and makes both entry points (the v2
/// connection API via EmbeddedServer, and query_stable() / chdb_query_cmdline()
/// / the standalone binary via LocalServer) share one implementation instead of
/// drifting apart, as they did while the multi-NUMA max_threads cap existed
/// only in the EmbeddedServer copy.
///
/// Call right after upstream's applySettingsOverridesForLocal(), before
/// setDefaultProfiles() copies the context.
void applySettingsOverridesForChdb(DB::ContextMutablePtr context);

}
