#include "QueryClassifier.h"

#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTDropResourceQuery.h>
#include <Parsers/ASTDropWorkloadQuery.h>
#include <Parsers/ASTHypotheticalIndexQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowEngineQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTSnapshotQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/Access/ASTShowPrivilegesQuery.h>

namespace CHDB
{

namespace
{

using namespace DB;

/// Statements whose effect is persistent and worth replaying, but which no
/// `BACKUP DATABASE` can carry, because what they change lives beside the
/// databases rather than inside one. Several of these parse as CREATE or DROP
/// and would otherwise be indistinguishable from a table definition.
bool isGlobalMutatingStatement(const IAST & ast)
{
    return
        /// Global UDFs: an SQL lambda, a WebAssembly function, a driver-backed
        /// function, or a DROP of any of them.
        ast.as<ASTCreateSQLFunctionQuery>() || ast.as<ASTCreateWasmFunctionQuery>()
        || ast.as<ASTCreateFunctionWithDriverQuery>() || ast.as<ASTDropFunctionQuery>()
        /// Named collections.
        || ast.as<ASTCreateNamedCollectionQuery>() || ast.as<ASTDropNamedCollectionQuery>()
        || ast.as<ASTAlterNamedCollectionQuery>()
        /// Workload / resource scheduling objects.
        || ast.as<ASTCreateWorkloadQuery>() || ast.as<ASTDropWorkloadQuery>() || ast.as<ASTCreateResourceQuery>()
        || ast.as<ASTDropResourceQuery>()
        /// Access management.
        || ast.as<ASTCreateUserQuery>() || ast.as<ASTCreateRoleQuery>() || ast.as<ASTCreateQuotaQuery>()
        || ast.as<ASTCreateRowPolicyQuery>() || ast.as<ASTCreateMaskingPolicyQuery>()
        || ast.as<ASTCreateSettingsProfileQuery>() || ast.as<ASTDropAccessEntityQuery>()
        || ast.as<ASTMoveAccessEntityQuery>() || ast.as<ASTGrantQuery>() || ast.as<ASTCheckGrantQuery>();
}

/// Statements that either only touch the session, so replaying them is
/// meaningless, or that a managed connection should not be issuing at all.
bool isControlStatement(const IAST & ast)
{
    return ast.as<ASTUseQuery>() || ast.as<ASTSetQuery>() || ast.as<ASTSetRoleQuery>() || ast.as<ASTSystemQuery>()
        || ast.as<ASTBackupQuery>() || ast.as<ASTSnapshotQuery>() || ast.as<ASTKillQueryQuery>()
        || ast.as<ASTTransactionControl>() || ast.as<ASTExecuteAsQuery>()
        /// WATCH streams a live view for as long as the session lasts; it ends
        /// no more than a SYSTEM statement does.
        || ast.as<ASTWatchQuery>()
        /// Advisor state for EXPLAIN, held outside the databases and outside
        /// any backup.
        || ast.as<ASTHypotheticalIndexQuery>();
}

/// The `system` database is engine-owned: its storage engine refuses backups
/// outright, so a write landing there survives nothing. It is also how some
/// global objects get their payload -- a WebAssembly module arrives as
/// `INSERT INTO system.wasm_modules`, and the bytes are written to the user
/// scripts directory, not to any database. Treat writes there as global.
bool writesToSystemDatabase(const IAST & ast)
{
    if (const auto * insert = ast.as<ASTInsertQuery>())
        return insert->getDatabase() == DatabaseCatalog::SYSTEM_DATABASE;

    if (const auto * with_table = dynamic_cast<const ASTQueryWithTableAndOutput *>(&ast))
        return with_table->getDatabase() == DatabaseCatalog::SYSTEM_DATABASE;

    return false;
}

/// Reads that ClickHouse models as their own AST rather than through
/// QueryKind, so they would otherwise fall through to QueryKind::None.
bool isReadOnlyStatement(const IAST & ast)
{
    return ast.as<ASTShowProcesslistQuery>() || ast.as<ASTShowEnginesQuery>() || ast.as<ASTShowAccessQuery>()
        || ast.as<ASTShowPrivilegesQuery>() || ast.as<ASTDescribeCacheQuery>();
}

chdb_query_class classifyByQueryKind(IAST::QueryKind kind)
{
    switch (kind)
    {
        case IAST::QueryKind::Select:
        case IAST::QueryKind::Show:
        case IAST::QueryKind::Exists:
        case IAST::QueryKind::Describe:
        case IAST::QueryKind::Explain:
        case IAST::QueryKind::Check:
            return CHDB_QUERY_READ_ONLY;

        case IAST::QueryKind::Insert:
        case IAST::QueryKind::Delete:
        case IAST::QueryKind::Update:
        case IAST::QueryKind::Create:
        case IAST::QueryKind::Drop:
        case IAST::QueryKind::Undrop:
        case IAST::QueryKind::Rename:
        case IAST::QueryKind::Optimize:
        case IAST::QueryKind::Alter:
        case IAST::QueryKind::Copy:
            return CHDB_QUERY_MUTATING;

        /// Reached only if an access statement grows an AST that
        /// isGlobalMutatingStatement() has not been taught yet; the class is
        /// the same either way.
        case IAST::QueryKind::Grant:
        case IAST::QueryKind::Revoke:
        case IAST::QueryKind::Move:
            return CHDB_QUERY_MUTATING_GLOBAL;

        case IAST::QueryKind::System:
        case IAST::QueryKind::Set:
        case IAST::QueryKind::Use:
        case IAST::QueryKind::Backup:
        case IAST::QueryKind::Restore:
        case IAST::QueryKind::KillQuery:
        case IAST::QueryKind::ExternalDDL:
        case IAST::QueryKind::Begin:
        case IAST::QueryKind::Commit:
        case IAST::QueryKind::Rollback:
        case IAST::QueryKind::SetTransactionSnapshot:
        case IAST::QueryKind::AsyncInsertFlush:
        case IAST::QueryKind::Snapshot:
            return CHDB_QUERY_CONTROL;

        /// ParallelWithQuery is folded over its children before we get here;
        /// reaching it means the fold missed a shape. None is either a
        /// statement ClickHouse never gave a kind or one added since this
        /// switch was written. Both refuse.
        case IAST::QueryKind::ParallelWithQuery:
        case IAST::QueryKind::None:
            return CHDB_QUERY_UNKNOWN;
    }
    return CHDB_QUERY_UNKNOWN;
}

}

bool statementHasSecrets(const IAST & ast)
{
    return ast.hasSecretParts();
}

chdb_query_class classifyStatement(const IAST & ast)
{
    /// `stmt1 PARALLEL WITH stmt2` is as restricted as its most restricted arm.
    if (ast.as<ASTParallelWithQuery>())
    {
        if (ast.children.empty())
            return CHDB_QUERY_UNKNOWN;

        chdb_query_class result = CHDB_QUERY_READ_ONLY;
        for (const auto & child : ast.children)
        {
            if (!child)
                return CHDB_QUERY_UNKNOWN;
            result = combineQueryClass(result, classifyStatement(*child));
        }
        return result;
    }

    /// INTO OUTFILE turns any read into a filesystem write, which no database
    /// checkpoint carries. `as<>` is an exact-type match, so this one asks for
    /// the base: every statement that can carry an out_file derives from it.
    if (const auto * with_output = dynamic_cast<const ASTQueryWithOutput *>(&ast); with_output && with_output->out_file)
        return CHDB_QUERY_CONTROL;

    if (isControlStatement(ast))
        return CHDB_QUERY_CONTROL;

    if (isReadOnlyStatement(ast))
        return CHDB_QUERY_READ_ONLY;

    if (isGlobalMutatingStatement(ast))
        return CHDB_QUERY_MUTATING_GLOBAL;

    /// ATTACH shares ASTCreateQuery with CREATE, and DETACH shares ASTDropQuery
    /// with DROP; both attach an object rather than define one.
    if (const auto * create = ast.as<ASTCreateQuery>(); create && create->attach)
        return CHDB_QUERY_CONTROL;

    if (const auto * drop = ast.as<ASTDropQuery>(); drop && drop->kind == ASTDropQuery::Kind::Detach)
        return CHDB_QUERY_CONTROL;

    /// `INSERT INTO FUNCTION file(...)/s3(...)/remote(...)` writes somewhere
    /// other than the database being backed up.
    if (const auto * insert = ast.as<ASTInsertQuery>(); insert && insert->table_function)
        return CHDB_QUERY_CONTROL;

    auto result = CHDB_QUERY_UNKNOWN;
    if (const auto * alter = ast.as<ASTAlterQuery>())
    {
        /// FREEZE/UNFREEZE hardlink parts outside the data directory, FETCH
        /// pulls a part from a replica, MOVE PARTITION TO DISK relocates
        /// storage: filesystem and cluster state, not table content.
        if (alter->isFreezeAlter() || alter->isUnlockSnapshot() || alter->isFetchAlter()
            || alter->isMovePartitionToDiskOrVolumeAlter())
            return CHDB_QUERY_CONTROL;

        if (alter->alter_object == ASTAlterQuery::AlterObjectType::UNKNOWN)
            return CHDB_QUERY_UNKNOWN;

        result = CHDB_QUERY_MUTATING;
    }
    else
    {
        result = classifyByQueryKind(ast.getQueryKind());
    }

    /// A write is only MUTATING if a checkpoint of the database would hold it.
    /// This has to sit after the ALTER branch as well as after the QueryKind
    /// fallback: `ALTER TABLE system.x` reaches the database no backup covers
    /// just as surely as `INSERT INTO system.x` does.
    if (result == CHDB_QUERY_MUTATING && writesToSystemDatabase(ast))
        return CHDB_QUERY_MUTATING_GLOBAL;

    return result;
}

}
