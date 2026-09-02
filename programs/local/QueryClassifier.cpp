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
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>

#include <set>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
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
        /// CHECK GRANT only evaluates an authorization and changes nothing;
        /// its QueryKind::Check already lands it in READ_ONLY.
        || ast.as<ASTMoveAccessEntityQuery>() || ast.as<ASTGrantQuery>();
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

/// Every database a statement writes to, with unqualified names resolved the
/// way execution would resolve them. Returns false when a write leaves the
/// engine entirely -- a table function, an outfile -- because such a write
/// belongs to no database and no caller can claim it lands in theirs.
///
/// `out_databases` is only added to; a false return means "and also somewhere
/// outside", not that the collected set is meaningless.
bool collectWrittenDatabases(const IAST & ast, const String & current_database, std::set<String> & out_databases)
{
    const auto resolve = [&](const String & explicit_database)
    { return explicit_database.empty() ? current_database : explicit_database; };

    if (ast.as<ASTParallelWithQuery>())
    {
        bool contained = true;
        for (const auto & child : ast.children)
        {
            if (!child)
                return false;
            contained &= collectWrittenDatabases(*child, current_database, out_databases);
        }
        return contained;
    }

    /// A read that writes a file writes outside every database.
    if (const auto * with_output = dynamic_cast<const ASTQueryWithOutput *>(&ast); with_output && with_output->out_file)
        return false;

    if (const auto * insert = ast.as<ASTInsertQuery>())
    {
        /// `INSERT INTO FUNCTION file(...)/s3(...)` names a sink, not a table.
        if (insert->table_function)
            return false;
        out_databases.insert(resolve(insert->getDatabase()));
        return true;
    }

    /// RENAME and EXCHANGE touch two objects per element, and either side may
    /// sit in a different database.
    if (const auto * rename = ast.as<ASTRenameQuery>())
    {
        for (const auto & element : rename->getElements())
        {
            out_databases.insert(resolve(element.from.getDatabase()));
            out_databases.insert(resolve(element.to.getDatabase()));
        }
        return true;
    }

    /// `DROP TABLE a, b` keeps its targets in a list rather than in the
    /// statement's own database/table fields.
    if (const auto * drop = ast.as<ASTDropQuery>(); drop && drop->database_and_tables)
    {
        for (const auto & child : drop->database_and_tables->children)
        {
            String database;
            if (const auto * identifier = child->as<ASTTableIdentifier>())
                database = identifier->getDatabaseName();
            out_databases.insert(resolve(database));
        }
        return true;
    }

    /// `ALTER TABLE src MOVE PARTITION p TO TABLE dst.t` writes to two places,
    /// and the destination is on the command rather than on the statement.
    if (const auto * alter = ast.as<ASTAlterQuery>())
    {
        out_databases.insert(resolve(alter->getDatabase()));
        if (alter->command_list)
        {
            for (const auto & child : alter->command_list->children)
            {
                const auto * command = child->as<ASTAlterCommand>();
                if (command && !command->to_table.empty())
                    out_databases.insert(resolve(command->to_database));
            }
        }
        return true;
    }

    if (const auto * with_table = dynamic_cast<const ASTQueryWithTableAndOutput *>(&ast))
    {
        out_databases.insert(resolve(with_table->getDatabase()));
        return true;
    }

    /// A statement whose target this function does not know how to read is a
    /// statement we cannot place. Saying "outside" keeps the caller honest.
    return false;
}

/// Statement-level settings that would take a write off the connection's own
/// synchronous-completion policy. A managed connection configures these once;
/// a statement re-specifying any of them is changing the policy, whichever
/// direction it moves, so the value is not inspected.
bool overridesSynchronousCompletion(const IAST & ast)
{
    if (const auto * set = ast.as<ASTSetQuery>(); set && !set->is_standalone)
    {
        const auto guarded = [](const String & name)
        {
            return name == "async_insert" || name == "wait_for_async_insert" || name == "mutations_sync"
                || name == "alter_sync" || name == "lightweight_deletes_sync";
        };

        for (const auto & change : set->changes)
        {
            if (guarded(change.name))
                return true;
        }
        /// `SETTINGS async_insert = DEFAULT` resets the value the connection
        /// chose, which is the same override by another spelling. The parser
        /// files those under default_settings rather than changes.
        for (const auto & name : set->default_settings)
        {
            if (guarded(name))
                return true;
        }
    }

    for (const auto & child : ast.children)
    {
        if (child && overridesSynchronousCompletion(*child))
            return true;
    }
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
            return CHDB_QUERY_MUTATING;

        /// Reached only if an access statement grows an AST that
        /// isGlobalMutatingStatement() has not been taught yet; the class is
        /// the same either way.
        case IAST::QueryKind::Grant:
        case IAST::QueryKind::Revoke:
        case IAST::QueryKind::Move:
            return CHDB_QUERY_MUTATING_GLOBAL;

        /// `COPY ... TO` writes a file no backup covers. The statement serves
        /// the PostgreSQL wire protocol, which a managed connection does not
        /// speak, so both directions refuse rather than split hairs.
        case IAST::QueryKind::Copy:
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

bool changesDatabaseLifecycle(const IAST & ast)
{
    if (ast.as<ASTParallelWithQuery>())
    {
        for (const auto & child : ast.children)
        {
            if (child && changesDatabaseLifecycle(*child))
                return true;
        }
        return false;
    }

    /// A database statement names a database and no table. ATTACH and DETACH
    /// of a database count too: they change which databases exist.
    if (const auto * create = ast.as<ASTCreateQuery>())
        return create->getTable().empty() && !create->getDatabase().empty();

    if (const auto * drop = ast.as<ASTDropQuery>())
        return drop->getTable().empty() && !drop->getDatabase().empty() && !drop->database_and_tables;

    if (const auto * rename = ast.as<ASTRenameQuery>())
        return rename->database;

    return false;
}

bool writesOnlyToDatabase(const IAST & ast, const String & target_database, const String & current_database)
{
    if (target_database.empty())
        return false;

    /// A statement that writes nothing is vacuously contained. Asking
    /// collectWrittenDatabases() about a SELECT would take the "target I
    /// cannot read" branch and answer no, which is the wrong kind of caution:
    /// there is no write to place.
    if (classifyStatement(ast, current_database) == CHDB_QUERY_READ_ONLY)
        return true;

    std::set<String> written;
    if (!collectWrittenDatabases(ast, current_database, written))
        return false;

    for (const auto & database : written)
    {
        if (database != target_database)
            return false;
    }
    /// A statement that writes nothing is vacuously contained.
    return true;
}

size_t countExecutableStatements(const IAST & ast)
{
    if (!ast.as<ASTParallelWithQuery>())
        return 1;

    /// Both arms of `a PARALLEL WITH b` run, so the text is not one statement
    /// a caller could replay on its own.
    size_t count = 0;
    for (const auto & child : ast.children)
        count += child ? countExecutableStatements(*child) : 1;
    return count;
}

chdb_query_class classifyStatement(const IAST & ast, const String & current_database)
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
            result = combineQueryClass(result, classifyStatement(*child, current_database));
        }
        return result;
    }

    /// A statement-level setting that moves a write off the connection's
    /// synchronous-completion policy is the connection's business, not the
    /// statement's.
    if (overridesSynchronousCompletion(ast))
        return CHDB_QUERY_CONTROL;

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
    /// with DROP; both attach an object rather than define one. A temporary
    /// table shares the AST too, and lives outside every database, so no
    /// backup of one can hold it.
    if (const auto * create = ast.as<ASTCreateQuery>(); create && (create->attach || create->isTemporary()))
        return CHDB_QUERY_CONTROL;

    if (const auto * drop = ast.as<ASTDropQuery>(); drop && drop->isTemporary())
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

    /// A write is only MUTATING if a checkpoint of some database would hold
    /// it. The `system` database is engine-owned and its storage engine
    /// refuses backups, so a write landing there survives nothing -- and an
    /// unqualified name lands there whenever `system` is the current database,
    /// which is exactly how a WebAssembly module gets uploaded.
    ///
    /// This sits after the ALTER branch as well as after the QueryKind
    /// fallback: `ALTER TABLE system.x` reaches that database just as surely
    /// as `INSERT INTO system.x` does.
    if (result == CHDB_QUERY_MUTATING)
    {
        std::set<String> written;
        collectWrittenDatabases(ast, current_database, written);
        if (written.contains(DatabaseCatalog::SYSTEM_DATABASE))
            return CHDB_QUERY_MUTATING_GLOBAL;
    }

    return result;
}

}
