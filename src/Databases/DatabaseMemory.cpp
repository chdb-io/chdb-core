#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/quoteString.h>
#include <Storages/IStorage.h>
#include <Core/Settings.h>
#include <Core/UUID.h>

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
}

DatabaseMemory::DatabaseMemory(const String & name_, ContextPtr context_)
: DatabaseMemory(
    name_,
    /// TEMPORARY_DATABASE must have Nil UUID (this is expected by the codebase)
    /// Other databases get a generated UUID to satisfy the assertion in DatabaseWithOwnTablesBase::shutdown()
    name_ == DatabaseCatalog::TEMPORARY_DATABASE ? UUIDHelpers::Nil : UUIDHelpers::generateV4(),
    context_)
{
}


DatabaseMemory::DatabaseMemory(const String & name_, UUID uuid, ContextPtr context_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")", context_)
    , data_path(
          DatabaseCatalog::getDataDirPath(
              (name_ != DatabaseCatalog::TEMPORARY_DATABASE && context_ && context_->getChdbNamespace())
                  ? (context_->getChdbNamespace()->catalog_prefix + name_)
                  : name_)
          / "")
    , db_uuid(uuid)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseMemory::DatabaseMemory");
    /// Temporary database should not have any data at the moment of its creation.
    /// In case of starting up after sudden server shutdown, remove the database folder of the temporary database.
    if (name_ == DatabaseCatalog::TEMPORARY_DATABASE)
        removeDataPath(context_);
}

void DatabaseMemory::createTable(
    ContextPtr /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    ensurePopulated();
    std::lock_guard lock{mutex};

    /// A detached table keeps its definition and dependencies under its name, only a short `ATTACH` may reuse it.
    const auto * create_query = query ? query->as<ASTCreateQuery>() : nullptr;
    if (!create_query || !create_query->attach_short_syntax)
        checkMetadataFilenameAvailabilityUnlocked(table_name);

    attachTableUnlocked(table_name, table);

    /// Clean the query from temporary flags.
    ASTPtr query_to_store = query;
    if (query)
    {
        query_to_store = query->clone();
        auto * create = query_to_store->as<ASTCreateQuery>();
        if (!create)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", query->formatForErrorMessage());
        cleanupObjectDefinitionFromTemporaryFlags(*create);
    }

    create_queries.emplace(table_name, query_to_store);
}

void DatabaseMemory::dropTable(
    ContextPtr /*context*/,
    const String & table_name,
    bool /*sync*/)
{
    ensurePopulated();
    StoragePtr table;
    {
        std::lock_guard lock{mutex};
        table = detachTableUnlocked(table_name);
    }
    try
    {
        /// Remove table without lock since
        /// - it does not require it
        /// - it may cause lock-order-inversion if underlying storage need to resolve tables
        table->drop();

        if (table->storesDataOnDisk())
        {
            auto metdata_disk = getDisk();
            metdata_disk->removeRecursive(getTableDataPath(table_name));
        }
    }
    catch (...)
    {
        std::lock_guard lock{mutex};
        attachTableUnlocked(table_name, table);
        throw;
    }

    std::lock_guard lock{mutex};
    table->is_dropped = true;
    create_queries.erase(table_name);
    snapshot_detached_tables.erase(table_name);
    UUID table_uuid = table->getStorageID().uuid;
    if (table_uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().removeUUIDMappingFinally(table_uuid);
}

/// A Memory database has a UUID, so `CREATE TABLE ... AS SELECT` and `CREATE OR REPLACE TABLE` populate a
/// temporary table and publish it under the final name with RENAME or EXCHANGE. Only renames inside the
/// same database are supported. Tables that keep data on disk move it with the name.
void DatabaseMemory::renameTable(
    ContextPtr local_context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool exchange,
    bool dictionary)
{
    if (&to_database != this)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables out of a Memory database is not supported");

    auto component_guard = Coordination::setCurrentComponent("DatabaseMemory::renameTable");
    ensurePopulated();

    StoragePtr table = getTable(table_name, local_context);
    StoragePtr other_table = exchange ? getTable(to_table_name, local_context) : nullptr;
    for (const auto & storage : {table, other_table})
    {
        if (storage && storage->isDictionary() != dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE DICTIONARY for dictionaries and RENAME/EXCHANGE TABLE for tables");
    }

    const auto lock_timeout = local_context->getSettingsRef()[Setting::lock_acquire_timeout];
    TableExclusiveLockHolder table_lock;
    TableExclusiveLockHolder other_table_lock;
    if (table->storesDataOnDisk())
        table_lock = table->lockExclusively(local_context->getCurrentQueryId(), lock_timeout);
    if (other_table && other_table->storesDataOnDisk())
        other_table_lock = other_table->lockExclusively(local_context->getCurrentQueryId(), lock_timeout);

    auto renamed_id = [](const StoragePtr & storage, const String & name)
    {
        StorageID id = storage->getStorageID();
        id.table_name = name;
        return id;
    };

    std::lock_guard lock{mutex};
    auto it = tables.find(table_name);
    if (it == tables.end() || it->second != table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(database_name), backQuote(table_name));

    auto query_it = create_queries.find(table_name);
    ASTPtr query = query_it != create_queries.end() ? query_it->second : nullptr;

    if (!exchange)
    {
        if (tables.contains(to_table_name))
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists", backQuote(database_name), backQuote(to_table_name));
        checkMetadataFilenameAvailabilityUnlocked(to_table_name);

        table->rename(getTableDataPath(to_table_name), renamed_id(table, to_table_name));
        tables.erase(it);
        tables.emplace(to_table_name, table);
        create_queries.erase(table_name);
        if (query)
        {
            query = query->clone();
            query->as<ASTCreateQuery &>().setTable(to_table_name);
        }
        create_queries.emplace(to_table_name, query);
        return;
    }

    auto other_it = tables.find(to_table_name);
    if (other_it == tables.end() || other_it->second != other_table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(database_name), backQuote(to_table_name));

    auto other_query_it = create_queries.find(to_table_name);
    ASTPtr other_query = other_query_it != create_queries.end() ? other_query_it->second : nullptr;

    /// The data directories trade places, so move the first one out of the way.
    const String scratch_path = getTableDataPath("_exchange_" + toString(UUIDHelpers::generateV4()));
    table->rename(scratch_path, renamed_id(table, to_table_name));
    other_table->rename(getTableDataPath(table_name), renamed_id(other_table, table_name));
    table->rename(getTableDataPath(to_table_name), renamed_id(table, to_table_name));

    std::swap(it->second, other_it->second);
    if (query)
    {
        query = query->clone();
        query->as<ASTCreateQuery &>().setTable(to_table_name);
    }
    if (other_query)
    {
        other_query = other_query->clone();
        other_query->as<ASTCreateQuery &>().setTable(table_name);
    }
    create_queries.erase(table_name);
    create_queries.erase(to_table_name);
    create_queries.emplace(table_name, other_query);
    create_queries.emplace(to_table_name, query);
}

ASTPtr DatabaseMemory::getCreateDatabaseQueryImpl() const
{
    auto create_query = make_intrusive<ASTCreateQuery>();
    create_query->setDatabase(database_name);
    create_query->set(create_query->storage, make_intrusive<ASTStorage>());
    auto engine = makeASTFunction(getEngineName());
    engine->setNoEmptyArgs(true);
    create_query->storage->set(create_query->storage->engine, engine);

    if (!comment.empty())
        create_query->set(create_query->comment, make_intrusive<ASTLiteral>(comment));

    return create_query;
}

ASTPtr DatabaseMemory::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    ensurePopulated();
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_name);
    if (it == create_queries.end() || !it->second)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "There is no metadata of table {} in database {}", table_name, database_name);
        return {};
    }
    return it->second->clone();
}

void DatabaseMemory::checkMetadataFilenameAvailability(const String & table_name) const
{
    ensurePopulated();
    std::lock_guard lock{mutex};
    checkMetadataFilenameAvailabilityUnlocked(table_name);
}

void DatabaseMemory::checkMetadataFilenameAvailabilityUnlocked(const String & table_name) const
{
    if (snapshot_detached_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists (detached)",
                        backQuote(database_name), backQuote(table_name));
}

UUID DatabaseMemory::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseMemory::removeDataPath(ContextPtr)
{
    /// This method is called in two cases:
    /// 1. During startup for the temporary database (_temporary_and_external_tables) to clean up
    ///    stale directories from previous server sessions (e.g., after crash or Ctrl+C).
    ///    Temporary tables with disk-based engines (like MergeTree) may leave behind files that
    ///    need to be removed.
    /// 2. On explicit DROP DATABASE to remove all data.
    ///
    /// We must use removeRecursive() instead of removeDirectoryIfExists() because the directory
    /// may contain files from temporary tables. Using removeDirectoryIfExists()
    /// would fail or throw an exception if the directory is not empty.
    auto db_disk = getDisk();
    db_disk->removeRecursive(data_path);
}

void DatabaseMemory::drop(ContextPtr local_context)
{
    /// Remove data on explicit DROP DATABASE
    removeDataPath(local_context);
}

void DatabaseMemory::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, const bool validate_new_create_query)
{
    ensurePopulated();
    ASTPtr create_query;
    {
        std::lock_guard lock{mutex};
        auto it = tables.find(table_id.table_name);
        if (it == tables.end() || (table_id.uuid != UUIDHelpers::Nil && it->second->getStorageID().uuid != table_id.uuid))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exist", table_id.getNameForLogs());

        auto it_query = create_queries.find(table_id.table_name);
        if (it_query == create_queries.end() || !it_query->second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot alter: There is no metadata of table {}", table_id.getNameForLogs());

        create_query = it_query->second->clone();
    }

    /// Apply metadata changes to the cloned AST without holding a lock to avoid possible deadlock
    /// (i.e. when ALTER contains IN (table)).
    applyMetadataChangesToCreateQuery(create_query, metadata, local_context, validate_new_create_query);

    /// The create query of the table has been just changed, we need to update dependencies too.
    auto ref_dependencies = getDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), create_query, local_context->getCurrentDatabase());
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), create_query, local_context->getCurrentDatabase());
    DatabaseCatalog::instance().checkTableCanBeAddedWithNoCyclicDependencies(table_id.getQualifiedName(), ref_dependencies.dependencies, loading_dependencies);

    {
        std::lock_guard lock{mutex};
        create_queries[table_id.table_name] = create_query;
    }

    DatabaseCatalog::instance().updateDependencies(table_id, ref_dependencies.dependencies, loading_dependencies, ref_dependencies.mv_from_dependency ? TableNamesSet{ref_dependencies.mv_from_dependency->getQualifiedName()} : TableNamesSet{});
}

std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseMemory::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    /// We need a special processing for the temporary database.
    if (getDatabaseName() != DatabaseCatalog::TEMPORARY_DATABASE)
        return DatabaseWithOwnTablesBase::getTablesForBackup(filter, local_context);

    std::vector<std::pair<ASTPtr, StoragePtr>> res;

    /// `this->tables` for the temporary database doesn't contain real names of tables.
    /// That's why we need to call Context::getExternalTables() and then resolve those names using tryResolveStorageID() below.
    auto external_tables = local_context->getExternalTables();

    for (const auto & [table_name, storage] : external_tables)
    {
        if (!filter(table_name))
            continue;

        auto storage_id = local_context->tryResolveStorageID(StorageID{"", table_name}, Context::ResolveExternal);
        if (!storage_id)
        {
            LOG_WARNING(log, "Couldn't resolve the name of temporary table {}", backQuoteIfNeed(table_name));
            continue;
        }

        /// Here `storage_id.table_name` looks like looks like "_tmp_ab9b15a3-fb43-4670-abec-14a0e9eb70f1"
        /// it's not the real name of the table.
        auto create_table_query = tryGetCreateTableQuery(storage_id.table_name, local_context);
        if (!create_table_query)
        {
            LOG_WARNING(log, "Couldn't get a create query for temporary table {}", backQuoteIfNeed(table_name));
            continue;
        }

        auto * create = create_table_query->as<ASTCreateQuery>();
        if (create->getTable() != table_name)
        {
            /// Probably the database has been just renamed. Use the older name for backup to keep the backup consistent.
            LOG_WARNING(log, "Got a create query with unexpected name {} for temporary table {}",
                        backQuoteIfNeed(create->getTable()), backQuoteIfNeed(table_name));
            create_table_query = create_table_query->clone();
            create = create_table_query->as<ASTCreateQuery>();
            create->setTable(table_name);
        }

        chassert(storage);
        storage->applyMetadataChangesToCreateQueryForBackup(create_table_query);
        res.emplace_back(create_table_query, storage);
    }

    return res;
}

void registerDatabaseMemory(DatabaseFactory & factory);
void registerDatabaseMemory(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        return make_shared<DatabaseMemory>(
            args.database_name,
            args.context);
    };
    factory.registerDatabase("Memory", create_fn, SecretArgumentsSpec{}, {}, Documentation{
        .description = R"DOCS_MD(
The `Memory` database engine keeps its metadata and table definitions only in memory. It is intended for temporary databases: the database and its tables are lost when the server stops or restarts.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE temporary_data
ENGINE = Memory;
```

## Usage {#usage}

Create and use tables as in an [`Atomic`](/reference/engines/database-engines/atomic) database:

```sql
CREATE TABLE temporary_data.events
(
    id UInt64,
    name String
)
ENGINE = Memory;

INSERT INTO temporary_data.events VALUES (1, 'started');
```

Do not use this engine for data or definitions that must survive a restart. Use `Atomic`, the default open-source database engine, for persistent database metadata.

## See also {#see-also}

- [Atomic database engine](/reference/engines/database-engines/atomic)
- [`Memory` table engine](/reference/engines/table-engines/special/memory)
)DOCS_MD",
        .syntax = "ENGINE = Memory",
        .related = {"Atomic"}});
}

}
