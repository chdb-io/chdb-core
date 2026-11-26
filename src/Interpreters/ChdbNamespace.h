#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <string_view>
#include <vector>

namespace DB
{

class IDisk;
class IStoragePolicy;
class IUserDefinedSQLObjectsStorage;

/// Per-`chdb_database` catalog and storage namespace descriptor shared by a
/// `chdb_database` instance and all `Session` / `Context` objects created on it.
///
/// - In-memory (`is_memory == true`): isolates `"default"` and user-created
///   databases under `catalog_prefix + logical_name` in the single process-wide
///   `DatabaseCatalog`, backed by `DatabaseMemory` without creating `/tmp` dirs.
/// - Persistent directory (`is_memory == false`): isolates `"default"` and user
///   databases under `catalog_prefix + logical_name`, backed by a dedicated
///   `DiskLocal` + `StoragePolicy` + `UserDefinedSQLObjectsDiskStorage` on `data_path`.
struct ChdbNamespace
{
    static constexpr std::string_view INTERNAL_PREFIX_ROOT = "__chdb_ns_";

    String catalog_prefix;
    String data_path;
    String disk_name;
    String policy_name;
    bool is_memory = true;
    std::shared_ptr<IDisk> disk;
    std::shared_ptr<const IStoragePolicy> storage_policy;
    std::shared_ptr<IUserDefinedSQLObjectsStorage> sql_objects_storage;
    std::vector<String> backups_allowed_paths;

    static bool isInternalCatalogName(std::string_view name) noexcept
    {
        return name.starts_with(INTERNAL_PREFIX_ROOT);
    }

    bool ownsCatalogName(std::string_view catalog_name) const noexcept
    {
        return !catalog_prefix.empty() && catalog_name.starts_with(catalog_prefix);
    }

    std::string_view stripPrefix(std::string_view catalog_name) const noexcept
    {
        if (ownsCatalogName(catalog_name))
            return catalog_name.substr(catalog_prefix.size());
        return catalog_name;
    }
};

using ChdbNamespacePtr = std::shared_ptr<ChdbNamespace>;

/// Scoped thread-local active namespace override for mount/unmount and async
/// metadata-loading tasks where `CurrentThread::tryGetQueryContext()` is null.
class ChdbNamespaceScope
{
public:
    explicit ChdbNamespaceScope(const ChdbNamespace * ns) noexcept;
    explicit ChdbNamespaceScope(const ChdbNamespacePtr & ns) noexcept
        : ChdbNamespaceScope(ns.get())
    {
    }
    ~ChdbNamespaceScope() noexcept;

    ChdbNamespaceScope(const ChdbNamespaceScope &) = delete;
    ChdbNamespaceScope & operator=(const ChdbNamespaceScope &) = delete;

    static const ChdbNamespace * tryGetThreadOverride() noexcept;
    static const ChdbNamespace * tryGetActive(const ContextPtr & explicit_context = nullptr) noexcept;

private:
    const ChdbNamespace * previous = nullptr;
    bool active = false;
};

}
