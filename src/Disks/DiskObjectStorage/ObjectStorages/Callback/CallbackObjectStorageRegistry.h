#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>

namespace DB
{

/// Process-global table of host callbacks, looked up by `storage_name` when a callback disk is created.
class CallbackObjectStorageRegistry
{
public:
    static CallbackObjectStorageRegistry & instance();

    /// False if the name is already registered.
    bool add(const String & name, CallbackObjectStorageOpsPtr ops);
    /// Also forgets the scratch sweeps done for `name`: a store registered again is a new store.
    bool remove(const String & name);
    CallbackObjectStorageOpsPtr tryGet(const String & name) const;

    /// True the first time it is called for this (name, key_prefix) since registration. The `__tmp`
    /// scratch sweep runs once per keyspace, so a second disk over the same keys cannot delete a
    /// sibling disk's in-flight scratch copy.
    bool markScratchSwept(const String & name, const String & key_prefix);

private:
    mutable std::shared_mutex mutex;
    std::unordered_map<String, CallbackObjectStorageOpsPtr> storages;
    std::unordered_set<String> swept_keyspaces;
};

}
