#pragma once

#include <shared_mutex>
#include <unordered_map>

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
    bool remove(const String & name);
    CallbackObjectStorageOpsPtr tryGet(const String & name) const;

private:
    mutable std::shared_mutex mutex;
    std::unordered_map<String, CallbackObjectStorageOpsPtr> storages;
};

}
