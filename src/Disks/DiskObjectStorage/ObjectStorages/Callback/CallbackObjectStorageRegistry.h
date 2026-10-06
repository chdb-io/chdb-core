#pragma once

#include <mutex>
#include <unordered_map>

#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>

namespace DB
{

/// Process-global table of host callbacks, looked up by `storage_name` when a callback disk is created.
class CallbackObjectStorageRegistry
{
public:
    static CallbackObjectStorageRegistry & instance();

    /// Throws BAD_ARGUMENTS if the name is registered, or if disks made from an earlier registration under
    /// it are still open: a disk keeps the table it was created with, and the engine reuses a disk for
    /// every disk(...) with the same arguments, so new tables would land on the fenced one. Disks close
    /// when the last connection closes.
    void add(const String & name, CallbackObjectStorageOpsPtr ops);
    /// Removes the name and fences its table (CallbackObjectStorageOps::fence): once this returns, no
    /// callback of that registration runs again. Throws BAD_ARGUMENTS if the name is not registered.
    void remove(const String & name);
    CallbackObjectStorageOpsPtr tryGet(const String & name) const;

private:
    mutable std::mutex mutex;
    std::unordered_map<String, CallbackObjectStorageOpsPtr> storages;
    /// Unregistered tables, alive for as long as a disk object still holds them.
    std::unordered_map<String, std::weak_ptr<CallbackObjectStorageOps>> retired;
};

}
