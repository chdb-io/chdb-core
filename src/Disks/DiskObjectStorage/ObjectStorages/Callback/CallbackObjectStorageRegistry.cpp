#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageRegistry.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CallbackObjectStorageRegistry & CallbackObjectStorageRegistry::instance()
{
    static CallbackObjectStorageRegistry registry;
    return registry;
}

void CallbackObjectStorageRegistry::add(const String & name, CallbackObjectStorageOpsPtr ops)
{
    std::lock_guard lock(mutex);
    if (storages.contains(name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Callback object storage '{}' is already registered", name);

    if (auto it = retired.find(name); it != retired.end())
    {
        if (auto previous = it->second.lock(); previous && previous->hasAttachedDisks())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Callback object storage '{}' cannot be registered again while disks created from its previous registration "
                "are still open; close every connection first",
                name);
        retired.erase(it);
    }
    storages.emplace(name, std::move(ops));
}

void CallbackObjectStorageRegistry::remove(const String & name)
{
    CallbackObjectStorageOpsPtr ops;
    {
        std::lock_guard lock(mutex);
        auto it = storages.find(name);
        if (it == storages.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Callback object storage '{}' is not registered", name);
        ops = std::move(it->second);
        storages.erase(it);
        retired[name] = ops;
    }
    /// Outside the lock: the wait lasts as long as the slowest callback in flight.
    ops->fence();
}

CallbackObjectStorageOpsPtr CallbackObjectStorageRegistry::tryGet(const String & name) const
{
    std::lock_guard lock(mutex);
    auto it = storages.find(name);
    return it == storages.end() ? nullptr : it->second;
}

}
