#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageRegistry.h>

namespace DB
{

CallbackObjectStorageRegistry & CallbackObjectStorageRegistry::instance()
{
    static CallbackObjectStorageRegistry registry;
    return registry;
}

bool CallbackObjectStorageRegistry::add(const String & name, CallbackObjectStorageOpsPtr ops)
{
    std::unique_lock lock(mutex);
    return storages.emplace(name, std::move(ops)).second;
}

bool CallbackObjectStorageRegistry::remove(const String & name)
{
    std::unique_lock lock(mutex);
    std::erase_if(swept_keyspaces, [&](const String & keyspace) { return keyspace.starts_with(name + '\0'); });
    return storages.erase(name) > 0;
}

CallbackObjectStorageOpsPtr CallbackObjectStorageRegistry::tryGet(const String & name) const
{
    std::shared_lock lock(mutex);
    auto it = storages.find(name);
    return it == storages.end() ? nullptr : it->second;
}

bool CallbackObjectStorageRegistry::markScratchSwept(const String & name, const String & key_prefix)
{
    std::unique_lock lock(mutex);
    return swept_keyspaces.insert(name + '\0' + key_prefix).second;
}

}
