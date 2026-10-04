#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CALLBACK_OBJECT_STORAGE_ERROR;
}

void CallbackObjectStorageOps::check(int code, const char * operation, const String & key) const
{
    if (code == 0)
        return;

    const char * error = last_error ? last_error(ud) : nullptr;
    throw Exception(
        ErrorCodes::CALLBACK_OBJECT_STORAGE_ERROR,
        "Callback object storage '{}': {} of '{}' failed with code {}{}{}",
        name,
        operation,
        key,
        code,
        error && *error ? ": " : "",
        error ? error : "");
}

std::optional<CallbackObjectStat> statCallbackObject(const CallbackObjectStorageOps & ops, const String & key)
{
    int found = 0;
    CallbackObjectStat stat;
    ops.check(ops.metadata(ops.ud, key.c_str(), &found, &stat.size, &stat.mtime), "metadata", key);
    if (!found)
        return std::nullopt;
    return stat;
}

}
