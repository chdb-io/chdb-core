#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorage.h>

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageRegistry.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Callback/ReadBufferFromCallback.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Callback/WriteBufferToCallback.h>
#include <IO/copyData.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

ObjectMetadata toObjectMetadata(const CallbackObjectStat & stat)
{
    ObjectMetadata metadata;
    metadata.size_bytes = stat.size;
    metadata.last_modified = Poco::Timestamp::fromEpochTime(static_cast<std::time_t>(stat.mtime));
    return metadata;
}

struct ListContext
{
    RelativePathsWithMetadata * children;
    size_t max_keys;
    std::exception_ptr error;
};

int collectListed(void * sink_ud, const char * key, uint64_t size, int64_t mtime)
{
    auto & context = *static_cast<ListContext *>(sink_ud);
    try
    {
        context.children->push_back(std::make_shared<RelativePathWithMetadata>(key, toObjectMetadata(CallbackObjectStat{size, mtime})));
        return context.max_keys && context.children->size() >= context.max_keys;
    }
    catch (...)
    {
        context.error = std::current_exception();
        return 1;
    }
}

}

CallbackObjectStorage::CallbackObjectStorage(String disk_name_, String key_prefix_, CallbackObjectStorageOpsPtr ops_)
    : disk_name(std::move(disk_name_))
    , key_prefix(std::move(key_prefix_))
    , ops(std::move(ops_))
    , log(getLogger("CallbackObjectStorage(" + disk_name + ")"))
{
}

bool CallbackObjectStorage::exists(const StoredObject & object) const
{
    int out = 0;
    ops->check(ops->exists(ops->ud, object.remote_path.c_str(), &out), "exists", object.remote_path);
    return out != 0;
}

void CallbackObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    ListContext context{&children, max_keys, nullptr};
    const int code = ops->list(ops->ud, path.c_str(), collectListed, &context);
    if (context.error)
        std::rethrow_exception(context.error);
    ops->check(code, "list", path);
}

ObjectMetadata CallbackObjectStorage::getObjectMetadata(const std::string & path, bool with_tags) const
{
    auto metadata = tryGetObjectMetadata(path, with_tags);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Callback object '{}' does not exist", path);
    return *metadata;
}

std::optional<ObjectMetadata> CallbackObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    auto stat = statCallbackObject(*ops, path);
    if (!stat)
        return std::nullopt;
    return toObjectMetadata(*stat);
}

std::unique_ptr<ReadBufferFromFileBase> CallbackObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    bool use_external_buffer,
    bool /* restrict_seek */) const
{
    return std::make_unique<ReadBufferFromCallback>(
        ops, object.remote_path, patchSettings(read_settings).remote_fs_settings.buffer_size, use_external_buffer, read_hint);
}

std::unique_ptr<WriteBufferFromFileBase> CallbackObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /* attributes */,
    size_t buf_size,
    const WriteSettings & /* write_settings */)
{
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "CallbackObjectStorage doesn't support append to objects");

    return std::make_unique<AutoCanceledWriteBuffer<WriteBufferToCallback>>(ops, object.remote_path, buf_size);
}

void CallbackObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    ops->check(ops->remove(ops->ud, object.remote_path.c_str()), "remove", object.remote_path);
}

void CallbackObjectStorage::removeObjectsIfExist(const StoredObjects & objects, StoredObjects * successful_objects)
{
    for (const auto & object : objects)
    {
        removeObjectIfExists(object);
        if (successful_objects)
            successful_objects->push_back(object);
    }
}

String CallbackObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes>)
{
    if (ops->copy)
    {
        ops->check(ops->copy(ops->ud, object_from.remote_path.c_str(), object_to.remote_path.c_str()), "copy", object_from.remote_path);
        return {};
    }

    auto in = readObject(object_from, read_settings);
    auto out = writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
    return {};
}

ObjectStorageKeyGeneratorPtr CallbackObjectStorage::createKeyGenerator() const
{
    return createObjectStorageKeyGeneratorByPrefix(key_prefix);
}

/// plain_rewritable copies a blob to <key_prefix>/__tmp/<random> before each unlink and deletes the
/// copy only when the operation finalizes, so a crash in between leaves blobs that load() skips by
/// design and nothing else reclaims. The sweep runs after the metadata load and before any
/// transaction (DiskObjectStorage::startupImpl), once per keyspace in this process.
void CallbackObjectStorage::startup()
{
    if (!CallbackObjectStorageRegistry::instance().markScratchSwept(ops->name, key_prefix))
        return;

    const String prefix = std::filesystem::path(key_prefix) / PlainRewritableLayout::SCRATCH_DIRECTORY_TOKEN / "";
    try
    {
        RelativePathsWithMetadata children;
        listObjects(prefix, children, /* max_keys= */ 0);
        StoredObjects objects;
        for (const auto & child : children)
            objects.emplace_back(child->relative_path);
        removeObjectsIfExist(objects);
        LOG_INFO(log, "Swept {} scratch objects under '{}'", objects.size(), prefix);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Could not sweep scratch objects under '{}'", prefix));
    }
}

}

