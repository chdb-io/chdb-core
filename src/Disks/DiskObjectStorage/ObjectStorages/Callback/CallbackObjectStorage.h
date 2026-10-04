#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace DB
{

/// An object storage whose blobs live in the host process, reached through `CallbackObjectStorageOps`.
/// It is meant to sit under the `plain_rewritable` metadata storage, so it only offers flat opaque keys.
///
/// `isRemote()` is true: callbacks are an opaque, possibly slow call-out, and `DiskObjectStorage` reports
/// itself remote whatever its storage says, so MergeTree already treats the disk as remote (no fsync of
/// part files or directories; the orphan-parts scan skips this disk because disk(...) marks it custom,
/// not because it is remote). The flag only changes how `plain_rewritable` detects an empty store,
/// which then uses `existsOrHasAnyChild` instead of a full listing.
class CallbackObjectStorage : public IObjectStorage
{
public:
    CallbackObjectStorage(String disk_name_, String key_prefix_, CallbackObjectStorageOpsPtr ops_);

    std::string getName() const override { return "Callback"; }
    std::string getDiskName() const override { return disk_name; }
    ObjectStorageType getType() const override { return ObjectStorageType::Callback; }
    std::string getCommonKeyPrefix() const override { return key_prefix; }
    std::string getDescription() const override { return "callback:" + ops->name; }
    bool isRemote() const override { return true; }

    bool exists(const StoredObject & object) const override;
    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;
    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;
    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        bool use_external_buffer = false,
        bool restrict_seek = false) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObjectIfExists(const StoredObject & object) override;
    void removeObjectsIfExist(const StoredObjects & objects, StoredObjects * successful_objects = nullptr) override; /// NOLINT

    String copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override { }
    void startup() override {}

    String getObjectsNamespace() const override { return ""; }
    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

private:
    const String disk_name;
    const String key_prefix;
    const CallbackObjectStorageOpsPtr ops;
};

}
