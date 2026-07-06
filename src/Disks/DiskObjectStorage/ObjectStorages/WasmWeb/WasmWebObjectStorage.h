#pragma once

#if defined(OS_WASM)

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/HTTPHeaderEntries.h>

namespace DB
{

struct WasmWebObjectStorageSettings
{
    /// http(s) URL prefix that object keys are appended to, without a trailing
    /// slash. For S3 this is the (virtual-hosted or path-style) bucket root.
    String base_url;

    /// When access_key_id is set, every request carries AWS SigV4 headers
    /// (computed per request — the signature covers the method and expires).
    String region;
    String access_key_id;
    String secret_access_key;
    String session_token;

    /// Extra headers attached to every request (e.g. a vended Bearer token).
    HTTPHeaderEntries static_headers;
};

/// Read-only object storage for the WASM build: objects are fetched over
/// http(s) through the host JavaScript environment (ReadBufferFromWebFetch /
/// WasmHTTPBridge), since neither raw sockets nor the AWS SDK exist here.
/// Serves the data plane of data-lake tables (Iceberg/Paimon/DeltaLake) whose
/// object keys are known from catalog + table metadata, so listing is not
/// required; writes and listing throw NOT_IMPLEMENTED.
class WasmWebObjectStorage : public IObjectStorage
{
public:
    explicit WasmWebObjectStorage(WasmWebObjectStorageSettings settings_);

    std::string getName() const override { return "WasmWeb"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Web; }

    std::string getCommonKeyPrefix() const override { return ""; }

    std::string getDescription() const override { return settings.base_url; }

    bool isReadOnly() const override { return true; }

    bool isRemote() const override { return true; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override {}

    void startup() override {}

    String getObjectsNamespace() const override { return ""; }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    ReadSettings patchSettings(const ReadSettings & read_settings) const override;

private:
    String urlFor(const std::string & path) const;
    HTTPHeaderEntries headersFor(const String & method, const String & url) const;
    [[noreturn]] void throwReadOnly() const;

    WasmWebObjectStorageSettings settings;
};

}

#endif
