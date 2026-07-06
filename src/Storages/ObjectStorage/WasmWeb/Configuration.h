#pragma once

#if defined(OS_WASM)

#include <Disks/DiskObjectStorage/ObjectStorages/WasmWeb/WasmWebObjectStorage.h>

#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

/// Object storage configuration for the WASM build's web-fetch data plane.
/// Understands the table endpoints a data-lake catalog hands out:
///   - s3://bucket/key        -> https virtual-hosted URL + SigV4 (when credentials given)
///   - http(s)://host/prefix  -> used as-is (storage_endpoint overrides, S3-compatible
///                               services, plain web servers); signed when credentials given
/// Engine args mirror what DatabaseDataLake passes: (url[, access_key_id,
/// secret_access_key[, session_token]]) or (url, NOSIGN[, headers(...)]).
/// Read-only: creates WasmWebObjectStorage.
class StorageWasmWebConfiguration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::Web;
    static constexpr auto type_name = "web";

    StorageWasmWebConfiguration() = default;
    StorageWasmWebConfiguration(const StorageWasmWebConfiguration & other) = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return "WasmWeb"; }

    Path getRawPath() const override { return path; }
    void setRawPath(const Path & p) override { path = p; }
    const String & getRawURI() const override { return raw_uri; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override
    {
        paths = paths_;
        path = paths_[0];
    }

    String getNamespace() const override { return bucket; }
    String getDataSourceDescription() const override { return base_url; }
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const override;

    ObjectStoragePtr createObjectStorage(ContextPtr, bool readonly, CredentialsConfigurationCallback refresh_credentials_callback) override;

    void addStructureAndFormatToArgsIfNeeded(ASTs &, const String &, const String &, ContextPtr, bool) override { }

    bool supportsWrites() const override { return false; }

protected:
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;

private:
    /// Original endpoint from the engine args (s3:// or http(s)://).
    String raw_uri;
    /// http(s) URL the object keys get appended to (bucket root), no trailing slash.
    String base_url;
    /// Bucket (S3) or first path segment; informational (IcebergPathResolver namespace).
    String bucket;
    String region;
    String access_key_id;
    String secret_access_key;
    String session_token;
    bool no_sign = false;
    HTTPHeaderEntries static_headers;

    /// Key prefix of the table inside base_url (what IcebergMetadata sees as table_path).
    Path path;
    Paths paths;
};

}

#endif
