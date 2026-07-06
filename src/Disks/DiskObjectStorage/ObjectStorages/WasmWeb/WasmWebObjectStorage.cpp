#include <Disks/DiskObjectStorage/ObjectStorages/WasmWeb/WasmWebObjectStorage.h>

#if defined(OS_WASM)

#include <Common/Exception.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/ReadBufferFromWebFetch.h>
#include <IO/ReadSettings.h>
#include <IO/WasmHTTPBridge.h>
#include <IO/WasmS3Auth.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NETWORK_ERROR;
    extern const int FILE_DOESNT_EXIST;
}

WasmWebObjectStorage::WasmWebObjectStorage(WasmWebObjectStorageSettings settings_)
    : settings(std::move(settings_))
{
    while (settings.base_url.ends_with('/'))
        settings.base_url.pop_back();
}

String WasmWebObjectStorage::urlFor(const std::string & path) const
{
    if (path.starts_with('/'))
        return settings.base_url + path;
    return settings.base_url + "/" + path;
}

HTTPHeaderEntries WasmWebObjectStorage::headersFor(const String & method, const String & url) const
{
    HTTPHeaderEntries headers = settings.static_headers;
    if (!settings.access_key_id.empty())
    {
        auto signed_headers
            = WasmS3::signV4Request(method, url, settings.region, settings.access_key_id, settings.secret_access_key, settings.session_token);
        headers.insert(headers.end(), signed_headers.begin(), signed_headers.end());
    }
    return headers;
}

std::unique_ptr<ReadBufferFromFileBase> WasmWebObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>) const
{
    const String url = urlFor(object.remote_path);
    return std::make_unique<ReadBufferFromWebFetch>(
        url,
        HTTPHeaderEntries{},
        read_settings.remote_fs_buffer_size,
        /* skip_not_found */ false,
        /* headers_provider */ [this, url](const std::string & method) { return headersFor(method, url); });
}

std::optional<ObjectMetadata> WasmWebObjectStorage::tryGetObjectMetadata(const std::string & path, bool /* with_tags */) const
{
    const String url = urlFor(path);
    HTTPHeaderEntries headers = headersFor("HEAD", url);
    String blob;
    for (const auto & entry : headers)
    {
        blob += entry.name;
        blob += ": ";
        blob += entry.value;
        blob += '\n';
    }
    auto result = performWasmHTTPRequest("HEAD", url, blob, {});
    if (result.status == 404)
        return std::nullopt;
    if (result.status < 200 || result.status >= 300)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to HEAD '{}' (HTTP status {})", url, result.status);

    ObjectMetadata metadata;
    metadata.is_size_known = false;
    size_t line_start = 0;
    while (line_start < result.headers.size())
    {
        size_t line_end = result.headers.find('\n', line_start);
        if (line_end == std::string::npos)
            line_end = result.headers.size();
        std::string_view line(result.headers.data() + line_start, line_end - line_start);
        line_start = line_end + 1;

        size_t colon = line.find(": ");
        if (colon == std::string_view::npos)
            continue;
        std::string name(line.substr(0, colon));
        std::string value(line.substr(colon + 2));
        if (Poco::icompare(name, "Content-Length") == 0)
        {
            metadata.size_bytes = std::stoull(value);
            metadata.is_size_known = true;
        }
        else if (Poco::icompare(name, "ETag") == 0)
            metadata.etag = value;
    }
    return metadata;
}

ObjectMetadata WasmWebObjectStorage::getObjectMetadata(const std::string & path, bool with_tags) const
{
    auto metadata = tryGetObjectMetadata(path, with_tags);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Object '{}' does not exist (HTTP 404)", urlFor(path));
    return *metadata;
}

bool WasmWebObjectStorage::exists(const StoredObject & object) const
{
    return tryGetObjectMetadata(object.remote_path, /* with_tags */ false).has_value();
}

ReadSettings WasmWebObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    /// Reads are plain synchronous range fetches: no threadpool reader, no
    /// prefetch (an async wrapper would tie up scarce wasm pthreads).
    auto modified_settings{read_settings};
    modified_settings.remote_fs_method = RemoteFSReadMethod::read;
    modified_settings.remote_fs_prefetch = false;
    return IObjectStorage::patchSettings(modified_settings);
}

ObjectStorageKeyGeneratorPtr WasmWebObjectStorage::createKeyGenerator() const
{
    return createObjectStorageKeyGeneratorByPrefix("");
}

std::unique_ptr<WriteBufferFromFileBase> WasmWebObjectStorage::writeObject( /// NOLINT
    const StoredObject &, WriteMode, std::optional<ObjectAttributes>, size_t, const WriteSettings &)
{
    throwReadOnly();
}

void WasmWebObjectStorage::removeObjectIfExists(const StoredObject &)
{
    throwReadOnly();
}

void WasmWebObjectStorage::removeObjectsIfExist(const StoredObjects &)
{
    throwReadOnly();
}

void WasmWebObjectStorage::copyObject( /// NOLINT
    const StoredObject &, const StoredObject &, const ReadSettings &, const WriteSettings &, std::optional<ObjectAttributes>)
{
    throwReadOnly();
}

void WasmWebObjectStorage::throwReadOnly() const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "WasmWeb object storage '{}' is read-only: writes are not supported on WASM", settings.base_url);
}

}

#endif
