#include <Disks/DiskObjectStorage/ObjectStorages/WasmWeb/WasmWebObjectStorage.h>

#if defined(OS_WASM)

#include <Common/Exception.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/ReadBufferFromWebFetch.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WasmHTTPBridge.h>
#include <IO/WasmS3Auth.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NETWORK_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_DATA;
}

WasmWebObjectStorage::WasmWebObjectStorage(WasmWebObjectStorageSettings settings_)
    : settings(std::move(settings_))
{
    while (settings.base_url.ends_with('/'))
        settings.base_url.pop_back();
}

String WasmWebObjectStorage::urlFor(const std::string & path) const
{
    /// Percent-encode the key with the exact AWS unreserved set: object keys may
    /// contain spaces/'%'/'#' (Delta partition values do), which would otherwise
    /// break the XHR URL and never match the SigV4 canonical URI. signV4Request
    /// decodes + re-encodes the path with the same set, so the signature covers
    /// byte-identical bytes to what the transport sends.
    std::string_view key = path;
    while (key.starts_with('/'))
        key.remove_prefix(1);
    return settings.base_url + "/" + WasmS3::uriEncode(String(key), /*encode_slash=*/false);
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
    std::optional<size_t> read_hint,
    bool /*use_external_buffer*/,
    bool /*restrict_seek*/) const
{
    /// Data-lake manifests carry exact file sizes; seeding them here spares a
    /// signed HEAD per file (and works on endpoints that only permit GET).
    std::optional<size_t> known_size = read_hint;
    if (!known_size && object.bytes_size != 0 && object.bytes_size != std::numeric_limits<uint64_t>::max())
        known_size = object.bytes_size;

    const String url = urlFor(object.remote_path);
    return std::make_unique<ReadBufferFromWebFetch>(
        url,
        HTTPHeaderEntries{},
        read_settings.remote_fs_settings.buffer_size,
        /* skip_not_found */ false,
        /* headers_provider */ [this, url](const std::string & method) { return headersFor(method, url); },
        known_size);
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
            /// tryParse: a malformed server-supplied length must degrade to
            /// "size unknown", not escape as std::invalid_argument.
            if (tryParse(metadata.size_bytes, value))
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

void WasmWebObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    /// With a path-style endpoint object keys carry the bucket as their first
    /// segment, but ListObjectsV2 addresses the bucket in the URL path and its
    /// prefix/keys are bucket-relative — strip it here, re-prepend it below so
    /// the returned paths keep readObject()'s key semantics.
    String list_base = settings.base_url;
    String key_prefix_to_readd;
    String prefix = path;
    if (!settings.bucket.empty() && prefix.starts_with(settings.bucket + "/"))
    {
        list_base += "/" + settings.bucket;
        key_prefix_to_readd = settings.bucket + "/";
        prefix = prefix.substr(settings.bucket.size() + 1);
    }

    String continuation_token;
    do
    {
        String encoded_prefix;
        Poco::URI::encode(prefix, "/&?=+ ", encoded_prefix);
        String url = list_base + "/?list-type=2&prefix=" + encoded_prefix;
        if (max_keys)
            url += "&max-keys=" + std::to_string(max_keys);
        if (!continuation_token.empty())
        {
            String encoded_token;
            Poco::URI::encode(continuation_token, "/&?=+ ", encoded_token);
            url += "&continuation-token=" + encoded_token;
        }

        HTTPHeaderEntries headers = headersFor("GET", url);
        String blob;
        for (const auto & entry : headers)
        {
            blob += entry.name;
            blob += ": ";
            blob += entry.value;
            blob += '\n';
        }
        auto result = performWasmHTTPRequest("GET", url, blob, {});
        if (result.status < 200 || result.status >= 300)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to list '{}' (HTTP status {})", url, result.status);

        Poco::XML::DOMParser parser;
        Poco::AutoPtr<Poco::XML::Document> document;
        try
        {
            document = parser.parseString(result.body);
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse ListObjectsV2 response for '{}': {}", url, e.displayText());
        }

        Poco::AutoPtr<Poco::XML::NodeList> contents = document->getElementsByTagName("Contents");
        for (unsigned long i = 0; i < contents->length(); ++i)
        {
            const auto * element = dynamic_cast<Poco::XML::Element *>(contents->item(i));
            if (!element)
                continue;
            const auto * key_node = element->getChildElement("Key");
            if (!key_node)
                continue;
            ObjectMetadata metadata;
            metadata.is_size_known = false;
            if (const auto * size_node = element->getChildElement("Size"))
            {
                if (tryParse(metadata.size_bytes, size_node->innerText()))
                    metadata.is_size_known = true;
            }
            if (const auto * etag_node = element->getChildElement("ETag"))
                metadata.etag = etag_node->innerText();
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(key_prefix_to_readd + key_node->innerText(), std::move(metadata)));
            if (max_keys && children.size() >= max_keys)
                return;
        }

        continuation_token.clear();
        Poco::AutoPtr<Poco::XML::NodeList> token_nodes = document->getElementsByTagName("NextContinuationToken");
        if (token_nodes->length() > 0)
            continuation_token = token_nodes->item(0)->innerText();
    } while (!continuation_token.empty());
}

ReadSettings WasmWebObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    /// Reads are plain synchronous range fetches: no threadpool reader, no
    /// prefetch (an async wrapper would tie up scarce wasm pthreads).
    auto modified_settings{read_settings};
    modified_settings.remote_fs_settings.method = RemoteFSReadMethod::read;
    modified_settings.remote_fs_settings.prefetch = false;
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
