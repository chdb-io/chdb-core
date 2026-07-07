#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/HTTPHeaderEntries.h>
#include <Core/Defines.h>

#include <functional>
#include <optional>
#include <string>

namespace DB
{

/// Reads an http(s) URL through the host JavaScript environment (synchronous XHR
/// with HTTP Range), for the WASM build where raw sockets (and thus Poco/curl
/// based HTTP) are unavailable. Mirrors duckdb-wasm: the C++ side never opens a
/// socket — it asks JS to fetch a byte range and copies the bytes into wasm memory.
/// Reads are sequential-friendly (CSV/JSON/etc.); seek() is supported via Range.
/// Optional request headers are forwarded to the XHR (used by s3() for SigV4 auth).
/// Inherits ReadBufferFromFileBase so it can serve as an IObjectStorage read buffer.
class ReadBufferFromWebFetch : public ReadBufferFromFileBase
{
public:
    /// Called per HTTP request with the method ("GET" for range reads, "HEAD" for
    /// size queries) to produce fresh request headers. Needed for AWS SigV4, whose
    /// signature covers the method and expires after a clock-skew window — static
    /// headers would go stale over long scans and be invalid for HEAD.
    using HeadersProvider = std::function<HTTPHeaderEntries(const std::string & method)>;

    /// skip_not_found_: when true, a 404 is treated as end-of-file (empty) instead of an
    /// error — mirrors the native withSkipNotFound() used for http_skip_not_found_url_for_globs.
    /// known_file_size_: pre-known object size (e.g. from data-lake manifests); avoids the
    /// HEAD request tryGetFileSize() would otherwise issue (some endpoints only permit GET).
    explicit ReadBufferFromWebFetch(
        std::string url_, HTTPHeaderEntries headers_ = {}, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        bool skip_not_found_ = false, HeadersProvider headers_provider_ = {},
        std::optional<size_t> known_file_size_ = std::nullopt);

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    String getFileName() const override { return url; }
    std::optional<size_t> tryGetFileSize() override;

private:
    bool nextImpl() override;

    std::string headersBlobFor(const std::string & method) const;

    std::string url;
    HTTPHeaderEntries headers;
    /// Pre-serialized "Name: value\n..." blob handed to the JS side (XHR setRequestHeader).
    std::string headers_blob;
    HeadersProvider headers_provider;
    /// Absolute byte offset of the next range read (i.e. of internal_buffer's start once filled).
    off_t read_offset = 0;
    bool file_size_queried = false;
    bool skip_not_found = false;
};

}
