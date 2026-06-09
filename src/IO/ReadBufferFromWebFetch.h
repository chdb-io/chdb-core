#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileName.h>
#include <IO/WithFileSize.h>
#include <IO/HTTPHeaderEntries.h>
#include <Core/Defines.h>

#include <optional>
#include <string>
#include <vector>

namespace DB
{

/// Reads an http(s) URL through the host JavaScript environment (synchronous XHR
/// with HTTP Range), for the WASM build where raw sockets (and thus Poco/curl
/// based HTTP) are unavailable. Mirrors duckdb-wasm: the C++ side never opens a
/// socket — it asks JS to fetch a byte range and copies the bytes into wasm memory.
/// Reads are sequential-friendly (CSV/JSON/etc.); seek() is supported via Range.
/// Optional request headers are forwarded to the XHR (used by s3() for SigV4 auth).
class ReadBufferFromWebFetch : public SeekableReadBuffer, public WithFileName, public WithFileSize
{
public:
    /// skip_not_found_: when true, a 404 is treated as end-of-file (empty) instead of an
    /// error — mirrors the native withSkipNotFound() used for http_skip_not_found_url_for_globs.
    explicit ReadBufferFromWebFetch(
        std::string url_, HTTPHeaderEntries headers_ = {}, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        bool skip_not_found_ = false);

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    String getFileName() const override { return url; }
    std::optional<size_t> tryGetFileSize() override;

private:
    bool nextImpl() override;

    std::string url;
    HTTPHeaderEntries headers;
    /// Pre-serialized "Name: value\n..." blob handed to the JS side (XHR setRequestHeader).
    std::string headers_blob;
    std::vector<char> buffer;
    /// Absolute byte offset of the next range read (i.e. of internal_buffer's start once filled).
    off_t read_offset = 0;
    std::optional<size_t> file_size;
    bool file_size_queried = false;
    bool skip_not_found = false;
};

}
