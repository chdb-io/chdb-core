#include <IO/ReadBufferFromWebFetch.h>

#if defined(OS_WASM)

#include <IO/WasmHTTPBridge.h>
#include <Common/Exception.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

namespace
{

void appendHeadersBlob(std::string & blob, const HTTPHeaderEntries & entries)
{
    for (const auto & entry : entries)
    {
        blob += entry.name;
        blob += ": ";
        blob += entry.value;
        blob += '\n';
    }
}

}

ReadBufferFromWebFetch::ReadBufferFromWebFetch(
    std::string url_, HTTPHeaderEntries headers_, size_t buffer_size, bool skip_not_found_, HeadersProvider headers_provider_)
    : ReadBufferFromFileBase(buffer_size, nullptr, 0)
    , url(std::move(url_))
    , headers(std::move(headers_))
    , headers_provider(std::move(headers_provider_))
    , skip_not_found(skip_not_found_)
{
    appendHeadersBlob(headers_blob, headers);
}

std::string ReadBufferFromWebFetch::headersBlobFor(const std::string & method) const
{
    if (!headers_provider)
        return headers_blob;
    std::string blob = headers_blob;
    appendHeadersBlob(blob, headers_provider(method));
    return blob;
}

bool ReadBufferFromWebFetch::nextImpl()
{
    /// The bridge adds the Range header and, if the server ignores it (200),
    /// slices the requested window out before copying into wasm memory.
    auto result = performWasmHTTPRequest(
        "GET", url, headersBlobFor("GET"), {}, static_cast<long long>(read_offset), static_cast<long long>(memory.size()));

    if (result.status == 416)
        return false;  /// range not satisfiable -> EOF
    if (result.status == 404)
    {
        /// 404: when skipping is requested (http_skip_not_found_url_for_globs), treat it as
        /// an empty file so the URL is skipped rather than failing the read.
        if (skip_not_found)
            return false;
        throw Exception(ErrorCodes::NETWORK_ERROR, "Not found (HTTP 404): '{}'", url);
    }
    if (result.status != 200 && result.status != 206)
        throw Exception(
            ErrorCodes::NETWORK_ERROR, "Failed to fetch '{}' (range at offset {}, HTTP status {})", url, read_offset, result.status);

    size_t n = std::min(result.body.size(), memory.size());
    if (n == 0)
        return false;  /// EOF
    memcpy(memory.data(), result.body.data(), n);

    read_offset += static_cast<off_t>(n);
    working_buffer = internal_buffer = Buffer(memory.data(), memory.data() + n);
    pos = working_buffer.begin();
    return true;
}

off_t ReadBufferFromWebFetch::seek(off_t off, int whence)
{
    if (whence == SEEK_CUR)
        off = getPosition() + off;
    else if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET and SEEK_CUR are supported");

    if (off < 0)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Seek to negative offset {}", off);

    /// Drop the buffer and resume range reads from the new absolute offset.
    read_offset = off;
    resetWorkingBuffer();
    return off;
}

off_t ReadBufferFromWebFetch::getPosition()
{
    /// read_offset is the end of what we've fetched; subtract what's still unread in the buffer.
    return read_offset - static_cast<off_t>(available());
}

std::optional<size_t> ReadBufferFromWebFetch::tryGetFileSize()
{
    if (!file_size_queried && !file_size)
    {
        file_size_queried = true;
        auto result = performWasmHTTPRequest("HEAD", url, headersBlobFor("HEAD"), {});
        if (result.status >= 200 && result.status < 300)
        {
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
                if (Poco::icompare(std::string(line.substr(0, colon)), "Content-Length") == 0)
                {
                    file_size = std::stoull(std::string(line.substr(colon + 2)));
                    break;
                }
            }
        }
    }
    return file_size;
}

}

#endif
