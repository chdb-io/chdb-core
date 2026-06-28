#include <IO/ReadBufferFromWebFetch.h>

#if defined(OS_WASM)

#include <Common/Exception.h>
#include <emscripten.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

/// Synchronous XHR helpers, run in the module's JS runtime (a Web Worker, where
/// synchronous XHR with a binary responseType is permitted). Return -1 on error.
/// Pointer arguments arrive as BigInt under -sMEMORY64, so they are normalized
/// with Number() before use. `headers_ptr` is a newline-delimited "Name: value"
/// blob; each line is applied via setRequestHeader (used by s3() for SigV4 auth).
/// chdb_web_read copies the fetched bytes into the wasm heap at `dst`.

EM_JS(double, chdb_web_read, (const char * url_ptr, double offset, double length, const char * headers_ptr, char * dst), {
    try {
        var url = UTF8ToString(Number(url_ptr));
        var off = offset;
        var len = length;
        var xhr = new XMLHttpRequest();
        xhr.open('GET', url, false);  // synchronous
        xhr.responseType = 'arraybuffer';
        var hp = Number(headers_ptr);
        if (hp) {
            var blob = UTF8ToString(hp);
            if (blob) {
                var lines = blob.split('\n');
                for (var i = 0; i < lines.length; i++) {
                    var line = lines[i];
                    if (!line) continue;
                    var c = line.indexOf(': ');
                    if (c > 0) { try { xhr.setRequestHeader(line.substring(0, c), line.substring(c + 2)); } catch (e) {} }
                }
            }
        }
        xhr.setRequestHeader('Range', 'bytes=' + off + '-' + (off + len - 1));
        xhr.send();
        if (xhr.status === 416) return 0;            // range not satisfiable -> EOF
        if (xhr.status === 404) return -404;         // not found -> caller may skip (vs generic error)
        if (xhr.status !== 200 && xhr.status !== 206) return -1;
        var all = new Uint8Array(xhr.response);
        // A 206 returns just the requested range; a 200 means the server ignored
        // Range and returned the whole body, so slice out the [off, off+len) window
        // (empty once off is past the end -> EOF) to keep sequential reads correct.
        var src = (xhr.status === 200) ? all.subarray(off, off + len) : all.subarray(0, len);
        var n = src.length;
        HEAPU8.set(src, Number(dst));
        return n;
    } catch (e) {
        return -1;
    }
});

EM_JS(double, chdb_web_size, (const char * url_ptr, const char * headers_ptr), {
    try {
        var url = UTF8ToString(Number(url_ptr));
        var xhr = new XMLHttpRequest();
        xhr.open('HEAD', url, false);
        var hp = Number(headers_ptr);
        if (hp) {
            var blob = UTF8ToString(hp);
            if (blob) {
                var lines = blob.split('\n');
                for (var i = 0; i < lines.length; i++) {
                    var line = lines[i];
                    if (!line) continue;
                    var c = line.indexOf(': ');
                    if (c > 0) { try { xhr.setRequestHeader(line.substring(0, c), line.substring(c + 2)); } catch (e) {} }
                }
            }
        }
        xhr.send();
        if (xhr.status >= 200 && xhr.status < 300) {
            var cl = xhr.getResponseHeader('Content-Length');
            if (cl) return parseInt(cl, 10);
        }
        return -1;
    } catch (e) {
        return -1;
    }
});

ReadBufferFromWebFetch::ReadBufferFromWebFetch(
    std::string url_, HTTPHeaderEntries headers_, size_t buffer_size, bool skip_not_found_)
    : SeekableReadBuffer(nullptr, 0), url(std::move(url_)), headers(std::move(headers_)), buffer(buffer_size)
    , skip_not_found(skip_not_found_)
{
    for (const auto & entry : headers)
    {
        headers_blob += entry.name;
        headers_blob += ": ";
        headers_blob += entry.value;
        headers_blob += '\n';
    }
}

bool ReadBufferFromWebFetch::nextImpl()
{
    double n = chdb_web_read(
        url.c_str(), static_cast<double>(read_offset), static_cast<double>(buffer.size()), headers_blob.c_str(), buffer.data());
    if (n == -404)
    {
        /// 404: when skipping is requested (http_skip_not_found_url_for_globs), treat it as
        /// an empty file so the URL is skipped rather than failing the read.
        if (skip_not_found)
            return false;
        throw Exception(ErrorCodes::NETWORK_ERROR, "Not found (HTTP 404): '{}'", url);
    }
    if (n < 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to fetch '{}' (range at offset {})", url, read_offset);
    if (n == 0)
        return false;  // EOF

    read_offset += static_cast<off_t>(n);
    working_buffer = internal_buffer = Buffer(buffer.data(), buffer.data() + static_cast<size_t>(n));
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
    if (!file_size_queried)
    {
        file_size_queried = true;
        double sz = chdb_web_size(url.c_str(), headers_blob.c_str());
        if (sz >= 0)
            file_size = static_cast<size_t>(sz);
    }
    return file_size;
}

}

#endif
