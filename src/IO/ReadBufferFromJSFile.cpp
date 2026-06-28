#include <IO/ReadBufferFromJSFile.h>

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

/// Synchronous reads of a registered File/Blob. The handle is stored in
/// globalThis.__CHDB_FILES by the npm package's registerFile(), which runs on the
/// module's *main runtime thread* (a Web Worker, where FileReaderSync is available).
///
/// On the threaded build a query's StorageFile read executes on a pool pthread (a
/// different Worker that does NOT have the handle — JS objects are not shared across
/// Workers, only wasm linear memory is). So we run the JS on the main runtime thread
/// via MAIN_THREAD_EM_ASM: it proxies the call from the pool pthread to the main
/// thread (the same path Emscripten uses to proxy FS syscalls, which is why MEMFS
/// reads already work on the threaded build) and runs inline on the single-threaded
/// build. The destination/heap is shared linear memory, so bytes written on the main
/// thread are visible to the pool pthread.
///
/// Pointers are passed as double (a plain JS number, < 2^53 for any real wasm address)
/// to avoid -sMEMORY64 BigInt marshalling across the proxy boundary. Return -1 on
/// error, 0 at EOF, else the number of bytes read.
static double chdb_file_read(const char * name_ptr, double offset, double length, char * dst)
{
    return MAIN_THREAD_EM_ASM_DOUBLE({
        try {
            var files = globalThis.__CHDB_FILES;
            var h = files ? files.get(UTF8ToString($0)) : undefined;
            if (!h) return -1;
            var off = $1;
            var size = h.size;
            if (off >= size) return 0;                   // EOF
            var end = Math.min(off + $2, size);
            var slice = h.slice(off, end);               // Blob.slice — no copy of the whole file
            var data = new Uint8Array(new FileReaderSync().readAsArrayBuffer(slice));
            HEAPU8.set(data, $3);
            return data.byteLength;
        } catch (e) {
            return -1;
        }
    }, static_cast<double>(reinterpret_cast<uintptr_t>(name_ptr)), offset, length,
       static_cast<double>(reinterpret_cast<uintptr_t>(dst)));
}

static double chdb_file_size(const char * name_ptr)
{
    return MAIN_THREAD_EM_ASM_DOUBLE({
        try {
            var files = globalThis.__CHDB_FILES;
            var h = files ? files.get(UTF8ToString($0)) : undefined;
            return h ? h.size : -1;
        } catch (e) {
            return -1;
        }
    }, static_cast<double>(reinterpret_cast<uintptr_t>(name_ptr)));
}

ReadBufferFromJSFile::ReadBufferFromJSFile(std::string name_, size_t buffer_size)
    : SeekableReadBuffer(nullptr, 0), name(std::move(name_)), buffer(buffer_size)
{
}

bool ReadBufferFromJSFile::nextImpl()
{
    double n = chdb_file_read(name.c_str(), static_cast<double>(read_offset), static_cast<double>(buffer.size()), buffer.data());
    if (n < 0)
        throw Exception(ErrorCodes::NETWORK_ERROR, "Failed to read registered file '{}' (range at offset {})", name, read_offset);
    if (n == 0)
        return false;  // EOF

    read_offset += static_cast<off_t>(n);
    working_buffer = internal_buffer = Buffer(buffer.data(), buffer.data() + static_cast<size_t>(n));
    pos = working_buffer.begin();
    return true;
}

off_t ReadBufferFromJSFile::seek(off_t off, int whence)
{
    if (whence == SEEK_CUR)
        off = getPosition() + off;
    else if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET and SEEK_CUR are supported");

    if (off < 0)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Seek to negative offset {}", off);

    read_offset = off;
    resetWorkingBuffer();
    return off;
}

off_t ReadBufferFromJSFile::getPosition()
{
    return read_offset - static_cast<off_t>(available());
}

std::optional<size_t> ReadBufferFromJSFile::tryGetFileSize()
{
    if (!file_size_queried)
    {
        file_size_queried = true;
        double sz = chdb_file_size(name.c_str());
        if (sz >= 0)
            file_size = static_cast<size_t>(sz);
    }
    return file_size;
}

std::optional<size_t> tryGetJSFileSize(const std::string & name)
{
    double sz = chdb_file_size(name.c_str());
    if (sz >= 0)
        return static_cast<size_t>(sz);
    return std::nullopt;
}

}

#endif
