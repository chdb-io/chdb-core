#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileName.h>
#include <IO/WithFileSize.h>
#include <Core/Defines.h>

#include <optional>
#include <string>
#include <vector>

namespace DB
{

/// Reads a File/Blob registered on the JS side (globalThis.__CHDB_FILES, keyed by
/// name) via synchronous, on-demand byte-range reads (Blob.slice + FileReaderSync),
/// for the WASM build. Lazy: only the bytes actually read are pulled into wasm memory
/// (no full copy), mirroring duckdb-wasm's registerFileHandle. The C++ side never
/// touches the handle — it asks JS for a byte range. Worker-only (FileReaderSync).
/// Used by StorageFile for `file('<name>', ...)` on wasm (a registered name is read
/// through this instead of the OS filesystem). Mirrors ReadBufferFromWebFetch.
class ReadBufferFromJSFile : public SeekableReadBuffer, public WithFileName, public WithFileSize
{
public:
    explicit ReadBufferFromJSFile(std::string name_, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE);

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    String getFileName() const override { return name; }
    std::optional<size_t> tryGetFileSize() override;

private:
    bool nextImpl() override;

    std::string name;
    std::vector<char> buffer;
    /// Absolute byte offset of the next range read.
    off_t read_offset = 0;
    std::optional<size_t> file_size;
    bool file_size_queried = false;
};

/// Size of a registered JS file (globalThis.__CHDB_FILES), or nullopt if not registered.
/// Used by StorageFile's OS_WASM stat shim. Defined (EM_JS-backed) only under OS_WASM.
std::optional<size_t> tryGetJSFileSize(const std::string & name);

}
