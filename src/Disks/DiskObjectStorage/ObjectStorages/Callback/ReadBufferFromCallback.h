#pragma once

#include <algorithm>
#include <optional>
#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CALLBACK_OBJECT_STORAGE_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int FILE_DOESNT_EXIST;
}

/// Reads a blob at arbitrary offsets. Seeks only move the offset: every refill is one `read` callback.
class ReadBufferFromCallback : public ReadBufferFromFileBase
{
public:
    /// With an external buffer the memory comes from `set()`, so none is allocated here.
    ReadBufferFromCallback(
        CallbackObjectStorageOpsPtr ops_, String key_, size_t buf_size, bool use_external_buffer_, std::optional<size_t> file_size_)
        : ReadBufferFromFileBase(use_external_buffer_ ? 0 : buf_size, nullptr, 0, file_size_)
        , ops(std::move(ops_))
        , key(std::move(key_))
        , use_external_buffer(use_external_buffer_)
    {
    }

    String getFileName() const override { return key; }
    bool supportsExternalBufferMode() const override { return use_external_buffer; }
    size_t getFileOffsetOfBufferEnd() const override { return offset; }
    off_t getPosition() override { return static_cast<off_t>(offset - available()); }

    /// A seek is pure bookkeeping here, and a right bound clamps the next `read` to what the caller
    /// needs instead of asking the host for a full buffer that the gather then throws away.
    bool isSeekCheap() override { return true; }
    bool supportsRightBoundedReads() const override { return true; }
    void setReadUntilPosition(size_t position) override { read_until_position = position; }
    void setReadUntilEnd() override { read_until_position.reset(); }

    std::optional<size_t> tryGetFileSize() override
    {
        if (!file_size)
        {
            auto stat = statCallbackObject(*ops, key);
            if (!stat)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Callback object '{}' does not exist", key);
            file_size = stat->size;
        }
        return file_size;
    }

    off_t seek(off_t off, int whence) override
    {
        if (whence == SEEK_CUR)
            off += getPosition();
        else if (whence != SEEK_SET)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET and SEEK_CUR are supported");
        if (off < 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Seek to negative offset {} in '{}'", off, key);

        /// Stay inside the buffer when the target is already there.
        const auto target = static_cast<size_t>(off);
        if (!working_buffer.empty() && target >= offset - working_buffer.size() && target < offset)
        {
            pos = working_buffer.end() - (offset - target);
            return off;
        }
        resetWorkingBuffer();
        offset = target;
        return off;
    }

private:
    bool nextImpl() override
    {
        size_t len = internal_buffer.size();
        if (read_until_position)
        {
            if (offset >= *read_until_position)
                return false;
            len = std::min(len, *read_until_position - offset);
        }

        size_t read = 0;
        ops->check(ops->read(ops->ud, key.c_str(), offset, internal_buffer.begin(), len, &read), "read", key);
        if (read == 0)
            return false;
        /// A host that overruns the buffer broke the contract; it is an external failure, not an engine invariant.
        if (read > len)
            throw Exception(
                ErrorCodes::CALLBACK_OBJECT_STORAGE_ERROR,
                "Callback object storage '{}': read of '{}' at offset {} returned {} bytes for a {} byte buffer",
                ops->name,
                key,
                offset,
                read,
                len);

        /// Re-base the window on the allocation: after an EOF `ReadBuffer::next()` leaves `working_buffer`
        /// as an empty window at the old end, and under the gather's `SwapHelper` it is the caller's window.
        working_buffer = internal_buffer;
        working_buffer.resize(read);
        offset += read;
        return true;
    }

    const CallbackObjectStorageOpsPtr ops;
    const String key;
    const bool use_external_buffer;
    /// File offset of the end of the working buffer.
    size_t offset = 0;
    /// Object-local right bound, in the same coordinate space as `offset`.
    std::optional<size_t> read_until_position;
};

}
