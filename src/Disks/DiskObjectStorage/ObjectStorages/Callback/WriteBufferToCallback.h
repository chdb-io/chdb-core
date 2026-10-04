#pragma once

#include <utility>
#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>
#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

/// Buffers writes and streams them through `write_append`. `finalize` commits, `cancel` aborts.
class WriteBufferToCallback : public WriteBufferFromFileBase
{
public:
    WriteBufferToCallback(CallbackObjectStorageOpsPtr ops_, String key_, size_t buf_size)
        : WriteBufferFromFileBase(buf_size, nullptr, 0)
        , ops(std::move(ops_))
        , key(std::move(key_))
    {
        ops->check(ops->write_begin(ops->ud, key.c_str(), &handle), "write_begin", key);
    }

    std::string getFileName() const override { return key; }
    void sync() override { next(); }

private:
    void nextImpl() override { ops->check(ops->write_append(ops->ud, handle, working_buffer.begin(), offset()), "write_append", key); }

    void finalizeImpl() override
    {
        next();
        /// The handle is released by commit even when it fails, so it must not be aborted afterwards.
        void * committing = std::exchange(handle, nullptr);
        ops->check(ops->write_commit(ops->ud, committing), "write_commit", key);
    }

    void cancelImpl() noexcept override
    {
        if (void * pending = std::exchange(handle, nullptr))
            ops->write_abort(ops->ud, pending);
    }

    const CallbackObjectStorageOpsPtr ops;
    const String key;
    void * handle = nullptr;
};

}
