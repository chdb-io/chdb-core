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
        , handle(ops->writeBegin(key))
    {
    }

    std::string getFileName() const override { return key; }
    void sync() override { next(); }

private:
    void nextImpl() override { ops->writeAppend(handle, key, working_buffer.begin(), offset()); }

    void finalizeImpl() override
    {
        next();
        /// The handle is released by commit even when it fails, so it must not be aborted afterwards.
        pending = false;
        ops->writeCommit(handle, key);
    }

    /// The handle is opaque, so "released" is tracked here rather than by nulling it.
    void cancelImpl() noexcept override
    {
        if (std::exchange(pending, false))
            ops->writeAbort(handle);
    }

    const CallbackObjectStorageOpsPtr ops;
    const String key;
    void * const handle;
    bool pending = true;
};

}
