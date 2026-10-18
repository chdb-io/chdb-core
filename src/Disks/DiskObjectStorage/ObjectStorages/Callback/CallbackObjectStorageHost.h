#pragma once

#include <base/types.h>

#include <cstddef>
#include <cstdint>
#include <functional>

namespace DB
{

/// What the host reports for a key.
struct CallbackObjectStat
{
    uint64_t size = 0;
    int64_t mtime = 0;
};

/// The blob operations of an embedding host, one method per callback. chdb.cpp implements it over the C
/// table passed to chdb_register_object_storage, whose comments in programs/local/chdb.h state the contract
/// of every operation. Only CallbackObjectStorageOps calls it: fencing, re-entrancy detection and the
/// translation of statuses into exceptions live there, so this layer only moves arguments across the ABI.
/// Every method may leave a message for the user in `error`.
class ICallbackObjectStorageHost
{
public:
    enum class Status
    {
        Ok,
        NotFound,
        Error,
    };

    /// Receives one listed blob; returns true to stop the listing. Never throws.
    using ListSink = std::function<bool(const char * key, uint64_t size, int64_t mtime)>;

    virtual ~ICallbackObjectStorageHost() = default;

    virtual Status metadata(const char * key, CallbackObjectStat & stat, String & error) = 0;
    virtual Status read(const char * key, uint64_t offset, char * buf, size_t len, size_t & bytes_read, String & error) = 0;

    virtual Status writeBegin(const char * key, void *& handle, String & error) = 0;
    virtual Status writeAppend(void * handle, const char * buf, size_t len, String & error) = 0;
    /// Releases the handle whether or not it succeeds.
    virtual Status writeCommit(void * handle, String & error) = 0;
    virtual void writeAbort(void * handle) = 0;

    virtual Status remove(const char * key, String & error) = 0;
    virtual Status list(const char * prefix, const ListSink & sink, String & error) = 0;

    /// Optional operations; the engine calls them only when the matching has*() is true.
    virtual bool hasCopy() const = 0;
    virtual Status copy(const char * from_key, const char * to_key, String & error) = 0;

    virtual bool hasKeyspaceHooks() const = 0;
    virtual Status openKeyspace(const char * key_prefix, bool read_only, String & error) = 0;
    virtual void closeKeyspace(const char * key_prefix) = 0;
};

}
