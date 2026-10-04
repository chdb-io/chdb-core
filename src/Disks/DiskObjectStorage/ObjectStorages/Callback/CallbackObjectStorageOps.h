#pragma once

#include <base/types.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace DB
{

/// Blob operations supplied by the embedding host. A C-side mirror of `chdb_object_storage_callbacks`
/// (programs/local/chdb.h), which documents the contract of every operation. Depends on base/types.h
/// alone so that the C API translation unit can include it without the object storage headers.
struct CallbackObjectStorageOps
{
    using ListSink = int (*)(void * sink_ud, const char * key, uint64_t size, int64_t mtime);

    String name;
    void * ud = nullptr;
    int (*exists)(void * ud, const char * key, int * out) = nullptr;
    int (*metadata)(void * ud, const char * key, int * found, uint64_t * size, int64_t * mtime) = nullptr;
    int (*read)(void * ud, const char * key, uint64_t offset, void * buf, size_t len, size_t * out) = nullptr;
    int (*write_begin)(void * ud, const char * key, void ** handle) = nullptr;
    int (*write_append)(void * ud, void * handle, const void * buf, size_t len) = nullptr;
    int (*write_commit)(void * ud, void * handle) = nullptr;
    int (*write_abort)(void * ud, void * handle) = nullptr;
    int (*remove)(void * ud, const char * key) = nullptr;
    int (*list)(void * ud, const char * prefix, ListSink sink, void * sink_ud) = nullptr;
    /// Optional: a host-side copy; without it `copyObject` streams through `read` and `write_*`.
    int (*copy)(void * ud, const char * from_key, const char * to_key) = nullptr;
    const char * (*last_error)(void * ud) = nullptr;

    /// Throws CALLBACK_OBJECT_STORAGE_ERROR, with the host's last_error text, if `code` is nonzero.
    void check(int code, const char * operation, const String & key) const;
};

using CallbackObjectStorageOpsPtr = std::shared_ptr<const CallbackObjectStorageOps>;

/// What `metadata` reports for a key; shared by the storage and its read buffer.
struct CallbackObjectStat
{
    uint64_t size = 0;
    int64_t mtime = 0;
};

std::optional<CallbackObjectStat> statCallbackObject(const CallbackObjectStorageOps & ops, const String & key);

}
