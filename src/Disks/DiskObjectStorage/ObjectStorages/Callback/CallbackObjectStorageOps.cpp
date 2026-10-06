#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageOps.h>

#include <Common/Exception.h>

#include <algorithm>
#include <exception>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CALLBACK_OBJECT_STORAGE_ERROR;
}

namespace
{

/// Host frames currently on this thread's stack.
thread_local size_t callback_depth = 0;

/// Keys are joined to the prefix with '/', so 'a' and 'a/' name the same keyspace.
std::string_view normalizeKeyspace(std::string_view key_prefix)
{
    while (!key_prefix.empty() && key_prefix.back() == '/')
        key_prefix.remove_suffix(1);
    return key_prefix;
}

}

/// Counts a call in flight unless the registration is fenced.
class CallbackObjectStorageOps::CallScope
{
public:
    explicit CallScope(const CallbackObjectStorageOps & ops_)
        : ops(ops_)
    {
        std::lock_guard lock(ops.mutex);
        entered = !ops.fenced;
        if (entered)
        {
            ++ops.in_flight;
            ++callback_depth;
        }
    }

    ~CallScope()
    {
        if (!entered)
            return;
        --callback_depth;
        std::lock_guard lock(ops.mutex);
        if (--ops.in_flight == 0 && ops.fenced)
            ops.drained.notify_all();
    }

    CallScope(const CallScope &) = delete;
    CallScope & operator=(const CallScope &) = delete;

    bool isEntered() const { return entered; }

private:
    const CallbackObjectStorageOps & ops;
    bool entered = false;
};

CallbackObjectStorageOps::CallbackObjectStorageOps(String name_, std::unique_ptr<ICallbackObjectStorageHost> host_)
    : name(std::move(name_))
    , host(std::move(host_))
{
}

template <typename F>
CallbackObjectStorageOps::Status
CallbackObjectStorageOps::invoke(const char * operation, const String & key, String & error, F && call) const
{
    CallScope scope(*this);
    if (!scope.isEntered())
        throw Exception(
            ErrorCodes::CALLBACK_OBJECT_STORAGE_ERROR,
            "Callback object storage '{}': {} of '{}' refused: the storage was unregistered",
            name,
            operation,
            key);
    try
    {
        return call();
    }
    catch (...)
    {
        error = "the callback threw an exception: " + getCurrentExceptionMessage(false);
        return Status::Error;
    }
}

void CallbackObjectStorageOps::throwFailure(const char * operation, const String & key, Status status, const String & error) const
{
    throw Exception(
        ErrorCodes::CALLBACK_OBJECT_STORAGE_ERROR,
        "Callback object storage '{}': {} of '{}' failed: {}{}{}",
        name,
        operation,
        key,
        status == Status::NotFound ? "not found" : "error",
        error.empty() ? "" : ": ",
        error);
}

std::optional<CallbackObjectStat> CallbackObjectStorageOps::stat(const String & key) const
{
    CallbackObjectStat result;
    String error;
    const auto status = invoke("metadata", key, error, [&] { return host->metadata(key.c_str(), result, error); });
    if (status == Status::NotFound)
        return std::nullopt;
    if (status != Status::Ok)
        throwFailure("metadata", key, status, error);
    return result;
}

size_t CallbackObjectStorageOps::read(const String & key, uint64_t offset, char * buf, size_t len) const
{
    size_t bytes_read = 0;
    String error;
    const auto status = invoke("read", key, error, [&] { return host->read(key.c_str(), offset, buf, len, bytes_read, error); });
    if (status != Status::Ok)
        throwFailure("read", key, status, error);
    /// A host that overruns the buffer broke the contract; it is an external failure, not an engine invariant.
    if (bytes_read > len)
        throw Exception(
            ErrorCodes::CALLBACK_OBJECT_STORAGE_ERROR,
            "Callback object storage '{}': read of '{}' at offset {} returned {} bytes for a {} byte buffer",
            name,
            key,
            offset,
            bytes_read,
            len);
    return bytes_read;
}

void * CallbackObjectStorageOps::writeBegin(const String & key) const
{
    void * handle = nullptr;
    String error;
    const auto status = invoke("write_begin", key, error, [&] { return host->writeBegin(key.c_str(), handle, error); });
    if (status != Status::Ok)
        throwFailure("write_begin", key, status, error);
    return handle;
}

void CallbackObjectStorageOps::writeAppend(void * handle, const String & key, const char * buf, size_t len) const
{
    String error;
    const auto status = invoke("write_append", key, error, [&] { return host->writeAppend(handle, buf, len, error); });
    if (status != Status::Ok)
        throwFailure("write_append", key, status, error);
}

void CallbackObjectStorageOps::writeCommit(void * handle, const String & key) const
{
    String error;
    const auto status = invoke("write_commit", key, error, [&] { return host->writeCommit(handle, error); });
    if (status != Status::Ok)
        throwFailure("write_commit", key, status, error);
}

void CallbackObjectStorageOps::writeAbort(void * handle) const noexcept
{
    CallScope scope(*this);
    if (!scope.isEntered())
        return;
    try
    {
        host->writeAbort(handle);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void CallbackObjectStorageOps::remove(const String & key) const
{
    String error;
    const auto status = invoke("remove", key, error, [&] { return host->remove(key.c_str(), error); });
    if (status == Status::Error)
        throwFailure("remove", key, status, error);
}

void CallbackObjectStorageOps::list(const String & prefix, const ICallbackObjectStorageHost::ListSink & sink) const
{
    /// The host may call the sink from any thread; calls are serialized here so a parallel lister cannot
    /// race on the caller's state, and an exception stops the listing instead of crossing the C ABI.
    std::mutex sink_mutex;
    std::exception_ptr sink_error;
    const ICallbackObjectStorageHost::ListSink guarded_sink = [&](const char * key, uint64_t size, int64_t mtime)
    {
        std::lock_guard lock(sink_mutex);
        if (sink_error)
            return true;
        try
        {
            if (!key)
                throw Exception(
                    ErrorCodes::CALLBACK_OBJECT_STORAGE_ERROR, "Callback object storage '{}': list of '{}' passed a NULL key", name, prefix);
            return sink(key, size, mtime);
        }
        catch (...)
        {
            sink_error = std::current_exception();
            return true;
        }
    };

    String error;
    const auto status = invoke("list", prefix, error, [&] { return host->list(prefix.c_str(), guarded_sink, error); });
    if (sink_error)
        std::rethrow_exception(sink_error);
    /// An empty prefix is not an error, however the host spells it.
    if (status == Status::Error)
        throwFailure("list", prefix, status, error);
}

void CallbackObjectStorageOps::copy(const String & from_key, const String & to_key) const
{
    String error;
    const auto status = invoke("copy", from_key, error, [&] { return host->copy(from_key.c_str(), to_key.c_str(), error); });
    if (status != Status::Ok)
        throwFailure("copy", from_key, status, error);
}

void CallbackObjectStorageOps::attachDisk(const String & key_prefix, bool read_only)
{
    /// Read-only disks load a snapshot and never write, so they may share a keyspace, as upstream allows
    /// for read-only plain disks.
    if (!read_only)
        claimKeyspace(key_prefix);
    try
    {
        openKeyspace(key_prefix, read_only);
    }
    catch (...)
    {
        if (!read_only)
            releaseKeyspace(key_prefix);
        throw;
    }
    std::lock_guard lock(mutex);
    ++attached_disks;
}

void CallbackObjectStorageOps::detachDisk(const String & key_prefix, bool read_only) noexcept
{
    closeKeyspace(key_prefix);
    if (!read_only)
        releaseKeyspace(key_prefix);
    std::lock_guard lock(mutex);
    --attached_disks;
}

bool CallbackObjectStorageOps::hasAttachedDisks() const
{
    std::lock_guard lock(mutex);
    return attached_disks != 0;
}

void CallbackObjectStorageOps::claimKeyspace(const String & key_prefix)
{
    const auto keyspace = normalizeKeyspace(key_prefix);
    std::lock_guard lock(mutex);
    for (const auto & claimed : claimed_keyspaces)
    {
        if (keyspace.starts_with(claimed) || std::string_view(claimed).starts_with(keyspace))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Callback object storage '{}': key_prefix '{}' overlaps key_prefix '{}' of a disk already open on this store. "
                "Give every disk on one store its own prefix, none a prefix of another (an empty prefix overlaps every other)",
                name,
                key_prefix,
                claimed);
    }
    claimed_keyspaces.emplace_back(keyspace);
}

void CallbackObjectStorageOps::releaseKeyspace(const String & key_prefix) noexcept
{
    const auto keyspace = normalizeKeyspace(key_prefix);
    std::lock_guard lock(mutex);
    auto it = std::find(claimed_keyspaces.begin(), claimed_keyspaces.end(), keyspace);
    if (it != claimed_keyspaces.end())
        claimed_keyspaces.erase(it);
}

void CallbackObjectStorageOps::openKeyspace(const String & key_prefix, bool read_only) const
{
    if (!host->hasKeyspaceHooks())
        return;
    String error;
    const auto status
        = invoke("open_keyspace", key_prefix, error, [&] { return host->openKeyspace(key_prefix.c_str(), read_only, error); });
    if (status != Status::Ok)
        throwFailure("open_keyspace", key_prefix, status, error);
}

void CallbackObjectStorageOps::closeKeyspace(const String & key_prefix) const noexcept
{
    if (!host->hasKeyspaceHooks())
        return;
    CallScope scope(*this);
    if (!scope.isEntered())
        return;
    try
    {
        host->closeKeyspace(key_prefix.c_str());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void CallbackObjectStorageOps::fence()
{
    std::unique_lock lock(mutex);
    fenced = true;
    drained.wait(lock, [&] { return in_flight == 0; });
}

bool CallbackObjectStorageOps::isInsideCallback()
{
    return callback_depth > 0;
}

}
