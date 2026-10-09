#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/Callback/CallbackObjectStorageHost.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// One registration of a host object storage: the host's operations plus the guarantees the engine adds
/// around every call into them. Depends on base/types.h and the standard library alone so that the C API
/// translation unit can include it without the object storage headers.
///
/// - Fencing: after fence() returns no call reaches the host again; every later operation throws
///   CALLBACK_OBJECT_STORAGE_ERROR. Disks made from this registration may outlive it (they live until the
///   last connection closes), so this is what makes unregistering safe while they exist.
/// - Re-entrancy: isInsideCallback() is true on a thread while it runs host code, so the C API can refuse
///   a chdb_* call from a callback instead of deadlocking on the connection mutex.
/// - Errors: host failures become CALLBACK_OBJECT_STORAGE_ERROR naming the store, the operation, the key
///   and the host's message; a C++ exception escaping the host is reported the same way.
/// - Keyspaces: attachDisk() gives one read-write disk exclusive use of a key prefix in this process and
///   lets the host gate it across processes.
class CallbackObjectStorageOps
{
public:
    CallbackObjectStorageOps(String name_, std::unique_ptr<ICallbackObjectStorageHost> host_);

    const String & getName() const { return name; }

    std::optional<CallbackObjectStat> stat(const String & key) const;
    /// May return fewer bytes than len; 0 means the end of the blob.
    size_t read(const String & key, uint64_t offset, char * buf, size_t len) const;

    void * writeBegin(const String & key) const;
    void writeAppend(void * handle, const String & key, const char * buf, size_t len) const;
    /// The handle is released even when this throws.
    void writeCommit(void * handle, const String & key) const;
    void writeAbort(void * handle) const noexcept;

    /// Removing a missing blob is not an error.
    void remove(const String & key) const;
    /// `sink` may throw: the listing then stops and the exception is rethrown here.
    void list(const String & prefix, const ICallbackObjectStorageHost::ListSink & sink) const;

    bool hasCopy() const { return host->hasCopy(); }
    void copy(const String & from_key, const String & to_key) const;

    /// Opens a disk on key_prefix: a read-write disk claims the keyspace (see claimKeyspace), then the
    /// host's open_keyspace hook runs. detachDisk() undoes both and must follow every successful attach.
    void attachDisk(const String & key_prefix, bool read_only);
    void detachDisk(const String & key_prefix, bool read_only) noexcept;
    /// Disks attached and not yet detached. A disk is detached when it shuts down, which is when the
    /// last connection closes, even if something still references the object afterwards.
    bool hasAttachedDisks() const;

    /// Throws BAD_ARGUMENTS when a live read-write disk of this store already holds an overlapping prefix
    /// (equal, empty, or one a prefix of the other once trailing '/' are dropped). Released by
    /// releaseKeyspace(); a store can run one plain_rewritable writer per keyspace.
    void claimKeyspace(const String & key_prefix);
    void releaseKeyspace(const String & key_prefix) noexcept;

    /// Refuses every later call and waits for the calls in flight to return. Idempotent.
    void fence();

    static bool isInsideCallback();

private:
    class CallScope;

    using Status = ICallbackObjectStorageHost::Status;

    /// Runs `call` inside a CallScope; a C++ exception escaping the host becomes Status::Error.
    template <typename F>
    Status invoke(const char * operation, const String & key, String & error, F && call) const;

    [[noreturn]] void throwFailure(const char * operation, const String & key, Status status, const String & error) const;

    /// The host's own (cross-process) gate for a keyspace; no-ops when the host supplies no hooks.
    void openKeyspace(const String & key_prefix, bool read_only) const;
    void closeKeyspace(const String & key_prefix) const noexcept;

    const String name;
    const std::unique_ptr<ICallbackObjectStorageHost> host;

    mutable std::mutex mutex;
    mutable std::condition_variable drained;
    mutable size_t in_flight = 0;
    bool fenced = false;
    std::vector<String> claimed_keyspaces;
    size_t attached_disks = 0;
};

using CallbackObjectStorageOpsPtr = std::shared_ptr<CallbackObjectStorageOps>;

}
