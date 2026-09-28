#pragma once

#include "PybindWrapper.h"
#include "PythonUtils.h"

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>

namespace CHDB {

/// Session-level cache of per-DataFrame metadata whose recomputation dominates
/// small-query latency: schema inference, memory_usage-based column sizes and
/// the per-chunk Arrow buffer tables. Entries cache only facts that are either
/// re-read live each query, implied by backing-object identity (Arrow arrays
/// are immutable; pandas CoW swaps the backing object on modification), or
/// self-healing location facts. Value-derived facts (object-dtype sampling,
/// category dictionaries) disqualify the whole DataFrame from caching.
struct PandasTableMeta
{
    PyObject * df_ptr = nullptr;
    py::object weak_ref;
    py::object columns_index;
    std::vector<py::object> dtype_objs;
    /// (column position, original label object, weakref to the backing
    /// ChunkedArray) per string column. The label is kept as a Python object
    /// so validation indexes the frame correctly for non-string labels too.
    /// The backing is held only through a weakref: identity validation needs
    /// to know whether the old object DIED, not to keep it alive — a dead
    /// weakref invalidates the entry before any pointer is compared, and a
    /// live one makes the identity check exact. This way the cache never
    /// pins the string payload of a deleted DataFrame.
    struct StrBacking
    {
        size_t pos;
        py::object label;
        py::object chunked_wr;
    };
    std::vector<StrBacking> str_backings;
    size_t row_count = 0;
    DB::ColumnsDescription schema;
    bool has_column_sizes = false;
    DB::IStorage::ColumnSizeByName column_sizes;
    /// Cached per-chunk buffer tables for Arrow string columns: plain PODs
    /// plus the identity of the backing ChunkedArray. No Python references —
    /// validity is re-established per query from the live column (whose
    /// identity the str_backings witness already verified).
    struct ArrowWrapperMaster
    {
        std::vector<DB::ArrowStringChunkView> chunks;
        bool large_offsets = false;
        size_t row_count = 0;
        uintptr_t chunked_ptr = 0;
    };
    std::unordered_map<std::string, ArrowWrapperMaster> arrow_wrappers;
    UInt64 validated_epoch = 0;

    ~PandasTableMeta();
};

using PandasTableMetaPtr = std::shared_ptr<PandasTableMeta>;

class PythonTableCache {
public:
    ~PythonTableCache();

    /// Bind every Python(name) the query references (or bump the refcount of
    /// a binding another in-flight query already made) and return a token
    /// identifying exactly the bindings this call created. 0 = nothing bound.
    UInt64 findQueryableObjFromQuery(const String & query_str);

    /// Drop the bindings the findQueryableObjFromQuery call that returned
    /// `token` created (refcounted: a name stays bound while any in-flight
    /// query on the connection references it). Safe without the GIL - erased
    /// references are released through the deferred-decref queue. Called from
    /// every query-exit path; clear() remains the connection-close full wipe.
    void releaseQueryableObjs(UInt64 token);

    /// Strong reference: the caller's use may outlive a concurrent release
    /// of the binding (Py_INCREF under state_mutex is C API, no bytecode).
    py::object getQueryableObj(const String & table_name);

    void clear();

    /// Pandas metadata cache. All methods must be called with the GIL held.
    PandasTableMetaPtr findValidatedPandasMeta(const py::handle & df);
    void storePandasMeta(const py::handle & df, const DB::ColumnsDescription & schema);
    void invalidatePandasMeta(PyObject * df_ptr) { dropMeta(df_ptr); }

private:
    /// Locks state_mutex internally; entry destructors run outside the lock.
    void dropMeta(PyObject * df_ptr);

    /// Decrement/erase the given name bindings (shared by token release and
    /// the bind-loop exception rollback). GIL not required.
    void releaseBoundNames(const std::vector<String> & names);

    /// Guards py_table_cache, meta_lru and query_epoch. The GIL alone is not
    /// enough: the pybind-side prefill (before client_mutex is taken) can
    /// interleave with an in-flight query's storage-side lookups at GIL yield
    /// points, and on free-threaded builds the GIL is no lock at all. Lock
    /// order is GIL first, then state_mutex; no Python code that could
    /// re-enter this object runs while the mutex is held by the same thread.
    std::mutex state_mutex;

    /// Strong reference plus the number of in-flight queries that bound it.
    /// A query must not clear another query's bindings: with one connection
    /// shared by several threads, thread B's prefill runs while thread A's
    /// query is still executing, and A's exit used to wipe B's entry. The
    /// reference is owned so a binding reused by a later overlapping query
    /// can never dangle when the user rebinds the Python variable meanwhile.
    struct NamedEntry
    {
        py::object obj;
        size_t refcount = 0;
    };
    std::unordered_map<String, NamedEntry> py_table_cache;

    /// Names each outstanding findQueryableObjFromQuery call actually bound,
    /// keyed by its token. Exact pairing: releasing one query can neither
    /// under- nor over-release names bound by a concurrent query, even for
    /// identical query texts with different per-thread name visibility.
    std::unordered_map<UInt64, std::vector<String>> bound_names_by_token;

    static constexpr size_t max_meta_entries = 4;
    std::list<PandasTableMetaPtr> meta_lru;
    UInt64 query_epoch = 0;
};

} // namespace CHDB
