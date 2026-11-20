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

    /// Register or replace a persistent connection-scoped Python table (`Python(name)`).
    /// Must be called with the GIL held.
    void registerObject(const String & name, const py::object & obj);

    /// Unregister a previously registered connection-scoped Python table (idempotent).
    /// Must be called with the GIL held.
    void unregisterObject(const String & name);

    /// Bind per-query `explicit_tables` (if provided), connection-registered tables,
    /// and caller-frame `Python(name)` fallbacks into an isolated token slot.
    /// Returns 0 when nothing was bound. Must be called with the GIL held.
    UInt64 findQueryableObjFromQuery(const String & query_str, const py::object & explicit_tables = py::none());

    /// Associate/disassociate a live `DB::Context *` (`query_context`) with a bind token
    /// so concurrent queries on the same connection see their own per-query `tables=` bindings.
    void attachQueryContext(const void * query_ctx, UInt64 token);
    void detachQueryContext(const void * query_ctx) noexcept;

    /// Drop the bindings associated with `token`. Safe without the GIL — released
    /// references are routed through the deferred-decref queue.
    void releaseQueryableObjs(UInt64 token);

    /// Resolve `table_name` for the currently executing query context (per-query token
    /// first, then connection-registered tables). Must be called with the GIL held.
    py::object getQueryableObj(const String & table_name);

    void clear();

    /// Pandas metadata cache. All methods must be called with the GIL held.
    PandasTableMetaPtr findValidatedPandasMeta(const py::handle & df);
    void storePandasMeta(const py::handle & df, const DB::ColumnsDescription & schema);
    void invalidatePandasMeta(PyObject * df_ptr) { dropMeta(df_ptr); }

private:
    /// Locks state_mutex internally; entry destructors run outside the lock.
    void dropMeta(PyObject * df_ptr);

    /// Guards registered_tables, bound_objs_by_token, token_by_query_context, meta_lru,
    /// and query_epoch. Lock order is strictly `GIL -> state_mutex`; no Python bytecode
    /// or arbitrary object destructors may execute while `state_mutex` is held.
    std::mutex state_mutex;

    /// Persistent connection-level tables registered via `conn.register(name, obj)`.
    std::unordered_map<String, py::object> registered_tables;

    /// Per-query/per-stream bound objects keyed by unique monotonic bind token.
    std::unordered_map<UInt64, std::unordered_map<String, py::object>> bound_objs_by_token;

    /// Maps active `DB::Context *` (`query_context`) -> bind token.
    std::unordered_map<const void *, UInt64> token_by_query_context;

    static constexpr size_t max_meta_entries = 4;
    std::list<PandasTableMetaPtr> meta_lru;
    UInt64 query_epoch = 0;
};

} // namespace CHDB
