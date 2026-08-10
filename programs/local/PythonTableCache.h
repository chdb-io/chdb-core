#pragma once

#include "PybindWrapper.h"
#include "PythonUtils.h"

#include <list>
#include <memory>
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
    /// (column position, original label object, backing ChunkedArray) per
    /// string column. The label is kept as a Python object so validation
    /// indexes the frame correctly for non-string labels too.
    struct StrBacking
    {
        size_t pos;
        py::object label;
        py::object chunked;
    };
    std::vector<StrBacking> str_backings;
    size_t row_count = 0;
    DB::ColumnsDescription schema;
    bool has_column_sizes = false;
    DB::IStorage::ColumnSizeByName column_sizes;
    std::unordered_map<std::string, std::shared_ptr<DB::ColumnWrapper>> arrow_wrappers;
    UInt64 validated_epoch = 0;

    ~PandasTableMeta();
};

using PandasTableMetaPtr = std::shared_ptr<PandasTableMeta>;

class PythonTableCache {
public:
    ~PythonTableCache();

    void findQueryableObjFromQuery(const String & query_str);

    py::handle getQueryableObj(const String & table_name);

    void clear();

    /// Pandas metadata cache. All methods must be called with the GIL held.
    PandasTableMetaPtr findValidatedPandasMeta(const py::handle & df);
    void storePandasMeta(const py::handle & df, const DB::ColumnsDescription & schema);
    void invalidatePandasMeta(PyObject * df_ptr) { dropMeta(df_ptr); }

private:
    void dropMeta(PyObject * df_ptr);

    std::unordered_map<String, py::handle> py_table_cache;

    static constexpr size_t max_meta_entries = 4;
    std::list<PandasTableMetaPtr> meta_lru;
    UInt64 query_epoch = 0;
};

} // namespace CHDB
