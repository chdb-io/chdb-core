#include "PythonTableCache.h"
#include "NumpyType.h"
#include "PyBorrowGuard.h"
#include "PybindWrapper.h"
#include "PythonArrowStream.h"
#include "PythonUtils.h"

#include <Common/re2.h>

namespace CHDB {

static bool pandasMetaCacheEnabled()
{
    static const bool enabled = []
    {
        const char * env = getenv("CHDB_PANDAS_META_CACHE"); // NOLINT(concurrency-mt-unsafe)
        return !(env && env[0] == '0');
    }();
    return enabled;
}

static py::object makeWeakref(const py::handle & obj)
{
    static py::handle weakref_ref = []
    {
        py::object ref_fn = py::module_::import("weakref").attr("ref");
        return ref_fn.release();
    }();
    return weakref_ref(obj);
}

PandasTableMeta::~PandasTableMeta()
{
    if (!Py_IsInitialized())
    {
        /// Interpreter is gone: decref / GIL are unusable. Leak the Python
        /// references (and the wrappers, whose destructors take the GIL)
        /// instead of crashing at teardown.
        weak_ref.release();
        columns_index.release();
        for (auto & dtype_obj : dtype_objs)
            dtype_obj.release();
        for (auto & sb : str_backings)
        {
            sb.label.release();
            sb.chunked_wr.release();
        }
        return;
    }
    try
    {
        py::gil_scoped_acquire acquire;
        weak_ref.release().dec_ref();
        columns_index.release().dec_ref();
        dtype_objs.clear();
        str_backings.clear();
        arrow_wrappers.clear();
    }
    catch (...)
    {
    }
}

PythonTableCache::~PythonTableCache()
{
    if (!Py_IsInitialized())
    {
        auto * leaked = new std::list<PandasTableMetaPtr>(std::move(meta_lru));
        (void)leaked;
        return;
    }
    try
    {
        py::gil_scoped_acquire acquire;
        std::lock_guard lock(state_mutex);
        meta_lru.clear();
        drainPyBorrowGuardQueue();
    }
    catch (...)
    {
    }
}

/// Function to find instance of PyReader, pandas DataFrame, or PyArrow Table, filtered by variable name
static py::object findQueryableObj(const String & var_name)
{
    py::module inspect = py::module_::import("inspect");
    py::object current_frame = inspect.attr("currentframe")();

    while (!current_frame.is_none())
    {
        // Get f_locals and f_globals
        py::object locals_obj = current_frame.attr("f_locals");
        py::object globals_obj = current_frame.attr("f_globals");

        // For each namespace (locals and globals)
        for (const auto & namespace_obj : {locals_obj, globals_obj})
        {
            // Use Python's __contains__ method to check if the key exists
            // This works with both regular dicts and FrameLocalsProxy (Python 3.13+)
            if (py::bool_(namespace_obj.attr("__contains__")(var_name)))
            {
                py::object obj;
                try
                {
                    // Get the object using Python's indexing syntax
                    obj = namespace_obj[py::cast(var_name)];
                    if (DB::isInheritsFromPyReader(obj) || DB::isPandasDf(obj) || DB::isPyarrowTable(obj)
                        || CHDB::hasArrowCStreamMethod(obj) || DB::hasGetItem(obj))
                    {
                        return obj;
                    }
                }
                catch (const py::error_already_set &)
                {
                    continue; // If getting the value fails, continue to the next namespace
                }
            }
        }

        // Move to the parent frame
        current_frame = current_frame.attr("f_back");
    }

    // Object not found
    return py::none();
}

void PythonTableCache::findQueryableObjFromQuery(const String & query_str)
{
    // Find the queriable object in the Python environment
    // return nullptr if no Python obj is referenced in query string
    // return py::none if the obj referenced not found
    // return the Python object if found
    // The object name is extracted from the query string, must referenced by
    // Python(var_name) or Python('var_name') or python("var_name") or python('var_name')
    // such as:
    //  - `SELECT * FROM Python('PyReader')`
    //  - `SELECT * FROM Python(PyReader_instance)`
    //  - `SELECT * FROM Python(some_var_with_type_pandas_DataFrame_or_pyarrow_Table)`
    // The object can be any thing that Python Table supported, like PyReader, pandas DataFrame, or PyArrow Table
    // The object should be in the global or local scope

    py::gil_assert();

    std::lock_guard lock(state_mutex);

    /// New query: cached-metadata witnesses must be re-checked once per query.
    ++query_epoch;
    drainPyBorrowGuardQueue();

    /// Evict entries whose DataFrame died, so cached backing arrays (string
    /// payloads) are not kept alive longer than one query boundary.
    meta_lru.remove_if([](const PandasTableMetaPtr & e)
    {
        try
        {
            return e->weak_ref.ptr() != nullptr && e->weak_ref().is_none();
        }
        catch (...)
        {
            return false;
        }
    });

    // RE2 pattern to match Python()/python() patterns with single/double quotes or no quotes
    static const RE2 pattern(R"([Pp]ython\s*\(\s*(?:['"]([^'"]+)['"]|([a-zA-Z_][a-zA-Z0-9_]*))\s*\))");

    re2::StringPiece input(query_str);
    std::string quoted_match, unquoted_match;

    // Try to match and extract the groups
    while (RE2::FindAndConsume(&input, pattern, &quoted_match, &unquoted_match))
    {
        // Skip the (expensive) inspect frame walk when the name is already cached;
        // emplace never overwrites an existing entry anyway.
        const auto & matched = !quoted_match.empty() ? quoted_match : unquoted_match;
        if (matched.empty() || py_table_cache.contains(matched))
            continue;

        auto handle = findQueryableObj(matched);
        if (!handle.is_none())
            py_table_cache.emplace(matched, handle);
    }
}

py::handle PythonTableCache::getQueryableObj(const String & table_name)
{
    std::lock_guard lock(state_mutex);

    auto iter = py_table_cache.find(table_name);

    if (iter != py_table_cache.end())
        return iter->second;

    return py::none();
}

void PythonTableCache::clear()
{
    try
	{
        py::gil_scoped_acquire acquire;
        std::lock_guard lock(state_mutex);
        py_table_cache.clear();
        drainPyBorrowGuardQueue();
	}
	catch (...)
	{
	}
}

void PythonTableCache::dropMeta(PyObject * df_ptr)
{
    meta_lru.remove_if([&](const PandasTableMetaPtr & e) { return e->df_ptr == df_ptr; });
}

PandasTableMetaPtr PythonTableCache::findValidatedPandasMeta(const py::handle & df)
{
    if (!pandasMetaCacheEnabled())
        return nullptr;

    py::gil_assert();

    std::lock_guard lock(state_mutex);

    auto it = std::find_if(meta_lru.begin(), meta_lru.end(), [&](const PandasTableMetaPtr & e) { return e->df_ptr == df.ptr(); });
    if (it == meta_lru.end())
        return nullptr;

    auto entry = *it;
    if (entry->validated_epoch == query_epoch)
    {
        meta_lru.splice(meta_lru.begin(), meta_lru, it);
        return entry;
    }

    bool valid = false;
    try
    {
        do
        {
            py::object alive = entry->weak_ref();
            if (!alive.is(df))
                break;
            if (static_cast<size_t>(py::len(df)) != entry->row_count)
                break;
            py::object columns = df.attr("columns");
            if (columns.ptr() != entry->columns_index.ptr())
                break;
            py::list dtypes = py::list(df.attr("dtypes"));
            if (dtypes.size() != entry->dtype_objs.size())
                break;
            bool dtypes_match = true;
            for (size_t i = 0; i < entry->dtype_objs.size(); ++i)
            {
                py::object cur = dtypes[i];
                if (cur.ptr() != entry->dtype_objs[i].ptr())
                {
                    dtypes_match = false;
                    break;
                }
            }
            if (!dtypes_match)
                break;
            bool backings_match = true;
            for (const auto & sb : entry->str_backings)
            {
                py::object alive = sb.chunked_wr();
                if (alive.is_none())
                {
                    backings_match = false;
                    break;
                }
                py::object series = df[sb.label];
                py::object arr = series.attr("array");
                if (!py::hasattr(arr, "_pa_array") || !alive.is(arr.attr("_pa_array")))
                {
                    backings_match = false;
                    break;
                }
            }
            if (!backings_match)
                break;
            valid = true;
        } while (false);
    }
    catch (...)
    {
        valid = false;
    }

    if (!valid)
    {
        meta_lru.erase(it);
        return nullptr;
    }

    entry->validated_epoch = query_epoch;
    meta_lru.splice(meta_lru.begin(), meta_lru, it);
    return entry;
}

void PythonTableCache::storePandasMeta(const py::handle & df, const DB::ColumnsDescription & schema)
{
    if (!pandasMetaCacheEnabled())
        return;

    py::gil_assert();

    std::lock_guard lock(state_mutex);

    try
    {
        auto entry = std::make_shared<PandasTableMeta>();
        entry->df_ptr = df.ptr();
        entry->schema = schema;
        entry->row_count = py::len(df);
        entry->columns_index = df.attr("columns");
        py::list names = py::list(entry->columns_index);
        py::list dtypes = py::list(df.attr("dtypes"));
        if (names.size() != dtypes.size())
            return;

        for (size_t i = 0; i < names.size(); ++i)
        {
            py::object name_obj = names[i];
            py::object dtype_obj = dtypes[i];
            auto numpy_type = ConvertNumpyType(dtype_obj);
            switch (numpy_type.type)
            {
                case NumpyNullableType::OBJECT:
                case NumpyNullableType::CATEGORY:
                    return; /// value-derived schema / per-query wrapper side effects: not cacheable
                case NumpyNullableType::STRING:
                {
                    py::object series = df[name_obj];
                    py::object arr = series.attr("array");
                    if (py::hasattr(arr, "_pa_array"))
                        entry->str_backings.push_back({i, name_obj, makeWeakref(arr.attr("_pa_array"))});
                    break;
                }
                default:
                    break;
            }
            entry->dtype_objs.emplace_back(std::move(dtype_obj));
        }

        entry->weak_ref = makeWeakref(df);
        entry->validated_epoch = query_epoch;

        dropMeta(df.ptr());
        meta_lru.push_front(std::move(entry));
        while (meta_lru.size() > max_meta_entries)
            meta_lru.pop_back();
    }
    catch (...)
    {
        /// Building the witness set failed: skip caching, behavior stays per-query.
    }
}

} // namespace CHDB
