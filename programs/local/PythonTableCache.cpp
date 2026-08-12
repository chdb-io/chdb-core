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
        for (auto & [name, entry] : py_table_cache)
            entry.obj.release();
        return;
    }
    try
    {
        py::gil_scoped_acquire acquire;
        std::list<PandasTableMetaPtr> metas;
        std::unordered_map<String, NamedEntry> bindings;
        {
            std::lock_guard lock(state_mutex);
            metas.swap(meta_lru);
            bindings.swap(py_table_cache);
            bound_names_by_token.clear();
        }
        metas.clear();
        bindings.clear();
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

/// Unique Python(...) table names referenced by the query, in match order.
/// The object name is extracted from the query string, referenced as
/// Python(var_name) / Python('var_name') / python("var_name").
static std::vector<String> extractPythonTableNames(const String & query_str)
{
    // RE2 pattern to match Python()/python() patterns with single/double quotes or no quotes
    static const RE2 pattern(R"([Pp]ython\s*\(\s*(?:['"]([^'"]+)['"]|([a-zA-Z_][a-zA-Z0-9_]*))\s*\))");

    std::vector<String> names;
    re2::StringPiece input(query_str);
    std::string quoted_match, unquoted_match;
    while (RE2::FindAndConsume(&input, pattern, &quoted_match, &unquoted_match))
    {
        const auto & matched = !quoted_match.empty() ? quoted_match : unquoted_match;
        if (!matched.empty() && std::find(names.begin(), names.end(), matched) == names.end())
            names.push_back(matched);
    }
    return names;
}

UInt64 PythonTableCache::findQueryableObjFromQuery(const String & query_str)
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

    const auto names = extractPythonTableNames(query_str);

    /// Nothing under state_mutex may run Python bytecode: a query thread can
    /// block on the mutex while holding the GIL, so a mutex holder yielding
    /// the GIL mid-bytecode would deadlock. Dead metadata entries are only
    /// unlinked under the lock (weakref liveness via the C API, no bytecode)
    /// and destroyed after it is released.
    std::list<PandasTableMetaPtr> dead_meta;
    {
        std::lock_guard lock(state_mutex);

        /// New query: cached-metadata witnesses must be re-checked once per query.
        ++query_epoch;

        /// Unlink entries whose DataFrame died, so cached backing arrays are
        /// not kept alive longer than one query boundary.
        for (auto it = meta_lru.begin(); it != meta_lru.end();)
        {
            /// PyWeakref_GetObject is deprecated in favour of PyWeakref_GetRef,
            /// but the abi3 wheel is built against the 3.9 limited API where
            /// GetRef (3.13+) does not exist. The borrowed pointer is only
            /// COMPARED to Py_None under the GIL, never dereferenced, so the
            /// borrowed-reference hazard does not apply here.
            PyObject * wr = (*it)->weak_ref.ptr();
            if (wr != nullptr && PyWeakref_GetObject(wr) == Py_None)
                dead_meta.splice(dead_meta.end(), meta_lru, it++);
            else
                ++it;
        }
    }
    dead_meta.clear();
    drainPyBorrowGuardQueue();

    if (names.empty())
        return 0;

    std::vector<String> bound;
    bound.reserve(names.size());
    try
    {
        for (const auto & matched : names)
        {
            {
                /// A name another in-flight query already bound is reused (and
                /// kept alive by its refcount); the expensive inspect frame walk
                /// only runs for names not currently bound.
                std::lock_guard lock(state_mutex);
                if (auto it = py_table_cache.find(matched); it != py_table_cache.end())
                {
                    ++it->second.refcount;
                    bound.push_back(matched);
                    continue;
                }
            }

            auto obj = findQueryableObj(matched); /// frame walk: GIL only, no lock
            if (obj.is_none())
                continue;

            std::lock_guard lock(state_mutex);
            if (auto it = py_table_cache.find(matched); it != py_table_cache.end())
                ++it->second.refcount; /// another thread bound it meanwhile
            else
                py_table_cache.emplace(matched, NamedEntry{std::move(obj), 1});
            bound.push_back(matched);
        }
    }
    catch (...)
    {
        /// A frame-walk failure mid-loop must not strand the bumps already
        /// made: no token would record them, pinning the objects until close.
        releaseBoundNames(bound);
        throw;
    }

    if (bound.empty())
        return 0;

    /// Process-global: a stale thread-local token (prefill whose execute never
    /// ran, e.g. connection closed in between) can be consumed by a later
    /// query on another connection; distinct values make that a no-op instead
    /// of releasing an unrelated in-flight query's bindings.
    static std::atomic<UInt64> global_bind_token{0};

    std::lock_guard lock(state_mutex);
    const UInt64 token = ++global_bind_token;
    bound_names_by_token.emplace(token, std::move(bound));
    return token;
}

void PythonTableCache::releaseBoundNames(const std::vector<String> & names)
{
    /// No GIL required: erased references go through the deferred-decref
    /// queue, so this is safe from any exit path regardless of lock/GIL context.
    std::vector<py::object> released;
    {
        std::lock_guard lock(state_mutex);
        for (const auto & name : names)
        {
            auto it = py_table_cache.find(name);
            if (it == py_table_cache.end())
                continue;
            if (it->second.refcount <= 1)
            {
                released.push_back(std::move(it->second.obj));
                py_table_cache.erase(it);
            }
            else
                --it->second.refcount;
        }
    }
    for (auto & obj : released)
        enqueuePyDecref(obj.release().ptr());
}

void PythonTableCache::releaseQueryableObjs(UInt64 token)
{
    if (token == 0)
        return;

    std::vector<String> names;
    {
        std::lock_guard lock(state_mutex);
        auto rec = bound_names_by_token.find(token);
        if (rec == bound_names_by_token.end())
            return;
        names = std::move(rec->second);
        bound_names_by_token.erase(rec);
    }
    releaseBoundNames(names);
}

py::object PythonTableCache::getQueryableObj(const String & table_name)
{
    /// The returned copy increfs, which is only safe under the GIL - the
    /// state_mutex serializes map access, not refcount manipulation.
    py::gil_assert();

    std::lock_guard lock(state_mutex);

    auto iter = py_table_cache.find(table_name);

    if (iter != py_table_cache.end())
        return iter->second.obj;

    return py::none();
}

void PythonTableCache::clear()
{
    try
    {
        py::gil_scoped_acquire acquire;
        std::unordered_map<String, NamedEntry> dropped;
        {
            std::lock_guard lock(state_mutex);
            dropped.swap(py_table_cache);
            bound_names_by_token.clear();
        }
        dropped.clear();
        drainPyBorrowGuardQueue();
    }
    catch (...)
    {
    }
}

void PythonTableCache::dropMeta(PyObject * df_ptr)
{
    std::list<PandasTableMetaPtr> dropped;
    {
        std::lock_guard lock(state_mutex);
        for (auto it = meta_lru.begin(); it != meta_lru.end();)
        {
            if ((*it)->df_ptr == df_ptr)
                dropped.splice(dropped.end(), meta_lru, it++);
            else
                ++it;
        }
    }
    dropped.clear(); /// entry destructors run outside the lock
}

PandasTableMetaPtr PythonTableCache::findValidatedPandasMeta(const py::handle & df)
{
    if (!pandasMetaCacheEnabled())
        return nullptr;

    py::gil_assert();

    PandasTableMetaPtr entry;
    UInt64 epoch_at_validation = 0;
    {
        std::lock_guard lock(state_mutex);
        auto it = std::find_if(meta_lru.begin(), meta_lru.end(), [&](const PandasTableMetaPtr & e) { return e->df_ptr == df.ptr(); });
        if (it == meta_lru.end())
            return nullptr;
        entry = *it;
        if (entry->validated_epoch == query_epoch)
        {
            meta_lru.splice(meta_lru.begin(), meta_lru, it);
            return entry;
        }
        epoch_at_validation = query_epoch;
    }

    /// Witness checks call into Python (pandas properties), so they must run
    /// without state_mutex: a mutex holder yielding the GIL mid-bytecode
    /// would deadlock against a GIL-holding thread blocked on the mutex.
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

    std::lock_guard lock(state_mutex);
    auto it = std::find_if(meta_lru.begin(), meta_lru.end(), [&](const PandasTableMetaPtr & e) { return e.get() == entry.get(); });
    if (it == meta_lru.end())
        return nullptr; /// concurrently invalidated while we validated
    if (!valid)
    {
        meta_lru.erase(it);
        return nullptr;
    }

    /// Stamp the epoch the witnesses were checked against, not the current
    /// one: a prefill may have bumped it while validation ran unlocked.
    entry->validated_epoch = epoch_at_validation;
    meta_lru.splice(meta_lru.begin(), meta_lru, it);
    return entry;
}

void PythonTableCache::storePandasMeta(const py::handle & df, const DB::ColumnsDescription & schema)
{
    if (!pandasMetaCacheEnabled())
        return;

    py::gil_assert();

    try
    {
        /// Built entirely outside state_mutex: witness construction calls
        /// into Python (see findValidatedPandasMeta for the lock rule).
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

        std::list<PandasTableMetaPtr> displaced;
        {
            std::lock_guard lock(state_mutex);
            entry->validated_epoch = query_epoch;
            for (auto it = meta_lru.begin(); it != meta_lru.end();)
            {
                if ((*it)->df_ptr == df.ptr())
                    displaced.splice(displaced.end(), meta_lru, it++);
                else
                    ++it;
            }
            meta_lru.push_front(std::move(entry));
            while (meta_lru.size() > max_meta_entries)
            {
                displaced.splice(displaced.end(), meta_lru, std::prev(meta_lru.end()));
            }
        }
        displaced.clear(); /// destroyed outside the lock
    }
    catch (...)
    {
        /// Building the witness set failed: skip caching, behavior stays per-query.
    }
}

} // namespace CHDB
