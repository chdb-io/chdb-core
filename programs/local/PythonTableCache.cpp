#include "PythonTableCache.h"
#include "NumpyType.h"
#include "PyArrowTable.h"
#include "PyBorrowGuard.h"
#include "PybindWrapper.h"
#include "PolarsDataFrame.h"
#include "PythonArrowStream.h"
#include "PythonUtils.h"

#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Common/re2.h>

#include <atomic>

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

static bool isQueryablePythonObject(const py::object & obj)
{
    if (!obj || obj.is_none())
        return false;
    PyObject * raw = obj.ptr();
    if (PyUnicode_Check(raw) || PyBytes_Check(raw) || PyByteArray_Check(raw))
        return false;
    return DB::isInheritsFromPyReader(obj) || DB::isPandasDf(obj)
        || CHDB::PyArrowTable::isPyArrowObject(obj)
        || CHDB::hasArrowCStreamMethod(obj) || DB::hasGetItem(obj);
}

static py::object normalizeAndValidateQueryableObj(const String & name, const py::handle & raw_obj)
{
    if (!raw_obj || raw_obj.is_none())
        throw py::type_error("Cannot bind None for Python table '" + name + "'");

    py::object obj = py::reinterpret_borrow<py::object>(raw_obj);
    obj = PolarsDataFrame::normalize(obj);
    if (!isQueryablePythonObject(obj))
    {
        throw py::type_error(
            "Unsupported Python table object for '" + name
            + "': expected pandas DataFrame, pyarrow Table/RecordBatch/Dataset, "
              "polars DataFrame/LazyFrame/Series, PyReader, dict, or an object implementing __arrow_c_stream__");
    }
    return obj;
}

PandasTableMeta::~PandasTableMeta()
{
    if (!Py_IsInitialized())
    {
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
        for (auto & [_, obj] : registered_tables)
            obj.release();
        for (auto & [_, table_map] : bound_objs_by_token)
            for (auto & [__, obj] : table_map)
                obj.release();
        return;
    }
    try
    {
        py::gil_scoped_acquire acquire;
        std::list<PandasTableMetaPtr> metas;
        std::unordered_map<String, py::object> reg_bindings;
        std::unordered_map<UInt64, std::unordered_map<String, py::object>> token_bindings;
        {
            std::lock_guard lock(state_mutex);
            metas.swap(meta_lru);
            reg_bindings.swap(registered_tables);
            token_bindings.swap(bound_objs_by_token);
            token_by_query_context.clear();
        }
        metas.clear();
        reg_bindings.clear();
        token_bindings.clear();
        drainPyBorrowGuardQueue();
    }
    catch (...)
    {
    }
}

void PythonTableCache::registerObject(const String & name, const py::object & obj)
{
    py::gil_assert();
    if (name.empty())
        throw py::value_error("Table name must not be empty");

    py::object normalized = normalizeAndValidateQueryableObj(name, obj);
    dropMeta(normalized.ptr());

    py::object displaced;
    {
        std::lock_guard lock(state_mutex);
        auto it = registered_tables.find(name);
        if (it != registered_tables.end())
        {
            displaced = std::move(it->second);
            it->second = std::move(normalized);
        }
        else
        {
            registered_tables.emplace(name, std::move(normalized));
        }
    }
    displaced = py::object{};
    drainPyBorrowGuardQueue();
}

void PythonTableCache::unregisterObject(const String & name)
{
    py::gil_assert();
    if (name.empty())
        throw py::value_error("Table name must not be empty");

    py::object removed;
    PyObject * removed_ptr = nullptr;
    {
        std::lock_guard lock(state_mutex);
        auto it = registered_tables.find(name);
        if (it != registered_tables.end())
        {
            removed_ptr = it->second.ptr();
            removed = std::move(it->second);
            registered_tables.erase(it);
        }
    }
    if (removed_ptr)
        dropMeta(removed_ptr);
    removed = py::object{};
    drainPyBorrowGuardQueue();
}

/// Function to find instance of PyReader, pandas DataFrame, or PyArrow Table, filtered by variable name
static py::object findQueryableObj(const String & var_name)
{
    py::module inspect = py::module_::import("inspect");
    py::object current_frame = inspect.attr("currentframe")();

    while (!current_frame.is_none())
    {
        py::object locals_obj = current_frame.attr("f_locals");
        py::object globals_obj = current_frame.attr("f_globals");

        for (const auto & namespace_obj : {locals_obj, globals_obj})
        {
            if (py::bool_(namespace_obj.attr("__contains__")(var_name)))
            {
                py::object obj;
                try
                {
                    obj = namespace_obj[py::cast(var_name)];
                    if (isQueryablePythonObject(obj))
                        return obj;
                }
                catch (const py::error_already_set &)
                {
                    continue;
                }
            }
        }

        current_frame = current_frame.attr("f_back");
    }

    return py::none();
}

/// Unique Python(...) table names referenced by the query, in match order.
static std::vector<String> extractPythonTableNames(const String & query_str)
{
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

UInt64 PythonTableCache::findQueryableObjFromQuery(const String & query_str, const py::object & explicit_tables)
{
    py::gil_assert();

    std::list<PandasTableMetaPtr> dead_meta;
    {
        std::lock_guard lock(state_mutex);
        ++query_epoch;

        for (auto it = meta_lru.begin(); it != meta_lru.end();)
        {
            PyObject * wr = (*it)->weak_ref.ptr();
            if (wr != nullptr && PyWeakref_GetObject(wr) == Py_None)
                dead_meta.splice(dead_meta.end(), meta_lru, it++);
            else
                ++it;
        }
    }
    dead_meta.clear();
    drainPyBorrowGuardQueue();

    std::unordered_map<String, py::object> local_bindings;

    if (explicit_tables && !explicit_tables.is_none())
    {
        auto bind_entry = [&](const py::handle & k, const py::handle & v)
        {
            if (!py::isinstance<py::str>(k))
                throw py::type_error("tables keys must be strings");
            String key = py::cast<String>(k);
            if (key.empty())
                throw py::value_error("tables key must not be empty");
            local_bindings.insert_or_assign(key, normalizeAndValidateQueryableObj(key, v));
        };

        if (py::isinstance<py::dict>(explicit_tables))
        {
            auto d = py::reinterpret_borrow<py::dict>(explicit_tables);
            local_bindings.reserve(static_cast<size_t>(py::len(d)));
            for (const auto & item : d)
                bind_entry(item.first, item.second);
        }
        else if (py::hasattr(explicit_tables, "items"))
        {
            py::object items = explicit_tables.attr("items")();
            for (const auto & pair_handle : items)
            {
                auto seq = py::reinterpret_borrow<py::sequence>(pair_handle);
                if (py::len(seq) != 2)
                    throw py::type_error("tables.items() must yield (name, object) pairs");
                bind_entry(seq[0], seq[1]);
            }
        }
        else
        {
            throw py::type_error("tables must be a dict mapping table names to table objects, or None");
        }
    }

    const auto names = extractPythonTableNames(query_str);
    for (const auto & matched : names)
    {
        if (local_bindings.contains(matched))
            continue;

        py::object reg_obj;
        {
            std::lock_guard lock(state_mutex);
            if (auto it = registered_tables.find(matched); it != registered_tables.end())
                reg_obj = it->second;
        }
        if (reg_obj && !reg_obj.is_none())
        {
            local_bindings.emplace(matched, std::move(reg_obj));
            continue;
        }

        auto obj = findQueryableObj(matched);
        if (obj.is_none())
            continue;

        obj = PolarsDataFrame::normalize(obj);
        local_bindings.emplace(matched, std::move(obj));
    }

    if (local_bindings.empty())
        return 0;

    static std::atomic<UInt64> global_bind_token{0};
    const UInt64 token = ++global_bind_token;

    std::lock_guard lock(state_mutex);
    bound_objs_by_token.emplace(token, std::move(local_bindings));
    return token;
}

void PythonTableCache::attachQueryContext(const void * query_ctx, UInt64 token)
{
    if (!query_ctx || token == 0)
        return;

    std::lock_guard lock(state_mutex);
    token_by_query_context[query_ctx] = token;
}

void PythonTableCache::detachQueryContext(const void * query_ctx) noexcept
{
    if (!query_ctx)
        return;

    std::lock_guard lock(state_mutex);
    token_by_query_context.erase(query_ctx);
}

void PythonTableCache::releaseQueryableObjs(UInt64 token)
{
    if (token == 0)
        return;

    std::unordered_map<String, py::object> released;
    {
        std::lock_guard lock(state_mutex);
        for (auto it = token_by_query_context.begin(); it != token_by_query_context.end();)
        {
            if (it->second == token)
                it = token_by_query_context.erase(it);
            else
                ++it;
        }
        auto rec = bound_objs_by_token.find(token);
        if (rec == bound_objs_by_token.end())
            return;
        released = std::move(rec->second);
        bound_objs_by_token.erase(rec);
    }

    for (auto & [_, obj] : released)
        enqueuePyDecref(obj.release().ptr());
}

py::object PythonTableCache::getQueryableObj(const String & table_name)
{
    py::gil_assert();

    const void * qctx_ptr = nullptr;
    const void * root_qctx_ptr = nullptr;
    if (auto cur = DB::CurrentThread::tryGetQueryContext())
    {
        qctx_ptr = cur.get();
        if (cur->hasQueryContext())
            root_qctx_ptr = cur->getQueryContext().get();
    }

    std::lock_guard lock(state_mutex);

    auto lookup_for_ctx = [&](const void * ctx_key) -> py::object
    {
        if (!ctx_key)
            return py::none();
        auto tok_it = token_by_query_context.find(ctx_key);
        if (tok_it == token_by_query_context.end())
            return py::none();
        auto map_it = bound_objs_by_token.find(tok_it->second);
        if (map_it == bound_objs_by_token.end())
            return py::none();
        auto obj_it = map_it->second.find(table_name);
        if (obj_it == map_it->second.end())
            return py::none();
        return obj_it->second;
    };

    if (py::object found = lookup_for_ctx(qctx_ptr); !found.is_none())
        return found;
    if (root_qctx_ptr != qctx_ptr)
    {
        if (py::object found = lookup_for_ctx(root_qctx_ptr); !found.is_none())
            return found;
    }

    if (auto reg_it = registered_tables.find(table_name); reg_it != registered_tables.end())
        return reg_it->second;

    /// Fallback when invoked outside an attached query context.
    for (const auto & [_, table_map] : bound_objs_by_token)
    {
        if (auto it = table_map.find(table_name); it != table_map.end())
            return it->second;
    }

    return py::none();
}

void PythonTableCache::clear()
{
    try
    {
        py::gil_scoped_acquire acquire;
        std::unordered_map<String, py::object> dropped_reg;
        std::unordered_map<UInt64, std::unordered_map<String, py::object>> dropped_tokens;
        {
            std::lock_guard lock(state_mutex);
            dropped_reg.swap(registered_tables);
            dropped_tokens.swap(bound_objs_by_token);
            token_by_query_context.clear();
        }
        dropped_reg.clear();
        dropped_tokens.clear();
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
    dropped.clear();
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
        return nullptr;
    if (!valid)
    {
        meta_lru.erase(it);
        return nullptr;
    }

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
                    return;
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
        displaced.clear();
    }
    catch (...)
    {
    }
}

} // namespace CHDB
