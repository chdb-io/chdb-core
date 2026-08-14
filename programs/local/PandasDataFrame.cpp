#include "PandasDataFrame.h"
#include "NumpyType.h"
#include "PyBorrowGuard.h"

#include <functional>
#include <set>
#include "PandasAnalyzer.h"
#include "PandasCacheItem.h"
#include "PythonImporter.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#if USE_JEMALLOC
#    include <Common/memory.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

}

using namespace DB;

namespace CHDB
{

/// True iff `dtype` is a pandas masked/nullable extension dtype (Int64, UInt32,
/// Float64, boolean, ...). Checked structurally against pandas' BaseMaskedDtype
/// from the dtype object alone (no Series materialization), so every current and
/// future masked dtype is covered. If pandas' internal BaseMaskedDtype cannot be
/// located (e.g. a future internal-layout change), falls back to matching the
/// known masked dtype reprs, so a nullable column is never silently inferred as
/// non-Nullable.
static bool isMaskedExtensionDtype(const py::handle & dtype)
{
    static const py::handle base_masked_dtype = []() -> py::handle
    {
        try
        {
            /// Leaked on purpose: never torn down, which avoids a Python decref
            /// running at process shutdown without the GIL.
            py::object obj = py::module_::import("pandas.core.dtypes.dtypes").attr("BaseMaskedDtype");
            return obj.release();
        }
        catch (py::error_already_set & e)
        {
            e.discard_as_unraisable("locating pandas BaseMaskedDtype");
            return {};
        }
    }();

    if (base_masked_dtype)
        return py::isinstance(dtype, base_masked_dtype);

    /// Fallback only if BaseMaskedDtype could not be imported.
    static const std::set<String> masked_reprs = {
        "boolean", "Int8", "Int16", "Int32", "Int64",
        "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64",
    };
    return masked_reprs.contains(String(py::str(dtype)));
}

/// `get_handle()` lazily extracts the column's Series (df.__getitem__), which is
/// the dominant per-column cost of schema inference. It is only called for the
/// column kinds whose type cannot be decided from the dtype alone (category,
/// object, string null-count); plain numeric/datetime columns are resolved from
/// the dtype without ever touching the Series.
static DataTypePtr inferDataTypeFromPandasColumn(
    const py::handle & col_type,
    const py::handle & col_name,
    const std::function<py::object()> & get_handle,
    ContextPtr & context,
    DataSourceWrapper & wrapper)
{
    /// ArrowDtype columns (pandas dtype_backend="pyarrow") keep their values in
    /// Arrow buffers, not in the numpy layout the scan expects, and are not
    /// supported yet. They must fail here, per column: treating the whole
    /// DataFrame as "not a DataFrame" instead dropped it into the legacy
    /// regex-based schema path, which substring-matched e.g. "int64[pyarrow]"
    /// as Int64 and returned garbage values without any error.
    py::handle arrow_dtype = PythonImporter::ImportCache().pandas.ArrowDtype();
    if (arrow_dtype && !arrow_dtype.is_none() && py::isinstance(col_type, arrow_dtype))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Column '{}' has pandas ArrowDtype '{}', which is not supported by the Python() table engine. "
            "Convert it to a NumPy-backed dtype first, e.g. with .astype(...), or pass the data as a pyarrow.Table.",
            String(py::str(col_name)),
            String(py::str(col_type)));

    auto numpy_type = ConvertNumpyType(col_type);

    if (numpy_type.type == NumpyNullableType::CATEGORY)
    {
        py::object handle = get_handle();
        chassert(py::hasattr(handle, "cat"));
		chassert(py::hasattr(handle.attr("cat"), "categories"));

        py::array categories = py::array(handle.attr("cat").attr("categories"));
        auto categories_numpy_type = ConvertNumpyType(categories.attr("dtype"));
        if (categories_numpy_type.type == NumpyNullableType::OBJECT)
        {
            auto inner_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            return std::make_shared<DataTypeLowCardinality>(inner_type);
        }
        else
        {
            py::array values = py::array(handle.attr("to_numpy")());
            auto name = py::str(col_name).cast<std::string>();
            wrapper.cacheColumnData(name, values);
            numpy_type = ConvertNumpyType(values.attr("dtype"));
        }
    }

    if (numpy_type.type == NumpyNullableType::OBJECT)
    {
		PandasAnalyzer analyzer(context->getSettingsRef());
		if (analyzer.Analyze(get_handle()))
        {
            const auto & analyzed_type = analyzer.analyzedType();
            const bool use_string_fallback = !context->getQueryContext() || !context->getQueryContext()->isJSONSupported();
            const bool is_json_type = typeid_cast<const DataTypeObject *>(analyzed_type.get()) != nullptr;

            if (!is_json_type || !use_string_fallback)
                return analyzed_type;
		}

        numpy_type.type = NumpyNullableType::STRING;
	}

    /// Arrow-backed string columns expose an O(1) null count. Columns without
    /// nulls map to plain String instead of Nullable(String): the scan skips
    /// the null map entirely and downstream kernels take non-nullable paths.
    if (numpy_type.type == NumpyNullableType::STRING)
    {
        py::object handle = get_handle();
        if (py::hasattr(handle, "array"))
        {
            py::object underlying_array = handle.attr("array");
            if (py::hasattr(underlying_array, "_pa_array"))
            {
                py::object chunked = underlying_array.attr("_pa_array");
                if (py::hasattr(chunked, "null_count") && chunked.attr("null_count").cast<Int64>() == 0)
                    return std::make_shared<DataTypeString>();
            }
        }
    }

    auto data_type = NumpyToDataType(numpy_type);

    /// numpy datetime64 has no O(1) null metadata (NaT is the in-band Int64::min
    /// sentinel; no validity bitmap / null_count / mask), so deciding non-Nullable
    /// would require an O(n) scan on every query. Keep datetime Nullable instead.

    /// pandas masked/nullable numerics (Int64, Float64, boolean, ...) carry a
    /// `_mask`; detect them structurally from the dtype so Nullable-ness is decided
    /// without materializing the column's array.
    if (!data_type->isNullable() && isMaskedExtensionDtype(col_type))
        return std::make_shared<DataTypeNullable>(data_type);

    return data_type;
}

ColumnsDescription PandasDataFrame::getActualTableStructure(DataSourceWrapper & wrapper, ContextPtr & context)
{
#if USE_JEMALLOC
    ::Memory::MemoryCheckScope memory_check_scope;
#endif
    chassert(py::gil_check());
    NamesAndTypesList names_and_types;

    const auto & object = wrapper.getDataSource();
    PandasDataFrameBind df(object);
    size_t column_count = py::len(df.names);
    if (column_count == 0 || py::len(df.types) == 0)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected empty DataFrame");

    if (column_count != py::len(df.types))
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Unexpected DataFrame with column count: {} and type count: {}", column_count, py::len(df.types));

    for (size_t col_idx = 0; col_idx < column_count; col_idx++)
    {
        py::handle col_name = df.names[col_idx];
        py::handle col_type = df.types[col_idx];
        /// Series extraction is deferred: inferDataTypeFromPandasColumn pulls it
        /// only for category/object/string columns, not plain numeric/datetime.
        auto get_handle = [&df, col_idx]() { return df.getColumn(col_idx); };
        auto data_type = inferDataTypeFromPandasColumn(col_type, col_name, get_handle, context, wrapper);

        names_and_types.push_back({py::str(col_name), data_type});
    }

    return ColumnsDescription(names_and_types);
}

bool PandasDataFrame::isPandasDataframe(const py::object & object)
{
#if USE_JEMALLOC
    ::Memory::MemoryCheckScope memory_check_scope;
#endif
    chassert(py::gil_check());

    if (!ModuleIsLoaded<PandasCacheItem>())
		return false;

	auto & importer_cache = PythonImporter::ImportCache();
	return py::isinstance(object, importer_cache.pandas.DataFrame());
}

bool PandasDataFrame::isPyArrowBacked(const py::handle & /*object*/)
{
    /// TODO: check if object is pyarrow backed
    return false;
}

/// Capture zero-copy buffer views over an Arrow-backed string column
/// (pandas StringDtype with pyarrow storage, the default `str` dtype since pandas 3.x).
/// Reading the Arrow offsets/data buffers directly avoids materializing the whole
/// column into an object ndarray (one PyObject per row) via to_numpy() and avoids
/// any per-row Python C-API calls during the scan.
static bool tryFillArrowStringColumn(DB::ColumnWrapper & column, const py::object & underlying_array)
{
    if (!py::hasattr(underlying_array, "_pa_array"))
        return false;

    py::object chunked = underlying_array.attr("_pa_array");
    if (!py::hasattr(chunked, "chunks") || !py::hasattr(chunked, "type"))
        return false;

    auto type_str = py::str(chunked.attr("type")).cast<std::string>();
    bool large_offsets;
    if (type_str == "large_string")
        large_offsets = true;
    else if (type_str == "string")
        large_offsets = false;
    else
        return false; /// e.g. string_view has a different buffer layout

    std::vector<DB::ArrowStringChunkView> views;
    size_t row_start = 0;
    for (const auto & chunk_handle : chunked.attr("chunks"))
    {
        auto chunk = py::reinterpret_borrow<py::object>(chunk_handle);
        DB::ArrowStringChunkView view;
        view.length = py::len(chunk);
        view.offset = chunk.attr("offset").cast<size_t>();
        view.row_start = row_start;

        py::list buffers = chunk.attr("buffers")();
        if (py::len(buffers) != 3)
            return false;

        auto buffer_address = [](const py::handle & buffer) -> const void *
        {
            if (buffer.is_none())
                return nullptr;
            return reinterpret_cast<const void *>(buffer.attr("address").cast<uintptr_t>());
        };

        view.validity = static_cast<const UInt8 *>(buffer_address(buffers[0]));
        view.offsets = buffer_address(buffers[1]);
        view.data = static_cast<const char *>(buffer_address(buffers[2]));

        /// A chunk without nulls scans via the bulk-memcpy path even when a
        /// validity buffer happens to be allocated.
        if (view.validity && chunk.attr("null_count").cast<Int64>() == 0)
            view.validity = nullptr;

        if (view.length > 0 && view.offsets == nullptr)
            return false;

        row_start += view.length;
        views.emplace_back(view);
    }

    if (row_start != column.row_count)
        return false;

    column.arrow_string_chunks = std::move(views);
    column.arrow_large_offsets = large_offsets;
    column.is_arrow_string = true;
    column.is_object_type = false;
    column.data = chunked;
    column.buf = chunked.ptr(); /// non-null sentinel, never dereferenced on this path
    column.tmp = chunked;       /// keep the ChunkedArray (and its buffers) alive
    column.tmp.inc_ref();
    if (zeroCopyEnabled())
        column.borrow_guard = makePyBorrowGuard(chunked.ptr());
    return true;
}

void PandasDataFrame::fillColumn(
    const py::handle & data_source,
    const std::string & col_name,
    DB::ColumnWrapper & column,
    DataSourceWrapper & wrapper)
{
    chassert(py::gil_check());

    py::object series = data_source[py::str(col_name)];
    py::object dtype = data_source.attr("dtypes")[py::str(col_name)];

    auto numpy_type = ConvertNumpyType(dtype);
    column.is_object_type = (numpy_type.type == NumpyNullableType::OBJECT || numpy_type.type == NumpyNullableType::STRING);

    if (numpy_type.type == NumpyNullableType::CATEGORY)
    {
        chassert(py::hasattr(series, "cat"));
		chassert(py::hasattr(series.attr("cat"), "categories"));

        py::array categories = py::array(series.attr("cat").attr("categories"));
        auto categories_numpy_type = ConvertNumpyType(categories.attr("dtype"));

        if (categories_numpy_type.type == NumpyNullableType::OBJECT)
        {
            column.is_category = true;

            std::vector<std::string> category_strings;
            try
            {
                category_strings = py::cast<std::vector<std::string>>(categories);
            }
            catch (const py::cast_error &)
            {
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Categorical column '{}' contains non-string categories. "
                    "Only string categories are supported for LowCardinality conversion",
                    col_name);
            }
            auto dict_column = DB::ColumnString::create();
            dict_column->insertDefault();
            for (const auto & s : category_strings)
                dict_column->insertData(s.data(), s.size());

            auto nullable_string_type = std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeString>());
            column.category_unique = DB::DataTypeLowCardinality::createColumnUnique(*nullable_string_type, std::move(dict_column));

            chassert(py::hasattr(series.attr("cat"), "codes"));
            py::array codes = py::array(series.attr("cat").attr("codes"));
            column.row_count = static_cast<size_t>(codes.size());
            column.data = codes;
            column.tmp = codes;
            column.tmp.inc_ref();
            column.buf = const_cast<void *>(codes.data());
            column.stride = static_cast<size_t>(codes.strides(0));
            column.category_codes_type = py::str(codes.attr("dtype")).cast<std::string>();
            return;
        }
        else
        {
            py::array * cached = wrapper.getCachedColumnData(col_name);
            if (!cached)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Column {} not found in cache", col_name);
            column.row_count = static_cast<size_t>(cached->size());
            column.data = *cached;
            column.buf = const_cast<void *>(cached->data());
            column.stride = static_cast<size_t>(cached->strides(0));
            return;
        }
    }

    column.row_count = py::len(series);

    /// Fast path for pandas 2.x object-dtype string columns:
    /// series.values returns the underlying ndarray directly (zero-copy).
    /// For pandas 3.x StringDtype columns, dtype is "string" or "str" (not "object"),
    /// so they will skip this fast path and go through to_numpy() below.
    if (column.row_count > 0 && numpy_type.type == NumpyNullableType::OBJECT)
    {
        auto elem_type = series.attr("iloc").attr("__getitem__")(0).attr("__class__").attr("__name__").cast<std::string>();

        if (elem_type == "str" || elem_type == "unicode")
        {
            py::array values_array = series.attr("values");
            column.data = values_array;
            column.buf = const_cast<void *>(values_array.data());
            chassert(py::hasattr(values_array, "strides"));
            column.stride = values_array.attr("strides").attr("__getitem__")(0).cast<size_t>();
            return;
        }
    }

    py::object underlying_array = series.attr("array");

    /// Fast path for Arrow-backed string columns (pandas 3.x default `str` dtype,
    /// pandas 2.x string[pyarrow]): scan the Arrow buffers directly, zero-copy.
    if (numpy_type.type == NumpyNullableType::STRING && tryFillArrowStringColumn(column, underlying_array))
        return;

    if (py::hasattr(underlying_array, "_mask"))
    {
        py::array mask_array = underlying_array.attr("_mask");
        column.mask_stride = mask_array.attr("strides").attr("__getitem__")(0).cast<size_t>();
        column.registered_array = std::make_unique<DB::RegisteredArray>(mask_array);
    }

    py::array array;
    if (py::hasattr(underlying_array, "_data"))
    {
        array = underlying_array.attr("_data");
    }
    else if (py::hasattr(underlying_array, "asi8"))
    {
        /// DatetimeArray, TimedeltaArray use asi8 to get int64 representation
        array = py::array(underlying_array.attr("asi8"));
    }
    else if (py::hasattr(underlying_array, "_ndarray"))
    {
        /// NumpyExtensionArray (plain numpy-backed int/float/bool columns).
        /// Its to_numpy() runs an O(n) isna() scan on every call even when it
        /// returns the underlying buffer unchanged; with a wide DataFrame that
        /// serial per-column scan dominated SELECT * queries (the columns are
        /// resolved under the GIL before the parallel scan starts). _ndarray
        /// is that same underlying buffer without the scan.
        array = underlying_array.attr("_ndarray");
    }
    else
    {
        array = underlying_array.attr("to_numpy")();
    }

    chassert(py::hasattr(array, "strides"));
    column.tmp = array;
    column.tmp.inc_ref();
    column.stride = array.attr("strides").attr("__getitem__")(0).cast<size_t>();
    column.data = array;
    column.buf = const_cast<void *>(array.data());
    /// Plain (non-masked) numpy buffers may be mounted zero-copy by the scan.
    if (zeroCopyEnabled() && !column.registered_array && !column.is_object_type)
        column.borrow_guard = makePyBorrowGuard(array.ptr());
}

} // namespace CHDB
