#include "PandasScan.h"
#include "PyBorrowGuard.h"
#include "PythonConversion.h"
#include "PythonImporter.h"
#include "ColumnVectorHelper.h"
#include <string_view>
#include <stringzilla/stringzilla.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationJSON.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>
#include <Common/assert_cast.h>
#if USE_JEMALLOC
#    include <Common/memory.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PY_EXCEPTION_OCCURED;
}

}

using namespace DB;

namespace CHDB
{

static inline const bool * getMaskPtr(const ColumnWrapper & col_wrap)
{
    return col_wrap.registered_array
        ? static_cast<const bool *>(col_wrap.registered_array->numpy_array.data())
        : nullptr;
}

template <typename T>
static void insertIntegerValue(py::handle h, typename ColumnVector<T>::Container & container)
{
    auto * ptr = h.ptr();

    if constexpr (std::is_signed_v<T>)
    {
        int overflow;
        int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);
        if (overflow)
        {
            container.push_back(T{});
            return;
        }

        if constexpr (sizeof(T) < sizeof(int64_t))
        {
            if (value > std::numeric_limits<T>::max() || value < std::numeric_limits<T>::min())
            {
                container.push_back(T{});
                return;
            }
        }
        container.push_back(static_cast<T>(value));
    }
    else
    {
        uint64_t value = PyLong_AsUnsignedLongLong(ptr);
        if (PyErr_Occurred())
        {
            PyErr_Clear();
            container.push_back(T{});
            return;
        }

        if constexpr (sizeof(T) < sizeof(uint64_t))
        {
            if (value > std::numeric_limits<T>::max())
            {
                container.push_back(T{});
                return;
            }
        }
        container.push_back(static_cast<T>(value));
    }
}

template <typename T>
static void scanIntegerColumn(py::handle handle, MutableColumnPtr & column)
{
    auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
    auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
    auto & null_map = nullable_column.getNullMapData();

    if (!py::isinstance<py::int_>(handle))
    {
        null_map.push_back(1);
        data_column->insertDefault();
        return;
    }

    null_map.push_back(0);
    auto & container = assert_cast<ColumnVector<T> &>(*data_column).getData();
    insertIntegerValue<T>(handle, container);
}

ColumnPtr PandasScan::scanColumn(
    const DB::ColumnWrapper & col_wrap,
    const size_t cursor,
    const size_t count,
    const DB::FormatSettings & format_settings)
{
    innerCheck(col_wrap);

    const auto & data_type = col_wrap.dest_type;
    auto column = data_type->createColumn();
    column->reserve(count);

    if (col_wrap.is_category)
    {
        chassert(data_type->lowCardinality());
        const auto & codes_type = col_wrap.category_codes_type;
        if (codes_type == "int8")
            innerScanCategory<Int8, UInt8>(cursor, count, static_cast<const Int8 *>(col_wrap.buf), col_wrap.category_unique, column, col_wrap.stride);
        else if (codes_type == "int16")
            innerScanCategory<Int16, UInt16>(cursor, count, static_cast<const Int16 *>(col_wrap.buf), col_wrap.category_unique, column, col_wrap.stride);
        else if (codes_type == "int32")
            innerScanCategory<Int32, UInt32>(cursor, count, static_cast<const Int32 *>(col_wrap.buf), col_wrap.category_unique, column, col_wrap.stride);
        else if (codes_type == "int64")
            innerScanCategory<Int64, UInt64>(cursor, count, static_cast<const Int64 *>(col_wrap.buf), col_wrap.category_unique, column, col_wrap.stride);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported category codes type: {}", codes_type);
        return column;
    }

    if (col_wrap.is_arrow_string)
    {
        /// Plain String for null-free columns, Nullable(String) otherwise.
        innerScanArrowString(cursor, count, col_wrap, column);
        return column;
    }

    chassert(data_type->isNullable());
    auto real_type = removeNullable(data_type);

    WhichDataType which(real_type);

    if (col_wrap.is_object_type)
    {
        SerializationPtr serialization;
        if (which.idx == TypeIndex::Object)
            serialization = real_type->getDefaultSerialization();

        auto * object_array = static_cast<PyObject **>(col_wrap.buf);
        innerScanObject(cursor, count, format_settings, serialization, object_array, column, which, col_wrap.stride);
        return column;
    }

    switch (which.idx)
	{
    case TypeIndex::Float32:
        innerScanFloat<Float32>(cursor, count, static_cast<const Float32 *>(col_wrap.buf), column, col_wrap.stride);
        break;
    case TypeIndex::Float64:
        innerScanFloat<Float64>(cursor, count, static_cast<const Float64 *>(col_wrap.buf), column, col_wrap.stride);
        break;
    case TypeIndex::Int8:
        innerScanNumeric<Int8>(cursor, count, static_cast<const Int8 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::Int16:
        innerScanNumeric<Int16>(cursor, count, static_cast<const Int16 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::Int32:
        innerScanNumeric<Int32>(cursor, count, static_cast<const Int32 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::Int64:
        innerScanNumeric<Int64>(cursor, count, static_cast<const Int64 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::UInt8:
        innerScanNumeric<UInt8>(cursor, count, static_cast<const UInt8 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::UInt16:
        innerScanNumeric<UInt16>(cursor, count, static_cast<const UInt16 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::UInt32:
        innerScanNumeric<UInt32>(cursor, count, static_cast<const UInt32 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::UInt64:
        innerScanNumeric<UInt64>(cursor, count, static_cast<const UInt64 *>(col_wrap.buf), getMaskPtr(col_wrap), column, col_wrap.stride, col_wrap.mask_stride);
        break;
    case TypeIndex::DateTime64:
        innerScanDateTime64(cursor, count, static_cast<const Int64 *>(col_wrap.buf), column, col_wrap.stride);
        break;
    case TypeIndex::Interval:
        // Interval uses ColumnVector<Int64> storage (different from DateTime64 which uses ColumnDecimal)
        // pandas timedelta64[ns] is also Int64 (nanoseconds)
        innerScanInterval(cursor, count, static_cast<const Int64 *>(col_wrap.buf), column, col_wrap.stride);
        break;
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported target type: {}", which.idx);
	}

    return column;
}

ColumnPtr PandasScan::scanObject(
    const ColumnWrapper & col_wrap,
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings)
{
    innerCheck(col_wrap);

    const auto & data_type = col_wrap.dest_type;
    auto column = data_type->createColumn();
    auto ** object_array = static_cast<PyObject **>(col_wrap.buf);
    auto serialization = data_type->getDefaultSerialization();

    innerScanObject(
        cursor, count, format_settings, serialization, object_array, column,
        WhichDataType(TypeIndex::Object), col_wrap.stride);

    return column;
}

void PandasScan::scanObject(
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings,
    const void * buf,
    MutableColumnPtr & column)
{
    auto * object_array = static_cast<PyObject **>(const_cast<void *>(buf));
    auto data_type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    SerializationPtr serialization = data_type->getDefaultSerialization();

    innerScanObject(
        cursor, count, format_settings, serialization, object_array, column,
        WhichDataType(TypeIndex::Object), sizeof(PyObject *));
}

void PandasScan::innerScanObject(
    const size_t cursor,
    const size_t count,
    const FormatSettings & format_settings,
    SerializationPtr & serialization,
    PyObject ** objects,
    MutableColumnPtr & column,
    WhichDataType which,
    size_t stride)
{
    /// stride == 0 is a numpy broadcast view over a single element: multiplying
    /// by the raw stride re-reads element 0, which is exactly what broadcast
    /// semantics require (and is also correct for the count <= 1 case).
    const auto * base_ptr = reinterpret_cast<const char *>(objects);

    switch (which.idx)
    {
    case TypeIndex::Object:
        {
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif
            auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
            auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
            auto & null_map = nullable_column.getNullMapData();

            for (size_t i = cursor; i < cursor + count; ++i)
            {
                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                auto handle = py::handle(obj_ptr);
                if (!py::isinstance<py::dict>(handle))
                {
                    null_map.push_back(1);
                    data_column->insertDefault();
                    continue;
                }

                null_map.push_back(0);
                tryInsertJsonResult(handle, format_settings, data_column, serialization);
            }
            break;
        }
    case TypeIndex::String:
        {
            /// The loop below calls into the Python C API (PyUnicode_Check,
            /// PyFloat_AsDouble, FillColumnString, ...) for every element, so the
            /// GIL must be held — exactly like the Object/Float64 cases. Without it,
            /// a Nullable string column (the only case that reaches here) corrupts
            /// the interpreter state and deadlocks.
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif
            auto & nullable_col = assert_cast<ColumnNullable &>(*column);
            auto data_column = nullable_col.getNestedColumnPtr()->assumeMutable();
            auto & null_map = nullable_col.getNullMapData();
            auto * column_string = assert_cast<ColumnString *>(data_column.get());
            auto * string_chars_ptr = &column_string->getChars();

            for (size_t i = cursor; i < cursor + count; ++i)
            {
                size_t local_idx = i - cursor;
                if (local_idx % 10 == 9)
                {
                    size_t data_size = string_chars_ptr->size();
                    size_t counter = local_idx + 1;
                    size_t avg_size = data_size / counter;
                    size_t reserve_size = avg_size * count;
                    if (reserve_size > string_chars_ptr->capacity())
                        string_chars_ptr->reserve(reserve_size);
                }

                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                auto handle = py::handle(obj_ptr);

                bool is_null = false;
                bool is_str = PyUnicode_Check(obj_ptr);
                if (!is_str)
                {
                    if (isNone(handle) || (isFloat(handle) && std::isnan(PyFloat_AsDouble(handle.ptr()))))
                        is_null = true;
                }

                if (is_null)
                {
                    null_map.push_back(1);
                    data_column->insertDefault();
                    continue;
                }

                null_map.push_back(0);
                auto * obj = handle.ptr();
                if (!is_str)
                    insertObjToStringColumn(obj, column_string);
                else
                    FillColumnString(obj, column_string);
            }
            break;
        }
    case TypeIndex::Float64:
        {
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif
            auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
            auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
            auto & null_map = nullable_column.getNullMapData();
            auto & container = assert_cast<ColumnVector<Float64> &>(*data_column).getData();

            for (size_t i = cursor; i < cursor + count; ++i)
            {
                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                auto handle = py::handle(obj_ptr);

                if (!py::isinstance<py::int_>(handle) && !py::isinstance<py::float_>(handle))
                {
                    null_map.push_back(1);
                    data_column->insertDefault();
                    continue;
                }

                if (py::isinstance<py::int_>(handle))
                {
                    double number = PyLong_AsDouble(handle.ptr());
                    if (number == -1.0 && PyErr_Occurred())
                    {
                        number = 0.0;
                        PyErr_Clear();
                    }
                    null_map.push_back(0);
                    container.push_back(number);
                }
                else
                {
                    double value = handle.cast<double>();
                    if (std::isnan(value))
                    {
                        null_map.push_back(1);
                        data_column->insertDefault();
                    }
                    else
                    {
                        null_map.push_back(0);
                        container.push_back(value);
                    }
                }
            }
            break;
        }
    case TypeIndex::Int64:
        {
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif
            for (size_t i = cursor; i < cursor + count; ++i)
            {
                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                scanIntegerColumn<Int64>(py::handle(obj_ptr), column);
            }
            break;
        }
    case TypeIndex::Int32:
        {
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif
            for (size_t i = cursor; i < cursor + count; ++i)
            {
                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                scanIntegerColumn<Int32>(py::handle(obj_ptr), column);
            }
            break;
        }
    case TypeIndex::UInt64:
        {
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif
            for (size_t i = cursor; i < cursor + count; ++i)
            {
                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                scanIntegerColumn<UInt64>(py::handle(obj_ptr), column);
            }
            break;
        }
    case TypeIndex::UInt8:
        {
            py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
            ::Memory::MemoryCheckScope memory_check_scope;
#endif

            auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
            auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
            auto & null_map = nullable_column.getNullMapData();
            auto & container = assert_cast<ColumnVector<UInt8> &>(*data_column).getData();

            for (size_t i = cursor; i < cursor + count; ++i)
            {
                auto * obj_ptr = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
                auto handle = py::handle(obj_ptr);

                if (isNone(handle) || (isFloat(handle) && std::isnan(PyFloat_AsDouble(handle.ptr()))))
                {
                    null_map.push_back(1);
                    container.push_back(0);
                    continue;
                }

                int result = PyObject_IsTrue(obj_ptr);
                if (result < 0)
                {
                    PyErr_Clear();
                    null_map.push_back(1);
                    container.push_back(0);
                    continue;
                }
                null_map.push_back(0);
                container.push_back(result ? 1 : 0);
            }
            break;
        }
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported nullable target type: {}", which.idx);
    }
}

template <typename T>
void PandasScan::innerScanFloat(
    const size_t cursor,
    const size_t count,
    const T * ptr,
    DB::MutableColumnPtr & column,
    size_t stride)
{
    auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
    auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
    auto & null_map = nullable_column.getNullMapData();

    if (stride == sizeof(T))
    {
        ColumnVectorHelper * helper = static_cast<ColumnVectorHelper *>(data_column.get());
        const T * start = ptr + cursor;
        helper->appendRawData<sizeof(T)>(reinterpret_cast<const char *>(start), count);

        const size_t old_size = null_map.size();
        null_map.resize(old_size + count);
        UInt8 * null_pos = null_map.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            null_pos[i] = start[i] != start[i] ? 1 : 0; /// NaN check, auto-vectorizable
    }
    else
    {
        auto & container = assert_cast<ColumnVector<T> &>(*data_column).getData();
        const auto * base_ptr = reinterpret_cast<const char *>(ptr);
        for (size_t i = cursor; i < cursor + count; ++i)
        {
            T value = *reinterpret_cast<const T *>(base_ptr + i * stride);
            container.push_back(value);
            null_map.push_back(std::isnan(value) ? 1 : 0);
        }
    }
}

template void PandasScan::innerScanFloat<Float32>(const size_t, const size_t, const Float32 *, DB::MutableColumnPtr &, size_t);
template void PandasScan::innerScanFloat<Float64>(const size_t, const size_t, const Float64 *, DB::MutableColumnPtr &, size_t);

template <typename T>
void PandasScan::innerScanNumeric(
    const size_t cursor,
    const size_t count,
    const T * data_ptr,
    const bool * mask_ptr,
    DB::MutableColumnPtr & column,
    size_t stride,
    size_t mask_stride)
{
    auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
    auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
    auto & null_map = nullable_column.getNullMapData();

    const bool data_contiguous = (stride == sizeof(T));
    const bool mask_contiguous = (mask_stride == sizeof(bool));

    if (data_contiguous && mask_contiguous)
    {
        ColumnVectorHelper * helper = static_cast<ColumnVectorHelper *>(data_column.get());
        const T * start = data_ptr + cursor;
        helper->appendRawData<sizeof(T)>(reinterpret_cast<const char *>(start), count);

        if (mask_ptr != nullptr)
        {
            const bool * mask_start = mask_ptr + cursor;
            null_map.insert(reinterpret_cast<const UInt8 *>(mask_start), reinterpret_cast<const UInt8 *>(mask_start + count));
        }
        else
        {
            null_map.resize_fill(null_map.size() + count, 0);
        }
    }
    else
    {
        // Slow path: non-contiguous data or mask
        auto & container = assert_cast<ColumnVector<T> &>(*data_column).getData();
        const auto * data_base = reinterpret_cast<const char *>(data_ptr);
        const auto * mask_base = reinterpret_cast<const char *>(mask_ptr);

        for (size_t i = cursor; i < cursor + count; ++i)
        {
            T value = *reinterpret_cast<const T *>(data_base + i * stride);
            container.push_back(value);

            if (mask_ptr != nullptr)
            {
                bool is_null = *reinterpret_cast<const bool *>(mask_base + i * mask_stride);
                null_map.push_back(is_null ? 1 : 0);
            }
            else
            {
                null_map.push_back(0);
            }
        }
    }
}

template void PandasScan::innerScanNumeric<Int8>(const size_t, const size_t, const Int8 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<Int16>(const size_t, const size_t, const Int16 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<Int32>(const size_t, const size_t, const Int32 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<Int64>(const size_t, const size_t, const Int64 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<UInt8>(const size_t, const size_t, const UInt8 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<UInt16>(const size_t, const size_t, const UInt16 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<UInt32>(const size_t, const size_t, const UInt32 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);
template void PandasScan::innerScanNumeric<UInt64>(const size_t, const size_t, const UInt64 *, const bool *, DB::MutableColumnPtr &, size_t, size_t);

void PandasScan::innerScanDateTime64(
    const size_t cursor,
    const size_t count,
    const Int64 * ptr,
    DB::MutableColumnPtr & column,
    size_t stride)
{
    auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
    auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
    auto & null_map = nullable_column.getNullMapData();

    if (stride == sizeof(Int64))
    {
        ColumnVectorHelper * helper = static_cast<ColumnVectorHelper *>(data_column.get());
        const Int64 * start = ptr + cursor;
        helper->appendRawData<sizeof(Int64)>(reinterpret_cast<const char *>(start), count);

        const size_t old_size = null_map.size();
        null_map.resize(old_size + count);
        UInt8 * null_pos = null_map.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            null_pos[i] = start[i] == std::numeric_limits<Int64>::min() ? 1 : 0; /// NaT check, auto-vectorizable
    }
    else
    {
        // DateTime64 uses ColumnDecimal<DateTime64>, which has the same memory layout as Int64
        auto & container = assert_cast<ColumnDecimal<DateTime64> &>(*data_column).getData();
        const auto * base_ptr = reinterpret_cast<const char *>(ptr);
        for (size_t i = cursor; i < cursor + count; ++i)
        {
            Int64 value = *reinterpret_cast<const Int64 *>(base_ptr + i * stride);
            container.push_back(DateTime64(value));
            bool is_nat = value <= std::numeric_limits<Int64>::min();
            null_map.push_back(is_nat ? 1 : 0);
        }
    }
}

void PandasScan::innerScanInterval(
    const size_t cursor,
    const size_t count,
    const Int64 * ptr,
    DB::MutableColumnPtr & column,
    size_t stride)
{
    auto & nullable_column = typeid_cast<ColumnNullable &>(*column);
    auto data_column = nullable_column.getNestedColumnPtr()->assumeMutable();
    auto & null_map = nullable_column.getNullMapData();

    if (stride == sizeof(Int64))
    {
        ColumnVectorHelper * helper = static_cast<ColumnVectorHelper *>(data_column.get());
        const Int64 * start = ptr + cursor;
        helper->appendRawData<sizeof(Int64)>(reinterpret_cast<const char *>(start), count);

        const size_t old_size = null_map.size();
        null_map.resize(old_size + count);
        UInt8 * null_pos = null_map.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            null_pos[i] = start[i] == std::numeric_limits<Int64>::min() ? 1 : 0; /// NaT check, auto-vectorizable
    }
    else
    {
        // Interval uses ColumnVector<Int64>
        auto & container = assert_cast<ColumnVector<Int64> &>(*data_column).getData();
        const auto * base_ptr = reinterpret_cast<const char *>(ptr);
        for (size_t i = cursor; i < cursor + count; ++i)
        {
            Int64 value = *reinterpret_cast<const Int64 *>(base_ptr + i * stride);
            container.push_back(value);
            bool is_nat = value <= std::numeric_limits<Int64>::min();
            null_map.push_back(is_nat ? 1 : 0);
        }
    }
}

static constexpr size_t BORROW_MIN_BYTES = 16384;

/// Mount the payload of a fully-valid, single-chunk segment as borrowed chars;
/// offsets are rebuilt (Arrow start-offsets -> ClickHouse cumulative offsets).
template <typename OffsetT>
static bool tryBorrowArrowStringSegment(
    const ArrowStringChunkView & chunk,
    const size_t local_start,
    const size_t n,
    ColumnString & column_string,
    NullMap * null_map,
    const std::shared_ptr<void> & guard)
{
    if (chunk.validity != nullptr || !column_string.getChars().empty() || !column_string.getOffsets().empty())
        return false;

    const OffsetT * off = static_cast<const OffsetT *>(chunk.offsets) + chunk.offset + local_start;
    const size_t total_bytes = static_cast<size_t>(off[n] - off[0]);
    if (total_bytes < BORROW_MIN_BYTES)
        return false;

    const char * begin = chunk.data + off[0];
    if (!CHDB::borrowTailReadable(begin + total_bytes))
        return false;

    column_string.borrowChars(begin, total_bytes, guard);

    auto & offsets = column_string.getOffsets();
    offsets.resize_exact(n);
    const OffsetT base = off[0];
    for (size_t i = 0; i < n; ++i)
        offsets[i] = static_cast<size_t>(off[i + 1] - base);

    if (null_map)
        null_map->resize_fill(null_map->size() + n, 0);
    return true;
}

template <typename OffsetT>
static void scanArrowStringSegment(
    const ArrowStringChunkView & chunk,
    const size_t local_start,
    const size_t n,
    ColumnString & column_string,
    NullMap * null_map)
{
    auto & chars = column_string.getChars();
    auto & offsets = column_string.getOffsets();

    const OffsetT * off = static_cast<const OffsetT *>(chunk.offsets) + chunk.offset + local_start;
    const char * data = chunk.data;

    /// ColumnString stores raw payload back to back (no terminators), so for a
    /// fully-valid segment the Arrow data buffer slice can be copied wholesale.
    const size_t total_bytes = static_cast<size_t>(off[n] - off[0]);
    size_t chars_pos = chars.size();
    chars.resize(chars_pos + total_bytes);

    const size_t offsets_old = offsets.size();
    offsets.resize(offsets_old + n);
    auto * offsets_pos = offsets.data() + offsets_old;

    UInt8 * null_pos = nullptr;
    if (null_map)
    {
        const size_t null_old = null_map->size();
        null_map->resize(null_old + n);
        null_pos = null_map->data() + null_old;
    }

    char * dst = reinterpret_cast<char *>(chars.data());

    if (chunk.validity == nullptr)
    {
        if (total_bytes)
            memcpy(dst + chars_pos, data + off[0], total_bytes);
        const OffsetT base = off[0];
        for (size_t i = 0; i < n; ++i)
            offsets_pos[i] = chars_pos + static_cast<size_t>(off[i + 1] - base);
        if (null_pos)
            memset(null_pos, 0, n);
        chars_pos += total_bytes;
    }
    else
    {
        chassert(null_pos); /// nullable schema is guaranteed when the column has nulls
        const UInt8 * validity = chunk.validity;
        const size_t bit_base = chunk.offset + local_start;
        for (size_t i = 0; i < n; ++i)
        {
            const size_t bit = bit_base + i;
            const bool is_valid = validity[bit >> 3] & (1u << (bit & 7));
            if (is_valid)
            {
                const size_t len = static_cast<size_t>(off[i + 1] - off[i]);
                if (len)
                    memcpy(dst + chars_pos, data + off[i], len);
                chars_pos += len;
                null_pos[i] = 0;
            }
            else
            {
                null_pos[i] = 1;
            }
            offsets_pos[i] = chars_pos;
        }

        if (chars.size() != chars_pos)
            chars.resize(chars_pos); /// null rows reserve payload they do not use
    }
}

void PandasScan::innerScanArrowString(
    const size_t cursor,
    const size_t count,
    const ColumnWrapper & col_wrap,
    MutableColumnPtr & column)
{
    ColumnString * column_string;
    NullMap * null_map = nullptr;
    MutableColumnPtr data_column;
    if (auto * nullable_column = typeid_cast<ColumnNullable *>(column.get()))
    {
        data_column = nullable_column->getNestedColumnPtr()->assumeMutable();
        null_map = &nullable_column->getNullMapData();
        column_string = assert_cast<ColumnString *>(data_column.get());
    }
    else
    {
        column_string = assert_cast<ColumnString *>(column.get());
    }

    const auto & chunks = col_wrap.arrow_string_chunks;

    /// Find the chunk containing `cursor` (chunks are sorted by row_start)
    size_t lo = 0;
    size_t hi = chunks.size();
    while (lo < hi)
    {
        const size_t mid = (lo + hi) / 2;
        if (chunks[mid].row_start + chunks[mid].length <= cursor)
            lo = mid + 1;
        else
            hi = mid;
    }

    /// Zero-copy: a block that lies entirely inside one fully-valid chunk
    /// mounts the Arrow payload instead of copying it.
    if (col_wrap.borrow_guard && lo < chunks.size())
    {
        const auto & chunk = chunks[lo];
        const size_t local_start = cursor - chunk.row_start;
        if (local_start + count <= chunk.length)
        {
            const bool borrowed = col_wrap.arrow_large_offsets
                ? tryBorrowArrowStringSegment<Int64>(chunk, local_start, count, *column_string, null_map, col_wrap.borrow_guard)
                : tryBorrowArrowStringSegment<Int32>(chunk, local_start, count, *column_string, null_map, col_wrap.borrow_guard);
            if (borrowed)
                return;
        }
    }

    size_t row = cursor;
    size_t remaining = count;
    for (size_t chunk_idx = lo; remaining > 0; ++chunk_idx)
    {
        chassert(chunk_idx < chunks.size());
        const auto & chunk = chunks[chunk_idx];
        const size_t local_start = row - chunk.row_start;
        const size_t n = std::min(remaining, chunk.length - local_start);

        if (col_wrap.arrow_large_offsets)
            scanArrowStringSegment<Int64>(chunk, local_start, n, *column_string, null_map);
        else
            scanArrowStringSegment<Int32>(chunk, local_start, n, *column_string, null_map);

        row += n;
        remaining -= n;
    }
}

template <typename OffsetT>
static void evalArrowPredSegment(
    const ArrowStringChunkView & chunk,
    const size_t local_start,
    const size_t n,
    const PandasScan::StringPredicate predicate,
    const std::string_view needle,
    unsigned char * out)
{
    const OffsetT * off = static_cast<const OffsetT *>(chunk.offsets) + chunk.offset + local_start;
    const char * data = chunk.data;
    const UInt8 * validity = chunk.validity;
    const size_t bit_base = chunk.offset + local_start;

    /// Contains with a non-empty needle: one SIMD substring search over the
    /// segment's contiguous byte range instead of one sz_find call per row.
    /// Per-row calls pay the searcher setup on every short haystack, which
    /// makes the scan several times slower than one pass over the buffer.
    /// Hits are attributed to rows through the offsets; a hit straddling a
    /// row boundary is not a match, and a hit inside a null slot is discarded
    /// via the validity bitmap (checked only on hits).
    if (predicate == PandasScan::StringPredicate::LikeContains && !needle.empty())
    {
        memset(out, 0, n);
        const char * const end = data + off[n];
        const char * p = data + off[0];
        size_t row = 0;
        while (p < end)
        {
            const char * hit = sz_find(p, static_cast<size_t>(end - p), needle.data(), needle.size());
            if (!hit)
                break;
            const auto pos = static_cast<OffsetT>(hit - data);
            while (row < n && off[row + 1] <= pos)
                ++row;
            if (row >= n)
                break;
            if (pos + static_cast<OffsetT>(needle.size()) <= off[row + 1])
            {
                /// Null slots usually have zero extent, but the Arrow spec only
                /// requires monotonic offsets: a null slot may span arbitrary
                /// bytes that could contain the needle. Check validity on hit.
                const size_t bit = bit_base + row;
                if (!validity || (validity[bit >> 3] & (1u << (bit & 7))))
                    out[row] = 1;
                p = data + off[row + 1];
                ++row;
            }
            else
                p = hit + 1;
        }
        return;
    }

    for (size_t i = 0; i < n; ++i)
    {
        const bool is_valid = !validity || (validity[(bit_base + i) >> 3] & (1u << ((bit_base + i) & 7)));
        unsigned char r = 0;
        if (is_valid)
        {
            const size_t len = static_cast<size_t>(off[i + 1] - off[i]);
            switch (predicate)
            {
                case PandasScan::StringPredicate::NotEmpty:
                    r = len > 0;
                    break;
                case PandasScan::StringPredicate::Empty:
                    r = len == 0;
                    break;
                case PandasScan::StringPredicate::LikeContains:
                    /// Only the empty-needle case reaches here: LIKE '%%'
                    /// matches every non-null row.
                    r = 1;
                    break;
            }
        }
        out[i] = r;
    }
}

void PandasScan::evalArrowStringPredicate(
    const ColumnWrapper & col_wrap,
    const size_t cursor,
    const size_t count,
    StringPredicate predicate,
    const std::string & needle,
    unsigned char * out)
{
    const auto & chunks = col_wrap.arrow_string_chunks;

    size_t lo = 0;
    size_t hi = chunks.size();
    while (lo < hi)
    {
        const size_t mid = (lo + hi) / 2;
        if (chunks[mid].row_start + chunks[mid].length <= cursor)
            lo = mid + 1;
        else
            hi = mid;
    }

    const std::string_view needle_view(needle);
    size_t row = cursor;
    size_t remaining = count;
    size_t out_pos = 0;
    for (size_t chunk_idx = lo; remaining > 0; ++chunk_idx)
    {
        chassert(chunk_idx < chunks.size());
        const auto & chunk = chunks[chunk_idx];
        const size_t local_start = row - chunk.row_start;
        const size_t n = std::min(remaining, chunk.length - local_start);

        if (col_wrap.arrow_large_offsets)
            evalArrowPredSegment<Int64>(chunk, local_start, n, predicate, needle_view, out + out_pos);
        else
            evalArrowPredSegment<Int32>(chunk, local_start, n, predicate, needle_view, out + out_pos);

        row += n;
        remaining -= n;
        out_pos += n;
    }
}

template <typename OffsetT>
static void gatherArrowStringSegment(
    const ArrowStringChunkView & chunk,
    const size_t local_start,
    const size_t n,
    const UInt8 * mask,
    ColumnString & column_string,
    NullMap * null_map)
{
    auto & chars = column_string.getChars();
    auto & offsets = column_string.getOffsets();

    const OffsetT * off = static_cast<const OffsetT *>(chunk.offsets) + chunk.offset + local_start;
    const char * data = chunk.data;
    const UInt8 * validity = chunk.validity;
    const size_t bit_base = chunk.offset + local_start;

    /// Exact byte count of the selected rows for a single reserve.
    size_t selected_bytes = 0;
    size_t selected_rows = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (!mask[i])
            continue;
        ++selected_rows;
        if (!validity || (validity[(bit_base + i) >> 3] & (1u << ((bit_base + i) & 7))))
            selected_bytes += static_cast<size_t>(off[i + 1] - off[i]);
    }
    if (selected_rows == 0)
        return;

    size_t chars_pos = chars.size();
    chars.resize(chars_pos + selected_bytes);
    char * dst = reinterpret_cast<char *>(chars.data());

    const size_t offsets_old = offsets.size();
    offsets.reserve(offsets_old + selected_rows);
    if (null_map)
        null_map->reserve(null_map->size() + selected_rows);

    for (size_t i = 0; i < n; ++i)
    {
        if (!mask[i])
            continue;

        const bool is_valid = !validity || (validity[(bit_base + i) >> 3] & (1u << ((bit_base + i) & 7)));
        if (is_valid)
        {
            const size_t len = static_cast<size_t>(off[i + 1] - off[i]);
            if (len)
                memcpy(dst + chars_pos, data + off[i], len);
            chars_pos += len;
            if (null_map)
                null_map->push_back(0);
        }
        else
        {
            chassert(null_map);
            null_map->push_back(1);
        }
        offsets.push_back(chars_pos);
    }
}

ColumnPtr PandasScan::scanColumnFiltered(
    const ColumnWrapper & col_wrap,
    const size_t cursor,
    const size_t count,
    const IColumn::Filter & filter,
    const size_t selected)
{
    chassert(col_wrap.is_arrow_string);

    const auto & data_type = col_wrap.dest_type;
    auto column = data_type->createColumn();
    column->reserve(selected);

    ColumnString * column_string;
    NullMap * null_map = nullptr;
    MutableColumnPtr data_column;
    if (auto * nullable_column = typeid_cast<ColumnNullable *>(column.get()))
    {
        data_column = nullable_column->getNestedColumnPtr()->assumeMutable();
        null_map = &nullable_column->getNullMapData();
        column_string = assert_cast<ColumnString *>(data_column.get());
    }
    else
    {
        column_string = assert_cast<ColumnString *>(column.get());
    }

    const auto & chunks = col_wrap.arrow_string_chunks;
    size_t lo = 0;
    size_t hi = chunks.size();
    while (lo < hi)
    {
        const size_t mid = (lo + hi) / 2;
        if (chunks[mid].row_start + chunks[mid].length <= cursor)
            lo = mid + 1;
        else
            hi = mid;
    }

    size_t row = cursor;
    size_t remaining = count;
    const UInt8 * mask = filter.data();
    for (size_t chunk_idx = lo; remaining > 0; ++chunk_idx)
    {
        chassert(chunk_idx < chunks.size());
        const auto & chunk = chunks[chunk_idx];
        const size_t local_start = row - chunk.row_start;
        const size_t n = std::min(remaining, chunk.length - local_start);

        if (col_wrap.arrow_large_offsets)
            gatherArrowStringSegment<Int64>(chunk, local_start, n, mask, *column_string, null_map);
        else
            gatherArrowStringSegment<Int32>(chunk, local_start, n, mask, *column_string, null_map);

        row += n;
        remaining -= n;
        mask += n;
    }

    return column;
}

void PandasScan::innerCheck(const ColumnWrapper & col_wrap)
{
    if (col_wrap.data.is_none())
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column data is None");

    if (!col_wrap.buf)
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column buffer is null");
}

template <typename T, typename IndexType>
void PandasScan::innerScanCategory(
    const size_t cursor,
    const size_t count,
    const T * codes_ptr,
    const ColumnUniquePtr & category_unique,
    MutableColumnPtr & column,
    size_t stride)
{
    /// stride == 0 is a numpy broadcast view over a single element: multiplying
    /// by the raw stride re-reads element 0, which is exactly what broadcast
    /// semantics require (and is also correct for the count <= 1 case).
    const auto * base_ptr = reinterpret_cast<const char *>(codes_ptr);

    auto indexes_column = ColumnVector<IndexType>::create();
    auto & indexes_data = indexes_column->getData();
    indexes_data.reserve(count);

    for (size_t i = cursor; i < cursor + count; ++i)
    {
        T code = *reinterpret_cast<const T *>(base_ptr + i * stride);
        indexes_data.push_back(code < 0 ? 0 : static_cast<IndexType>(code + 1));
    }

    column = ColumnLowCardinality::create(category_unique->assumeMutable(), std::move(indexes_column), true);
}

template void PandasScan::innerScanCategory<Int8, UInt8>(size_t, size_t, const Int8 *, const ColumnUniquePtr &, MutableColumnPtr &, size_t);
template void PandasScan::innerScanCategory<Int16, UInt16>(size_t, size_t, const Int16 *, const ColumnUniquePtr &, MutableColumnPtr &, size_t);
template void PandasScan::innerScanCategory<Int32, UInt32>(size_t, size_t, const Int32 *, const ColumnUniquePtr &, MutableColumnPtr &, size_t);
template void PandasScan::innerScanCategory<Int64, UInt64>(size_t, size_t, const Int64 *, const ColumnUniquePtr &, MutableColumnPtr &, size_t);

} // namespace CHDB
