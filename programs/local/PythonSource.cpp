#include "PythonSource.h"
#include "ColumnVectorHelper.h"
#include "ListScan.h"
#include "PandasScan.h"
#include "PyBorrowGuard.h"
#include "StoragePython.h"

#include <algorithm>
#include <exception>
#include <type_traits>
#include <boolobject.h>
#include <pybind11/gil.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#ifndef CHDB_FREE_THREADING
#include <pybind11/detail/non_limited_api.h>
#endif

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/sortBlock.h>
#include <Poco/Logger.h>
#include <Common/COW.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/iota.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <base/Decimal.h>
#include <base/Decimal_fwd.h>
#include <base/scope_guard.h>
#include <base/types.h>
#include <Common/typeid_cast.h>
#if USE_JEMALLOC
#    include <Common/memory.h>
#endif

using namespace CHDB;

namespace DB
{

namespace py = pybind11;

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int PY_OBJECT_NOT_FOUND;
    extern const int PY_EXCEPTION_OCCURED;
}

PythonSource::PythonSource(
    CHDB::DataSourceWrapperPtr data_source_wrapper_,
    bool isInheritsFromPyReader_,
    bool isPandasDataFrame_,
    const Block & sample_block_,
    PyColumnVecPtr column_cache,
    size_t data_source_row_count,
    size_t max_block_size_,
    size_t stream_index,
    size_t num_streams,
    const FormatSettings & format_settings_,
    ArrowTableReaderPtr arrow_table_reader_,
    PrewhereActionsPtr prewhere_,
    TopKActionsPtr topk_,
    BlockBoundsPtr block_bounds_)
    : ISource(std::make_shared<Block>(prewhere_ ? prewhere_->output_header.cloneEmpty() : sample_block_.cloneEmpty()))
    , data_source_wrapper(std::move(data_source_wrapper_))
    , isInheritsFromPyReader(isInheritsFromPyReader_)
    , isPandasDataFrame(isPandasDataFrame_)
    , sample_block(sample_block_)
    , column_cache(column_cache)
    , data_source_row_count(data_source_row_count)
    , max_block_size(max_block_size_)
    , stream_index(stream_index)
    , num_streams(num_streams)
    , cursor(0)
    , format_settings(format_settings_)
    , arrow_table_reader(arrow_table_reader_)
    , prewhere(std::move(prewhere_))
    , topk(std::move(topk_))
    , block_bounds(std::move(block_bounds_))
{
}

template <typename T>
void PythonSource::insert_from_list(const py::list & obj, const MutableColumnPtr & column)
{
    py::gil_scoped_acquire acquire;
#if USE_JEMALLOC
    ::Memory::MemoryCheckScope memory_check_scope;
#endif
    for (auto && item : obj)
    {
        if constexpr (std::is_same_v<T, UInt8>)
        {
            if (PyBool_Check(item.ptr()))
            {
                column->insert(static_cast<UInt8>(py::cast<bool>(item) ? 1 : 0));
            }
            else
            {
                column->insert(py::cast<UInt8>(item));
            }
        }
        else if (item.is_none())
        {
            column->insertDefault();
        }
        else
        {
            column->insert(item.cast<T>());
        }
    }
}

void PythonSource::insert_string_from_array(const py::handle obj, const MutableColumnPtr & column)
{
    auto array = castToPyHandleVector(obj);
    auto * string_column = static_cast<ColumnString *>(column.get());
    for (auto && item : array)
    {
        auto * item_ptr = item.ptr();
        // FillColumnString assumes the item is a Python unicode object and uses
        // PyUnicode_* macros that reinterpret_cast obj to PyCompactUnicodeObject.
        // Array items can be None/NaN/bytes/etc., so validate here and fall back
        // to insertObjToStringColumn for non-unicode items (mirrors the pattern
        // in convert_string_array_to_block).
        if (!PyUnicode_Check(item_ptr))
        {
            insertObjToStringColumn(item_ptr, string_column);
            continue;
        }
        FillColumnString(item_ptr, string_column);
    }
}

void PythonSource::convert_string_array_to_block(
    PyObject ** buf, const MutableColumnPtr & column, const size_t offset, const size_t row_count, size_t stride)
{
    ColumnString * string_column = typeid_cast<ColumnString *>(column.get());
    if (string_column == nullptr)
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Column is not a string column");
    ColumnString::Chars & data = string_column->getChars();
    ColumnString::Offsets & offsets = string_column->getOffsets();
    offsets.reserve(row_count);

    /// stride == 0 is a numpy broadcast view over a single element: multiplying
    /// by the raw stride re-reads element 0, which is exactly what broadcast
    /// semantics require (and is also correct for the row_count <= 1 case).
    const auto * base_ptr = reinterpret_cast<const char *>(buf);

    for (size_t i = offset; i < offset + row_count; ++i)
    {
        auto * obj = *reinterpret_cast<PyObject * const *>(base_ptr + i * stride);
        if (!PyUnicode_Check(obj))
        {
            insertObjToStringColumn(obj, string_column);
            continue;
        }
        FillColumnString(obj, string_column);
        // Try to help reserve memory for the string column data every 100 rows to avoid frequent reallocations
        // Check the avg size of the string column data and reserve memory accordingly
        if ((i - offset) % 10 == 9)
        {
            size_t data_size = data.size();
            size_t counter = i - offset + 1;
            size_t avg_size = data_size / counter;
            size_t reserve_size = avg_size * row_count;
            if (reserve_size > data.capacity())
            {
                LOG_DEBUG(logger, "Reserving memory for string column data from {} to {}, avg size: {}, count: {}",
                            data_size, reserve_size, avg_size, counter);
                data.reserve(reserve_size);
            }
        }
    }
}

template <typename T>
void PythonSource::insert_from_ptr(
    const void * ptr, const MutableColumnPtr & column, const size_t offset, const size_t row_count, size_t stride,
    const std::shared_ptr<void> & borrow_guard)
{
    /// stride == 0 means a broadcast view backed by a single element; it must
    /// take the per-element loop below (which re-reads element 0), never the
    /// contiguous paths that would walk row_count * sizeof(T) bytes of memory
    /// that does not belong to the array.
    if (stride == sizeof(T))
    {
        const char * start = static_cast<const char *>(ptr) + offset * sizeof(T);

        /// Zero-copy: mount the numpy buffer slice instead of copying it.
        if constexpr (!is_decimal<T>)
        {
            if (borrow_guard && row_count * sizeof(T) >= 4096
                && reinterpret_cast<uintptr_t>(start) % alignof(T) == 0)
            {
                if (auto * vec = typeid_cast<ColumnVector<T> *>(column.get());
                    vec && vec->getData().empty()
                    && CHDB::borrowTailReadable(start + row_count * sizeof(T)))
                {
                    vec->borrowData(start, row_count, borrow_guard);
                    return;
                }
            }
        }

        column->reserve(row_count);
        ColumnVectorHelper * helper = static_cast<ColumnVectorHelper *>(column.get());
        helper->appendRawData<sizeof(T)>(start, row_count);
    }
    else
    {
        column->reserve(row_count);
        const auto * base_ptr = static_cast<const char *>(ptr);
        for (size_t i = offset; i < offset + row_count; ++i)
        {
            T value = *reinterpret_cast<const T *>(base_ptr + i * stride);
            column->insert(value);
        }
    }
}

template <typename T>
ColumnPtr PythonSource::convert_and_insert(const py::object & obj, UInt32 scale, bool is_json)
{
    MutableColumnPtr column;
    if (is_json)
    {
        auto nested_type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
        auto data_type = std::make_shared<DataTypeNullable>(std::move(nested_type));
        column = data_type->createColumn();
    }
    else if constexpr (std::is_same_v<T, DateTime64> || std::is_same_v<T, Decimal128> || std::is_same_v<T, Decimal256>)
        column = ColumnDecimal<T>::create(0, scale);
    else if constexpr (std::is_same_v<T, String>)
        column = ColumnString::create();
    else
        column = ColumnVector<T>::create();

    std::string type_name;
    size_t row_count = 0;
    py::handle py_array;
    py::handle tmp;
    SCOPE_EXIT({
        if (!tmp.is_none())
            tmp.dec_ref();
    });
    const void * data = tryGetPyArray(obj, py_array, tmp, type_name, row_count);
    if (type_name == "list")
    {
        if (is_json)
        {
            CHDB::ListScan::scanObject(0, row_count, format_settings, obj, column);
            return column;
        }

        //reserve the size of the column
        column->reserve(row_count);
        insert_from_list<T>(obj, column);
        return column;
    }

    if (!py_array.is_none() && data != nullptr)
    {
        if (is_json)
            CHDB::PandasScan::scanObject(0, row_count, format_settings, data, column);
        else if constexpr (std::is_same_v<T, String>)
            insert_string_from_array(py_array, column);
        else
            insert_from_ptr<T>(data, column, 0, row_count, sizeof(T));
        return column;
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unsupported type {} for value {}", getPyType(obj), castToStr(obj));
}


template <typename T>
ColumnPtr PythonSource::convert_and_insert_array(const ColumnWrapper & col_wrap, size_t & cursor, const size_t count, UInt32 scale)
{
    MutableColumnPtr column;
    if constexpr (std::is_same_v<T, DateTime64> || std::is_same_v<T, Decimal128> || std::is_same_v<T, Decimal256>)
        column = ColumnDecimal<T>::create(0, scale);
    else if constexpr (std::is_same_v<T, String>)
        column = ColumnString::create();
    else
        column = ColumnVector<T>::create();

    if (col_wrap.data.is_none())
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Column data is None");

    if (col_wrap.py_type == "list")
    {
        py::gil_scoped_acquire acquire;
        insert_from_list<T>(col_wrap.data.cast<py::list>().attr("__getitem__")(py::slice(cursor, cursor + count, 1)), column);
        return column;
    }
    if constexpr (std::is_same_v<T, String>)
        convert_string_array_to_block(static_cast<PyObject **>(col_wrap.buf), column, cursor, count, col_wrap.stride);
    else
        insert_from_ptr<T>(col_wrap.buf, column, cursor, count, col_wrap.stride, col_wrap.borrow_guard);

    return column;
}

void PythonSource::destory(PyObjectVecPtr & data)
{
    // manually destory PyObjectVec and trigger the py::object dec_ref with GIL holded
    py::gil_scoped_acquire acquire;
    data->clear();
    data.reset();
}

Chunk PythonSource::genChunk(size_t & num_rows, PyObjectVecPtr data)
{
    Columns columns(sample_block.columns());
    for (size_t i = 0; i < data->size(); ++i)
    {
        if (i == 0)
            num_rows = getObjectLength((*data)[i]);
        const auto & column = (*data)[i];
        const auto & type = sample_block.getByPosition(i).type;
        auto data_type = removeNullable(type);
        WhichDataType which(data_type);

        try
        {
            // Dispatch to the appropriate conversion function based on data type
            if (which.isUInt8())
                columns[i] = convert_and_insert<UInt8>(column);
            else if (which.isUInt16())
                columns[i] = convert_and_insert<UInt16>(column);
            else if (which.isUInt32())
                columns[i] = convert_and_insert<UInt32>(column);
            else if (which.isUInt64())
                columns[i] = convert_and_insert<UInt64>(column);
            else if (which.isUInt128())
                columns[i] = convert_and_insert<UInt128>(column);
            else if (which.isUInt256())
                columns[i] = convert_and_insert<UInt256>(column);
            else if (which.isInt8())
                columns[i] = convert_and_insert<Int8>(column);
            else if (which.isInt16())
                columns[i] = convert_and_insert<Int16>(column);
            else if (which.isInt32())
                columns[i] = convert_and_insert<Int32>(column);
            else if (which.isInt64())
                columns[i] = convert_and_insert<Int64>(column);
            else if (which.isInt128())
                columns[i] = convert_and_insert<Int128>(column);
            else if (which.isInt256())
                columns[i] = convert_and_insert<Int256>(column);
            else if (which.isFloat32())
                columns[i] = convert_and_insert<Float32>(column);
            else if (which.isFloat64())
                columns[i] = convert_and_insert<Float64>(column);
            else if (which.isDecimal128())
            {
                const auto & dtype = typeid_cast<const DataTypeDecimal<Decimal128> *>(type.get());
                columns[i] = convert_and_insert<Decimal128>(column, dtype->getScale());
            }
            else if (which.isDecimal256())
            {
                const auto & dtype = typeid_cast<const DataTypeDecimal<Decimal256> *>(type.get());
                columns[i] = convert_and_insert<Decimal256>(column, dtype->getScale());
            }
            else if (which.isDateTime())
                columns[i] = convert_and_insert<UInt32>(column);
            else if (which.isDateTime64())
            {
                const auto & dtype = typeid_cast<const DataTypeDateTime64 *>(type.get());
                columns[i] = convert_and_insert<DateTime64>(column, dtype->getScale());
            }
            else if (which.isString())
                columns[i] = convert_and_insert<String>(column);
            else if (which.isObject())
                columns[i] = convert_and_insert<String>(column, 0, true);
            else
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "Unsupported type {} for column {}",
                    type->getName(),
                    sample_block.getByPosition(i).name);
        }
        catch (Exception & e)
        {
            destory(data);
            LOG_ERROR(logger, "Error processing column \"{}\": {}", sample_block.getByPosition(i).name, e.what());
            throw Exception(
                ErrorCodes::PY_EXCEPTION_OCCURED,
                "Error processing column \"{}\": {}",
                sample_block.getByPosition(i).name,
                e.what());
        }
        catch (std::exception & e)
        {
            destory(data);
            LOG_ERROR(logger, "Error processing column \"{}\": {}", sample_block.getByPosition(i).name, e.what());
            throw Exception(
                ErrorCodes::PY_EXCEPTION_OCCURED,
                "Error processing column \"{}\": {}",
                sample_block.getByPosition(i).name,
                e.what());
        }
        catch (...)
        {
            destory(data);
            LOG_ERROR(logger, "Error processing column \"{}\": unknown exception", sample_block.getByPosition(i).name);
            throw Exception(
                ErrorCodes::PY_EXCEPTION_OCCURED,
                "Error processing column \"{}\": unknown exception",
                sample_block.getByPosition(i).name);
        }
    }

    destory(data);

    if (num_rows == 0)
        return {};

    return Chunk(std::move(columns), num_rows);
}

std::shared_ptr<PyObjectVec>
PythonSource::scanData(const py::object & data, const std::vector<std::string> & col_names, size_t & cursor, size_t count)
{
    py::gil_scoped_acquire acquire;
    auto block = std::make_shared<PyObjectVec>();
    // Access columns directly by name and slice
    for (const auto & col : col_names)
    {
        py::object col_data = data[py::str(col)]; // Use dictionary-style access
        block->push_back(col_data.attr("__getitem__")(py::slice(cursor, cursor + count, 1)));
    }

    if (!block->empty())
        cursor += py::len((*block)[0]); // Update cursor based on the length of the first column slice

    return std::move(block);
}



ColumnPtr PythonSource::convertOneColumn(size_t i, size_t offset, size_t count)
{
    const auto & col = (*column_cache)[i];
    const auto & type = sample_block.getByPosition(i).type;

    if (col.is_virtual)
    {
        chassert(sample_block.getByPosition(i).name == "_row_id");
        auto row_id_column = ColumnVector<UInt64>::create(count);
        auto & row_id_data = row_id_column->getData();
        iota(row_id_data.data(), count, static_cast<UInt64>(offset));
        return row_id_column;
    }

    bool is_nullable = type->isNullable();
    auto data_type = removeNullable(type);
    WhichDataType which(data_type);

    try
    {
        if (isPandasDataFrame && (is_nullable || col.is_category || col.is_arrow_string))
            return PandasScan::scanColumn(col, offset, count, format_settings);

        // Dispatch to the appropriate conversion function based on data type
        if (which.isUInt8())
            return convert_and_insert_array<UInt8>(col, offset, count);
        else if (which.isUInt16())
            return convert_and_insert_array<UInt16>(col, offset, count);
        else if (which.isUInt32())
            return convert_and_insert_array<UInt32>(col, offset, count);
        else if (which.isUInt64())
            return convert_and_insert_array<UInt64>(col, offset, count);
        else if (which.isUInt128())
            return convert_and_insert_array<UInt128>(col, offset, count);
        else if (which.isUInt256())
            return convert_and_insert_array<UInt256>(col, offset, count);
        else if (which.isInt8())
            return convert_and_insert_array<Int8>(col, offset, count);
        else if (which.isInt16())
            return convert_and_insert_array<Int16>(col, offset, count);
        else if (which.isInt32())
            return convert_and_insert_array<Int32>(col, offset, count);
        else if (which.isInt64())
            return convert_and_insert_array<Int64>(col, offset, count);
        else if (which.isInt128())
            return convert_and_insert_array<Int128>(col, offset, count);
        else if (which.isInt256())
            return convert_and_insert_array<Int256>(col, offset, count);
        else if (which.isFloat32())
            return convert_and_insert_array<Float32>(col, offset, count);
        else if (which.isFloat64())
            return convert_and_insert_array<Float64>(col, offset, count);
        else if (which.isDecimal128())
        {
            const auto & dtype = typeid_cast<const DataTypeDecimal<Decimal128> *>(type.get());
            return convert_and_insert_array<Decimal128>(col, offset, count, dtype->getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & dtype = typeid_cast<const DataTypeDecimal<Decimal256> *>(type.get());
            return convert_and_insert_array<Decimal256>(col, offset, count, dtype->getScale());
        }
        else if (which.isDateTime())
            return convert_and_insert_array<UInt32>(col, offset, count);
        else if (which.isDateTime64())
        {
            const auto & dtype = typeid_cast<const DataTypeDateTime64 *>(type.get());
            return convert_and_insert_array<DateTime64>(col, offset, count, dtype->getScale());
        }
        else if (which.isDate32())
            return convert_and_insert_array<Int32>(col, offset, count);
        else if (which.isDate())
            return convert_and_insert_array<UInt16>(col, offset, count);
        else if (which.isString())
            return convert_and_insert_array<String>(col, offset, count);
        else if (which.isNullable())
            return convert_and_insert_array<String>(col, offset, count);
        else if (which.isObject())
        {
            if (col.py_type == "list")
                return CHDB::ListScan::scanObject(col, offset, count, format_settings);

            chassert(!isPandasDataFrame);
            return CHDB::PandasScan::scanObject(col, offset, count, format_settings);
        }
        else
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unsupported type {} for column {}", type->getName(), col.name);
    }
    catch (std::exception & e)
    {
        LOG_ERROR(logger, "Error processing column \"{}\": {}", col.name, e.what());
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Error processing column \"{}\": {}", col.name, e.what());
    }
    catch (...)
    {
        LOG_ERROR(logger, "Error processing column \"{}\": unknown exception", col.name);
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Error processing column \"{}\": unknown exception", col.name);
    }
}

Chunk PythonSource::scanDataToChunk()
{
    auto names = sample_block.getNames();
    if (names.empty())
        return {};

    auto [offset, count] = calculateOffsetAndCount();
    if (count == 0)
        return {};
    LOG_DEBUG(logger, "Stream index {} Reading {} rows from {}", stream_index, count, offset);

    Columns columns(sample_block.columns());
    for (size_t i = 0; i < sample_block.columns(); ++i)
        columns[i] = convertOneColumn(i, offset, count);

    return Chunk(std::move(columns), count);
}

namespace
{

template <typename ColumnType, typename ValueT>
MutableColumnPtr gatherFromTypedBuffer(
    const void * buf, size_t stride, size_t offset, size_t count, const IColumn::Filter & mask, size_t selected, MutableColumnPtr column)
{
    auto & container = assert_cast<ColumnType &>(*column).getData();
    container.reserve(selected);
    const auto * base = reinterpret_cast<const char *>(buf);
    /// stride == 0 is a numpy broadcast view over a single element: multiplying
    /// by the raw stride re-reads element 0, which is exactly what broadcast
    /// semantics require (and is also correct for the count <= 1 case).
    for (size_t i = 0; i < count; ++i)
        if (mask[i])
            container.push_back(*reinterpret_cast<const ValueT *>(base + (offset + i) * stride));
    return column;
}

/// NaN (floats) / NaT (datetimes) null maps for gathered values.
template <typename T>
void fillGatheredNullMap(const IColumn & data_column, NullMap & null_map)
{
    const auto & data = assert_cast<const T &>(data_column).getData();
    const size_t n = data.size();
    null_map.resize(n);
    for (size_t i = 0; i < n; ++i)
    {
        if constexpr (std::is_same_v<T, ColumnVector<Float32>> || std::is_same_v<T, ColumnVector<Float64>>)
            null_map[i] = data[i] != data[i] ? 1 : 0;
        else if constexpr (std::is_same_v<T, ColumnDecimal<DateTime64>>)
            null_map[i] = data[i].value == std::numeric_limits<Int64>::min() ? 1 : 0;
        else
            null_map[i] = false; /// nullable integers carry an explicit pandas mask instead
    }
}

/// Nullable wrapper for gathered pandas values: the null map comes from the
/// pandas mask when present, otherwise from NaN/NaT sentinels in the data.
template <typename NullSourceColumn>
ColumnPtr wrapGatheredNullable(
    MutableColumnPtr nested,
    const ColumnWrapper & col,
    size_t offset,
    size_t count,
    const IColumn::Filter & mask,
    size_t selected)
{
    auto null_map_column = ColumnVector<UInt8>::create();
    auto & null_map = null_map_column->getData();
    if (col.registered_array)
    {
        null_map.reserve(selected);
        const auto * mask_base = reinterpret_cast<const char *>(col.registered_array->numpy_array.data());
        for (size_t r = 0; r < count; ++r)
            if (mask[r])
                null_map.push_back(*reinterpret_cast<const bool *>(mask_base + (offset + r) * col.mask_stride) ? 1 : 0);
    }
    else
        fillGatheredNullMap<NullSourceColumn>(*nested, null_map);
    return ColumnNullable::create(std::move(nested), std::move(null_map_column));
}

}

/// Source column materialized only for the selected rows of one block.
ColumnPtr PythonSource::gatherOneColumn(size_t i, size_t offset, size_t count, const IColumn::Filter & mask, size_t selected)
{
    const auto & col = (*column_cache)[i];
    const auto & type = sample_block.getByPosition(i).type;

    if (col.is_virtual)
    {
        auto row_id_column = ColumnVector<UInt64>::create();
        auto & row_id_data = row_id_column->getData();
        row_id_data.reserve(selected);
        for (size_t r = 0; r < count; ++r)
            if (mask[r])
                row_id_data.push_back(offset + r);
        return row_id_column;
    }

    if (isPandasDataFrame && col.is_arrow_string)
        return PandasScan::scanColumnFiltered(col, offset, count, mask, selected);

    const bool is_nullable = type->isNullable();
    auto inner_type = removeNullable(type);
    WhichDataType which(inner_type);


    auto gather_plain = [&](auto value_tag) -> MutableColumnPtr
    {
        using ValueT = decltype(value_tag);
        return gatherFromTypedBuffer<ColumnVector<ValueT>, ValueT>(
            col.buf, col.stride, offset, count, mask, selected, ColumnVector<ValueT>::create());
    };

    /// At mid/high selectivity a full conversion (memcpy or zero-copy borrow)
    /// plus one vectorized filter beats the per-row gather loop. Strings are
    /// exempt: gathering them always avoids copying the unselected payload.
    const bool gather_worthwhile = selected * 8 <= count;
    if (!gather_worthwhile && !col.is_virtual)
        return convertOneColumn(i, offset, count)->filter(mask, selected);

    if (!col.is_object_type && !col.is_category && col.buf)
    {
        /// Non-nullable contiguous/strided numeric buffers.
        if (!is_nullable)
        {
            if (which.isUInt8()) return gather_plain(UInt8{});
            if (which.isUInt16() || which.isDate()) return gather_plain(UInt16{});
            if (which.isUInt32() || which.isDateTime()) return gather_plain(UInt32{});
            if (which.isUInt64()) return gather_plain(UInt64{});
            if (which.isInt8()) return gather_plain(Int8{});
            if (which.isInt16()) return gather_plain(Int16{});
            if (which.isInt32() || which.isDate32()) return gather_plain(Int32{});
            if (which.isInt64()) return gather_plain(Int64{});
            if (which.isFloat32()) return gather_plain(Float32{});
            if (which.isFloat64()) return gather_plain(Float64{});
            if (which.isDateTime64())
            {
                const auto & dtype = typeid_cast<const DataTypeDateTime64 *>(inner_type.get());
                return gatherFromTypedBuffer<ColumnDecimal<DateTime64>, Int64>(
                    col.buf, col.stride, offset, count, mask, selected, ColumnDecimal<DateTime64>::create(0, dtype->getScale()));
            }
        }
        else if (isPandasDataFrame)
        {
            /// Nullable pandas columns: gather values, then derive the null map
            /// from the pandas mask / NaN / NaT exactly like PandasScan does.
            if (which.isFloat32())
                return wrapGatheredNullable<ColumnVector<Float32>>(gather_plain(Float32{}), col, offset, count, mask, selected);
            if (which.isFloat64())
                return wrapGatheredNullable<ColumnVector<Float64>>(gather_plain(Float64{}), col, offset, count, mask, selected);
            if (which.isInt8())
                return wrapGatheredNullable<ColumnVector<Int8>>(gather_plain(Int8{}), col, offset, count, mask, selected);
            if (which.isInt16())
                return wrapGatheredNullable<ColumnVector<Int16>>(gather_plain(Int16{}), col, offset, count, mask, selected);
            if (which.isInt32())
                return wrapGatheredNullable<ColumnVector<Int32>>(gather_plain(Int32{}), col, offset, count, mask, selected);
            if (which.isInt64())
                return wrapGatheredNullable<ColumnVector<Int64>>(gather_plain(Int64{}), col, offset, count, mask, selected);
            if (which.isUInt8())
                return wrapGatheredNullable<ColumnVector<UInt8>>(gather_plain(UInt8{}), col, offset, count, mask, selected);
            if (which.isUInt16())
                return wrapGatheredNullable<ColumnVector<UInt16>>(gather_plain(UInt16{}), col, offset, count, mask, selected);
            if (which.isUInt32())
                return wrapGatheredNullable<ColumnVector<UInt32>>(gather_plain(UInt32{}), col, offset, count, mask, selected);
            if (which.isUInt64())
                return wrapGatheredNullable<ColumnVector<UInt64>>(gather_plain(UInt64{}), col, offset, count, mask, selected);
            if (which.isDateTime64())
            {
                const auto & dtype = typeid_cast<const DataTypeDateTime64 *>(inner_type.get());
                auto nested = gatherFromTypedBuffer<ColumnDecimal<DateTime64>, Int64>(
                    col.buf, col.stride, offset, count, mask, selected, ColumnDecimal<DateTime64>::create(0, dtype->getScale()));
                return wrapGatheredNullable<ColumnDecimal<DateTime64>>(std::move(nested), col, offset, count, mask, selected);
            }
        }
    }

    /// Fallback for exotic columns: convert the block, then filter.
    return convertOneColumn(i, offset, count)->filter(mask, selected);
}

/// One block of the PREWHERE-enabled scan, executed in steps (cheapest
/// conditions first): every step materializes its source columns only for
/// the rows that survived the previous steps, evaluates its filter and
/// narrows the selection. Returns an empty chunk for fully-filtered blocks
/// with `exhausted` = false, so the caller can continue with the next block.
bool PythonSource::computeSurvivors(size_t offset, size_t count, Block & work, IColumn::Filter & cumulative, size_t & current_rows)
{
    current_rows = count;

    for (size_t step_idx = 0; step_idx < prewhere->steps.size(); ++step_idx)
    {
        const auto & step = prewhere->steps[step_idx];

        /// Arrow-direct predicate (single-step PREWHERE only, so cumulative is
        /// empty here): evaluate the filter on the Arrow buffers without
        /// materializing the string column into a ColumnString.
        const auto & arrow_pred = prewhere->step_arrow_preds[step_idx];
        if (arrow_pred.active)
        {
            IColumn::Filter filt(count);
            PandasScan::evalArrowStringPredicate(
                (*column_cache)[arrow_pred.sample_index], offset, count, arrow_pred.kind, arrow_pred.needle,
                reinterpret_cast<unsigned char *>(filt.data()));
            const size_t selected = countBytesInFilter(filt);
            if (selected == 0)
                return false;
            if (selected < count)
                cumulative = std::move(filt);
            current_rows = selected;
            continue;
        }

        for (const auto & [idx, name] : prewhere->step_source_inputs[step_idx])
        {
            ColumnPtr column = cumulative.empty() ? convertOneColumn(idx, offset, count)
                                                  : gatherOneColumn(idx, offset, count, cumulative, current_rows);
            work.insert({std::move(column), sample_block.getByPosition(idx).type, name});
        }

        size_t num_rows = current_rows;
        step->actions->execute(work, num_rows);

        const auto & filter_entry = work.getByName(step->filter_column_name);
        FilterDescription filter_description(*filter_entry.column);
        const size_t selected = filter_description.countBytesInFilter();
        if (selected == 0)
            return false;

        if (selected < current_rows)
        {
            for (auto & work_col : work)
                if (work_col.name != step->filter_column_name)
                    work_col.column = work_col.column->filter(*filter_description.data, selected);

            if (cumulative.empty())
                cumulative.assign(filter_description.data->begin(), filter_description.data->end());
            else
            {
                /// Narrow the cumulative selection: walk previously-selected
                /// rows and AND them with this step's filter.
                size_t j = 0;
                for (size_t r = 0; r < count; ++r)
                    if (cumulative[r])
                        cumulative[r] = (*filter_description.data)[j++];
            }
            current_rows = selected;
        }

        if (step->remove_filter_column)
            work.erase(step->filter_column_name);
        else
        {
            /// Rows are filtered; the kept filter column becomes all-true.
            auto & kept = work.getByName(step->filter_column_name);
            kept.column = kept.type->createColumnConst(current_rows, 1u)->convertToFullColumnIfConst();
        }
    }

    return true;
}

Chunk PythonSource::scanDataToChunkPrewhere(bool & exhausted)
{
    auto [offset, count] = calculateOffsetAndCount();
    if (count == 0)
    {
        exhausted = true;
        return {};
    }
    exhausted = false;

    Block work;
    size_t current_rows = count;
    IColumn::Filter cumulative; /// selection over the original block rows; empty = all selected

    if (!computeSurvivors(offset, count, work, cumulative, current_rows))
        return {};

    /// Assemble the output following the precomputed plan.
    Columns columns;
    columns.reserve(prewhere->outputs.size());
    for (const auto & plan : prewhere->outputs)
    {
        switch (plan.kind)
        {
            case PrewhereActions::OutputKind::KeepFilterColumn:
            {
                const auto & out_type = prewhere->output_header.getByName(plan.name).type;
                columns.push_back(out_type->createColumnConst(current_rows, 1u)->convertToFullColumnIfConst());
                break;
            }
            case PrewhereActions::OutputKind::FromWorkBlock:
            {
                columns.push_back(work.getByName(plan.name).column);
                break;
            }
            case PrewhereActions::OutputKind::GatherFromSource:
            {
                if (cumulative.empty())
                    columns.push_back(convertOneColumn(plan.sample_index, offset, count));
                else
                    columns.push_back(gatherOneColumn(plan.sample_index, offset, count, cumulative, current_rows));
                break;
            }
        }
    }

    return Chunk(std::move(columns), current_rows);
}

ColumnPtr PythonSource::gatherRowsByIndex(size_t sample_index, const PaddedPODArray<UInt64> & gidx, size_t k)
{
    /// Per-row materialization (count == 1) reuses the full per-type conversion
    /// path of convertOneColumn for every column kind; k is tiny (<= limit).
    MutableColumnPtr out = sample_block.getByPosition(sample_index).type->createColumn();
    out->reserve(k);
    for (size_t j = 0; j < k; ++j)
    {
        ColumnPtr one = convertOneColumn(sample_index, gidx[j], 1);
        out->insertFrom(*one, 0);
    }
    return out;
}

Chunk PythonSource::scanDataToChunkTopK()
{
    const size_t n_sort = topk->sort_sample_indices.size();

    /// Phase 1: scan every block of this stream, keep only sort-key columns and
    /// the global row index of each surviving (PREWHERE-passing) row.
    MutableColumns sort_accum(n_sort);
    for (size_t s = 0; s < n_sort; ++s)
        sort_accum[s] = sample_block.getByPosition(topk->sort_sample_indices[s]).type->createColumn();
    PaddedPODArray<UInt64> gidx_accum;

    for (;;)
    {
        auto [offset, count] = calculateOffsetAndCount();
        if (count == 0)
            break;

        IColumn::Filter cumulative; /// empty == all rows of the block selected
        size_t current_rows = count;
        if (prewhere)
        {
            Block work;
            if (!computeSurvivors(offset, count, work, cumulative, current_rows))
                continue;
        }

        for (size_t s = 0; s < n_sort; ++s)
        {
            const size_t si = topk->sort_sample_indices[s];
            ColumnPtr c = cumulative.empty() ? convertOneColumn(si, offset, count)
                                             : gatherOneColumn(si, offset, count, cumulative, current_rows);
            sort_accum[s]->insertRangeFrom(*c, 0, current_rows);
        }

        if (cumulative.empty())
            for (size_t r = 0; r < count; ++r)
                gidx_accum.push_back(offset + r);
        else
            for (size_t r = 0; r < count; ++r)
                if (cumulative[r])
                    gidx_accum.push_back(offset + r);
    }

    if (gidx_accum.empty())
        return {};

    /// Partial-sort the survivors by the ORDER BY keys (the global-index column
    /// rides along) and keep the top-`limit`.
    Block sort_block;
    for (size_t s = 0; s < n_sort; ++s)
        sort_block.insert({std::move(sort_accum[s]),
                           sample_block.getByPosition(topk->sort_sample_indices[s]).type,
                           topk->sort_description[s].column_name});

    static const String gidx_name = "__topk_gidx";
    auto gidx_col = ColumnVector<UInt64>::create();
    gidx_col->getData().swap(gidx_accum);
    sort_block.insert({std::move(gidx_col), std::make_shared<DataTypeUInt64>(), gidx_name});

    sortBlock(sort_block, topk->sort_description, topk->limit);

    const auto & sorted_gidx = assert_cast<const ColumnVector<UInt64> &>(*sort_block.getByName(gidx_name).column).getData();
    const size_t k = std::min<size_t>(topk->limit, sorted_gidx.size());

    /// Phase 2: materialize all output columns for just the top-k global indices.
    Columns columns;
    if (prewhere)
    {
        columns.reserve(prewhere->outputs.size());
        for (const auto & plan : prewhere->outputs)
        {
            switch (plan.kind)
            {
                case PrewhereActions::OutputKind::KeepFilterColumn:
                {
                    const auto & out_type = prewhere->output_header.getByName(plan.name).type;
                    columns.push_back(out_type->createColumnConst(k, 1u)->convertToFullColumnIfConst());
                    break;
                }
                case PrewhereActions::OutputKind::GatherFromSource:
                    columns.push_back(gatherRowsByIndex(plan.sample_index, sorted_gidx, k));
                    break;
                case PrewhereActions::OutputKind::FromWorkBlock:
                    /// A PREWHERE input that is also selected (e.g. URL): it is a
                    /// plain source column (validated in readImpl), so gather it
                    /// from the source by row index like any other output.
                    columns.push_back(gatherRowsByIndex(sample_block.getPositionByName(plan.name), sorted_gidx, k));
                    break;
            }
        }
    }
    else
    {
        const size_t ncols = sample_block.columns();
        columns.reserve(ncols);
        for (size_t i = 0; i < ncols; ++i)
            columns.push_back(gatherRowsByIndex(i, sorted_gidx, k));
    }

    return Chunk(std::move(columns), k);
}

Chunk PythonSource::generate()
{
    size_t num_rows = 0;
    auto names = sample_block.getNames();
    if (names.empty())
        return {};

    try
    {
        if (arrow_table_reader)
        {
            auto chunk = arrow_table_reader->readNextChunk(stream_index);
            return chunk;
        }

        if (isInheritsFromPyReader)
        {
            PyObjectVecPtr data;
            py::gil_scoped_acquire acquire;
            data = std::move(castToSharedPtrVector<py::object>(data_source_wrapper->getDataSource().attr("read")(names, max_block_size)));
            if (data->empty())
                return {};

            return std::move(genChunk(num_rows, data));
        }

        if (topk)
        {
            /// Single-shot: one stream emits its local top-N then finishes.
            if (topk_done)
                return {};
            topk_done = true;
            return scanDataToChunkTopK();
        }

        if (prewhere)
        {
            /// An empty chunk would end the source, so fully-filtered blocks
            /// are skipped here and the scan continues with the next block.
            bool exhausted = false;
            while (!exhausted)
            {
                auto chunk = scanDataToChunkPrewhere(exhausted);
                if (chunk.getNumRows() > 0)
                    return chunk;
            }
            return {};
        }

        return std::move(scanDataToChunk());
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Python data handling {}", e.what());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Python data handling unknown exception");
    }
}

std::pair<size_t, size_t> PythonSource::calculateOffsetAndCount()
{
    /// Streams take blocks round-robin: stream i emits blocks
    /// i, i + num_streams, i + 2*num_streams, ...
    if (block_bounds)
    {
        /// Boundaries additionally cut at Arrow chunk starts so a block never
        /// spans chunks and its string payload can be mounted zero-copy.
        const size_t n_blocks = block_bounds->size() - 1;
        const size_t block_idx = stream_index + blocks_emitted * num_streams;
        if (block_idx >= n_blocks)
            return std::make_pair(0, 0);

        ++blocks_emitted;
        const size_t offset = (*block_bounds)[block_idx];
        return std::make_pair(offset, (*block_bounds)[block_idx + 1] - offset);
    }

    /// Fixed max_block_size boundaries keep blocks identical across queries
    /// regardless of stream count.
    const size_t n_blocks = (data_source_row_count + max_block_size - 1) / max_block_size;
    const size_t block_idx = stream_index + blocks_emitted * num_streams;
    if (block_idx >= n_blocks)
        return std::make_pair(0, 0);

    ++blocks_emitted;
    const size_t offset = block_idx * max_block_size;
    return std::make_pair(offset, std::min(max_block_size, data_source_row_count - offset));
}

}