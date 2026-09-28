#include "PythonUDFConversion.h"

#include "PyDateTimeHelper.h"
#include "PythonConversion.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/readIntText.h>
#include <base/defines.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}
}


namespace CHDB
{

void throwUnsupportedFastArgType(DB::TypeIndex type_id)
{
    throw DB::Exception(
        DB::ErrorCodes::LOGICAL_ERROR,
        "convertArgFast called for unsupported type id {}",
        static_cast<int>(type_id));
}

namespace
{

void handleFloat(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    double d)
{
    if (actual_type->getTypeId() != DB::TypeIndex::Float32 && actual_type->getTypeId() != DB::TypeIndex::Float64)
        throw DB::Exception(
            DB::ErrorCodes::TYPE_MISMATCH,
            "Cannot convert Python float to {}",
            actual_type->getName());

    column.insert(DB::Field(static_cast<Float64>(d)));
}

void handleInteger(
    DB::IColumn & column,
    DB::TypeIndex type_id,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    auto * ptr = value.ptr();
    int overflow;
    int64_t int_val = PyLong_AsLongLongAndOverflow(ptr, &overflow);

    if (overflow != 0)
    {
        PyErr_Clear();

        switch (type_id)
        {
            case DB::TypeIndex::Int8:
            case DB::TypeIndex::Int16:
            case DB::TypeIndex::Int32:
            case DB::TypeIndex::Int64:
            case DB::TypeIndex::UInt8:
            case DB::TypeIndex::UInt16:
            case DB::TypeIndex::UInt32:
                throw DB::Exception(
                    DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer out of range for type {}",
                    actual_type->getName());
            default:
                break;
        }

        if (overflow == 1)
        {
            UInt64 unsigned_val = PyLong_AsUnsignedLongLong(ptr);
            if (!PyErr_Occurred())
            {
                switch (type_id)
                {
                    case DB::TypeIndex::UInt64:
                        column.insert(DB::Field(unsigned_val));
                        return;
                    case DB::TypeIndex::UInt128:
                        column.insert(DB::Field(UInt128(unsigned_val)));
                        return;
                    case DB::TypeIndex::Int128:
                        column.insert(DB::Field(Int128(unsigned_val)));
                        return;
                    case DB::TypeIndex::UInt256:
                        column.insert(DB::Field(UInt256(unsigned_val)));
                        return;
                    case DB::TypeIndex::Int256:
                        column.insert(DB::Field(Int256(unsigned_val)));
                        return;
                    default:
                        break;
                }
            }
            if (type_id == DB::TypeIndex::UInt64)
                throw DB::Exception(
                    DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer out of range for type {}",
                    actual_type->getName());
            PyErr_Clear();
        }

        if (type_id == DB::TypeIndex::Int128 || type_id == DB::TypeIndex::UInt128
            || type_id == DB::TypeIndex::Int256 || type_id == DB::TypeIndex::UInt256)
        {
            py::str py_str(value);
            std::string s = py_str.cast<std::string>();
            DB::ReadBufferFromMemory buf(s.data(), s.size());

            switch (type_id)
            {
                case DB::TypeIndex::Int128:
                {
                    Int128 v;
                    DB::readIntText(v, buf);
                    column.insert(DB::Field(v));
                    return;
                }
                case DB::TypeIndex::UInt128:
                {
                    UInt128 v;
                    DB::readIntText(v, buf);
                    column.insert(DB::Field(v));
                    return;
                }
                case DB::TypeIndex::Int256:
                {
                    Int256 v;
                    DB::readIntText(v, buf);
                    column.insert(DB::Field(v));
                    return;
                }
                case DB::TypeIndex::UInt256:
                {
                    UInt256 v;
                    DB::readIntText(v, buf);
                    column.insert(DB::Field(v));
                    return;
                }
                default:
                    break;
            }
        }

        double number = PyLong_AsDouble(value.ptr());
        if (number == -1.0 && PyErr_Occurred()) {
            PyErr_Clear();
            throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH, "An error occurred attempting to convert a python integer");
        }
		handleFloat(column, actual_type, number);
        return;
    }

    if (int_val == -1 && PyErr_Occurred())
    {
        PyErr_Clear();
        throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH, "Failed to convert Python integer");
    }

    switch (type_id)
    {
        case DB::TypeIndex::UInt8:
            if (int_val < 0 || int_val > std::numeric_limits<uint8_t>::max())
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<UInt8>(int_val)));
            break;
        case DB::TypeIndex::UInt16:
            if (int_val < 0 || int_val > std::numeric_limits<uint16_t>::max())
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<UInt16>(int_val)));
            break;
        case DB::TypeIndex::UInt32:
            if (int_val < 0 || int_val > std::numeric_limits<uint32_t>::max())
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<UInt32>(int_val)));
            break;
        case DB::TypeIndex::UInt64:
            if (int_val < 0)
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<UInt64>(int_val)));
            break;
        case DB::TypeIndex::Int8:
            if (int_val < std::numeric_limits<int8_t>::min() || int_val > std::numeric_limits<int8_t>::max())
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<Int8>(int_val)));
            break;
        case DB::TypeIndex::Int16:
            if (int_val < std::numeric_limits<int16_t>::min() || int_val > std::numeric_limits<int16_t>::max())
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<Int16>(int_val)));
            break;
        case DB::TypeIndex::Int32:
            if (int_val < std::numeric_limits<int32_t>::min() || int_val > std::numeric_limits<int32_t>::max())
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(static_cast<Int32>(int_val)));
            break;
        case DB::TypeIndex::Int64:
            column.insert(DB::Field(int_val));
            break;
        case DB::TypeIndex::UInt128:
            if (int_val < 0)
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(UInt128(static_cast<UInt64>(int_val))));
            break;
        case DB::TypeIndex::UInt256:
            if (int_val < 0)
                throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH,
                    "Python integer {} out of range for type {}", int_val, actual_type->getName());
            column.insert(DB::Field(UInt256(static_cast<UInt64>(int_val))));
            break;
        case DB::TypeIndex::Int128:
            column.insert(DB::Field(Int128(int_val)));
            break;
        case DB::TypeIndex::Int256:
            column.insert(DB::Field(Int256(int_val)));
            break;
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            column.insert(DB::Field(static_cast<Float64>(int_val)));
            break;
        default:
            throw DB::Exception(
                DB::ErrorCodes::TYPE_MISMATCH,
                "Cannot convert Python int to {}",
                actual_type->getName());
    }
}

void handleBool(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    if (!DB::isBool(actual_type))
        throw DB::Exception(
            DB::ErrorCodes::TYPE_MISMATCH,
            "Cannot convert Python bool to {}",
            actual_type->getName());

    column.insert(DB::Field(static_cast<UInt64>(value.cast<bool>() ? 1 : 0)));
}


void handleDate(
    DB::IColumn & column,
    DB::TypeIndex type_id,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    int32_t days = PyDateTimeHelper::daysSinceEpoch(value);

    switch (type_id)
    {
        case DB::TypeIndex::Date:
            column.insert(DB::Field(static_cast<UInt16>(days)));
            break;
        case DB::TypeIndex::Date32:
            column.insert(DB::Field(static_cast<Int32>(days)));
            break;
        default:
            throw DB::Exception(
                DB::ErrorCodes::TYPE_MISMATCH,
                "Cannot convert Python date to {}",
                actual_type->getName());
    }
}


void handleDatetime(
    DB::IColumn & column,
    DB::TypeIndex type_id,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    switch (type_id)
    {
        case DB::TypeIndex::DateTime:
        {
            auto ts = value.attr("timestamp")();
            column.insert(DB::Field(static_cast<UInt64>(ts.cast<double>())));
            break;
        }
        case DB::TypeIndex::DateTime64:
        {
            const auto * dt64 = typeid_cast<const DB::DataTypeDateTime64 *>(actual_type.get());
            UInt32 scale = dt64 ? dt64->getScale() : 3;
            Int64 multiplier = DB::DecimalUtils::scaleMultiplier<DB::DateTime64::NativeType>(scale);

            Int64 epoch_seconds = static_cast<Int64>(std::floor(value.attr("timestamp")().cast<double>()));
            Int64 microseconds = value.attr("microsecond").cast<Int64>();
            Int64 fractional_ticks = microseconds * multiplier / 1000000;
            Int64 ticks = epoch_seconds * multiplier + fractional_ticks;

            column.insert(DB::Field(DB::DecimalField<DB::DateTime64>(DB::DateTime64(ticks), scale)));
            break;
        }
        default:
            throw DB::Exception(
                DB::ErrorCodes::TYPE_MISMATCH,
                "Cannot convert Python datetime to {}",
                actual_type->getName());
    }
}

void handleString(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    const std::string & str)
{
    auto type_id = actual_type->getTypeId();
    if (type_id != DB::TypeIndex::String)
        throw DB::Exception(
            DB::ErrorCodes::TYPE_MISMATCH,
            "Cannot convert Python string to {}",
            actual_type->getName());

    column.insert(DB::Field(str));
}

/// bytes / bytearray / memoryview return values. ClickHouse String is binary-safe,
/// so the raw bytes are inserted verbatim (no UTF-8 decode). Only String is accepted,
/// mirroring handleString. object_type is the already-resolved kind from the caller.
void handleBytes(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    const py::handle & value,
    PythonObjectType object_type)
{
    if (actual_type->getTypeId() != DB::TypeIndex::String)
        throw DB::Exception(
            DB::ErrorCodes::TYPE_MISMATCH,
            "Cannot convert Python object of type '{}' to {}",
            String(py::str(value.get_type())),
            actual_type->getName());

    if (object_type == PythonObjectType::Bytes)
    {
        char * buffer = nullptr;
        Py_ssize_t length = 0;
        if (PyBytes_AsStringAndSize(value.ptr(), &buffer, &length) != 0)
            throw py::error_already_set();
        column.insertData(buffer, static_cast<size_t>(length));
        return;
    }

    if (object_type == PythonObjectType::ByteArray)
    {
        char * buffer = PyByteArray_AsString(value.ptr());
        if (!buffer)
            throw py::error_already_set();
        column.insertData(buffer, static_cast<size_t>(PyByteArray_Size(value.ptr())));
        return;
    }

    /// memoryview (or any other buffer exporter): materialize a contiguous copy.
    /// PyBytes_FromObject is part of the stable ABI, unlike the buffer protocol
    /// (PyObject_GetBuffer only entered the limited API in 3.11, but the abi3
    /// build targets 3.9).
    PyObject * as_bytes = PyBytes_FromObject(value.ptr());
    if (!as_bytes)
        throw py::error_already_set();
    py::object owner = py::reinterpret_steal<py::object>(as_bytes);

    char * buffer = nullptr;
    Py_ssize_t length = 0;
    if (PyBytes_AsStringAndSize(as_bytes, &buffer, &length) != 0)
        throw py::error_already_set();
    column.insertData(buffer, static_cast<size_t>(length));
}

void handleNull(DB::IColumn & column)
{
    column.insertDefault();
}

} // anonymous namespace


UDFArgSpec makeUDFArgSpec(
    const DB::IColumn & column,
    const DB::DataTypePtr & type,
    const DB::DataTypePtr & declared_type,
    bool declared_bytes)
{
    UDFArgSpec spec;
    spec.column = &column;
    spec.type = type;
    spec.declared_type = declared_type;
    spec.as_bytes = declared_bytes;

    const DB::IColumn * col = &column;
    if (const auto * col_const = typeid_cast<const DB::ColumnConst *>(col))
    {
        spec.is_const = true;
        col = &col_const->getDataColumn();
    }
    if (const auto * col_nullable = typeid_cast<const DB::ColumnNullable *>(col))
        col = &col_nullable->getNestedColumn();
    spec.value_column = col;

    auto actual_type = DB::removeNullable(type);
    spec.type_id = actual_type->getTypeId();
    spec.is_bool = DB::isBool(actual_type);
    if (declared_type)
    {
        auto declared_id = DB::removeNullable(declared_type)->getTypeId();
        spec.cast_to_float = declared_id == DB::TypeIndex::Float32 || declared_id == DB::TypeIndex::Float64;
    }

    switch (spec.type_id)
    {
        case DB::TypeIndex::UInt8:
        case DB::TypeIndex::UInt16:
        case DB::TypeIndex::UInt32:
        case DB::TypeIndex::UInt64:
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
        case DB::TypeIndex::BFloat16:
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
            spec.fast = true;
            break;
        default:
            spec.fast = false;
    }

    /// as_bytes is only honored by convertArgFast; the generic convertColumnValueForUDF
    /// path would hand back a UTF-8-decoded str. String/FixedString always take the fast
    /// path today, so this invariant holds — assert it so a future change to spec.fast
    /// eligibility can't silently regress bytes-declared arguments.
    chassert(!spec.as_bytes || spec.fast);

    return spec;
}

UDFArgSpec makeUDFArgSpec(const DB::ColumnWithTypeAndName & arg, const DB::DataTypePtr & declared_type, bool declared_bytes)
{
    return makeUDFArgSpec(*arg.column, arg.type, declared_type, declared_bytes);
}

void insertPythonObjectToColumn(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    auto object_type = GetPythonObjectType(value);

    switch (object_type)
    {
    case PythonObjectType::None:
        handleNull(column);
        break;

    case PythonObjectType::Bool:
        handleBool(column, actual_type, value);
        break;

    case PythonObjectType::Integer:
        handleInteger(column, actual_type->getTypeId(), actual_type, value);
        break;

    case PythonObjectType::Float:
    {
        double d = PyFloat_AsDouble(value.ptr());
        if (std::isnan(d))
            handleNull(column);
        else
            handleFloat(column, actual_type, d);
        break;
    }

    case PythonObjectType::String:
        handleString(column, actual_type, value.cast<std::string>());
        break;

    case PythonObjectType::Date:
        handleDate(column, actual_type->getTypeId(), actual_type, value);
        break;

    case PythonObjectType::Datetime:
        handleDatetime(column, actual_type->getTypeId(), actual_type, value);
        break;

    case PythonObjectType::Bytes:
    case PythonObjectType::ByteArray:
    case PythonObjectType::MemoryView:
        handleBytes(column, actual_type, value, object_type);
        break;

    case PythonObjectType::Decimal:
    case PythonObjectType::Uuid:
    case PythonObjectType::Time:
    case PythonObjectType::Timedelta:
    case PythonObjectType::Dict:
    case PythonObjectType::NdDatetime:
    case PythonObjectType::NdArray:
    case PythonObjectType::List:
    case PythonObjectType::Tuple:
    case PythonObjectType::Other:
    default:
        throw DB::Exception(
            DB::ErrorCodes::TYPE_MISMATCH,
            "Cannot convert Python object of type '{}' to {}",
            String(py::str(value.get_type())),
            actual_type->getName());
    }
}

} // namespace CHDB
