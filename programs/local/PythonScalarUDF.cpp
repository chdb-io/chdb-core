#include "PythonScalarUDF.h"
#include "PythonConversion.h"
#include "FieldToPython.h"

#include "PyDateTimeHelper.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <cmath>
#include <Core/DecimalFunctions.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int TYPE_MISMATCH;
    extern const int PY_EXCEPTION_OCCURED;
}
}


namespace CHDB
{

namespace
{

struct ParameterKind
{
    enum class Type : uint8_t
    {
        POSITIONAL_ONLY,
        POSITIONAL_OR_KEYWORD,
        VAR_POSITIONAL,
        KEYWORD_ONLY,
        VAR_KEYWORD,
    };

    static Type fromString(const std::string & kind_str)
    {
        if (kind_str == "POSITIONAL_ONLY")
            return Type::POSITIONAL_ONLY;
        if (kind_str == "POSITIONAL_OR_KEYWORD")
            return Type::POSITIONAL_OR_KEYWORD;
        if (kind_str == "VAR_POSITIONAL")
            return Type::VAR_POSITIONAL;
        if (kind_str == "KEYWORD_ONLY")
            return Type::KEYWORD_ONLY;
        if (kind_str == "VAR_KEYWORD")
            return Type::VAR_KEYWORD;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown parameter kind: '{}'", kind_str);
    }
};

py::object getSignature(const py::function & udf)
{
    auto signature_func = py::module_::import("inspect").attr("signature");
    if (PY_VERSION_HEX >= 0x030a00f0)
        return signature_func(udf, py::arg("eval_str") = true);
    return signature_func(udf);
}

} // anonymous namespace


PythonScalarUDF::PythonScalarUDF(
    const String & name_,
    py::function func_,
    DB::DataTypePtr return_type_)
    : name(name_)
    , func(std::move(func_))
    , return_type(DB::makeNullable(std::move(return_type_)))
    , num_args(0)
    , is_variadic(true)
{
    try
    {
        auto signature = getSignature(func);
        auto params = py::dict(signature.attr("parameters"));

        size_t positional_count = 0;
        bool found_varargs = false;

        for (const auto & item : params)
        {
            auto param_name = py::str(item.first);
            auto kind = ParameterKind::fromString(py::str(item.second.attr("kind")));
            if (kind == ParameterKind::Type::VAR_POSITIONAL)
                found_varargs = true;
            else if (kind == ParameterKind::Type::POSITIONAL_ONLY
                     || kind == ParameterKind::Type::POSITIONAL_OR_KEYWORD)
                positional_count++;
            else
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Python UDF '{}': parameter '{}' is {}, only positional parameters are supported",
                    name, std::string(param_name), std::string(py::str(item.second.attr("kind"))));
        }

        if (found_varargs)
        {
            is_variadic = true;
            num_args = 0;
        }
        else
        {
            is_variadic = false;
            num_args = positional_count;
        }
    }
    catch (py::error_already_set &)
    {
        is_variadic = true;
        num_args = 0;
    }
}

PythonScalarUDF::~PythonScalarUDF()
{
    py::gil_scoped_acquire acquire;
    func.release().dec_ref();
}


namespace
{

void insertPythonObjectToColumn(
    DB::IColumn & column,
    const DB::DataTypePtr & type,
    const py::handle & value);

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
    auto ptr = value.ptr();
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
            column.insert(DB::Field(static_cast<UInt64>(ts.cast<uint64_t>())));
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

void handleNull(DB::IColumn & column)
{
    column.insertDefault();
}

void insertPythonObjectToColumn(
    DB::IColumn & column,
    const DB::DataTypePtr & type,
    const py::handle & value)
{
    DB::DataTypePtr actual_type = DB::removeNullable(type);
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

    case PythonObjectType::Decimal:
    case PythonObjectType::Bytes:
    case PythonObjectType::ByteArray:
    case PythonObjectType::MemoryView:
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

} // anonymous namespace


DB::ColumnPtr PythonScalarUDF::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::DataTypePtr & result_type,
    size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return result_type->createColumn();

    py::gil_scoped_acquire acquire;

    auto result_column = result_type->createColumn();
    result_column->reserve(input_rows_count);

    for (size_t row = 0; row < input_rows_count; ++row)
    {
        py::tuple py_args(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & col = arguments[i];
            py_args[i] = convertFieldToPython(*col.column, col.type, row);
        }

        py::object py_result;
        try
        {
            py_result = func(*py_args);
        }
        catch (py::error_already_set & e)
        {
            throw DB::Exception(
                DB::ErrorCodes::PY_EXCEPTION_OCCURED,
                "Python UDF '{}' raised an exception at row {}: {}",
                name, row, e.what());
        }

        insertPythonObjectToColumn(*result_column, return_type, py_result);
    }

    return result_column;
}

} // namespace CHDB
