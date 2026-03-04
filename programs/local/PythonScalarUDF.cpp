#include "PythonScalarUDF.h"
#include "PythonConversion.h"
#include "FieldToPython.h"

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
#include <Common/DateLUTImpl.h>
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

void insertLargeInteger(
    DB::IColumn & column,
    DB::TypeIndex type_id,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    using namespace DB;

    auto py_int = py::reinterpret_borrow<py::int_>(value);
    auto mask64 = py::int_(UINT64_MAX);
    auto shift64 = py::int_(64);

    switch (type_id)
    {
        case TypeIndex::UInt128:
        {
            UInt64 lo = py::cast<uint64_t>(py_int.attr("__and__")(mask64));
            UInt64 hi = py::cast<uint64_t>(py_int.attr("__rshift__")(shift64));
            column.insert(Field(UInt128{lo, hi}));
            break;
        }
        case TypeIndex::Int128:
        {
            UInt64 lo = py::cast<uint64_t>(py_int.attr("__and__")(mask64));
            UInt64 hi = py::cast<uint64_t>(py_int.attr("__rshift__")(shift64).attr("__and__")(mask64));
            column.insert(Field(Int128{lo, hi}));
            break;
        }
        case TypeIndex::UInt256:
        {
            UInt64 limbs[4];
            py::int_ remaining = py_int;
            for (int i = 0; i < 4; ++i)
            {
                limbs[i] = py::cast<uint64_t>(remaining.attr("__and__")(mask64));
                remaining = remaining.attr("__rshift__")(shift64);
            }
            column.insert(Field(UInt256{limbs[0], limbs[1], limbs[2], limbs[3]}));
            break;
        }
        case TypeIndex::Int256:
        {
            UInt64 limbs[4];
            py::int_ remaining = py_int;
            for (int i = 0; i < 4; ++i)
            {
                limbs[i] = py::cast<uint64_t>(remaining.attr("__and__")(mask64));
                remaining = remaining.attr("__rshift__")(shift64);
            }
            column.insert(Field(Int256{limbs[0], limbs[1], limbs[2], limbs[3]}));
            break;
        }
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        {
            double d = PyLong_AsDouble(value.ptr());
            if (d == -1.0 && PyErr_Occurred())
            {
                PyErr_Clear();
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Python integer too large to convert to Float64");
            }
            column.insert(Field(static_cast<Float64>(d)));
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString:
            column.insert(Field(std::string(py::str(value))));
            break;
        default:
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Python integer out of range for type {}",
                actual_type->getName());
    }
}


void insertPythonObjectToColumn(
    DB::IColumn & column,
    const DB::DataTypePtr & type,
    const py::handle & value);


void handleInteger(
    DB::IColumn & column,
    DB::TypeIndex type_id,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    using namespace DB;

    int overflow;
    int64_t int_val = PyLong_AsLongLongAndOverflow(value.ptr(), &overflow);

    if (overflow == 0 && !PyErr_Occurred())
    {
        switch (type_id)
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
                column.insert(Field(static_cast<UInt64>(int_val)));
                break;
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Int64:
                column.insert(Field(int_val));
                break;
            case TypeIndex::UInt128:
            case TypeIndex::UInt256:
            case TypeIndex::Int128:
            case TypeIndex::Int256:
                insertLargeInteger(column, type_id, actual_type, value);
                break;
            case TypeIndex::Float32:
            case TypeIndex::Float64:
                column.insert(Field(static_cast<Float64>(int_val)));
                break;
            case TypeIndex::Date:
                column.insert(Field(static_cast<UInt64>(int_val)));
                break;
            case TypeIndex::Date32:
                column.insert(Field(static_cast<Int64>(static_cast<Int32>(int_val))));
                break;
            case TypeIndex::DateTime:
                column.insert(Field(static_cast<UInt64>(int_val)));
                break;
            case TypeIndex::DateTime64:
            {
                const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(actual_type.get());
                UInt32 scale = dt64 ? dt64->getScale() : 3;
                column.insert(Field(DecimalField<DateTime64>(DateTime64(int_val), scale)));
                break;
            }
            case TypeIndex::String:
            case TypeIndex::FixedString:
                column.insert(Field(std::to_string(int_val)));
                break;
            default:
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Cannot convert Python int to {}",
                    actual_type->getName());
        }
        return;
    }

    if (overflow != 0)
    {
        PyErr_Clear();

        if (overflow == 1)
        {
            uint64_t unsigned_val = PyLong_AsUnsignedLongLong(value.ptr());
            if (!PyErr_Occurred())
            {
                if (type_id == TypeIndex::UInt64)
                {
                    column.insert(Field(static_cast<UInt64>(unsigned_val)));
                    return;
                }
            }
            PyErr_Clear();
        }

        insertLargeInteger(column, type_id, actual_type, value);
        return;
    }

    PyErr_Clear();
    throw Exception(ErrorCodes::TYPE_MISMATCH, "Failed to convert Python integer");
}


void handleBool(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    const py::handle & value)
{
    if (!isBool(actual_type))
        throw DB::Exception(
            ErrorCodes::TYPE_MISMATCH,
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
    using namespace DB;

    int year = value.attr("year").cast<int>();
    int month = value.attr("month").cast<int>();
    int day = value.attr("day").cast<int>();
    const auto & lut = DateLUT::instance();
    auto day_num = lut.makeDayNum(year, month, day);

    switch (type_id)
    {
        case TypeIndex::Date:
            column.insert(Field(static_cast<UInt64>(day_num)));
            break;
        case TypeIndex::Date32:
            column.insert(Field(static_cast<Int64>(static_cast<Int32>(day_num))));
            break;
        case TypeIndex::DateTime:
        {
            time_t ts = lut.makeDateTime(year, month, day, 0, 0, 0);
            column.insert(Field(static_cast<UInt64>(ts)));
            break;
        }
        case TypeIndex::DateTime64:
        {
            const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(actual_type.get());
            UInt32 scale = dt64 ? dt64->getScale() : 3;
            Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale);
            time_t ts = lut.makeDateTime(year, month, day, 0, 0, 0);
            column.insert(Field(DecimalField<DateTime64>(DateTime64(ts * multiplier), scale)));
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString:
            column.insert(Field(std::string(py::str(value))));
            break;
        default:
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
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
    using namespace DB;

    switch (type_id)
    {
        case TypeIndex::DateTime:
        {
            auto ts = value.attr("timestamp")();
            column.insert(Field(static_cast<UInt64>(ts.cast<uint64_t>())));
            break;
        }
        case TypeIndex::DateTime64:
        {
            const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(actual_type.get());
            UInt32 scale = dt64 ? dt64->getScale() : 3;
            Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale);
            double ts = value.attr("timestamp")().cast<double>();
            Int64 ticks = static_cast<Int64>(ts * multiplier);
            column.insert(Field(DecimalField<DateTime64>(DateTime64(ticks), scale)));
            break;
        }
        case TypeIndex::Date:
        {
            int year = value.attr("year").cast<int>();
            int month = value.attr("month").cast<int>();
            int day = value.attr("day").cast<int>();
            const auto & lut = DateLUT::instance();
            auto day_num = lut.makeDayNum(year, month, day);
            column.insert(Field(static_cast<UInt64>(day_num)));
            break;
        }
        case TypeIndex::Date32:
        {
            int year = value.attr("year").cast<int>();
            int month = value.attr("month").cast<int>();
            int day = value.attr("day").cast<int>();
            const auto & lut = DateLUT::instance();
            auto day_num = lut.makeDayNum(year, month, day);
            column.insert(Field(static_cast<Int64>(static_cast<Int32>(day_num))));
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString:
            column.insert(Field(std::string(py::str(value))));
            break;
        default:
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Cannot convert Python datetime to {}",
                actual_type->getName());
    }
}

void handleFloat(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    double d)
{
    auto type_id = actual_type->getTypeId();
    if (type_id != DB::TypeIndex::Float32 && type_id != DB::TypeIndex::Float64)
        throw DB::Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Cannot convert Python float to {}",
            actual_type->getName());

    column.insert(DB::Field(static_cast<Float64>(d)));
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
    using namespace DB;

    DataTypePtr actual_type = removeNullable(type);
    auto type_id = actual_type->getTypeId();
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
        handleInteger(column, type_id, actual_type, value);
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

    case PythonObjectType::Decimal:
    {
        double d = py::cast<double>(value.attr("__float__")());
        switch (type_id)
        {
            case TypeIndex::Float32:
            case TypeIndex::Float64:
                column.insert(Field(static_cast<Float64>(d)));
                break;
            case TypeIndex::String:
            case TypeIndex::FixedString:
                column.insert(Field(std::string(py::str(value))));
                break;
            default:
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Cannot convert Python Decimal to {}",
                    actual_type->getName());
        }
        break;
    }

    case PythonObjectType::String:
    {
        auto str = value.cast<std::string>();
        column.insert(Field(str));
        break;
    }

    case PythonObjectType::Bytes:
    case PythonObjectType::ByteArray:
    case PythonObjectType::MemoryView:
    {
        auto str = value.cast<std::string>();
        column.insert(Field(str));
        break;
    }

    case PythonObjectType::Date:
        handleDate(column, type_id, actual_type, value);
        break;

    case PythonObjectType::Datetime:
        handleDatetime(column, type_id, actual_type, value);
        break;

    case PythonObjectType::NdDatetime:
    case PythonObjectType::NdArray:
        insertPythonObjectToColumn(column, type, value.attr("tolist")());
        break;

    case PythonObjectType::List:
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(actual_type.get());
        if (!array_type)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Cannot convert Python list to {}",
                actual_type->getName());

        const auto & element_type = array_type->getNestedType();
        auto & array_column = typeid_cast<ColumnArray &>(column);
        auto & nested_column = array_column.getData();
        auto & offsets = array_column.getOffsets();

        auto py_list = py::reinterpret_borrow<py::list>(value);
        for (const auto & item : py_list)
            insertPythonObjectToColumn(nested_column, element_type, item);

        offsets.push_back(nested_column.size());
        break;
    }

    case PythonObjectType::Tuple:
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(actual_type.get());
        if (!tuple_type)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Cannot convert Python tuple to {}",
                actual_type->getName());

        const auto & element_types = tuple_type->getElements();
        auto & tuple_column = typeid_cast<ColumnTuple &>(column);
        auto py_tuple = py::reinterpret_borrow<py::tuple>(value);

        for (size_t i = 0; i < element_types.size(); ++i)
            insertPythonObjectToColumn(tuple_column.getColumn(i), element_types[i], py_tuple[i]);
        break;
    }

    case PythonObjectType::Uuid:
    case PythonObjectType::Time:
    case PythonObjectType::Timedelta:
    case PythonObjectType::Dict:
        column.insert(Field(std::string(py::str(value))));
        break;

    case PythonObjectType::Other:
    default:
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
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
