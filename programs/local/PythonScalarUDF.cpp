#include "PythonScalarUDF.h"
#include "ChdbPyType.h"
#include "PythonConversion.h"
#include "FieldToPython.h"

#include "PyDateTimeHelper.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <IO/readIntText.h>

#if USE_JEMALLOC
#include <Common/memory.h>
#endif


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

enum class PythonTypeObject : uint8_t {
	INVALID,
	BASE,
	STRING,
	TYPE,
};

PythonTypeObject getPythonObjectType(const py::handle &type_object) {
	if (py::isinstance<py::type>(type_object))
		return PythonTypeObject::BASE;

	if (py::isinstance<py::str>(type_object))
		return PythonTypeObject::STRING;

	if (py::isinstance<ChdbPyType>(type_object))
		return PythonTypeObject::TYPE;

	return PythonTypeObject::INVALID;
}

DB::DataTypePtr fromNumpyType(const py::object & type)
{
    auto obj = type();
    if (!py::hasattr(obj, "dtype"))
        return nullptr;

    auto type_str = std::string(py::str(obj.attr("dtype")));
    if (type_str == "bool")    return DB::DataTypeFactory::instance().get("Bool");
    if (type_str == "int8")    return std::make_shared<DB::DataTypeInt8>();
    if (type_str == "uint8")   return std::make_shared<DB::DataTypeUInt8>();
    if (type_str == "int16")   return std::make_shared<DB::DataTypeInt16>();
    if (type_str == "uint16")  return std::make_shared<DB::DataTypeUInt16>();
    if (type_str == "int32")   return std::make_shared<DB::DataTypeInt32>();
    if (type_str == "uint32")  return std::make_shared<DB::DataTypeUInt32>();
    if (type_str == "int64")   return std::make_shared<DB::DataTypeInt64>();
    if (type_str == "uint64")  return std::make_shared<DB::DataTypeUInt64>();
    if (type_str == "float16") return std::make_shared<DB::DataTypeFloat32>();
    if (type_str == "float32") return std::make_shared<DB::DataTypeFloat32>();
    if (type_str == "float64") return std::make_shared<DB::DataTypeFloat64>();

    return nullptr;
}

DB::DataTypePtr fromPythonType(const py::object & annotation)
{
    auto builtins = py::module_::import("builtins");

    if (annotation.is(builtins.attr("bool")))
        return DB::DataTypeFactory::instance().get("Bool");

    if (annotation.is(builtins.attr("int")))
        return std::make_shared<DB::DataTypeInt64>();

    if (annotation.is(builtins.attr("float")))
        return std::make_shared<DB::DataTypeFloat64>();

    if (annotation.is(builtins.attr("str")))
        return std::make_shared<DB::DataTypeString>();

    if (annotation.is(builtins.attr("bytes")))
        return std::make_shared<DB::DataTypeString>();

    if (annotation.is(builtins.attr("bytearray")))
        return std::make_shared<DB::DataTypeString>();

    auto datetime_mod = py::module_::import("datetime");
    if (annotation.is(datetime_mod.attr("date")))
        return std::make_shared<DB::DataTypeDate>();
    if (annotation.is(datetime_mod.attr("datetime")))
        return std::make_shared<DB::DataTypeDateTime64>(6);

    auto numpy_result = fromNumpyType(annotation);
    if (numpy_result)
        return numpy_result;

    throw DB::Exception(
        DB::ErrorCodes::BAD_ARGUMENTS,
        "Cannot convert Python type '{}' to a ClickHouse type",
        std::string(py::str(annotation)));
}

DB::DataTypePtr fromString(const py::object & annotation)
{
    auto string_value = std::string(py::str(annotation));
    return ChdbPyType(string_value).dataType();
}

DB::DataTypePtr fromChdbPyType(const py::object & annotation)
{
    std::shared_ptr<ChdbPyType> type_object;
    if (!py::try_cast<std::shared_ptr<ChdbPyType>>(annotation, type_object)) {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Expected ChdbPyType type, got '{}'",
            std::string(py::str(annotation.get_type())));
    }
	return type_object->dataType();
}

} // anonymous namespace

DB::DataTypePtr annotationToDataType(const py::object & annotation)
{
    auto type_object = getPythonObjectType(annotation);
    switch (type_object)
    {
        case PythonTypeObject::BASE:
            return fromPythonType(annotation);
        case PythonTypeObject::STRING:
            return fromString(annotation);
        case PythonTypeObject::TYPE:
            return fromChdbPyType(annotation);
        case PythonTypeObject::INVALID:
        default:
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown Python UDF type annotation: {}",
                String(py::str(annotation.get_type())));
    }
}

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
    const int32_t PYTHON_3_10_HEX = 0x030a00f0;
	const auto python_version = PY_VERSION_HEX;

    auto signature_func = py::module_::import("inspect").attr("signature");
    if (python_version >= PYTHON_3_10_HEX)
        return signature_func(udf, py::arg("eval_str") = true);
    return signature_func(udf);
}

DB::DataTypePtr inferReturnType(const py::object & signature, const py::object & empty)
{
    auto return_annotation = signature.attr("return_annotation");

    if (!py::none().is(return_annotation) && !empty.is(return_annotation))
    {
        auto data_type = annotationToDataType(return_annotation);

        chassert(data_type);
        return DB::makeNullable(data_type);
    }

    return nullptr;
}

void resolveArgTypes(
    const String & name,
    const py::list & arg_types_hint,
    const size_t arg_types_hint_count,
    const size_t num_args,
    DB::DataTypes & arg_types)
{
    chassert(arg_types_hint_count > 0);
    chassert(arg_types.empty());

    if (arg_types_hint_count != num_args)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDF '{}': arg_types has {} elements but function has {} parameters",
            name, py::len(arg_types_hint), num_args);

    for (auto item : arg_types_hint)
        arg_types.push_back(annotationToDataType(py::reinterpret_borrow<py::object>(item)));
}

bool isSupportedUDFType(DB::TypeIndex type_id)
{
    switch (type_id)
    {
        case DB::TypeIndex::UInt8:
        case DB::TypeIndex::UInt16:
        case DB::TypeIndex::UInt32:
        case DB::TypeIndex::UInt64:
        case DB::TypeIndex::UInt128:
        case DB::TypeIndex::UInt256:
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
        case DB::TypeIndex::Int128:
        case DB::TypeIndex::Int256:
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
        case DB::TypeIndex::String:
        case DB::TypeIndex::Date:
        case DB::TypeIndex::Date32:
        case DB::TypeIndex::DateTime:
        case DB::TypeIndex::DateTime64:
            return true;
        default:
            return false;
    }
}

void validateUDFTypes(
    const String & name,
    const DB::DataTypePtr & return_type,
    const DB::DataTypes & arg_types)
{
    auto raw_return = DB::removeNullable(return_type);
    if (!isSupportedUDFType(raw_return->getTypeId()))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDF '{}': unsupported return type '{}'",
            name, raw_return->getName());

    for (size_t i = 0; i < arg_types.size(); ++i)
    {
        if (!arg_types[i])
            continue;
        if (!isSupportedUDFType(arg_types[i]->getTypeId()))
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDF '{}': unsupported argument {} type '{}'",
                name, i + 1, arg_types[i]->getName());
    }
}

} // anonymous namespace


PythonScalarUDF::PythonScalarUDF(
    const String & name_,
    py::function func_,
    DB::DataTypePtr return_type_,
    NullHandling null_handling_,
    ExceptionHandling exception_handling_)
    : name(name_)
    , func(std::move(func_))
    , return_type(return_type_ ? DB::makeNullable(std::move(return_type_)) : nullptr)
    , num_args(0)
    , is_variadic(true)
    , null_handling(null_handling_)
    , exception_handling(exception_handling_)
{}

void PythonScalarUDF::initSignature(const py::list & arg_types_hint)
{
    try
    {
        auto signature = getSignature(func);
        auto params = py::dict(signature.attr("parameters"));
        auto empty = py::module_::import("inspect").attr("Parameter").attr("empty");

        size_t positional_count = 0;
        bool found_varargs = false;
        size_t arg_count = static_cast<size_t>(py::len(params));
        arg_types.reserve(arg_count);
        const size_t arg_types_hint_count = py::len(arg_types_hint);
        bool no_arg_types_hint = arg_types_hint_count == 0;

        for (const auto & item : params)
        {
            auto param_name = py::str(item.first);
            if (found_varargs)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                    "Python UDF '{}': parameter '{}' after *args is not supported", name, String(param_name));

            const auto & value = item.second;
            auto kind = ParameterKind::fromString(String(py::str(value.attr("kind"))));

            if (kind == ParameterKind::Type::VAR_POSITIONAL)
            {
                found_varargs = true;
                continue;
            }

            if (kind != ParameterKind::Type::POSITIONAL_ONLY
                && kind != ParameterKind::Type::POSITIONAL_OR_KEYWORD)
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Python UDF '{}': parameter '{}' is {}, only positional parameters are supported",
                    name, String(param_name), String(py::str(value.attr("kind"))));

            positional_count++;

            if (no_arg_types_hint)
            {
                auto arg_annotation = value.attr("annotation");
                if (py::none().is(arg_annotation) || empty.is(arg_annotation))
                    arg_types.emplace_back();
                else
                    arg_types.push_back(annotationToDataType(arg_annotation));
            }
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

        if (!return_type)
            return_type = inferReturnType(signature, empty);

        if (!return_type)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDF '{}': return type not specified", name);

        if (!no_arg_types_hint)
            resolveArgTypes(name, arg_types_hint, arg_types_hint_count, positional_count, arg_types);

        validateUDFTypes(name, return_type, arg_types);
    }
    catch (py::error_already_set & e)
    {
#if USE_JEMALLOC
        ::Memory::MemoryCheckScope memory_check_scope;
#endif
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDF '{}': failed to inspect function signature: {}",
            name, e.what());
    }
}

DB::DataTypePtr PythonScalarUDF::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (is_variadic)
    {
        if (arguments.size() < arg_types.size())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDF '{}': expected at least {} arguments, got {}",
                name, arg_types.size(), arguments.size());
    }
    else if (arguments.size() != arg_types.size())
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDF '{}': expected {} arguments, got {}",
            name, arg_types.size(), arguments.size());
    }

    const size_t check_count = std::min(arguments.size(), arg_types.size());
    for (size_t i = 0; i < check_count; ++i)
    {
        auto arg_type = arg_types[i];
        if (!arg_type)
            continue;

        auto actual_type = DB::removeNullable(arguments[i]);
        auto supertype = DB::tryGetLeastSupertype(DB::DataTypes{actual_type, arg_type});
        if (!supertype || !supertype->equals(*arg_type))
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDF '{}': argument {} type mismatch: expected {}, got {}",
                name, i + 1, arg_type->getName(), actual_type->getName());
    }

    return return_type;
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
        if (null_handling == NullHandling::SKIP)
        {
            bool has_null_arg = false;
            for (const auto & arg : arguments)
            {
                if (arg.column->isNullAt(row))
                {
                    has_null_arg = true;
                    break;
                }
            }

            if (has_null_arg)
            {
                /// The return type is always Nullable, so the default value is NULL.
                result_column->insertDefault();
                continue;
            }
        }

        py::tuple py_args(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & col = arguments[i];
            DB::DataTypePtr expected = (i < arg_types.size() && arg_types[i]) ? arg_types[i] : nullptr;
            py_args[i] = convertColumnValueForUDF(*col.column, col.type, row, expected);
        }

        py::object py_result;
        try
        {
            py_result = func(*py_args);
        }
        catch (py::error_already_set & e)
        {
            if (exception_handling == ExceptionHandling::PROPAGATE)
            {
#if USE_JEMALLOC
                ::Memory::MemoryCheckScope memory_check_scope;
#endif
                throw DB::Exception(
                    DB::ErrorCodes::PY_EXCEPTION_OCCURED,
                    "Python UDF '{}' raised an exception at row {}: {}",
                    name, row, e.what());
            }
            result_column->insertDefault();
            continue;
        }

        insertPythonObjectToColumn(*result_column, return_type, py_result);
    }

    return result_column;
}

} // namespace CHDB
