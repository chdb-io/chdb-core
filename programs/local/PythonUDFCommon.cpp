#include "PythonUDFCommon.h"
#include "ChdbPyType.h"

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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


py::object unwrapOptionalAnnotation(const py::object & annotation)
{
    auto typing_module = py::module_::import("typing");
    auto origin = typing_module.attr("get_origin")(annotation);

    if (origin.is_none())
        return annotation;

    /// typing.Optional[X] and typing.Union[...] both report typing.Union as origin;
    /// PEP 604 unions (X | None) report types.UnionType (Python 3.10+).
    bool is_union = origin.is(typing_module.attr("Union"));
    if (!is_union)
    {
        auto types_module = py::module_::import("types");
        if (py::hasattr(types_module, "UnionType"))
            is_union = origin.is(types_module.attr("UnionType"));
    }

    if (!is_union)
        return annotation;

    /// Keep the single non-None member as the base type. The engine already makes
    /// every UDF argument and return type Nullable (see inferReturnType and the
    /// return_type_ constructor initializer), so the None member carries no extra
    /// meaning beyond selecting X. Multi-member unions like Union[int, str] are
    /// ambiguous and rejected.
    auto none_type = py::none().get_type();
    py::object base;
    size_t non_none_count = 0;
    for (auto member : typing_module.attr("get_args")(annotation))
    {
        auto member_type = py::reinterpret_borrow<py::object>(member);
        if (member_type.is(none_type))
            continue;
        base = member_type;
        ++non_none_count;
    }

    if (non_none_count == 1)
        return base;

    throw DB::Exception(
        DB::ErrorCodes::BAD_ARGUMENTS,
        "Unsupported Python UDF union type annotation '{}': only Optional[X] "
        "(equivalently Union[X, None] or X | None) is supported",
        String(py::str(annotation)));
}

DB::DataTypePtr annotationToDataType(const py::object & annotation)
{
    auto resolved = unwrapOptionalAnnotation(annotation);
    auto type_object = getPythonObjectType(resolved);
    switch (type_object)
    {
        case PythonTypeObject::BASE:
            return fromPythonType(resolved);
        case PythonTypeObject::STRING:
            return fromString(resolved);
        case PythonTypeObject::TYPE:
            return fromChdbPyType(resolved);
        case PythonTypeObject::INVALID:
        default:
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown Python UDF type annotation: {}",
                String(py::str(resolved.get_type())));
    }
}

/// True when the annotation is the builtin `bytes` or `bytearray` type. Both map
/// to ClickHouse String, but an argument declared this way should receive a raw
/// Python `bytes` value rather than a UTF-8-decoded `str`.
bool isBytesLikeAnnotation(const py::object & annotation)
{
    auto resolved = unwrapOptionalAnnotation(annotation);
    if (!py::isinstance<py::type>(resolved))
        return false;

    auto builtins = py::module_::import("builtins");
    return resolved.is(builtins.attr("bytes")) || resolved.is(builtins.attr("bytearray"));
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

py::object getSignature(const py::object & udf)
{
    const int32_t PYTHON_3_10_HEX = 0x030a00f0;
	const auto python_version = PY_VERSION_HEX;

    auto signature_func = py::module_::import("inspect").attr("signature");
    if (python_version >= PYTHON_3_10_HEX)
        return signature_func(udf, py::arg("eval_str") = true);
    return signature_func(udf);
}

} // anonymous namespace


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
    const String & kind,
    const String & name,
    const DB::DataTypePtr & return_type,
    const DB::DataTypes & arg_types)
{
    auto raw_return = DB::removeNullable(return_type);
    if (!isSupportedUDFType(raw_return->getTypeId()))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "{} '{}': unsupported return type '{}'",
            kind, name, raw_return->getName());

    for (size_t i = 0; i < arg_types.size(); ++i)
    {
        if (!arg_types[i])
            continue;
        if (!isSupportedUDFType(arg_types[i]->getTypeId()))
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "{} '{}': unsupported argument {} type '{}'",
                kind, name, i + 1, arg_types[i]->getName());
    }
}

UDFSignature inspectUDFSignature(
    const String & kind,
    const String & name,
    const py::object & callable,
    size_t skip_leading)
{
    UDFSignature result;

    try
    {
        auto signature = getSignature(callable);
        auto params = py::dict(signature.attr("parameters"));
        py::object empty = py::module_::import("inspect").attr("Parameter").attr("empty");

        auto resolve_annotation = [&](py::object annotation) -> py::object
        {
            if (py::none().is(annotation) || empty.is(annotation))
                return py::object();
            return annotation;
        };

        size_t index = 0;
        result.positional.reserve(static_cast<size_t>(py::len(params)));

        for (const auto & item : params)
        {
            auto param_name = String(py::str(item.first));
            if (index++ < skip_leading)
                continue;

            if (result.has_varargs)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                    "{} '{}': parameter '{}' after *args is not supported", kind, name, param_name);

            const auto & value = item.second;
            auto param_kind = ParameterKind::fromString(String(py::str(value.attr("kind"))));

            if (param_kind == ParameterKind::Type::VAR_POSITIONAL)
            {
                result.has_varargs = true;
                continue;
            }

            if (param_kind != ParameterKind::Type::POSITIONAL_ONLY
                && param_kind != ParameterKind::Type::POSITIONAL_OR_KEYWORD)
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "{} '{}': parameter '{}' is {}, only positional parameters are supported",
                    kind, name, param_name, String(py::str(value.attr("kind"))));

            result.positional.push_back(UDFParameter{
                std::move(param_name),
                resolve_annotation(py::reinterpret_borrow<py::object>(value.attr("annotation")))});
        }

        result.return_annotation
            = resolve_annotation(py::reinterpret_borrow<py::object>(signature.attr("return_annotation")));
    }
    catch (py::error_already_set & e)
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "{} '{}': failed to inspect function signature: {}",
            kind, name, e.what());
    }

    return result;
}

} // namespace CHDB
