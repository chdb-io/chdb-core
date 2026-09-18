#include "PythonScalarUDF.h"
#include "PythonUDFCommon.h"
#include "PythonUDFConversion.h"
#include "FieldToPython.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <base/defines.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PY_EXCEPTION_OCCURED;
}
}


namespace CHDB
{

namespace
{

void resolveArgTypes(
    const String & name,
    const py::list & arg_types_hint,
    const size_t arg_types_hint_count,
    const size_t num_args,
    DB::DataTypes & arg_types,
    std::vector<bool> & arg_wants_bytes)
{
    chassert(arg_types_hint_count > 0);
    chassert(arg_types.empty());
    chassert(arg_wants_bytes.empty());

    if (arg_types_hint_count != num_args)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDF '{}': arg_types has {} elements but function has {} parameters",
            name, py::len(arg_types_hint), num_args);

    for (auto item : arg_types_hint)
    {
        auto annotation = py::reinterpret_borrow<py::object>(item);
        arg_types.push_back(annotationToDataType(annotation));
        arg_wants_bytes.push_back(isBytesLikeAnnotation(annotation));
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
    const String kind = "Python UDF";

    try
    {
        auto signature = inspectUDFSignature(kind, name, func);

        const size_t positional_count = signature.positional.size();
        const size_t arg_types_hint_count = py::len(arg_types_hint);
        const bool no_arg_types_hint = arg_types_hint_count == 0;

        is_variadic = signature.has_varargs;
        num_args = signature.has_varargs ? 0 : positional_count;

        if (no_arg_types_hint)
        {
            arg_types.reserve(positional_count);
            arg_wants_bytes.reserve(positional_count);

            for (const auto & parameter : signature.positional)
            {
                if (!parameter.annotation)
                {
                    arg_types.emplace_back();
                    arg_wants_bytes.push_back(false);
                }
                else
                {
                    arg_types.push_back(annotationToDataType(parameter.annotation));
                    arg_wants_bytes.push_back(isBytesLikeAnnotation(parameter.annotation));
                }
            }
        }

        if (!return_type && signature.return_annotation)
            return_type = DB::makeNullable(annotationToDataType(signature.return_annotation));

        if (!return_type)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDF '{}': return type not specified", name);

        if (!no_arg_types_hint)
            resolveArgTypes(name, arg_types_hint, arg_types_hint_count, positional_count, arg_types, arg_wants_bytes);

        validateUDFTypes(kind, name, return_type, arg_types);
    }
    catch (py::error_already_set & e)
    {
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

    const size_t argc = arguments.size();

    /// Hoist every loop invariant out of the per-row path: type unwrapping,
    /// dispatch flags and the Nullable peel of the return type.
    std::vector<UDFArgSpec> specs;
    specs.reserve(argc);
    for (size_t i = 0; i < argc; ++i)
        specs.push_back(makeUDFArgSpec(
            arguments[i],
            (i < arg_types.size() && arg_types[i]) ? arg_types[i] : nullptr,
            i < arg_wants_bytes.size() && arg_wants_bytes[i]));

    const DB::DataTypePtr result_actual_type = DB::removeNullable(return_type);

#ifdef CHDB_FREE_THREADING
    /// One reused vectorcall frame instead of a fresh py::tuple per row: the
    /// tuple is a GC-tracked allocation, and at millions of rows per second
    /// across many threads it dominates the free-threaded GC trigger rate
    /// (every young collection is a stop-the-world). Slot 0 stays reserved for
    /// PY_VECTORCALL_ARGUMENTS_OFFSET; row_args owns the references.
    /// (PyObject_Vectorcall is not part of the 3.9 stable ABI, so the abi3
    /// build below keeps a per-row tuple and calls through the limited API.)
    std::vector<PyObject *> argv(argc + 1, nullptr);
#endif
    std::vector<py::object> row_args(argc);

    for (size_t row = 0; row < input_rows_count; ++row)
    {
        if (null_handling == NullHandling::SKIP)
        {
            bool has_null_arg = false;
            for (const auto & spec : specs)
            {
                if (spec.column->isNullAt(row))
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

        for (size_t i = 0; i < argc; ++i)
        {
            const auto & spec = specs[i];
            row_args[i] = spec.fast
                ? convertArgFast(spec, row)
                : convertColumnValueForUDF(*spec.column, spec.type, row, spec.declared_type);
        }

#ifdef CHDB_FREE_THREADING
        for (size_t i = 0; i < argc; ++i)
            argv[i + 1] = row_args[i].ptr();

        PyObject * raw_result = PyObject_Vectorcall(
            func.ptr(), argv.data() + 1, argc | PY_VECTORCALL_ARGUMENTS_OFFSET, nullptr);
#else
        py::tuple py_args(argc);
        for (size_t i = 0; i < argc; ++i)
            py_args[i] = row_args[i];

        PyObject * raw_result = PyObject_CallObject(func.ptr(), py_args.ptr());
#endif

        if (!raw_result)
        {
            py::error_already_set e;
            if (exception_handling == ExceptionHandling::PROPAGATE)
            {
                throw DB::Exception(
                    DB::ErrorCodes::PY_EXCEPTION_OCCURED,
                    "Python UDF '{}' raised an exception at row {}: {}",
                    name, row, e.what());
            }
            result_column->insertDefault();
            continue;
        }

        py::object py_result = py::reinterpret_steal<py::object>(raw_result);
        insertPythonObjectToColumn(*result_column, result_actual_type, py_result);
    }

    return result_column;
}

} // namespace CHDB
