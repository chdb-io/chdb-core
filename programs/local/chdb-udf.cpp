/// C API for scalar user-defined functions: chdb_create_scalar_function / chdb_drop_scalar_function.

#include "chdb.h"
#include "chdb-internal.h"
#include "ArrowUDFConversion.h"
#include "CApiScalarUDF.h"
#include "QueryResult.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/PythonUDAFFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/CApiScalarUDFFactory.h>
#include <Functions/UserDefined/PythonUDFFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Common/Exception.h>

#include <cstddef>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}
}

namespace
{

/// The size of the first version of chdb_scalar_function_options, which a caller must provide.
constexpr size_t OPTIONS_V1_SIZE = offsetof(chdb_scalar_function_options, destroy) + sizeof(chdb_scalar_function_options::destroy);

constexpr uint32_t KNOWN_FLAGS
    = CHDB_SCALAR_FUNCTION_VOLATILE | CHDB_SCALAR_FUNCTION_SPECIAL_NULL_HANDLING | CHDB_SCALAR_FUNCTION_BINARY_STRINGS;

chdb_result * makeErrorResult(String message)
{
    return reinterpret_cast<chdb_result *>(new CHDB::MaterializedQueryResult(std::move(message)));
}

chdb_result * makeEmptyResult()
{
    return reinterpret_cast<chdb_result *>(
        new CHDB::MaterializedQueryResult(std::make_unique<std::vector<char>>(), 0.0, 0, 0, 0, 0));
}

/// A declared type must have an Arrow form, so that the host can read the argument or build the
/// result.
void checkArrowConvertible(const String & name, const DB::DataTypePtr & type, const String & what)
{
    const auto arrow_type = CHDB::ArrowUDF::argumentTypeForArrow(type);
    const DB::ColumnsWithTypeAndName header{{arrow_type->createColumn(), arrow_type, "c0"}};
    try
    {
        DB::CHColumnToArrowColumn::calculateArrowSchema(header, "Arrow", nullptr, CHDB::ArrowUDF::argumentSettings());
    }
    catch (DB::Exception & e)
    {
        e.addMessage("Scalar function '{}': {} type {} has no Arrow form", name, what, type->getName());
        throw;
    }
}

DB::DataTypePtr parseType(const String & name, const char * type_name, const String & what)
{
    if (!type_name || !*type_name)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Scalar function '{}': {} type is missing", name, what);

    auto type = DB::DataTypeFactory::instance().get(type_name);
    checkArrowConvertible(name, type, what);
    return type;
}

/// Calls are resolved in a fixed order, so a function registered under a name something else
/// resolves first would never run, and one that resolves before it would silently take its calls.
void checkNameIsFree(const String & name, const DB::ContextPtr & context)
{
    auto taken = [&](std::string_view by)
    {
        throw DB::Exception(
            DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "Scalar function '{}' cannot be registered: {} with that name already exists",
            name, by);
    };

    const auto & functions = DB::FunctionFactory::instance();
    if (functions.hasNameOrAlias(name) || functions.isCaseInsensitive(name))
        taken("a built-in function");

    const auto & aggregate_functions = DB::AggregateFunctionFactory::instance();
    if (aggregate_functions.isAggregateFunctionName(name) || aggregate_functions.isCaseInsensitive(name)
        || CHDB::isPythonUDAFName(name))
        taken("an aggregate function");

    if (DB::UserDefinedSQLFunctionFactory::instance().has(name))
        taken("a SQL user-defined function");

    if (DB::UserDefinedExecutableFunctionFactory::has(name, context)) /// NOLINT(readability-static-accessed-through-instance)
        taken("an executable user-defined function");

    if (CHDB::PythonUDFFactory::instance().tryGetFunction(name))
        taken("a Python UDF");
}

template <typename F>
chdb_result * runGuarded(F && f)
{
    try
    {
        return f();
    }
    catch (const DB::Exception & e)
    {
        return makeErrorResult(DB::getExceptionMessage(e, false));
    }
    catch (const std::exception & e)
    {
        return makeErrorResult(e.what());
    }
    catch (...)
    {
        return makeErrorResult(DB::getCurrentExceptionMessage(false));
    }
}

}

chdb_result * chdb_create_scalar_function(
    chdb_connection conn, const char * name, const chdb_scalar_function_options * options)
{
    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return makeErrorResult("Invalid or closed connection");
    if (!name || !*name)
        return makeErrorResult("Scalar function name must not be empty");
    if (!options)
        return makeErrorResult("Unexpected null scalar function options");
    if (options->struct_size < OPTIONS_V1_SIZE)
        return makeErrorResult("chdb_scalar_function_options.struct_size is smaller than the struct");
    if (!options->callback)
        return makeErrorResult("Scalar function callback must not be null");
    if (options->argument_count != 0 && !options->argument_types)
        return makeErrorResult("Unexpected null scalar function argument types");
    if (options->flags & ~KNOWN_FLAGS)
        return makeErrorResult("Unknown chdb_scalar_function_flag bits");

    return runGuarded([&]
    {
        const String function_name(name);

        DB::DataTypes argument_types;
        argument_types.reserve(options->argument_count);
        for (size_t i = 0; i < options->argument_count; ++i)
        {
            /// NULL handling is the function's, not the argument type's, and LowCardinality is a
            /// storage detail the function never sees.
            auto type = parseType(function_name, options->argument_types[i], "argument " + std::to_string(i + 1));
            argument_types.push_back(DB::removeNullable(DB::removeLowCardinality(type)));
        }
        auto return_type = parseType(function_name, options->return_type, "return");

        checkNameIsFree(function_name, DB::Context::getGlobalContextInstance());

        CHDB::CApiScalarUDFFactory::instance().add(function_name, [&]
        {
            return std::make_shared<CHDB::CApiScalarUDF>(
                function_name,
                std::move(argument_types),
                std::move(return_type),
                options->flags,
                options->callback,
                options->user_data,
                options->destroy);
        });
        return makeEmptyResult();
    });
}

chdb_result * chdb_drop_scalar_function(chdb_connection conn, const char * name)
{
    auto * connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return makeErrorResult("Invalid or closed connection");
    if (!name || !*name)
        return makeErrorResult("Scalar function name must not be empty");

    return runGuarded([&]
    {
        if (!CHDB::CApiScalarUDFFactory::instance().remove(name))
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_FUNCTION, "Scalar function '{}' is not registered", name);
        return makeEmptyResult();
    });
}
