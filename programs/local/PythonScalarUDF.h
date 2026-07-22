#pragma once

#include "PybindWrapper.h"

#include <Functions/IFunction.h>
#include <DataTypes/IDataType.h>


namespace CHDB
{

enum class NullHandling : uint8_t
{
    SKIP,
    PASS,
};

enum class ExceptionHandling : uint8_t
{
    PROPAGATE,
    IGNORE,
};

class PythonScalarUDF : public DB::IFunction
{
public:
    PythonScalarUDF(
        const String & name,
        py::function func,
        DB::DataTypePtr return_type,
        NullHandling null_handling,
        ExceptionHandling exception_handling);

    ~PythonScalarUDF() override;

    void initSignature(const py::list & arg_types_hint);

    String getName() const override { return name; }
    bool isVariadic() const override { return is_variadic; }
    size_t getNumberOfArguments() const override { return num_args; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return false; }
    bool isDeterministic() const override { return false; }
    /// NULL handling is done in executeImpl for both modes. The default implementation
    /// for NULLs would still call the function on NULL rows with placeholder values
    /// (discarding the results), which is observable for Python UDFs through side
    /// effects and exceptions (with on_error=propagate).
    bool useDefaultImplementationForNulls() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments,
        const DB::DataTypePtr & result_type,
        size_t input_rows_count) const override;

private:
    String name;
    py::function func;
    DB::DataTypePtr return_type;
    DB::DataTypes arg_types;
    size_t num_args;
    bool is_variadic;
    NullHandling null_handling;
    ExceptionHandling exception_handling;
};

DB::DataTypePtr annotationToDataType(const py::object & annotation);

} // namespace CHDB
