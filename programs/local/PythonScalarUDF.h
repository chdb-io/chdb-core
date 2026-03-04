#pragma once

#include "PybindWrapper.h"

#include <Functions/IFunction.h>
#include <DataTypes/IDataType.h>


namespace CHDB
{

class PythonScalarUDF : public DB::IFunction
{
public:
    PythonScalarUDF(
        const String & name,
        py::function func,
        DB::DataTypePtr return_type);

    ~PythonScalarUDF() override;

    String getName() const override { return name; }
    bool isVariadic() const override { return is_variadic; }
    size_t getNumberOfArguments() const override { return num_args; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return false; }
    bool isDeterministic() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes &) const override { return return_type; }

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments,
        const DB::DataTypePtr & result_type,
        size_t input_rows_count) const override;

private:
    String name;
    py::function func;
    DB::DataTypePtr return_type;
    size_t num_args;
    bool is_variadic;
};

} // namespace CHDB
