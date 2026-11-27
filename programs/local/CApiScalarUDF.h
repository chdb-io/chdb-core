#pragma once

#include "chdb.h"

#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>

#include <arrow/type_fwd.h>


namespace CHDB
{

/// A scalar function a host registered through chdb_create_scalar_function() (chdb.h).
///
/// The callback is called once per block, or once per record batch when a block converts to
/// several, with the arguments of the rows it has to evaluate as an Arrow struct array, and returns
/// one value per row as an Arrow array of any type that converts to the declared return type.
/// See chdb.h for the contract with the host.
class CApiScalarUDF final : public DB::IFunction
{
public:
    /// `argument_types` and `return_type` are the declared ones, the argument types without
    /// Nullable and LowCardinality. The function owns `user_data` and destroys it with `destroy`.
    CApiScalarUDF(
        String name_,
        DB::DataTypes argument_types_,
        DB::DataTypePtr return_type_,
        uint32_t flags,
        chdb_scalar_function_callback callback_,
        void * user_data_,
        void (*destroy_)(void *));

    ~CApiScalarUDF() override;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return argument_types.size(); }

    /// The host can drop the function and register another one under its name, so a call need not
    /// give the same result in another query even when the function is not volatile - as for now(),
    /// which keeps the query result and query condition caches from reusing it. Within a query it
    /// does, unless the function is volatile.
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return !is_volatile; }
    bool isSuitableForConstantFolding() const override { return !is_volatile; }
    /// A call goes through the host: let the optimizer evaluate it after cheaper conditions, e.g. in
    /// PREWHERE, which also keeps it off the rows they exclude.
    bool isExpensive() const override { return true; }
    /// Constant arguments are evaluated once rather than once per row, unless the function is
    /// volatile.
    bool useDefaultImplementationForConstants() const override { return !is_volatile; }
    /// A call can fail on a row a branch of if() does not take, and can be expensive: evaluate it
    /// only for the rows the branch takes.
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return true; }
    /// NULL handling is done in executeImpl. The default implementation would call the function on
    /// the rows with a NULL argument too, with placeholder values (discarding the results), which
    /// the host can observe, and on which its function can fail.
    bool useDefaultImplementationForNulls() const override { return false; }
    /// A LowCardinality dictionary holds values no row has - its default value at least, and values
    /// of rows a filter already dropped - but the function must only ever see the values of real rows.
    bool canBeExecutedOnLowCardinalityDictionary() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments,
        const DB::DataTypePtr & result_type,
        size_t input_rows_count) const override;

private:
    /// Evaluates every one of `input_rows_count` rows through the host.
    DB::ColumnPtr evaluate(
        const DB::ColumnsWithTypeAndName & arguments,
        const DB::DataTypePtr & result_type,
        size_t input_rows_count) const;

    /// Calls the host on one batch and returns the array it produced, imported into chDB's Arrow.
    std::shared_ptr<arrow::Array> call(const arrow::RecordBatch & batch) const;

    String label() const;

    const String name;
    const DB::DataTypes argument_types;
    const DB::DataTypePtr return_type;
    const bool is_volatile;
    const bool special_null_handling;
    const bool binary_strings;
    const chdb_scalar_function_callback callback;
    void * const user_data;
    void (* const destroy)(void *);
};

} // namespace CHDB
