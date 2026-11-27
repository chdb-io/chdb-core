#include "CApiScalarUDF.h"
#include "ArrowSchema.h"
#include "ArrowStreamWrapper.h"
#include "ArrowUDFConversion.h"

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>
#include <Common/Exception.h>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

#include <cstring>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int UDF_EXECUTION_FAILED;
}
}


namespace CHDB
{

namespace
{

/// Room for the message the host writes when its callback fails.
constexpr size_t CALLBACK_ERROR_SIZE = 4096;

/// `type` with the element names of every Tuple in it dropped.
DB::DataTypePtr withoutTupleNames(const DB::DataTypePtr & type)
{
    if (const auto * nullable = typeid_cast<const DB::DataTypeNullable *>(type.get()))
        return DB::makeNullable(withoutTupleNames(nullable->getNestedType()));
    if (const auto * low_cardinality = typeid_cast<const DB::DataTypeLowCardinality *>(type.get()))
        return std::make_shared<DB::DataTypeLowCardinality>(withoutTupleNames(low_cardinality->getDictionaryType()));
    if (const auto * array = typeid_cast<const DB::DataTypeArray *>(type.get()))
        return std::make_shared<DB::DataTypeArray>(withoutTupleNames(array->getNestedType()));
    if (const auto * map = typeid_cast<const DB::DataTypeMap *>(type.get()))
        return std::make_shared<DB::DataTypeMap>(withoutTupleNames(map->getKeyType()), withoutTupleNames(map->getValueType()));
    if (const auto * tuple = typeid_cast<const DB::DataTypeTuple *>(type.get()))
    {
        DB::DataTypes elements;
        elements.reserve(tuple->getElements().size());
        for (const auto & element : tuple->getElements())
            elements.push_back(withoutTupleNames(element));
        return std::make_shared<DB::DataTypeTuple>(elements);
    }
    return type;
}

/// Whether a value of type `actual` converts to `declared` without loss, e.g. an Int32 to an Int64.
/// A tuple without element names, as a literal such as (1, 'a') is, converts to a declared tuple
/// of the same shape whatever its names: the conversion goes by position.
bool convertsWithoutLoss(const DB::DataTypePtr & actual, const DB::DataTypePtr & declared)
{
    if (const auto supertype = DB::tryGetLeastSupertype(DB::DataTypes{actual, declared}); supertype && supertype->equals(*declared))
        return true;

    const auto unnamed_declared = withoutTupleNames(declared);
    if (unnamed_declared->equals(*declared) || !withoutTupleNames(actual)->equals(*actual))
        return false;
    const auto supertype = DB::tryGetLeastSupertype(DB::DataTypes{actual, unnamed_declared});
    return supertype && supertype->equals(*unnamed_declared);
}

}

CApiScalarUDF::CApiScalarUDF(
    String name_,
    DB::DataTypes argument_types_,
    DB::DataTypePtr return_type_,
    uint32_t flags,
    chdb_scalar_function_callback callback_,
    void * user_data_,
    void (*destroy_)(void *))
    : name(std::move(name_))
    , argument_types(std::move(argument_types_))
    , return_type(std::move(return_type_))
    , is_volatile(flags & CHDB_SCALAR_FUNCTION_VOLATILE)
    , special_null_handling(flags & CHDB_SCALAR_FUNCTION_SPECIAL_NULL_HANDLING)
    , binary_strings(flags & CHDB_SCALAR_FUNCTION_BINARY_STRINGS)
    , callback(callback_)
    , user_data(user_data_)
    , destroy(destroy_)
{
}

CApiScalarUDF::~CApiScalarUDF()
{
    if (destroy)
        destroy(user_data);
}

String CApiScalarUDF::label() const
{
    return "Scalar function '" + name + "'";
}

DB::DataTypePtr CApiScalarUDF::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (arguments.size() != argument_types.size())
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "{} takes {} arguments, got {}",
            label(), argument_types.size(), arguments.size());

    bool can_be_null = false;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        can_be_null = can_be_null || DB::canContainNull(*arguments[i]);

        /// With special NULL handling a NULL reaches the function, and has to fit its declared type.
        if (special_null_handling && DB::canContainNull(*arguments[i]) && !argument_types[i]->canBeInsideNullable()
            && !DB::canContainNull(*argument_types[i]))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{}: argument {} is {}, which can be NULL, but the declared {} cannot hold a NULL",
                label(), i + 1, arguments[i]->getName(), argument_types[i]->getName());

        /// A NULL literal converts to any type.
        const auto actual = DB::removeNullable(DB::removeLowCardinality(arguments[i]));
        if (DB::isNothing(actual))
            continue;

        if (!convertsWithoutLoss(actual, argument_types[i]))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{}: argument {} is {}, which does not convert to the declared {}",
                label(), i + 1, arguments[i]->getName(), argument_types[i]->getName());
    }

    /// A row with a NULL argument yields NULL - or the default value of a type that cannot be
    /// inside Nullable.
    if (!special_null_handling && can_be_null)
        return DB::makeNullableSafe(return_type);
    return return_type;
}

DB::ColumnPtr CApiScalarUDF::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::DataTypePtr & result_type,
    size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return result_type->createColumn();

    /// Without arguments a deterministic function has the same value in every row: compute it once
    /// and return it as a constant, which also lets the analyzer fold the call.
    if (arguments.empty() && !is_volatile)
        return DB::ColumnConst::create(evaluate(arguments, result_type, 1), input_rows_count);

    return evaluate(arguments, result_type, input_rows_count);
}

DB::ColumnPtr CApiScalarUDF::evaluate(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::DataTypePtr & result_type,
    size_t input_rows_count) const
{
    const size_t argc = arguments.size();

    DB::Columns full_columns;
    full_columns.reserve(argc);
    for (const auto & argument : arguments)
        full_columns.push_back(argument.column->convertToFullColumnIfConst());

    /// Without special NULL handling the rows with a NULL argument stay out of the batch. `filter`
    /// keeps the others and stays empty when there is nothing to drop.
    DB::IColumn::Filter filter;
    size_t rows = input_rows_count;
    if (!special_null_handling)
    {
        rows = ArrowUDF::filterOutNullRows(arguments, full_columns, input_rows_count, filter);
        if (rows == 0)
            return ArrowUDF::allNullColumn(result_type, input_rows_count);
    }

    auto batch_columns = ArrowUDF::prepareArguments(arguments, full_columns, filter, rows, !special_null_handling, argument_types);

    arrow::ArrayVector chunks;
    if (argc == 0)
    {
        /// The batch has no column to carry its length, so build it directly.
        chunks.push_back(call(*arrow::RecordBatch::Make(arrow::schema(arrow::FieldVector{}), static_cast<int64_t>(rows), arrow::ArrayVector{})));
    }
    else
    {
        auto table = ArrowUDF::argumentsToTable(
            label(),
            batch_columns,
            rows,
            std::vector<bool>(argc, false),
            binary_strings,
            ". Register the function with CHDB_SCALAR_FUNCTION_BINARY_STRINGS to receive String arguments as Arrow binary");

        /// A block too large for one Arrow record batch converts to several; each is a call.
        arrow::TableBatchReader reader(*table);
        std::shared_ptr<arrow::RecordBatch> batch;
        while (true)
        {
            auto status = reader.ReadNext(&batch);
            if (!status.ok())
                throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
                    "{}: cannot split the arguments into record batches: {}", label(), status.ToString());
            if (!batch)
                break;
            chunks.push_back(call(*batch));
        }
    }

    auto result = ArrowUDF::resultToColumn(label(), std::move(chunks), rows, result_type, /*nan_as_null=*/false);

    /// Put NULL back where the dropped rows were.
    return ArrowUDF::expandFilteredRows(std::move(result), filter);
}

std::shared_ptr<arrow::Array> CApiScalarUDF::call(const arrow::RecordBatch & batch) const
{
    /// What the callback leaves in these is released when they go out of scope.
    ArrowSchemaWrapper argument_schema;
    ArrowArrayWrapper arguments;
    auto status = arrow::ExportRecordBatch(batch, &arguments.arrow_array, &argument_schema.arrow_schema);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
            "{}: cannot export the arguments to Arrow: {}", label(), status.ToString());

    ArrowSchemaWrapper result_schema;
    ArrowArrayWrapper result;
    std::memset(&result_schema.arrow_schema, 0, sizeof(result_schema.arrow_schema));
    std::memset(&result.arrow_array, 0, sizeof(result.arrow_array));

    char error[CALLBACK_ERROR_SIZE] = {};
    const int code = callback(
        user_data,
        reinterpret_cast<chdb_arrow_schema>(&argument_schema.arrow_schema),
        reinterpret_cast<chdb_arrow_array>(&arguments.arrow_array),
        reinterpret_cast<chdb_arrow_schema>(&result_schema.arrow_schema),
        reinterpret_cast<chdb_arrow_array>(&result.arrow_array),
        error,
        sizeof(error));

    if (code != 0)
    {
        error[sizeof(error) - 1] = '\0';
        throw DB::Exception(
            DB::ErrorCodes::UDF_EXECUTION_FAILED,
            "{} failed on a batch of {} rows: {}",
            label(), batch.num_rows(), error[0] ? error : "the callback reported an error without a message");
    }

    if (!result.arrow_array.release || !result_schema.arrow_schema.release)
        throw DB::Exception(
            DB::ErrorCodes::UDF_EXECUTION_FAILED,
            "{} reported success without filling in its result", label());

    /// Moves the array into the result and releases the schema; on failure the wrapper releases
    /// what is left of the array.
    auto imported = arrow::ImportArray(&result.arrow_array, &result_schema.arrow_schema);
    if (!imported.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
            "{}: cannot import the result: {}", label(), imported.status().ToString());

    auto array = std::move(imported).ValueOrDie();
    if (array->length() != batch.num_rows())
        throw DB::Exception(
            DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "{} returned {} rows for a batch of {} rows",
            label(), array->length(), batch.num_rows());
    return array;
}

} // namespace CHDB
