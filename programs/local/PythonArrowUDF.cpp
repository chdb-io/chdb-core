#include "PythonArrowUDF.h"
#include "ArrowSchema.h"
#include "ArrowUDFConversion.h"
#include "ArrowStreamWrapper.h"
#include "PythonImporter.h"

#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Common/Exception.h>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int PY_EXCEPTION_OCCURED;
    extern const int TYPE_MISMATCH;
}
}


namespace CHDB
{

namespace
{

constexpr auto ARROW_FORMAT_NAME = "Arrow";

uint64_t address(const void * pointer)
{
    return reinterpret_cast<uint64_t>(pointer);
}

/// The pyarrow type a value of `type` is handed to Python as. Needs the GIL.
py::object pyarrowType(const DB::DataTypePtr & type)
{
    const auto arrow_friendly_type = ArrowUDF::argumentTypeForArrow(type);
    const DB::ColumnsWithTypeAndName header{{arrow_friendly_type->createColumn(), arrow_friendly_type, "c0"}};
    const auto schema = DB::CHColumnToArrowColumn::calculateArrowSchema(header, ARROW_FORMAT_NAME, nullptr, ArrowUDF::argumentSettings());

    ArrowSchemaWrapper c_type;
    auto status = arrow::ExportType(*schema->field(0)->type(), &c_type.arrow_schema);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
            "Cannot export the Arrow type of {}: {}", type->getName(), status.ToString());
    /// _import_from_c moves the struct into pyarrow, which takes over releasing it.
    return PythonImporter::ImportCache().pyarrow().attr("DataType").attr("_import_from_c")(address(&c_type.arrow_schema));
}

/// pyarrow infers the type of a result from its values, and cannot place some it could hold: a
/// UInt64 above the Int64 range after smaller values, an integer wider than 64 bits. Convert such a
/// result for the return type instead, where that is unambiguous. Returns a null object when that
/// does not apply or fails too. Needs the GIL.
py::object arrayForReturnType(const py::object & value, const DB::DataTypePtr & result_type)
{
    auto & pyarrow = PythonImporter::ImportCache().pyarrow;
    const auto type = DB::removeNullable(result_type);
    try
    {
        /// An integer this wide reaches pyarrow only as text, which the result is parsed from anyway.
        if (ArrowUDF::isWideInteger(type->getTypeId()))
        {
            py::list texts;
            for (auto item : value)
                texts.append(item.is_none() ? py::none() : py::object(py::str(item)));
            return pyarrow.array()(texts);
        }

        /// Not for a date or time type: given a time zone, pyarrow would take a naive datetime as UTC.
        if (DB::isNumber(type) || DB::isString(type))
            return pyarrow.array()(value, py::arg("type") = pyarrowType(result_type));
    }
    catch (py::error_already_set &) // NOLINT(bugprone-empty-catch)
    {
    }
    catch (DB::Exception &) // NOLINT(bugprone-empty-catch)
    {
    }
    return py::object();
}

} // anonymous namespace


void PythonArrowUDF::initSignature(const py::list & arg_types_hint)
{
    PythonScalarUDF::initSignature(arg_types_hint);

    if (!is_variadic && num_args == 0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python Arrow UDF '{}' must take at least one argument: it returns one value per row "
            "of the arrays it receives",
            name);
}

DB::DataTypePtr PythonArrowUDF::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (arguments.empty())
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Python Arrow UDF '{}' needs at least one argument: it returns one value per row "
            "of the arrays it receives",
            name);

    return PythonScalarUDF::getReturnTypeImpl(arguments);
}

DB::ColumnPtr PythonArrowUDF::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::DataTypePtr & result_type,
    size_t input_rows_count) const
{
    if (input_rows_count == 0)
        return result_type->createColumn();

    const size_t argc = arguments.size();

    DB::Columns full_columns;
    full_columns.reserve(argc);
    for (const auto & argument : arguments)
        full_columns.push_back(argument.column->convertToFullColumnIfConst());

    /// on_null="skip": the rows with a NULL argument stay out of the batch. `filter` keeps the
    /// others and stays empty when there is nothing to drop.
    DB::IColumn::Filter filter;
    size_t rows = input_rows_count;
    const bool skip_nulls = null_handling == NullHandling::SKIP;
    if (skip_nulls)
    {
        rows = ArrowUDF::filterOutNullRows(arguments, full_columns, input_rows_count, filter);
        /// The result type is always Nullable, so its default is NULL.
        if (rows == 0)
            return ArrowUDF::allNullColumn(result_type, input_rows_count);
    }

    /// Convert the arguments before taking the GIL, so that other threads can run Python
    /// meanwhile.
    auto batch_columns = ArrowUDF::prepareArguments(arguments, full_columns, filter, rows, skip_nulls, arg_types);
    std::vector<bool> as_bytes(argc, false);
    for (size_t i = 0; i < argc; ++i)
        as_bytes[i] = i < arg_wants_bytes.size() && arg_wants_bytes[i];

    auto batch = ArrowUDF::argumentsToTable(
        "Python Arrow UDF '" + name + "'",
        batch_columns,
        rows,
        as_bytes,
        /*strings_as_binary=*/false,
        ". Declare a String argument as bytes to receive it as pyarrow.binary()");

    std::optional<arrow::ArrayVector> result_chunks;
    {
        py::gil_scoped_acquire acquire;
        result_chunks = callFunction(batch, result_type);
    }

    if (!result_chunks)
        return ArrowUDF::allNullColumn(result_type, input_rows_count);

    /// Put NULL back where the dropped rows were.
    return ArrowUDF::expandFilteredRows(resultToColumn(std::move(*result_chunks), rows, result_type), filter);
}

std::optional<arrow::ArrayVector> PythonArrowUDF::callFunction(
    const std::shared_ptr<arrow::Table> & batch, const DB::DataTypePtr & result_type) const
{
    auto & pyarrow = PythonImporter::ImportCache().pyarrow;

    py::tuple columns;
    try
    {
        ArrowArrayStreamWrapper stream;
        auto status = arrow::ExportRecordBatchReader(
            std::make_shared<arrow::TableBatchReader>(batch), &stream.arrow_array_stream);
        if (!status.ok())
            throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
                "Python Arrow UDF '{}': cannot export the arguments to Arrow: {}", name, status.ToString());

        /// _import_from_c moves the stream into pyarrow, which takes over releasing it.
        auto reader = pyarrow.record_batch_reader().attr("_import_from_c")(address(&stream.arrow_array_stream));
        columns = py::tuple(reader.attr("read_all")().attr("columns"));
    }
    catch (py::error_already_set & e)
    {
        throw DB::Exception(DB::ErrorCodes::PY_EXCEPTION_OCCURED,
            "Python Arrow UDF '{}': cannot pass the arguments to pyarrow: {}", name, e.what());
    }

    PyObject * raw_result = PyObject_CallObject(func.ptr(), columns.ptr());
    if (!raw_result)
    {
        py::error_already_set e;
        if (exception_handling == ExceptionHandling::PROPAGATE)
            throw DB::Exception(
                DB::ErrorCodes::PY_EXCEPTION_OCCURED,
                "Python Arrow UDF '{}' raised an exception on a batch of {} rows: {}",
                name, batch->num_rows(), e.what());
        return std::nullopt;
    }
    auto result = py::reinterpret_steal<py::object>(raw_result);

    try
    {
        arrow::ArrayVector chunks;
        for (auto chunk : resultChunks(result, result_type))
        {
            ArrowArrayWrapper c_array;
            ArrowSchemaWrapper c_schema;
            chunk.attr("_export_to_c")(address(&c_array.arrow_array), address(&c_schema.arrow_schema));

            /// The imported array keeps the pyarrow buffers alive and releases them through
            /// pyarrow, which takes the GIL itself where a buffer belongs to a Python object.
            auto imported = arrow::ImportArray(&c_array.arrow_array, &c_schema.arrow_schema);
            if (!imported.ok())
                throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Python Arrow UDF '{}': cannot import the result: {}", name, imported.status().ToString());
            chunks.push_back(std::move(imported).ValueOrDie());
        }
        return chunks;
    }
    catch (py::error_already_set & e)
    {
        throw DB::Exception(DB::ErrorCodes::PY_EXCEPTION_OCCURED,
            "Python Arrow UDF '{}': cannot read the result from pyarrow: {}", name, e.what());
    }
}

py::list PythonArrowUDF::resultChunks(const py::object & result, const DB::DataTypePtr & result_type) const
{
    auto & pyarrow = PythonImporter::ImportCache().pyarrow;

    py::object value = result;
    if (isInstanceOfCached(value, pyarrow.table()) || isInstanceOfCached(value, pyarrow.record_batch()))
    {
        auto num_columns = value.attr("num_columns").cast<int64_t>();
        if (num_columns != 1)
            throw DB::Exception(
                DB::ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                "Python Arrow UDF '{}' returned a {} with {} columns, expected exactly one",
                name, String(py::str(value.get_type().attr("__name__"))), num_columns);
        value = value.attr("column")(0);
    }

    if (isInstanceOfCached(value, pyarrow.ChunkedArray()))
        return value.attr("chunks").cast<py::list>();

    if (!isInstanceOfCached(value, pyarrow.Array()))
    {
        if (value.is_none())
            throw DB::Exception(
                DB::ErrorCodes::TYPE_MISMATCH,
                "Python Arrow UDF '{}' returned None, expected an array with one value per row",
                name);

        try
        {
            value = pyarrow.array()(value);
        }
        catch (py::error_already_set & e)
        {
            auto typed = arrayForReturnType(value, result_type);
            if (!typed)
                throw DB::Exception(
                    DB::ErrorCodes::TYPE_MISMATCH,
                    "Python Arrow UDF '{}': cannot convert the returned {} to an Arrow array: {}",
                    name, String(py::str(result.get_type().attr("__name__"))), e.what());
            value = std::move(typed);
        }

        /// pyarrow.array() answers some inputs, e.g. an Arrow-backed pandas Series, with a
        /// ChunkedArray.
        if (isInstanceOfCached(value, pyarrow.ChunkedArray()))
            return value.attr("chunks").cast<py::list>();
    }

    py::list chunks;
    chunks.append(value);
    return chunks;
}

DB::ColumnPtr PythonArrowUDF::resultToColumn(arrow::ArrayVector chunks, size_t rows, const DB::DataTypePtr & result_type) const
{
    /// A float NaN is NULL, as when the per-row UDF returns one, whatever the return type.
    return ArrowUDF::resultToColumn("Python Arrow UDF '" + name + "'", std::move(chunks), rows, result_type, /*nan_as_null=*/true);
}

} // namespace CHDB
