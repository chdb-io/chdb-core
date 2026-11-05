#include "PythonArrowUDF.h"
#include "ArrowSchema.h"
#include "ArrowStreamWrapper.h"
#include "PythonImporter.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <base/defines.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/utf8.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 output_format_arrow_max_record_batch_size_bytes;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int PY_EXCEPTION_OCCURED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}
}


namespace CHDB
{

namespace
{

constexpr auto ARROW_FORMAT_NAME = "Arrow";

const DB::CHColumnToArrowColumn::Settings & argumentArrowSettings()
{
    static const auto settings = []
    {
        DB::CHColumnToArrowColumn::Settings result;
        /// pa.string(), whose values are str, as the per-row UDF hands out str.
        result.output_string_as_string = true;
        result.low_cardinality_as_dictionary = false;
        /// pa.timestamp("s", tz) instead of the raw UInt32 seconds the Arrow format writes.
        result.output_datetime_as_timestamp = true;
        /// A NULL literal under on_null="pass" (Nullable(Nothing)) becomes a pa.null() array.
        result.output_nothing_as_null = true;
        return result;
    }();
    return settings;
}

/// The type an argument is cast to before it is converted, where the Arrow format's own mapping
/// gives no usable value - at any depth, inside an Array, a Map or a Tuple as well: an Enum travels
/// as its names, which is what the per-row UDF hands out too, a 128/256-bit integer, wider than any
/// Arrow integer, as its decimal text instead of opaque little-endian bytes, and a BFloat16, which
/// Arrow does not have, as the Float32 that holds every BFloat16 exactly.
DB::DataTypePtr argumentTypeForArrow(const DB::DataTypePtr & type)
{
    if (const auto * nullable = typeid_cast<const DB::DataTypeNullable *>(type.get()))
        return DB::makeNullable(argumentTypeForArrow(nullable->getNestedType()));

    if (const auto * array = typeid_cast<const DB::DataTypeArray *>(type.get()))
        return std::make_shared<DB::DataTypeArray>(argumentTypeForArrow(array->getNestedType()));

    if (const auto * map = typeid_cast<const DB::DataTypeMap *>(type.get()))
        return std::make_shared<DB::DataTypeMap>(
            argumentTypeForArrow(map->getKeyType()), argumentTypeForArrow(map->getValueType()));

    if (const auto * tuple = typeid_cast<const DB::DataTypeTuple *>(type.get()))
    {
        DB::DataTypes elements;
        elements.reserve(tuple->getElements().size());
        for (const auto & element : tuple->getElements())
            elements.push_back(argumentTypeForArrow(element));
        if (tuple->hasExplicitNames())
            return std::make_shared<DB::DataTypeTuple>(elements, tuple->getElementNames());
        return std::make_shared<DB::DataTypeTuple>(elements);
    }

    switch (type->getTypeId())
    {
        case DB::TypeIndex::Enum8:
        case DB::TypeIndex::Enum16:
        case DB::TypeIndex::Int128:
        case DB::TypeIndex::UInt128:
        case DB::TypeIndex::Int256:
        case DB::TypeIndex::UInt256:
            return std::make_shared<DB::DataTypeString>();
        case DB::TypeIndex::BFloat16:
            return std::make_shared<DB::DataTypeFloat32>();
        default:
            return type;
    }
}

/// Whether every value of a string array is valid UTF-8. Checking the value bytes in one go is
/// much faster than string by string, and equivalent: valid bytes split into valid strings exactly
/// when no string starts in the middle of a character, that is, at a continuation byte.
template <typename StringArray>
bool isValidUtf8Strings(const StringArray & array)
{
    if (array.length() == 0)
        return true;

    const uint8_t * data = array.raw_data();
    const int64_t begin = array.value_offset(0);
    const int64_t end = array.value_offset(array.length());

    arrow::util::InitializeUTF8();
    if (!arrow::util::ValidateUTF8(data + begin, end - begin))
        return false;

    for (int64_t i = 0; i < array.length(); ++i)
    {
        const int64_t offset = array.value_offset(i);
        if (offset < end && (data[offset] & 0xC0) == 0x80)
            return false;
    }
    return true;
}

/// Whether every string in the array, at any depth, is valid UTF-8.
bool isValidUtf8(const arrow::Array & array)
{
    switch (array.type_id())
    {
        case arrow::Type::STRING:
            return isValidUtf8Strings(static_cast<const arrow::StringArray &>(array));
        case arrow::Type::LARGE_STRING:
            return isValidUtf8Strings(static_cast<const arrow::LargeStringArray &>(array));
        case arrow::Type::EXTENSION:
            return isValidUtf8(*static_cast<const arrow::ExtensionArray &>(array).storage());
        default:
            for (const auto & child : array.data()->child_data)
                if (!isValidUtf8(*arrow::MakeArray(child)))
                    return false;
            return true;
    }
}

bool containsUtf8(const arrow::DataType & type)
{
    if (type.id() == arrow::Type::STRING || type.id() == arrow::Type::LARGE_STRING)
        return true;
    if (type.id() == arrow::Type::EXTENSION)
        return containsUtf8(*static_cast<const arrow::ExtensionType &>(type).storage_type());
    for (const auto & field : type.fields())
        if (containsUtf8(*field->type()))
            return true;
    return false;
}

/// One block worth of arguments as an Arrow table, one column per argument. `as_bytes` marks the
/// String arguments declared as bytes/bytearray, which become pa.binary() (values are bytes).
std::shared_ptr<arrow::Table> argumentsToTable(
    const String & name,
    const DB::ColumnsWithTypeAndName & arguments,
    size_t rows,
    const std::vector<bool> & as_bytes)
{
    auto settings = argumentArrowSettings();
    /// Arrow string, binary and list arrays address their bytes with 32-bit offsets. A block holding
    /// more than that, or than the query's output_format_arrow_max_record_batch_size_bytes, which
    /// the Arrow output format follows too, is converted into several chunks.
    if (auto context = DB::CurrentThread::tryGetQueryContext())
        settings.output_max_record_batch_bytes = context->getSettingsRef()[DB::Setting::output_format_arrow_max_record_batch_size_bytes];

    DB::Columns chunk_columns;
    chunk_columns.reserve(arguments.size());
    for (const auto & argument : arguments)
        chunk_columns.push_back(argument.column);
    std::vector<DB::Chunk> chunks;
    chunks.emplace_back(std::move(chunk_columns), rows);

    DB::CHColumnToArrowColumn converter(arguments, ARROW_FORMAT_NAME, settings);
    std::shared_ptr<arrow::Table> table;
    converter.chChunkToArrowTable(table, chunks, arguments.size());

    arrow::FieldVector fields;
    arrow::ChunkedArrayVector columns;
    fields.reserve(arguments.size());
    columns.reserve(arguments.size());
    for (int i = 0; i < table->num_columns(); ++i)
    {
        auto field = table->schema()->field(i);
        auto column = table->column(i);

        if (as_bytes[static_cast<size_t>(i)] && field->type()->id() == arrow::Type::STRING)
        {
            /// String and binary arrays share one layout, so this is a zero-copy view.
            auto view = column->View(arrow::binary());
            if (!view.ok())
                throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Python Arrow UDF '{}': cannot view argument {} as Arrow binary: {}",
                    name, i + 1, view.status().ToString());
            column = std::move(view).ValueOrDie();
            field = field->WithType(arrow::binary());
        }
        else if (containsUtf8(*field->type()))
        {
            /// An Arrow string array must hold valid UTF-8; pyarrow takes it for granted and fails
            /// later, and further from the cause, on anything else. The per-row UDF rejects such a
            /// value while converting it, too.
            for (const auto & chunk : column->chunks())
            {
                if (!isValidUtf8(*chunk))
                    throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE,
                        "Python Arrow UDF '{}': argument {} holds a string that is not valid UTF-8. Declare a String "
                        "argument as bytes to receive it as pyarrow.binary()",
                        name, i + 1);
            }
        }

        fields.push_back(std::move(field));
        columns.push_back(std::move(column));
    }

    return arrow::Table::Make(arrow::schema(std::move(fields)), std::move(columns), static_cast<int64_t>(rows));
}

uint64_t address(const void * pointer)
{
    return reinterpret_cast<uint64_t>(pointer);
}

DB::ColumnPtr allNullColumn(const DB::DataTypePtr & result_type, size_t rows)
{
    /// The result type is always Nullable, so its default is NULL.
    return result_type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
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
    if (null_handling == NullHandling::SKIP)
    {
        for (size_t i = 0; i < argc; ++i)
        {
            const auto & column = full_columns[i];
            const auto * nullable = typeid_cast<const DB::ColumnNullable *>(column.get());
            /// A Variant or a Dynamic value can be NULL without the column being Nullable.
            if (!nullable && !DB::canContainNull(*arguments[i].type))
                continue;

            if (filter.empty())
                filter.resize_fill(input_rows_count, 1);

            if (nullable)
            {
                const auto & null_map = nullable->getNullMapData();
                for (size_t row = 0; row < input_rows_count; ++row)
                    if (null_map[row])
                        filter[row] = 0;
            }
            else
            {
                for (size_t row = 0; row < input_rows_count; ++row)
                    if (column->isNullAt(row))
                        filter[row] = 0;
            }
        }

        if (!filter.empty())
        {
            rows = DB::countBytesInFilter(filter);
            if (rows == 0)
                return allNullColumn(result_type, input_rows_count);
            if (rows == input_rows_count)
                filter.clear();
        }
    }

    /// Convert the arguments before taking the GIL, so that other threads can run Python
    /// meanwhile.
    DB::ColumnsWithTypeAndName batch_columns;
    batch_columns.reserve(argc);
    std::vector<bool> as_bytes(argc, false);
    for (size_t i = 0; i < argc; ++i)
    {
        DB::ColumnWithTypeAndName column{full_columns[i], arguments[i].type, "c" + std::to_string(i)};

        if (!filter.empty())
            column.column = column.column->filter(filter, static_cast<ssize_t>(rows));

        /// What is left under on_null="skip" holds no NULL, so it travels without a validity bitmap.
        if (null_handling == NullHandling::SKIP && column.type->isNullable())
        {
            column.column = assert_cast<const DB::ColumnNullable &>(*column.column).getNestedColumnPtr();
            column.type = DB::removeNullable(column.type);
        }

        /// A declared type is what the function sees, e.g. Float64 for an integer column that
        /// getReturnTypeImpl accepted for a Float64 parameter.
        DB::DataTypePtr target_type = column.type;
        if (i < arg_types.size() && arg_types[i])
            target_type = column.type->isNullable() ? DB::makeNullable(arg_types[i]) : arg_types[i];
        target_type = argumentTypeForArrow(target_type);

        if (!target_type->equals(*column.type))
        {
            column.column = DB::castColumn(column, target_type);
            column.type = target_type;
        }

        as_bytes[i] = i < arg_wants_bytes.size() && arg_wants_bytes[i];
        batch_columns.push_back(std::move(column));
    }

    auto batch = argumentsToTable(name, batch_columns, rows, as_bytes);

    std::optional<arrow::ArrayVector> result_chunks;
    {
        py::gil_scoped_acquire acquire;
        result_chunks = callFunction(batch);
    }

    if (!result_chunks)
        return allNullColumn(result_type, input_rows_count);

    auto result = resultToColumn(std::move(*result_chunks), rows, result_type);
    if (filter.empty())
        return result;

    /// Put NULL back where the dropped rows were.
    auto expanded = DB::IColumn::mutate(std::move(result));
    expanded->expand(filter, /*inverted=*/false);
    return expanded;
}

std::optional<arrow::ArrayVector> PythonArrowUDF::callFunction(const std::shared_ptr<arrow::Table> & batch) const
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
        for (auto chunk : resultChunks(result))
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

py::list PythonArrowUDF::resultChunks(const py::object & result) const
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
            throw DB::Exception(
                DB::ErrorCodes::TYPE_MISMATCH,
                "Python Arrow UDF '{}': cannot convert the returned {} to an Arrow array: {}",
                name, String(py::str(result.get_type().attr("__name__"))), e.what());
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
    size_t result_rows = 0;
    for (const auto & chunk : chunks)
        result_rows += static_cast<size_t>(chunk->length());

    if (result_rows != rows)
        throw DB::Exception(
            DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Python Arrow UDF '{}' returned {} rows for a batch of {} rows",
            name, result_rows, rows);

    /// rows > 0 here, so there is at least one chunk.
    auto arrow_type = chunks.front()->type();
    auto schema = arrow::schema({arrow::field("result", arrow_type)});
    auto table = arrow::Table::Make(
        schema, {std::make_shared<arrow::ChunkedArray>(std::move(chunks), arrow_type)}, static_cast<int64_t>(rows));

    try
    {
        /// Read the array as the ClickHouse type its Arrow type maps to and cast it afterwards:
        /// the cast ArrowColumnToCHColumn applies to a requested type silently wraps integers
        /// that do not fit, where the per-row UDF rejects them.
        const DB::FormatSettings format_settings;
        auto header = DB::ArrowColumnToCHColumn::arrowSchemaToCHHeader(
            *schema,
            nullptr,
            ARROW_FORMAT_NAME,
            format_settings,
            /*skip_columns_with_unsupported_types=*/false,
            /*allow_arrow_null_type=*/true,
            /*allow_inferring_nullable_columns=*/true,
            /*case_insensitive_matching=*/false,
            /*allow_geoparquet_parser=*/false,
            /*enable_json_parsing=*/false);

        DB::ArrowColumnToCHColumn converter(
            header,
            ARROW_FORMAT_NAME,
            format_settings,
            std::nullopt,
            std::nullopt,
            /*allow_missing_columns_=*/false,
            /*null_as_default_=*/false,
            DB::FormatSettings::DateTimeOverflowBehavior::Throw,
            /*allow_geoparquet_parser_=*/false,
            /*case_insensitive_matching_=*/false,
            /*is_stream_=*/false,
            /*enable_json_parsing_=*/false);
        auto chunk = converter.arrowTableToCHChunk(table, rows, nullptr);

        DB::ColumnWithTypeAndName column{chunk.getColumns().front(), header.getByPosition(0).type, "result"};
        if (column.type->equals(*result_type))
            return std::move(column.column);
        /// A value that does not fit is an error, but the precision a float loses on its way to
        /// Float32 is not: NumPy computes in float64.
        if (DB::isFloat(DB::removeNullable(result_type)))
            return DB::castColumn(column, result_type);
        return DB::castColumnAccurate(column, result_type);
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while converting the result of Python Arrow UDF '{}' from Arrow type {} to {}",
            name, arrow_type->ToString(), result_type->getName());
        throw;
    }
}

} // namespace CHDB
