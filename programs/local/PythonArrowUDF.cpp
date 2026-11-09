#include "PythonArrowUDF.h"
#include "ArrowSchema.h"
#include "ArrowStreamWrapper.h"
#include "PythonImporter.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/TimezoneMixin.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <base/defines.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/utf8.h>

#include <cmath>

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
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
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

bool isWideInteger(DB::TypeIndex type_id)
{
    return type_id == DB::TypeIndex::Int128 || type_id == DB::TypeIndex::UInt128
        || type_id == DB::TypeIndex::Int256 || type_id == DB::TypeIndex::UInt256;
}

/// The pyarrow type a value of `type` is handed to Python as. Needs the GIL.
py::object pyarrowType(const DB::DataTypePtr & type)
{
    const auto arrow_friendly_type = argumentTypeForArrow(type);
    const DB::ColumnsWithTypeAndName header{{arrow_friendly_type->createColumn(), arrow_friendly_type, "c0"}};
    const auto schema = DB::CHColumnToArrowColumn::calculateArrowSchema(header, ARROW_FORMAT_NAME, nullptr, argumentArrowSettings());

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
        if (isWideInteger(type->getTypeId()))
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

/// ClickHouse parses a 128/256-bit integer without an overflow check: a value that does not fit
/// comes out modulo 2^N, where a narrower integer fails to parse. Print what was parsed and
/// compare it with the text it came from, spelled the same way.
void checkWideIntegersFit(const DB::ColumnPtr & text, const DB::ColumnPtr & parsed, const DB::DataTypePtr & result_type)
{
    const auto text_column = DB::recursiveRemoveLowCardinality(text);
    const auto & text_nullable = assert_cast<const DB::ColumnNullable &>(*text_column);
    const auto & strings = text_nullable.getNestedColumn();

    const auto printed_column = DB::castColumn({parsed, result_type, ""}, DB::makeNullable(std::make_shared<DB::DataTypeString>()));
    const auto & printed = assert_cast<const DB::ColumnNullable &>(*printed_column).getNestedColumn();

    for (size_t row = 0; row < strings.size(); ++row)
    {
        if (text_nullable.isNullAt(row))
            continue;

        /// The parser takes [+-]?[0-9]+ and prints neither '+' nor leading zeros nor "-0".
        std::string_view digits = strings.getDataAt(row);
        bool negative = false;
        if (!digits.empty() && (digits.front() == '+' || digits.front() == '-'))
        {
            negative = digits.front() == '-';
            digits.remove_prefix(1);
        }
        while (digits.size() > 1 && digits.front() == '0')
            digits.remove_prefix(1);
        negative = negative && digits != "0";

        std::string_view printed_digits = printed.getDataAt(row);
        const bool printed_negative = printed_digits.starts_with('-');
        if (printed_negative)
            printed_digits.remove_prefix(1);

        if (negative != printed_negative || digits != printed_digits)
            throw DB::Exception(
                DB::ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "Value {} does not fit into {}",
                strings.getDataAt(row), DB::removeNullable(result_type)->getName());
    }
}

/// pyarrow keeps a naive datetime as its wall-clock reading counted from the epoch as if in UTC.
/// Turn each such reading into the moment it names in `time_zone`.
DB::ColumnPtr wallClockToMoment(DB::ColumnPtr column, UInt32 scale, const DateLUTImpl & time_zone)
{
    const auto & utc = DateLUT::instance("UTC");
    const auto multiplier = DB::DecimalUtils::scaleMultiplier<Int64>(scale);

    auto mutable_column = DB::IColumn::mutate(std::move(column));
    auto & values_column = mutable_column->isNullable()
        ? assert_cast<DB::ColumnNullable &>(*mutable_column).getNestedColumn()
        : *mutable_column;
    for (auto & value : assert_cast<DB::ColumnDecimal<DB::DateTime64> &>(values_column).getData())
    {
        Int64 seconds = value.value / multiplier;
        Int64 fraction = value.value % multiplier;
        if (fraction < 0)
        {
            --seconds;
            fraction += multiplier;
        }

        const auto reading = utc.toDateTimeComponents(seconds);
        const Int64 moment = time_zone.makeDateTime(
            static_cast<Int16>(reading.date.year),
            reading.date.month,
            reading.date.day,
            static_cast<UInt8>(reading.time.hour),
            reading.time.minute,
            reading.time.second);
        value = DB::DateTime64(moment * multiplier + fraction);
    }
    return mutable_column;
}

template <typename T>
void nanToNull(DB::ColumnNullable & column)
{
    auto & values = assert_cast<DB::ColumnVector<T> &>(column.getNestedColumn()).getData();
    auto & null_map = column.getNullMapData();
    for (size_t row = 0; row < values.size(); ++row)
    {
        if (std::isnan(values[row]))
        {
            null_map[row] = 1;
            /// So that an accurate cast to an integer, which also sees the values under NULL,
            /// does not trip over it.
            values[row] = 0;
        }
    }
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
        result_chunks = callFunction(batch, result_type);
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

        /// Detached, the column is not shared, so the steps below change it in place.
        DB::ColumnWithTypeAndName column{std::move(chunk.detachColumns().front()), header.getByPosition(0).type, "result"};

        /// A float NaN is NULL, as when the per-row UDF returns one, whatever the return type.
        const auto source_type_id = DB::removeNullable(column.type)->getTypeId();
        if (source_type_id == DB::TypeIndex::Float32 || source_type_id == DB::TypeIndex::Float64)
        {
            auto nullable = DB::IColumn::mutate(std::move(column.column));
            auto & nullable_column = assert_cast<DB::ColumnNullable &>(*nullable);
            if (source_type_id == DB::TypeIndex::Float32)
                nanToNull<Float32>(nullable_column);
            else
                nanToNull<Float64>(nullable_column);
            column.column = std::move(nullable);
        }

        /// A naive datetime names a moment in the time zone of the return type, the way ClickHouse
        /// parses '2024-01-02 03:04:05' into a column of that type, so that it reads back as the
        /// same wall clock. Other types take the session time zone.
        if (const auto * datetime64 = typeid_cast<const DB::DataTypeDateTime64 *>(DB::removeNullable(column.type).get());
            datetime64 && !datetime64->hasExplicitTimeZone())
        {
            const auto * result_time_zone = dynamic_cast<const TimezoneMixin *>(DB::removeNullable(result_type).get());
            column.column = wallClockToMoment(
                std::move(column.column),
                datetime64->getScale(),
                result_time_zone ? result_time_zone->getTimeZone() : DateLUT::instance());
        }

        const auto result_type_id = DB::removeNullable(result_type)->getTypeId();
        const bool is_float = result_type_id == DB::TypeIndex::Float32 || result_type_id == DB::TypeIndex::Float64;

        DB::ColumnPtr result;
        if (column.type->equals(*result_type))
            result = std::move(column.column);
        /// A value that does not fit is an error, but the precision a float loses on its way to
        /// Float32 is not: NumPy computes in float64.
        else if (is_float)
            result = DB::castColumn(column, result_type);
        else
        {
            result = DB::castColumnAccurate(column, result_type);
            if (isWideInteger(result_type_id) && DB::isString(DB::removeLowCardinalityAndNullable(column.type)))
                checkWideIntegersFit(column.column, result, result_type);
        }

        return result;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while converting the result of Python Arrow UDF '{}' from Arrow type {} to {}",
            name, arrow_type->ToString(), result_type->getName());
        throw;
    }
}

} // namespace CHDB
