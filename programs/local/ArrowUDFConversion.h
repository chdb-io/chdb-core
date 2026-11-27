#pragma once

#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Common/PODArray.h>

#include <arrow/type_fwd.h>

#include <memory>
#include <vector>


/// The conversions shared by the scalar UDFs that are called once per block with Arrow data: the
/// Python Arrow UDF (PythonArrowUDF) and the C API scalar function (CApiScalarUDF). `label` names
/// the function in error messages, e.g. "Python Arrow UDF 'f'".
namespace CHDB::ArrowUDF
{

/// How the arguments of a block are converted to Arrow.
const DB::CHColumnToArrowColumn::Settings & argumentSettings();

/// The type an argument is cast to before it is converted, where the Arrow format's own mapping
/// gives no usable value - at any depth, inside an Array, a Map or a Tuple as well: an Enum travels
/// as its names, a 128/256-bit integer, wider than any Arrow integer, as its decimal text instead of
/// opaque little-endian bytes, and a BFloat16, which Arrow does not have, as the Float32 that holds
/// every BFloat16 exactly.
DB::DataTypePtr argumentTypeForArrow(const DB::DataTypePtr & type);

bool isWideInteger(DB::TypeIndex type_id);

/// Marks in `filter` the rows of the block where no argument is NULL and returns how many there
/// are. `filter` stays empty when no argument can be NULL or no row has to go.
size_t filterOutNullRows(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::Columns & full_columns,
    size_t rows,
    DB::IColumn::Filter & filter);

/// The arguments of one call: `full_columns` (the arguments with constants expanded) without the
/// rows `filter` drops, without the Nullable wrapper where `nulls_skipped` says no NULL is left,
/// and cast to the declared type of the argument where `declared_types` has one (a null entry
/// means undeclared), as argumentTypeForArrow passes it.
DB::ColumnsWithTypeAndName prepareArguments(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::Columns & full_columns,
    const DB::IColumn::Filter & filter,
    size_t rows,
    bool nulls_skipped,
    const DB::DataTypes & declared_types);

/// One block worth of arguments as an Arrow table, one column per argument. `as_binary` marks the
/// String arguments that become Arrow binary instead of UTF-8 strings; `strings_as_binary` makes
/// every String value one, at any depth. A UTF-8 string argument holding invalid UTF-8 is an error;
/// `invalid_utf8_hint` ends its message.
std::shared_ptr<arrow::Table> argumentsToTable(
    const String & label,
    const DB::ColumnsWithTypeAndName & arguments,
    size_t rows,
    const std::vector<bool> & as_binary,
    bool strings_as_binary,
    const String & invalid_utf8_hint);

/// Checks that a declared type has an Arrow form, so that the host can read the argument or build
/// the result, and for a result type that chDB can also read that form back. `what` names the
/// type in the error, e.g. "argument 1".
void checkDeclaredType(const String & label, const DB::DataTypePtr & type, const String & what, bool is_result);

/// Reads the chunks of a result, which must hold `rows` values together, as the ClickHouse type
/// their Arrow type maps to and casts that to `result_type`. Each chunk is read on its own, so the
/// chunks may differ in type. A value that does not fit is an error (an integer out of range, a
/// fraction for an integer type), but precision lost on the way to a float type is not. A naive
/// Arrow timestamp is read in the time zone of the return type. `nan_as_null` turns a float NaN
/// into NULL.
DB::ColumnPtr resultToColumn(
    const String & label,
    arrow::ArrayVector chunks,
    size_t rows,
    const DB::DataTypePtr & result_type,
    bool nan_as_null);

/// A column of `rows` values of `result_type`, all NULL when the type is Nullable.
DB::ColumnPtr allNullColumn(const DB::DataTypePtr & result_type, size_t rows);

/// Puts back the rows `filter` dropped from `result`, as NULL (as the default value where the type
/// cannot be Nullable). Returns `result` unchanged when `filter` is empty.
DB::ColumnPtr expandFilteredRows(DB::ColumnPtr result, const DB::IColumn::Filter & filter);

} // namespace CHDB::ArrowUDF
