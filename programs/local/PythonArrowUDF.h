#pragma once

#include "PythonScalarUDF.h"

#include <arrow/type_fwd.h>

#include <optional>


namespace CHDB
{

/// A Python scalar UDF evaluated one block at a time (`type="arrow"`).
///
/// Every argument reaches the callable as a pyarrow.ChunkedArray holding the whole block, and
/// the callable returns one value per row: a pyarrow Array/ChunkedArray, a one-column
/// Table/RecordBatch, or anything pyarrow.array() accepts (a list, a NumPy array, a pandas
/// Series, ...). The result is read as the ClickHouse type its Arrow type maps to and then cast
/// to the declared return type, where a value that does not fit is an error (an integer out of
/// range, a fraction for an integer type) but precision lost on the way to Float32 is not.
/// A block holding more than an Arrow string array can address, or than the query's
/// output_format_arrow_max_record_batch_size_bytes, arrives as ChunkedArrays of several chunks.
///
/// Signature inspection, type inference and type validation are those of the row-at-a-time
/// PythonScalarUDF; on_null and on_error keep their meaning at block granularity:
///   - on_null="skip" drops the rows with a NULL argument before the call, so the arrays hold
///     no NULLs, and puts NULL back in their place afterwards. A block without any remaining
///     row never reaches Python.
///   - on_error="ignore" turns the whole block into NULL, since a single call covers all rows.
///     Converting the result can still fail; that always propagates, like it does per row.
class PythonArrowUDF final : public PythonScalarUDF
{
public:
    using PythonScalarUDF::PythonScalarUDF;

    void initSignature(const py::list & arg_types_hint) override;

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments,
        const DB::DataTypePtr & result_type,
        size_t input_rows_count) const override;

private:
    /// Calls the function on `batch`, one column per argument, and returns the chunks of the array
    /// it produced, imported into chDB's Arrow. Returns no chunks at all when on_error="ignore"
    /// swallowed an exception. Needs the GIL.
    std::optional<arrow::ArrayVector> callFunction(const std::shared_ptr<arrow::Table> & batch) const;

    /// The chunks of the value the function returned, as a Python list of pyarrow.Array.
    py::list resultChunks(const py::object & result) const;

    DB::ColumnPtr resultToColumn(arrow::ArrayVector chunks, size_t rows, const DB::DataTypePtr & result_type) const;
};

} // namespace CHDB
