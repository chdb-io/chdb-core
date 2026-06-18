#pragma once

#include <cstddef>
#include "PybindWrapper.h"
#include "PythonUtils.h"
#include <DataTypes/IDataType.h>

namespace CHDB {

class PandasScan
{
public:
    static DB::ColumnPtr scanColumn(
        const DB::ColumnWrapper & col_wrap,
        const size_t cursor,
        const size_t count,
        const DB::FormatSettings & format_settings);

    /// PREWHERE gather: materialize only the rows of [cursor, cursor+count)
    /// selected by `filter` (with `selected` ones). Arrow-backed strings only.
    static DB::ColumnPtr scanColumnFiltered(
        const DB::ColumnWrapper & col_wrap,
        const size_t cursor,
        const size_t count,
        const DB::IColumn::Filter & filter,
        const size_t selected);

    static DB::ColumnPtr scanObject(
        const DB::ColumnWrapper & col_wrap,
        const size_t cursor,
        const size_t count,
        const DB::FormatSettings & format_settings);

    static void scanObject(
        const size_t cursor,
        const size_t count,
        const DB::FormatSettings & format_settings,
        const void * buf,
        DB::MutableColumnPtr & column);

    /// Direct PREWHERE predicate evaluation on an Arrow-backed string column,
    /// avoiding materialization of the column into a ColumnString. Fills `out`
    /// (which has `count` bytes) with 1 for rows matching the predicate.
    enum class StringPredicate : uint8_t { NotEmpty, Empty, LikeContains };
    static void evalArrowStringPredicate(
        const DB::ColumnWrapper & col_wrap,
        const size_t cursor,
        const size_t count,
        StringPredicate predicate,
        const std::string & needle,
        unsigned char * out);

private:
    static void innerCheck(const DB::ColumnWrapper & col_wrap);

    static void innerScanArrowString(
        const size_t cursor,
        const size_t count,
        const DB::ColumnWrapper & col_wrap,
        DB::MutableColumnPtr & column);

    static void innerScanObject(
        const size_t cursor,
        const size_t count,
        const DB::FormatSettings & format_settings,
        DB::SerializationPtr & serialization,
        PyObject ** objects,
        DB::MutableColumnPtr & column,
        DB::WhichDataType which = DB::WhichDataType(DB::TypeIndex::Object),
        size_t stride = 0);

    template <typename T>
    static void innerScanFloat(
        const size_t cursor,
        const size_t count,
        const T * ptr,
        DB::MutableColumnPtr & column,
        size_t stride = 0);

    template <typename T>
    static void innerScanNumeric(
        const size_t cursor,
        const size_t count,
        const T * data_ptr,
        const bool * mask_ptr,
        DB::MutableColumnPtr & column,
        size_t stride = 0,
        size_t mask_stride = 0);

    static void innerScanDateTime64(
        const size_t cursor,
        const size_t count,
        const Int64 * ptr,
        DB::MutableColumnPtr & column,
        size_t stride = 0);

    static void innerScanInterval(
        const size_t cursor,
        const size_t count,
        const Int64 * ptr,
        DB::MutableColumnPtr & column,
        size_t stride = 0);

    template <typename T, typename IndexType>
    static void innerScanCategory(
        const size_t cursor,
        const size_t count,
        const T * codes_ptr,
        const DB::ColumnUniquePtr & category_unique,
        DB::MutableColumnPtr & column,
        size_t stride = 0);
};

} // namespace CHDB
