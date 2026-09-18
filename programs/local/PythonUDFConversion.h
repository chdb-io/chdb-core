#pragma once

#include "PybindWrapper.h"

#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>


namespace CHDB
{

/// Loop-invariant per-argument state, computed once per block. The generic
/// convertColumnValueForUDF() re-derives all of it on every row (Const/Nullable
/// unwrapping, removeNullable, expected-type checks, dtype dispatch through a
/// Field), which profiles as several percent of a UDF-heavy query.
struct UDFArgSpec
{
    const DB::IColumn * column = nullptr;        /// original column (null checks)
    const DB::IColumn * value_column = nullptr;  /// Const/Nullable unwrapped
    DB::DataTypePtr type;                        /// original (possibly Nullable) type
    DB::DataTypePtr declared_type;               /// UDF-declared arg type, if any
    DB::TypeIndex type_id = DB::TypeIndex::Nothing;
    bool is_const = false;
    bool is_bool = false;
    bool cast_to_float = false;                  /// declared arg type is Float32/64
    bool as_bytes = false;                       /// declared bytes/bytearray -> deliver Python bytes
    bool fast = false;                           /// convertArgFast handles this type
};

UDFArgSpec makeUDFArgSpec(
    const DB::IColumn & column,
    const DB::DataTypePtr & type,
    const DB::DataTypePtr & declared_type,
    bool declared_bytes);

UDFArgSpec makeUDFArgSpec(const DB::ColumnWithTypeAndName & arg, const DB::DataTypePtr & declared_type, bool declared_bytes);

[[noreturn]] void throwUnsupportedFastArgType(DB::TypeIndex type_id);

/// Row conversion for the common argument types using one IColumn virtual call per value
/// instead of a Field round-trip; matches the generic path's results. Defined here rather
/// than in the .cpp so it still inlines into the per-row loops that call it.
inline py::object convertArgFast(const UDFArgSpec & spec, size_t input_row)
{
    if (spec.column->isNullAt(input_row))
        return py::none();

    const size_t row = spec.is_const ? 0 : input_row;

    switch (spec.type_id)
    {
        case DB::TypeIndex::UInt8:
            if (spec.is_bool)
                return py::cast(spec.value_column->getBool(row));
            [[fallthrough]];
        case DB::TypeIndex::UInt16:
        case DB::TypeIndex::UInt32:
        case DB::TypeIndex::UInt64:
        {
            UInt64 value = spec.value_column->getUInt(row);
            if (spec.cast_to_float)
                return py::reinterpret_steal<py::object>(PyFloat_FromDouble(static_cast<double>(value)));
            return py::reinterpret_steal<py::object>(PyLong_FromUnsignedLongLong(value));
        }
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
        {
            Int64 value = spec.value_column->getInt(row);
            if (spec.cast_to_float)
                return py::reinterpret_steal<py::object>(PyFloat_FromDouble(static_cast<double>(value)));
            return py::reinterpret_steal<py::object>(PyLong_FromLongLong(value));
        }
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
        case DB::TypeIndex::BFloat16:
            return py::reinterpret_steal<py::object>(PyFloat_FromDouble(spec.value_column->getFloat64(row)));
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
        {
            auto ref = spec.value_column->getDataAt(row);
            PyObject * obj = spec.as_bytes
                ? PyBytes_FromStringAndSize(ref.data(), static_cast<Py_ssize_t>(ref.size()))
                : PyUnicode_FromStringAndSize(ref.data(), static_cast<Py_ssize_t>(ref.size()));
            if (!obj)
                throw py::error_already_set();
            return py::reinterpret_steal<py::object>(obj);
        }
        default:
            throwUnsupportedFastArgType(spec.type_id);
    }
}

/// Append a Python return value to a result column of `actual_type` (the type with
/// Nullable already peeled off). Python None inserts the column default, which is
/// NULL for the Nullable result columns the UDF machinery builds.
void insertPythonObjectToColumn(
    DB::IColumn & column,
    const DB::DataTypePtr & actual_type,
    const py::handle & value);

} // namespace CHDB
