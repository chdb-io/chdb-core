#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <base/Decimal.h>
#include <pybind11/pybind11.h>

namespace CHDB
{

pybind11::object convertTimeFieldToPython(const DB::Field & field);

/// A decimal.Decimal holding exactly `value` with `scale` fractional digits: 330 at scale 2 is Decimal('3.30'),
/// as pyarrow's to_pandas() returns it.
template <typename T>
pybind11::object convertDecimalToPython(const DB::Decimal<T> & value, UInt32 scale);

/// The same, with the decimal.Decimal class looked up once by a caller that converts many values.
template <typename T>
pybind11::object convertDecimalToPython(const DB::Decimal<T> & value, UInt32 scale, const pybind11::handle & decimal_class);

pybind11::object convertTime64FieldToPython(const DB::Field & field);

pybind11::object convertFieldToPython(
    const DB::IColumn & column,
    const DB::DataTypePtr & type,
    size_t index);

pybind11::object convertColumnValueForUDF(
    const DB::IColumn & column,
    const DB::DataTypePtr & type,
    size_t index,
    const DB::DataTypePtr & expected_type = nullptr);

} // namespace CHDB
