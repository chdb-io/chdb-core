#pragma once

#include "PybindWrapper.h"
#include "PythonUtils.h"

#include <base/types.h>
#include <rapidjson/document.h>

#include <DataTypes/Serializations/ISerialization.h>

namespace CHDB
{

enum class PythonObjectType
{
	Other,
	None,
	Integer,
	Float,
	Bool,
	Decimal,
	Uuid,
	Datetime,
	Date,
	Time,
	Timedelta,
	String,
	ByteArray,
	MemoryView,
	Bytes,
	List,
	Tuple,
	Dict,
	NdArray,
	NdDatetime
};

PythonObjectType GetPythonObjectType(const py::handle & obj);

bool isInteger(const py::handle & obj);

bool isNone(const py::handle & obj);

bool isFloat(const py::handle & obj);

bool isBoolean(const py::handle & obj);

bool isDecimal(const py::handle & obj);

/// The values pandas treats as missing in an object column: None, pd.NA, pd.NaT, float NaN and Decimal('NaN').
/// The analyzer skips them and the scan of a column it typed writes them as NULL, so both have to agree on this.
/// A String column (no type could be inferred) keeps the text of every value that is not None or float NaN.
bool isMissingValue(const py::handle & obj);

bool isString(const py::handle & obj);

bool isByteArray(const py::handle & obj);

bool isMemoryView(const py::handle & obj);

void convert_to_json_str(const py::handle & obj, String & ret);

void tryInsertJsonResult(
    const py::handle & handle,
    const DB::FormatSettings & format_settings,
    DB::MutableColumnPtr & column,
    DB::SerializationPtr & serialization);

} // namespace CHDB
