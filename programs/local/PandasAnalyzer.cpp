#include "PandasAnalyzer.h"
#include "PythonConversion.h"
#include "PythonImporter.h"

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>

#include <charconv>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
	extern const SettingsUInt64 pandas_analyze_sample;
}

}

using namespace DB;

namespace CHDB
{

static TypeIndex getTypeIndex(const DataTypePtr & type)
{
    return type ? type->getTypeId() : TypeIndex::Nothing;
}

static bool mergeAnalyzedTypes(DataTypePtr & current, const DataTypePtr & next)
{
    auto current_idx = getTypeIndex(current);
    auto next_idx = getTypeIndex(next);

    if (current_idx == TypeIndex::Nothing)
    {
        current = next;
        return true;
    }
    if (next_idx == TypeIndex::Nothing)
        return true;

	chassert(current && next);

    bool current_is_bool = isBool(current);
    bool next_is_bool = isBool(next);
    if (current_is_bool != next_is_bool)
        return false;
    if (current_is_bool && next_is_bool)
        return true;

    auto is_integer = [](TypeIndex idx) { return idx == TypeIndex::Int32 || idx == TypeIndex::Int64 || idx == TypeIndex::UInt64; };
    if (isDecimal(current) || isDecimal(next))
    {
        if (isDecimal(next) && (isDecimal(current) || is_integer(current_idx)))
            current = next;
        return isDecimal(current) && (isDecimal(next) || is_integer(next_idx));
    }

    if (current_idx == next_idx)
        return true;

    if (current_idx == TypeIndex::Object || next_idx == TypeIndex::Object)
        return false;

    auto is_signed_int = [](TypeIndex idx) { return idx == TypeIndex::Int32 || idx == TypeIndex::Int64; };
    if (is_signed_int(current_idx) && is_signed_int(next_idx))
    {
        current = std::make_shared<DataTypeInt64>();
        return true;
    }

    auto is_expected_numeric = [](TypeIndex idx) {
        return idx == TypeIndex::Int32 || idx == TypeIndex::Int64
            || idx == TypeIndex::UInt64 || idx == TypeIndex::Float64;
    };
    if (!is_expected_numeric(current_idx) || !is_expected_numeric(next_idx))
        return false;

    current = std::make_shared<DataTypeFloat64>();
    return true;
}

static DataTypePtr getBestIntegerType(PyObject * obj)
{
	chassert(obj);

    int overflow = 0;
    int64_t val = PyLong_AsLongLongAndOverflow(obj, &overflow);

	if (overflow == -1)
	{
		return std::make_shared<DataTypeFloat64>();
	}
	else if (overflow == 1)
	{
		uint64_t unsigned_value = PyLong_AsUnsignedLongLong(obj);
		if (PyErr_Occurred())
		{
			PyErr_Clear();
			return std::make_shared<DataTypeFloat64>();
		}
		else
		{
			return std::make_shared<DataTypeUInt64>();
		}
	}
	else if (val == -1 && PyErr_Occurred())
	{
		PyErr_Clear();
		return {};
	}

	if (val < static_cast<Int64>(std::numeric_limits<int32_t>::min()) || val > static_cast<Int64>(std::numeric_limits<int32_t>::max()))
		return std::make_shared<DataTypeInt64>();
	else
		return std::make_shared<DataTypeInt32>();
}

/// Reads the digit counts from the str() of a finite decimal.Decimal, such as "-12.50", "0.00" or "1.2E-7".
/// Returns false for "Infinity", "-Infinity", and exponents too large to matter for a ClickHouse Decimal.
static bool getDecimalDigits(std::string_view text, Int64 & integer_digits, Int64 & scale)
{
	if (!text.empty() && text[0] == '-')
		text.remove_prefix(1);
	if (text.empty() || !isNumericASCII(text[0]))
		return false;

	Int64 exponent = 0;
	if (auto e = text.find_first_of("eE"); e != std::string_view::npos)
	{
		auto exponent_text = text.substr(e + 1);
		if (!exponent_text.empty() && exponent_text[0] == '+')
			exponent_text.remove_prefix(1);
		auto [end, error] = std::from_chars(exponent_text.data(), exponent_text.data() + exponent_text.size(), exponent);
		if (error != std::errc() || end != exponent_text.data() + exponent_text.size() || std::abs(exponent) > 1000)
			return false;
		text = text.substr(0, e);
	}

	auto point = text.find('.');
	auto fraction_size = point == std::string_view::npos ? 0 : static_cast<Int64>(text.size() - point - 1);
	auto coefficient_size = static_cast<Int64>(text.size()) - (point == std::string_view::npos ? 0 : 1);
	Int64 leading_zeros = 0;
	for (char c : text)
	{
		if (c == '0')
			++leading_zeros;
		else if (c != '.')
			break;
	}

	exponent -= fraction_size;
	Int64 significant_digits = std::max<Int64>(coefficient_size - leading_zeros, 1);
	scale = std::max<Int64>(-exponent, 0);
	integer_digits = std::max<Int64>(significant_digits + exponent, 0);
	return true;
}

/// The narrowest Decimal(P, S) that holds every value of the column exactly, or nullptr when there is none: a value
/// that is neither a decimal.Decimal nor an int (missing values aside), an infinity, or more than 76 digits.
/// Every row is visited because a value outside the sample that did not fit would be truncated or overflow.
static DataTypePtr getExactDecimalType(py::object column)
{
	auto & import_cache = PythonImporter::ImportCache();
	auto pandas_series = import_cache.pandas.Series();
	if (pandas_series && py::isinstance(column, pandas_series))
		column = column.attr("__array__")();

	auto row = column.attr("__getitem__");
	size_t rows = py::len(column);
	Int64 scale = 0;
	Int64 integer_digits = 0;

	for (size_t i = 0; i < rows; ++i)
	{
		auto obj = row(i);
		switch (GetPythonObjectType(obj))
		{
		case PythonObjectType::None:
			continue;
		case PythonObjectType::Float:
			if (std::isnan(PyFloat_AsDouble(obj.ptr())))
				continue;
			return nullptr;
		case PythonObjectType::Integer:
			{
				auto digits = py::str(obj).cast<std::string>();
				integer_digits = std::max<Int64>(integer_digits, digits.size() - (digits[0] == '-'));
				continue;
			}
		case PythonObjectType::Decimal:
			{
				auto text = py::str(obj).cast<std::string>();
				/// pandas treats Decimal('NaN') as a missing value.
				if (text.ends_with("NaN"))
					continue;
				Int64 value_integer_digits = 0;
				Int64 value_scale = 0;
				if (!getDecimalDigits(text, value_integer_digits, value_scale))
					return nullptr;
				scale = std::max(scale, value_scale);
				integer_digits = std::max(integer_digits, value_integer_digits);
				continue;
			}
		default:
			return nullptr;
		}
	}

	Int64 precision = std::max<Int64>(integer_digits + scale, 1);
	if (precision > static_cast<Int64>(DecimalUtils::max_precision<Decimal256>))
		return nullptr;
	return createDecimal<DataTypeDecimal>(precision, scale);
}

PandasAnalyzer::PandasAnalyzer(const DB::Settings & settings)
{
	analyzed_type = {};

	sample_size = settings[DB::Setting::pandas_analyze_sample];
}

bool PandasAnalyzer::Analyze(py::object column) {
	if (sample_size == 0)
		return false;

	if (sample_size < 0)
	{
		analyzed_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
		return true;
	}

	auto & import_cache = PythonImporter::ImportCache();
	auto pandas = import_cache.pandas();
	if (!pandas)
		return false;

	bool can_convert = true;
	auto increment = getSampleIncrement(py::len(column));
	auto type = innerAnalyze(column, can_convert, increment);

	if (can_convert && !type && increment > 1) {
		auto first_valid_index = column.attr("first_valid_index")();
		if (GetPythonObjectType(first_valid_index) != PythonObjectType::None)
		{
			auto row = column.attr("__getitem__");
			auto obj = row(first_valid_index);
			type = getItemType(obj, can_convert);
		}
	}

	if (can_convert && type && isDecimal(type))
	{
		type = getExactDecimalType(column);
		can_convert = type != nullptr;
	}

	if (can_convert)
	{
		if (!type)
			type = std::make_shared<DataTypeString>();
		analyzed_type = std::make_shared<DataTypeNullable>(type);
	}

	return can_convert;
}

size_t PandasAnalyzer::getSampleIncrement(size_t rows)
{
	auto sample = static_cast<uint64_t>(sample_size);
	if (sample > rows)
		sample = rows;

	if (sample == 0)
		return rows;

	return rows / sample;
}

DataTypePtr PandasAnalyzer::getItemType(py::object obj, bool & can_convert)
{
	auto object_type = GetPythonObjectType(obj);

	switch (object_type)
	{
	case PythonObjectType::Dict:
		return std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
	case PythonObjectType::Integer:
		{
			auto best_type = getBestIntegerType(obj.ptr());
			if (!best_type)
			{
				can_convert = false;
				return std::make_shared<DataTypeString>();
			}
			return best_type;
		}
	case PythonObjectType::Float:
		{
			if (std::isnan(PyFloat_AsDouble(obj.ptr())))
				return {};
			return std::make_shared<DataTypeFloat64>();
		}
	case PythonObjectType::Bool:
		return std::const_pointer_cast<IDataType>(DataTypeFactory::instance().get("Bool"));
	case PythonObjectType::None:
		return {};
	case PythonObjectType::Decimal:
		/// pandas treats Decimal('NaN') as a missing value. The scale is settled later from every row.
		if (obj.attr("is_nan")().cast<bool>())
			return {};
		return createDecimal<DataTypeDecimal>(1, 0);
	case PythonObjectType::Tuple:
	case PythonObjectType::List:
	case PythonObjectType::Datetime:
	case PythonObjectType::Time:
	case PythonObjectType::Date:
	case PythonObjectType::Timedelta:
	case PythonObjectType::String:
	case PythonObjectType::Uuid:
	case PythonObjectType::ByteArray:
	case PythonObjectType::MemoryView:
	case PythonObjectType::Bytes:
	case PythonObjectType::NdDatetime:
	case PythonObjectType::NdArray:
	case PythonObjectType::Other:
		{
			can_convert = false;
			return std::make_shared<DataTypeString>();
		}
	default:
		throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
							"Unknown python object type {}", object_type);
	}
}

DataTypePtr PandasAnalyzer::innerAnalyze(py::object column, bool & can_convert, size_t increment) {
	size_t rows = py::len(column);
	can_convert = true;

	if (rows == 0)
		return {};

	auto & import_cache = PythonImporter::ImportCache();
	auto pandas_series = import_cache.pandas.Series();

	if (pandas_series && py::isinstance(column, pandas_series))
		column = column.attr("__array__")();

	auto row = column.attr("__getitem__");

	DataTypePtr current_type = {};
	for (size_t i = 0; i < rows; i += increment)
	{
		auto obj = row(i);
		DataTypePtr next_type = getItemType(obj, can_convert);

		if (!can_convert)
			return next_type;

		if (!mergeAnalyzedTypes(current_type, next_type))
		{
			can_convert = false;
			return next_type;
		}
	}

	return current_type;
}

} // namespace CHDB
