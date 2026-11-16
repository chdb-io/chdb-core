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

/// getItemType runs for every row of an object column, so the types it returns are shared instances.
static const DataTypePtr & int32Type() { static const DataTypePtr type = std::make_shared<DataTypeInt32>(); return type; }
static const DataTypePtr & int64Type() { static const DataTypePtr type = std::make_shared<DataTypeInt64>(); return type; }
static const DataTypePtr & uint64Type() { static const DataTypePtr type = std::make_shared<DataTypeUInt64>(); return type; }
static const DataTypePtr & float64Type() { static const DataTypePtr type = std::make_shared<DataTypeFloat64>(); return type; }
static const DataTypePtr & boolType() { static const DataTypePtr type = DataTypeFactory::instance().get("Bool"); return type; }
static const DataTypePtr & jsonType()
{
	static const DataTypePtr type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
	return type;
}
static const DataTypePtr & decimalPlaceholderType() { static const DataTypePtr type = createDecimal<DataTypeDecimal>(1, 0); return type; }

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

    if (current_idx == next_idx)
        return true;

    if (current_idx == TypeIndex::Object || next_idx == TypeIndex::Object)
        return false;

    auto is_signed_int = [](TypeIndex idx) { return idx == TypeIndex::Int32 || idx == TypeIndex::Int64; };
    if (is_signed_int(current_idx) && is_signed_int(next_idx))
    {
        current = int64Type();
        return true;
    }

    /// innerAnalyze rejects the UInt64 column when one of its values is negative.
    if ((is_signed_int(current_idx) && next_idx == TypeIndex::UInt64) || (current_idx == TypeIndex::UInt64 && is_signed_int(next_idx)))
    {
        current = uint64Type();
        return true;
    }

    auto is_expected_numeric = [](TypeIndex idx) {
        return idx == TypeIndex::Int32 || idx == TypeIndex::Int64
            || idx == TypeIndex::UInt64 || idx == TypeIndex::Float64;
    };
    if (!is_expected_numeric(current_idx) || !is_expected_numeric(next_idx))
        return false;

    current = float64Type();
    return true;
}

/// Returns nullptr for an integer outside both Int64 and UInt64: Float64 would round it.
static DataTypePtr getBestIntegerType(PyObject * obj, bool & negative)
{
	chassert(obj);

    int overflow = 0;
    int64_t val = PyLong_AsLongLongAndOverflow(obj, &overflow);

	if (overflow == -1)
	{
		negative = true;
		return nullptr;
	}
	if (overflow == 1)
	{
		PyLong_AsUnsignedLongLong(obj);
		if (PyErr_Occurred())
		{
			PyErr_Clear();
			return nullptr;
		}
		return uint64Type();
	}
	if (val == -1 && PyErr_Occurred())
	{
		PyErr_Clear();
		return nullptr;
	}

	if (val < 0)
		negative = true;
	if (val < static_cast<Int64>(std::numeric_limits<int32_t>::min()) || val > static_cast<Int64>(std::numeric_limits<int32_t>::max()))
		return int64Type();
	return int32Type();
}

/// Calls `f` with every value of `column` until it returns false. A pandas Series is read through its
/// numpy array; an object array is walked over its buffer, without a Python call per value.
template <typename F>
static void forEachValue(py::object column, F && f)
{
	auto & import_cache = PythonImporter::ImportCache();
	auto pandas_series = import_cache.pandas.Series();
	if (pandas_series && py::isinstance(column, pandas_series))
		column = column.attr("__array__")();

	size_t rows = py::len(column);
	if (py::isinstance<py::array>(column))
	{
		auto array = py::reinterpret_borrow<py::array>(column);
		if (array.ndim() == 1 && array.dtype().kind() == 'O')
		{
			const auto * data = static_cast<const char *>(array.data());
			auto stride = array.strides(0);
			for (size_t i = 0; i < rows; ++i)
			{
				auto * obj = *reinterpret_cast<PyObject * const *>(data + static_cast<ssize_t>(i) * stride);
				if (!f(py::reinterpret_borrow<py::object>(obj)))
					return;
			}
			return;
		}
	}

	auto row = column.attr("__getitem__");
	for (size_t i = 0; i < rows; ++i)
		if (!f(row(i)))
			return;
}

/// Reads the digit counts from the str() of a finite decimal.Decimal, such as "-12.50", "0.00" or "1.2E-7".
/// Returns false for "Infinity", "-Infinity", and exponents too large for any ClickHouse Decimal.
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

	/// A zero, whatever its exponent (0E+76, 0E-80), fits every Decimal type.
	if (leading_zeros == coefficient_size)
	{
		integer_digits = 0;
		scale = 0;
		return true;
	}

	exponent -= fraction_size;
	scale = std::max<Int64>(-exponent, 0);
	integer_digits = std::max<Int64>(coefficient_size - leading_zeros + exponent, 0);
	return true;
}

/// The narrowest Decimal(P, S) that holds every value of the column exactly, or nullptr when there is none: a value
/// that is neither a decimal.Decimal nor an int (missing values aside), an infinity, or more than 76 digits.
/// Every row is visited because a value outside the sample that did not fit would be truncated or overflow.
static DataTypePtr getExactDecimalType(py::object column)
{
	Int64 scale = 0;
	Int64 integer_digits = 0;
	bool fits = true;

	forEachValue(column, [&](const py::object & obj)
	{
		switch (GetPythonObjectType(obj))
		{
		case PythonObjectType::None:
			return true;
		case PythonObjectType::Float:
			if (std::isnan(PyFloat_AsDouble(obj.ptr())))
				return true;
			fits = false;
			return false;
		case PythonObjectType::Integer:
			{
				auto digits = py::str(obj).cast<std::string>();
				integer_digits = std::max<Int64>(integer_digits, digits.size() - (digits[0] == '-'));
				return true;
			}
		case PythonObjectType::Decimal:
			{
				auto text = py::str(obj).cast<std::string>();
				/// pandas treats Decimal('NaN') as a missing value; str() spells it NaN, -NaN, sNaN or NaN<payload>.
				if (text.find("NaN") != std::string::npos)
					return true;
				Int64 value_integer_digits = 0;
				Int64 value_scale = 0;
				if (!getDecimalDigits(text, value_integer_digits, value_scale))
				{
					fits = false;
					return false;
				}
				scale = std::max(scale, value_scale);
				integer_digits = std::max(integer_digits, value_integer_digits);
				return true;
			}
		default:
			fits = false;
			return false;
		}
	});

	if (!fits)
		return nullptr;

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

	/// Every row is read: the scan cannot widen a column once it reads it, so a value outside a sample that did
	/// not fit would be lost. Reading stops at the first value that rules out every type but String.
	bool can_convert = true;
	auto type = innerAnalyze(column, can_convert);

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

DataTypePtr PandasAnalyzer::getItemType(py::object obj, bool & can_convert, bool & negative_integer)
{
	/// Exactly the values the scan writes as NULL.
	if (isMissingValue(obj))
		return {};

	auto object_type = GetPythonObjectType(obj);

	switch (object_type)
	{
	case PythonObjectType::Dict:
		return jsonType();
	case PythonObjectType::Integer:
		{
			/// An integer beyond 64 bits reads as String unless a later Decimal makes the column a Decimal one, which
			/// holds it exactly: String merges with no number, so innerAnalyze keeps looking instead of stopping here.
			static const DataTypePtr string_type = std::make_shared<DataTypeString>();
			auto best_type = getBestIntegerType(obj.ptr(), negative_integer);
			return best_type ? best_type : string_type;
		}
	case PythonObjectType::Float:
		return float64Type();
	case PythonObjectType::Bool:
		return boolType();
	case PythonObjectType::None:
		return {};
	case PythonObjectType::Decimal:
		/// The type is settled later from every row.
		return decimalPlaceholderType();
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

DataTypePtr PandasAnalyzer::innerAnalyze(py::object column, bool & can_convert) {
	can_convert = true;

	DataTypePtr current_type = {};
	DataTypePtr result;
	DataTypePtr unmergeable;
	bool negative_integer = false;
	forEachValue(column, [&](const py::object & obj)
	{
		DataTypePtr next_type = getItemType(obj, can_convert, negative_integer);

		if (!can_convert)
		{
			result = next_type;
			return false;
		}

		/// Which other values a Decimal column may hold is settled by getExactDecimalType over every row,
		/// including integers that do not merge with each other (1 and 2**63) before it.
		if (next_type && isDecimal(next_type))
		{
			result = next_type;
			return false;
		}

		if (!unmergeable && !mergeAnalyzedTypes(current_type, next_type))
			unmergeable = next_type;
		return true;
	});

	if (result)
		return result;
	if (unmergeable)
	{
		can_convert = false;
		return unmergeable;
	}
	/// A negative value and one above Int64's range fit no integer type, and Float64 would round them.
	if (negative_integer && getTypeIndex(current_type) == TypeIndex::UInt64)
		can_convert = false;
	return current_type;
}

} // namespace CHDB
