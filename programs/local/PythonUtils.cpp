#include "PythonUtils.h"
#include "config.h"

#include <pybind11/gil.h>
#include <pybind11/pytypes.h>
#include <pybind11/numpy.h>
#ifndef CHDB_FREE_THREADING
#include <pybind11/detail/non_limited_api.h>
#endif
#include <utf8proc.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int PY_EXCEPTION_OCCURED;
}

/// Helper function to convert Python 1,2,4 bytes unicode string to utf8 with icu4c
/// kind: 1 for 1-byte characters (Latin1/ASCII equivalent in ICU)
///       2 for 2-byte characters (UTF-16 equivalent)
///       4 for 4-byte characters (Assume UCS-4/UTF-32)
static size_t ConvertPyUnicodeToUtf8(const void * input, int kind, size_t codepoint_cnt, ColumnString::Offsets & offsets, ColumnString::Chars & chars)
{
    if (input == nullptr)
    {
        return 0;
    }

    // Estimate the maximum buffer size required for the UTF-8 output
    // Buffers is reserved from the caller, so we can safely resize it and memory will not be wasted
    size_t estimated_size = codepoint_cnt * 4; // Allocate buffer for UTF-8 output
    size_t chars_cursor = chars.size();
    chars.resize(chars_cursor + estimated_size);

    size_t offset = chars_cursor;
    switch (kind)
    {
        case 1: { // Latin1/ASCII
            const auto * start = static_cast<const uint8_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                auto sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(&chars[offset]));
                offset += sz;
            }
            break;
        }
        case 2: { // UTF-16
            const auto * start = static_cast<const uint16_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                auto sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(&chars[offset]));
                offset += sz;
            }
            break;
        }
        case 4: { // UTF-32
            const auto * start = static_cast<const uint32_t *>(input);
            for (size_t i = 0; i < codepoint_cnt; ++i)
            {
                auto sz = utf8proc_encode_char(start[i], reinterpret_cast<utf8proc_uint8_t *>(&chars[offset]));
                offset += sz;
            }
            break;
        }
    }

    /// ColumnString stores raw payload without terminators: offsets point just
    /// past the last byte of each value (same convention as ColumnString::insertData).
    /// The old code appended a '\0' here, corrupting every non-ASCII string value.
    offsets.push_back(offset);
    chars.resize(offset); // Resize to the actual used size

    return offset; // Return the number of bytes written
}

void FillColumnString(PyObject * obj, ColumnString * column)
{
    const char * data;
    size_t length;
    int kind;
    size_t codepoint_cnt;
    bool direct_insert;

#ifdef CHDB_FREE_THREADING
    direct_insert = true;

    if (PyUnicode_IS_COMPACT_ASCII(obj))
    {
        data = reinterpret_cast<const char *>(PyUnicode_1BYTE_DATA(obj));
        length = PyUnicode_GET_LENGTH(obj);
    }
    else
    {
        auto * unicode = reinterpret_cast<PyCompactUnicodeObject *>(obj);
        if (unicode->utf8 != nullptr)
        {
            data = reinterpret_cast<const char *>(unicode->utf8);
            length = unicode->utf8_length;
        }
        else if (PyUnicode_IS_COMPACT(obj))
        {
            kind = PyUnicode_KIND(obj);
            if (kind == PyUnicode_1BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_1BYTE_DATA(obj));
            else if (kind == PyUnicode_2BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_2BYTE_DATA(obj));
            else if (kind == PyUnicode_4BYTE_KIND)
                data = reinterpret_cast<const char *>(PyUnicode_4BYTE_DATA(obj));
            else
                throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Failed to convert Python unicode object to UTF-8");

            codepoint_cnt = PyUnicode_GET_LENGTH(obj);
            direct_insert = false;
        }
        else
        {
            pybind11::gil_scoped_acquire acquire;
            Py_ssize_t bytes_size = -1;
            data = PyUnicode_AsUTF8AndSize(obj, &bytes_size);
            if (!data || bytes_size < 0)
                throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Failed to convert Python unicode object to UTF-8");
            length = bytes_size;
        }
    }
#else
    if (!pybind11::non_limited_api::getPyUnicodeUtf8(obj, data, length, kind, codepoint_cnt, direct_insert))
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Failed to convert Python unicode object to UTF-8");
#endif

    if (direct_insert)
    {
        column->insertData(data, length);
    }
    else
    {
        ConvertPyUnicodeToUtf8(data, kind, codepoint_cnt, column->getOffsets(), column->getChars());
    }
}

bool _isInheritsFromPyReader(const py::handle & obj)
{
    try
    {
        // Check directly if obj is an instance of a class named "PyReader"
        if (py::str(obj.attr("__class__").attr("__name__")).cast<std::string>() == "PyReader")
            return true;

        // Check the direct base classes of obj's class for "PyReader"
        py::tuple bases = obj.attr("__class__").attr("__bases__");
        for (auto base : bases)
            if (py::str(base.attr("__name__")).cast<std::string>() == "PyReader")
                return true;
    }
    catch (const py::error_already_set &)
    {
        // Ignore the exception, and return false
    }

    return false;
}

// Will try to get the ref of py::array from pandas Series, or PyArrow Table
// without import numpy or pyarrow. Just from class name for now.
const void * tryGetPyArray(
    const py::object & obj,
    py::handle & result,
    py::handle & tmp,
    std::string & type_name,
    size_t & row_count)
{
    py::gil_scoped_acquire acquire;
    type_name = py::str(obj.attr("__class__").attr("__name__")).cast<std::string>();
    if (type_name == "ndarray")
    {
        // Return the handle of py::array directly
        row_count = py::len(obj);
        py::array array = obj.cast<py::array>();
        result = array;
        return array.data();
    }
    else if (type_name == "Series")
    {
        // Try to get the handle of py::array from pandas Series
        py::array array = obj.attr("values");
        row_count = py::len(obj);
        // if element type is bytes or object, we need to convert it to string
        // chdb todo: need more type check
        if (row_count > 0)
        {
            auto elem_type = obj.attr("iloc").attr("__getitem__")(0).attr("__class__").attr("__name__").cast<std::string>();
            if (elem_type == "str" || elem_type == "unicode")
            {
                result = array;
                return array.data();
            }
            if (elem_type == "bytes" || elem_type == "object")
            {
                // chdb todo: better handle for bytes and object type
                auto str_obj = obj.attr("astype")(py::dtype("str"));
                array = str_obj.attr("values");
                result = array;
                tmp = array;
                tmp.inc_ref();
                return array.data();
            }
        }

        result = array;
        return array.data();
    }
    else if (type_name == "Table")
    {
        // Try to get the handle of py::array from PyArrow Table
        py::array array = obj.attr("to_pandas")();
        row_count = py::len(obj);
        result = array;
        tmp = array;
        tmp.inc_ref();
        return array.data();
    }
    else if (type_name == "ChunkedArray")
    {
        // Try to get the handle of py::array from PyArrow ChunkedArray
        py::array array = obj.attr("to_numpy")();
        row_count = py::len(obj);
        result = array;
        tmp = array;
        tmp.inc_ref();
        return array.data();
    }
    else if (type_name == "list")
    {
        // Just set the row count for list
        row_count = py::len(obj);
        result = obj;
        return obj.ptr();
    }

    return nullptr;
}

void insertObjToStringColumn(PyObject * obj, ColumnString * string_column)
{
    /// check if the object is NaN
    if (obj == Py_None || (PyFloat_Check(obj) && std::isnan(PyFloat_AsDouble(obj))))
    {
        // insert default value for string column, which is empty string
        string_column->insertDefault();
        return;
    }
    /// if object is list, tuple, or dict, convert it to json string
    if (PyList_Check(obj) || PyTuple_Check(obj) || PyDict_Check(obj))
    {
        py::gil_scoped_acquire acquire;
        std::string str = py::module::import("json").attr("dumps")(py::reinterpret_borrow<py::object>(obj)).cast<std::string>();
        string_column->insertData(str.data(), str.size());
        return;
    }
    /// try convert the object to string
    try
    {
        py::gil_scoped_acquire acquire;
        std::string str = py::str(obj);
        string_column->insertData(str.data(), str.size());
        return;
    }
    catch (const py::error_already_set & e)
    {
        /// Get type name using stable API
        py::gil_scoped_acquire acquire;
        std::string type_name;
        try
        {
            type_name = py::str(py::handle(obj).attr("__class__").attr("__name__")).cast<std::string>();
        }
        catch (...)
        {
            type_name = "unknown";
        }

        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Error converting Python object {} to string: {}", type_name, e.what());
    }
}

}
