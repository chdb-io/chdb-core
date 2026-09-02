#include "ChdbGlobalFunctions.h"
#include "ChdbPyType.h"
#include "PythonScalarUDF.h"
#include "PythonUDFRegistry.h"

#include <algorithm>
#include <tuple>
#include <Common/Exception.h>


namespace CHDB
{

namespace
{

std::shared_ptr<ChdbPyType> toChdbPyType(const py::object & raw_obj)
{
    /// Accept Optional[X] / X | None here too, so an explicit return_type may spell
    /// the same thing an annotation can; the engine makes the type Nullable regardless.
    auto obj = unwrapOptionalAnnotation(raw_obj);
    if (py::isinstance<ChdbPyType>(obj))
        return obj.cast<std::shared_ptr<ChdbPyType>>();
    if (py::isinstance<py::str>(obj))
        return std::make_shared<ChdbPyType>(obj.cast<std::string>());
    if (py::isinstance<py::type>(obj))
        return std::make_shared<ChdbPyType>(annotationToDataType(obj));
    throw std::runtime_error("return_type must be a ChdbType, a string, or a Python type, got " + String(py::str(obj.get_type())));
}

std::string toLower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    return s;
}

NullHandling parseNullHandling(const py::object & obj)
{
    if (obj.is_none())
        return NullHandling::SKIP;
    if (py::isinstance<NullHandling>(obj))
        return obj.cast<NullHandling>();
    if (py::isinstance<py::str>(obj))
    {
        auto val = toLower(obj.cast<std::string>());
        if (val == "skip")
            return NullHandling::SKIP;
        if (val == "pass")
            return NullHandling::PASS;
        throw std::runtime_error("on_null must be 'skip' or 'pass', got '" + obj.cast<std::string>() + "'");
    }
    throw std::runtime_error("on_null must be a NullHandling instance or a string, got " + std::string(py::str(obj.get_type())));
}

ExceptionHandling parseExceptionHandling(const py::object & obj)
{
    if (obj.is_none())
        return ExceptionHandling::PROPAGATE;
    if (py::isinstance<ExceptionHandling>(obj))
        return obj.cast<ExceptionHandling>();
    if (py::isinstance<py::str>(obj))
    {
        auto val = toLower(obj.cast<std::string>());
        if (val == "propagate")
            return ExceptionHandling::PROPAGATE;
        if (val == "ignore")
            return ExceptionHandling::IGNORE;
        throw std::runtime_error("on_error must be 'propagate' or 'ignore', got '" + obj.cast<std::string>() + "'");
    }
    throw std::runtime_error("on_error must be an ExceptionHandling instance or a string, got " + std::string(py::str(obj.get_type())));
}

void createFunction(
    const std::string & name,
    const py::function & func,
    const py::object & arg_types,
    const py::object & return_type,
    const py::object & on_null,
    const py::object & on_error)
{
    try
    {
        DB::DataTypePtr data_type = nullptr;
        if (!return_type.is_none())
            data_type = toChdbPyType(return_type)->dataType();

        py::list arg_types_list;
        if (!arg_types.is_none())
        {
            if (!py::isinstance<py::list>(arg_types))
                throw std::runtime_error("arg_types must be a list, got " + std::string(py::str(arg_types.get_type())));
            arg_types_list = arg_types.cast<py::list>();
        }

        auto null_handling = parseNullHandling(on_null);
        auto exception_handling = parseExceptionHandling(on_error);

        registerPythonUDF(name, func, std::move(data_type), arg_types_list, null_handling, exception_handling);
    }
    catch (const DB::Exception & e)
    {
        throw std::runtime_error("Failed to create function '" + name + "': " + e.message());
    }
}

void dropFunction(const std::string & name)
{
    try
    {
        std::ignore = removePythonUDF(name);
    }
    catch (const DB::Exception & e)
    {
        throw std::runtime_error("Failed to drop function '" + name + "': " + e.message());
    }
}

} // anonymous namespace


void registerGlobalFunctions(py::module_ & m)
{
    py::enum_<NullHandling>(m, "NullHandling")
        .value("SKIP", NullHandling::SKIP)
        .value("PASS", NullHandling::PASS);

    py::enum_<ExceptionHandling>(m, "ExceptionHandling")
        .value("PROPAGATE", ExceptionHandling::PROPAGATE)
        .value("IGNORE", ExceptionHandling::IGNORE);

    m.def(
        "create_function",
        &createFunction,
        py::arg("name"),
        py::arg("func"),
        py::arg("arg_types") = py::none(),
        py::arg("return_type") = py::none(),
        py::kw_only(),
        py::arg("on_null") = py::none(),
        py::arg("on_error") = py::none(),
        "Register a Python scalar UDF globally.\n\n"
        "Args:\n"
        "    name (str): Function name to use in SQL queries.\n"
        "    func (callable): Python function to call for each row.\n"
        "    arg_types: List of argument types (ChdbType, str, or Python type).\n"
        "              Optional; if omitted, inferred from parameter annotations.\n"
        "              If provided, must specify types for ALL parameters.\n"
        "    return_type: Return type (ChdbType or str). Optional; if omitted,\n"
        "                 inferred from the function's return type annotation.\n"
        "    on_null (str): How to handle NULL inputs. 'skip' (default) returns\n"
        "                   NULL without calling the function; 'pass' converts\n"
        "                   NULL to None and calls the function.\n"
        "    on_error (str): How to handle exceptions. 'propagate' (default)\n"
        "                    raises the error; 'ignore' returns NULL for that row.\n");

    m.def(
        "drop_function",
        &dropFunction,
        py::arg("name"),
        "Remove a previously registered Python scalar UDF.\n\n"
        "Does nothing if the function is not registered.\n\n"
        "Args:\n"
        "    name (str): Name of the function to remove.\n"
        "Example:\n"
        "    chdb.drop_function('add_int')");
}

} // namespace CHDB
