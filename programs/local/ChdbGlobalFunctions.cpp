#include "ChdbGlobalFunctions.h"
#include "ChdbPyType.h"
#include "PythonUDFRegistry.h"

#include <Common/Exception.h>


namespace CHDB
{

namespace
{

void createFunction(
    const std::string & name,
    const py::function & func,
    const std::shared_ptr<ChdbPyType> & return_type)
{
    try
    {
        registerPythonUDF(name, func, return_type->dataType());
    }
    catch (const DB::Exception & e)
    {
        throw std::runtime_error("Failed to create function '" + name + "': " + e.message());
    }
}

} // anonymous namespace


void registerGlobalFunctions(py::module_ & m)
{
    m.def(
        "create_function",
        &createFunction,
        py::arg("name"),
        py::arg("func"),
        py::arg("return_type"),
        "Register a Python scalar UDF globally.\n\n"
        "Args:\n"
        "    name (str): Function name to use in SQL queries.\n"
        "    func (callable): Python function to call for each row.\n"
        "    return_type: Return type (ChdbType).\n"
        "Example:\n"
        "    import chdb\n"
        "    from chdb.sqltypes import INT64\n"
        "    chdb.create_function('add_int', lambda a, b: a + b, INT64)");
}

} // namespace CHDB
