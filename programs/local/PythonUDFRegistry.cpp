#include "PythonUDFRegistry.h"
#include <PybindWrapper.h>
#include "PythonScalarUDF.h"

#include <Functions/IFunctionAdaptors.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
}
}


namespace CHDB
{

PythonUDFRegistry & PythonUDFRegistry::instance()
{
    static PythonUDFRegistry registry;
    static std::once_flag flag;
    std::call_once(flag, [] { PythonUDFFactory::setInstance(&registry); });
    return registry;
}

void PythonUDFRegistry::registerUDF(
    const String & name,
    py::function func,
    DB::DataTypePtr return_type)
{
    py::gil_assert();

    std::unique_lock lock(mutex_);

    if (udfs.contains(name))
        throw DB::Exception(DB::ErrorCodes::FUNCTION_ALREADY_EXISTS, "Python UDF '{}' is already registered", name);

    auto udf = std::make_shared<PythonScalarUDF>(name, std::move(func), std::move(return_type));
    udfs[name] = std::move(udf);
}

DB::FunctionOverloadResolverPtr PythonUDFRegistry::tryGetFunction(const String & name) const
{
    std::shared_lock lock(mutex_);
    auto it = udfs.find(name);
    if (it == udfs.end())
        return nullptr;

    return std::make_unique<DB::FunctionToOverloadResolverAdaptor>(it->second);
}

std::vector<String> PythonUDFRegistry::getRegisteredNames() const
{
    std::shared_lock lock(mutex_);
    std::vector<String> names;
    names.reserve(udfs.size());
    for (const auto & [n, _] : udfs)
        names.push_back(n);
    return names;
}

void PythonUDFRegistry::clear()
{
    py::gil_assert();

    std::unique_lock lock(mutex_);
    udfs.clear();
}

void registerPythonUDF(
    const String & name,
    py::function func,
    DB::DataTypePtr return_type)
{
    PythonUDFRegistry::instance().registerUDF(
        name, std::move(func), std::move(return_type));
}

} // namespace CHDB
