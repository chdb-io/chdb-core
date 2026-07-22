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
    DB::DataTypePtr return_type,
    const py::list & arg_types_hint,
    NullHandling null_handling,
    ExceptionHandling exception_handling)
{
    py::gil_assert();

    {
        /// Fail fast on duplicate names before paying the Python signature
        /// inspection below; the authoritative re-check still happens under
        /// the unique lock. No Python runs while this lock is held.
        std::shared_lock read_lock(mutex);
        if (udfs.contains(name))
            throw DB::Exception(DB::ErrorCodes::FUNCTION_ALREADY_EXISTS, "Python UDF '{}' is already registered", name);
    }

    /// Build the UDF (initSignature runs Python: inspect.signature) BEFORE
    /// taking the registry lock. On free-threaded builds, running Python while
    /// holding a lock that other attached threads may block on is the same
    /// stop-the-world deadlock class as issue #131: the blocked waiters never
    /// reach a safepoint, so a GC stop-the-world issued during the signature
    /// inspection could never complete.
    auto udf = std::make_shared<PythonScalarUDF>(name, std::move(func), std::move(return_type), null_handling, exception_handling);
    udf->initSignature(arg_types_hint);

    std::unique_lock lock(mutex);

    if (udfs.contains(name))
        throw DB::Exception(DB::ErrorCodes::FUNCTION_ALREADY_EXISTS, "Python UDF '{}' is already registered", name);

    udfs[name] = std::move(udf);
}

DB::FunctionOverloadResolverPtr PythonUDFRegistry::tryGetFunction(const String & name) const
{
    std::shared_lock lock(mutex);
    auto it = udfs.find(name);
    if (it == udfs.end())
        return nullptr;

    return std::make_unique<DB::FunctionToOverloadResolverAdaptor>(it->second);
}

std::vector<String> PythonUDFRegistry::getRegisteredNames() const
{
    std::shared_lock lock(mutex);
    std::vector<String> names;
    names.reserve(udfs.size());
    for (const auto & [n, _] : udfs)
        names.push_back(n);
    return names;
}

bool PythonUDFRegistry::removeUDF(const String & name)
{
    py::gil_assert();

    std::unique_lock lock(mutex);
    return udfs.erase(name) > 0;
}

void PythonUDFRegistry::clear()
{
    py::gil_assert();

    std::unique_lock lock(mutex);
    udfs.clear();
}

void registerPythonUDF(
    const String & name,
    py::function func,
    DB::DataTypePtr return_type,
    const py::list & arg_types_hint,
    NullHandling null_handling,
    ExceptionHandling exception_handling)
{
    PythonUDFRegistry::instance().registerUDF(
        name, std::move(func), std::move(return_type), arg_types_hint, null_handling, exception_handling);
}

bool removePythonUDF(const String & name)
{
    return PythonUDFRegistry::instance().removeUDF(name);
}

} // namespace CHDB
