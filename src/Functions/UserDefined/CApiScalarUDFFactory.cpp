#include <Functions/UserDefined/CApiScalarUDFFactory.h>

#include <Functions/IFunctionAdaptors.h>
#include <Common/Exception.h>

#include <mutex>


namespace DB
{
namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
}
}


namespace CHDB
{

CApiScalarUDFFactory & CApiScalarUDFFactory::instance()
{
    /// Never destroyed: destroying a function calls back into the host that registered it, which
    /// must not happen during static destruction at exit, when the host's runtime may be gone.
    static auto * factory = new CApiScalarUDFFactory;
    return *factory;
}

void CApiScalarUDFFactory::add(const String & name, const std::function<std::shared_ptr<DB::IFunction>()> & create)
{
    std::unique_lock lock(mutex);

    auto [it, inserted] = functions.try_emplace(name);
    if (!inserted)
        throw DB::Exception(DB::ErrorCodes::FUNCTION_ALREADY_EXISTS, "Scalar function '{}' is already registered", name);

    try
    {
        it->second = create();
    }
    catch (...)
    {
        functions.erase(it);
        throw;
    }
}

bool CApiScalarUDFFactory::remove(const String & name)
{
    std::shared_ptr<DB::IFunction> removed;
    {
        std::unique_lock lock(mutex);
        auto it = functions.find(name);
        if (it == functions.end())
            return false;
        removed = std::move(it->second);
        functions.erase(it);
    }
    /// Released here, outside the lock: the last reference calls back into the host.
    removed.reset();
    return true;
}

DB::FunctionOverloadResolverPtr CApiScalarUDFFactory::tryGetFunction(const String & name) const
{
    std::shared_lock lock(mutex);
    auto it = functions.find(name);
    if (it == functions.end())
        return nullptr;

    return std::make_unique<DB::FunctionToOverloadResolverAdaptor>(it->second);
}

bool CApiScalarUDFFactory::has(const String & name) const
{
    std::shared_lock lock(mutex);
    return functions.contains(name);
}

std::vector<String> CApiScalarUDFFactory::getRegisteredNames() const
{
    std::shared_lock lock(mutex);
    std::vector<String> names;
    names.reserve(functions.size());
    for (const auto & [name, _] : functions)
        names.push_back(name);
    return names;
}

} // namespace CHDB
