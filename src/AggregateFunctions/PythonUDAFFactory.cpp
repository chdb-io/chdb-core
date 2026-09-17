#include <AggregateFunctions/PythonUDAFFactory.h>

#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>


namespace CHDB
{

namespace
{

struct NullPythonUDAFFactory : PythonUDAFFactory
{
    bool empty() const override { return true; }

    bool has(const String &) const override { return false; }

    DB::AggregateFunctionPtr tryGet(
        const String &, const DB::DataTypes &, const DB::Array &, DB::AggregateFunctionProperties &) const override
    {
        return nullptr;
    }

    std::optional<DB::AggregateFunctionProperties> tryGetProperties(const String &) const override { return {}; }

    std::vector<String> getRegisteredNames() const override { return {}; }
};

NullPythonUDAFFactory null_factory;

}

PythonUDAFFactory * PythonUDAFFactory::impl_ = nullptr;

PythonUDAFFactory & PythonUDAFFactory::instance()
{
    return impl_ ? *impl_ : null_factory;
}

void PythonUDAFFactory::setInstance(PythonUDAFFactory * impl)
{
    impl_ = impl;
}

bool isPythonUDAFName(const String & name_)
{
    auto & factory = PythonUDAFFactory::instance();
    if (factory.empty())
        return false;

    if (factory.has(name_))
        return true;

    /// Mirror AggregateFunctionFactory::isAggregateFunctionName: peel one combinator
    /// suffix at a time and re-check the base name.
    String name = name_;
    while (DB::AggregateFunctionCombinatorPtr combinator
           = DB::AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        name = name.substr(0, name.size() - combinator->getName().size());
        if (factory.has(name))
            return true;
    }

    return false;
}

} // namespace CHDB
