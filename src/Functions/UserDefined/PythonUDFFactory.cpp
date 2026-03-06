#include <Functions/UserDefined/PythonUDFFactory.h>


namespace CHDB
{

namespace
{

struct NullPythonUDFFactory : PythonUDFFactory
{
    DB::FunctionOverloadResolverPtr tryGetFunction(const String &) const override { return nullptr; }
    std::vector<String> getRegisteredNames() const override { return {}; }
};

NullPythonUDFFactory null_factory;

}

PythonUDFFactory * PythonUDFFactory::impl_ = nullptr;

PythonUDFFactory & PythonUDFFactory::instance()
{
    return impl_ ? *impl_ : null_factory;
}

void PythonUDFFactory::setInstance(PythonUDFFactory * impl)
{
    impl_ = impl;
}

} // namespace CHDB
