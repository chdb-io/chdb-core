#pragma once

#include <Functions/IFunction.h>


namespace CHDB
{

class PythonUDFFactory
{
public:
    static PythonUDFFactory & instance();
    static void setInstance(PythonUDFFactory * impl);

    virtual DB::FunctionOverloadResolverPtr tryGetFunction(const String & name) const = 0;
    virtual std::vector<String> getRegisteredNames() const = 0;

    virtual ~PythonUDFFactory() = default;

private:
    static PythonUDFFactory * impl_;
};

} // namespace CHDB
