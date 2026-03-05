#pragma once

#include "PybindWrapper.h"

#include <shared_mutex>
#include <unordered_map>
#include <Functions/UserDefined/PythonUDFFactory.h>
#include <DataTypes/IDataType.h>


namespace CHDB
{

class PythonScalarUDF;

class PythonUDFRegistry : public PythonUDFFactory
{
public:
    static PythonUDFRegistry & instance();

    void registerUDF(
        const String & name,
        py::function func,
        DB::DataTypePtr return_type);

    DB::FunctionOverloadResolverPtr tryGetFunction(const String & name) const override;

    std::vector<String> getRegisteredNames() const override;

    bool removeUDF(const String & name);

    void clear();

private:
    std::unordered_map<String, std::shared_ptr<PythonScalarUDF>> udfs;
    mutable std::shared_mutex mutex_;
};


void registerPythonUDF(
    const String & name,
    py::function func,
    DB::DataTypePtr return_type);

bool removePythonUDF(const String & name);

} // namespace CHDB
