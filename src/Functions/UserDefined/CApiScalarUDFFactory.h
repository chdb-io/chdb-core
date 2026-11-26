#pragma once

#include <Functions/IFunction.h>

#include <functional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>


namespace CHDB
{

/// The scalar functions a host registered through chdb_create_scalar_function() (chdb.h).
///
/// The functions are implemented in programs/local; this registry only maps their names to them,
/// so that the analyzer can resolve a call and the query result cache can tell such a call apart.
/// It is process-wide, like the engine, and outlives every connection: a function stays registered
/// until it is dropped. Calls are resolved after Python UDFs and before the built-in functions.
class CApiScalarUDFFactory
{
public:
    static CApiScalarUDFFactory & instance();

    /// Registers the function `create` returns under `name`. `create` runs only once the name is
    /// known to be free and with the registry locked, so that a function which owns resources
    /// (the host's user_data) is never constructed just to be thrown away. Throws
    /// FUNCTION_ALREADY_EXISTS when the name is taken.
    void add(const String & name, const std::function<std::shared_ptr<DB::IFunction>()> & create);

    /// Unregisters the function. A query that already resolved it keeps it alive until it ends.
    /// Returns false when nothing was registered under that name.
    bool remove(const String & name);

    DB::FunctionOverloadResolverPtr tryGetFunction(const String & name) const;

    bool has(const String & name) const;

    std::vector<String> getRegisteredNames() const;

private:
    CApiScalarUDFFactory() = default;

    mutable std::shared_mutex mutex;
    std::unordered_map<String, std::shared_ptr<DB::IFunction>> functions;
};

} // namespace CHDB
