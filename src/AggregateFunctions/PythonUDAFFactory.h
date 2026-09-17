#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>

#include <optional>


namespace CHDB
{

/** Indirection for the registry of Python user-defined AGGREGATE functions, mirroring
  * CHDB::PythonUDFFactory for scalar UDFs. The registry itself lives in the Python module
  * (programs/local/PythonUDAFRegistry.cpp) and installs itself here at import time; builds
  * without Python keep the null implementation and report nothing.
  *
  * Python UDAFs are deliberately kept out of DB::AggregateFunctionFactory's own map: that
  * map is documented as lock-free ("you must register all functions before usage of get"),
  * while UDAFs are registered and dropped at runtime from user code. Instead
  * AggregateFunctionFactory consults this registry, which keeps combinator resolution, the
  * implicit Null combinator and AggregateFunction(...) type parsing working unchanged.
  *
  * All methods take the exact function name: AggregateFunctionFactory strips combinator
  * suffixes itself before asking.
  */
class PythonUDAFFactory
{
public:
    static PythonUDAFFactory & instance();
    static void setInstance(PythonUDAFFactory * impl);

    /// True when nothing is registered. Lets callers skip the combinator-stripping probe
    /// below, which otherwise runs for every function name of every query.
    virtual bool empty() const = 0;

    virtual bool has(const String & name) const = 0;

    /// Build an aggregate function for this call site and fill `out_properties`, or return
    /// nullptr if `name` is not a Python UDAF. Throws if the name matches but the arguments
    /// do not. Properties are filled by the same locked lookup that builds the function, so
    /// a concurrent drop cannot leave the caller with properties but no function.
    virtual DB::AggregateFunctionPtr tryGet(
        const String & name,
        const DB::DataTypes & argument_types,
        const DB::Array & parameters,
        DB::AggregateFunctionProperties & out_properties) const = 0;

    virtual std::optional<DB::AggregateFunctionProperties> tryGetProperties(const String & name) const = 0;

    virtual std::vector<String> getRegisteredNames() const = 0;

    virtual ~PythonUDAFFactory() = default;

private:
    static PythonUDAFFactory * impl_;
};

/// True when `name`, after stripping any aggregate-function combinator suffixes, names a
/// registered Python UDAF. For callers that see a raw name and cannot strip suffixes
/// themselves (the query result cache walks the AST).
bool isPythonUDAFName(const String & name);

} // namespace CHDB
