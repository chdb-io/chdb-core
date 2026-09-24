#pragma once

#include "PybindWrapper.h"
#include "PythonAggregateUDF.h"

#include <AggregateFunctions/PythonUDAFFactory.h>

#include <atomic>
#include <shared_mutex>
#include <unordered_map>


namespace CHDB
{

/// Runtime registry of Python aggregate functions, consulted by
/// DB::AggregateFunctionFactory through the CHDB::PythonUDAFFactory indirection.
class PythonUDAFRegistry : public PythonUDAFFactory
{
public:
    static PythonUDAFRegistry & instance();

    void registerUDAF(
        const String & name,
        py::object factory,
        DB::DataTypePtr return_type,
        const py::object & arg_types_hint,
        NullHandling null_handling,
        ExceptionHandling exception_handling);

    bool empty() const override;

    bool has(const String & name) const override;

    DB::AggregateFunctionPtr tryGet(
        const String & name,
        const DB::DataTypes & argument_types,
        const DB::Array & parameters,
        DB::AggregateFunctionProperties & out_properties) const override;

    std::optional<DB::AggregateFunctionProperties> tryGetProperties(const String & name) const override;

    std::vector<String> getRegisteredNames() const override;

    bool removeUDAF(const String & name);

    void clear();

private:
    PythonUDAFDescriptorPtr find(const String & name) const;

    std::unordered_map<String, PythonUDAFDescriptorPtr> udafs;
    mutable std::shared_mutex mutex;
    /// Mirrors `!udafs.empty()`, written under the unique lock. Lookups happen for every
    /// function name of every query, and the overwhelmingly common case is that no UDAF is
    /// registered at all; this lets that case skip the shared lock entirely.
    std::atomic<bool> has_any{false};
};


void registerPythonUDAF(
    const String & name,
    py::object factory,
    DB::DataTypePtr return_type,
    const py::object & arg_types_hint,
    NullHandling null_handling,
    ExceptionHandling exception_handling);

bool removePythonUDAF(const String & name);

} // namespace CHDB
