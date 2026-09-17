#include "PythonUDAFRegistry.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
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

PythonUDAFRegistry & PythonUDAFRegistry::instance()
{
    static PythonUDAFRegistry registry;
    static std::once_flag flag;
    std::call_once(flag, [] { PythonUDAFFactory::setInstance(&registry); });
    return registry;
}

void PythonUDAFRegistry::registerUDAF(
    const String & name,
    py::object factory,
    DB::DataTypePtr return_type,
    const py::object & arg_types_hint,
    NullHandling null_handling,
    ExceptionHandling exception_handling)
{
    py::gil_assert();

    {
        /// Fail fast on duplicate names before paying the Python inspection below; the
        /// authoritative re-check still happens under the unique lock. No Python runs
        /// while this lock is held.
        std::shared_lock read_lock(mutex);
        if (udafs.contains(name))
            throw DB::Exception(DB::ErrorCodes::FUNCTION_ALREADY_EXISTS, "Python UDAF '{}' is already registered", name);
    }

    /// Anything the engine already resolves wins over a UDAF, so a colliding registration
    /// would be unreachable - and worse, CHDB::isPythonUDAFName would then report every call
    /// of the shadowed function to the query result cache as non-deterministic. Reject it.
    if (isPythonUDAFName(name))
        throw DB::Exception(
            DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "Python UDAF '{}' cannot be registered: it is a combinator form of an already registered Python UDAF",
            name);

    if (DB::AggregateFunctionFactory::instance().isAggregateFunctionName(name))
        throw DB::Exception(
            DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "Python UDAF '{}' cannot be registered: a built-in aggregate function with that name already exists",
            name);

    if (DB::FunctionFactory::instance().hasNameOrAlias(name))
        throw DB::Exception(
            DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "Python UDAF '{}' cannot be registered: an ordinary function with that name already exists",
            name);

    /// Build the descriptor (which runs Python: inspect.signature, pickle import) BEFORE
    /// taking the registry lock. On free-threaded builds, running Python while holding a
    /// lock that other attached threads may block on is the same stop-the-world deadlock
    /// class as issue #131: the blocked waiters never reach a safepoint, so a GC
    /// stop-the-world issued during the inspection could never complete.
    auto descriptor = makePythonUDAFDescriptor(
        name, std::move(factory), arg_types_hint, std::move(return_type), null_handling, exception_handling);

    std::unique_lock lock(mutex);

    if (udafs.contains(name))
        throw DB::Exception(DB::ErrorCodes::FUNCTION_ALREADY_EXISTS, "Python UDAF '{}' is already registered", name);

    udafs[name] = std::move(descriptor);
    has_any.store(true, std::memory_order_release);
}

bool PythonUDAFRegistry::empty() const
{
    return !has_any.load(std::memory_order_acquire);
}

PythonUDAFDescriptorPtr PythonUDAFRegistry::find(const String & name) const
{
    if (empty())
        return nullptr;

    std::shared_lock lock(mutex);
    auto it = udafs.find(name);
    if (it == udafs.end())
        return nullptr;
    return it->second;
}

bool PythonUDAFRegistry::has(const String & name) const
{
    if (empty())
        return false;

    std::shared_lock lock(mutex);
    return udafs.contains(name);
}

namespace
{

DB::AggregateFunctionProperties makeProperties()
{
    DB::AggregateFunctionProperties properties;

    /// An accumulator is arbitrary Python: assume its result can depend on the order rows
    /// arrive in, so that an ORDER BY feeding the aggregate is not optimized away.
    properties.is_order_dependent = true;

    /// This flag means "do not wrap me in the Null combinator, I handle Nullable arguments
    /// myself" (see AggregateFunctionFactory::get). PythonAggregateUDF does exactly that:
    /// on_null="skip" drops rows with a NULL argument and on_null="pass" hands None to
    /// update(). Letting the combinator wrap us instead would silently defeat on_null="pass"
    /// whenever another combinator sits on the outside, and would force the per-row add()
    /// path - one GIL acquisition per row - for every grouped aggregation over a Nullable
    /// column.
    properties.is_window_function = true;

    return properties;
}

}

DB::AggregateFunctionPtr PythonUDAFRegistry::tryGet(
    const String & name,
    const DB::DataTypes & argument_types,
    const DB::Array & parameters,
    DB::AggregateFunctionProperties & out_properties) const
{
    auto descriptor = find(name);
    if (!descriptor)
        return nullptr;

    out_properties = makeProperties();

    /// No Python runs here, so this is safe to call from query threads without the GIL.
    return std::make_shared<PythonAggregateUDF>(std::move(descriptor), argument_types, parameters);
}

std::optional<DB::AggregateFunctionProperties> PythonUDAFRegistry::tryGetProperties(const String & name) const
{
    if (!has(name))
        return {};

    return makeProperties();
}

std::vector<String> PythonUDAFRegistry::getRegisteredNames() const
{
    std::shared_lock lock(mutex);
    std::vector<String> names;
    names.reserve(udafs.size());
    for (const auto & [name, _] : udafs)
        names.push_back(name);
    return names;
}

bool PythonUDAFRegistry::removeUDAF(const String & name)
{
    py::gil_assert();

    /// Dropping the last reference to a descriptor decrefs Python objects and can run
    /// __del__, so the descriptor must die outside the registry lock (see registerUDAF).
    PythonUDAFDescriptorPtr removed;
    {
        std::unique_lock lock(mutex);
        auto it = udafs.find(name);
        if (it == udafs.end())
            return false;
        removed = std::move(it->second);
        udafs.erase(it);
        has_any.store(!udafs.empty(), std::memory_order_release);
    }
    return true;
}

void PythonUDAFRegistry::clear()
{
    py::gil_assert();

    std::unordered_map<String, PythonUDAFDescriptorPtr> removed;
    {
        std::unique_lock lock(mutex);
        removed.swap(udafs);
        has_any.store(false, std::memory_order_release);
    }
}

void registerPythonUDAF(
    const String & name,
    py::object factory,
    DB::DataTypePtr return_type,
    const py::object & arg_types_hint,
    NullHandling null_handling,
    ExceptionHandling exception_handling)
{
    PythonUDAFRegistry::instance().registerUDAF(
        name, std::move(factory), std::move(return_type), arg_types_hint, null_handling, exception_handling);
}

bool removePythonUDAF(const String & name)
{
    return PythonUDAFRegistry::instance().removeUDAF(name);
}

} // namespace CHDB
