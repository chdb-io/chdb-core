#include "PythonUDAFRegistry.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/PythonUDFFactory.h>
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

    /// Ordinary functions are resolved before aggregate ones (see resolveFunction), so a
    /// scalar UDF would take every call the aggregate is meant to serve. That covers the
    /// combinator forms too: with an aggregate `foo`, a scalar UDF named `fooIf` captures
    /// `fooIf(x, cond)`. Registering an aggregate therefore has to look at every scalar
    /// name, not just this one.
    for (const auto & scalar_name : PythonUDFFactory::instance().getRegisteredNames())
    {
        if (!isAggregateNameOrCombinatorForm(scalar_name, name))
            continue;

        if (scalar_name == name)
            throw DB::Exception(
                DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
                "Python UDAF '{}' cannot be registered: a Python scalar UDF with that name already exists",
                name);

        throw DB::Exception(
            DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
            "Python UDAF '{}' cannot be registered: the Python scalar UDF '{}' would capture that "
            "combinator form of it",
            name, scalar_name);
    }

    /// Registries that need a query context to answer - SQL user-defined functions,
    /// executable UDFs, WASM UDFs - are deliberately not consulted here: registration
    /// happens on the Python thread and there may be no context at all (the very first
    /// thing a user does is register, before opening any connection). The reverse
    /// direction is covered centrally instead - AggregateFunctionFactory::hasNameOrAlias
    /// now reports Python UDAFs, which is what those registries check before taking a
    /// name - and resolveFunction refuses a call that is ambiguous at query time.

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

DB::AggregateFunctionProperties makeProperties(const PythonUDAFDescriptor & descriptor)
{
    DB::AggregateFunctionProperties properties;

    /// An accumulator is arbitrary Python: assume its result can depend on the order rows
    /// arrive in, so that an ORDER BY feeding the aggregate is not optimized away.
    properties.is_order_dependent = true;

    /// This flag means "do not wrap me in the Null combinator, I take Nullable arguments as
    /// they are" (see AggregateFunctionFactory::get). Only on_null="pass" may claim it, and
    /// it has to: the combinator would strip the NULL rows that mode exists to deliver, and
    /// getOwnNullAdapter cannot prevent that because it is consulted on the outermost
    /// function only - any combinator sitting outside us would defeat it.
    ///
    /// on_null="skip" deliberately keeps the adapter even though it duplicates a check we
    /// also make ourselves. Combinators that track whether any row contributed read that
    /// from the adapter: without it, AggregateFunctionOrFill sets its "seen a row" flag for
    /// rows we silently drop, so py_sumOrNull() over an all-NULL column would finalize a
    /// fresh accumulator instead of returning NULL (and aggregate_functions_null_for_empty
    /// introduces exactly that shape automatically).
    properties.is_window_function = descriptor.null_handling == NullHandling::PASS;

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

    out_properties = makeProperties(*descriptor);

    /// No Python runs here, so this is safe to call from query threads without the GIL.
    return std::make_shared<PythonAggregateUDF>(std::move(descriptor), argument_types, parameters);
}

std::optional<DB::AggregateFunctionProperties> PythonUDAFRegistry::tryGetProperties(const String & name) const
{
    auto descriptor = find(name);
    if (!descriptor)
        return {};

    return makeProperties(*descriptor);
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
