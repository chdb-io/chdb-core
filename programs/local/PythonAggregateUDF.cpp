#include "PythonAggregateUDF.h"

#include "FieldToPython.h"
#include "PythonUDFConversion.h"

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int PY_EXCEPTION_OCCURED;
}
}


namespace CHDB
{

namespace
{

const String UDAF_KIND = "Python UDAF";

/// The methods an accumulator must provide. update_batch is optional.
const char * const METHOD_UPDATE = "update";
const char * const METHOD_UPDATE_BATCH = "update_batch";
const char * const METHOD_MERGE = "merge";
const char * const METHOD_EVALUATE = "evaluate";

const UInt8 * getConditionFlags(const DB::IColumn * column)
{
    return assert_cast<const DB::ColumnUInt8 &>(*column).getData().data();
}

} // anonymous namespace


PythonUDAFDescriptor::~PythonUDAFDescriptor()
{
    py::gil_scoped_acquire acquire;
    factory.release().dec_ref();
    pickle_dumps.release().dec_ref();
    pickle_loads.release().dec_ref();
}


PythonUDAFDescriptorPtr makePythonUDAFDescriptor(
    const String & name,
    py::object factory,
    const py::object & arg_types_hint,
    DB::DataTypePtr return_type,
    NullHandling null_handling,
    ExceptionHandling exception_handling)
{
    py::gil_assert();

    auto descriptor = std::make_shared<PythonUDAFDescriptor>();
    descriptor->name = name;
    descriptor->factory = std::move(factory);
    descriptor->return_type = return_type ? DB::makeNullable(std::move(return_type)) : nullptr;
    descriptor->null_handling = null_handling;
    descriptor->exception_handling = exception_handling;

    if (!py::isinstance<py::function>(descriptor->factory) && !PyCallable_Check(descriptor->factory.ptr()))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDAF '{}': the accumulator must be a callable returning a fresh accumulator "
            "(typically the accumulator class itself), got '{}'",
            name, String(py::str(descriptor->factory.get_type())));

    const bool arg_types_given = !arg_types_hint.is_none();
    const py::list declared_arg_types = arg_types_given ? arg_types_hint.cast<py::list>() : py::list();
    const size_t arg_types_hint_count = py::len(declared_arg_types);

    /// A class exposes its methods as plain functions, so the protocol can be validated and
    /// the types inferred without instantiating an accumulator. Any other callable (a lambda
    /// closing over parameters, say) is opaque until it is called, so it must declare its
    /// types explicitly and is validated when the first accumulator is built.
    const bool factory_is_class = py::isinstance<py::type>(descriptor->factory);
    /// Fixed positional parameters of update(), excluding self and *args. Only meaningful
    /// for a class factory; a plain callable is opaque until it is invoked.
    size_t positional_count = 0;

    if (factory_is_class)
    {
        for (const char * method : {METHOD_UPDATE, METHOD_MERGE, METHOD_EVALUATE})
            if (!py::hasattr(descriptor->factory, method))
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Python UDAF '{}': accumulator class '{}' has no '{}' method; an accumulator must "
                    "define update(self, *args), merge(self, other) and evaluate(self)",
                    name, String(py::str(descriptor->factory.attr("__name__"))), method);

        descriptor->has_update_batch = py::hasattr(descriptor->factory, METHOD_UPDATE_BATCH);

        /// The first parameter of the unbound method is `self`.
        auto signature = inspectUDFSignature(UDAF_KIND, name, descriptor->factory.attr(METHOD_UPDATE), 1);
        descriptor->is_variadic = signature.has_varargs;
        positional_count = signature.positional.size();

        if (!arg_types_given)
        {
            descriptor->arg_types.reserve(signature.positional.size());
            descriptor->arg_wants_bytes.reserve(signature.positional.size());
            for (const auto & parameter : signature.positional)
            {
                if (!parameter.annotation)
                {
                    descriptor->arg_types.emplace_back();
                    descriptor->arg_wants_bytes.push_back(false);
                }
                else
                {
                    descriptor->arg_types.push_back(annotationToDataType(parameter.annotation));
                    descriptor->arg_wants_bytes.push_back(isBytesLikeAnnotation(parameter.annotation));
                }
            }
        }

        if (!descriptor->return_type)
        {
            auto evaluate_signature = inspectUDFSignature(UDAF_KIND, name, descriptor->factory.attr(METHOD_EVALUATE), 1);
            if (evaluate_signature.return_annotation)
                descriptor->return_type = DB::makeNullable(annotationToDataType(evaluate_signature.return_annotation));
        }
    }
    else if (!arg_types_given || !descriptor->return_type)
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDAF '{}': arg_types and return_type must be given explicitly when the accumulator "
            "is not a class (only a class exposes its update()/evaluate() annotations for inference)",
            name);
    }

    if (arg_types_given)
    {
        /// Mirrors resolveArgTypes() for scalar UDFs: the hint describes the fixed positional
        /// parameters, so it must match them exactly even when update() also takes *args.
        if (factory_is_class && arg_types_hint_count != positional_count)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDAF '{}': arg_types has {} elements but update() has {} positional parameters",
                name, arg_types_hint_count, positional_count);

        descriptor->arg_types.clear();
        descriptor->arg_wants_bytes.clear();
        descriptor->arg_types.reserve(arg_types_hint_count);
        descriptor->arg_wants_bytes.reserve(arg_types_hint_count);
        for (auto item : declared_arg_types)
        {
            auto annotation = py::reinterpret_borrow<py::object>(item);
            descriptor->arg_types.push_back(annotationToDataType(annotation));
            descriptor->arg_wants_bytes.push_back(isBytesLikeAnnotation(annotation));
        }

    }

    if (!descriptor->return_type)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDAF '{}': return type not specified; annotate evaluate() or pass return_type",
            name);

    validateUDFTypes(UDAF_KIND, name, descriptor->return_type, descriptor->arg_types);

    auto pickle = py::module_::import("pickle");
    descriptor->pickle_dumps = pickle.attr("dumps");
    descriptor->pickle_loads = pickle.attr("loads");

    return descriptor;
}


PythonAggregateUDF::RowScratch::RowScratch(size_t argc)
    : args(argc)
#ifdef CHDB_FREE_THREADING
    , argv(argc + 1, nullptr)
#endif
{
}


PythonAggregateUDF::PythonAggregateUDF(
    PythonUDAFDescriptorPtr descriptor_,
    const DB::DataTypes & argument_types_,
    const DB::Array & parameters_)
    : DB::IAggregateFunctionHelper<PythonAggregateUDF>(argument_types_, parameters_, descriptor_->return_type)
    , descriptor(std::move(descriptor_))
    , result_actual_type(DB::removeNullable(descriptor->return_type))
{
    if (!parameters_.empty())
        throw DB::Exception(
            DB::ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS,
            "Python UDAF '{}' is not parametric", descriptor->name);

    const size_t declared_count = descriptor->arg_types.size();

    if (descriptor->is_variadic)
    {
        if (argument_types_.size() < declared_count)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDAF '{}': expected at least {} arguments, got {}",
                descriptor->name, declared_count, argument_types_.size());
    }
    else if (argument_types_.size() != declared_count)
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Python UDAF '{}': expected {} arguments, got {}",
            descriptor->name, declared_count, argument_types_.size());
    }

    arguments_can_be_null = std::any_of(
        argument_types_.begin(), argument_types_.end(), [](const auto & type) { return DB::canContainNull(*type); });

    const size_t check_count = std::min(argument_types_.size(), declared_count);
    for (size_t i = 0; i < check_count; ++i)
    {
        const auto & declared_type = descriptor->arg_types[i];
        if (!declared_type)
            continue;

        auto actual_type = DB::removeNullable(argument_types_[i]);
        auto supertype = DB::tryGetLeastSupertype(DB::DataTypes{actual_type, declared_type});
        if (!supertype || !supertype->equals(*declared_type))
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Python UDAF '{}': argument {} type mismatch: expected {}, got {}",
                descriptor->name, i + 1, declared_type->getName(), actual_type->getName());
    }
}

String PythonAggregateUDF::getName() const
{
    return descriptor->name;
}

size_t PythonAggregateUDF::sizeOfData() const
{
    return sizeof(State);
}

size_t PythonAggregateUDF::alignOfData() const
{
    return alignof(State);
}

void PythonAggregateUDF::create(DB::AggregateDataPtr __restrict place) const
{
    /// No Python here on purpose: create() runs once per GROUP BY key, and the accumulator
    /// is materialized later, when the caller already holds the GIL for the whole block.
    new (place) State{nullptr, nullptr};
}

void PythonAggregateUDF::destroy(DB::AggregateDataPtr __restrict place) const noexcept
{
    auto & target = state(place);
    if (!target.accumulator && !target.update)
        return;

    py::gil_scoped_acquire acquire;
    resetAccumulator(target);
}

void PythonAggregateUDF::resetAccumulator(State & target) const noexcept
{
    if (target.update)
    {
        py::handle(target.update).dec_ref();
        target.update = nullptr;
    }
    if (target.accumulator)
    {
        py::handle(target.accumulator).dec_ref();
        target.accumulator = nullptr;
    }
}

PyObject * PythonAggregateUDF::ensureAccumulator(State & target) const
{
    if (target.accumulator)
        return target.accumulator;

    try
    {
        py::object accumulator = descriptor->factory();
        py::object update = accumulator.attr(METHOD_UPDATE);
        target.accumulator = accumulator.release().ptr();
        target.update = update.release().ptr();
    }
    catch (py::error_already_set & e)
    {
        rethrowPythonError(e, "failed to create an accumulator");
    }

    return target.accumulator;
}

void PythonAggregateUDF::rethrowPythonError(py::error_already_set & error, const String & context) const
{
    throw DB::Exception(
        DB::ErrorCodes::PY_EXCEPTION_OCCURED,
        "Python UDAF '{}': {}: {}",
        descriptor->name, context, error.what());
}

PythonAggregateUDF::SmallVector<UDFArgSpec> PythonAggregateUDF::makeArgSpecs(const DB::IColumn ** columns) const
{
    const size_t argc = argument_types.size();
    SmallVector<UDFArgSpec> specs;
    specs.reserve(argc);

    for (size_t i = 0; i < argc; ++i)
        specs.push_back(makeUDFArgSpec(
            *columns[i],
            argument_types[i],
            i < descriptor->arg_types.size() ? descriptor->arg_types[i] : nullptr,
            i < descriptor->arg_wants_bytes.size() && descriptor->arg_wants_bytes[i]));

    return specs;
}

bool PythonAggregateUDF::useUpdateBatch() const
{
    /// A zero-argument aggregate has no columns to hand over, so a batch call would lose the
    /// row count entirely; keep calling update() once per row.
    if (argument_types.empty())
        return false;

    /// on_error="ignore" is a per-row contract: a batch call cannot report which row raised,
    /// so it would have to drop every row of the block. Fall back to the per-row path.
    return descriptor->has_update_batch && descriptor->exception_handling == ExceptionHandling::PROPAGATE;
}

namespace
{

py::object convertArg(const UDFArgSpec & spec, size_t row)
{
    return spec.fast ? convertArgFast(spec, row) : convertColumnValueForUDF(*spec.column, spec.type, row, spec.declared_type);
}

template <typename Specs>
bool hasNullArgument(const Specs & specs, size_t row)
{
    for (const auto & spec : specs)
        if (spec.column->isNullAt(row))
            return true;
    return false;
}

} // anonymous namespace

void PythonAggregateUDF::addRow(
    DB::AggregateDataPtr place,
    const SmallVector<UDFArgSpec> & specs,
    size_t row,
    RowScratch & scratch) const
{
    if (descriptor->null_handling == NullHandling::SKIP && arguments_can_be_null && hasNullArgument(specs, row))
        return;

    auto & target = state(place);
    ensureAccumulator(target);

    const size_t argc = specs.size();
    for (size_t i = 0; i < argc; ++i)
        scratch.args[i] = convertArg(specs[i], row);

#ifdef CHDB_FREE_THREADING
    for (size_t i = 0; i < argc; ++i)
        scratch.argv[i + 1] = scratch.args[i].ptr();

    PyObject * raw_result = PyObject_Vectorcall(
        target.update, scratch.argv.data() + 1, argc | PY_VECTORCALL_ARGUMENTS_OFFSET, nullptr);
#else
    py::tuple py_args(argc);
    for (size_t i = 0; i < argc; ++i)
        py_args[i] = scratch.args[i];

    PyObject * raw_result = PyObject_CallObject(target.update, py_args.ptr());
#endif

    if (!raw_result)
    {
        py::error_already_set e;
        if (descriptor->exception_handling == ExceptionHandling::PROPAGATE)
            throw DB::Exception(
                DB::ErrorCodes::PY_EXCEPTION_OCCURED,
                "Python UDAF '{}' raised an exception in update() at row {}: {}",
                descriptor->name, row, e.what());
        /// on_error="ignore": the row simply does not contribute to this state.
        return;
    }

    py::handle(raw_result).dec_ref();
}

void PythonAggregateUDF::addRowsAsBatch(
    DB::AggregateDataPtr place,
    const SmallVector<UDFArgSpec> & specs,
    size_t row_begin,
    size_t row_end,
    const UInt8 * flags,
    const UInt8 * null_map) const
{
    const size_t argc = specs.size();
    const bool skip_nulls = descriptor->null_handling == NullHandling::SKIP && arguments_can_be_null;

    SmallVector<py::list> columns(argc);
    size_t accepted = 0;

    for (size_t row = row_begin; row < row_end; ++row)
    {
        if (flags && !flags[row])
            continue;
        if (null_map && null_map[row])
            continue;
        if (skip_nulls && hasNullArgument(specs, row))
            continue;

        for (size_t i = 0; i < argc; ++i)
            columns[i].append(convertArg(specs[i], row));
        ++accepted;
    }

    if (accepted == 0)
        return;

    auto & target = state(place);
    ensureAccumulator(target);

    try
    {
        py::tuple args(argc);
        for (size_t i = 0; i < argc; ++i)
            args[i] = columns[i];

        py::handle(target.accumulator).attr(METHOD_UPDATE_BATCH)(*args);
    }
    catch (py::error_already_set & e)
    {
        rethrowPythonError(e, "update_batch() raised an exception");
    }
}

void PythonAggregateUDF::add(
    DB::AggregateDataPtr __restrict place,
    const DB::IColumn ** columns,
    size_t row_num,
    DB::Arena *) const
{
    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    RowScratch scratch(specs.size());
    addRow(place, specs, row_num, scratch);
}

void PythonAggregateUDF::addManyDefaults(
    DB::AggregateDataPtr __restrict place,
    const DB::IColumn ** columns,
    size_t length,
    DB::Arena *) const
{
    if (length == 0)
        return;

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    RowScratch scratch(specs.size());
    for (size_t i = 0; i < length; ++i)
        addRow(place, specs, 0, scratch);
}

void PythonAggregateUDF::addBatch(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr * places,
    size_t place_offset,
    const DB::IColumn ** columns,
    DB::Arena *,
    ssize_t if_argument_pos) const
{
    if (row_begin >= row_end)
        return;

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    RowScratch scratch(specs.size());
    const UInt8 * flags = if_argument_pos >= 0 ? getConditionFlags(columns[if_argument_pos]) : nullptr;

    for (size_t row = row_begin; row < row_end; ++row)
    {
        if (!places[row])
            continue;
        if (flags && !flags[row])
            continue;
        addRow(places[row] + place_offset, specs, row, scratch);
    }
}

void PythonAggregateUDF::addBatchSinglePlace(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr __restrict place,
    const DB::IColumn ** __restrict columns,
    DB::Arena *,
    ssize_t if_argument_pos) const
{
    if (row_begin >= row_end)
        return;

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    const UInt8 * flags = if_argument_pos >= 0 ? getConditionFlags(columns[if_argument_pos]) : nullptr;

    if (useUpdateBatch())
    {
        addRowsAsBatch(place, specs, row_begin, row_end, flags, nullptr);
        return;
    }

    RowScratch scratch(specs.size());
    for (size_t row = row_begin; row < row_end; ++row)
    {
        if (flags && !flags[row])
            continue;
        addRow(place, specs, row, scratch);
    }
}

void PythonAggregateUDF::addBatchSinglePlaceNotNull(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr __restrict place,
    const DB::IColumn ** columns,
    const UInt8 * null_map,
    DB::Arena *,
    ssize_t if_argument_pos) const
{
    if (row_begin >= row_end)
        return;

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    const UInt8 * flags = if_argument_pos >= 0 ? getConditionFlags(columns[if_argument_pos]) : nullptr;

    if (useUpdateBatch())
    {
        addRowsAsBatch(place, specs, row_begin, row_end, flags, null_map);
        return;
    }

    RowScratch scratch(specs.size());
    for (size_t row = row_begin; row < row_end; ++row)
    {
        if (null_map[row])
            continue;
        if (flags && !flags[row])
            continue;
        addRow(place, specs, row, scratch);
    }
}

void PythonAggregateUDF::addBatchSparse(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr * places,
    size_t place_offset,
    const DB::IColumn ** columns,
    DB::Arena *) const
{
    if (row_begin >= row_end)
        return;

    /// Aggregator::prepareAggregateInstructions only allows sparse arguments for
    /// single-argument aggregates ("allow_sparse_arguments = aggregate_columns[i].size() == 1"),
    /// and the generic implementation makes the same assumption: it hands `add` the address of
    /// a single unwrapped values column. Reading argument specs past columns[0] would be an
    /// out-of-bounds read, so state the invariant instead of silently corrupting arguments.
    if (argument_types.size() != 1)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Python UDAF '{}': sparse arguments are only supported for single-argument aggregates, got {}",
            descriptor->name, argument_types.size());

    const auto & column_sparse = assert_cast<const DB::ColumnSparse &>(*columns[0]);
    const auto * values = &column_sparse.getValuesColumn();
    auto offset_it = column_sparse.getIterator(row_begin);

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(&values);
    RowScratch scratch(specs.size());

    for (size_t row = row_begin; row < row_end; ++row, ++offset_it)
    {
        auto * place = places[offset_it.getCurrentRow()];
        if (!place)
            continue;
        addRow(place + place_offset, specs, offset_it.getValueIndex(), scratch);
    }
}

void PythonAggregateUDF::addBatchSparseSinglePlace(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr __restrict place,
    const DB::IColumn ** columns,
    DB::Arena *) const
{
    if (row_begin >= row_end)
        return;

    if (argument_types.size() != 1)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Python UDAF '{}': sparse arguments are only supported for single-argument aggregates, got {}",
            descriptor->name, argument_types.size());

    /// The generic implementation feeds all stored values first and only then the implicit
    /// defaults. An accumulator is arbitrary Python and is declared order-dependent, so walk
    /// the rows in their real order instead.
    const auto & column_sparse = assert_cast<const DB::ColumnSparse &>(*columns[0]);
    const auto * values = &column_sparse.getValuesColumn();
    auto offset_it = column_sparse.getIterator(row_begin);

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(&values);
    RowScratch scratch(specs.size());

    for (size_t row = row_begin; row < row_end; ++row, ++offset_it)
        addRow(place, specs, offset_it.getValueIndex(), scratch);
}

void PythonAggregateUDF::addBatchArray(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr * places,
    size_t place_offset,
    const DB::IColumn ** columns,
    const UInt64 * offsets,
    DB::Arena *) const
{
    if (row_begin >= row_end)
        return;

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    RowScratch scratch(specs.size());

    size_t current_offset = offsets[static_cast<ssize_t>(row_begin) - 1];
    for (size_t row = row_begin; row < row_end; ++row)
    {
        size_t next_offset = offsets[row];
        if (places[row])
        {
            for (size_t nested_row = current_offset; nested_row < next_offset; ++nested_row)
                addRow(places[row] + place_offset, specs, nested_row, scratch);
        }
        current_offset = next_offset;
    }
}

void PythonAggregateUDF::addBatchLookupTable8(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr * map,
    size_t place_offset,
    std::function<void(DB::AggregateDataPtr &)> init,
    const UInt8 * key,
    const DB::IColumn ** columns,
    DB::Arena *) const
{
    if (row_begin >= row_end)
        return;

    py::gil_scoped_acquire acquire;
    auto specs = makeArgSpecs(columns);
    RowScratch scratch(specs.size());

    for (size_t row = row_begin; row < row_end; ++row)
    {
        DB::AggregateDataPtr & place = map[key[row]];
        if (!place)
            init(place);
        addRow(place + place_offset, specs, row, scratch);
    }
}

void PythonAggregateUDF::mergeImpl(
    DB::AggregateDataPtr __restrict place,
    DB::ConstAggregateDataPtr rhs,
    DB::Arena *) const
{
    const auto & source = state(rhs);
    if (!source.accumulator)
        return;

    py::gil_scoped_acquire acquire;
    auto & target = state(place);
    ensureAccumulator(target);

    try
    {
        py::handle(target.accumulator).attr(METHOD_MERGE)(py::handle(source.accumulator));
    }
    catch (py::error_already_set & e)
    {
        rethrowPythonError(e, "merge() raised an exception");
    }
}

void PythonAggregateUDF::mergeBatch(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr * places,
    size_t place_offset,
    const DB::AggregateDataPtr * rhs,
    ThreadPool & thread_pool,
    std::atomic<bool> & is_cancelled,
    DB::Arena * arena) const
{
    py::gil_scoped_acquire acquire;
    DB::IAggregateFunctionHelper<PythonAggregateUDF>::mergeBatch(
        row_begin, row_end, places, place_offset, rhs, thread_pool, is_cancelled, arena);
}

void PythonAggregateUDF::mergeAndDestroyBatch(
    DB::AggregateDataPtr * dst_places,
    DB::AggregateDataPtr * rhs_places,
    size_t size,
    size_t offset,
    ThreadPool &,
    std::atomic<bool> &,
    DB::Arena * arena) const
{
    py::gil_scoped_acquire acquire;

    size_t index = 0;
    try
    {
        for (; index < size; ++index)
        {
            chassert(dst_places[index] + offset != rhs_places[index] + offset);
            mergeImpl(dst_places[index] + offset, rhs_places[index] + offset, arena);
            destroy(rhs_places[index] + offset);
        }
    }
    catch (...)
    {
        /// Unlike the generic implementation, do not let a throwing Python merge() strand the
        /// source states: Aggregator::mergeDataImpl has already detached them from the hash
        /// table, so nothing else would ever destroy them and their accumulators would leak.
        for (size_t rest = index; rest < size; ++rest)
            destroy(rhs_places[rest] + offset);
        throw;
    }
}

void PythonAggregateUDF::serialize(
    DB::ConstAggregateDataPtr __restrict place,
    DB::WriteBuffer & buf,
    std::optional<size_t>) const
{
    const auto & target = state(place);

    /// An empty blob means "this state never received a row"; deserialize() then leaves the
    /// state pristine so that the accumulator is built lazily, exactly as it would have been.
    if (!target.accumulator)
    {
        DB::writeStringBinary(String{}, buf);
        return;
    }

    py::gil_scoped_acquire acquire;

    py::object blob;
    try
    {
        blob = descriptor->pickle_dumps(py::handle(target.accumulator));
    }
    catch (py::error_already_set & e)
    {
        rethrowPythonError(
            e,
            "the accumulator could not be pickled, which is required for -State/-Merge and for "
            "external aggregation (accumulator classes must be importable by name)");
    }

    char * buffer = nullptr;
    Py_ssize_t length = 0;
    if (PyBytes_AsStringAndSize(blob.ptr(), &buffer, &length) != 0)
    {
        py::error_already_set e;
        rethrowPythonError(e, "pickle.dumps() did not return bytes");
    }

    DB::writeVarUInt(static_cast<UInt64>(length), buf);
    buf.write(buffer, static_cast<size_t>(length));
}

void PythonAggregateUDF::deserialize(
    DB::AggregateDataPtr __restrict place,
    DB::ReadBuffer & buf,
    std::optional<size_t>,
    DB::Arena *) const
{
    String blob;
    DB::readStringBinary(blob, buf);

    py::gil_scoped_acquire acquire;
    auto & target = state(place);
    resetAccumulator(target);

    if (blob.empty())
        return;

    try
    {
        py::object accumulator = descriptor->pickle_loads(py::bytes(blob));
        py::object update = accumulator.attr(METHOD_UPDATE);
        target.accumulator = accumulator.release().ptr();
        target.update = update.release().ptr();
    }
    catch (py::error_already_set & e)
    {
        rethrowPythonError(e, "the accumulator could not be unpickled");
    }
}

void PythonAggregateUDF::insertResultInto(
    DB::AggregateDataPtr __restrict place,
    DB::IColumn & to,
    DB::Arena *) const
{
    py::gil_scoped_acquire acquire;

    /// A state that never received a row still has to produce a value: ClickHouse finalizes
    /// empty groups (for example `SELECT myudaf(x) FROM empty_table`). Build the accumulator
    /// and let evaluate() decide what "nothing aggregated" means.
    auto & target = state(place);
    ensureAccumulator(target);

    py::object result;
    try
    {
        result = py::handle(target.accumulator).attr(METHOD_EVALUATE)();
    }
    catch (py::error_already_set & e)
    {
        rethrowPythonError(e, "evaluate() raised an exception");
    }

    insertPythonObjectToColumn(to, result_actual_type, result);
}

void PythonAggregateUDF::insertResultIntoBatch(
    size_t row_begin,
    size_t row_end,
    DB::AggregateDataPtr * places,
    size_t place_offset,
    DB::IColumn & to,
    DB::Arena * arena) const
{
    py::gil_scoped_acquire acquire;
    DB::IAggregateFunctionHelper<PythonAggregateUDF>::insertResultIntoBatch(
        row_begin, row_end, places, place_offset, to, arena);
}

DB::AggregateFunctionPtr PythonAggregateUDF::getOwnNullAdapter(
    const DB::AggregateFunctionPtr & nested_function,
    const DB::DataTypes &,
    const DB::Array &,
    const DB::AggregateFunctionProperties &) const
{
    if (descriptor->null_handling != NullHandling::PASS)
        return nullptr;

    /// Returning the function itself is this codebase's "I keep NULL rows, do not wrap me"
    /// signal. Two callers rely on it: AggregateFunctionCombinatorNull (which this function
    /// never reaches, because AggregateFunctionProperties::is_window_function already keeps
    /// the combinator away) and RewriteAggregateFunctionWithIfPass, which probes for exactly
    /// this identity before rewriting `f(if(cond, x, NULL))` into `fIf(x, cond)`. That
    /// rewrite drops the NULL rows, which is precisely what on_null="pass" must not do.
    return nested_function;
}

} // namespace CHDB
