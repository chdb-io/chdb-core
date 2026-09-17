#pragma once

#include "PybindWrapper.h"
#include "PythonUDFCommon.h"

#include <AggregateFunctions/IAggregateFunction.h>

#include <boost/container/small_vector.hpp>

#include <memory>
#include <vector>


namespace CHDB
{

struct UDFArgSpec;

/// Everything a registered Python aggregate function knows about itself. One descriptor
/// is shared by every PythonAggregateUDF instance built from it (the factory builds one
/// per call site, because the argument types differ per call site).
struct PythonUDAFDescriptor
{
    String name;
    /// Zero-argument callable producing a fresh accumulator; usually the accumulator class
    /// itself, but any factory works (e.g. `lambda: WeightedAvg(bias)`). It is called once
    /// per aggregation state, so accumulators are never shared between groups.
    py::object factory;
    /// Declared argument types. A null entry means "not declared", i.e. accept whatever
    /// the call site passes (same convention as PythonScalarUDF).
    DB::DataTypes arg_types;
    /// Parallel to arg_types: the argument was declared as Python bytes/bytearray and
    /// should be delivered as `bytes` rather than a UTF-8-decoded `str`.
    std::vector<bool> arg_wants_bytes;
    /// Always Nullable, so that `evaluate()` returning None becomes SQL NULL.
    DB::DataTypePtr return_type;
    /// update() ends in *args, so the aggregate accepts at least arg_types.size() arguments.
    bool is_variadic = false;
    NullHandling null_handling = NullHandling::SKIP;
    ExceptionHandling exception_handling = ExceptionHandling::PROPAGATE;
    /// The accumulator defines update_batch(*columns); see PythonAggregateUDF for when
    /// it is preferred over the per-row update().
    bool has_update_batch = false;
    /// pickle.dumps / pickle.loads, resolved once at registration. Used by
    /// serialize()/deserialize(), i.e. by -State/-Merge and external aggregation.
    py::object pickle_dumps;
    py::object pickle_loads;

    ~PythonUDAFDescriptor();
};

using PythonUDAFDescriptorPtr = std::shared_ptr<PythonUDAFDescriptor>;

/// Inspect `factory` and build the descriptor. Runs Python, so the caller must hold the
/// GIL and must not hold any lock that another attached thread could block on.
/// `arg_types_hint` is either None (infer from the accumulator class) or a list, possibly
/// empty, declaring the argument types.
PythonUDAFDescriptorPtr makePythonUDAFDescriptor(
    const String & name,
    py::object factory,
    const py::object & arg_types_hint,
    DB::DataTypePtr return_type,
    NullHandling null_handling,
    ExceptionHandling exception_handling);

/** A ClickHouse aggregate function backed by a Python accumulator object.
  *
  * The aggregation state is a plain POD holding borrowed-then-owned PyObject pointers, so
  * it stays trivially relocatable: ClickHouse rehomes states between hash tables and
  * converts single-level tables to two-level ones by moving cells, never by touching state
  * bytes, and Arena chunks are never reallocated.
  *
  * The accumulator object is created lazily on first use rather than in create(). create()
  * runs once per GROUP BY key on the hot path and must not need the GIL; by the time a
  * state is actually used we are already inside a batch-wide py::gil_scoped_acquire.
  *
  * NULL handling is ours, not the Null combinator's: the registry reports
  * AggregateFunctionProperties::is_window_function, which is how an aggregate tells
  * AggregateFunctionFactory::get "I take Nullable arguments as they are". That is what makes
  * on_null="pass" survive an outer combinator (getOwnNullAdapter is only consulted on the
  * outermost function), and it keeps grouped aggregation over a Nullable column on the
  * batched add path instead of the combinator's per-row one.
  *
  * Deriving from IAggregateFunctionHelper rather than IAggregateFunctionDataHelper is
  * deliberate: the latter marks addBatchLookupTable8 final and, for states of at most 16
  * bytes that do not allocate in an arena, aggregates through a stack array of `Data`
  * built outside create()/destroy(). Those temporaries would leak their Python references.
  */
class PythonAggregateUDF final : public DB::IAggregateFunctionHelper<PythonAggregateUDF>
{
public:
    PythonAggregateUDF(
        PythonUDAFDescriptorPtr descriptor_,
        const DB::DataTypes & argument_types_,
        const DB::Array & parameters_);

    String getName() const override;

    bool allocatesMemoryInArena() const override { return false; }

    void create(DB::AggregateDataPtr __restrict place) const override;
    void destroy(DB::AggregateDataPtr __restrict place) const noexcept override;
    bool hasTrivialDestructor() const override { return false; }
    size_t sizeOfData() const override;
    size_t alignOfData() const override;

    void add(
        DB::AggregateDataPtr __restrict place,
        const DB::IColumn ** columns,
        size_t row_num,
        DB::Arena * arena) const override;

    void addManyDefaults(
        DB::AggregateDataPtr __restrict place,
        const DB::IColumn ** columns,
        size_t length,
        DB::Arena * arena) const override;

    void addBatch(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr * places,
        size_t place_offset,
        const DB::IColumn ** columns,
        DB::Arena * arena,
        ssize_t if_argument_pos) const override;

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr __restrict place,
        const DB::IColumn ** __restrict columns,
        DB::Arena * arena,
        ssize_t if_argument_pos) const override;

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr __restrict place,
        const DB::IColumn ** columns,
        const UInt8 * null_map,
        DB::Arena * arena,
        ssize_t if_argument_pos) const override;

    void addBatchSparse(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr * places,
        size_t place_offset,
        const DB::IColumn ** columns,
        DB::Arena * arena) const override;

    void addBatchSparseSinglePlace(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr __restrict place,
        const DB::IColumn ** columns,
        DB::Arena * arena) const override;

    void addBatchArray(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr * places,
        size_t place_offset,
        const DB::IColumn ** columns,
        const UInt64 * offsets,
        DB::Arena * arena) const override;

    void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(DB::AggregateDataPtr &)> init,
        const UInt8 * key,
        const DB::IColumn ** columns,
        DB::Arena * arena) const override;

    void mergeImpl(
        DB::AggregateDataPtr __restrict place,
        DB::ConstAggregateDataPtr rhs,
        DB::Arena * arena) const override;

    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr * places,
        size_t place_offset,
        const DB::AggregateDataPtr * rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        DB::Arena * arena) const override;

    void mergeAndDestroyBatch(
        DB::AggregateDataPtr * dst_places,
        DB::AggregateDataPtr * rhs_places,
        size_t size,
        size_t offset,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        DB::Arena * arena) const override;

    void serialize(
        DB::ConstAggregateDataPtr __restrict place,
        DB::WriteBuffer & buf,
        std::optional<size_t> version) const override;

    void deserialize(
        DB::AggregateDataPtr __restrict place,
        DB::ReadBuffer & buf,
        std::optional<size_t> version,
        DB::Arena * arena) const override;

    void insertResultInto(
        DB::AggregateDataPtr __restrict place,
        DB::IColumn & to,
        DB::Arena * arena) const override;

    void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        DB::AggregateDataPtr * places,
        size_t place_offset,
        DB::IColumn & to,
        DB::Arena * arena) const override;

    /// on_null="pass" wants to see SQL NULLs, so it opts out of the Null combinator and
    /// consumes the Nullable columns directly. on_null="skip" keeps the default adapter,
    /// which filters NULL rows before they reach Python.
    DB::AggregateFunctionPtr getOwnNullAdapter(
        const DB::AggregateFunctionPtr & nested_function,
        const DB::DataTypes & arguments,
        const DB::Array & params,
        const DB::AggregateFunctionProperties & properties) const override;

private:
    /// Aggregation state. POD and trivially relocatable on purpose (see the class comment).
    struct State
    {
        PyObject * accumulator;
        /// Bound accumulator.update, cached so the per-row path skips one attribute
        /// lookup and one bound-method allocation per row.
        PyObject * update;
    };

    /// Small-buffer vectors: the batch paths build these once per block, but add() has to
    /// build them per row (the Null combinator's grouped path calls add() row by row), so
    /// the common low-arity case must not allocate.
    template <typename T>
    using SmallVector = boost::container::small_vector<T, 8>;

    /// Scratch buffers owned by a call site so the per-row path allocates nothing.
    struct RowScratch
    {
        explicit RowScratch(size_t argc);

        SmallVector<py::object> args;
#ifdef CHDB_FREE_THREADING
        /// Reused vectorcall frame; slot 0 is reserved for PY_VECTORCALL_ARGUMENTS_OFFSET.
        SmallVector<PyObject *> argv;
#endif
    };

    static State & state(DB::AggregateDataPtr __restrict place) { return *reinterpret_cast<State *>(place); }
    static const State & state(DB::ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const State *>(place); }

    /// Per-block loop invariants for every declared argument.
    SmallVector<UDFArgSpec> makeArgSpecs(const DB::IColumn ** columns) const;

    /// Create the accumulator if this state does not have one yet. Requires the GIL.
    PyObject * ensureAccumulator(State & target) const;
    void resetAccumulator(State & target) const noexcept;

    /// True when the rows of this block should be pushed through update_batch() instead
    /// of one update() call per row.
    bool useUpdateBatch() const;

    /// Feed one row to accumulator.update(). Requires the GIL.
    void addRow(DB::AggregateDataPtr place, const SmallVector<UDFArgSpec> & specs, size_t row, RowScratch & scratch) const;

    /// Feed a contiguous row range to accumulator.update_batch(), one Python list per
    /// argument. `flags` and `null_map` are optional per-row filters. Requires the GIL.
    void addRowsAsBatch(
        DB::AggregateDataPtr place,
        const SmallVector<UDFArgSpec> & specs,
        size_t row_begin,
        size_t row_end,
        const UInt8 * flags,
        const UInt8 * null_map) const;

    [[noreturn]] void rethrowPythonError(py::error_already_set & error, const String & context) const;

    PythonUDAFDescriptorPtr descriptor;
    /// getResultType() with Nullable peeled off, for insertPythonObjectToColumn.
    DB::DataTypePtr result_actual_type;
};

} // namespace CHDB
