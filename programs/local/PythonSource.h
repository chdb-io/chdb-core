#pragma once

#include "ArrowTableReader.h"
#include "DataSourceWrapper.h"
#include "PandasScan.h"
#include "PythonUtils.h"
#include "config.h"

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Processors/ISource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <Poco/Logger.h>

namespace DB
{

namespace py = pybind11;

class PyReader;


class PythonSource : public ISource
{
public:
    /// PREWHERE state shared by all sources of one read: the planner-provided
    /// info, the (possibly multi-step) compiled filter steps, the
    /// post-prewhere output header and the per-block plan precomputed once.
    /// Steps run cheapest-conditions-first; each later step only materializes
    /// its input columns for the rows that survived the previous steps.
    struct PrewhereActions
    {
        enum class OutputKind : uint8_t
        {
            KeepFilterColumn, /// the kept prewhere column: all-true after filtering
            FromWorkBlock,    /// computed by the prewhere actions (or an input passed through)
            GatherFromSource, /// source column materialized only for selected rows
        };
        struct OutputPlan
        {
            OutputKind kind = OutputKind::GatherFromSource;
            size_t sample_index = 0; /// for GatherFromSource
            String name;
        };

        /// A PREWHERE step whose filter is a simple predicate on one Arrow-backed
        /// string column (e.g. `URL LIKE '%x%'`, `SearchPhrase <> ''`) is
        /// evaluated directly on the Arrow buffers, skipping materialization of
        /// the column into a ColumnString. `active` marks such a step.
        struct ArrowPredicate
        {
            bool active = false;
            CHDB::PandasScan::StringPredicate kind = CHDB::PandasScan::StringPredicate::NotEmpty;
            size_t sample_index = 0;
            std::string needle;
        };

        PrewhereInfoPtr info;
        PrewhereExprSteps steps;
        /// Per step: source columns it needs (sample index + name); columns
        /// already produced by earlier steps are consumed from the work block.
        std::vector<std::vector<std::pair<size_t, String>>> step_source_inputs;
        /// Per step: Arrow-direct predicate (active only for eligible steps).
        std::vector<ArrowPredicate> step_arrow_preds;
        Block output_header;
        std::vector<OutputPlan> outputs;
    };
    using PrewhereActionsPtr = std::shared_ptr<const PrewhereActions>;

    /// ORDER BY ... LIMIT top-N pushdown: instead of materializing every
    /// surviving row of every output column and letting a downstream Sort+Limit
    /// discard all but N, each source scans only the sort-key column(s) (plus
    /// the PREWHERE predicate), keeps its local top-N rows by global index, and
    /// materializes the full output columns for just those N. The downstream
    /// Sort+Limit then merges the per-stream top-N sets into the exact result.
    struct TopKActions
    {
        SortDescription sort_description;        /// ORDER BY keys (names + direction/nulls)
        std::vector<size_t> sort_sample_indices; /// sample_block position of each sort key
        size_t limit = 0;                        /// rows to keep per stream (limit + offset)
    };
    using TopKActionsPtr = std::shared_ptr<const TopKActions>;

    PythonSource(
        CHDB::DataSourceWrapperPtr data_source_wrapper_,
        bool isInheritsFromPyReader_,
        bool isPandasDataFrame_,
        const Block & sample_block_,
        PyColumnVecPtr column_cache,
        size_t data_source_row_count,
        size_t max_block_size_,
        size_t stream_index,
        size_t num_streams,
        const FormatSettings & format_settings_,
        CHDB::ArrowTableReaderPtr arrow_table_reader_ = nullptr,
        PrewhereActionsPtr prewhere_ = nullptr,
        TopKActionsPtr topk_ = nullptr);

    ~PythonSource() override = default;

    String getName() const override { return "Python"; }

    Chunk generate() override;


private:
    CHDB::DataSourceWrapperPtr data_source_wrapper;
    bool isInheritsFromPyReader; // If the data_source is a PyReader object
    bool isPandasDataFrame;

    Block sample_block;
    PyColumnVecPtr column_cache;
    size_t data_source_row_count;
    const size_t max_block_size;
    // Caller will only pass stream index and total stream count
    // to the constructor, we need to calculate the start offset and end offset.
    const size_t stream_index;
    const size_t num_streams;
    size_t cursor;
    size_t blocks_emitted = 0; /// pandas scan: blocks claimed by this stream (round-robin)

    Poco::Logger * logger = &Poco::Logger::get("TableFunctionPython");

    const FormatSettings format_settings;

    CHDB::ArrowTableReaderPtr arrow_table_reader;

    Chunk genChunk(size_t & num_rows, PyObjectVecPtr data);

    PyObjectVecPtr scanData(const py::object & data, const std::vector<std::string> & col_names, size_t & cursor, size_t count);
    template <typename T>
    ColumnPtr convert_and_insert_array(const ColumnWrapper & col_wrap, size_t & cursor, size_t count, UInt32 scale = 0);
    template <typename T>
    ColumnPtr convert_and_insert(const py::object & obj, UInt32 scale = 0, bool is_json = false);
    template <typename T>
    void insert_from_ptr(const void * ptr, const MutableColumnPtr & column, size_t offset, size_t row_count, size_t stride = 0);

    void convert_string_array_to_block(PyObject ** buf, const MutableColumnPtr & column, size_t offset, size_t row_count, size_t stride = 0);

    template <typename T>
    void insert_from_list(const py::list & obj, const MutableColumnPtr & column);

    void insert_string_from_array(py::handle obj, const MutableColumnPtr & column);

    Chunk scanDataToChunk();
    Chunk scanDataToChunkPrewhere(bool & exhausted);
    /// Run the PREWHERE steps for [offset, offset+count); on return `current_rows`
    /// is the number of survivors and `cumulative` their selection over the block
    /// (empty == all selected). Returns false when nothing survives.
    bool computeSurvivors(size_t offset, size_t count, Block & work, IColumn::Filter & cumulative, size_t & current_rows);
    Chunk scanDataToChunkTopK();
    /// Materialize column `sample_index` for the `k` rows at the given global indices.
    ColumnPtr gatherRowsByIndex(size_t sample_index, const PaddedPODArray<UInt64> & gidx, size_t k);
    ColumnPtr convertOneColumn(size_t i, size_t offset, size_t count);
    ColumnPtr gatherOneColumn(size_t i, size_t offset, size_t count, const IColumn::Filter & mask, size_t selected);
    void destory(PyObjectVecPtr & data);
    std::pair<size_t, size_t> calculateOffsetAndCount();

    PrewhereActionsPtr prewhere;
    TopKActionsPtr topk;
    bool topk_done = false;
};
}
