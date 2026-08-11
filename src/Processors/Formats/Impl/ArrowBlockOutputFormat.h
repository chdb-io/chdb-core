#pragma once
#include "config.h"

#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>

namespace arrow { class Schema; class Table; }
namespace arrow::ipc { class RecordBatchWriter; }

namespace DB
{

class CHColumnToArrowColumn;

class ArrowBlockOutputFormat final : public IOutputFormat
{
public:
    ArrowBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_);
    ~ArrowBlockOutputFormat() override;

    String getName() const override { return "Arrow"; }

    void onCancel() noexcept override
    {
        is_stopped = true;
    }

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    /// Lazy initialization that decides between serial and parallel encoding paths
    /// (based on output_format_arrow_parallel_encoding, max_threads, and presence of
    /// LowCardinality-as-Dictionary columns).
    void initializeOnFirstChunk(const Chunk & chunk);

    void prepareWriter(const std::shared_ptr<arrow::Schema> & schema);
    void initWriterIfNeeded();

    void consumeSerial(Chunk chunk);

    /// Schedules an encoding task on the thread pool and drains finished batches in order.
    void scheduleParallel(Chunk chunk);
    void drainReady(std::unique_lock<std::mutex> & lock);

    /// Returns true if we cannot safely run the converter from multiple threads concurrently
    /// because LowCardinality columns are being emitted as Arrow Dictionary (the dictionary
    /// values map is shared across chunks and is not thread-safe).
    bool needsSerialModeForLowCardinality();

    void writeArrowTable(const arrow::Table & table);

    bool stream;
    const FormatSettings format_settings;
    std::shared_ptr<ArrowBufferedOutputStream> arrow_ostream;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;

    /// Initialization happens lazily on the first chunk so that we can inspect the chunk
    /// to set up the Arrow schema. After this flag flips to true, parallel_mode is fixed.
    bool initialized = false;
    bool parallel_mode = false;

    /// Parallel encoding state.
    std::unique_ptr<ThreadPool> pool;

    struct EncodingTask
    {
        size_t seq = 0;
        Chunk chunk;
        std::shared_ptr<arrow::Table> table;
        bool done = false;
    };

    std::deque<std::shared_ptr<EncodingTask>> task_queue;
    std::mutex mutex;
    std::condition_variable cv;
    size_t next_seq = 0;
    size_t in_flight = 0;
    std::exception_ptr background_exception;
    std::atomic_bool is_stopped{false};
};

}

#endif
