#include <Processors/Formats/Impl/ArrowBlockOutputFormat.h>

#if USE_ARROW

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Formats/FormatFactory.h>
#include <Processors/Port.h>

#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockOutputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchEncoder.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/assert_cast.h>

#include <arrow/ipc/writer.h>
#include <arrow/table.h>
#include <arrow/result.h>


namespace CurrentMetrics
{
    /// Reuse the Parquet encoder thread-pool metrics to avoid touching ClickHouse-wide metric
    /// definitions; both pools serve "columnar output encoding" and are mutually exclusive per query.
    extern const Metric ParquetEncoderThreads;
    extern const Metric ParquetEncoderThreadsActive;
    extern const Metric ParquetEncoderThreadsScheduled;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{

arrow::Compression::type getArrowCompression(FormatSettings::ArrowCompression method)
{
    switch (method)
    {
        case FormatSettings::ArrowCompression::NONE:
            return arrow::Compression::type::UNCOMPRESSED;
        case FormatSettings::ArrowCompression::ZSTD:
            return arrow::Compression::type::ZSTD;
        case FormatSettings::ArrowCompression::LZ4_FRAME:
            return arrow::Compression::type::LZ4_FRAME;
    }
}

}

ArrowBlockOutputFormat::ArrowBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , stream{stream_}
    , format_settings{format_settings_}
{
}

ArrowBlockOutputFormat::~ArrowBlockOutputFormat()
{
    if (pool)
    {
        is_stopped = true;
        {
            std::lock_guard lock(mutex);
            cv.notify_all();
        }
        pool->wait();
    }
}

bool ArrowBlockOutputFormat::needsSerialModeForLowCardinality()
{
    /// When low_cardinality_as_dictionary = false the converter calls
    /// recursiveRemoveLowCardinality() and never touches the shared dictionary_values
    /// map, so parallel encoding is safe. Otherwise the dictionary state is per-converter
    /// and would diverge across worker threads, so we fall back to serial mode.
    if (!format_settings.arrow.low_cardinality_as_dictionary)
        return false;

    const Block & header = getPort(PortKind::Main).getHeader();
    for (const auto & col : header)
    {
        if (col.type && col.type->lowCardinality())
            return true;
    }
    return false;
}

void ArrowBlockOutputFormat::initializeOnFirstChunk(const Chunk & chunk)
{
    const Block & header = getPort(PortKind::Main).getHeader();
    ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(
        header,
        "Arrow",
        CHColumnToArrowColumn::Settings
        {
            .output_string_as_string = format_settings.arrow.output_string_as_string,
            .output_fixed_string_as_fixed_byte_array = format_settings.arrow.output_fixed_string_as_fixed_byte_array,
            .low_cardinality_as_dictionary = format_settings.arrow.low_cardinality_as_dictionary,
            .use_signed_indexes_for_dictionary = format_settings.arrow.use_signed_indexes_for_dictionary,
            .use_64_bit_indexes_for_dictionary = format_settings.arrow.use_64_bit_indexes_for_dictionary,
            .output_date_as_uint16 = format_settings.arrow.output_date_as_uint16,
            .output_uuid_as_fixed_byte_array = format_settings.arrow.output_uuid_as_fixed_byte_array,
            .output_unsupported_types_as_binary = format_settings.arrow.output_unsupported_types_as_binary,
        });

    /// Use the first chunk to materialize the Arrow schema so that the writer header
    /// (and all worker clones) agree on it before any RecordBatch is produced.
    ch_column_to_arrow_column->initializeArrowSchema(&chunk, chunk.getNumColumns());

    bool wants_parallel = format_settings.arrow.parallel_encoding && format_settings.max_threads > 1;
    parallel_mode = wants_parallel && !needsSerialModeForLowCardinality();

    if (parallel_mode)
    {
        pool = std::make_unique<ThreadPool>(
            CurrentMetrics::ParquetEncoderThreads,
            CurrentMetrics::ParquetEncoderThreadsActive,
            CurrentMetrics::ParquetEncoderThreadsScheduled,
            format_settings.max_threads);
    }

    initialized = true;
}

void ArrowBlockOutputFormat::consume(Chunk chunk)
{
    if (!initialized)
        initializeOnFirstChunk(chunk);

    if (parallel_mode)
        scheduleParallel(std::move(chunk));
    else
        consumeSerial(std::move(chunk));
}

void ArrowBlockOutputFormat::consumeSerial(Chunk chunk)
{
    const size_t columns_num = chunk.getNumColumns();
    std::vector<Chunk> chunks;
    chunks.push_back(std::move(chunk));

    std::shared_ptr<arrow::Table> arrow_table;
    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    initWriterIfNeeded();
    writeArrowTable(*arrow_table);
}

void ArrowBlockOutputFormat::scheduleParallel(Chunk chunk)
{
    auto task = std::make_shared<EncodingTask>();
    task->chunk = std::move(chunk);

    {
        std::unique_lock lock(mutex);

        /// Apply backpressure when too many tasks are in flight.
        const size_t max_in_flight = std::max<size_t>(2, format_settings.max_threads * 4);
        while (!is_stopped && in_flight >= max_in_flight && !background_exception)
            cv.wait(lock);

        if (is_stopped)
            return;
        if (background_exception)
            std::rethrow_exception(background_exception);

        task->seq = next_seq++;
        task_queue.push_back(task);
        ++in_flight;
    }

    auto job = [this, task, thread_group = CurrentThread::getGroup()]() noexcept
    {
        std::shared_ptr<arrow::Table> table;
        std::exception_ptr local_exc;
        try
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::PARQUET_ENCODER);

            if (is_stopped)
            {
                std::lock_guard lock(mutex);
                task->done = true;
                cv.notify_all();
                return;
            }

            auto local_conv = ch_column_to_arrow_column->clone(/*copy_arrow_schema=*/true);
            std::vector<Chunk> chunks;
            chunks.push_back(std::move(task->chunk));
            const size_t columns_num = chunks.front().getNumColumns();
            local_conv->chChunkToArrowTable(table, chunks, columns_num);
        }
        catch (...)
        {
            local_exc = std::current_exception();
        }

        std::lock_guard lock(mutex);
        if (local_exc && !background_exception)
            background_exception = local_exc;
        task->table = std::move(table);
        task->done = true;
        cv.notify_all();
    };

    pool->scheduleOrThrowOnError(std::move(job));

    /// Opportunistically write any prefix of the queue that's already finished, in order.
    std::unique_lock lock(mutex);
    drainReady(lock);
}

void ArrowBlockOutputFormat::drainReady(std::unique_lock<std::mutex> & lock)
{
    while (!task_queue.empty() && task_queue.front()->done && !is_stopped)
    {
        auto task = task_queue.front();
        task_queue.pop_front();
        --in_flight;
        cv.notify_all();

        lock.unlock();
        try
        {
            if (task->table)
            {
                initWriterIfNeeded();
                writeArrowTable(*task->table);
            }
        }
        catch (...)
        {
            lock.lock();
            if (!background_exception)
                background_exception = std::current_exception();
            cv.notify_all();
            std::rethrow_exception(background_exception);
        }
        lock.lock();
    }

    if (background_exception)
        std::rethrow_exception(background_exception);
}

void ArrowBlockOutputFormat::writeArrowTable(const arrow::Table & table)
{
    auto status = writer->WriteTable(table, format_settings.arrow.row_group_size);
    if (!status.ok())
        throwFromArrowStatus(status, ErrorCodes::UNKNOWN_EXCEPTION, "Error while writing a table");
}

void ArrowBlockOutputFormat::initWriterIfNeeded()
{
    if (writer)
        return;
    /// In serial mode the Arrow schema is initialized by chChunkToArrowTable; in parallel mode
    /// we initialized it eagerly on the first chunk in initializeOnFirstChunk().
    prepareWriter(ch_column_to_arrow_column->getArrowSchema());
}

void ArrowBlockOutputFormat::finalizeImpl()
{
    /// Wait for all in-flight encoding tasks to finish and drain them in order.
    if (parallel_mode && pool)
    {
        std::unique_lock lock(mutex);
        while (!task_queue.empty() && !is_stopped)
        {
            cv.wait(lock, [&]{
                return is_stopped || (!task_queue.empty() && task_queue.front()->done) || background_exception;
            });

            if (background_exception)
            {
                /// Discard remaining tasks - their workers may still be running but
                /// the pool wait below will join them.
                task_queue.clear();
                in_flight = 0;
                cv.notify_all();
                std::rethrow_exception(background_exception);
            }
            if (is_stopped)
                break;

            drainReady(lock);
        }
    }

    if (!ch_column_to_arrow_column)
    {
        Block header = materializeBlock(getPort(PortKind::Main).getHeader());
        consume(Chunk(header.getColumns(), 0));
    }

    initWriterIfNeeded();

    auto status = writer->Close();
    if (!status.ok())
        throwFromArrowStatus(status, ErrorCodes::UNKNOWN_EXCEPTION, "Error while closing a table");
}

void ArrowBlockOutputFormat::resetFormatterImpl()
{
    if (pool)
    {
        is_stopped = true;
        {
            std::lock_guard lock(mutex);
            cv.notify_all();
        }
        pool->wait();
        is_stopped = false;
    }
    {
        std::lock_guard lock(mutex);
        task_queue.clear();
        in_flight = 0;
        next_seq = 0;
        background_exception = nullptr;
    }
    pool.reset();
    writer.reset();
    arrow_ostream.reset();
    ch_column_to_arrow_column.reset();
    initialized = false;
    parallel_mode = false;
}

void ArrowBlockOutputFormat::prepareWriter(const std::shared_ptr<arrow::Schema> & schema)
{
    arrow_ostream = std::make_shared<ArrowBufferedOutputStream>(out);
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> writer_status;
    arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
    options.codec = *arrow::util::Codec::Create(getArrowCompression(format_settings.arrow.output_compression_method));
    options.emit_dictionary_deltas = true;

    // TODO: should we use arrow::ipc::IpcOptions::alignment?
    if (stream)
        writer_status = arrow::ipc::MakeStreamWriter(arrow_ostream.get(), schema, options);
    else
        writer_status = arrow::ipc::MakeFileWriter(arrow_ostream.get(), schema,options);

    if (!writer_status.ok())
        throwFromArrowStatus(writer_status.status(), ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table writer");

    writer = *writer_status;
}

void registerOutputFormatArrow(FormatFactory & factory);
void registerOutputFormatArrow(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Arrow",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings,
           FormatFilterInfoPtr /*format_filter_info*/) -> OutputFormatPtr
        {
            auto header = std::make_shared<const Block>(sample);
            if (format_settings.arrow.output_use_native_writer)
                return std::make_shared<ArrowIPCBlockOutputFormat>(buf, header, false, format_settings);
            return std::make_shared<ArrowBlockOutputFormat>(buf, header, false, format_settings);
        });
    factory.markFormatHasNoAppendSupport("Arrow");
    factory.markOutputFormatNotTTYFriendly("Arrow");
    factory.setContentType("Arrow", "application/octet-stream");

    factory.registerOutputFormat(
        "ArrowStream",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings,
          FormatFilterInfoPtr /*format_filter_info*/) -> OutputFormatPtr
        {
            auto header = std::make_shared<const Block>(sample);
            if (format_settings.arrow.output_use_native_writer)
                return std::make_shared<ArrowIPCBlockOutputFormat>(buf, header, true, format_settings);
            return std::make_shared<ArrowBlockOutputFormat>(buf, header, true, format_settings);
        });
    factory.markFormatHasNoAppendSupport("ArrowStream");
    factory.markOutputFormatPrefersLargeBlocks("ArrowStream");
    factory.markOutputFormatNotTTYFriendly("ArrowStream");
    factory.setContentType("ArrowStream", "application/octet-stream");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatArrow(FormatFactory &);
void registerOutputFormatArrow(FormatFactory &)
{
}
}

#endif
