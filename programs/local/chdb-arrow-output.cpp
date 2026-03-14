#include "chdb.h"

#include <cstring>

#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

namespace
{

/// Wrapper that ties the lifetime of a chdb_result to a shared_ptr so that
/// the Arrow BufferReader (which does not own the data) cannot outlive the
/// underlying byte buffer.
struct ResultGuard
{
    chdb_result * result;
    explicit ResultGuard(chdb_result * r) : result(r) {}
    ~ResultGuard()
    {
        if (result)
            chdb_destroy_query_result(result);
    }
};

} // anonymous namespace

chdb_state chdb_query_arrow_stream(
    chdb_connection conn, const char * query,
    chdb_arrow_stream out_stream)
{
    return chdb_query_arrow_stream_n(
        conn, query, query ? std::strlen(query) : 0, out_stream);
}

chdb_state chdb_query_arrow_stream_n(
    chdb_connection conn, const char * query, size_t query_len,
    chdb_arrow_stream out_stream)
{
    if (!conn || !out_stream)
        return CHDBError;

    // 1. Execute query with ArrowStream format
    static const char format[] = "ArrowStream";
    chdb_result * result = chdb_query_n(conn, query, query_len, format, sizeof(format) - 1);
    if (!result)
        return CHDBError;

    const char * err = chdb_result_error(result);
    if (err)
    {
        chdb_destroy_query_result(result);
        return CHDBError;
    }

    char * buf = chdb_result_buffer(result);
    size_t len = chdb_result_length(result);

    // Empty result (0 rows) still needs a valid stream — the IPC bytes may
    // contain just the schema message.  If there are truly no bytes at all
    // we still try; the IPC reader will produce an empty stream.
    if (!buf || !len)
    {
        buf = nullptr;
        len = 0;
    }

    // 2. Keep the result alive via a shared guard
    auto guard = std::make_shared<ResultGuard>(result);

    // 3. Wrap the bytes in an Arrow Buffer (zero-copy, non-owning) and then
    //    create a BufferReader.  The guard captured by the custom destructor
    //    ensures the underlying memory stays valid.
    auto arrow_buf = buf
        ? std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(buf), static_cast<int64_t>(len))
        : std::make_shared<arrow::Buffer>(nullptr, 0);
    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buf);

    // 4. Open an IPC RecordBatchStreamReader
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
    if (!reader_result.ok())
        return CHDBError;

    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> ipc_reader = *reader_result;

    // 5. Wrap the IPC reader together with the guard so that the chdb_result
    //    stays alive as long as the exported ArrowArrayStream is alive.
    //    We do this by capturing guard in a shared_ptr custom destructor that
    //    is itself captured inside a shared_ptr<RecordBatchReader> wrapper.
    //    Since ExportRecordBatchReader takes shared_ptr<RecordBatchReader> and
    //    the exported stream internally ref-counts it, attaching the guard
    //    to the same shared_ptr ensures correct lifetime.
    std::shared_ptr<arrow::RecordBatchReader> reader(
        ipc_reader.get(),
        [ipc_reader, guard, buffer_reader, arrow_buf](arrow::RecordBatchReader *) {
            // prevent premature destruction; captured shared_ptrs are destroyed
            // when this deleter is destroyed (i.e. when the ArrowArrayStream is
            // released).
            (void)ipc_reader;
            (void)guard;
            (void)buffer_reader;
            (void)arrow_buf;
        });

    // 6. Export to the caller's ArrowArrayStream
    auto * stream = reinterpret_cast<ArrowArrayStream *>(out_stream);
    auto status = arrow::ExportRecordBatchReader(std::move(reader), stream);
    if (!status.ok())
        return CHDBError;

    return CHDBSuccess;
}
