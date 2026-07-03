#pragma once

#include <IO/ReadBuffer.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Core/Block.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/IAST_fwd.h>

#include <atomic>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace DB
{
class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;
}

namespace CHDB
{

/// A ReadBuffer whose data is pushed in by the caller (chdb_stream_append) and
/// consumed by the engine's INSERT input-format parser running on a background
/// worker thread (see ChdbClient::executeInsertStreaming*).
///
/// The queue is bounded, so append() blocks (applies backpressure) when the
/// engine falls behind the producer. finish() marks end-of-input: once the
/// queue drains, nextImpl() returns false and the input format sees EOF.
///
/// Threading: append()/finish() run on the caller thread; nextImpl() runs on
/// the worker thread. ConcurrentBoundedQueue provides all the synchronization.
class QueueReadBuffer : public DB::ReadBuffer
{
public:
    explicit QueueReadBuffer(size_t max_chunks = 16)
        : DB::ReadBuffer(nullptr, 0), queue(max_chunks)
    {
    }

    /// Caller thread: copy `len` bytes and enqueue them. The C ABI contract is
    /// that the caller owns `data`, so we must copy. Returns false if the
    /// stream was already finished/cancelled (queue closed).
    bool append(const char * data, size_t len)
    {
        if (len == 0)
            return true;
        return queue.push(std::string(data, len));
    }

    /// Caller thread: signal end-of-input. The worker's nextImpl() returns
    /// false once the already-queued chunks have been consumed.
    void finish() { queue.finish(); }

private:
    bool nextImpl() override
    {
        std::string chunk;
        /// Skip empty chunks defensively; stop at EOF (queue finished + drained).
        do
        {
            if (!queue.pop(chunk))
                return false;
        } while (chunk.empty());

        current_chunk = std::move(chunk);
        working_buffer = Buffer(current_chunk.data(), current_chunk.data() + current_chunk.size());
        return true;
    }

    ConcurrentBoundedQueue<std::string> queue;
    std::string current_chunk;
};

/// Per-stream state for a streaming INSERT. Owned (via shared_ptr) by both the
/// InsertStreamResult handle returned to the C layer and the worker thread.
///
/// The worker thread runs the INSERT pipeline: it pulls parsed blocks from an
/// input format reading `queue_buf` and pushes them into the engine via
/// LocalConnection::sendData, then finalizes. The caller-facing append()/done()/
/// cancel() feed/close `queue_buf` and observe `error`/`worker_done`.
struct InsertStreamContext
{
    std::shared_ptr<QueueReadBuffer> queue_buf;
    DB::ASTPtr parsed_query;
    std::string full_query;
    DB::Block sample;
    DB::ColumnsDescription columns;
    std::string format;

    DB::ThreadGroupPtr thread_group;
    ThreadFromGlobalPool worker;

    /// Handshake so executeInsertStreamingInit() can report setup errors
    /// (bad SQL, missing table) synchronously: the worker runs sendQuery +
    /// receiveSampleBlock, then signals "" on success or the message on failure.
    std::promise<std::string> init_promise;
    bool init_signaled = false;

    std::atomic<bool> cancelled{false};
    std::atomic<bool> worker_done{false};
    /// Set once done() or cancel() has joined the worker; makes a subsequent
    /// cancel()/destroy() a no-op so we never sendCancel() a finished query.
    std::atomic<bool> finalized{false};

    /// Set by the worker thread on failure; surfaced via chdb_stream_insert_error.
    /// `error`/`error_message` are non-atomic and written only by the worker; a
    /// reader (append on the caller thread) must observe `error_set` (acquire)
    /// before touching them, establishing a happens-before edge with the worker's
    /// release store. done()/cancel() read them only after joining the worker.
    std::atomic<bool> error_set{false};
    std::exception_ptr error;
    std::string error_message;

    /// Engine write-progress accumulated for this INSERT (filled in done()).
    uint64_t rows_written = 0;
    uint64_t bytes_written = 0;
    double elapsed = 0.0;

};

using InsertStreamContextPtr = std::shared_ptr<InsertStreamContext>;

}
