#include "PythonArrowStream.h"
#include "ArrowSchema.h"
#include "PybindWrapper.h"

#include <cstring>

#include <Common/Exception.h>
#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <pybind11/gil.h>

#if USE_JEMALLOC
#include <Common/memory.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int PY_EXCEPTION_OCCURED;
extern const int BAD_ARGUMENTS;
}

}

using namespace DB;

namespace CHDB
{

static constexpr const char * kArrowStreamCapsuleName = "arrow_array_stream";

bool hasArrowCStreamMethod(const py::handle & obj)
{
    chassert(py::gil_check());
    return py::hasattr(obj, "__arrow_c_stream__");
}

std::unique_ptr<ArrowArrayStreamWrapper> importArrowCStream(const py::object & obj)
{
    chassert(py::gil_check());

    try
    {
        py::object capsule = obj.attr("__arrow_c_stream__")();
        if (!PyCapsule_IsValid(capsule.ptr(), kArrowStreamCapsuleName))
            throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED,
                "__arrow_c_stream__ did not return an 'arrow_array_stream' PyCapsule");

        auto * src = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(capsule.ptr(), kArrowStreamCapsuleName));
        if (!src || !src->release)
            throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED,
                "__arrow_c_stream__ returned a capsule with an already-released ArrowArrayStream");

        /// Move the stream out of the capsule (Arrow C Data Interface move
        /// convention): the capsule keeps a released husk, its destructor no-ops.
        auto res = std::make_unique<ArrowArrayStreamWrapper>();
        res->arrow_array_stream = *src;
        src->release = nullptr;
        return res;
    }
    catch (const py::error_already_set & e)
    {
#if USE_JEMALLOC
        ::Memory::MemoryCheckScope memory_check_scope;
#endif
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED,
            "Failed to import Arrow stream from Python object via __arrow_c_stream__: {}", e.what());
    }
}

ColumnsDescription getTableStructureFromArrowCStream(const py::object & obj, ContextPtr & context)
{
    auto stream = importArrowCStream(obj);

    ArrowSchemaWrapper schema;
    stream->getSchema(schema);

    NamesAndTypesList names_and_types;
    ArrowSchemaWrapper::convertArrowSchema(schema, names_and_types, context);
    return ColumnsDescription(names_and_types);
}

namespace
{

/// arrow::Buffer view over externally-owned memory; `owner` keeps the memory
/// alive for as long as any exported record batch references it.
class KeepaliveBuffer : public arrow::Buffer
{
public:
    KeepaliveBuffer(const uint8_t * data, int64_t size, std::shared_ptr<void> owner_)
        : arrow::Buffer(data, size), owner(std::move(owner_))
    {
    }

private:
    std::shared_ptr<void> owner;
};

void arrowStreamCapsuleDestructor(PyObject * capsule)
{
    auto * stream = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(capsule, kArrowStreamCapsuleName));
    if (stream)
    {
        /// Only release if the consumer did not take ownership already.
        if (stream->release)
            stream->release(stream);
        delete stream;
    }
}

}

py::object exportArrowIPCAsCapsule(const char * data, size_t size, std::shared_ptr<void> keepalive)
{
    chassert(py::gil_check());

    if (!data || size == 0)
        throw py::value_error(
            "result has no Arrow payload; run the query with output format \"Arrow\" (or \"ArrowStream\")");

    auto buffer = std::make_shared<KeepaliveBuffer>(
        reinterpret_cast<const uint8_t *>(data), static_cast<int64_t>(size), std::move(keepalive));

    static constexpr char kFileMagic[6] = {'A', 'R', 'R', 'O', 'W', '1'};
    std::shared_ptr<arrow::RecordBatchReader> reader;
    if (size >= 8 && std::memcmp(data, kFileMagic, sizeof(kFileMagic)) == 0)
    {
        /// Arrow IPC file format (output format "Arrow").
        auto file_reader_result = arrow::ipc::RecordBatchFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
        if (!file_reader_result.ok())
            throw py::value_error("failed to open Arrow IPC file from result buffer: " + file_reader_result.status().ToString());
        auto file_reader = std::move(file_reader_result).ValueOrDie();

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        batches.reserve(file_reader->num_record_batches());
        for (int i = 0; i < file_reader->num_record_batches(); ++i)
        {
            auto batch_result = file_reader->ReadRecordBatch(i);
            if (!batch_result.ok())
                throw py::value_error("failed to read Arrow record batch from result buffer: " + batch_result.status().ToString());
            batches.push_back(std::move(batch_result).ValueOrDie());
        }

        auto reader_result = arrow::RecordBatchReader::Make(std::move(batches), file_reader->schema());
        if (!reader_result.ok())
            throw py::value_error("failed to create Arrow record batch reader: " + reader_result.status().ToString());
        reader = std::move(reader_result).ValueOrDie();
    }
    else
    {
        /// Arrow IPC stream format (output format "ArrowStream").
        auto stream_reader_result = arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
        if (!stream_reader_result.ok())
            throw py::value_error(
                "result buffer is not Arrow IPC data; run the query with output format \"Arrow\" (or \"ArrowStream\"): "
                + stream_reader_result.status().ToString());
        reader = std::move(stream_reader_result).ValueOrDie();
    }

    auto stream = std::make_unique<ArrowArrayStream>();
    auto status = arrow::ExportRecordBatchReader(std::move(reader), stream.get());
    if (!status.ok())
        throw py::value_error("failed to export Arrow record batch reader: " + status.ToString());

    PyObject * capsule = PyCapsule_New(stream.get(), kArrowStreamCapsuleName, arrowStreamCapsuleDestructor);
    if (!capsule)
    {
        stream->release(stream.get());
        throw py::error_already_set();
    }
    stream.release();
    return py::reinterpret_steal<py::object>(capsule);
}

} // namespace CHDB
