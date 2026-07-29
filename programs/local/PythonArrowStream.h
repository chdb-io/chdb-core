#pragma once

#include "ArrowStreamWrapper.h"

#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace CHDB
{

/// Arrow PyCapsule interface support (both directions).
/// https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

/// True if the object exposes the Arrow PyCapsule stream protocol
/// (has an __arrow_c_stream__ method). GIL must be held.
bool hasArrowCStreamMethod(const py::handle & obj);

/// Call obj.__arrow_c_stream__() and take ownership of the ArrowArrayStream
/// carried by the returned "arrow_array_stream" PyCapsule. GIL must be held.
std::unique_ptr<ArrowArrayStreamWrapper> importArrowCStream(const py::object & obj);

/// Derive the table structure from the stream schema. get_schema is idempotent
/// and consumes no batches, so this may be called repeatedly on a stream that
/// is later handed to the scan.
DB::ColumnsDescription tableStructureFromArrowStream(ArrowArrayStreamWrapper & stream, DB::ContextPtr & context);

/// Export a buffer holding Arrow IPC data (file or stream format) as a
/// PyCapsule("arrow_array_stream"). The record batches reference the buffer
/// directly (no data copy); `keepalive` is retained until the exported stream
/// is released by the consumer. GIL must be held.
py::object exportArrowIPCAsCapsule(const char * data, size_t size, std::shared_ptr<void> keepalive);

} // namespace CHDB
