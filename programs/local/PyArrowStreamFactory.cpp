#include "PyArrowStreamFactory.h"
#include "PyArrowTable.h"
#include "PythonImporter.h"

#include <unordered_set>
#include <memory>

#include <Common/Exception.h>
#include <pybind11/pybind11.h>
#include <pybind11/gil.h>

namespace DB
{

namespace ErrorCodes
{
extern const int PY_EXCEPTION_OCCURED;
}

}

using namespace DB;
namespace py = pybind11;

namespace CHDB
{

std::unique_ptr<ArrowArrayStreamWrapper> PyArrowStreamFactory::createFromPyObject(
    py::object & py_obj,
    const Names & column_names)
{
    chassert(py::gil_check());

    try
    {
        auto arrow_object_type = PyArrowTable::getArrowType(py_obj);

        switch (arrow_object_type)
        {
            case PyArrowObjectType::Table:
            case PyArrowObjectType::RecordBatch:
            {
                /// pyarrow.dataset.dataset() wraps an in-memory Table or
                /// RecordBatch into an InMemoryDataset, so both share the
                /// dataset scan below and its projection pushdown.
                auto & import_cache = PythonImporter::ImportCache();
                auto dataset = import_cache.pyarrow.dataset().attr("dataset")(py_obj);
                return createFromDataset(dataset, column_names);
            }
            case PyArrowObjectType::Dataset:
                /// Already a Dataset: pyarrow.dataset.dataset() rejects a single
                /// Dataset instance (it only accepts a *list* of them), so scan
                /// it as is.
                return createFromDataset(py_obj, column_names);
            default:
                throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED,
                    "Unsupported PyArrow object type: {}", arrow_object_type);
        }
    }
    catch (const py::error_already_set & e)
    {
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED,
                        "Failed to convert PyArrow object to arrow array stream: {}", e.what());
    }
}

std::unique_ptr<ArrowArrayStreamWrapper> PyArrowStreamFactory::createFromDataset(
    py::object & dataset,
    const Names & column_names)
{
    chassert(py::gil_check());

	/// Unbound Dataset.scanner: the concrete class varies with the dataset kind
	/// (InMemoryDataset, FileSystemDataset, UnionDataset, ...).
	py::object arrow_scanner = dataset.attr("__class__").attr("scanner");

	py::dict kwargs;
	if (!column_names.empty()) {
        ArrowSchemaWrapper schema;
        auto obj_schema = dataset.attr("schema");
        auto export_to_c = obj_schema.attr("_export_to_c");
        export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));

        /// Get available column names from schema
        std::unordered_set<std::string> available_columns;
        if (schema.arrow_schema.n_children > 0 && schema.arrow_schema.children)
        {
            for (int64_t i = 0; i < schema.arrow_schema.n_children; ++i)
            {
                if (schema.arrow_schema.children[i] && schema.arrow_schema.children[i]->name)
                {
                    available_columns.insert(schema.arrow_schema.children[i]->name);
                }
            }
        }

        /// Only add column names that exist in the schema
        py::list projection_list;
        for (const auto & name : column_names)
        {
            if (available_columns.contains(name))
            {
                projection_list.append(name);
            }
        }

        /// Only set columns if we have valid projections
        if (projection_list.size() > 0)
        {
            kwargs["columns"] = projection_list;
        }
	}

	auto scanner = arrow_scanner(dataset, **kwargs);

    auto record_batches = scanner.attr("to_reader")();
	auto res = std::make_unique<ArrowArrayStreamWrapper>();
	auto export_to_c = record_batches.attr("_export_to_c");
	export_to_c(reinterpret_cast<uint64_t>(&res->arrow_array_stream));
	return res;
}

} // namespace CHDB
