#include "PyArrowTable.h"
#include "ArrowSchema.h"
#include "PyArrowCacheItem.h"
#include "PythonImporter.h"

#include <Interpreters/Context.h>

using namespace DB;

namespace CHDB
{

namespace
{

/// PythonImportCacheItem publishes a *null* handle when the attribute is absent
/// from the module (a stub/shadowing `pyarrow` on sys.path, a build without a
/// given class). py::isinstance would then hand PyObject_IsInstance a null type
/// and crash, so treat a missing class as "object is not of this type".
bool isInstanceOfCached(const py::object & obj, py::handle type)
{
    return type.ptr() != nullptr && py::isinstance(obj, type);
}

}

PyArrowObjectType PyArrowTable::getArrowType(const py::object & obj)
{
    chassert(py::gil_check());

	if (!ModuleIsLoaded<PyarrowCacheItem>())
		return PyArrowObjectType::Invalid;

	auto & import_cache = PythonImporter::ImportCache();

	if (isInstanceOfCached(obj, import_cache.pyarrow.table()))
		return PyArrowObjectType::Table;

	if (isInstanceOfCached(obj, import_cache.pyarrow.record_batch()))
		return PyArrowObjectType::RecordBatch;

	/// pyarrow.dataset is a separate submodule and importing it is not free, so
	/// only probe it when the process has already imported it: a Dataset
	/// instance cannot exist otherwise.
	if (ModuleIsLoaded<PyarrowDatasetCacheItem>()
		&& isInstanceOfCached(obj, import_cache.pyarrow.dataset.dataset()))
		return PyArrowObjectType::Dataset;

	return PyArrowObjectType::Invalid;
}

bool PyArrowTable::isPyArrowObject(const py::object & object)
{
    try
    {
        return getArrowType(object) != PyArrowObjectType::Invalid;
    }
    catch (const py::error_already_set &)
    {
        return false;
    }
}

ColumnsDescription PyArrowTable::getActualTableStructure(const py::object & object, ContextPtr & context)
{
    chassert(py::gil_check());
    chassert(isPyArrowObject(object));

    NamesAndTypesList names_and_types;

    auto obj_schema = object.attr("schema");
	auto export_to_c = obj_schema.attr("_export_to_c");
	ArrowSchemaWrapper schema;
	export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));

    ArrowSchemaWrapper::convertArrowSchema(schema, names_and_types, context);

    return ColumnsDescription(names_and_types);
}

} // namespace CHDB
