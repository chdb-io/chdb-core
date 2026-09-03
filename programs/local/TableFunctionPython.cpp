#include "TableFunctionPython.h"
#include "StoragePython.h"
#include "PandasDataFrame.h"
#include "PyArrowTable.h"
#include "PythonArrowStream.h"
#include "PythonDict.h"
#include "PythonReader.h"
#include "PythonTableCache.h"
#include "PythonUtils.h"

#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>
#include <Common/logger_useful.h>

namespace py = pybind11;

using namespace CHDB;

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_python_table_function;
}

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int PY_OBJECT_NOT_FOUND;
extern const int PY_EXCEPTION_OCCURED;
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_FORMAT;
extern const int FUNCTION_NOT_ALLOWED;
}

void TableFunctionPython::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::allow_python_table_function])
        throw Exception(
            ErrorCodes::FUNCTION_NOT_ALLOWED, "Python table function is disabled, because setting 'allow_python_table_function' is set to 0");

    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'python' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Python table requires 1 argument: PyReader object");

    auto py_reader_arg = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);

    try
    {
        // get the py_reader_arg without quotes
        auto py_reader_arg_str = py_reader_arg->as<ASTLiteral &>().value.safeGet<String>();
        LOG_DEBUG(logger, "Python object name: {}", py_reader_arg_str);

        // strip all quotes like '"` if any. eg. 'PyReader' -> PyReader, "PyReader" -> PyReader
        py_reader_arg_str.erase(
            std::remove_if(py_reader_arg_str.begin(), py_reader_arg_str.end(), [](char c) { return c == '\'' || c == '\"' || c == '`'; }),
            py_reader_arg_str.end());

        py::gil_scoped_acquire acquire;
        auto instance = context->getQueryContext()->getPythonTableCache()->getQueryableObj(py_reader_arg_str);
        if (instance == nullptr || instance.is_none())
            throw Exception(ErrorCodes::PY_OBJECT_NOT_FOUND,
                            "Python object not found in the Python environment\n"
                            "Ensure that the object is type of PyReader, pandas DataFrame, or PyArrow Table and is in the global or local scope");

        LOG_DEBUG(
            logger,
            "Python object found in Python environment with name: {} type: {}",
            py_reader_arg_str,
            py::str(instance.attr("__class__")).cast<std::string>());

        auto reader = instance.cast<py::object>();
        data_source_wrapper = std::make_shared<DataSourceWrapper>(reader);
        is_pandas_df = PandasDataFrame::isPandasDataframe(reader);
    }
    catch (py::error_already_set & e)
    {
        throw Exception(ErrorCodes::PY_EXCEPTION_OCCURED, "Python exception occurred: {}", e.what());
    }
}

StoragePtr TableFunctionPython::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    if (!data_source_wrapper)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python data source not initialized");

    auto columns = getActualTableStructure(context, is_insert_query);

    std::shared_ptr<StoragePython> storage;
    {
        py::gil_scoped_acquire acquire;
        storage = std::make_shared<StoragePython>(
            StorageID(getDatabaseName(), table_name), columns,
            ConstraintsDescription{}, context, is_pandas_df, std::move(data_source_wrapper));
    }
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionPython::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    py::gil_scoped_acquire acquire;

    if (!data_source_wrapper)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python data source not initialized");

    const auto & reader = data_source_wrapper->getDataSource();

    if (is_pandas_df)
    {
        auto * table_cache = context->getQueryContext() ? context->getQueryContext()->getPythonTableCache() : nullptr;
        if (table_cache)
        {
            if (auto meta = table_cache->findValidatedPandasMeta(reader))
            {
                static const bool strict = []
                {
                    const char * env = getenv("CHDB_PANDAS_META_CACHE_STRICT"); // NOLINT(concurrency-mt-unsafe)
                    return env && env[0] == '1';
                }();
                if (strict)
                {
                    auto fresh = PandasDataFrame::getActualTableStructure(*data_source_wrapper, context);
                    if (fresh.toString(false) != meta->schema.toString(false))
                    {
                        LOG_ERROR(
                            logger,
                            "Pandas metadata cache strict check failed, cached schema differs from fresh inference:\n{}\nvs\n{}",
                            meta->schema.toString(false),
                            fresh.toString(false));
                        table_cache->invalidatePandasMeta(reader.ptr());
                        return fresh;
                    }
                }
                return meta->schema;
            }
        }
        auto columns = PandasDataFrame::getActualTableStructure(*data_source_wrapper, context);
        if (table_cache)
            table_cache->storePandasMeta(reader, columns);
        return columns;
    }

    if (PyArrowTable::isPyArrowTable(reader))
        return PyArrowTable::getActualTableStructure(reader, context);

    if (PythonDict::isPythonDict(reader))
        return PythonDict::getActualTableStructure(reader, context);

    if (PythonReader::isPythonReader(reader))
        return PythonReader::getActualTableStructure(reader, context);

    /// Generic Arrow PyCapsule stream protocol (polars DataFrame,
    /// pyarrow.RecordBatchReader, chdb/duckdb results, ...). Checked after the
    /// specialized paths above so pandas / pyarrow.Table keep their optimized
    /// scans (projection & predicate pushdown). The imported stream is parked
    /// on the wrapper and reused by the scan, so the producer's
    /// __arrow_c_stream__ is called only once per query lifecycle -- one-shot
    /// producers cannot export a second stream.
    if (hasArrowCStreamMethod(reader))
    {
        if (!data_source_wrapper->peekCachedArrowStream())
            data_source_wrapper->cacheArrowStream(importArrowCStream(reader));
        return tableStructureFromArrowStream(*data_source_wrapper->peekCachedArrowStream(), context);
    }

    auto schema = PyReader::getSchemaFromPyObj(reader);
    return StoragePython::getTableStructureFromData(schema, context);
}

void registerTableFunctionPython(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPython>(
        {
            .description = R"(
Passing Pandas DataFrame or Pyarrow Table to ClickHouse engine.
For any other data structure, you can also create a table interface to a Python data source and reads data
from a PyReader object.
This table function requires a single argument which is a PyReader object used to read data from Python.
)",
            .examples = {{"python", "SELECT * FROM Python(PyReader)", ""}},
            .category = FunctionDocumentation::Category::TableFunction,
        },
        {},
        TableFunctionFactory::Case::Insensitive);
}

}
