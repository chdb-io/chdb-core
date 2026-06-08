#pragma once

#include "config.h"

#if USE_PYTHON

#include "QueryResult.h"

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace CHDB
{

/// pandas DataFrame-backed query result. Only built into USE_PYTHON wheels.
/// Kept in its own header so QueryResult.h stays pybind11-free and can be
/// included from non-Python translation units (e.g. chdb-arrow-output.cpp)
/// without dragging Python.h into them.
class DataFrameQueryResult : public QueryResult
{
public:
    explicit DataFrameQueryResult(py::handle dataframe_, uint64_t rows_read)
        : QueryResult(QueryResultType::RESULT_TYPE_DATAFRAME),
        dataframe(dataframe_),
        is_empty(rows_read == 0)
    {}

    bool isEmpty() const override { return is_empty; }

    py::handle dataframe;
    bool is_empty;
};

using DataFrameQueryResultPtr = std::unique_ptr<DataFrameQueryResult>;

} // namespace CHDB

#endif
