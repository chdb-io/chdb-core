#pragma once

#include "PybindWrapper.h"

#include <Storages/ColumnsDescription.h>
#include <DataTypes/IDataType.h>

namespace CHDB
{

enum class PyArrowObjectType
{
    Invalid,
    Table,
    RecordBatch,
    Dataset
};

class PyArrowTable
{
public:
    static DB::ColumnsDescription getActualTableStructure(const py::object & object, DB::ContextPtr & context);

    /// True for every PyArrow object the Python() table engine scans natively:
    /// pyarrow.Table, pyarrow.RecordBatch and pyarrow.dataset.Dataset.
    static bool isPyArrowObject(const py::object & object);

    static PyArrowObjectType getArrowType(const py::object & object);
};

} // namespace CHDB
