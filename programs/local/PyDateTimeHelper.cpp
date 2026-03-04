#include "PyDateTimeHelper.h"

namespace CHDB
{

namespace
{

PyObject * attr_toordinal = nullptr;

/// date(1970, 1, 1).toordinal()
constexpr int32_t EPOCH_ORDINAL = 719163;

} // anonymous namespace

void PyDateTimeHelper::initialize()
{
    attr_toordinal = PyUnicode_InternFromString("toordinal");
}

int32_t PyDateTimeHelper::daysSinceEpoch(const py::handle & obj)
{
    PyObject * result = PyObject_CallMethodObjArgs(obj.ptr(), attr_toordinal, nullptr);
    int32_t ordinal = static_cast<int32_t>(PyLong_AsLong(result));
    Py_DECREF(result);
    return ordinal - EPOCH_ORDINAL;
}

} // namespace CHDB
