#pragma once

#include "PybindWrapper.h"

namespace CHDB
{

/// Share the Python exit gate with one-shot queries and global UDF calls.
class PythonRuntimeOperationGuard
{
public:
    PythonRuntimeOperationGuard();
    ~PythonRuntimeOperationGuard();
    PythonRuntimeOperationGuard(const PythonRuntimeOperationGuard &) = delete;
    PythonRuntimeOperationGuard & operator=(const PythonRuntimeOperationGuard &) = delete;
};

void registerGlobalFunctions(py::module_ & m);

} // namespace CHDB
