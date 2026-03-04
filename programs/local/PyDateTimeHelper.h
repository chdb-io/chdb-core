#pragma once

#include "PybindWrapper.h"

namespace CHDB
{

class PyDateTimeHelper
{
public:
    static void initialize();

    static int32_t daysSinceEpoch(const py::handle & obj);
};

} // namespace CHDB
