#pragma once

#include "PybindWrapper.h"

#include <DataTypes/IDataType.h>

namespace CHDB
{

class ChdbPyType
{
public:
    explicit ChdbPyType(const DB::DataTypePtr & type_);
    explicit ChdbPyType(const String & type_name);

    const DB::DataTypePtr & dataType() const { return type; }
    String name() const { return type->getName(); }

    static void initialize(py::module_ & parent);

private:
    DB::DataTypePtr type;
};

} // namespace CHDB
