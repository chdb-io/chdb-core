#include "ChdbPyType.h"

#include <pybind11/stl.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace CHDB
{

ChdbPyType::ChdbPyType(const DB::DataTypePtr & type_) : type(type_) {}

ChdbPyType::ChdbPyType(const String & type_name)
    : type(DB::DataTypeFactory::instance().get(type_name))
{
}

static void defineBaseTypes(py::module_ & m)
{
    m.attr("BOOL") = std::make_shared<ChdbPyType>(DataTypeFactory::instance().get("Bool"));

    m.attr("INT8") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeInt8>());
    m.attr("INT16") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeInt16>());
    m.attr("INT32") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeInt32>());
    m.attr("INT64") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeInt64>());
    m.attr("INT128") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeInt128>());
    m.attr("INT256") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeInt256>());
    m.attr("UINT8") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeUInt8>());
    m.attr("UINT16") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeUInt16>());
    m.attr("UINT32") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeUInt32>());
    m.attr("UINT64") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeUInt64>());
    m.attr("UINT128") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeUInt128>());
    m.attr("UINT256") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeUInt256>());

    m.attr("FLOAT32") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeFloat32>());
    m.attr("FLOAT64") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeFloat64>());

    m.attr("STRING") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeString>());

    m.attr("DATE") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeDate>());
    m.attr("DATE32") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeDate32>());

    m.attr("DATETIME") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeDateTime>());
    m.attr("DATETIME64") = std::make_shared<ChdbPyType>(std::make_shared<DB::DataTypeDateTime64>(3));
}

void ChdbPyType::initialize(py::module_ & parent)
{
    auto m = parent.def_submodule("_sqltypes", "ClickHouse SQL type definitions for chDB");

    py::class_<ChdbPyType, std::shared_ptr<ChdbPyType>>(m, "ChdbType", py::module_local())
        .def(py::init<const String &>(), py::arg("name"))
        .def("__repr__", [](const ChdbPyType & self) {
            return "chdb.sqltypes.ChdbType('" + self.name() + "')";
        })
        .def("__str__", &ChdbPyType::name)
        .def("__eq__", [](const ChdbPyType & self, const std::shared_ptr<ChdbPyType> & other) {
            return other && self.dataType()->equals(*other->dataType());
        }, py::arg("other"), py::is_operator())
        .def("__eq__", [](const ChdbPyType & self, const std::string & s) {
            return self.dataType()->getName() == s;
        }, py::arg("other"), py::is_operator())
        .def("__hash__", [](const ChdbPyType & self) {
            return std::hash<std::string>{}(self.name());
        })
        .def_property_readonly("name", &ChdbPyType::name);

    /// py::implicitly_convertible<std::string, ChdbPyType>();
    /// py::implicitly_convertible<py::type, ChdbPyType>();

    defineBaseTypes(m);
}

} // namespace CHDB
