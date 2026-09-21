#include "PolarsDataFrame.h"

#include "PolarsCacheItem.h"
#include "PythonImporter.h"

namespace CHDB
{

namespace
{

/// isinstance against a polars class, given that polars is known to be loaded.
///
/// The caller checks that first: an object of a polars class cannot exist
/// while the module is absent from sys.modules, so a negative answer is
/// correct, and probing would otherwise import polars in every process that
/// never asked for it.
bool isInstanceOfLoadedPolarsClass(const py::object & object, PythonImportCacheItem & item)
{
    try
    {
        auto cls = item();
        if (!cls.ptr())
            return false;
        return py::isinstance(object, cls);
    }
    catch (const py::error_already_set &)
    {
        return false;
    }
}

}

bool PolarsDataFrame::isPolarsLazyFrame(const py::object & object)
{
    if (!ModuleIsLoaded<PolarsCacheItem>())
        return false;
    return isInstanceOfLoadedPolarsClass(object, PythonImporter::ImportCache().polars.LazyFrame);
}

bool PolarsDataFrame::isPolarsSeries(const py::object & object)
{
    if (!ModuleIsLoaded<PolarsCacheItem>())
        return false;
    return isInstanceOfLoadedPolarsClass(object, PythonImporter::ImportCache().polars.Series);
}

py::object PolarsDataFrame::normalize(const py::object & object)
{
    py::gil_assert();

    /// One sys.modules probe for the whole function: this runs for every
    /// Python(name) of every query, most of which have nothing to do with polars.
    if (!ModuleIsLoaded<PolarsCacheItem>())
        return object;

    auto & cache = PythonImporter::ImportCache().polars;

    if (isInstanceOfLoadedPolarsClass(object, cache.LazyFrame))
        return object.attr("collect")();

    if (isInstanceOfLoadedPolarsClass(object, cache.Series))
        return object.attr("to_frame")();

    return object;
}

} // namespace CHDB
