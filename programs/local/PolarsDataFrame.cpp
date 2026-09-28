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

    const bool is_lazy = isInstanceOfLoadedPolarsClass(object, cache.LazyFrame);
    const bool is_series = !is_lazy && isInstanceOfLoadedPolarsClass(object, cache.Series);
    if (!is_lazy && !is_series && !isInstanceOfLoadedPolarsClass(object, cache.DataFrame))
        return object;

    /// The scan reads polars through the Arrow PyCapsule interface, which polars
    /// exports since 1.3.0. Checked before collect() so that an older polars
    /// fails fast with the real reason, rather than after running the plan, in
    /// the duck-typed fallback, with an unrelated TypeError.
    auto frame_class = cache.DataFrame();
    if (frame_class.ptr() && !py::hasattr(frame_class, "__arrow_c_stream__"))
    {
        auto version = py::str(py::getattr(cache(), "__version__", py::str("unknown"))).cast<std::string>();
        throw py::import_error(
            "Querying Polars objects requires polars>=1.3.0, found " + version
            + ": earlier releases do not export the Arrow PyCapsule interface");
    }

    if (is_lazy)
        return object.attr("collect")();

    if (is_series)
        return object.attr("to_frame")();

    return object;
}

} // namespace CHDB
