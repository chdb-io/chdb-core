#pragma once

#include "PybindWrapper.h"

namespace CHDB
{

/// polars objects the scan paths cannot read as they are.
///
/// A polars DataFrame already exports the Arrow C stream protocol, so it is
/// scanned by the generic Arrow path with no help from this file. Its two
/// siblings are not: a LazyFrame exports nothing (it has not run yet) and a
/// Series exports a bare array instead of a struct.
class PolarsDataFrame
{
public:
    static bool isPolarsLazyFrame(const py::object & object);

    static bool isPolarsSeries(const py::object & object);

    /// Return an equivalent object the scan paths do accept: a LazyFrame is
    /// collected, a Series becomes a one-column frame. Everything else --
    /// including a plain polars DataFrame -- is returned unchanged.
    ///
    /// Requires the GIL. A failure inside polars (a LazyFrame whose plan does
    /// not resolve, say) propagates as py::error_already_set; only the type
    /// checks are silent, so that a polars build without one of these classes
    /// leaves the object alone instead of failing the query.
    static py::object normalize(const py::object & object);
};

} // namespace CHDB
