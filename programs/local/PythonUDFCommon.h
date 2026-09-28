#pragma once

#include "PybindWrapper.h"

#include <Core/Types.h>
#include <DataTypes/IDataType.h>

#include <vector>


namespace CHDB
{

/// How a Python UDF/UDAF sees SQL NULL inputs.
enum class NullHandling : uint8_t
{
    /// Rows with a NULL argument never reach Python.
    SKIP,
    /// NULL is converted to Python None and handed to the function.
    PASS,
};

/// What happens when the Python callable raises.
enum class ExceptionHandling : uint8_t
{
    PROPAGATE,
    IGNORE,
};

/// If the annotation is `Optional[X]` / `Union[X, None]` (or PEP 604 `X | None`),
/// returns the inner base type `X`; any other annotation is returned unchanged.
/// A union with more than one non-None member (e.g. `Union[int, str]`) is rejected.
py::object unwrapOptionalAnnotation(const py::object & annotation);

/// Resolve a user-supplied type spelling - a ChdbType instance, a type-name string or a
/// Python type (optionally wrapped in Optional[...]) - to a ClickHouse data type.
DB::DataTypePtr annotationToDataType(const py::object & annotation);

/// True when the annotation is the builtin `bytes` or `bytearray` type. Both map
/// to ClickHouse String, but an argument declared this way should receive a raw
/// Python `bytes` value rather than a UTF-8-decoded `str`.
bool isBytesLikeAnnotation(const py::object & annotation);

/// The ClickHouse types a Python UDF/UDAF may declare for its arguments and result.
bool isSupportedUDFType(DB::TypeIndex type_id);

/// `kind` prefixes the error messages, e.g. "Python UDF" / "Python UDAF".
/// Entries of `arg_types` may be null, meaning "not declared".
void validateUDFTypes(
    const String & kind,
    const String & name,
    const DB::DataTypePtr & return_type,
    const DB::DataTypes & arg_types);

struct UDFParameter
{
    String name;
    /// A null py::object when the parameter carries no usable annotation
    /// (missing, or spelled as `None`).
    py::object annotation;
};

struct UDFSignature
{
    std::vector<UDFParameter> positional;
    bool has_varargs = false;
    /// A null py::object when the callable has no usable return annotation.
    py::object return_annotation;
};

/// Inspect a Python callable and return its positional parameters, rejecting the
/// parameter kinds a UDF cannot express (keyword-only, **kwargs, anything after *args).
/// `skip_leading` drops that many leading parameters, which is how the bound `self` of
/// an accumulator method is ignored. `kind` prefixes error messages.
UDFSignature inspectUDFSignature(
    const String & kind,
    const String & name,
    const py::object & callable,
    size_t skip_leading = 0);

} // namespace CHDB
