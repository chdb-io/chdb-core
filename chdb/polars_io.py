"""polars interop for chdb query results.

chdb results already expose the Arrow PyCapsule interface, so
``pl.DataFrame(result)`` works out of the box. This module packages that into
the two shapes DuckDB's Python API offers:

* :func:`to_polars` materializes a finished result as a ``pl.DataFrame``. It
  goes through ``query_result.__arrow_c_stream__``, which is implemented in
  C++, so it needs neither pyarrow nor pandas.
* :func:`chdb_source` returns a ``pl.LazyFrame`` backed by a polars IO plugin.
  polars hands column pruning, ``LIMIT`` and predicates to the plugin, which
  rewrites them into the SQL chdb runs. Anything that cannot be translated is
  applied in polars instead, so the result never depends on how much of the
  query we managed to push down.
"""

from __future__ import annotations

import datetime
import json

__all__ = ["to_polars", "chdb_source"]


def _require_polars(feature):
    try:
        import polars as pl
    except ImportError as e:
        raise ImportError(
            f'{feature} requires polars. Install it via "pip install polars".'
        ) from e
    return pl


def to_polars(res):
    """Convert a materialized chdb query result to a ``pl.DataFrame``.

    The result must have been produced with an Arrow output format ("Arrow" or
    "ArrowStream"); the record batches are imported straight from the result
    buffer through the Arrow PyCapsule interface, without a copy and without
    pyarrow.

    Args:
        res: Query result object carrying an Arrow payload.

    Returns:
        pl.DataFrame: the query results. Statements that produce no result set
        at all (DDL, INSERT, ...) give an empty DataFrame with no columns.

    Raises:
        ImportError: If polars is not installed.
        ValueError: If the result was produced with a non-Arrow output format.

    Examples:
        >>> import chdb
        >>> chdb.query("SELECT 1 AS a, 'x' AS b", "polars")
        shape: (1, 2)
        ...
    """
    pl = _require_polars("polars output format")
    if len(res) == 0:
        # No Arrow payload at all (DDL, INSERT, ...); __arrow_c_stream__ would
        # raise. An empty frame matches what to_arrowTable() returns.
        return pl.DataFrame()
    return pl.DataFrame(res)


def chdb_source(conn, sql, params=None):
    """Build a ``pl.LazyFrame`` that runs ``sql`` on ``conn`` when collected.

    The returned LazyFrame is a polars IO plugin source: on ``collect()``
    polars passes down the columns it needs, a row limit and a predicate, and
    this source folds them into the SQL it sends to chdb. A predicate that
    cannot be expressed in SQL is applied in polars after the fact, so
    pushdown is an optimization only, never a change in results.

    Args:
        conn: A :class:`chdb.state.sqlitelike.Connection` (anything exposing
            ``query``/``send_query``).
        sql (str): A single SELECT statement. It is wrapped in a subquery, so
            it must not carry a trailing ``FORMAT`` clause.
        params (dict, optional): Named query parameters, forwarded to every
            statement this source runs.

    Returns:
        pl.LazyFrame: lazy view over the query.
    """
    pl = _require_polars("lazy polars output")
    from polars.io.plugins import register_io_source

    base = sql.strip().rstrip(";").strip()
    cached_schema = {}

    def resolve_schema():
        if "schema" not in cached_schema:
            empty = conn.query(f"SELECT * FROM ({base}) LIMIT 0", "polars", params=params)
            cached_schema["schema"] = empty.schema
        return cached_schema["schema"]

    def source_generator(with_columns, predicate, n_rows, batch_size):
        query, fallback = _plan_query(base, with_columns, predicate, n_rows, batch_size)

        remaining = n_rows
        produced = False
        stream = conn.send_query(query, "Arrow", params=params)
        try:
            for chunk in stream:
                frame = pl.DataFrame(chunk)
                if fallback is not None:
                    frame = frame.filter(fallback)
                    if with_columns is not None:
                        frame = frame.select(with_columns)
                if remaining is not None:
                    if frame.height > remaining:
                        frame = frame.head(remaining)
                    remaining -= frame.height
                produced = True
                yield frame
                if remaining is not None and remaining <= 0:
                    break
        finally:
            stream.close()

        if not produced:
            # An all-empty result yields no chunk at all, but polars still
            # needs one frame to learn the shape of the (empty) answer.
            empty = pl.DataFrame(schema=resolve_schema())
            yield empty if with_columns is None else empty.select(with_columns)

    return register_io_source(source_generator, schema=resolve_schema)


def _plan_query(base, with_columns, predicate, n_rows, batch_size):
    """Fold what polars pushed down into one SQL statement.

    Returns the statement plus the predicate that still has to be applied in
    polars (None when it was translated into the WHERE clause).
    """
    pushed = _predicate_to_sql(predicate) if predicate is not None else None
    fallback = predicate if pushed is None else None

    # A predicate we could not translate is applied in polars afterwards, so
    # the columns it reads have to survive projection pushdown.
    projection = with_columns
    if projection is not None and fallback is not None:
        projection = list(projection) + [
            name for name in fallback.meta.root_names() if name not in projection
        ]

    # `not projection` also covers the empty list: polars does not ask for
    # zero columns today, and reading them all beats emitting invalid SQL.
    select = "*" if not projection else ", ".join(_quote_ident(c) for c in projection)
    query = f"SELECT {select} FROM ({base})"
    if pushed is not None:
        query += f" WHERE {pushed}"
    # A LIMIT would cut rows the fallback filter has not seen yet; in that case
    # the row count is enforced on the polars side instead.
    if n_rows is not None and fallback is None:
        query += f" LIMIT {int(n_rows)}"
    if batch_size is not None:
        query += f" SETTINGS max_block_size = {int(batch_size)}"
    return query, fallback


class _Unsupported(Exception):
    """A polars expression node that has no faithful SQL translation."""


def _predicate_to_sql(predicate):
    """Translate a polars predicate to a ClickHouse boolean expression.

    Returns None when the expression uses anything this translator does not
    model exactly; the caller then filters in polars instead. The serialized
    expression tree is a polars implementation detail, so any failure at all
    -- including a format change in a future polars -- has to degrade to that
    fallback rather than break the query.
    """
    try:
        return _node_to_sql(json.loads(predicate.meta.serialize(format="json")))
    except Exception:
        return None


_BINARY_OPS = {
    "Lt": "<",
    "LtEq": "<=",
    "Gt": ">",
    "GtEq": ">=",
    "Eq": "=",
    "NotEq": "!=",
    "And": "AND",
    "Or": "OR",
    "Plus": "+",
    "Minus": "-",
    "Multiply": "*",
}

# ClickHouse Date32, the widest date type a literal can land in.
_MIN_DATE = datetime.date(1900, 1, 1)
_MAX_DATE = datetime.date(2299, 12, 31)

_TIME_UNIT_FUNCS = {
    "Milliseconds": "fromUnixTimestamp64Milli",
    "Microseconds": "fromUnixTimestamp64Micro",
    "Nanoseconds": "fromUnixTimestamp64Nano",
}


def _node_to_sql(node):
    if not isinstance(node, dict) or len(node) != 1:
        raise _Unsupported(repr(node))
    kind, value = next(iter(node.items()))

    if kind == "BinaryExpr":
        op = _BINARY_OPS[value["op"]]
        return f"({_node_to_sql(value['left'])} {op} {_node_to_sql(value['right'])})"
    if kind == "Column":
        return _quote_ident(value)
    if kind in ("Literal", "Dyn", "Scalar"):
        return _node_to_sql(value)
    if kind == "Function":
        return _function_to_sql(value)
    return _scalar_to_sql(kind, value)


def _function_to_sql(node):
    function = node.get("function")
    if not isinstance(function, dict) or len(function) != 1:
        raise _Unsupported(repr(function))
    namespace, name = next(iter(function.items()))
    inputs = node.get("input") or []
    if namespace != "Boolean" or len(inputs) != 1:
        raise _Unsupported(f"{namespace}.{name}")

    operand = _node_to_sql(inputs[0])
    if name == "IsNull":
        return f"({operand} IS NULL)"
    if name == "IsNotNull":
        return f"({operand} IS NOT NULL)"
    if name == "Not":
        return f"(NOT {operand})"
    raise _Unsupported(f"{namespace}.{name}")


def _scalar_to_sql(kind, value):
    if kind == "Null":
        return "NULL"
    if kind == "Boolean":
        return "true" if value else "false"
    if kind in ("Int", "UInt", "Int8", "Int16", "Int32", "Int64",
                "UInt8", "UInt16", "UInt32", "UInt64"):
        return str(int(value))
    if kind in ("Float", "Float32", "Float64"):
        number = float(value)
        # inf/nan have no literal spelling ClickHouse parses the same way.
        if number != number or number in (float("inf"), float("-inf")):
            raise _Unsupported(repr(value))
        return repr(number)
    if kind in ("String", "StringOwned"):
        return _quote_string(value)
    if kind == "Date":
        date = datetime.date(1970, 1, 1) + datetime.timedelta(days=int(value))
        if not _MIN_DATE <= date <= _MAX_DATE:
            raise _Unsupported(repr(date))
        return f"toDate32('{date.isoformat()}')"
    if kind == "Datetime":
        # polars stores the ticks of a tz-aware literal as the UTC epoch and
        # keeps the zone as display metadata only, so the UTC spelling is the
        # faithful one either way. A naive literal never meets a tz-aware
        # column: polars type-checks that against the declared schema before
        # the source is ever called.
        ticks, unit, _timezone = value
        return f"{_TIME_UNIT_FUNCS[unit]}({int(ticks)}, 'UTC')"
    raise _Unsupported(kind)


def _quote_ident(name):
    if not isinstance(name, str):
        raise _Unsupported(repr(name))
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _quote_string(value):
    if not isinstance(value, str):
        raise _Unsupported(repr(value))
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
