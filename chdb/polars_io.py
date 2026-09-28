"""polars output for chdb query results.

chdb results already expose the Arrow PyCapsule interface, so polars can read
them directly. :func:`to_polars` goes through
``query_result.__arrow_c_stream__``, which is implemented in C++, so it needs
neither pyarrow nor pandas.
"""

from __future__ import annotations

import re

__all__ = ["to_polars"]

# polars gained Arrow PyCapsule import in 1.3.0, but until 1.10.0 it reads only
# the first record batch of the stream -- silently, and chdb results carry one
# batch per block, so multi-block results would come back truncated.
_MIN_POLARS_VERSION = (1, 10)


def _polars_version(pl):
    match = re.match(r"(\d+)\.(\d+)", getattr(pl, "__version__", ""))
    return (int(match.group(1)), int(match.group(2))) if match else None


def _require_polars(feature):
    try:
        import polars as pl
    except ImportError as e:
        raise ImportError(
            f'{feature} requires polars. Install it via "pip install polars".'
        ) from e
    version = _polars_version(pl)
    if version is not None and version < _MIN_POLARS_VERSION:
        raise ImportError(
            f"{feature} requires polars>=1.10.0, found {pl.__version__}: earlier "
            "releases do not import Arrow streams correctly. "
            'Upgrade it via "pip install -U polars".'
        )
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
