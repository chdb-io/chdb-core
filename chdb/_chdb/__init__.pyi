import enum
from typing import Callable, Optional, Sequence, Union

from . import _sqltypes as _sqltypes
from ._sqltypes import ChdbType

class NullHandling(enum.Enum):
    SKIP: int    # Do not call UDF for NULL inputs, return NULL directly (default)
    PASS: int    # Convert NULL to None and call the UDF

class ExceptionHandling(enum.Enum):
    PROPAGATE: int  # Raise the exception (default)
    IGNORE: int     # Return NULL for the row and continue

def create_function(
    name: str,
    func: Callable[..., object],
    arg_types: Optional[Sequence[Union[ChdbType, str, type]]] = None,
    return_type: Optional[Union[ChdbType, str, type]] = None,
    *,
    on_null: Optional[Union[NullHandling, str]] = None,
    on_error: Optional[Union[ExceptionHandling, str]] = None,
) -> None: ...

def drop_function(name: str) -> None: ...

def create_aggregate_function(
    name: str,
    accumulator: Callable[[], object],
    arg_types: Optional[Sequence[Union[ChdbType, str, type]]] = None,
    return_type: Optional[Union[ChdbType, str, type]] = None,
    *,
    on_null: Optional[Union[NullHandling, str]] = None,
    on_error: Optional[Union[ExceptionHandling, str]] = None,
) -> None: ...

def drop_aggregate_function(name: str) -> None: ...
