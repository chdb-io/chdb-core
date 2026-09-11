"""The exception the chDB engine raises.

Kept out of ``chdb/__init__.py`` because the chdb wrapper package ships its own
``chdb/__init__.py`` that shadows this one, so the engine would otherwise raise
whichever class that file happens to define.
"""


class ChdbError(RuntimeError):
    """Base exception class for chDB-related errors.

    This exception is raised when chDB query execution fails or encounters
    an error. It inherits from RuntimeError, so code that caught the
    RuntimeError previously raised by the connection/session APIs keeps
    working, and provides error information from the underlying ClickHouse
    engine.

    The exception message typically contains detailed error information
    from ClickHouse, including syntax errors, type mismatches, missing
    tables/columns, and other query execution issues.

    Attributes:
        args: Tuple containing the error message and any additional arguments

    Examples:
        >>> try:
        ...     result = chdb.query("SELECT * FROM non_existent_table")
        ... except chdb.ChdbError as e:
        ...     print(f"Query failed: {e}")
        Query failed: Table 'non_existent_table' doesn't exist

        >>> try:
        ...     result = chdb.query("SELECT invalid_syntax FROM")
        ... except chdb.ChdbError as e:
        ...     print(f"Syntax error: {e}")
        Syntax error: Syntax error near 'FROM'

    Note:
        This exception is automatically raised by chdb.query() and related
        functions when the underlying ClickHouse engine reports an error.
        You should catch this exception when handling potentially failing
        queries to provide appropriate error handling in your application.
    """
