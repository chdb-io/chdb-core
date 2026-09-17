import functools
import inspect
import os
import sys
import tempfile
import atexit
import shutil
import textwrap
from xml.etree import ElementTree as ET
import chdb


def func(arg_types=None, return_type=None, *, on_null=None, on_error=None):
    """Decorator to register a Python function as a chDB SQL function.

    Uses the native Python UDF mechanism (create_function) for direct
    in-process invocation without subprocess overhead.

    Args:
        arg_types: List of argument types. Optional. Each element accepts:
            - A ChdbType instance: e.g. ``INT64``
            - A type string: e.g. ``"Int64"``
            - A Python type: e.g. ``int``, ``float``, ``str``
            - ``Optional[X]`` / ``X | None``: treated as ``X`` (every UDF argument is
              Nullable regardless, so the ``None`` member only selects the base type).
            - ``None`` (default): inferred from parameter type annotations.
            If provided, must specify types for ALL parameters.
        return_type: ClickHouse return type. Optional. Accepts:
            - A ChdbType instance: e.g. ``INT64``, ``STRING``, ``FLOAT64``
            - A type string: e.g. ``"Int64"``, ``"String"``, ``"DateTime64(3)"``
            - A Python type, optionally wrapped as ``Optional[X]`` / ``X | None``.
            - ``None`` (default): inferred from the function's return type annotation.
        on_null (str): How to handle NULL inputs. Keyword-only. Accepts:
            - ``"skip"`` (default): return NULL without calling the function.
            - ``"pass"``: convert NULL to ``None`` and call the function.
        on_error (str): How to handle exceptions. Keyword-only. Accepts:
            - ``"propagate"`` (default): raise the error.
            - ``"ignore"``: return NULL for that row.

    Returns:
        The original function, unchanged. It remains callable as normal Python
        and is simultaneously available in SQL queries by its ``__name__``.

    Examples:
        .. code-block:: python

            from chdb import func
            from chdb.sqltypes import INT64, STRING

            @func([INT64, INT64], INT64)
            def add(a, b):
                return a + b

            @func(return_type="String")
            def greet(name):
                return f"Hello, {name}!"

            # Inferred from annotation:
            @func()
            def multiply(a: int, b: int) -> int:
                return a * b

            # Pass NULL as None to the function:
            @func(return_type=INT64, on_null="pass")
            def null_safe(x):
                return 0 if x is None else x + 1

    To remove a registered function, use ``chdb.drop_function(name)``.
    """

    def decorator(fn):
        chdb.create_function(fn.__name__, fn, arg_types, return_type,
                             on_null=on_null, on_error=on_error)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def agg(arg_types=None, return_type=None, *, name=None, on_null=None, on_error=None):
    """Decorator to register a Python accumulator class as a chDB SQL aggregate function.

    The decorated class is the accumulator factory: chDB instantiates it once per
    aggregation state (once per GROUP BY key, per aggregating thread), so accumulators
    are never shared between groups.

    An accumulator must define:
        ``update(self, *args)``
            Consume one row. Arguments arrive already converted to Python values.
        ``merge(self, other)``
            Fold another accumulator of the same class into ``self``. chDB aggregates
            in parallel and merges the partial states.
        ``evaluate(self)``
            Return the final value for the group. ``None`` becomes SQL NULL.

    It may additionally define:
        ``update_batch(self, *columns)``
            Consume a whole block at once, receiving one Python ``list`` per argument.
            Used for ungrouped aggregation (``SELECT myagg(x) FROM t``), where it
            replaces one ``update()`` call per row. Ignored when ``on_error="ignore"``,
            because dropping a single offending row is not expressible in a batch call,
            and only detected on an accumulator *class* - a plain callable factory
            (see below) always takes the per-row path.

    Accumulators must be picklable for ``-State``/``-Merge`` and for spill-to-disk
    aggregation (``max_bytes_before_external_group_by``), which means the class has to
    be importable by name - define it at module level, not inside a function.

    :func:`chdb.create_aggregate_function` also accepts a plain zero-argument callable
    instead of a class (``lambda: WeightedAvg(bias)``), which is how an accumulator takes
    construction parameters. Such a factory is opaque until it is called, so it must pass
    ``arg_types`` and ``return_type`` explicitly and never uses the ``update_batch`` fast
    path.

    Args:
        arg_types: List of argument types, in the same spellings :func:`func` accepts
            (``ChdbType``, type string, or Python type). Optional; inferred from the
            annotations on ``update()`` when omitted.
        return_type: Result type, same spellings. Optional; inferred from the return
            annotation on ``evaluate()`` when omitted.
        name (str): SQL name. Keyword-only. Defaults to the class ``__name__``, which is
            usually not the spelling you want in SQL - unlike :func:`func`, whose decorated
            functions are already named like SQL functions. Registration fails if the name
            collides with a built-in function or with an already registered UDAF.
        on_null (str): How to handle NULL inputs. Keyword-only. ``"skip"`` (default)
            drops rows where any argument is NULL; ``"pass"`` converts NULL to ``None``
            and calls ``update()``.
        on_error (str): How to handle exceptions raised by ``update()``. Keyword-only.
            ``"propagate"`` (default) raises; ``"ignore"`` drops the offending row.
            Exceptions from ``merge()`` and ``evaluate()`` always propagate.

    Returns:
        The class, unchanged and still usable as a normal Python class.

    Examples:
        .. code-block:: python

            from chdb import agg
            from chdb.sqltypes import FLOAT64

            @agg([FLOAT64, FLOAT64], FLOAT64, name="wavg")
            class WeightedAvg:
                def __init__(self):
                    self.num = 0.0
                    self.den = 0.0

                def update(self, value, weight):
                    self.num += value * weight
                    self.den += weight

                def merge(self, other):
                    self.num += other.num
                    self.den += other.den

                def evaluate(self):
                    return self.num / self.den if self.den else None

            # Types inferred from annotations:
            @agg()
            class py_sum:
                def __init__(self):
                    self.total = 0

                def update(self, value: int) -> None:
                    self.total += value

                def merge(self, other):
                    self.total += other.total

                def evaluate(self) -> int:
                    return self.total

    To remove a registered aggregate function, use ``chdb.drop_aggregate_function(name)``.
    """

    def decorator(cls):
        chdb.create_aggregate_function(
            name if name is not None else cls.__name__,
            cls,
            arg_types,
            return_type,
            on_null=on_null,
            on_error=on_error,
        )
        return cls

    return decorator


def generate_udf(func_name, args, return_type, udf_body):
    """Generate UDF configuration and executable script files.

    This function creates the necessary files for a User Defined Function (UDF) in chDB:
    1. A Python executable script that processes input data
    2. An XML configuration file that registers the UDF with ClickHouse

    Args:
        func_name (str): Name of the UDF function
        args (list): List of argument names for the function
        return_type (str): ClickHouse return type for the function
        udf_body (str): Python source code body of the UDF function

    Note:
        This function is typically called by the @chdb_udf decorator and should not
        be called directly by users.
    """
    # generate python script
    with open(f"{chdb.g_udf_path}/{func_name}.py", "w") as f:
        f.write(f"#!{sys.executable}\n")
        f.write("import sys\n")
        f.write("\n")
        for line in udf_body.split("\n"):
            f.write(f"{line}\n")
        f.write("\n")
        f.write("if __name__ == '__main__':\n")
        f.write("    for line in sys.stdin:\n")
        f.write("        args = line.strip().split('\t')\n")
        for i, arg in enumerate(args):
            f.write(f"        {arg} = args[{i}]\n")
        f.write(f"        print({func_name}({', '.join(args)}))\n")
        f.write("        sys.stdout.flush()\n")
    os.chmod(f"{chdb.g_udf_path}/{func_name}.py", 0o755)
    # generate xml file
    xml_file = f"{chdb.g_udf_path}/udf_config.xml"
    root = ET.Element("functions")
    if os.path.exists(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()
    function = ET.SubElement(root, "function")
    ET.SubElement(function, "type").text = "executable"
    ET.SubElement(function, "name").text = func_name
    ET.SubElement(function, "return_type").text = return_type
    ET.SubElement(function, "format").text = "TabSeparated"
    ET.SubElement(function, "command").text = f"{func_name}.py"
    for arg in args:
        argument = ET.SubElement(function, "argument")
        # We use TabSeparated format, so assume all arguments are strings
        ET.SubElement(argument, "type").text = "String"
        ET.SubElement(argument, "name").text = arg
    tree = ET.ElementTree(root)
    tree.write(xml_file)


def chdb_udf(return_type="String"):
    """Decorator for chDB Python UDF(User Defined Function).

    Args:
        return_type (str): Return type of the function. Default is "String".
            Should be one of the ClickHouse data types.

    Notes:
        1. The function should be stateless. Only UDFs are supported, not UDAFs.
        2. Default return type is String. The return type should be one of the ClickHouse data types.
        3. The function should take in arguments of type String. All arguments are strings.
        4. The function will be called for each line of input.
        5. The function should be pure python function. Import all modules used IN THE FUNCTION.
        6. Python interpreter used is the same as the one used to run the script.

    Example:
        .. code-block:: python

            @chdb_udf()
            def sum_udf(lhs, rhs):
                return int(lhs) + int(rhs)

            @chdb_udf()
            def func_use_json(arg):
                import json
                # ... use json module
    """

    def decorator(func):
        func_name = func.__name__
        sig = inspect.signature(func)
        args = list(sig.parameters.keys())
        src = inspect.getsource(func)
        src = textwrap.dedent(src)
        udf_body = src.split("\n", 1)[1]  # remove the first line "@chdb_udf()"
        # create tmp dir and make sure the dir is deleted when the process exits
        if chdb.g_udf_path == "":
            chdb.g_udf_path = tempfile.mkdtemp()

        # clean up the tmp dir on exit
        @atexit.register
        def _cleanup():
            try:
                shutil.rmtree(chdb.g_udf_path)
            except:  # noqa
                pass

        generate_udf(func_name, args, return_type, udf_body)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator
