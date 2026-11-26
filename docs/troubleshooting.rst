Troubleshooting
===============

Installation Issues
-------------------

Platform Compatibility
~~~~~~~~~~~~~~~~~~~~~~~

chdb-core supports Python 3.9+ on **macOS and Linux** (x86_64 and ARM64), and CPython 3.9 to 3.14 on **Windows x86_64**.

**Supported Platforms:**

- macOS (x86_64 and ARM64)
- Linux (x86_64 and ARM64)
- Windows x86_64: Windows 10 version 1607 / Windows Server 2016 or later, a CPU with AVX2, and CPython 3.9 to 3.14 (not free-threaded builds). Windows on ARM64 is not supported.
- Python 3.9, 3.10, 3.11, 3.12, 3.13, 3.14

For Windows requirements and limitations, see :ref:`windows-support`. For Windows-specific problems, see `Windows Issues`_.

Import Errors
~~~~~~~~~~~~~

If you encounter import errors:

.. code-block:: bash

   ImportError: No module named '_chdb'
   ImportError: No module named 'chdb'

**Solution**: Ensure chdb-core is properly installed:

.. code-block:: bash

   pip uninstall chdb-core
   pip install chdb-core

**Alternative installation methods:**

.. code-block:: bash

   # Force reinstall
   pip install --force-reinstall chdb-core
   
   # Install specific version
   pip install chdb-core==26.11.1

**Check installation:**

.. code-block:: python

   import chdb
   print(f"chdb-core version: {chdb.__version__}")
   print(f"Engine version: {chdb.engine_version}")

Python Version Issues
~~~~~~~~~~~~~~~~~~~~~

If you get Python version compatibility errors:

.. code-block:: text

   ERROR: chdb-core requires Python >=3.9

**Solution**: Upgrade your Python version:

.. code-block:: bash

   # Check current Python version
   python --version
   
   # Use Python 3.9+ explicitly
   python3.9 -m pip install chdb-core

.. _windows-troubleshooting:

Windows Issues
--------------

DLL Load Failed on Import
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

   ImportError: DLL load failed while importing _chdb

The module needs Windows 10 version 1607 or later, or Windows Server 2016 or later. It imports
``SetThreadDescription``, which older Windows versions do not have, so they cannot load it.

**Solution**: Check the Windows version, and upgrade Windows if it is older than 10 version 1607
(build 14393):

.. code-block:: python

   import platform
   print(platform.version())   # for example 10.0.14393; the build number must be 14393 or higher

Python Dies Without an Error (Illegal Instruction)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The x86_64 wheels, for Windows and for Linux, are built for x86-64-v3 and need a CPU with AVX2.
On a CPU without AVX2 the Python process dies with an illegal instruction. There is no Python
exception and no error message, and on Windows no stack trace. The exit code on Windows is
``0xC000001D`` (``STATUS_ILLEGAL_INSTRUCTION``), shown as ``-1073741795`` by ``%ERRORLEVEL%``
or ``$LASTEXITCODE``.

**Solution**: Run chDB on a CPU with AVX2. In a virtual machine, check that the hypervisor
passes AVX2 through to the guest.

``import chdb`` Fails After pip Installs chdb-core 0.0.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When no chdb-core wheel matches your Python, pip installs the 0.0.1 placeholder release
instead, and ``import chdb`` then fails. On Windows this happens with a free-threaded Python
(3.13t, 3.14t), with a 32-bit Python and on ARM64.

**Check** the installed version (``Version: 0.0.1`` is the placeholder) and the interpreter:

.. code-block:: bat

   pip show chdb-core

.. code-block:: python

   import sys, sysconfig

   print(sys.version)
   print(sysconfig.get_platform())                     # must be win-amd64
   print(sysconfig.get_config_var("Py_GIL_DISABLED"))  # 1 means a free-threaded build

**Solution**: Uninstall the placeholder and install chdb-core with a 64-bit CPython 3.9 to
3.14 that is not a free-threaded build, on Windows x86_64, for example through the ``py``
launcher:

.. code-block:: bat

   pip uninstall chdb-core
   py -3.12 -m pip install chdb-core

Backslashes in File Paths
~~~~~~~~~~~~~~~~~~~~~~~~~

ClickHouse treats a backslash in an SQL string literal as an escape character, so a Windows
path such as ``'C:\data\new.parquet'`` reaches the engine as a different path (``\n`` becomes
a newline) and the query fails or reads the wrong file.

**Solution**: Inside SQL string literals, write paths with forward slashes, or double the
backslashes:

.. code-block:: python

   import os
   import chdb

   path = os.path.abspath("data.parquet").replace("\\", "/")
   chdb.query(f"SELECT count() FROM file('{path}', Parquet)")

   chdb.query("SELECT count() FROM file('C:/data/data.parquet', Parquet)")
   chdb.query(r"SELECT count() FROM file('C:\\data\\data.parquet', Parquet)")

Paths passed as Python arguments, such as a session path, are not SQL and need no change.

Unsupported Features (NOT_IMPLEMENTED)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some features are not available on Windows and fail with ``NOT_IMPLEMENTED``, for example:

.. code-block:: text

   Running external commands is not implemented on Windows
   RENAME EXCHANGE is not supported because exchanging files is not supported by the OS
   Can't create refreshable materialized view because exchanging files is not supported by the OS

**Solutions**:

- **Executable UDFs**, including the legacy ``@chdb_udf`` decorator and
  ``user_defined_executable_functions_config``: use the in-process ``@chdb.udf.func``
  decorator instead (``@chdb.udf.agg`` for aggregate functions).
- **EXCHANGE TABLES**, **CREATE OR REPLACE TABLE** of an existing table and ADBC ingest with
  ``mode="replace"`` need an atomic exchange that Windows builds do not have. Drop the table and
  create it again instead (for ADBC ingest, ``mode="create"``); this is not atomic.
- **Refreshable materialized views**: only ``APPEND`` refreshable materialized views work on
  Windows.

Features that are not built for Windows, such as S3, Azure Blob Storage, GCS, HDFS, Kafka,
PostgreSQL, Delta Lake and WebAssembly UDFs, fail with an error such as an unknown table
function or table engine. They are not available on Windows; see :ref:`windows-support`. The
MySQL and MongoDB integrations are available.

DataLakeCatalog cannot yet read tables whose location is a local Windows path. To read an
Iceberg table on local files, use ``icebergLocal()``.

Query Execution Issues
----------------------

Memory Issues
~~~~~~~~~~~~~

If you encounter memory-related errors:

.. code-block:: text

   Memory limit exceeded
   Out of memory while executing query

**Solutions**:

1. **Use Streaming Queries for Large Datasets**

.. code-block:: python

   from chdb import session as chs
   
   sess = chs.Session()
   
   # Process large datasets with streaming
   rows_cnt = 0
   with sess.send_query("SELECT * FROM numbers(1000000)", "CSV") as stream_result:
       for chunk in stream_result:
           # Process chunk by chunk to avoid memory issues
           rows_cnt += chunk.rows_read()
   
   print(f"Processed {rows_cnt} rows")

2. **Use File-based Sessions for Persistence**

.. code-block:: python

   # Use persistent storage to reduce memory usage
   sess = chs.Session("large_dataset.chdb")  # File-based storage
   
   # Instead of in-memory
   # sess = chs.Session()  # In-memory storage

3. **Process Data in Smaller Batches**

.. code-block:: python

   import chdb
   
   # Good: Process in batches
   for i in range(0, 1000000, 10000):
       result = chdb.query(f"SELECT * FROM numbers({i}, 10000)")
       # Process batch
   
   # Avoid: Loading entire dataset at once
   # result = chdb.query("SELECT * FROM numbers(1000000)")

4. **Use Column Selection**

.. code-block:: python

   # Good: Select only needed columns
   result = chdb.query("SELECT id, name FROM large_table WHERE id > 100")
   
   # Avoid: Select all columns
   # result = chdb.query("SELECT * FROM large_table WHERE id > 100")

File Access Issues
~~~~~~~~~~~~~~~~~~

If you encounter file access errors:

.. code-block:: text

   Permission denied: Cannot read file
   File not found: /path/to/file.csv
   Cannot determine file format

**Solutions**:

1. **Check File Permissions and Path**

.. code-block:: python

   import os
   import chdb
   
   # Check if file exists
   file_path = "data.csv"
   if not os.path.exists(file_path):
       print(f"File does not exist: {file_path}")
   
   # Use absolute path (with forward slashes, for Windows)
   abs_path = os.path.abspath(file_path).replace("\\", "/")
   result = chdb.query(f"SELECT * FROM file('{abs_path}', 'CSV')")

2. **Supported File Formats**

.. code-block:: python

   # chDB supports 60+ formats including:
   result = chdb.query("SELECT * FROM file('data.parquet', 'Parquet')")
   result = chdb.query("SELECT * FROM file('data.csv', 'CSV')")
   result = chdb.query("SELECT * FROM file('data.json', 'JSONEachRow')")
   result = chdb.query("SELECT * FROM file('data.orc', 'ORC')")

3. **File Format Detection Issues**

.. code-block:: python

   # Explicitly specify format and schema if auto-detection fails
   result = chdb.query("""
       SELECT * FROM file('data.csv', 'CSV', 
                         'id UInt32, name String, age UInt8')
   """)

4. **Working with Remote Files**

.. code-block:: python

   # HTTP/HTTPS files
   result = chdb.query("""
       SELECT * FROM url('https://example.com/data.csv', 'CSV')
   """)

Connection and Session Issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Engine Already Initialized With a Different Path**

.. code-block:: text

   EmbeddedServer already initialized with path '...', cannot connect with different path '...'

**Solution**: A process hosts one embedded engine bound to one database path.
Multiple sessions on the same path can be open at the same time, but opening a
session on a different path requires closing all existing sessions and
connections first:

.. code-block:: python

   from chdb import session as chs

   # Close sessions on the old path before opening a different path
   if 'sess' in locals():
       sess.close()

   sess = chs.Session("/path/to/other.db")

**DB-API Connection Issues**

.. code-block:: python

   import chdb.dbapi as dbapi
   
   # Always close connections properly
   conn = dbapi.connect()
   try:
       cur = conn.cursor()
       cur.execute("SELECT 1")
       result = cur.fetchone()
   finally:
       cur.close()
       conn.close()
   
   # Or use context manager for automatic cleanup
   with dbapi.connect() as conn:
       cur = conn.cursor()
       cur.execute("SELECT 1")
       result = cur.fetchone()
       cur.close()

Performance Issues
~~~~~~~~~~~~~~~~~~

If queries are running slowly:

**Solutions**:

1. **Use Efficient Query Patterns**

.. code-block:: python

   # Good: Select specific columns
   result = chdb.query("SELECT id, name FROM users WHERE id > 100")
   
   # Good: Use LIMIT for exploration
   result = chdb.query("SELECT * FROM large_table LIMIT 100")
   
   # Avoid: Select all columns from large tables
   # result = chdb.query("SELECT * FROM users WHERE id > 100")

2. **Optimize Data Formats**

.. code-block:: python

   # Parquet is usually faster than CSV for analytical queries
   result = chdb.query("SELECT * FROM file('data.parquet', 'Parquet')")
   
   # For repeated queries, consider using session with persistent storage
   from chdb import session as chs
   sess = chs.Session("analytics.chdb")
   
   # Load data once
   sess.query("CREATE TABLE users AS SELECT * FROM file('users.parquet', 'Parquet')")
   
   # Query multiple times efficiently
   result1 = sess.query("SELECT COUNT(*) FROM users WHERE age > 25")
   result2 = sess.query("SELECT AVG(age) FROM users GROUP BY department")

3. **Use Column-based Operations**

.. code-block:: python

   # Good: Use aggregations and grouping
   result = chdb.query("""
       SELECT department, COUNT(*), AVG(salary)
       FROM employees 
       GROUP BY department
       ORDER BY AVG(salary) DESC
   """)
   
   # Good: Use window functions for analytics
   result = chdb.query("""
       SELECT name, salary, 
              rank() OVER (PARTITION BY department ORDER BY salary DESC) as rank
       FROM employees
   """)

DataFrame Integration Issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Pandas DataFrame Problems**

.. code-block:: python

   import chdb.dataframe as cdf
   import pandas as pd
   
   # Ensure DataFrames have proper column types
   df = pd.DataFrame({
       'id': [1, 2, 3],
       'name': ['Alice', 'Bob', 'Charlie'],
       'score': [85.5, 92.0, 88.5]
   })
   
   # Use chDB dataframe query
   result = cdf.query("SELECT name, score FROM __tbl__ WHERE score > 85", tbl=df)
   
   # Or use Python table engine
   result = chdb.query("SELECT name, AVG(score) FROM Python(df) GROUP BY name")

**Arrow Integration Issues**

.. code-block:: python

   import pyarrow as pa
   import chdb
   
   # Create Arrow table with proper types
   arrow_table = pa.table({
       'id': pa.array([1, 2, 3], type=pa.int64()),
       'name': pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string()),
       'score': pa.array([85.5, 92.0, 88.5], type=pa.float64())
   })
   
   # Query Arrow table
   result = chdb.query("SELECT * FROM Python(arrow_table) WHERE score > 85")

UDF (User Defined Functions) Issues
------------------------------------

**UDF Import/Registration Problems**

The ``@chdb_udf`` examples below are executable UDFs, which are not available on Windows. There,
use the in-process ``@chdb.udf.func`` decorator instead.

.. code-block:: python

   from chdb.udf import chdb_udf
   from chdb import query
   
   # Ensure UDF is stateless and uses proper imports
   @chdb_udf()
   def clean_text(text):
       # Import modules inside the function
       import re
       return re.sub(r'[^\w\s]', '', text.lower())
   
   # Test UDF
   result = query("SELECT clean_text('Hello, World!') as cleaned")
   print(result)

**UDF Type Issues**

.. code-block:: python

   # Specify return type if not String
   @chdb_udf(return_type="UInt64")
   def calculate_sum(a, b):
       return int(a) + int(b)
   
   # All input arguments are strings, convert as needed
   @chdb_udf()
   def process_json(json_str):
       import json
       try:
           data = json.loads(json_str)
           return str(data.get('value', 0))
       except:
           return '0'

Streaming Query Issues
----------------------

**Resource Not Released**

.. code-block:: python

   from chdb import session as chs
   
   sess = chs.Session()
   
   # Always close streaming results if not fully consumed
   stream_result = sess.send_query("SELECT * FROM numbers(1000000)", "CSV")
   try:
       for i, chunk in enumerate(stream_result):
           if i >= 5:  # Early termination
               break
           # Process chunk
   finally:
       stream_result.close()  # Important: release resources
   
   # Or use with statement for automatic cleanup
   with sess.send_query("SELECT * FROM numbers(1000000)", "CSV") as stream_result:
       for chunk in stream_result:
           # Process chunk
           pass
   # Automatically closed

**Arrow RecordBatch Issues**

.. code-block:: python

   import pyarrow as pa
   
   # Ensure proper batch size for memory management
   stream_result = sess.send_query("SELECT * FROM numbers(100000)", "Arrow")
   
   # Use appropriate batch size
   batch_reader = stream_result.record_batch(rows_per_batch=10000)
   
   for batch in batch_reader:
       print(f"Processing batch: {batch.num_rows} rows")
       # Process batch
   
   stream_result.close()

Debug and Diagnostics
---------------------

**Enable Verbose Logging**

.. code-block:: python

   import chdb
   
   # Enable detailed output for debugging
   result = chdb.query("SELECT 1", "Pretty")
   
   # Check query performance metrics
   print(f"Rows read: {result.rows_read()}")
   print(f"Bytes read: {result.bytes_read()}")
   print(f"Elapsed time: {result.elapsed()} seconds")

**Session with Debug Parameters**

.. code-block:: python

   from chdb import session as chs
   
   # Create session with debug logging
   sess = chs.Session("debug.chdb?log-level=debug&verbose")
   
   result = sess.query("SELECT version()", "Pretty")
   print(result)

**Command Line Debug Mode**

.. code-block:: bash

   # Run chDB from command line with debug output
   python3 -m chdb "SELECT version()" Pretty
   python3 -m chdb "SELECT count() FROM numbers(100)" JSON

Getting Help
------------

If you need additional help:

1. Check the `GitHub Issues <https://github.com/chdb-io/chdb-core/issues>`_
2. Read the `ClickHouse Documentation <https://clickhouse.com/docs>`_
3. Join the `Discord Community <https://discord.gg/D2Daa2fM5K>`_
4. Check the `Project Documentation <https://clickhouse.com/docs/en/chdb>`_

Error Reporting
---------------

When reporting errors, please include:

1. chdb-core version: ``print(chdb.__version__)``
2. Python version: ``print(sys.version)``
3. Operating system
4. Complete error traceback
5. Minimal example that reproduces the issue

.. code-block:: python

   import chdb
   import sys
   
   print(f"chdb-core version: {chdb.__version__}")
   print(f"Python version: {sys.version}")
   print(f"Engine version: {chdb.engine_version}")

Common Error Messages
---------------------

**"EmbeddedServer already initialized with path ..."**
A process hosts one embedded engine bound to one database path. Sessions and connections on the same path can coexist; close all of them before opening a different path.

**"Memory limit exceeded"**  
Use streaming queries, file-based sessions, or process data in smaller batches.

**"File not found"**
Check file paths, use absolute paths, and ensure file exists and is readable.

**"Cannot determine file format"**
Explicitly specify file format and schema in your queries.

**"Import Error: No module named '_chdb'"**
Reinstall chdb-core: ``pip uninstall chdb-core && pip install chdb-core``

**"Python version not supported"**
chDB requires Python 3.9+. Upgrade your Python installation.

**"DLL load failed while importing _chdb"**
On Windows, the module needs Windows 10 version 1607 / Windows Server 2016 or later. See `DLL Load Failed on Import`_.

**"Running external commands is not implemented on Windows"**
Executable UDFs, including ``@chdb_udf``, are not available on Windows. Use ``@chdb.udf.func`` instead.

Frequently Asked Questions
--------------------------

**Q: What platforms does chDB support?**

A: chDB supports Python 3.9+ on macOS and Linux (x86_64 and ARM64), and CPython 3.9 to 3.14 on Windows x86_64 (Windows 10 version 1607 / Windows Server 2016 or later, a CPU with AVX2, no free-threaded builds). Some features are not available on Windows; see :ref:`windows-support`.

**Q: Can chDB work with large datasets?**

A: Yes, chDB can handle large datasets efficiently. Use streaming queries, file-based sessions, and persistent storage for very large datasets.

**Q: Can I use chDB in production?**

A: Yes, chDB is production-ready and part of the ClickHouse family. Test thoroughly in your specific environment and follow best practices for resource management.

**Q: How does chDB compare to SQLite?**

A: chDB is optimized for analytical workloads (OLAP) while SQLite is better for transactional workloads (OLTP). chDB offers better performance for complex analytical queries, aggregations, and data processing tasks.

**Q: What file formats does chDB support?**

A: chDB supports 70+ formats including Parquet, CSV, JSON, Arrow, ORC, and many more. See the `ClickHouse formats documentation <https://clickhouse.com/docs/en/interfaces/formats>`_ for the complete list.

**Q: Can I query Pandas DataFrames directly?**

A: Yes, chDB provides multiple ways to query Pandas DataFrames:
   - ``chdb.dataframe.query()`` function  
   - ``Python(df)`` table engine
   - DataFrame-to-Parquet conversion

**Q: How do I optimize query performance?**

A: Use column selection instead of ``SELECT *``, leverage Parquet format for better performance, use persistent sessions for repeated queries, and consider using streaming for large datasets.

**Q: Can I use external Python libraries in UDFs?**

A: Yes, but you must import all required modules inside the UDF function. UDFs should be stateless and pure Python functions.

