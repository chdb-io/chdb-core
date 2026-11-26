Installation
============

chdb-core is the in-process ClickHouse engine and Python SQL API for chDB.

Requirements
------------

- **Python**: 3.9 or higher (on Windows: CPython 3.9 to 3.14, not free-threaded builds)
- **Architecture**: 64-bit systems only
- **Platforms**: macOS and Linux (x86_64 and ARM64), Windows (x86_64)

Quick Installation
------------------

Install chdb-core with a single command:

.. code-block:: bash

   pip install chdb-core

This installs the ``chdb`` Python module from the full chdb-core wheel. For the
higher-level pandas-compatible DataStore API, install ``chdb`` from the
`chDB repository <https://github.com/chdb-io/chdb>`_ instead.

Supported Platforms
-------------------

chdb-core supports the following platforms:

**Linux**
  - x86_64 (Intel/AMD 64-bit)
  - ARM64 (AArch64)

**macOS**
  - x86_64 (Intel processors)
  - ARM64 (Apple Silicon: M1, M2, M3)

**Windows**
  - x86_64 (Intel/AMD 64-bit with AVX2), Windows 10 version 1607 / Windows Server 2016 or later

**Note**: Windows on ARM64 is not supported. Windows has its own requirements and
limitations; see `Windows`_ below.

.. _windows-support:

Windows
-------

chdb-core has a Windows x86_64 wheel, ``chdb_core-<version>-cp39-abi3-win_amd64.whl``: one
wheel for CPython 3.9 to 3.14.

**Requirements**

- Windows 10 version 1607 or later, or Windows Server 2016 or later.
- An x86_64 CPU with AVX2 (x86-64-v3, the same baseline as the Linux x86_64 wheels). On a CPU
  without AVX2 the Python process dies with an illegal instruction, without an error message.
- 64-bit CPython 3.9 to 3.14. Free-threaded builds (3.13t, 3.14t) are not supported.
- Windows on ARM64 is not supported.
- No Microsoft Visual C++ runtime is needed: the module uses the Universal C Runtime that is
  part of Windows 10 and later.

On a free-threaded or 32-bit Python, or on ARM64, pip finds no matching wheel and installs the
0.0.1 placeholder release instead, after which ``import chdb`` fails. See
:ref:`windows-troubleshooting`.

**Installation**

Windows wheels are published to PyPI starting with the first chdb-core release that includes
Windows support.

.. code-block:: bat

   pip install chdb-core

For the pandas-compatible DataStore API, install ``chdb``. It is pure Python and requires
chdb-core 26.11.1 or later, so it works on Windows once a Windows chdb-core wheel of that
version or later is on PyPI:

.. code-block:: bat

   pip install chdb

**Quick check**

.. code-block:: python

   import chdb

   print(chdb.__version__)
   print(chdb.query("SELECT 'chDB is working!' AS message, version() AS engine", "CSV"))
   print(chdb.query("SELECT number, number * 2 AS doubled FROM numbers(3)", "DataFrame"))

**Available on Windows**

- Parquet, Arrow, CSV, the JSON formats, Protobuf and Avro
- Iceberg tables on local files, through ``icebergLocal()``
- Backups and archives through libarchive (tar, tar.gz, 7z, ...)
- The MySQL and MongoDB table functions and table engines
- Vector similarity indexes
- Querying pandas DataFrames with ``Python(df)``, and DataFrame and Arrow output
- In-process Python UDFs (``@chdb.udf.func``) and aggregate functions (``@chdb.udf.agg``)
- Sessions and MergeTree tables on disk
- The ADBC driver: point the driver manager at the installed module, ``chdb._chdb.__file__``,
  with ``entrypoint = chdb_adbc_init`` (see :doc:`adbc`)

**Limitations on Windows**

- **Paths in SQL**: ClickHouse unescapes backslashes in SQL string literals, so write paths
  inside SQL with forward slashes, ``file('C:/data/x.parquet')``, or double the backslashes,
  ``file('C:\\data\\x.parquet')``. In Python, ``path.replace("\\", "/")`` converts a path.
- **Network-backed storage and services** are not available: S3, Azure Blob Storage, GCS,
  HDFS, Kafka, PostgreSQL and similar. The MySQL and MongoDB integrations are available.
- **Delta Lake and WebAssembly UDFs** are not available. They need Rust components that are
  not built for Windows.
- **Executable UDFs** are not available, because running external commands is not implemented
  on Windows. This covers the legacy ``@chdb_udf`` decorator and functions defined through
  ``user_defined_executable_functions_config``. Use the in-process ``@chdb.udf.func`` decorator
  instead.
- **Atomic table exchange** (``RENAME EXCHANGE``) is not available. ``EXCHANGE TABLES``,
  ``CREATE OR REPLACE TABLE`` of a table that already exists, refreshable materialized views
  without ``APPEND``, and ADBC ingest with ``mode="replace"`` fail with ``NOT_IMPLEMENTED``.
- **DataLakeCatalog** cannot yet read tables whose location is a local Windows path.
- **libchdb** (``libchdb.dll``), the C library, and the language bindings built on it are not
  built for Windows. Only the Python module is.
- **Crashes** print no stack trace.
- **Time zone**: the default time zone is UTC, not the Windows time zone. To use another one,
  set the ``session_timezone`` setting, for example ``SET session_timezone = 'Europe/Berlin'``.
  Setting the ``TZ`` environment variable to such a name also works for chDB, but the Windows C
  runtime reads ``TZ`` too, in its own format, so Python's own local time would then be wrong.

System Requirements
-------------------

**Minimum Requirements:**
  - **RAM**: 512MB available memory
  - **Disk**: 200MB free space for installation
  - **Python**: 3.9+ with pip

**Recommended:**
  - **RAM**: 2GB+ for processing large datasets
  - **Disk**: Additional space for data files and temporary storage

Verification
------------

Verify your installation works correctly:

.. code-block:: python

   import chdb
   
   # Test basic functionality
   result = chdb.query("SELECT 'chDB is working!' as message")
   print(result)
   
   # Check version
   print(f"chdb-core version: {chdb.__version__}")
   print(f"ClickHouse engine: {chdb.engine_version}")

You should see output similar to:

.. code-block:: text

   message
   chDB is working!
   
   chdb-core version: 26.11.1
   ClickHouse engine: ...

The engine version shown by your build can differ from the chdb-core package
version. To identify the exact synced ClickHouse source, consult
``UPSTREAM_COMMIT`` in the corresponding source tree.

Troubleshooting
---------------

**Import Errors**

If you encounter import errors, ensure you have Python 3.9+ and a supported platform:

.. code-block:: python

   import sys
   import platform
   
   print(f"Python version: {sys.version}")
   print(f"Platform: {platform.platform()}")
   print(f"Architecture: {platform.architecture()}")

**Performance Issues**

For better performance with large datasets:

1. Ensure sufficient RAM is available
2. Consider using chDB's connection-based API for repeated queries

**Getting Help**

If you encounter issues:

- Check the `GitHub Issues <https://github.com/chdb-io/chdb-core/issues>`_
- Visit our `Discord community <https://discord.gg/D2Daa2fM5K>`_
- Review the troubleshooting guide

Development Installation
------------------------

For contributing to chdb-core development, you need to build from source. The build process compiles both a Python module and a standalone library.

**Prerequisites:**

- Python 3.9 or higher (required for compatibility)
- CMake and Ninja build system
- Platform-specific compilers:

  - **macOS**: LLVM/Clang 19 (install via Homebrew: ``brew install llvm@19``)
  - **Linux**: GCC or Clang with support for C++20

**Build Process:**

.. code-block:: bash

   # Clone the repository with submodules
   git clone --recursive https://github.com/chdb-io/chdb-core.git
   cd chdb-core
   
   # Ensure Python 3.9+ is active
   python --version
   
   # Build chdb-core (creates both Python module and libchdb.so)
   bash chdb/build.sh

   # Run tests to verify installation
   python -m pytest tests/

**Build Options:**

- **Debug build**: ``bash chdb/build.sh Debug`` (includes debug symbols)
- **Release build**: ``bash chdb/build.sh Release`` (default, optimized)

**Build Outputs:**

The build script generates:

- ``_chdb*.so`` - Python extension module
- ``libchdb.so`` - Standalone C++ library for external use

**Platform Notes:**

- **Linux x86_64**: Enables full feature set including Rust components and embedded compiler
- **Linux ARM64**: Disables AVX instructions and embedded compiler for compatibility
- **Windows x86_64**: Cross-compiled on a Linux x86_64 host with
  ``bash chdb/build_windows_on_linux.sh`` (host clang/lld 21 and the mingw-w64 13 UCRT headers
  and import libraries), then packaged with ``CHDB_TARGET_OS=windows python -m build --wheel``.
  The build produces ``chdb/_chdb.pyd``, ``pybind11nonlimitedapi_stubs.dll`` and one
  ``pybind11nonlimitedapi_chdb_3.X.dll`` per supported Python, and no ``libchdb``.
