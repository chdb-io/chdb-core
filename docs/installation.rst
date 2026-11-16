Installation
============

chdb-core is the in-process ClickHouse engine and Python SQL API for chDB.

Requirements
------------

- **Python**: 3.9 or higher
- **Architecture**: 64-bit systems only
- **Platforms**: macOS and Linux (x86_64 and ARM64)

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

**Note**: Windows is not currently supported.

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
