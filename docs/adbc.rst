ADBC Driver
===========

chDB exports an experimental ADBC driver entrypoint named
``chdb_adbc_init``. Use it with an ADBC driver manager by pointing the driver
to ``libchdb.so``:

.. code-block:: text

   driver     = /path/to/libchdb.so
   entrypoint = chdb_adbc_init

Read-Only Connections
---------------------

Setting the standard ``adbc.connection.readonly`` option puts the connection's
session into ClickHouse's ``readonly = 2`` mode: writes and DDL are rejected by
the engine, so the restriction covers SQL the caller writes and not only the
driver's own entry points. Per-query settings still work, which ``readonly = 1``
would forbid.

.. code-block:: python

   conn = adbc_driver_manager.dbapi.connect(
       driver="/path/to/libchdb.so",
       entrypoint="chdb_adbc_init",
       db_kwargs={"path": ":memory:"},
       conn_kwargs={"adbc.connection.readonly": "true"},
       autocommit=True,
   )

The option can also be set on a live connection. Going read-only is one-way:
``readonly = 2`` forbids changing ``readonly`` itself, so the engine rejects
turning it back off.

Bounding a Read
---------------

Any ClickHouse setting can be set as a statement option and applies to that
statement alone; the connection's session is left as it was for the next
statement. Two settings bound how much a client reads:

.. list-table::
   :header-rows: 1

   * - Setting
     - Effect
   * - ``max_block_size``
     - Caps the rows in every Arrow batch, including the first one a client
       receives from ``ExecuteQuery``.
   * - ``max_result_rows`` with ``result_overflow_mode = 'break'``
     - Stops the query once it has produced that many rows. The engine stops on
       a block boundary, so set ``max_block_size`` to a divisor of the limit for
       an exact bound.

.. code-block:: python

   stmt.set_options(**{
       "max_block_size": "100",
       "max_result_rows": "200",
       "result_overflow_mode": "break",
   })
   stmt.set_sql_query("SELECT number FROM numbers(100000)")  # returns 200 rows

Cancelling a Query
------------------

``AdbcStatementCancel`` stops the statement's result stream. It is thread-safe,
so it can be called from another thread while one is blocked reading, and it
returns without waiting for the batch already in flight. chDB cancels between
batches: the query stops early rather than instantly, ADBC calls then report
``ADBC_STATUS_CANCELLED`` (stream reads report ``ECANCELED``), and the statement
and its connection are immediately reusable. Cancelling a statement with no
result stream reports ``ADBC_STATUS_INVALID_STATE``.

Compatibility Options
---------------------

chDB keeps Arrow output close to ClickHouse defaults. Some Arrow clients do
not support every Arrow extension or nested type yet, so the ADBC driver
provides opt-in compatibility options. These options can be set on either an
ADBC connection or statement; statement options override connection options.

Boolean values accept ``true``/``false``, ``TRUE``/``FALSE``, ``1``/``0``, and
the standard ADBC enabled/disabled values.

.. list-table::
   :header-rows: 1

   * - Option
     - Default
     - Effect
     - Use when
   * - ``output_format_arrow_uuid_as_fixed_byte_array``
     - ``false``
     - Writes ``UUID`` as plain Arrow ``FixedSizeBinary(16)`` without UUID
       extension metadata.
     - The client cannot read Arrow UUID extension types.
   * - ``output_format_arrow_variant_as_string``
     - ``false``
     - Writes ``Variant(...)`` as Arrow ``String`` containing JSON text.
       ``NULL`` values remain Arrow nulls.
     - The client cannot read Arrow dense unions, for example Polars.

Example
-------

.. code-block:: python

   import pyarrow as pa
   from adbc_driver_manager import AdbcConnection, AdbcDatabase, AdbcStatement

   db = AdbcDatabase(driver="/path/to/libchdb.so", entrypoint="chdb_adbc_init")
   conn = AdbcConnection(db)
   stmt = AdbcStatement(conn)

   stmt.set_options(
       output_format_arrow_uuid_as_fixed_byte_array="true",
       output_format_arrow_variant_as_string="true",
   )
   stmt.set_sql_query("""
       SELECT
           toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS id,
           CAST('abc', 'Variant(UInt64, String)') AS value
   """)

   handle, _ = stmt.execute_query()
   table = pa.RecordBatchReader._import_from_c(handle.address).read_all()

   stmt.close()
   conn.close()
   db.close()

With ``output_format_arrow_variant_as_string=true``, the ``Variant`` value in
the example is returned as JSON text, so the value above is ``"abc"``.
