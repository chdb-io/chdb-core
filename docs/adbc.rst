ADBC Driver
===========

chDB exports an experimental ADBC driver entrypoint named
``chdb_adbc_init``. Use it with an ADBC driver manager by pointing the driver
to ``libchdb.so``:

.. code-block:: text

   driver     = /path/to/libchdb.so
   entrypoint = chdb_adbc_init

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
