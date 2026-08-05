---
{}
---

{{ cross_reference|safe }}
# chDB Driver {{ version }}

{{ heading|safe }}

:::{warning}
The chDB ADBC driver is experimental.
:::

[chDB][chdb] is an embedded SQL engine: an in-process build of
[ClickHouse][clickhouse] with no server to run. Its shared library is also the
ADBC driver — `libchdb` exports `chdb_adbc_init`, so there is no separate driver
binary to install.

## Connecting

Point the driver manager at `libchdb` and give it a `uri`. `chdb://` is an
in-memory database; `chdb:///path/to/dir` uses a directory on disk.

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
    driver="/path/to/libchdb.so",
    entrypoint="chdb_adbc_init",
    db_kwargs={"uri": "chdb://"},
)
```

One process holds one engine: chDB cannot shut an engine down and start another
in the same process, so a process gets one database, and its `uri` is fixed for
the life of that process.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Caveats

Type-level limits are in the footnotes below. What the feature table marks
unsupported is one of two things.

**No equivalent in the engine.** ClickHouse has databases but no catalog layer
above them, so catalog-scoped features are not applicable. It reports no
constraint metadata, and has no session-temporary-table namespace. Its columns
are non-nullable unless declared `Nullable(...)`, the inverse of "non-nullable
fields are marked NOT NULL". The engine is autocommit-only, so there are no
transactions to toggle.

Get Table Schema is marked unsupported for one type only, `Time`; it works for
every other type tested.

**Not implemented by the driver yet.** `StatementExecuteSchema`,
`ConnectionGetStatistics`, and setting the current db_schema.

Binary values round-trip exactly but are stored as `String` and read back as
utf8, so non-UTF-8 payloads are out of scope.

## Compatibility

{{ compatibility_info|safe }}

{{ footnotes|safe }}

[chdb]: https://chdb.io/
[clickhouse]: https://clickhouse.com/
