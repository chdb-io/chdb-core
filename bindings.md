## Welcome to the Bindings Contributors

Welcome to the community of bindings contributors! chDB offers a stable C ABI, which facilitates the development of bindings in various languages. For a C language calling demo, please refer to the examples in the `/examples` directory, such as `chdbDlopen.c`, `chdbSimple.c`, and `chdbStub.c`.

### Core Features

chDB exposes four main capabilities through its C API ([`chdb.h`](programs/local/chdb.h)):

| Feature | C API | Description |
|---|---|---|
| **Stateless Query** | `query_stable()` | One-shot query execution; each call bootstraps a new engine context. Simple but incurs startup overhead per query. |
| **Session (Connection)** | `chdb_connect()` / `chdb_query()` | Persistent connection with reusable engine context. Supports multi-statement workflows. |
| **Streaming Query** | `chdb_stream_query()` / `chdb_stream_fetch_result()` | Chunked result iteration with constant memory usage. Ideal for large result sets that should not be fully materialized. |
| **Arrow Scan** | `chdb_arrow_scan()` / `chdb_arrow_array_scan()` | Register Arrow streams or arrays as queryable table functions. Enables zero-copy data exchange with Arrow-native ecosystems. |

### Feature Matrix

| Binding | Stateless Query | Session | Streaming | Arrow Scan | Repository |
|:---|:---:|:---:|:---:|:---:|:---|
| **Python** (chdb) | ✅ | ✅ | ✅ | | [chdb-io/chdb](https://github.com/chdb-io/chdb) |
| **Go** | ✅ | ✅ | ✅ | | [chdb-io/chdb-go](https://github.com/chdb-io/chdb-go) |
| **Rust** | ✅ | ✅ | | ✅ | [chdb-io/chdb-rust](https://github.com/chdb-io/chdb-rust) |
| **Node.js** | ✅ | ✅ | | | [chdb-io/chdb-node](https://github.com/chdb-io/chdb-node) |
| **Ruby** | ✅ | ✅ | ✅ | | [chdb-io/chdb-ruby](https://github.com/chdb-io/chdb-ruby) |
| **Zig** | ✅ | ✅ | ✅ | | [chdb-io/chdb-zig](https://github.com/chdb-io/chdb-zig) |
| **Bun** | ✅ | | | | [chdb-io/chdb-bun](https://github.com/chdb-io/chdb-bun) |
| **.NET** | ✅ | | | | [chdb-io/chdb-dotnet](https://github.com/chdb-io/chdb-dotnet) |
| **Java** | | | | | _Contributors Needed_ |
| **PHP** | | | | | _Contributors Needed_ |
| **R** | | | | | _Contributors Needed_ |

### ADBC (experimental)

> **Experimental.** The ADBC driver is a preview feature: its behavior and
> packaging may change before it is declared stable.

libchdb also exports an [ADBC](https://arrow.apache.org/adbc/) driver
entrypoint (`chdb_adbc_init`), so any language with an ADBC driver manager
(Python, Go, R, Ruby, Rust, C#, GLib) can use chDB through the standard ADBC
API — streamed Arrow results, qmark parameters, bulk ingestion and catalog
metadata — without a dedicated binding:

```
driver     = /path/to/libchdb.so
entrypoint = chdb_adbc_init
```

For languages without a binding above, ADBC is the recommended path for
standard database access; per-language bindings remain the home for
chDB-specific features. See `examples/chdbAdbcTest.c` for the raw C-ABI
contract and `tests/test_adbc_driver.py` for end-to-end usage.

> **Legend:** ✅ Supported  |  Blank = not yet implemented

### chDB stable ABI

chDB also provides `query_stable` (v1) and `query_stable_v2` as alternative C functions. These APIs are still available and fully functional.

The following is the definition of the `local_result` and `local_result_v2` structure:

```c
struct local_result
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *, for freeing
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
};

struct local_result_v2
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *, for freeing
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
    char * error_message;
};
```

The following is the definition of the `query_stable`, `free_result` and `query_stable_v2`, `free_result_v2` functions.

```c
// v1 API
struct local_result * query_stable(int argc, char ** argv);
void free_result(struct local_result * result);

// v2 API added `char * error_message`.
struct local_result_v2 * query_stable_v2(int argc, char ** argv);
void free_result_v2(struct local_result_v2 * result);
```

#### Query
`query_stable` and `query_stable_v2` accept the same parameters just like the `clickhouse-local` command line tool. You can check `queryToBuffer` function in [LocalChdb.cpp](programs/local/LocalChdb.cpp) as an example.

The difference is that `query_stable_v2` adds the `char * error_message` field.
You can check if the `error_message` field is `NULL` to determine if an error occurred.

#### Free Result
`free_result` and `free_result_v2` are used to free the `local_result` and `local_result_v2` memory. For GC languages, you can call `free_result` or `free_result_v2` in the destructor of the object.

#### Known Issues

- By chDB v1.2.0, the `query_stable_v2` returns nil if the query (eg. CREATE TABLE) successes but returns no data. We will change this behavior in the future.

### Adding a Feature to Your Binding

All bindings wrap the same stable C API defined in [`chdb.h`](programs/local/chdb.h):

1. **Session** — Wrap `chdb_connect()`, `chdb_query()`, and `chdb_close_conn()`.
2. **Streaming** — Wrap `chdb_stream_query()`, `chdb_stream_fetch_result()`, and `chdb_stream_cancel_query()`.
3. **Arrow Scan** — Wrap `chdb_arrow_scan()` / `chdb_arrow_array_scan()` and `chdb_arrow_unregister_table()`.

### Need Help?

If you have already developed bindings for a language not listed above, or are interested in contributing, please contact us at:

- Discord: [bindings](https://discord.gg/uUk6AKf7yM)
- Email: auxten@clickhouse.com
- Twitter: [@chdb](https://twitter.com/chdb_io)
