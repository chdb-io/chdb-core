## Bindings

Stable C ABI: `[chdb.h](programs/local/chdb.h)`. C demos: `examples/chdbDlopen.c`, `chdbSimple.c`, `chdbStub.c`.

Prefer `_n` entry points (pointer + length). The plain forms are NUL-terminated and cannot carry an interior NUL.

### Inventory

Everything a binding can wrap. A binding need not implement all of it — this is the checklist, and the list to update when the ABI grows.


| #   | Capability                  | Entry points                                                                                                                                                                                | Since                                    |
| --- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| 1   | Database & Connection       | `chdb_open`, `chdb_close_database`, `chdb_connect_database` (+ `chdb_open_options`, `chdb_connect_options`), `chdb_connect`, `chdb_close_conn`, `chdb_interrupt`                            | v26.1.0 (`chdb_open`, `chdb_close_database`, `chdb_connect_database`, `chdb_interrupt`: unreleased) |
| 2   | Query                       | `chdb_query`, `chdb_query_n`, `chdb_query_cmdline`                                                                                                                                          | v26.1.0                                  |
| 3   | Result accessors            | `chdb_result_{buffer,length,elapsed,error,error_code,rows_read,bytes_read,storage_rows_read,storage_bytes_read}`, `chdb_destroy_query_result`. Write metrics `rows_written` / `bytes_written`: v26.7.0; `error_code`: unreleased | v26.1.0                                  |
| 4   | Streaming query             | `chdb_stream_query[_n]`, `chdb_stream_fetch_result`, `chdb_stream_cancel_query`                                                                                                             | v26.1.0                                  |
| 5   | Arrow scan                  | `chdb_arrow_scan`, `chdb_arrow_array_scan`, `chdb_arrow_unregister_table`                                                                                                                   | v26.1.0                                  |
| 6   | Arrow insert                | `chdb_insert_arrow_array`, `chdb_insert_arrow_stream` (+ optional `chdb_arrow_insert_options`)                                                                                              | unreleased (in tree; not in tag v26.7.3) |
| 7   | Signal handlers             | `chdb_set_signal_handlers_enabled`, `chdb_reset_signal_handlers`                                                                                                                            | v26.3.0                                  |
| 8   | Parameters                  | `chdb_{query,stream_query}_with_params[_n]` (v26.5.0); `chdb_stream_{insert,query_arrow}_with_params[_n]` (v26.7.0)                                                                         | v26.5.0                                  |
| 9   | Arrow export                | `chdb_query_arrow[_n]`, `chdb_stream_query_arrow[_n]`, `chdb_stream_fetch_arrow`. Options struct: `chdb_arrow_options`                                                                      | v26.5.0                                  |
| 10  | Streaming INSERT            | `chdb_stream_insert[_n]`, `chdb_stream_append`, `chdb_stream_done`, `chdb_stream_cancel_insert`, `chdb_stream_insert_error`, `chdb_destroy_insert_stream`                                   | v26.7.0                                  |
| 11  | Version                     | `chdb_version`, `chdb_engine_version`                                                                                                                                                       | v26.7.0 (`chdb_engine_version`: unreleased) |
| 12  | Backup / restore / analysis | `chdb_backup_database_n`, `chdb_restore_database_n`, `chdb_classify_query_n`                                                                                                                | v26.7.2-rc.2                             |
| 13  | Shutdown                    | `chdb_shutdown`                                                                                                                                                                             | v26.7.2-rc.2                             |
| 14  | Result schema               | `chdb_result_column_{count,name,type}`                                                                                                                                                      | unreleased                               |
| 15  | Connection settings         | `chdb_set`, `chdb_get` (settings, and connection-level query parameters as `param_<name>`)                                                                                                 | unreleased                               |


### Feature matrix

Columns are inventory groups: Query = 1–3 · Stream = 4 · Params = 8 · Arrow scan = 5 · Arrow export = 9 · Arrow insert = 6 · Insert stream = 10 · Backup = 12 · Lifecycle = 7, 11, 13.

✅ implemented · ⬜ unverified / not yet · — n/a


| Binding                    | Query | Stream | Params | Arrow scan | Arrow export | Arrow insert | Insert stream | Backup | Lifecycle | Repository                                                    |
| -------------------------- | ----- | ------ | ------ | ---------- | ------------ | ------------ | ------------- | ------ | --------- | ------------------------------------------------------------- |
| **Python** (chdb)          | ✅     | ✅      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb](https://github.com/chdb-io/chdb)               |
| **Go**                     | ✅     | ✅      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb-go](https://github.com/chdb-io/chdb-go)         |
| **Rust**                   | ✅     | ✅      | ✅      | ✅          | ✅            | ✅            | ✅             | ✅      | ✅         | [chdb-io/chdb-rust](https://github.com/chdb-io/chdb-rust)     |
| **Node.js**                | ✅     | ⬜      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb-node](https://github.com/chdb-io/chdb-node)     |
| **Ruby**                   | ✅     | ✅      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb-ruby](https://github.com/chdb-io/chdb-ruby)     |
| **Zig**                    | ✅     | ✅      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb-zig](https://github.com/chdb-io/chdb-zig)       |
| **Bun**                    | ✅     | ⬜      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb-bun](https://github.com/chdb-io/chdb-bun)       |
| **.NET**                   | ✅     | ⬜      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | [chdb-io/chdb-dotnet](https://github.com/chdb-io/chdb-dotnet) |
| **Java** / **PHP** / **R** | ⬜     | ⬜      | ⬜      | ⬜          | ⬜            | ⬜            | ⬜             | ⬜      | ⬜         | *Contributors needed*                                         |


> Non-Rust rows have not been re-audited against this inventory. Treat ⬜ as unverified until the binding's owner confirms.

### Contracts

Stated in `chdb.h` comments and enforced in `EmbeddedSession`.

- **Destroy every `chdb_result`** with `chdb_destroy_query_result`, stream chunks and error results included. `chdb_result_error_code` returns `0` on success (or `NULL`) and a non-zero ClickHouse `DB::ErrorCodes` integer whenever `chdb_result_error` is non-`NULL`.
- **`chdb_interrupt` is lock-free and safe from any thread.** It cancels in-flight queries and active streams on `conn` via `QueryStatus::cancelQuery` without blocking on session or stream locks; calling it on a `NULL`, idle, or closed connection is a no-op.
- **`chdb_stream_insert*` never returns NULL.** Init failure is `chdb_stream_insert_error`, not a null handle. Wrap first, then check the error, so a failed init is still destroyed.
- **`chdb_stream_done` and `chdb_stream_cancel_insert` do not free the handle.** Call `chdb_destroy_insert_stream` on every path. It cancels if not finalized; cancelling twice is safe.
- **Cancel and destroy streaming query state.** `chdb_stream_cancel_query` stops execution but does not free the stream handle or fetched chunks — still call `chdb_destroy_query_result` on the stream handle and on every chunk from `chdb_stream_fetch_result`. The connection must outlive every result derived from it.
- **Multi-path coexistence and isolated `:memory:` databases.** `chdb_open(path, opts, &db)` opens a `chdb_database` handle on the single resident `EmbeddedServer`: `:memory:` handles create isolated zero-disk `DatabaseMemory` namespaces dropped on close, while persistent paths (`/dir/A`, `/dir/B`, ...) mount dynamically with dedicated `StatusFile` locks (`<path>/status`) and `DiskLocal` policies so multiple paths and `:memory:` instances coexist in one process. `chdb_connect_database(db, opts, &conn)` opens lightweight sessions sharing `db` (close with `chdb_close_conn(&conn)`). Legacy `chdb_connect` shares the default `:memory:` namespace when `--path` is omitted/`:memory:` (unless `--shared=0`), and joins/mounts `--path=<dir>`. Closing the last open `chdb_database` and `chdb_connection` tears the engine down; keep a `chdb_database` open in long-lived hosts to avoid engine start/stop churn.
- **Concurrent reads and multiple streaming cursors per connection are supported.** Read queries and `chdb_stream_query*` handles own independent query contexts and pipeline executors, while session-mutating statements (`SET`, `USE`, temporary-table DDL, `chdb_set`) take an exclusive session lock during context setup. **While a streaming INSERT is open**, any other statement on that connection is rejected (`EmbeddedSession`, `test_concurrent_statement_rejected_during_insert`). Do not interleave fetches on the *same* stream handle across threads (`chdb.h` on `chdb_stream_fetch_result`).
- **Arrow export transfers `out_stream->release` to the caller.** ClickHouse buffers live in `private_data` and are freed only by that callback. The companion `chdb_result` is metrics/error only — destroying it does not release the stream.
- **Signal handlers are process-wide.** `chdb_set_signal_handlers_enabled(0)` sets a disable flag and calls `chdb_reset_signal_handlers`, so later queries do not reinstall. `chdb_reset_signal_handlers` alone restores `SIG_DFL` for signals chDB installed and clears its list, without setting the disable flag — the next `setupCommonDeadlySignalHandlers()` call (on connect / query) may install again unless disabled.
- **`chdb_shutdown` is one-way.** It returns `CHDBError` while any connection is still open (`client_ref_count != 0`). Once `beginShutdown()` succeeds, `engine_stopped` is set and later `chdb_connect` fails for the rest of the process even if thread joins fail (shutdown may return `CHDBError` and remain retryable). Safe to call concurrently with `chdb_connect` (mutex-serialized). Skip it if the process is simply exiting.

### Arrow export options

`chdb_arrow_options` defaults (also what `NULL` means): `unsupported_as_binary=0`, `low_cardinality_as_dictionary=0`, `string_as_string=1`. A binding's "default options" struct must match, so defaults and "no options" behave the same.

DateTime columns export as Arrow `uint32` Unix seconds with no timezone. For a timezone-tagged timestamp, request `DateTime64` in SQL (`toDateTime64(col, 0, 'UTC')`).

### ADBC (experimental)

Preview: behavior and packaging may change. `libchdb` exports `chdb_adbc_init`, so any language with an [ADBC](https://arrow.apache.org/adbc/) driver manager (Python, Go, R, Ruby, Rust, C#, GLib) can use streamed Arrow, qmark parameters, bulk ingest, and catalog metadata without a dedicated binding:

```
driver     = /path/to/libchdb.so
entrypoint = chdb_adbc_init
```

Recommended for languages with no row above; per-language bindings remain the home for chDB-specific features. See `examples/chdbAdbcTest.c`, `tests/test_adbc_driver.py`, and [docs/adbc.rst](docs/adbc.rst).

### Do not bind

Still exported, marked for removal. New bindings should ignore them; existing ones should migrate.

`query_stable`, `query_stable_v2`, `free_result`, `free_result_v2`, `connect_chdb`, `close_conn`, `query_conn`, `query_conn_n`, `query_conn_streaming`, `query_conn_streaming_n`, `chdb_streaming_result_error`, `chdb_streaming_fetch_result`, `chdb_streaming_cancel_query`, `chdb_destroy_result`, and the `local_result` / `local_result_v2` structs. (`query_conn_n` is the deprecated connection API, not the modern `chdb_query_n`.)

Modern equivalent: `chdb_connect` + `chdb_query_n` + result accessors.

### Contact

New language, or a language not listed: [Discord](https://discord.gg/uUk6AKf7yM) · [auxten@clickhouse.com](mailto:auxten@clickhouse.com) · [@chdb](https://twitter.com/chdb_io)