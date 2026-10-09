## Bindings

Stable C ABI: `[chdb.h](programs/local/chdb.h)`. C demos: `examples/chdbDlopen.c`, `chdbSimple.c`, `chdbStub.c`.

Prefer `_n` entry points (pointer + length). The plain forms are NUL-terminated and cannot carry an interior NUL.

### Inventory

Everything a binding can wrap. A binding need not implement all of it — this is the checklist, and the list to update when the ABI grows.


| #   | Capability                  | Entry points                                                                                                                                                                                | Since                                    |
| --- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| 1   | Connection                  | `chdb_connect`, `chdb_close_conn`                                                                                                                                                           | v26.1.0                                  |
| 2   | Query                       | `chdb_query`, `chdb_query_n`, `chdb_query_cmdline`                                                                                                                                          | v26.1.0                                  |
| 3   | Result accessors            | `chdb_result_{buffer,length,elapsed,error,rows_read,bytes_read,storage_rows_read,storage_bytes_read}`, `chdb_destroy_query_result`. Write metrics `rows_written` / `bytes_written`: v26.7.0 | v26.1.0                                  |
| 4   | Streaming query             | `chdb_stream_query[_n]`, `chdb_stream_fetch_result`, `chdb_stream_cancel_query`                                                                                                             | v26.1.0                                  |
| 5   | Arrow scan                  | `chdb_arrow_scan`, `chdb_arrow_array_scan`, `chdb_arrow_unregister_table`                                                                                                                   | v26.1.0                                  |
| 6   | Arrow insert                | `chdb_insert_arrow_array`, `chdb_insert_arrow_stream` (+ optional `chdb_arrow_insert_options`)                                                                                              | unreleased (in tree; not in tag v26.7.3) |
| 7   | Signal handlers             | `chdb_set_signal_handlers_enabled`, `chdb_reset_signal_handlers`                                                                                                                            | v26.3.0                                  |
| 8   | Parameters                  | `chdb_{query,stream_query}_with_params[_n]` (v26.5.0); `chdb_stream_{insert,query_arrow}_with_params[_n]` (v26.7.0)                                                                         | v26.5.0                                  |
| 9   | Arrow export                | `chdb_query_arrow[_n]`, `chdb_stream_query_arrow[_n]`, `chdb_stream_fetch_arrow`. Options struct: `chdb_arrow_options`                                                                      | v26.5.0                                  |
| 10  | Streaming INSERT            | `chdb_stream_insert[_n]`, `chdb_stream_append`, `chdb_stream_done`, `chdb_stream_cancel_insert`, `chdb_stream_insert_error`, `chdb_destroy_insert_stream`                                   | v26.7.0                                  |
| 11  | Version                     | `chdb_version`                                                                                                                                                                              | v26.7.0                                  |
| 12  | Backup / restore / analysis | `chdb_backup_database_n`, `chdb_restore_database_n`, `chdb_classify_query_n`                                                                                                                | v26.7.2-rc.2                             |
| 13  | Shutdown                    | `chdb_shutdown`                                                                                                                                                                             | v26.7.2-rc.2                             |
| 14  | Host object storage         | `chdb_register_object_storage`, `chdb_unregister_object_storage`. Callback table: `chdb_object_storage_callbacks`                                                                          | unreleased (in tree)                     |


### Feature matrix

Columns are inventory groups: Query = 1–3 · Stream = 4 · Params = 8 · Arrow scan = 5 · Arrow export = 9 · Arrow insert = 6 · Insert stream = 10 · Backup = 12 · Lifecycle = 7, 11, 13 · Host storage = 14.

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

Stated in `chdb.h` comments and enforced in `ChdbClient`.

- **Destroy every `chdb_result`** with `chdb_destroy_query_result`, stream chunks and error results included.
- **`chdb_stream_insert*` never returns NULL.** Init failure is `chdb_stream_insert_error`, not a null handle. Wrap first, then check the error, so a failed init is still destroyed.
- **`chdb_stream_done` and `chdb_stream_cancel_insert` do not free the handle.** Call `chdb_destroy_insert_stream` on every path. It cancels if not finalized; cancelling twice is safe.
- **Cancel and destroy streaming query state.** `chdb_stream_cancel_query` stops execution but does not free the stream handle or fetched chunks — still call `chdb_destroy_query_result` on the stream handle and on every chunk from `chdb_stream_fetch_result`. The connection must outlive every result derived from it.
- **Host object storage (`chdb_register_object_storage`).** Register under the name later given as `storage_name` in `disk(type = 'callback', storage_name = '<name>'[, key_prefix = '<prefix>'])`. The table is copied; `ud` and the functions must stay valid until `chdb_unregister_object_storage` returns. Every callback returns a `chdb_object_storage_status` and may leave a message in `call->error`, and `write_commit` is the only point at which a blob becomes visible. Metadata is `plain_rewritable`, so the host stores flat blobs only. Both registration functions take an optional error buffer that receives the reason for a `CHDBError`. Guide with examples: [docs/callback-object-storage.md](docs/callback-object-storage.md); smallest host: `examples/chdbObjectStorageMinimal.c`; reference host: `examples/objectStorageMemStore.c`, driven by `examples/chdbObjectStorageTest.c`. Rules a binding must enforce or pass on to the host, each stated in `chdb.h`:
  - *Status and errors.* `CHDB_OBJECT_STORAGE_NOT_FOUND` is the only way to say a key is missing: `metadata` reports an absent blob, `remove` and `list` treat it as success, `read` and `copy` as a failure. Any other nonzero status is a failure. Write the message into `call->error` (at most `call->error_size` bytes) rather than into thread-local state: the `call` pointer is valid on any thread until the callback returns, so a host that forwards the work to another thread fills it there. Never longjmp out of a callback or let an exception escape it.
  - *Threads.* Callbacks run on the thread inside any `chdb_*` call, on engine pool threads while that thread is blocked there, and on background merge threads with no call in flight, for as long as the connection is open; calls may be concurrent (never on one write handle). Never call `chdb_*` from a callback; never block a callback on the thread inside `chdb_*`. A store bound to one thread needs a dedicated service thread that never calls `chdb_*`, or a lock. A `chdb_*` call made by a callback on the engine's thread is refused (error result, NULL connection or `CHDBError`) instead of deadlocking; one made by a thread the callback waits on cannot be detected.
  - *Write handles.* `write_commit` releases its handle even when it returns an error, and no `write_abort` follows; `write_abort` is called at most once for a handle that was never committed, and not at all once the registration is fenced. NULL is a valid handle.
  - *Durability.* `write_commit` must be durable before it returns, or durable in commit order (WAL ordering suffices): the engine never fsyncs. A directory rename is one marker commit per directory in unspecified order.
  - *mtime.* Report the host clock at commit in Unix seconds, never 0 or a constant: after a restart it becomes the part's `modification_time` and drives merge selection and old-part lifetimes.
  - *Handles.* `write_begin` fires at file open; a part writer holds two handles per column substream plus index streams until the part is done, so pending handles must not be bounded; a commit may follow begin with no append (zero-length blob).
  - *Order.* Register before the `chdb_connect` that reopens a `--path` holding tables on that `storage_name`: metadata load instantiates the disk at attach, an unknown name fails the attach and `chdb_connect` returns NULL. Registering after connect only serves tables created later in the session, so lazy registration on first `disk(...)` use is wrong for persisted tables; recover by registering and connecting again.
  - *key_prefix.* Optional; joined with `/` in front of every key the disk creates (its `__meta/` and `__root/` entries included), so several disks can share one store. Pick it once per store and give every disk its own: a second read-write disk whose prefix overlaps an open one (equal, empty, or one a prefix of the other, trailing `/` ignored) is refused before it reads or deletes a key.
  - *Keyspaces across processes.* The engine assumes it is the only writer of a keyspace and enforces that inside one process only. A store shared by several processes (each PostgreSQL backend is one) sets the optional `open_keyspace`/`close_keyspace` pair and takes a lock there, exclusive for writers and shared for `CHDB_OBJECT_STORAGE_READ_ONLY` (a disk created with `read_only = 1`); a failing `open_keyspace` refuses the disk.
  - *Copy.* `copy(from_key, to_key)` is optional (NULL): with it, removing a part (after a merge, OPTIMIZE, DROP PART or TRUNCATE) duplicates each of its blobs host-side before the unlink; without it the engine streams each blob out through `read` and back through `write_*`. FREEZE and the other ALTER partition commands that would hard-link are refused on plain_rewritable metadata.
  - *Reads.* `read` requests are bounded to the byte range the engine needs (a mark range, not a full buffer), and seeks cost nothing until the next `read`. A read may return fewer bytes than asked; only 0 ends the blob.
  - *Crash leftovers.* Keys under `<key_prefix>/__tmp/` are scratch copies a read-write disk deletes every time it starts (the host may delete them while no connection is open); directories named by a bare 16-character random root key are dropped parts the engine does not reclaim. See the "Crash recovery" paragraph in `chdb.h`.
  - *MergeTree limits.* No hard links, so the disk is immutable for MergeTree: no ALTER beyond MODIFY/RESET SETTING and COMMENT, no mutations; `DELETE FROM` only as a lightweight update, which needs `enable_block_number_column = 1`, `enable_block_offset_column = 1` on the table and `lightweight_delete_mode = 'lightweight_update_force'` in the session (or in `chdb_connect`'s argv).
  - *ATTACH rollback.* Upstream MergeTree undoes the `detached/<part>` -> `detached/attaching_<part>` rename of a failed `ATTACH PART` / `DROP DETACHED` with a file move, which plain_rewritable refuses for a directory, stranding the part; chdb-core carries the fix (moveDirectory in `PartsTemporaryRename::rollBackAll`) until it lands upstream.
  - *Exit.* `chdb_unregister_object_storage` fences the registration: it waits for the running callbacks, and afterwards nothing calls into the host again, including the atexit teardown of a connection left open. Call it from the host's own exit hook (PostgreSQL: `before_shmem_exit`) before the store becomes unusable, from a thread the running callbacks do not wait on. Closing every connection and calling `chdb_shutdown` first lets merges finish cleanly but is not needed for memory safety. The same name can be registered again only after every connection was closed.
- **One storage path per process.** Several connections may share that path. A connect to a different `--path` while the engine is already up fails until every connection on the old path is closed (see `EmbeddedServer::getInstance`). Closing the last connection tears the engine down; see `chdb_connect` for why hosts should not churn connect/close in a long-lived process.
- **Many buffered queries per connection are normal** (`chdb_query*` one after another). **While a streaming INSERT is open**, any other statement on that connection is rejected (`ChdbClient`, `test_concurrent_statement_rejected_during_insert`). **A second INSERT or streaming read** while an insert is open is rejected. With a streaming **read** still open, the C layer does not consistently block a buffered `chdb_query*` or a second `chdb_stream_query*` — bindings should treat the connection as busy until the read stream is cancelled/destroyed anyway. Do not interleave fetches on one stream across threads (`chdb.h` on `chdb_stream_fetch_result`).
- **Arrow export transfers `out_stream->release` to the caller.** ClickHouse buffers live in `private_data` and are freed only by that callback. The companion `chdb_result` is metrics/error only — destroying it does not release the stream.
- **Signal handlers are process-wide.** `chdb_set_signal_handlers_enabled(0)` sets a disable flag and calls `chdb_reset_signal_handlers`, so later queries do not reinstall. `chdb_reset_signal_handlers` alone restores `SIG_DFL` for signals chDB installed and clears its list, without setting the disable flag — the next `setupCommonDeadlySignalHandlers()` call (on connect / query) may install again unless disabled.
- **`chdb_shutdown` is one-way.** A host with a callback object storage unregisters it from its own exit hook (see *Exit* above) whether or not it calls this. It returns `CHDBError` while any connection is still open (`client_ref_count != 0`). Once `beginShutdown()` succeeds, `engine_stopped` is set and later `chdb_connect` fails for the rest of the process even if thread joins fail (shutdown may return `CHDBError` and remain retryable). Safe to call concurrently with `chdb_connect` (mutex-serialized). Skip it if the process is simply exiting.

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