# Callback object storage: MergeTree on a host-owned blob store

A *callback disk* lets the program that embeds chDB decide where MergeTree data lives. The host
registers a table of C callbacks (`metadata`, `read`, `write_*`, `remove`, `list`, ...) under a
name, and any table created on `disk(type = 'callback', storage_name = '<name>')` stores every part,
mark, index and directory marker through those callbacks. `--path` keeps only the table
definitions.

Use it when the host already owns a durable key-value store and wants analytical tables to share its
guarantees: a database extension that keeps MergeTree parts in its own pages so that its WAL,
backups and replicas cover them (the motivating case is a PostgreSQL extension), an application that
encrypts or replicates its own storage, or a test harness that needs an in-memory disk. For S3,
Azure, HDFS or local files, use the built-in disks instead.

The reference for every rule below is the "Host-supplied object storage" section of
[`programs/local/chdb.h`](../programs/local/chdb.h). [`bindings.md`](../bindings.md) lists the same
rules in short form for binding authors.

- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [The callback table](#the-callback-table)
- [Status codes and error messages](#status-codes-and-error-messages)
- [Lifecycle: register, connect, unregister](#lifecycle-register-connect-unregister)
- [Threads](#threads)
- [Durability and visibility](#durability-and-visibility)
- [Several disks on one store: key_prefix](#several-disks-on-one-store-key_prefix)
- [Several processes: open_keyspace and close_keyspace](#several-processes-open_keyspace-and-close_keyspace)
- [Read-only disks](#read-only-disks)
- [What MergeTree can do on this disk](#what-mergetree-can-do-on-this-disk)
- [Crash recovery](#crash-recovery)
- [Testing a host](#testing-a-host)

## How it works

```
 MergeTree table
      │  files and directories (parts, marks, indexes)
      ▼
 plain_rewritable metadata      directory tree kept in the same store, under __meta/
      │  flat, opaque keys
      ▼
 callback object storage        the engine side: fencing, error reporting, keyspace claims
      │  chdb_object_storage_callbacks
      ▼
 your host store                any durable key → bytes map
```

The engine only needs flat blobs addressed by opaque string keys. Directories and renames are
emulated by ClickHouse's `plain_rewritable` metadata, which keeps the directory tree as small
marker blobs in the same store. Keys look like `<key_prefix>/__meta/...` and
`<key_prefix>/<dir>/<file>`; treat them as opaque and never rewrite them.

## Quick start

[`examples/chdbObjectStorageMinimal.c`](../examples/chdbObjectStorageMinimal.c) is a complete host in
about 300 lines: an array of blobs behind one mutex. Build and run it from the repository root once
`libchdb.so` is built:

```bash
clang examples/chdbObjectStorageMinimal.c -I./programs/local -L. -lchdb -lpthread \
    -o examples/chdbObjectStorageMinimal
LD_LIBRARY_PATH=. ./examples/chdbObjectStorageMinimal     # DYLD_LIBRARY_PATH on macOS
```

The program does four things.

**1. Register the store** before any `chdb_connect` that may reopen tables on it:

```c
chdb_object_storage_callbacks cb = chdb_object_storage_callbacks_init();
cb.metadata = on_metadata;
cb.read = on_read;
cb.write_begin = on_write_begin;
cb.write_append = on_write_append;
cb.write_commit = on_write_commit;
cb.write_abort = on_write_abort;
cb.remove = on_remove;
cb.list = on_list;
/* copy, open_keyspace and close_keyspace are optional */

char error[256];
if (chdb_register_object_storage("mini", &cb, error, sizeof(error)) != CHDBSuccess)
    fprintf(stderr, "register failed: %s\n", error);   /* e.g. "required callbacks are NULL: read" */
```

**2. Create tables on the callback disk** and use them like any MergeTree table:

```sql
CREATE TABLE app.events (id UInt64, name String)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = disk(type = 'callback', storage_name = 'mini');

INSERT INTO app.events SELECT number, concat('event-', toString(number)) FROM numbers(1000);
SELECT count(), sum(id) FROM app.events;   -- 1000  499500
```

**3. Reopen the `--path`.** The table definition is attached from `--path`, and its data comes back
from the host store. This is why registration must come first: attaching the table creates the disk,
and an unregistered `storage_name` makes the attach, and so `chdb_connect`, fail.

**4. Unregister from the host's exit hook.** Once `chdb_unregister_object_storage` returns, the
engine never calls the host again, so the host can free its store.

A callback is only a few lines. Every fallible one receives a `chdb_object_storage_call`, returns a
status, and writes its failure message into the engine's buffer:

```c
static int on_read(void * ud, chdb_object_storage_call * call, const char * key,
                   uint64_t offset, void * buf, size_t len, size_t * out)
{
    pthread_mutex_lock(&lock);
    blob * b = find(key);
    if (b)
    {
        size_t n = offset >= b->size ? 0 : b->size - offset;
        if (n > len)
            n = len;                      /* never write past len */
        if (n)                            /* a zero-length blob has no data */
            memcpy(buf, b->data + offset, n);
        *out = n;                         /* 0 means end of blob */
    }
    pthread_mutex_unlock(&lock);
    if (!b)
    {
        snprintf(call->error, call->error_size, "no blob %s", key);
        return CHDB_OBJECT_STORAGE_NOT_FOUND;
    }
    return CHDB_OBJECT_STORAGE_OK;
}
```

## ABI v1

Include the installed `chdb.h` and assign functions without pointer casts. Start with
`chdb_object_storage_callbacks_init()`. It sets `struct_size` to the table size and `version` to
`CHDB_OBJECT_STORAGE_ABI_V1`. The number identifies the callback contract, independent of the chDB release.

- The library checks that `struct_size` covers the fixed header before reading `version`. An older
  library rejects a future version before reading its callbacks.
- Keep the v1 fields, signatures and required behavior unchanged. Optional callbacks may be appended
  without changing `CHDB_OBJECT_STORAGE_ABI_V1`.
- For v1, the library accepts the fixed `CHDB_OBJECT_STORAGE_CALLBACKS_V1_SIZE` prefix, zeros missing
  optional fields and ignores unknown tail fields. It reads a field only when the whole field fits.
- `call->struct_size` describes the engine's call context. Hosts check it before using future fields.
- An incompatible callback contract needs a new version and a type for its new signatures. Both
  types start with `struct_size` and `version`. A host selects the new version explicitly. The
  `chdb_register_object_storage` symbol stays the same; its `const void *` parameter accepts the
  table, and the library selects the layout after checking the header.
- Dynamic bindings resolve `chdb_register_object_storage` and `chdb_unregister_object_storage`.
  They set both `struct_size` and `version` in their callback table before registration.

These rules cover callbacks, not on-disk upgrade or rollback compatibility.

## The callback table

| Callback | Required | What it does |
| --- | --- | --- |
| `metadata(key, &size, &mtime)` | yes | Size and commit time (Unix seconds) of a blob, or `NOT_FOUND`. |
| `read(key, offset, buf, len, &out)` | yes | Up to `len` bytes from `offset`; may return fewer; `*out = 0` at the end. Missing blob is a failure. |
| `write_begin(key, &handle)` | yes | Starts a blob that replaces `key` on commit. `handle` is opaque (NULL is allowed). On failure, release any resources allocated for this attempt before returning. |
| `write_append(handle, buf, len)` | yes | Appends bytes in order. |
| `write_commit(handle)` | yes | Publishes the blob atomically and durably, and releases the handle even when it fails. |
| `write_abort(handle)` | yes | Discards a pending blob that was never committed. Returns nothing. |
| `remove(key)` | yes | Deletes a blob; a missing blob is success. |
| `list(prefix, sink, sink_ud)` | yes | Calls `sink` once per key starting with `prefix` (recursive, any order); stops when `sink` returns nonzero. |
| `copy(from, to)` | no | Replaces `to` with a copy of `from`. Without it the engine streams the bytes through `read` and `write_*`, which happens for every file of a part when a merge removes it. |
| `open_keyspace(prefix, flags)` | no | A disk is about to use `prefix`; refuse it to keep another process out. Set with `close_keyspace`. |
| `close_keyspace(prefix)` | no | The disk shut down. |

Details that hosts commonly get wrong:

- **Do not wait for write handles.** `write_begin` runs when the engine opens a file, before any
  data, and a part writer keeps two handles per column substream plus the index streams open until the
  part is finished. Concurrent inserts and merges add more. Resource limits may return an error;
  never wait for a handle whose release depends on this statement finishing.
- **A commit may follow `write_begin` with no append.** Store a zero-length blob.
- **`write_commit` releases the handle on failure too.** Discard the pending blob yourself; the
  engine does not call `write_abort` after a commit.
- **`mtime` must be the real commit time.** After a restart it becomes the part's
  `modification_time`, which drives merge selection and old-part cleanup. Never return 0 or a
  constant. The engine does not check its range before converting seconds to a microsecond timestamp;
  use a value representable as `time_t` and as signed 64-bit microseconds.
- **Set both `*size` and `*mtime` on a successful `metadata` call.** The engine currently starts
  both outputs at zero, so it cannot detect a host that forgets to fill either one.
- **Keys and buffers are only valid during the call.** Copy what you keep.
- **`sink` may only be called before `list` returns.** It may be called from any thread; the engine
  serializes the calls.

## Status codes and error messages

Every fallible callback returns a `chdb_object_storage_status`:

- `CHDB_OBJECT_STORAGE_OK` (0): success.
- `CHDB_OBJECT_STORAGE_NOT_FOUND`: the key does not exist. `metadata` reports an absent blob,
  `remove` and `list` treat it as success, and `read` and `copy` as a failure.
- `CHDB_OBJECT_STORAGE_ERROR`, or any other nonzero value besides `NOT_FOUND`: failure.

On failure, write a NUL-terminated message of at most `call->error_size` bytes into `call->error`
(the size may be 0). The `call` pointer stays valid until the callback returns and may be used from
any thread, so a host that hands the work to a service thread fills it there.

An `ERROR` result, or `NOT_FOUND` from `read` or `copy`, makes the statement fail with
`CALLBACK_OBJECT_STORAGE_ERROR`. The message names the store, the operation, the key and your text:

```
Code: 802. DB::Exception: Callback object storage 'mini': read of 'store/0b1/.../data.bin' failed:
error: page checksum mismatch in relation 16384 ...
```

Match the symbolic name `CALLBACK_OBJECT_STORAGE_ERROR` in `chdb_result_error()`; the numeric code
is not part of the ABI.

Callbacks must return normally: never `longjmp` out of one (for example through PostgreSQL's
`ereport(ERROR)`) and never let a C++ exception escape. If an exception does escape a C++ host, the
engine reports it as the same error.

`chdb_register_object_storage` and `chdb_unregister_object_storage` take an optional error buffer
too. A refusal names its reason: a missing callback by field name, a `struct_size` too short for
v1, `open_keyspace` without `close_keyspace`, a name that is already registered, or a
name whose previous registration still has open disks.

## Engine checks and host responsibilities

The engine checks the ABI and table size, required callbacks, paired keyspace hooks and the byte
count reported by `read`. It checks listed keys against the requested prefix, serializes sink calls,
and stops delivering keys after the sink asks it to stop. The host must stop calling the sink itself.
The engine also rejects same-thread engine reentry and conflicting disk claims. It cannot verify
actual writes to the read buffer, returned content, real timestamps, durable storage or locks across
processes, and it cannot sandbox C code. A host must not retain a `call` pointer after its callback returns.

## Lifecycle: register, connect, unregister

```
chdb_register_object_storage("store", &cb, ...)
chdb_connect(...)                      reopened tables create their disks: open_keyspace, list
   ... queries ...                     callbacks run (see Threads)
chdb_close_conn(...)                   the last close shuts the disks down: close_keyspace
chdb_unregister_object_storage("store", ...)
```

- **Register before connecting.** A `--path` that already holds tables on the store creates their
  disks while `chdb_connect` attaches the tables. If the name is not registered, the attach fails and
  `chdb_connect` returns NULL (the cause is in the engine log). Registering later only serves tables
  created afterwards. To recover, register and connect again.
- **The table is copied at registration.** `ud` and the functions must stay valid until
  `chdb_unregister_object_storage` returns.
- **Unregistering fences the store.** It waits for the callbacks already running to return; after
  that the engine never calls the host again. Disks that are still open fail every operation with
  `CALLBACK_OBJECT_STORAGE_ERROR` (`... refused: the storage was unregistered`), and the teardown of
  a connection left open at process exit skips the host entirely.
- **At exit, unregister.** That is the one call a host's exit hook needs for safety (for PostgreSQL,
  `before_shmem_exit`). Closing every connection and calling `chdb_shutdown` first lets running
  merges finish cleanly. Call it from a thread that the running callbacks do not wait on, never from
  a callback.
- **Registering the same name again** is allowed once every disk of the previous registration has
  closed, which happens when the last connection closes. Until then it is refused: a
  `disk(...)` with the same arguments would keep using the old, fenced disk.

After unregister returns, free any pending writes and external locks belonging to that registration.
The engine will not call its abort or close hooks again. Close connections first when possible.

## Threads

Callbacks run:

1. on the thread inside any `chdb_*` call (single-threaded pipelines such as `INSERT ... VALUES`
   run on the caller, and `CREATE TABLE` lists the store there),
2. on engine pool threads while that thread is blocked inside `chdb_*`, and
3. on background merge threads with no `chdb_*` call in flight, for as long as a connection is open.

Calls may be concurrent, including on the same key, but never on one write handle. Two rules follow:

- **Do not reenter the engine from a callback.** Query/stream operations, connection changes,
  shutdown and registration changes are refused on the callback's thread. Destroying an active
  INSERT stream is ignored; destroy it after the callback returns. Materialized result cleanup and
  version/result accessors are allowed. Reentry from a thread the callback waits on is not detected.
- **Never make a callback wait for the thread that is inside `chdb_*`.** That thread is blocked
  until the statement finishes.

A store that is safe to use from many threads (behind a lock, as in the quick start) needs nothing
more. A store that may only be touched from one thread, such as a PostgreSQL backend's buffer
manager, needs a service thread that never calls `chdb_*`: every callback hands its work to that
thread and waits. [`examples/objectStorageMemStore.c`](../examples/objectStorageMemStore.c) has a
working version (`CHDB_TEST_SERVICE_THREAD=1`); its core is:

```c
/* One job at a time; the engine thread that posted it waits for the result. */
static int dispatch(int (*fn)(void *), void * args)
{
    pthread_mutex_lock(&service.lock);
    while (service.state != IDLE)
        pthread_cond_wait(&service.cond, &service.lock);
    service.fn = fn;
    service.args = args;
    service.state = POSTED;
    pthread_cond_broadcast(&service.cond);
    while (service.state != DONE)
        pthread_cond_wait(&service.cond, &service.lock);
    int rc = service.rc;
    service.state = IDLE;
    pthread_cond_broadcast(&service.cond);
    pthread_mutex_unlock(&service.lock);
    return rc;
}

static int on_metadata(void * ud, chdb_object_storage_call * call, const char * key,
                       uint64_t * size, int64_t * mtime)
{
    metadata_args a = {ud, call, key, size, mtime};
    return dispatch(do_metadata, &a);   /* do_metadata runs on the service thread */
}
```

For a PostgreSQL backend the roles are reversed, because the backend's main thread is the only one
allowed to touch shared buffers: run `chdb_*` on a helper thread, and let the main thread drain the
callback queue for the whole life of the connection, idle periods included. `chdb_close_conn` waits
for background tasks and hangs if nobody drains the queue, and so does
`chdb_unregister_object_storage` if a callback is still waiting in it, so keep draining until those
calls return.

## Durability and visibility

- `write_commit` makes a blob visible atomically. A blob that was never committed, or was aborted,
  must not be visible to `metadata`, `read` or `list`.
- `write_commit` must be durable when it returns, or at least durable in commit order (WAL ordering
  is enough). The engine never fsyncs and treats a returned commit as persisted.
- A directory rename is one small marker commit per directory, in unspecified order, so a part with
  projections is not renamed atomically across a crash. The engine recovers that half-state as a
  broken projection or a detached broken part, never as loss of acknowledged data.

## Several disks on one store: key_prefix

`key_prefix` puts every key of a disk under one prefix, so several disks can share a store:

```sql
CREATE TABLE tenant_a.t (...) ENGINE = MergeTree ORDER BY k
SETTINGS disk = disk(type = 'callback', storage_name = 'store', key_prefix = 'tenant_a');
```

- Choose the prefix once per keyspace and never change it for existing data.
- Without a prefix, the disk's keys live at the store root next to `__meta/` and `__root/`; keep
  host-owned blobs out of that namespace.
- Read-only disks can use a writer's prefix. A second read-write disk cannot use an overlapping
  prefix (equal, empty, or one a prefix of the other; trailing `/` is ignored). It fails before
  reading or deleting any key.
- Tables that use exactly the same `disk(...)` arguments share one disk, which is fine.

## Several processes: open_keyspace and close_keyspace

`plain_rewritable` keeps a keyspace's directory tree in memory and assumes it is the only writer.
The engine enforces that within one process (see above). If several processes can open the same
store, as every PostgreSQL backend can, the host must keep a second writer out, and the optional
`open_keyspace`/`close_keyspace` pair is where to do it:

- `open_keyspace(prefix, flags)` is called once per disk, before the disk reads any key under
  `prefix`. `flags` contains `CHDB_OBJECT_STORAGE_READ_ONLY` for a disk created with
  `read_only = 1`. Returning a failure refuses the disk: the `CREATE TABLE` or `ATTACH` fails with
  your message, or `chdb_connect` returns NULL when it reopens such a table.
- `close_keyspace(prefix)` is called once when that disk shuts down, at the last connection close,
  unless the registration was fenced first.

A writer can stay attached between flushes. Holding an exclusive lock against readers for that
whole time would keep them from opening the index. The host can instead use `open_keyspace` to
lock out a second writer while admitting readers. In PostgreSQL, take that lock on the main thread
(see [Threads](#threads)).

`close_keyspace` receives only the prefix, with no read-only flag or claim ID. If a registration
opens a reader and writer on the same prefix, a reader's close must not release the writer's lock.
Track ownership separately, for example by keeping each registration to one role.

Concurrent readers need more than a single-writer lock. The host must keep old parts available
while a reader uses them ([ClickHouse #124083](https://github.com/ClickHouse/ClickHouse/issues/124083))
and refresh metadata across processes. Test a long-lived writer with several readers while it
flushes and merges parts.

## Read-only disks

A process that only reads a keyspace attaches the table from a disk declared with `read_only = 1`:

```sql
ATTACH TABLE app.events UUID '8ed1effc-b2f2-4385-a066-ba5d4fccbea9'
    (id UInt64, name String) ENGINE = MergeTree ORDER BY id
SETTINGS disk = disk(type = 'callback', storage_name = 'store', read_only = 1);
```

The disk loads the directory tree from the store when it opens, and `open_keyspace` receives
`CHDB_OBJECT_STORAGE_READ_ONLY`. The read-only disk wrapper rejects ordinary write operations;
the integration test checks that `INSERT` fails. This is not a write barrier at the callback layer:
the wrapper currently delegates `createTransaction()` and `writeFileUsingBlobWritingFunction()` to
the underlying disk, and callback `writeObject()` does not check `read_only`. Read-only disks may
share a keyspace with other readers and one writer. The host must stop writes through those
lower-level paths as well. Normal reads and writes through one
read-write disk are unchanged. Read-only disks skip the scratch sweep described in
[Crash recovery](#crash-recovery).

## What MergeTree can do on this disk

`plain_rewritable` has no hard links, so MergeTree treats the disk as immutable:

- `ALTER TABLE` is refused except `MODIFY SETTING`, `RESET SETTING` and `COMMENT`: no
  `ADD/DROP/MODIFY COLUMN`, no `ADD/DROP INDEX`, no `MODIFY TTL` or `ORDER BY`.
- Every mutation is refused (`ALTER UPDATE/DELETE`, `MATERIALIZE INDEX/COLUMN/PROJECTION`), and so
  are `FREEZE` and the other partition commands that would hard-link.
- `INSERT`, `SELECT`, `OPTIMIZE`, merges, `TRUNCATE`, `DROP`, `DETACH/ATTACH PART`, text and vector
  skip indexes all work.

`DELETE FROM` works as a lightweight update. The table needs the two block columns, and the session
needs the forcing mode (plain `'lightweight_update'` falls back to `ALTER UPDATE` and reports the
immutable-disk error instead):

```sql
CREATE TABLE app.t (k UInt64, v String) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         disk = disk(type = 'callback', storage_name = 'store');

SET lightweight_delete_mode = 'lightweight_update_force';
DELETE FROM app.t WHERE k < 1000;
```

Deleted rows live in patch parts until a merge or `OPTIMIZE TABLE ... FINAL` folds them in. To make
the mode the connection default, pass `--lightweight_delete_mode=lightweight_update_force` in
`chdb_connect`'s argv.

Passing `metadata_type = 'local'` in `disk(...)` lifts these limits, but keeps the table's metadata in
a local directory under `--path`, outside the host store.

## Crash recovery

After an unclean exit the store may hold two kinds of leftovers:

- Keys under `<key_prefix>/__tmp/`: scratch copies the engine takes before it unlinks a file. The
  engine never reads them back. A read-write disk attempts to delete them at startup; on a list or
  remove error it logs the failure and continues, leaving some copies behind. The host may delete
  them whenever no connection is open.
- Directories whose `__meta/<id>/prefix.path` content is a bare 16-character random name at the root:
  a dropped part that was being removed. The engine does not reclaim them; budget for them, as S3
  users of `plain_rewritable` do with bucket lifecycle rules.

## Testing a host

[`examples/runObjectStorageTest.sh`](../examples/runObjectStorageTest.sh) builds the minimal example
and the reference host, and runs the reference test three times: with callbacks on the engine threads,
without the `copy` callback, and with every callback on one service thread. `CHDB_TEST_UBSAN=1` builds
the host side with UBSan. The driver
([`examples/chdbObjectStorageTest.c`](../examples/chdbObjectStorageTest.c)) and its companions show how
to check a host's own behaviour:

`bash examples/runObjectStorageHostTest.sh` runs the reference-store and frozen-v1 fixture tests
without a libchdb build. Set `CHDB_TEST_SANITIZE=address,undefined` to enable sanitizers.
The fixture covers header/table handling, not the full library registry or query engine. Those are
exercised by the integration runner and the `ENABLE_TESTS=1` callback gtests.

- [`objectStorageFaultTests.c`](../examples/objectStorageFaultTests.c) injects a failure into each
  callback and checks that the statement fails with `CALLBACK_OBJECT_STORAGE_ERROR`, that no write
  handle leaks, and that reads capped at 1000 bytes give the same results.
- [`objectStorageLifecycleTests.c`](../examples/objectStorageLifecycleTests.c) covers registration
  refusals, overlapping prefixes, a refusing `open_keyspace`, a callback that calls back into chDB,
  unregistering a store whose disk is still open, and a read-only attach.
- [`objectStorageDiskTests.c`](../examples/objectStorageDiskTests.c) runs merges, part removal,
  `DETACH`/`ATTACH PART` and a second disk under `key_prefix`.

Before shipping a host, check it against this list:

- [ ] Returns `NOT_FOUND`, not `ERROR`, for a missing key in `metadata`, and success for a missing key
      in `remove`.
- [ ] Sets `*out` on every successful read, never above `len`, and never writes past the buffer.
- [ ] Lists only the requested prefix and stops delivering when the sink asks it to stop.
- [ ] Stores a zero-length blob when `write_commit` follows `write_begin` with no append.
- [ ] Releases the handle in `write_commit` even when the commit fails.
- [ ] Returns an error at resource limits rather than waiting for a handle the statement must release.
- [ ] Reports the real commit time as `mtime`.
- [ ] Makes a commit durable (or durable in order) before returning.
- [ ] Never reenters the engine from a callback or waits for the thread inside `chdb_*`.
- [ ] Writes failure messages into `call->error`, not into thread-local state.
- [ ] Registers before `chdb_connect`, and unregisters from its exit hook before freeing the store.
- [ ] Keeps independently opened writers off overlapping prefixes across processes in
      `open_keyspace`, without excluding readers for the writer's whole lifetime.
- [ ] Protects old parts in use, refreshes metadata across processes, blocks writes from read-only disks,
      and tests a long-lived writer with multiple readers and periodic flushes and merges.
