# chDB native Arrow binding contract

This contract defines how native language bindings use chDB's Arrow C Data and C Stream APIs. It sets the ownership rules for in-memory SQL sources, typed query results, and bulk inserts, along with the checks a binding must pass before exposing those features. Bindings must verify the loaded `libchdb` rather than assume that every release has the same Arrow symbols and behavior.

## Scope

"Direct" means that Arrow objects cross the binding boundary through C Data or C Stream, without Arrow IPC serialization. Conversion between ClickHouse columns and Arrow buffers may still copy data. ADBC is outside this contract.

| Capability | Core API | Binding responsibility |
| --- | --- | --- |
| A1/A2: query in-memory Arrow tables, batches, and readers | `chdb_arrow_scan`, `chdb_arrow_array_scan`, `arrowstream('name')`, and unregister | Accept the language's Arrow types, return a closable registration handle, and document one-shot reader and cancellation behavior. |
| A3/A4: return typed Arrow results, all at once or in batches | `chdb_query_arrow`, `chdb_stream_query_arrow`, and `chdb_stream_fetch_arrow` | Return native Arrow tables, batches, or readers; support close and cancellation after partial consumption. |
| A5: bulk insert | Register an Arrow source and run `INSERT ... SELECT ... FROM arrowstream('name')`. | Map columns, run the insert, report errors, and clean up the source. |
| A6: compatibility and types | Export the functions and type options declared in `chdb.h`. | Check the loaded library; reject unsupported paths or label an IPC fallback explicitly. Preserve existing APIs. |

## Input ownership

`chdb_arrow_scan` borrows the caller's `ArrowArrayStream`. `chdb_arrow_array_scan` owns the single-batch stream wrapper it creates, but retains pointers to the caller's `ArrowSchema` and `ArrowArray`. The caller still owns those objects and their buffers. Failed registration does not transfer input ownership. For each successful `get_schema` call, core must call `release` exactly once on the returned root schema. Core must also free any wrapper it created if registration fails.

Registration names are process-wide, and a duplicate name is rejected. A binding should generate a unique, opaque name. It must keep the input objects, callbacks, and backing buffers alive until every scan using the registration has finished or cancellation and query cleanup have completed. It may then unregister and release the input. Returning a lazy result reader does not mean the scan has finished. A one-shot reader cannot be assumed to support a second scan. Concurrent calls on the same connection are outside this contract.

Unregistering during a scan or releasing a borrowed source early violates these preconditions. Core cannot determine whether a borrowed pointer has already been freed. Bindings must manage this order for their users, including when a registration handle is closed during a read. Direct C API callers must manage it themselves.

## Output ownership

The caller releases exported Arrow streams, schemas, and arrays through their C interface callbacks. It separately destroys the `chdb_result` or cancels and destroys a streaming query handle. An empty result retains its schema, and query errors must reach the binding. Output types follow `chdb_arrow_options`; a binding must not silently change types or describe an IPC fallback as direct.

## Conformance

Core tests must cover valid scans, whole-result and batched output, and bulk inserts under the ownership rules above. They must check release counts, failed registration, empty results, multiple batches, cancellation, and error paths with a build matching the tested source. A binding must test that closing a registration while a reader is active defers unregister and input release until the scan finishes. Each binding declares the minimum `libchdb` release it has tested for the symbols and behavior it uses.
