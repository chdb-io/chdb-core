#pragma once

#include <pthread.h>
#include "chdb.h"

typedef struct
{
    char * key;
    char * data;
    size_t size;
    int64_t mtime;
} blob;

/* Passed as `ud` of one registration: counts the callbacks that reached the host through it. */
struct mem_registration
{
    size_t calls;
};

struct mem_store
{
    pthread_mutex_t lock;
    blob * blobs;
    size_t count;
    size_t open_writes, peak_open_writes, aborts, commits, copies;
    size_t bytes_read, bytes_written;
    /* write_abort calls for a handle that a failing write_commit had already released: a contract
     * violation by the engine, counted instead of freed twice. */
    size_t aborts_after_commit;
    void * last_failed_commit;
    /* open_keyspace / close_keyspace calls, and the flags of the last open. */
    size_t keyspace_opens, keyspace_closes;
    unsigned last_open_flags;
    /* When nonzero, every read returns at most this many bytes (a legal short read). */
    size_t max_read;
    /* Re-entrancy probe: when set, the next write_begin calls back into chDB on this connection and
     * records whether the engine refused instead of deadlocking. */
    chdb_connection reenter_conn;
    int reenter_query_refused, reenter_unregister_refused;
    /* Injected faults, read and written under lock. */
    struct
    {
        int write_append, write_commit, read, list;
        int read_overcount; /* read reports len + 1 bytes without writing them: a contract violation */
        const char * read_suffix; /* when set, only reads of keys ending with it fail */
        size_t append_after_bytes; /* when nonzero, write_append fails once bytes_written exceeds it */
        const char * open_keyspace; /* when set, open_keyspace refuses this prefix */
    } fail;
};

extern struct mem_store store;

struct mem_store_counts
{
    size_t count, open_writes, peak_open_writes, aborts, aborts_after_commit, commits, copies, bytes_read, bytes_written;
    size_t keyspace_opens, keyspace_closes;
};

/* Copies every counter under the lock: engine threads keep writing while a test reads. */
void mem_store_snapshot(struct mem_store_counts * out);

/* Test hooks (all under the lock): store a blob directly, ask whether a key exists, and count the
 * keys starting with prefix, leaving out those ending with skip_suffix when it is not NULL. */
void mem_store_put(const char * key, const char * data, size_t size);
int mem_store_has(const char * key);
size_t mem_store_count_keys(const char * prefix, const char * skip_suffix);
size_t mem_store_calls(const struct mem_registration * registration);

/* Fills cb with the store's callbacks and `ud` = registration (may be NULL). With CHDB_TEST_NO_COPY set,
 * copy stays NULL so the engine's streaming fallback runs instead. With CHDB_TEST_SERVICE_THREAD set,
 * every callback runs on one dedicated thread while the engine thread waits, as a host whose store is
 * bound to one thread must arrange. */
void mem_store_fill(chdb_object_storage_callbacks * cb, struct mem_registration * registration);
int mem_store_service_thread(void);
