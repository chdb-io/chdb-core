/**
 * chdbObjectStorageMinimal.c - the smallest complete host for a callback disk.
 *
 * The host keeps blobs in a mutex-guarded array and registers it as "mini". A
 * MergeTree table on disk(type = 'callback', storage_name = 'mini') keeps all
 * its data there; --path only holds the table definition. The program reopens
 * the --path to show that the rows come back from the host, then unregisters.
 *
 * docs/callback-object-storage.md walks through this file. The full reference
 * host, with fault injection and every optional callback, is
 * examples/objectStorageMemStore.c.
 *
 * Build: clang examples/chdbObjectStorageMinimal.c -I./programs/local -L. -lchdb -lpthread \
 *          -o examples/chdbObjectStorageMinimal
 * Run:   LD_LIBRARY_PATH=. ./examples/chdbObjectStorageMinimal
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "chdb.h"

/* ---- the host store ---- */

typedef struct
{
    char * key;
    char * data;
    size_t size;
    int64_t mtime;
} blob;

/* One per write handle: the bytes of a blob until write_commit publishes them. */
typedef struct
{
    char * key;
    char * data;
    size_t size;
} pending;

/* Callbacks run concurrently on engine threads, so one lock guards the store. */
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static blob * blobs;
static size_t blob_count;

static int fail(chdb_object_storage_call * call, int status, const char * what, const char * key)
{
    if (call->error_size)
        snprintf(call->error, call->error_size, "%s: %s", what, key);
    return status;
}

/* Called with lock held. */
static blob * find(const char * key)
{
    for (size_t i = 0; i < blob_count; ++i)
        if (strcmp(blobs[i].key, key) == 0)
            return &blobs[i];
    return NULL;
}

static int on_metadata(void * ud, chdb_object_storage_call * call, const char * key, uint64_t * size, int64_t * mtime)
{
    (void)ud;
    (void)call;
    pthread_mutex_lock(&lock);
    blob * b = find(key);
    if (b)
    {
        *size = b->size;
        *mtime = b->mtime;
    }
    pthread_mutex_unlock(&lock);
    return b ? CHDB_OBJECT_STORAGE_OK : CHDB_OBJECT_STORAGE_NOT_FOUND;
}

static int on_read(void * ud, chdb_object_storage_call * call, const char * key, uint64_t offset, void * buf, size_t len, size_t * out)
{
    (void)ud;
    pthread_mutex_lock(&lock);
    blob * b = find(key);
    if (b)
    {
        size_t n = offset >= b->size ? 0 : b->size - offset;
        if (n > len)
            n = len; /* never write past len */
        if (n)
            memcpy(buf, b->data + offset, n);
        *out = n;
    }
    pthread_mutex_unlock(&lock);
    return b ? CHDB_OBJECT_STORAGE_OK : fail(call, CHDB_OBJECT_STORAGE_NOT_FOUND, "read of a missing blob", key);
}

static int on_write_begin(void * ud, chdb_object_storage_call * call, const char * key, void ** handle)
{
    (void)ud;
    pending * p = calloc(1, sizeof(*p));
    if (!p || !(p->key = strdup(key)))
    {
        free(p);
        return fail(call, CHDB_OBJECT_STORAGE_ERROR, "out of memory", key);
    }
    *handle = p;
    return CHDB_OBJECT_STORAGE_OK;
}

static int on_write_append(void * ud, chdb_object_storage_call * call, void * handle, const void * buf, size_t len)
{
    (void)ud;
    pending * p = handle;
    if (len == 0)
        return CHDB_OBJECT_STORAGE_OK;
    char * data = realloc(p->data, p->size + len);
    if (!data)
        return fail(call, CHDB_OBJECT_STORAGE_ERROR, "out of memory", p->key);
    memcpy(data + p->size, buf, len);
    p->data = data;
    p->size += len;
    return CHDB_OBJECT_STORAGE_OK;
}

static void free_pending(pending * p)
{
    free(p->key);
    free(p->data);
    free(p);
}

/* Publishes the blob atomically and always releases the handle: no write_abort follows a commit. */
static int on_write_commit(void * ud, chdb_object_storage_call * call, void * handle)
{
    (void)ud;
    pending * p = handle;
    int rc = CHDB_OBJECT_STORAGE_OK;
    pthread_mutex_lock(&lock);
    blob * b = find(p->key);
    if (!b)
    {
        blob * grown = realloc(blobs, (blob_count + 1) * sizeof(blob));
        if (grown)
        {
            blobs = grown;
            b = &blobs[blob_count++];
            *b = (blob){p->key, NULL, 0, 0};
            p->key = NULL; /* the store owns it now */
        }
    }
    if (b)
    {
        free(b->data);
        b->data = p->data;
        b->size = p->size;
        b->mtime = (int64_t)time(NULL); /* the real commit time: it becomes the part's modification time */
        p->data = NULL;
    }
    else
        rc = fail(call, CHDB_OBJECT_STORAGE_ERROR, "out of memory", p->key);
    pthread_mutex_unlock(&lock);
    free_pending(p);
    return rc;
}

static void on_write_abort(void * ud, void * handle)
{
    (void)ud;
    free_pending(handle);
}

static int on_remove(void * ud, chdb_object_storage_call * call, const char * key)
{
    (void)ud;
    (void)call;
    pthread_mutex_lock(&lock);
    blob * b = find(key);
    if (b)
    {
        free(b->key);
        free(b->data);
        *b = blobs[--blob_count];
    }
    pthread_mutex_unlock(&lock);
    return CHDB_OBJECT_STORAGE_OK; /* removing a missing blob is success */
}

static int on_list(void * ud, chdb_object_storage_call * call, const char * prefix, chdb_object_storage_list_sink sink, void * sink_ud)
{
    (void)ud;
    (void)call;
    size_t n = strlen(prefix);
    pthread_mutex_lock(&lock);
    for (size_t i = 0; i < blob_count; ++i)
        if (strncmp(blobs[i].key, prefix, n) == 0 && sink(sink_ud, blobs[i].key, blobs[i].size, blobs[i].mtime))
            break; /* the engine has seen enough */
    pthread_mutex_unlock(&lock);
    return CHDB_OBJECT_STORAGE_OK;
}

/* ---- the program ---- */

/* Runs sql and compares its TabSeparated result with want (NULL: only success matters). */
static int query(chdb_connection conn, const char * sql, const char * want)
{
    chdb_result * r = chdb_query(conn, sql, "TabSeparated");
    const char * error = chdb_result_error(r);
    int ok = error == NULL;
    if (error)
        fprintf(stderr, "%s\n  failed: %s\n", sql, error);
    else if (want)
    {
        size_t len = chdb_result_length(r);
        const char * out = chdb_result_buffer(r);
        while (len && out[len - 1] == '\n')
            len--;
        ok = len == strlen(want) && memcmp(out, want, len) == 0;
        printf("%s\n  -> %.*s%s\n", sql, (int)len, out, ok ? "" : "  (unexpected)");
    }
    chdb_destroy_query_result(r);
    return ok;
}

int main(void)
{
    /* 1. Register the store, before any chdb_connect that may reopen tables on it. */
    chdb_object_storage_callbacks cb;
    memset(&cb, 0, sizeof(cb));
    cb.struct_size = sizeof(cb);
    cb.metadata = on_metadata;
    cb.read = on_read;
    cb.write_begin = on_write_begin;
    cb.write_append = on_write_append;
    cb.write_commit = on_write_commit;
    cb.write_abort = on_write_abort;
    cb.remove = on_remove;
    cb.list = on_list;
    /* copy, open_keyspace and close_keyspace are optional and left NULL here. */

    char error[256];
    if (chdb_register_object_storage("mini", &cb, error, sizeof(error)) != CHDBSuccess)
    {
        fprintf(stderr, "register failed: %s\n", error);
        return 1;
    }

    char path[] = "/tmp/chdb_object_storage_minimal_XXXXXX";
    if (!mkdtemp(path))
        return 1;
    char arg0[] = "chdb";
    char arg1[128];
    snprintf(arg1, sizeof(arg1), "--path=%s", path);
    char * argv[] = {arg0, arg1};

    /* 2. Create a table on the callback disk and use it like any MergeTree table. */
    int ok = 1;
    chdb_connection * conn = chdb_connect(2, argv);
    ok = ok && conn && *conn;
    ok = ok && query(*conn, "CREATE DATABASE app", NULL);
    ok = ok
        && query(*conn,
            "CREATE TABLE app.events (id UInt64, name String) ENGINE = MergeTree ORDER BY id"
            " SETTINGS disk = disk(type = 'callback', storage_name = 'mini')",
            NULL);
    ok = ok && query(*conn, "INSERT INTO app.events SELECT number, concat('event-', toString(number)) FROM numbers(1000)", NULL);
    ok = ok && query(*conn, "SELECT count(), sum(id) FROM app.events", "1000\t499500");
    if (conn)
        chdb_close_conn(conn);

    pthread_mutex_lock(&lock);
    printf("the host store holds %zu blobs\n", blob_count);
    pthread_mutex_unlock(&lock);

    /* 3. Reopen the --path: the table attaches from the store the host still holds. */
    conn = ok ? chdb_connect(2, argv) : NULL;
    ok = ok && conn && *conn;
    ok = ok && query(*conn, "SELECT name FROM app.events WHERE id = 42", "event-42");
    ok = ok && query(*conn, "DROP TABLE app.events SYNC", NULL);
    if (conn)
        chdb_close_conn(conn);

    /* 4. From the host's exit hook: after this returns no callback runs again. */
    if (chdb_unregister_object_storage("mini", error, sizeof(error)) != CHDBSuccess)
    {
        fprintf(stderr, "unregister failed: %s\n", error);
        ok = 0;
    }

    char cmd[200];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    if (system(cmd) != 0) { /* best effort */ }
    printf("%s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
