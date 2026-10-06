/**
 * objectStorageMemStore.c - the host side of chdbObjectStorageTest: an in-memory
 * blob store behind chdb_object_storage_callbacks. The engine calls these
 * concurrently, so one mutex guards the whole store. A real host would put the
 * blobs in its own durable pages instead of malloc.
 *
 * Every callback packs its arguments and goes through dispatch(): by default it
 * runs on the engine thread that called it; with CHDB_TEST_SERVICE_THREAD set it
 * runs on one service thread while that engine thread waits, which is how a host
 * whose store may only be touched from one thread (a PostgreSQL backend) must
 * serve the engine. Failure messages go into call->error from whichever thread
 * ran the callback.
 */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "objectStorageMemStore.h"

typedef struct
{
    char * key;
    char * data;
    size_t size, cap;
} pending_write;

struct mem_store store = {.lock = PTHREAD_MUTEX_INITIALIZER};

/* ---- dispatch ---- */

static struct
{
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int enabled;
    int (*fn)(void *);
    void * args;
    int rc;
    int state; /* 0 idle, 1 job posted, 2 job done */
} service = {.lock = PTHREAD_MUTEX_INITIALIZER, .cond = PTHREAD_COND_INITIALIZER};

static void * service_main(void * unused)
{
    (void)unused;
    pthread_mutex_lock(&service.lock);
    for (;;)
    {
        while (service.state != 1)
            pthread_cond_wait(&service.cond, &service.lock);
        int (*fn)(void *) = service.fn;
        void * args = service.args;
        pthread_mutex_unlock(&service.lock);
        int rc = fn(args);
        pthread_mutex_lock(&service.lock);
        service.rc = rc;
        service.state = 2;
        pthread_cond_broadcast(&service.cond);
    }
    return NULL;
}

/* Runs fn(args) on the service thread when enabled, one job at a time. */
static int dispatch(int (*fn)(void *), void * args)
{
    if (!service.enabled)
        return fn(args);
    pthread_mutex_lock(&service.lock);
    while (service.state != 0)
        pthread_cond_wait(&service.cond, &service.lock);
    service.fn = fn;
    service.args = args;
    service.state = 1;
    pthread_cond_broadcast(&service.cond);
    while (service.state != 2)
        pthread_cond_wait(&service.cond, &service.lock);
    int rc = service.rc;
    service.state = 0;
    pthread_cond_broadcast(&service.cond);
    pthread_mutex_unlock(&service.lock);
    return rc;
}

int mem_store_service_thread(void)
{
    return service.enabled;
}

/* ---- helpers ---- */

static int fail(chdb_object_storage_call * call, int status, const char * what, const char * key)
{
    if (call && call->error_size)
        snprintf(call->error, call->error_size, "%s: %s", what, key);
    return status;
}

static void count_call(void * ud)
{
    if (!ud)
        return;
    pthread_mutex_lock(&store.lock);
    ((struct mem_registration *)ud)->calls++;
    pthread_mutex_unlock(&store.lock);
}

static int ends_with(const char * key, const char * suffix)
{
    size_t k = strlen(key), n = strlen(suffix);
    return k >= n && strcmp(key + k - n, suffix) == 0;
}

static blob * find(const char * key)
{
    for (size_t i = 0; i < store.count; ++i)
        if (strcmp(store.blobs[i].key, key) == 0)
            return &store.blobs[i];
    return NULL;
}

/* Appends an empty entry for key (under the lock); NULL when out of memory. */
static blob * add_blob(const char * key)
{
    blob * blobs = realloc(store.blobs, (store.count + 1) * sizeof(blob));
    if (blobs)
        store.blobs = blobs;
    char * copy = blobs ? strdup(key) : NULL;
    if (!copy)
        return NULL;
    blob * b = &store.blobs[store.count++];
    *b = (blob){copy, NULL, 0, 0};
    return b;
}

/* ---- metadata ---- */

typedef struct { void * ud; chdb_object_storage_call * call; const char * key; uint64_t * size; int64_t * mtime; } metadata_args;

static int do_metadata(void * p)
{
    metadata_args * a = p;
    count_call(a->ud);
    pthread_mutex_lock(&store.lock);
    blob * b = find(a->key);
    if (b)
    {
        *a->size = b->size;
        *a->mtime = b->mtime;
    }
    pthread_mutex_unlock(&store.lock);
    return b ? CHDB_OBJECT_STORAGE_OK : CHDB_OBJECT_STORAGE_NOT_FOUND;
}

static int cb_metadata(void * ud, chdb_object_storage_call * call, const char * key, uint64_t * size, int64_t * mtime)
{
    metadata_args a = {ud, call, key, size, mtime};
    return dispatch(do_metadata, &a);
}

/* ---- read ---- */

typedef struct { void * ud; chdb_object_storage_call * call; const char * key; uint64_t offset; void * buf; size_t len; size_t * out; } read_args;

static int do_read(void * p)
{
    read_args * a = p;
    count_call(a->ud);
    int rc = CHDB_OBJECT_STORAGE_OK;
    pthread_mutex_lock(&store.lock);
    blob * b = find(a->key);
    if (store.fail.read || (store.fail.read_suffix && ends_with(a->key, store.fail.read_suffix)))
        rc = fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "injected read failure", a->key);
    else if (!b)
        rc = fail(a->call, CHDB_OBJECT_STORAGE_NOT_FOUND, "read of missing blob", a->key);
    else
    {
        size_t n = a->offset >= b->size ? 0 : (b->size - a->offset < a->len ? b->size - a->offset : a->len);
        if (store.max_read && n > store.max_read)
            n = store.max_read;
        if (n) /* a zero-byte blob has no data pointer */
            memcpy(a->buf, b->data + a->offset, n);
        *a->out = store.fail.read_overcount ? a->len + 1 : n;
        store.bytes_read += n;
    }
    pthread_mutex_unlock(&store.lock);
    return rc;
}

static int cb_read(void * ud, chdb_object_storage_call * call, const char * key, uint64_t offset, void * buf, size_t len, size_t * out)
{
    read_args a = {ud, call, key, offset, buf, len, out};
    return dispatch(do_read, &a);
}

/* ---- writes ---- */

/* Calls back into chDB from inside a callback, once, and records whether both calls were refused. */
static void reenter_probe(void)
{
    pthread_mutex_lock(&store.lock);
    chdb_connection conn = store.reenter_conn;
    store.reenter_conn = NULL;
    pthread_mutex_unlock(&store.lock);
    if (!conn)
        return;

    chdb_result * r = chdb_query(conn, "SELECT 1", "CSV");
    const char * e = chdb_result_error(r);
    int query_refused = e && strstr(e, "object storage callback") != NULL;
    chdb_destroy_query_result(r);

    char err[256] = "";
    int unregister_refused = chdb_unregister_object_storage("mem_store", err, sizeof(err)) == CHDBError
        && strstr(err, "object storage callback") != NULL;

    pthread_mutex_lock(&store.lock);
    store.reenter_query_refused = query_refused;
    store.reenter_unregister_refused = unregister_refused;
    pthread_mutex_unlock(&store.lock);
}

typedef struct { void * ud; chdb_object_storage_call * call; const char * key; void ** handle; } begin_args;

static int do_write_begin(void * p)
{
    begin_args * a = p;
    count_call(a->ud);
    reenter_probe();
    pending_write * w = calloc(1, sizeof(*w));
    if (w)
        w->key = strdup(a->key);
    if (!w || !w->key)
    {
        free(w);
        return fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "out of memory", a->key);
    }
    pthread_mutex_lock(&store.lock);
    store.open_writes++;
    if (store.open_writes > store.peak_open_writes)
        store.peak_open_writes = store.open_writes;
    if (store.last_failed_commit == w) /* the address was reused */
        store.last_failed_commit = NULL;
    pthread_mutex_unlock(&store.lock);
    *a->handle = w;
    return CHDB_OBJECT_STORAGE_OK;
}

static int cb_write_begin(void * ud, chdb_object_storage_call * call, const char * key, void ** handle)
{
    begin_args a = {ud, call, key, handle};
    return dispatch(do_write_begin, &a);
}

static void drop_write(pending_write * w)
{
    free(w->key);
    free(w->data);
    free(w);
    pthread_mutex_lock(&store.lock);
    store.open_writes--;
    pthread_mutex_unlock(&store.lock);
}

typedef struct { void * ud; chdb_object_storage_call * call; void * handle; const void * buf; size_t len; } append_args;

static int do_write_append(void * p)
{
    append_args * a = p;
    count_call(a->ud);
    pending_write * w = a->handle;
    int rc = CHDB_OBJECT_STORAGE_OK;
    pthread_mutex_lock(&store.lock);
    store.bytes_written += a->len;
    if (store.fail.write_append || (store.fail.append_after_bytes && store.bytes_written > store.fail.append_after_bytes))
        rc = fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "injected write failure", w->key);
    pthread_mutex_unlock(&store.lock);
    if (rc)
        return rc;
    if (w->size + a->len > w->cap)
    {
        size_t cap = (w->size + a->len) * 2;
        char * data = realloc(w->data, cap);
        if (!data)
            return fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "out of memory", w->key);
        w->data = data;
        w->cap = cap;
    }
    memcpy(w->data + w->size, a->buf, a->len);
    w->size += a->len;
    return CHDB_OBJECT_STORAGE_OK;
}

static int cb_write_append(void * ud, chdb_object_storage_call * call, void * handle, const void * buf, size_t len)
{
    append_args a = {ud, call, handle, buf, len};
    return dispatch(do_write_append, &a);
}

typedef struct { void * ud; chdb_object_storage_call * call; void * handle; } commit_args;

/* Commit releases the handle whether or not it succeeds: no write_abort follows. */
static int do_write_commit(void * p)
{
    commit_args * a = p;
    count_call(a->ud);
    pending_write * w = a->handle;
    const char * failure = NULL;
    pthread_mutex_lock(&store.lock);
    blob * b = store.fail.write_commit ? NULL : find(w->key);
    if (store.fail.write_commit)
        failure = "injected commit failure";
    else if (!b && !(b = add_blob(w->key)))
        failure = "out of memory";
    if (b)
    {
        free(b->data);
        b->data = w->data; /* ownership moves to the store */
        b->size = w->size;
        b->mtime = (int64_t)time(NULL); /* the commit time: the engine reads it back as the part's modification time */
        w->data = NULL;
        store.commits++;
    }
    else
        store.last_failed_commit = w;
    pthread_mutex_unlock(&store.lock);
    int rc = failure ? fail(a->call, CHDB_OBJECT_STORAGE_ERROR, failure, w->key) : CHDB_OBJECT_STORAGE_OK;
    drop_write(w);
    return rc;
}

static int cb_write_commit(void * ud, chdb_object_storage_call * call, void * handle)
{
    commit_args a = {ud, call, handle};
    return dispatch(do_write_commit, &a);
}

static int do_write_abort(void * p)
{
    commit_args * a = p;
    count_call(a->ud);
    pthread_mutex_lock(&store.lock);
    if (a->handle == store.last_failed_commit)
    {
        store.aborts_after_commit++;
        store.last_failed_commit = NULL;
        pthread_mutex_unlock(&store.lock);
        return 0;
    }
    store.aborts++;
    pthread_mutex_unlock(&store.lock);
    drop_write(a->handle);
    return 0;
}

static void cb_write_abort(void * ud, void * handle)
{
    commit_args a = {ud, NULL, handle};
    dispatch(do_write_abort, &a);
}

/* ---- remove, copy, list ---- */

typedef struct { void * ud; chdb_object_storage_call * call; const char * key; const char * to_key; } key_args;

static int do_remove(void * p)
{
    key_args * a = p;
    count_call(a->ud);
    pthread_mutex_lock(&store.lock);
    blob * b = find(a->key);
    if (b)
    {
        free(b->key);
        free(b->data);
        *b = store.blobs[--store.count];
    }
    pthread_mutex_unlock(&store.lock);
    return b ? CHDB_OBJECT_STORAGE_OK : CHDB_OBJECT_STORAGE_NOT_FOUND;
}

static int cb_remove(void * ud, chdb_object_storage_call * call, const char * key)
{
    key_args a = {ud, call, key, NULL};
    return dispatch(do_remove, &a);
}

/* Duplicates a blob under the lock; the engine uses it for hard links, moves and part removal. */
static int do_copy(void * p)
{
    key_args * a = p;
    count_call(a->ud);
    int rc = CHDB_OBJECT_STORAGE_OK;
    pthread_mutex_lock(&store.lock);
    blob * from = find(a->key);
    char * data = from ? malloc(from->size ? from->size : 1) : NULL;
    size_t size = from ? from->size : 0;
    if (data && size)
        memcpy(data, from->data, size);
    blob * to = data ? find(a->to_key) : NULL; /* add_blob may move the array, so from is not used past here */
    if (data && !to)
        to = add_blob(a->to_key);
    if (!from)
        rc = fail(a->call, CHDB_OBJECT_STORAGE_NOT_FOUND, "copy of missing blob", a->key);
    else if (!to)
        rc = fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "out of memory", a->to_key);
    else
    {
        free(to->data);
        to->data = data;
        to->size = size;
        to->mtime = (int64_t)time(NULL);
        store.copies++;
    }
    if (rc)
        free(data);
    pthread_mutex_unlock(&store.lock);
    return rc;
}

static int cb_copy(void * ud, chdb_object_storage_call * call, const char * from_key, const char * to_key)
{
    key_args a = {ud, call, from_key, to_key};
    return dispatch(do_copy, &a);
}

typedef struct { void * ud; chdb_object_storage_call * call; const char * prefix; chdb_object_storage_list_sink sink; void * sink_ud; } list_args;

static int do_list(void * p)
{
    list_args * a = p;
    count_call(a->ud);
    size_t n = strlen(a->prefix);
    int rc = CHDB_OBJECT_STORAGE_OK;
    pthread_mutex_lock(&store.lock);
    if (store.fail.list)
        rc = fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "injected list failure", a->prefix);
    for (size_t i = 0; !rc && i < store.count; ++i)
        if (strncmp(store.blobs[i].key, a->prefix, n) == 0
            && a->sink(a->sink_ud, store.blobs[i].key, store.blobs[i].size, store.blobs[i].mtime))
            break;
    pthread_mutex_unlock(&store.lock);
    return rc;
}

static int cb_list(void * ud, chdb_object_storage_call * call, const char * prefix, chdb_object_storage_list_sink sink, void * sink_ud)
{
    list_args a = {ud, call, prefix, sink, sink_ud};
    return dispatch(do_list, &a);
}

/* ---- keyspaces ---- */

typedef struct { void * ud; chdb_object_storage_call * call; const char * prefix; uint32_t flags; } keyspace_args;

/* A real host would take a cross-process lock here (exclusive unless CHDB_OBJECT_STORAGE_READ_ONLY). */
static int do_open_keyspace(void * p)
{
    keyspace_args * a = p;
    count_call(a->ud);
    int rc = CHDB_OBJECT_STORAGE_OK;
    pthread_mutex_lock(&store.lock);
    if (store.fail.open_keyspace && strcmp(store.fail.open_keyspace, a->prefix) == 0)
        rc = fail(a->call, CHDB_OBJECT_STORAGE_ERROR, "keyspace locked by another process", a->prefix);
    else
    {
        store.keyspace_opens++;
        store.last_open_flags = a->flags;
    }
    pthread_mutex_unlock(&store.lock);
    return rc;
}

static int cb_open_keyspace(void * ud, chdb_object_storage_call * call, const char * key_prefix, uint32_t flags)
{
    keyspace_args a = {ud, call, key_prefix, flags};
    return dispatch(do_open_keyspace, &a);
}

static int do_close_keyspace(void * p)
{
    keyspace_args * a = p;
    count_call(a->ud);
    pthread_mutex_lock(&store.lock);
    store.keyspace_closes++;
    pthread_mutex_unlock(&store.lock);
    return 0;
}

static void cb_close_keyspace(void * ud, const char * key_prefix)
{
    keyspace_args a = {ud, NULL, key_prefix, 0};
    dispatch(do_close_keyspace, &a);
}

/* ---- test hooks ---- */

void mem_store_put(const char * key, const char * data, size_t size)
{
    void * handle;
    begin_args b = {NULL, NULL, key, &handle};
    do_write_begin(&b);
    append_args ap = {NULL, NULL, handle, data, size};
    do_write_append(&ap);
    commit_args c = {NULL, NULL, handle};
    do_write_commit(&c);
}

int mem_store_has(const char * key)
{
    pthread_mutex_lock(&store.lock);
    int found = find(key) != NULL;
    pthread_mutex_unlock(&store.lock);
    return found;
}

size_t mem_store_count_keys(const char * prefix, const char * skip_suffix)
{
    size_t n = 0;
    pthread_mutex_lock(&store.lock);
    for (size_t i = 0; i < store.count; ++i)
        if (strncmp(store.blobs[i].key, prefix, strlen(prefix)) == 0 && !(skip_suffix && ends_with(store.blobs[i].key, skip_suffix)))
            n++;
    pthread_mutex_unlock(&store.lock);
    return n;
}

size_t mem_store_calls(const struct mem_registration * registration)
{
    pthread_mutex_lock(&store.lock);
    size_t calls = registration->calls;
    pthread_mutex_unlock(&store.lock);
    return calls;
}

void mem_store_snapshot(struct mem_store_counts * out)
{
    pthread_mutex_lock(&store.lock);
    *out = (struct mem_store_counts){
        .count = store.count,
        .open_writes = store.open_writes,
        .peak_open_writes = store.peak_open_writes,
        .aborts = store.aborts,
        .aborts_after_commit = store.aborts_after_commit,
        .commits = store.commits,
        .copies = store.copies,
        .bytes_read = store.bytes_read,
        .bytes_written = store.bytes_written,
        .keyspace_opens = store.keyspace_opens,
        .keyspace_closes = store.keyspace_closes};
    pthread_mutex_unlock(&store.lock);
}

/* Set before the first registration, so every engine thread sees it. */
static void start_service(void)
{
    pthread_t thread;
    if (pthread_create(&thread, NULL, service_main, NULL) == 0)
    {
        pthread_detach(thread);
        service.enabled = 1;
    }
}

void mem_store_fill(chdb_object_storage_callbacks * cb, struct mem_registration * registration)
{
    static pthread_once_t started = PTHREAD_ONCE_INIT;
    if (getenv("CHDB_TEST_SERVICE_THREAD"))
        pthread_once(&started, start_service);

    memset(cb, 0, sizeof(*cb));
    cb->struct_size = sizeof(*cb);
    cb->ud = registration;
    cb->metadata = cb_metadata;
    cb->read = cb_read;
    cb->write_begin = cb_write_begin;
    cb->write_append = cb_write_append;
    cb->write_commit = cb_write_commit;
    cb->write_abort = cb_write_abort;
    cb->remove = cb_remove;
    cb->list = cb_list;
    if (!getenv("CHDB_TEST_NO_COPY"))
        cb->copy = cb_copy;
    cb->open_keyspace = cb_open_keyspace;
    cb->close_keyspace = cb_close_keyspace;
}
