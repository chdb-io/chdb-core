/**
 * objectStorageMemStore.c - the host side of chdbObjectStorageTest: an in-memory
 * blob store behind chdb_object_storage_callbacks. The engine calls these
 * concurrently, so one mutex guards the whole store. A real host would put the
 * blobs in its own durable pages instead of malloc.
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

static __thread char last_error_text[256];

static int set_error(const char * what, const char * key)
{
    snprintf(last_error_text, sizeof(last_error_text), "%s: %s", what, key);
    return 1;
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

static int cb_exists(void * ud, const char * key, int * out)
{
    (void)ud;
    pthread_mutex_lock(&store.lock);
    *out = find(key) != NULL;
    pthread_mutex_unlock(&store.lock);
    return 0;
}

static int cb_metadata(void * ud, const char * key, int * found, uint64_t * size, int64_t * mtime)
{
    (void)ud;
    pthread_mutex_lock(&store.lock);
    blob * b = find(key);
    *found = b != NULL;
    if (b)
    {
        *size = b->size;
        *mtime = b->mtime;
    }
    pthread_mutex_unlock(&store.lock);
    return 0;
}

static int cb_read(void * ud, const char * key, uint64_t offset, void * buf, size_t len, size_t * out)
{
    (void)ud;
    int rc = 0;
    pthread_mutex_lock(&store.lock);
    blob * b = find(key);
    if (store.fail.read || (store.fail.read_suffix && ends_with(key, store.fail.read_suffix)))
        rc = set_error("injected read failure", key);
    else if (!b)
        rc = set_error("read of missing blob", key);
    else
    {
        size_t n = offset >= b->size ? 0 : (b->size - offset < len ? b->size - offset : len);
        if (n) /* a zero-byte blob has no data pointer */
            memcpy(buf, b->data + offset, n);
        *out = store.fail.read_overcount ? len + 1 : n;
        store.bytes_read += n;
    }
    pthread_mutex_unlock(&store.lock);
    return rc;
}

static int cb_write_begin(void * ud, const char * key, void ** handle)
{
    (void)ud;
    pending_write * w = calloc(1, sizeof(*w));
    if (w)
        w->key = strdup(key);
    if (!w || !w->key)
    {
        free(w);
        return set_error("out of memory", key);
    }
    pthread_mutex_lock(&store.lock);
    store.open_writes++;
    if (store.open_writes > store.peak_open_writes)
        store.peak_open_writes = store.open_writes;
    if (store.last_failed_commit == w) /* the address was reused */
        store.last_failed_commit = NULL;
    pthread_mutex_unlock(&store.lock);
    *handle = w;
    return 0;
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

static int cb_write_append(void * ud, void * handle, const void * buf, size_t len)
{
    (void)ud;
    pending_write * w = handle;
    int rc = 0;
    pthread_mutex_lock(&store.lock);
    store.bytes_written += len;
    if (store.fail.write_append || (store.fail.append_after_bytes && store.bytes_written > store.fail.append_after_bytes))
        rc = set_error("injected write failure", w->key);
    pthread_mutex_unlock(&store.lock);
    if (rc)
        return rc;
    if (w->size + len > w->cap)
    {
        size_t cap = (w->size + len) * 2;
        char * data = realloc(w->data, cap);
        if (!data)
            return set_error("out of memory", w->key);
        w->data = data;
        w->cap = cap;
    }
    memcpy(w->data + w->size, buf, len);
    w->size += len;
    return 0;
}

/* Commit releases the handle whether or not it succeeds: no write_abort follows. */
static int cb_write_commit(void * ud, void * handle)
{
    (void)ud;
    pending_write * w = handle;
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
    int rc = failure ? set_error(failure, w->key) : 0;
    drop_write(w);
    return rc;
}

static int cb_write_abort(void * ud, void * handle)
{
    (void)ud;
    pthread_mutex_lock(&store.lock);
    if (handle == store.last_failed_commit)
    {
        store.aborts_after_commit++;
        store.last_failed_commit = NULL;
        pthread_mutex_unlock(&store.lock);
        return 0;
    }
    store.aborts++;
    pthread_mutex_unlock(&store.lock);
    drop_write(handle);
    return 0;
}

static int cb_remove(void * ud, const char * key)
{
    (void)ud;
    pthread_mutex_lock(&store.lock);
    blob * b = find(key);
    if (b)
    {
        free(b->key);
        free(b->data);
        *b = store.blobs[--store.count];
    }
    pthread_mutex_unlock(&store.lock);
    return 0;
}

/* Duplicates a blob under the lock; the engine uses it for hard links, moves and part removal. */
static int cb_copy(void * ud, const char * from_key, const char * to_key)
{
    (void)ud;
    int rc = 0;
    pthread_mutex_lock(&store.lock);
    blob * from = find(from_key);
    char * data = from ? malloc(from->size ? from->size : 1) : NULL;
    size_t size = from ? from->size : 0;
    if (data && size)
        memcpy(data, from->data, size);
    blob * to = data ? find(to_key) : NULL; /* add_blob may move the array, so from is not used past here */
    if (data && !to)
        to = add_blob(to_key);
    if (!from)
        rc = set_error("copy of missing blob", from_key);
    else if (!to)
        rc = set_error("out of memory", to_key);
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

static int cb_list(void * ud, const char * prefix, chdb_object_storage_list_sink sink, void * sink_ud)
{
    (void)ud;
    size_t n = strlen(prefix);
    int rc = 0;
    pthread_mutex_lock(&store.lock);
    if (store.fail.list)
        rc = set_error("injected list failure", prefix);
    for (size_t i = 0; !rc && i < store.count; ++i)
        if (strncmp(store.blobs[i].key, prefix, n) == 0
            && sink(sink_ud, store.blobs[i].key, store.blobs[i].size, store.blobs[i].mtime))
            break;
    pthread_mutex_unlock(&store.lock);
    return rc;
}

static const char * cb_last_error(void * ud)
{
    (void)ud;
    return last_error_text;
}

void mem_store_put(const char * key, const char * data, size_t size)
{
    void * handle;
    cb_write_begin(NULL, key, &handle);
    cb_write_append(NULL, handle, data, size);
    cb_write_commit(NULL, handle);
}

int mem_store_has(const char * key)
{
    int out;
    cb_exists(NULL, key, &out);
    return out;
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
        .bytes_written = store.bytes_written};
    pthread_mutex_unlock(&store.lock);
}

void mem_store_fill(chdb_object_storage_callbacks * cb)
{
    memset(cb, 0, sizeof(*cb));
    cb->struct_size = sizeof(*cb);
    cb->exists = cb_exists;
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
    cb->last_error = cb_last_error;
}
