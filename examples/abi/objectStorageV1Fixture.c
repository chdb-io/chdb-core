#include "objectStorageV1Fixture.h"
#include <string.h>

static char context[] = "frozen-client";

static int metadata(void * ud, chdb_object_storage_call * call, const char * key, uint64_t * size, int64_t * mtime)
{
    (void)ud; (void)call; (void)key;
    *size = 7;
    *mtime = 123;
    return 0;
}
static int read_blob(void * ud, chdb_object_storage_call * call, const char * key,
    uint64_t offset, void * data, size_t len, size_t * count)
{
    (void)ud; (void)call; (void)key;
    const char blob[] = "content";
    *count = offset >= 7 ? 0 : 7 - (size_t)offset;
    if (*count > len)
        *count = len;
    if (*count)
        memcpy(data, blob + offset, *count);
    return 0;
}
static int begin(void * ud, chdb_object_storage_call * call, const char * key, void ** handle)
{ (void)call; (void)key; *handle = ud; return 0; }
static int append(void * ud, chdb_object_storage_call * call, void * handle, const void * data, size_t len)
{ (void)ud; (void)call; (void)handle; (void)data; (void)len; return 0; }
static int commit(void * ud, chdb_object_storage_call * call, void * handle)
{ (void)ud; (void)call; (void)handle; return 0; }
static void cancel(void * ud, void * handle)
{ (void)ud; (void)handle; }
static int remove_blob(void * ud, chdb_object_storage_call * call, const char * key)
{ (void)ud; (void)call; (void)key; return 0; }
static int list(void * ud, chdb_object_storage_call * call, const char * prefix,
    chdb_object_storage_list_sink sink, void * sink_ud)
{ (void)ud; (void)call; (void)prefix; (void)sink; (void)sink_ud; return 0; }

int register_frozen_object_storage_v1(void)
{
    chdb_object_storage_callbacks cb = {0};
    cb.struct_size = sizeof(cb);
    cb.version = 1;
    cb.ud = context;
    cb.metadata = metadata;
    cb.read = read_blob;
    cb.write_begin = begin;
    cb.write_append = append;
    cb.write_commit = commit;
    cb.write_abort = cancel;
    cb.remove = remove_blob;
    cb.list = list;
    char error[128];
    return chdb_register_object_storage("frozen-client", &cb, error, sizeof(error));
}

size_t frozen_object_storage_v1_size(void) { return sizeof(chdb_object_storage_callbacks); }
