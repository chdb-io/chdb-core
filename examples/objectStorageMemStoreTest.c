/* Exercises the reference store without starting the engine. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chdb.h"
#include "objectStorageMemStore.h"

/* The host-only test must not enter chDB. */
chdb_result * chdb_query(chdb_connection conn, const char * query, const char * format)
{
    (void)conn; (void)query; (void)format;
    abort();
}

chdb_result * chdb_query_cmdline(int argc, char ** argv)
{
    (void)argc; (void)argv;
    abort();
}

struct local_result * query_stable(int argc, char ** argv)
{
    (void)argc; (void)argv;
    abort();
}

struct local_result_v2 * query_stable_v2(int argc, char ** argv)
{
    (void)argc; (void)argv;
    abort();
}

void free_result(struct local_result * result)
{
    (void)result;
    abort();
}

void free_result_v2(struct local_result_v2 * result)
{
    (void)result;
    abort();
}

const char * chdb_result_error(chdb_result * result)
{
    (void)result;
    abort();
}

void chdb_destroy_query_result(chdb_result * result)
{
    (void)result;
    abort();
}

void chdb_destroy_insert_stream(chdb_insert_stream stream)
{
    (void)stream;
    abort();
}

chdb_state chdb_unregister_object_storage(const char * name, char * error, size_t error_size)
{
    (void)name; (void)error; (void)error_size;
    abort();
}

static void expect_blob(const chdb_object_storage_callbacks * cb, chdb_object_storage_call * call,
    const char * key, const char * expected)
{
    uint64_t size = 0;
    int64_t mtime = 0;
    assert(cb->metadata(cb->ud, call, key, &size, &mtime) == CHDB_OBJECT_STORAGE_OK);
    assert(size == strlen(expected));
    char data[64];
    size_t count = 0;
    assert(cb->read(cb->ud, call, key, 0, data, sizeof(data), &count) == CHDB_OBJECT_STORAGE_OK);
    assert(count == size && memcmp(data, expected, count) == 0);
}

int main(void)
{
    unsetenv("CHDB_TEST_NO_COPY");
    chdb_object_storage_callbacks cb;
    mem_store_fill(&cb, NULL);
    char error[256] = "";
    chdb_object_storage_call call = {sizeof(call), error, sizeof(error)};
    mem_store_put("source", "abcdefghij", 10);

    for (int i = 0; i < 64; ++i)
    {
        char key[32];
        snprintf(key, sizeof(key), "copy-%d", i);
        assert(cb.copy(cb.ud, &call, "source", key) == CHDB_OBJECT_STORAGE_OK);
        expect_blob(&cb, &call, key, "abcdefghij");
        assert(cb.remove(cb.ud, &call, key) == CHDB_OBJECT_STORAGE_OK);
    }
    mem_store_put("target", "old", 3);
    assert(cb.copy(cb.ud, &call, "source", "target") == CHDB_OBJECT_STORAGE_OK);
    expect_blob(&cb, &call, "target", "abcdefghij");
    assert(cb.copy(cb.ud, &call, "source", "source") == CHDB_OBJECT_STORAGE_OK);
    expect_blob(&cb, &call, "source", "abcdefghij");
    mem_store_put("empty", "", 0);
    assert(cb.copy(cb.ud, &call, "empty", "empty-copy") == CHDB_OBJECT_STORAGE_OK);
    expect_blob(&cb, &call, "empty-copy", "");
    assert(cb.copy(cb.ud, &call, "missing", "target") == CHDB_OBJECT_STORAGE_NOT_FOUND);
    expect_blob(&cb, &call, "target", "abcdefghij");

    for (const char ** key = (const char *[]){"source", "target", "empty", "empty-copy", NULL}; *key; ++key)
        assert(cb.remove(cb.ud, &call, *key) == CHDB_OBJECT_STORAGE_OK);
    puts("PASS: reference blob store");
    return 0;
}
