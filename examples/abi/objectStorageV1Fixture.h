/* Independent v1 declarations catch accidental layout changes in the production header. */
#pragma once
#include <stddef.h>
#include <stdint.h>

typedef enum chdb_state { CHDBSuccess = 0, CHDBError = 1 } chdb_state;
typedef struct chdb_object_storage_call
{
    uint32_t struct_size;
    char * error;
    size_t error_size;
} chdb_object_storage_call;
typedef int (*chdb_object_storage_list_sink)(void *, const char *, uint64_t, int64_t);
typedef struct chdb_object_storage_callbacks
{
    uint32_t struct_size;
    uint32_t version;
    void * ud;
    int (*metadata)(void *, chdb_object_storage_call *, const char *, uint64_t *, int64_t *);
    int (*read)(void *, chdb_object_storage_call *, const char *, uint64_t, void *, size_t, size_t *);
    int (*write_begin)(void *, chdb_object_storage_call *, const char *, void **);
    int (*write_append)(void *, chdb_object_storage_call *, void *, const void *, size_t);
    int (*write_commit)(void *, chdb_object_storage_call *, void *);
    void (*write_abort)(void *, void *);
    int (*remove)(void *, chdb_object_storage_call *, const char *);
    int (*list)(void *, chdb_object_storage_call *, const char *, chdb_object_storage_list_sink, void *);
    int (*copy)(void *, chdb_object_storage_call *, const char *, const char *);
    int (*open_keyspace)(void *, chdb_object_storage_call *, const char *, uint32_t);
    void (*close_keyspace)(void *, const char *);
} chdb_object_storage_callbacks;

chdb_state chdb_register_object_storage(const char *, const void *, char *, size_t);
chdb_state chdb_unregister_object_storage(const char *, char *, size_t);
