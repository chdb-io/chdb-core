/**
 * objectStorageLifecycleTests.c - the guarantees chdbObjectStorageTest checks around the blob
 * operations themselves: registration refusals that name their reason, keyspace claims and the
 * host's open/close hooks, chdb_* calls from inside a callback, and the fence that
 * chdb_unregister_object_storage puts in front of a store whose disks are still open.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "chdb.h"
#include "objectStorageMemStore.h"
#include "objectStorageTestUtil.h"

static struct mem_registration fence_registration;

/* Every refusal leaves its reason in the caller's buffer. */
void registration_api_tests(const chdb_object_storage_callbacks * cb)
{
    char err[512] = "";
    check(chdb_register_object_storage("mem_null", NULL, err, sizeof(err)) == CHDBError && strstr(err, "NULL"),
        "a NULL table is refused with a reason");
    check(chdb_register_object_storage("", cb, NULL, 0) == CHDBError, "an empty name is refused");

    /* struct_size below the engine's sizeof is a header/library mismatch; a larger struct is a newer
     * caller whose extra fields are ignored (the engine copies only sizeof bytes). */
    chdb_object_storage_callbacks bad = *cb;
    bad.struct_size = sizeof(bad) - 4;
    err[0] = 0;
    check(chdb_register_object_storage("mem_short", &bad, err, sizeof(err)) == CHDBError && strstr(err, "struct_size"),
        "a short struct_size is refused with a reason");
    bad.struct_size = sizeof(bad) + 8;
    check(chdb_register_object_storage("mem_big", &bad, NULL, 0) == CHDBSuccess, "a larger struct_size is accepted");
    chdb_unregister_object_storage("mem_big", NULL, 0);

    bad = *cb;
    bad.read = NULL;
    bad.list = NULL;
    err[0] = 0;
    check(chdb_register_object_storage("mem_missing", &bad, err, sizeof(err)) == CHDBError && strstr(err, "read, list"),
        "missing required callbacks are named");

    bad = *cb;
    bad.close_keyspace = NULL;
    err[0] = 0;
    check(chdb_register_object_storage("mem_half", &bad, err, sizeof(err)) == CHDBError && strstr(err, "together"),
        "open_keyspace without close_keyspace is refused");

    bad = *cb;
    bad.copy = NULL;
    bad.open_keyspace = NULL;
    bad.close_keyspace = NULL;
    check(chdb_register_object_storage("mem_minimal", &bad, NULL, 0) == CHDBSuccess, "the optional callbacks may be NULL");
    chdb_unregister_object_storage("mem_minimal", NULL, 0);

    err[0] = 0;
    check(chdb_unregister_object_storage("mem_never", err, sizeof(err)) == CHDBError && strstr(err, "not registered"),
        "unregistering an unknown name names the reason");
}

/* Runs with db.t open on key_prefix 'main'. */
void keyspace_tests(chdb_connection conn)
{
    struct mem_store_counts before, after;
    mem_store_snapshot(&before);
    pthread_mutex_lock(&store.lock);
    unsigned flags = store.last_open_flags;
    pthread_mutex_unlock(&store.lock);
    check(before.keyspace_opens > before.keyspace_closes && flags == 0, "an open read-write disk opened its keyspace");

    /* 'main/' names the keys of 'main'. The second disk must be refused before it lists or sweeps
     * anything: a scratch blob that a live operation of the first disk could own stays. */
    mem_store_put("main/__tmp/live", "x", 1);
    char err[1024] = "";
    free(run(conn,
        "CREATE TABLE db.t_overlap (k UInt64) ENGINE = MergeTree ORDER BY k"
        " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'main/')",
        err, sizeof(err)));
    check(strstr(err, "overlaps") != NULL, "an overlapping key_prefix is refused");
    check(mem_store_has("main/__tmp/live"), "the refused disk swept nothing");
    mem_store_snapshot(&after);
    check(after.keyspace_opens == before.keyspace_opens, "the refused disk never opened its keyspace");

    /* The host's own gate: a keyspace another process holds refuses the disk with the host's reason. */
    pthread_mutex_lock(&store.lock);
    store.fail.open_keyspace = "locked";
    pthread_mutex_unlock(&store.lock);
    err[0] = 0;
    free(run(conn,
        "CREATE TABLE db.t_locked (k UInt64) ENGINE = MergeTree ORDER BY k"
        " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'locked')",
        err, sizeof(err)));
    pthread_mutex_lock(&store.lock);
    store.fail.open_keyspace = NULL;
    pthread_mutex_unlock(&store.lock);
    check(strstr(err, "CALLBACK_OBJECT_STORAGE_ERROR") && strstr(err, "keyspace locked by another process"),
        "open_keyspace refuses a disk with the host's reason");
    check(mem_store_count_keys("locked/", NULL) == 0, "the refused disk wrote nothing");
}

/* A callback that calls back into chDB on the connection running the statement is refused instead of
 * waiting for the connection mutex forever. Not observable with the service thread: the engine cannot
 * tell that thread is serving a callback, which is why chdb.h forbids such calls outright. */
void reentrancy_test(chdb_connection conn)
{
    if (mem_store_service_thread())
        return;
    check(run_ok(conn,
              "CREATE TABLE db.t_reenter (k UInt64) ENGINE = MergeTree ORDER BY k"
              " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'main')"),
        "CREATE TABLE for the re-entrancy probe");
    pthread_mutex_lock(&store.lock);
    store.reenter_conn = conn;
    store.reenter_query_refused = store.reenter_unregister_refused = 0;
    pthread_mutex_unlock(&store.lock);
    check(run_ok(conn, "INSERT INTO db.t_reenter SELECT number FROM numbers(10)"), "INSERT whose callback calls back into chDB");
    pthread_mutex_lock(&store.lock);
    int query_refused = store.reenter_query_refused, unregister_refused = store.reenter_unregister_refused;
    store.reenter_conn = NULL;
    pthread_mutex_unlock(&store.lock);
    check(query_refused, "chdb_query from a callback is refused");
    check(unregister_refused, "chdb_unregister_object_storage from a callback is refused");
    check(run_ok(conn, "DROP TABLE db.t_reenter SYNC"), "DROP the re-entrancy table");
}

/* Unregistering a store whose disk is open fences it: later operations fail without reaching the host. */
void fence_test(chdb_connection conn)
{
    chdb_object_storage_callbacks cb;
    mem_store_fill(&cb, &fence_registration);
    check(chdb_register_object_storage("mem_fence", &cb, NULL, 0) == CHDBSuccess, "register a second store while connected");
    check(run_ok(conn,
              "CREATE TABLE db.tf (k UInt64) ENGINE = MergeTree ORDER BY k"
              " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_fence', key_prefix = 'fence')"),
        "CREATE TABLE on the second store");
    check(run_ok(conn, "INSERT INTO db.tf SELECT number FROM numbers(1000)"), "INSERT into the second store");
    expect(conn, "the second store serves reads", "SELECT sum(k) FROM db.tf", "499500");

    char err[1024] = "";
    check(chdb_unregister_object_storage("mem_fence", err, sizeof(err)) == CHDBSuccess, "unregister while its disk is open");
    size_t calls = mem_store_calls(&fence_registration);
    err[0] = 0;
    free(run(conn, "SELECT sum(k) FROM db.tf", err, sizeof(err)));
    check(strstr(err, "CALLBACK_OBJECT_STORAGE_ERROR") && strstr(err, "unregistered"), "a fenced disk refuses reads");
    check(mem_store_calls(&fence_registration) == calls, "no callback reaches a fenced registration");

    err[0] = 0;
    check(chdb_register_object_storage("mem_fence", &cb, err, sizeof(err)) == CHDBError && strstr(err, "close every connection"),
        "re-registering while its disks are open is refused");
}

/* Once every connection is closed the fenced disk is gone and the name can be registered again. */
void fence_reregister(void)
{
    chdb_object_storage_callbacks cb;
    mem_store_fill(&cb, &fence_registration);
    check(chdb_register_object_storage("mem_fence", &cb, NULL, 0) == CHDBSuccess, "re-register once every connection is closed");
}

void fence_test_after_reconnect(chdb_connection conn)
{
    expect(conn, "rows committed before the fence are intact", "SELECT sum(k) FROM db.tf", "499500");
    check(run_ok(conn, "DROP TABLE db.tf SYNC"), "DROP the second store's table");
}

/* Writes a table under key_prefix 'ro' for read_only_attach_test; returns its UUID (caller frees). */
char * read_only_source(chdb_connection conn)
{
    check(run_ok(conn,
              "CREATE TABLE db.t_ro (k UInt64) ENGINE = MergeTree ORDER BY k"
              " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'ro')"),
        "CREATE TABLE for the read-only attach");
    check(run_ok(conn, "INSERT INTO db.t_ro SELECT number FROM numbers(100)"), "INSERT for the read-only attach");
    return run(conn, "SELECT uuid FROM system.tables WHERE database = 'db' AND name = 't_ro'", NULL, 0);
}

/* A second engine, on its own --path, attaches the same table from a read_only = 1 disk: the host is
 * told so in open_keyspace, reads work and writes are refused. Needs every connection closed first. */
void read_only_attach_test(const char * uuid)
{
    char path[] = "/tmp/chdb_object_storage_ro_XXXXXX";
    if (!uuid || !mkdtemp(path))
    {
        check(0, "read-only attach set up");
        return;
    }
    char arg0[] = "chdb";
    char arg1[600];
    snprintf(arg1, sizeof(arg1), "--path=%s", path);
    char * args[] = {arg0, arg1};
    chdb_connection * conn = chdb_connect(2, args);
    check(conn && *conn, "connect a second --path to the same store");
    if (conn && *conn)
    {
        struct mem_store_counts before, after;
        mem_store_snapshot(&before);
        char sql[512];
        snprintf(sql, sizeof(sql),
            "ATTACH TABLE db.t_ro UUID '%s' (k UInt64) ENGINE = MergeTree ORDER BY k"
            " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'ro', read_only = 1)",
            uuid);
        check(run_ok(*conn, "CREATE DATABASE db") && run_ok(*conn, sql), "ATTACH from a read_only = 1 disk");
        mem_store_snapshot(&after);
        pthread_mutex_lock(&store.lock);
        unsigned flags = store.last_open_flags;
        pthread_mutex_unlock(&store.lock);
        check(after.keyspace_opens == before.keyspace_opens + 1 && flags == CHDB_OBJECT_STORAGE_READ_ONLY,
            "open_keyspace is told the disk is read-only");
        expect(*conn, "the read-only table reads the rows", "SELECT count(), sum(k) FROM db.t_ro", "100\t4950");
        char err[512] = "";
        free(run(*conn, "INSERT INTO db.t_ro VALUES (1000)", err, sizeof(err)));
        check(strstr(err, "readonly") != NULL, "the read-only table refuses writes");
        chdb_close_conn(conn);
    }
    char cmd[700];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    if (system(cmd) != 0) { /* best effort */ }
}
