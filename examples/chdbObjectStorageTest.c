/**
 * chdbObjectStorageTest.c - a host-supplied object storage ("callback disk").
 *
 * The host here keeps every blob in a mutex-guarded in-memory
 * table and hands the engine function pointers through
 * chdb_register_object_storage(). A MergeTree table on
 * disk(type = 'callback', storage_name = ...) then keeps all its parts, text
 * index and vector index in that table, and a second engine start on the same
 * --path finds them again. A real host (a database extension, say) would put
 * the blobs in its own durable pages instead of malloc.
 *
 * Build: clang examples/chdbObjectStorageTest.c examples/objectStorageMemStore.c examples/objectStorageDiskTests.c \
 *          examples/objectStorageFaultTests.c examples/objectStorageLifecycleTests.c -I./programs/local -L. -lchdb \
 *          -lpthread -o examples/chdbObjectStorageTest
 * Run:   LD_LIBRARY_PATH=. ./examples/chdbObjectStorageTest
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "chdb.h"
#include "objectStorageMemStore.h"
#include "objectStorageTestUtil.h"

/* ---- the test ---- */

int g_failed = 0;

void check(int ok, const char * label)
{
    printf("%-52s %s\n", label, ok ? "ok" : "FAIL");
    if (!ok)
        g_failed++;
}

/* Runs a query; returns the result text (caller frees) or NULL, copying the error into err when given. */
char * run(chdb_connection conn, const char * sql, char * err, size_t err_len)
{
    chdb_result * r = chdb_query(conn, sql, "TabSeparated");
    const char * e = chdb_result_error(r);
    char * out = NULL;
    if (e)
    {
        if (err)
            snprintf(err, err_len, "%s", e);
        else
            printf("  query failed: %s\n    %s\n", e, sql);
    }
    else
    {
        size_t len = chdb_result_length(r);
        out = malloc(len + 1);
        if (len) /* an empty result has no buffer */
            memcpy(out, chdb_result_buffer(r), len);
        out[len] = 0;
        if (len && out[len - 1] == '\n')
            out[len - 1] = 0;
    }
    chdb_destroy_query_result(r);
    return out;
}

int run_ok(chdb_connection conn, const char * sql)
{
    char * out = run(conn, sql, NULL, 0);
    int ok = out != NULL;
    free(out);
    return ok;
}

int expect(chdb_connection conn, const char * label, const char * sql, const char * want)
{
    char * got = run(conn, sql, NULL, 0);
    int ok = got && strcmp(got, want) == 0;
    check(ok, label);
    if (!ok)
        printf("  want '%s', got '%s'\n", want, got ? got : "(error)");
    free(got);
    return ok;
}

void verify(chdb_connection conn, const char * phase, const char * rows, const char * word7)
{
    char label[96];
    snprintf(label, sizeof(label), "%s: row count", phase);
    expect(conn, label, "SELECT count() FROM t", rows);
    snprintf(label, sizeof(label), "%s: hasAllTokens", phase);
    expect(conn, label, "SELECT count() FROM t WHERE hasAllTokens(s, 'word7 common')", word7);
    snprintf(label, sizeof(label), "%s: nearest by cosineDistance", phase);
    expect(conn, label,
        "SELECT countIf(k = 54321) FROM (SELECT k FROM t ORDER BY cosineDistance(v, " VEC(54321) ") LIMIT 10"
        " SETTINGS use_skip_indexes = 1)",
        "1");
}

/* Removes the temporary --path and returns rc, so every exit after mkdtemp cleans up. */
static int finish(int rc, const char * path)
{
    char cmd[700];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    if (system(cmd) != 0) { /* best effort */ }
    return rc;
}

int main(void)
{
    time_t start = time(NULL);
    chdb_object_storage_callbacks cb;
    mem_store_fill(&cb, NULL);
    printf("callbacks run on %s\n", mem_store_service_thread() ? "one service thread" : "the engine threads");

    /* Registration works before any connection exists. */
    char err[512] = "";
    check(chdb_register_object_storage("mem_store", &cb, NULL, 0) == CHDBSuccess, "register before connect");
    check(chdb_register_object_storage("mem_store", &cb, err, sizeof(err)) == CHDBError && strstr(err, "already registered"),
        "duplicate registration is refused with a reason");
    registration_api_tests(&cb);

    /* A commit with no append stores a zero-byte blob, which reads back as 0 bytes (no NULL memcpy);
     * a missing key is NOT_FOUND, which remove treats as success. */
    char message[128];
    chdb_object_storage_call call = {sizeof(call), message, sizeof(message)};
    void * handle;
    uint64_t size = 1;
    int64_t mtime = 0;
    size_t got = 1;
    char byte;
    check(cb.write_begin(cb.ud, &call, "empty", &handle) == CHDB_OBJECT_STORAGE_OK && cb.write_commit(cb.ud, &call, handle) == CHDB_OBJECT_STORAGE_OK
            && cb.metadata(cb.ud, &call, "empty", &size, &mtime) == CHDB_OBJECT_STORAGE_OK && size == 0
            && cb.read(cb.ud, &call, "empty", 0, &byte, 1, &got) == CHDB_OBJECT_STORAGE_OK && got == 0
            && cb.remove(cb.ud, &call, "empty") == CHDB_OBJECT_STORAGE_OK
            && cb.metadata(cb.ud, &call, "empty", &size, &mtime) == CHDB_OBJECT_STORAGE_NOT_FOUND,
        "zero-byte blob round-trips through the table");

    char path_template[] = "/tmp/chdb_object_storage_test_XXXXXX";
    if (!mkdtemp(path_template))
    {
        printf("mkdtemp failed\n");
        return 1;
    }
    char arg0[] = "chdb";
    char arg1[600];
    snprintf(arg1, sizeof(arg1), "--path=%s", path_template);
    char * args[] = {arg0, arg1};

    /* A scratch copy a crashed operation left behind, under the disk's key_prefix. */
    mem_store_put("main/__tmp/orphan", "x", 1);

    chdb_connection * conn = chdb_connect(2, args);
    if (!conn || !*conn)
    {
        printf("chdb_connect failed\n");
        return finish(1, path_template);
    }

    check(run_ok(*conn, "CREATE DATABASE IF NOT EXISTS db"), "CREATE DATABASE");
    /* The two block columns make lightweight DELETE possible on an immutable disk; see the
     * "MergeTree limits" paragraph in chdb.h. */
    int created = run(*conn,
        "CREATE TABLE db.t (k UInt64, s String, v Array(Float32),"
        " INDEX ti s TYPE text(tokenizer = splitByNonAlpha),"
        " INDEX vi v TYPE vector_similarity('hnsw', 'cosineDistance', 8))"
        " ENGINE = MergeTree ORDER BY k SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,"
        " disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'main')",
        NULL, 0) != NULL;
    check(created, "CREATE TABLE on callback disk");
    if (!created)
        return finish(1, path_template);
    check(!mem_store_has("main/__tmp/orphan"), "startup sweeps __tmp scratch blobs");
    keyspace_tests(*conn);
    check(run_ok(*conn, "USE db"), "USE db");

    check(run_ok(*conn, INSERT_100K_ROWS), "INSERT 100000 rows");
    expect(*conn, "inserted 100000 rows", "SELECT count() FROM t", "100000");
    struct mem_store_counts counts;
    mem_store_snapshot(&counts);
    check(counts.count > 0 && counts.open_writes == 0, "parts live in the host store");
    metrics_test(*conn);
    verify(*conn, "after insert", "100000", "100");
    prefixed_table_test(*conn);
    reentrancy_test(*conn);
    fence_test(*conn);

    /* Host failures surface as the dedicated error and release every handle. */
    fault_tests(*conn);

    /* The only DELETE an immutable disk allows ("MergeTree limits" in chdb.h). */
    check(run_ok(*conn, "SET lightweight_delete_mode = 'lightweight_update_force'"), "SET lightweight_delete_mode");
    check(run_ok(*conn, "DELETE FROM t WHERE k < 1000"), "lightweight DELETE on callback disk");
    verify(*conn, "after DELETE", "99000", "99");

    /* The merge, the rename to the final name and the removal of the old parts all run on the disk;
     * an unmerged table answers the same verify() queries, so the outcome is asserted. */
    check(run_ok(*conn, "OPTIMIZE TABLE t FINAL"), "OPTIMIZE FINAL on callback disk");
    expect(*conn, "one active data part after FINAL", ACTIVE_DATA_PARTS, "1");
    expect(*conn, "merged part has level > 0",
        "SELECT max(level) > 0 FROM system.parts WHERE database = 'db' AND table = 't' AND active AND NOT startsWith(name, 'patch-')", "1");
    verify(*conn, "after OPTIMIZE FINAL", "99000", "99");
    part_removal_test(*conn, cb.copy != NULL);
    detach_attach_test(*conn);

    chdb_close_conn(conn);
    /* Shutdown joined the parts-cleanup thread, whose delete_tmp_ renames also open handles. */
    mem_store_snapshot(&counts);
    check(counts.open_writes == 0, "no write handle left open");
    /* Every disk closed its keyspace at disconnect, except the fenced one: nothing reaches its host. */
    check(counts.keyspace_opens == counts.keyspace_closes + 1, "keyspaces are closed at disconnect");

    /* Reopening the --path attaches the table, which needs the store: registration must come first. */
    check(chdb_unregister_object_storage("mem_store", NULL, 0) == CHDBSuccess, "unregister between runs");
    conn = chdb_connect(2, args);
    check(!conn || !*conn, "reconnect without registration is refused");
    if (conn && *conn)
        chdb_close_conn(conn);
    check(chdb_register_object_storage("mem_store", &cb, NULL, 0) == CHDBSuccess, "re-register");
    fence_reregister();

    list_fault_at_reconnect(2, args);
    mem_store_put("main/__tmp/orphan2", "x", 1);
    conn = chdb_connect(2, args);
    check(conn && *conn, "reconnect on the same --path");
    if (!conn || !*conn)
        return finish(1, path_template);
    check(!mem_store_has("main/__tmp/orphan2"), "the sweep runs again for a re-registered store");
    check(run_ok(*conn, "USE db"), "USE db after restart");
    verify(*conn, "after restart", "99000", "99");
    expect(*conn, "one active data part after restart", ACTIVE_DATA_PARTS, "1");

    /* The blob mtime the host reported at commit comes back as the part's modification time. */
    char mtime_sql[320];
    snprintf(mtime_sql, sizeof(mtime_sql),
        "SELECT min(modification_time) >= toDateTime(%ld) AND max(modification_time) <= now() + 60"
        " FROM system.parts WHERE database = 'db' AND table = 't' AND active", (long)start);
    expect(*conn, "mtime is the commit time", mtime_sql, "1");
    mem_store_snapshot(&counts);
    check(counts.peak_open_writes > 2, "many handles pend at once");

    /* The synchronous read method refills the gather's own buffer through the callback buffer
     * (SwapHelper), which the default threadpool path never does. */
    check(run_ok(*conn, "SET remote_filesystem_read_method = 'read', use_page_cache_for_disks_without_file_cache = 0"),
        "switch to the synchronous read method");
    verify(*conn, "after restart, read method", "99000", "99");
    /* The callback buffer declares seeks cheap, so two mark ranges in one stream never make the
     * gather read and discard the gap through the host (RemoteFSLazySeeks stays unchanged). */
    char * seeks_before = run(*conn, "SELECT value FROM system.events WHERE event = 'RemoteFSLazySeeks'", NULL, 0);
    check(run_ok(*conn, "SELECT s FROM t WHERE k IN (1000, 50000) FORMAT Null"), "two mark ranges in one stream");
    char * seeks_after = run(*conn, "SELECT value FROM system.events WHERE event = 'RemoteFSLazySeeks'", NULL, 0);
    check(seeks_before && seeks_after && strcmp(seeks_before, seeks_after) == 0, "no lazy seeks on the callback disk");
    free(seeks_before);
    free(seeks_after);

    /* A reverse in-order scan seeks backwards through every column stream; the sum is exact. */
    expect(*conn, "after restart, read method: reverse scan", "SELECT sum(k) FROM (SELECT k FROM t ORDER BY k DESC LIMIT 99000)",
        "4999450500");

    fence_test_after_reconnect(*conn);

    /* Removal paths: TRUNCATE writes the 0-byte covering part, and DROP SYNC removes the table tree.
     * Only directory markers of the store/ ancestors may remain. */
    check(run_ok(*conn, "TRUNCATE TABLE t"), "TRUNCATE on callback disk");
    check(run_ok(*conn, "DROP TABLE db.t SYNC"), "DROP TABLE SYNC on callback disk");
    mem_store_snapshot(&counts);
    check(counts.open_writes == 0, "no write handle left open after DROP");
    size_t leftovers = mem_store_count_keys("", "/prefix.path");
    check(leftovers == 0, "DROP TABLE SYNC leaves only directory markers");
    if (leftovers)
    {
        pthread_mutex_lock(&store.lock);
        for (size_t i = 0; i < store.count; ++i)
            if (!strstr(store.blobs[i].key, "/prefix.path"))
                printf("  leftover %s (%zu bytes)\n", store.blobs[i].key, store.blobs[i].size);
        pthread_mutex_unlock(&store.lock);
    }
    char * ro_uuid = read_only_source(*conn);
    chdb_close_conn(conn);
    read_only_attach_test(ro_uuid);
    free(ro_uuid);

    /* A clean exit hook: close connections, stop the engine, then unregister. Only the unregister is
     * needed for safety (it fences the store); the first two let merges finish. chdb_shutdown()
     * reports CHDBError when a thread it cannot reach is still parked (it does after any MergeTree
     * insert in this build, callback disk or not); either way the engine is closed for the rest of
     * the process. */
    chdb_state stopped = chdb_shutdown();
    if (stopped != CHDBSuccess)
        printf("  note: chdb_shutdown left some thread running\n");
    check(chdb_connect(2, args) == NULL, "shutdown before unregister closes the engine");
    mem_store_snapshot(&counts);
    check(counts.keyspace_opens == counts.keyspace_closes + 1, "the reconnected disks closed their keyspaces");
    check(chdb_unregister_object_storage("mem_store", NULL, 0) == CHDBSuccess, "unregister");
    check(chdb_unregister_object_storage("mem_fence", NULL, 0) == CHDBSuccess, "unregister the second store");
    check(chdb_unregister_object_storage("mem_store", NULL, 0) == CHDBError, "unregister of an unknown name is an error");

    printf("%s\n", g_failed ? "FAIL" : "PASS");
    return finish(g_failed ? 1 : 0, path_template);
}
