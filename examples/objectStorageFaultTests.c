/**
 * objectStorageFaultTests.c - injected host failures for chdbObjectStorageTest.
 *
 * Each case arms one fault in the in-memory store, runs a statement that must
 * fail with CALLBACK_OBJECT_STORAGE_ERROR, and checks what the engine did with
 * the handles it had open. The table must hold rows when fault_tests runs: the
 * read fault needs a query that actually reads a column.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "chdb.h"
#include "objectStorageMemStore.h"
#include "objectStorageTestUtil.h"

static void set_flag(int * flag, int value)
{
    pthread_mutex_lock(&store.lock);
    *flag = value;
    pthread_mutex_unlock(&store.lock);
}

/* Runs sql expecting it to fail with the dedicated error code and `text` in the message. */
static int expect_error(chdb_connection conn, const char * label, const char * sql, const char * text)
{
    char err[1024] = "";
    char * out = run(conn, sql, err, sizeof(err));
    int ok = out == NULL && strstr(err, "CALLBACK_OBJECT_STORAGE_ERROR") && strstr(err, text);
    check(ok, label);
    if (!ok)
        printf("  got '%s'\n", out ? out : err);
    free(out);
    return ok;
}

/* Arms *flag around one failing statement. */
static int fault_case(chdb_connection conn, const char * label, int * flag, const char * sql, const char * text)
{
    set_flag(flag, 1);
    int ok = expect_error(conn, label, sql, text);
    set_flag(flag, 0);
    return ok;
}

/* A background merge that trips an armed fault would abort its own streams and be counted here. */
static void quiesce_merges(chdb_connection conn)
{
    run_ok(conn, "SYSTEM STOP MERGES t");
    for (int i = 0; i < 600; ++i)
    {
        char * running = run(conn, "SELECT count() FROM system.merges WHERE database = 'db' AND table = 't'", NULL, 0);
        int idle = running && strcmp(running, "0") == 0;
        free(running);
        if (idle)
            return;
        usleep(100 * 1000);
    }
    check(0, "merges on t finished before fault injection");
}

void fault_tests(chdb_connection conn)
{
    quiesce_merges(conn);
    char * rows = run(conn, "SELECT count() FROM t", NULL, 0);
    struct mem_store_counts before, now;
    mem_store_snapshot(&before);

    /* 1. The first write_commit of the INSERT fails while the part writer's other streams are still
     *    open; those are aborted, the failed one was released inside commit and must not be. */
    fault_case(conn, "commit fault surfaces as CALLBACK_OBJECT_STORAGE_ERROR", &store.fail.write_commit,
        "INSERT INTO t SELECT number, 'x', [0, 0, 0, 0, 0, 0, 0, 1] FROM numbers(10)", "injected commit failure");
    mem_store_snapshot(&now);
    check(now.open_writes == 0 && now.aborts_after_commit == 0, "failed commit released its handle without abort");
    if (now.open_writes || now.aborts_after_commit)
        printf("  open_writes %zu, aborts of a committed handle %zu\n", now.open_writes, now.aborts_after_commit);

    /* 2. An append that fails after 2 MiB hits a part writer with every column stream open, so each
     *    one is aborted. The store may keep blobs the engine committed before the failure (sibling
     *    files of the temporary part): plain_rewritable's undo runs only from commit(), so they are
     *    orphaned rather than removed. */
    pthread_mutex_lock(&store.lock);
    store.fail.append_after_bytes = store.bytes_written + (2u << 20);
    pthread_mutex_unlock(&store.lock);
    size_t count_before = mem_store_count_keys("", "/prefix.path");
    expect_error(conn, "late append fault surfaces as CALLBACK_OBJECT_STORAGE_ERROR", INSERT_100K_ROWS, "injected write failure");
    pthread_mutex_lock(&store.lock);
    store.fail.append_after_bytes = 0;
    pthread_mutex_unlock(&store.lock);
    mem_store_snapshot(&now);
    check(now.open_writes == 0 && now.aborts >= before.aborts + 2, "late append aborts every open stream");
    if (now.open_writes || now.aborts < before.aborts + 2)
        printf("  open_writes %zu, aborts %zu -> %zu\n", now.open_writes, before.aborts, now.aborts);
    size_t orphans = mem_store_count_keys("", "/prefix.path") - count_before;
    if (orphans)
        printf("  note: the failed INSERT left %zu blobs the engine never reads\n", orphans);

    /* 3. A failing read is the dedicated error too, and nothing stays broken once it is cleared. */
    fault_case(conn, "read fault surfaces as CALLBACK_OBJECT_STORAGE_ERROR", &store.fail.read, "SELECT sum(k) FROM t",
        "injected read failure");
    check(run_ok(conn, "SELECT sum(k) FROM t"), "readable again once the read fault is cleared");

    /* A read that claims more bytes than the buffer holds is a host bug, reported as the same error
     * rather than as an engine invariant (which would abort a debug build). */
    fault_case(conn, "over-long read is a callback error", &store.fail.read_overcount, "SELECT sum(k) FROM t", "returned");
    check(run_ok(conn, "SELECT sum(k) FROM t"), "readable again once the over-long read is cleared");

    /* A host may return fewer bytes than asked; only a 0-byte read ends a blob. */
    pthread_mutex_lock(&store.lock);
    store.max_read = 1000;
    pthread_mutex_unlock(&store.lock);
    verify(conn, "reads of at most 1000 bytes", "100000", "100");
    pthread_mutex_lock(&store.lock);
    store.max_read = 0;
    pthread_mutex_unlock(&store.lock);

    expect(conn, "row count unchanged by the failed statements", "SELECT count() FROM t", rows ? rows : "(error)");
    free(rows);
    run_ok(conn, "SYSTEM START MERGES t");
}

/* 5. ATTACH PART renames detached/<part> to detached/attaching_<part>, then loads it. When the load
 *    fails, the rename must be rolled back with a directory move, or the part is stranded under a
 *    prefix that every later ATTACH and DROP DETACHED refuses. Only the part's checksums read fails,
 *    so the rename itself (which reads directory markers) goes through. */
void attach_fault_test(chdb_connection conn, const char * part)
{
    char sql[512];
    snprintf(sql, sizeof(sql), "ALTER TABLE t ATTACH PART '%s'", part);
    pthread_mutex_lock(&store.lock);
    store.fail.read_suffix = "/checksums.txt";
    pthread_mutex_unlock(&store.lock);
    expect_error(conn, "read fault during ATTACH PART is a callback error", sql, "injected read failure");
    pthread_mutex_lock(&store.lock);
    store.fail.read_suffix = NULL;
    pthread_mutex_unlock(&store.lock);
    snprintf(sql, sizeof(sql),
        "SELECT count() FROM system.detached_parts WHERE database = 'db' AND table = 't' AND name = '%s' AND reason = ''", part);
    expect(conn, "failed ATTACH restores the detached name", sql, "1");
}

/* 4. The engine lists the store while it reopens --path. A failing list must fail the connect, or
 *    the first query on the table, and never crash. Leaves no connection open. */
void list_fault_at_reconnect(int argc, char ** argv)
{
    set_flag(&store.fail.list, 1);
    chdb_connection * conn = chdb_connect(argc, argv);
    int failed = conn == NULL || *conn == NULL;
    if (!failed)
    {
        char err[1024] = "";
        char * out = run(*conn, "SELECT count() FROM db.t", err, sizeof(err));
        failed = out == NULL;
        free(out);
    }
    set_flag(&store.fail.list, 0);
    if (conn && *conn)
        chdb_close_conn(conn);
    check(failed, "list fault at reconnect fails the connect or the first query");
}
