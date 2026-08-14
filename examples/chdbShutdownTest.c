/**
 * chdbShutdownTest.c
 *
 * chdb_shutdown() contract: once it returns, chDB has no thread of its own left
 * running, so a host is free to run its own exit sequence (global destructors, a
 * finalizing runtime, a sanitizer exit handler) without racing the engine.
 *
 * Verifies:
 *   1. A query starts engine thread-pool threads, and they are still running
 *      after every connection is closed — closing connections is not a shutdown.
 *   2. chdb_shutdown() refuses to run while a connection is still open, and
 *      leaves the engine untouched when it refuses.
 *   3. Once the last connection is closed, chdb_shutdown() succeeds and zero
 *      engine threads remain.
 *   4. A second chdb_shutdown() is a successful no-op.
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbShutdownTest.c -I./programs/local \
 *         -L. -lchdb -o examples/chdbShutdownTest
 *   LD_LIBRARY_PATH=. ./examples/chdbShutdownTest
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chdb.h"

#if defined(__APPLE__)
#    include <mach/mach.h>
#    include <pthread.h>
#else
#    include <dirent.h>
#endif

/* Name every thread of a ClickHouse thread pool carries while idle. */
#define POOL_THREAD_NAME "ThreadPool"

static int g_failed_assertions = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "  ASSERT FAIL: %s (%s:%d)\n",                 \
                    (msg), __FILE__, __LINE__);                            \
            g_failed_assertions += 1;                                      \
        }                                                                  \
    } while (0)

/* Number of live threads named POOL_THREAD_NAME. Out-params report the total
 * thread count so a failure says how far off the process is. */
#if defined(__APPLE__)
static int count_pool_threads(int * total_out)
{
    thread_act_array_t threads;
    mach_msg_type_number_t count = 0;
    int pool = 0;

    if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) {
        fprintf(stderr, "  task_threads() failed\n");
        *total_out = -1;
        return -1;
    }

    for (mach_msg_type_number_t i = 0; i < count; ++i) {
        pthread_t handle = pthread_from_mach_thread_np(threads[i]);
        char name[64] = "";
        if (handle && pthread_getname_np(handle, name, sizeof(name)) == 0
            && strcmp(name, POOL_THREAD_NAME) == 0)
            pool += 1;
        mach_port_deallocate(mach_task_self(), threads[i]);
    }
    vm_deallocate(mach_task_self(), (vm_address_t)threads, count * sizeof(thread_t));

    *total_out = (int)count;
    return pool;
}
#else
static int count_pool_threads(int * total_out)
{
    DIR * dir = opendir("/proc/self/task");
    struct dirent * entry;
    int pool = 0;
    int total = 0;

    if (!dir) {
        fprintf(stderr, "  cannot open /proc/self/task\n");
        *total_out = -1;
        return -1;
    }

    while ((entry = readdir(dir)) != NULL) {
        char path[320];
        char name[64] = "";
        FILE * comm;

        if (entry->d_name[0] == '.')
            continue;
        total += 1;

        snprintf(path, sizeof(path), "/proc/self/task/%s/comm", entry->d_name);
        comm = fopen(path, "r");
        if (!comm)
            continue;
        if (fgets(name, sizeof(name), comm)) {
            char * newline = strchr(name, '\n');
            if (newline)
                *newline = '\0';
            if (strcmp(name, POOL_THREAD_NAME) == 0)
                pool += 1;
        }
        fclose(comm);
    }
    closedir(dir);

    *total_out = total;
    return pool;
}
#endif

static void report(const char * tag)
{
    int total = 0;
    int pool = count_pool_threads(&total);
    printf("  %-24s pool threads: %d, total threads: %d\n", tag, pool, total);
}

int main(void)
{
    char * argv[] = {(char *)"chdb", (char *)"--path=:memory:", NULL};
    chdb_connection * conn = NULL;
    chdb_result * res = NULL;
    const char * err = NULL;
    int pool_after_query = 0;
    int pool_after_close = 0;
    int pool_after_shutdown = 0;
    int total = 0;
    chdb_state state;

    printf("=== chdb_shutdown() ===\n");
    report("before connect");

    conn = chdb_connect(2, argv);
    CHECK(conn != NULL, "chdb_connect succeeds");
    if (!conn)
        return 1;

    res = chdb_query(*conn, "SELECT count() FROM numbers(1000000)", "CSV");
    err = chdb_result_error(res);
    CHECK(err == NULL, "query succeeds");
    if (err)
        fprintf(stderr, "  query error: %s\n", err);
    chdb_destroy_query_result(res);

    pool_after_query = count_pool_threads(&total);
    report("after query");
    CHECK(pool_after_query > 0, "a query starts engine thread-pool threads");

    /* 2. Refuses to run while a connection is open, and changes nothing. */
    state = chdb_shutdown();
    CHECK(state == CHDBError, "chdb_shutdown fails while a connection is open");
    report("after refused shutdown");
    CHECK(count_pool_threads(&total) == pool_after_query,
          "a refused chdb_shutdown leaves the engine threads running");

    /* The connection still works after the refusal. */
    res = chdb_query(*conn, "SELECT 1", "CSV");
    CHECK(chdb_result_error(res) == NULL, "connection still usable after a refused shutdown");
    chdb_destroy_query_result(res);

    /* 1. Closing every connection is not a shutdown. */
    chdb_close_conn(conn);
    pool_after_close = count_pool_threads(&total);
    report("after close_conn");
    CHECK(pool_after_close > 0, "closing every connection leaves the engine threads running");

    /* 3. Now it stops the engine. */
    state = chdb_shutdown();
    CHECK(state == CHDBSuccess, "chdb_shutdown succeeds once no connection is open");
    pool_after_shutdown = count_pool_threads(&total);
    report("after chdb_shutdown");
    CHECK(pool_after_shutdown == 0, "no engine thread survives chdb_shutdown");

    /* 4. Idempotent. */
    state = chdb_shutdown();
    CHECK(state == CHDBSuccess, "a second chdb_shutdown is a successful no-op");
    CHECK(count_pool_threads(&total) == 0, "still no engine thread after the second call");

    if (g_failed_assertions == 0) {
        printf("PASS\n");
        return 0;
    }
    fprintf(stderr, "FAIL: %d assertion(s)\n", g_failed_assertions);
    return 1;
}
