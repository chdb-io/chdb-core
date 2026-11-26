/* C-side regressions for the defects the ASan+UBSan build surfaced.
 *
 * On the unfixed engine this binary aborts:
 *   - the second chdb_connect() in the process trips
 *     chassert(!background_context_instance) in Context::makeBackgroundContext()
 *   - the streaming INSERT worker trips chassert(!current_thread) in ThreadStatus,
 *     because startThreadFromGlobalPool already built one for that thread
 *
 * It also pins the result-buffer contract (buf is chdb_result_length() bytes and is not
 * NUL-terminated, so every consumer has to carry the length) and, when run under
 * AddressSanitizer, bounds what one engine start/stop cycle may leave behind.
 *
 * And two result regressions: a statement without output that follows a failed query
 * returns no buffer, not an empty one the failed query left behind; and the one-shot
 * chdb_query_cmdline() works more than once in a process (the second call used to fail
 * with "EventNotifier is initialized twice") and returns its errors instead of an
 * empty result.
 *
 * And the exit of a process that still has a connection open, with and without an engine
 * restart before: the teardown used to run on statics already destroyed (after a restart,
 * "The ShutdownTablesThreadPool is not initialized"), which could also crash the exit.
 *
 * And a chdb_arrow_array_scan() that fails (here: on a duplicate name) frees the stream
 * wrapper it allocated instead of leaking it.
 */
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include "chdb.h"
#include "arrow_c_abi.h"

static int failures = 0;

#define CHECK(cond, what) \
    do { \
        if (cond) \
            printf("  ok   %s\n", (what)); \
        else { printf("  FAIL %s\n", (what)); failures++; } \
    } while (0)

static chdb_connection * open_memory_connection(void)
{
    char arg0[] = "clickhouse";
    char arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    return chdb_connect(2, args);
}

/* Engine restart inside one process. */
static void test_repeated_connect(void)
{
    printf("test_repeated_connect\n");
    for (int i = 0; i < 3; i++)
    {
        chdb_connection * conn = open_memory_connection();
        CHECK(conn != NULL, "connection opens");
        if (!conn)
            return;

        chdb_result * r = chdb_query(*conn, "SELECT 1", "CSV");
        CHECK(r != NULL && chdb_result_error(r) == NULL, "query succeeds on a restarted engine");
        chdb_destroy_query_result(r);
        chdb_close_conn(conn);
    }
}

/* Streaming INSERT: the worker runs on a pooled thread that already owns a ThreadStatus. */
static void test_insert_stream_worker(void)
{
    printf("test_insert_stream_worker\n");
    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    chdb_destroy_query_result(
        chdb_query(*conn, "CREATE TABLE asan_ins (a UInt64, b String) ENGINE = Memory", "CSV"));

    const char * rows = "1,\"one\"\n2,\"two\"\n";
    chdb_insert_stream stream = chdb_stream_insert(*conn, "INSERT INTO asan_ins (a, b)", "CSV");
    CHECK(stream != NULL && chdb_stream_insert_error(stream) == NULL, "insert stream opens");
    CHECK(chdb_stream_append(stream, rows, strlen(rows)) == CHDBSuccess, "rows appended");
    chdb_destroy_query_result(chdb_stream_done(stream));
    chdb_destroy_insert_stream(stream);

    chdb_result * r = chdb_query(*conn, "SELECT a, b FROM asan_ins ORDER BY a", "CSV");
    CHECK(r != NULL && chdb_result_error(r) == NULL, "streamed rows are queryable");
    if (r)
    {
        const char * expected = "1,\"one\"\n2,\"two\"\n";
        size_t len = chdb_result_length(r);
        CHECK(len == strlen(expected) && memcmp(chdb_result_buffer(r), expected, len) == 0,
              "streamed rows read back intact");
    }
    chdb_destroy_query_result(r);
    chdb_close_conn(conn);
}

/* The buffer is not a C string: length and content must agree without a terminator. */
static void test_result_buffer_is_length_delimited(void)
{
    printf("test_result_buffer_is_length_delimited\n");
    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    chdb_result * r = chdb_query(*conn, "SELECT 'abc'", "CSV");
    CHECK(r != NULL && chdb_result_error(r) == NULL, "query succeeds");
    if (r)
    {
        const char * expected = "\"abc\"\n";
        size_t len = chdb_result_length(r);
        /* memcmp over exactly len bytes -- strcmp/strlen here would read out of bounds. */
        CHECK(len == strlen(expected) && memcmp(chdb_result_buffer(r), expected, len) == 0,
              "buffer holds exactly chdb_result_length() bytes");
    }
    chdb_destroy_query_result(r);
    chdb_close_conn(conn);
}


/* A query that failed after creating its output buffer, here on an unknown output format,
 * used to leave that buffer behind, and the next statement without output of its own
 * returned it as an empty, non-NULL result instead of none. */
static void test_no_output_after_failed_query(void)
{
    printf("test_no_output_after_failed_query\n");
    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    chdb_result * r = chdb_query(*conn, "CREATE TABLE before_failure (x Int32) ENGINE = Memory", "CSV");
    CHECK(r != NULL && chdb_result_error(r) == NULL && chdb_result_buffer(r) == NULL,
          "a statement without output returns no buffer");
    chdb_destroy_query_result(r);

    r = chdb_query(*conn, "SELECT 1", "NoSuchFormat");
    CHECK(r != NULL && chdb_result_error(r) != NULL, "an unknown output format fails");
    chdb_destroy_query_result(r);

    r = chdb_query(*conn, "CREATE TABLE after_failure (x Int32) ENGINE = Memory", "CSV");
    CHECK(r != NULL && chdb_result_error(r) == NULL && chdb_result_buffer(r) == NULL
              && chdb_result_length(r) == 0,
          "the next statement without output still returns no buffer");
    chdb_destroy_query_result(r);
    chdb_close_conn(conn);
}


/* chdb_query_cmdline() boots a one-shot server per call. No connection may be open. */
static void test_repeated_query_cmdline(void)
{
    printf("test_repeated_query_cmdline\n");
    char arg0[] = "clickhouse";
    char arg1[] = "--output-format=CSV";
    char ok_query[] = "--query=SELECT 6 * 7";
    char bad_query[] = "--query=SELEC 1";
    char * ok_args[] = {arg0, arg1, ok_query};
    char * bad_args[] = {arg0, arg1, bad_query};

    for (int i = 0; i < 3; i++)
    {
        chdb_result * r = chdb_query_cmdline(3, ok_args);
        const char * expected = "42\n";
        CHECK(r != NULL && chdb_result_error(r) == NULL && chdb_result_length(r) == strlen(expected)
                  && memcmp(chdb_result_buffer(r), expected, strlen(expected)) == 0,
              "a one-shot query succeeds, also after the first in the process");
        chdb_destroy_query_result(r);
    }

    chdb_result * r = chdb_query_cmdline(3, bad_args);
    const char * err = r ? chdb_result_error(r) : NULL;
    CHECK(err != NULL && strstr(err, "Syntax error") != NULL, "a failed one-shot query returns its error");
    chdb_destroy_query_result(r);
}


/* Run by a child of test_exit_with_connection_open: optionally restart the engine, then return
 * from main with a connection to a MergeTree table still open, so exit() tears the engine down. */
static int exit_with_connection_open_child(const char * dir, int restart)
{
    if (restart)
    {
        chdb_connection * first = open_memory_connection();
        if (!first)
            return 2;
        chdb_close_conn(first);
    }

    char arg0[] = "clickhouse";
    char path[64];
    snprintf(path, sizeof(path), "--path=%s", dir);
    char * args[] = {arg0, path};
    chdb_connection * conn = chdb_connect(2, args);
    if (!conn)
        return 4;
    const char * queries[] = {
        "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x",
        "INSERT INTO t SELECT number FROM numbers(1000)",
    };
    for (size_t i = 0; i < sizeof(queries) / sizeof(queries[0]); i++)
    {
        chdb_result * r = chdb_query(*conn, queries[i], "CSV");
        int ok = r != NULL && chdb_result_error(r) == NULL;
        chdb_destroy_query_result(r);
        if (!ok)
            return 5;
    }
    return 0;
}

static void test_exit_with_connection_open(const char * self)
{
    printf("test_exit_with_connection_open\n");
    /* Without a restart the teardown used the singletons the start constructs after it
     * registered the teardown, already destroyed (NamedCollectionFactory: a crash with the
     * static library); after one, the statics the earlier engine constructed. */
    for (int i = 0; i < 6; i++)
    {
        const char * mode = i % 2 ? "restart" : "first";
        char dir[] = "/tmp/chdb_exit_open_XXXXXX";
        int err_pipe[2];
        if (!mkdtemp(dir) || pipe(err_pipe) != 0)
        {
            CHECK(0, "data directory and pipe for the child");
            return;
        }
        fflush(stdout);
        pid_t pid = fork();
        if (pid == 0)
        {
            dup2(err_pipe[1], STDERR_FILENO);
            close(err_pipe[0]);
            close(err_pipe[1]);
            execl(self, self, "--exit-with-connection-open", dir, mode, (char *)NULL);
            _exit(127);
        }
        close(err_pipe[1]);
        char err[4096];
        size_t len = 0;
        ssize_t n;
        while ((n = read(err_pipe[0], err + len, sizeof(err) - 1 - len)) > 0 && len < sizeof(err) - 1)
            len += (size_t)n;
        err[len] = '\0';
        close(err_pipe[0]);
        int status = 0;
        waitpid(pid, &status, 0);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0,
              i % 2 ? "a process that restarted the engine exits cleanly with a connection open"
                    : "a process exits cleanly with its first engine's connection open");
        CHECK(strstr(err, "Exception") == NULL, "and reports no exception from the engine teardown");
        if (len)
            printf("       child stderr: %s\n", err);
        char rm[64];
        snprintf(rm, sizeof(rm), "rm -rf %s", dir);
        (void)!system(rm);
    }
}


/* Per-cycle growth of the engine. Only meaningful under AddressSanitizer, so the counter
 * is looked up at run time and the check is skipped where it is absent.
 *
 * Budget: re-parsing the built-in users XML on every start used to leak 40,736 bytes per
 * cycle on its own. What remains is the context shared-part family, measured at ~29 KiB
 * per cycle; the budget sits above that and below the regression it guards against. */
#define LEAK_BUDGET_PER_CYCLE (64 * 1024)

static void test_engine_start_leak_budget(void)
{
    size_t (*live_bytes)(void) = (size_t (*)(void))dlsym(RTLD_DEFAULT, "__sanitizer_get_current_allocated_bytes");

    printf("test_engine_start_leak_budget\n");
    if (!live_bytes)
    {
        printf("  skip (not an AddressSanitizer build)\n");
        return;
    }

    /* One cycle first: the engine's one-time lazy globals are not a per-cycle cost. */
    for (int warm = 0; warm < 1; warm++)
    {
        chdb_connection * conn = open_memory_connection();
        if (conn)
        {
            chdb_destroy_query_result(chdb_query(*conn, "SELECT 1", "CSV"));
            chdb_close_conn(conn);
        }
    }

    const int cycles = 10;
    size_t before = live_bytes();
    for (int i = 0; i < cycles; i++)
    {
        chdb_connection * conn = open_memory_connection();
        if (!conn)
        {
            CHECK(0, "connection opens during the leak measurement");
            return;
        }
        chdb_destroy_query_result(chdb_query(*conn, "SELECT count() FROM numbers(100)", "CSV"));
        chdb_close_conn(conn);
    }
    size_t after = live_bytes();

    /* Convert before subtracting: an unrelated free between the two reads would make
     * a size_t subtraction wrap into a huge positive delta. */
    double per_cycle = ((double)after - (double)before) / (double)cycles;
    printf("  %.0f bytes retained per connect/close cycle (budget %d)\n", per_cycle, LEAK_BUDGET_PER_CYCLE);
    CHECK(per_cycle < (double)LEAK_BUDGET_PER_CYCLE, "engine start/stop stays within its leak budget");
}

static void noop_schema_release(struct ArrowSchema * s) { s->release = NULL; }
static void noop_array_release(struct ArrowArray * a) { a->release = NULL; }

/* A failed chdb_arrow_array_scan() used to leak the stream wrapper it allocated
 * (ArrowArrayStream + its private data, ~64 bytes). Only measurable under AddressSanitizer. */
static void test_failed_arrow_array_scan_leak(void)
{
    size_t (*live_bytes)(void) = (size_t (*)(void))dlsym(RTLD_DEFAULT, "__sanitizer_get_current_allocated_bytes");

    printf("test_failed_arrow_array_scan_leak\n");
    if (!live_bytes)
    {
        printf("  skip (not an AddressSanitizer build)\n");
        return;
    }

    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    struct ArrowSchema schema;
    struct ArrowArray array;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    schema.format = "+s";
    schema.release = noop_schema_release;
    array.release = noop_array_release;

    CHECK(chdb_arrow_array_scan(*conn, "asan_dup_array", (chdb_arrow_schema)&schema, (chdb_arrow_array)&array) == CHDBSuccess,
        "first registration succeeds");

    /* Warm up, then measure: every call fails on the duplicate name. */
    for (int i = 0; i < 100; i++)
        chdb_arrow_array_scan(*conn, "asan_dup_array", (chdb_arrow_schema)&schema, (chdb_arrow_array)&array);

    const int calls = 10000;
    int failed = 0;
    size_t before = live_bytes();
    for (int i = 0; i < calls; i++)
        failed += chdb_arrow_array_scan(*conn, "asan_dup_array", (chdb_arrow_schema)&schema, (chdb_arrow_array)&array) == CHDBError;
    size_t after = live_bytes();

    CHECK(failed == calls, "every duplicate registration fails");
    double per_call = ((double)after - (double)before) / (double)calls;
    printf("  %.1f bytes retained per failed registration\n", per_call);
    CHECK(per_call < 8.0, "a failed chdb_arrow_array_scan frees its stream wrapper");

    CHECK(chdb_arrow_unregister_table(*conn, "asan_dup_array") == CHDBSuccess, "unregister succeeds");
    chdb_close_conn(conn);
}

/* [C1] Calling chdb_query_cmdline / query_stable* while a connection is open must fail
 * cleanly BEFORE constructing LocalServer, leaving the live EmbeddedServer catalog intact. */
static void test_query_cmdline_refused_while_connection_open(void)
{
    printf("test_query_cmdline_refused_while_connection_open\n");
    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    chdb_destroy_query_result(
        chdb_query(*conn, "CREATE TABLE keep_alive_tbl (x Int32) ENGINE = Memory", "CSV"));
    chdb_destroy_query_result(
        chdb_query(*conn, "INSERT INTO keep_alive_tbl VALUES (77)", "CSV"));

    char arg0[] = "clickhouse";
    char arg1[] = "--output-format=CSV";
    char arg2[] = "--query=SELECT 1";
    char * cmd_args[] = {arg0, arg1, arg2};

    chdb_result * cmd_res = chdb_query_cmdline(3, cmd_args);
    CHECK(cmd_res != NULL && chdb_result_error(cmd_res) != NULL,
          "chdb_query_cmdline is refused while a connection is open");
    chdb_destroy_query_result(cmd_res);

    struct local_result_v2 * v2_res = query_stable_v2(3, cmd_args);
    CHECK(v2_res != NULL && v2_res->error_message != NULL,
          "query_stable_v2 is refused while a connection is open");
    free_result_v2(v2_res);

    struct local_result * v1_res = query_stable(3, cmd_args);
    CHECK(v1_res == NULL, "query_stable returns NULL while a connection is open");

    /* The live connection and its DatabaseCatalog must still work normally. */
    chdb_result * check_res = chdb_query(*conn, "SELECT x FROM keep_alive_tbl", "CSV");
    const char * expected = "77\n";
    CHECK(check_res != NULL && chdb_result_error(check_res) == NULL
              && chdb_result_length(check_res) == strlen(expected)
              && memcmp(chdb_result_buffer(check_res), expected, strlen(expected)) == 0,
          "existing connection and catalog survive refused LocalServer calls");
    chdb_destroy_query_result(check_res);

    chdb_close_conn(conn);

    /* Once all connections are closed, chdb_query_cmdline works again. */
    cmd_res = chdb_query_cmdline(3, cmd_args);
    CHECK(cmd_res != NULL && chdb_result_error(cmd_res) == NULL,
          "chdb_query_cmdline succeeds again once all connections are closed");
    chdb_destroy_query_result(cmd_res);
}

struct close_race_ctx
{
    chdb_connection conn;
    int started;
};

static void * close_race_query_worker(void * arg)
{
    struct close_race_ctx * ctx = (struct close_race_ctx *)arg;
    __atomic_store_n(&ctx->started, 1, __ATOMIC_RELEASE);
    for (int i = 0; i < 20; ++i)
    {
        chdb_result * r = chdb_query(ctx->conn, "SELECT sum(number) FROM numbers(50000)", "CSV");
        if (r)
            chdb_destroy_query_result(r);
    }
    return NULL;
}

/* [C7] Concurrent chdb_query vs chdb_close_conn, plus post-close stale handle rejection. */
static void test_concurrent_close_and_stale_handle(void)
{
    printf("test_concurrent_close_and_stale_handle\n");
    /* Keep a background connection open so the loop tests connection lifecycle, not engine churn. */
    chdb_connection * anchor = open_memory_connection();
    CHECK(anchor != NULL, "anchor connection opens");
    if (!anchor)
        return;

    for (int round = 0; round < 5; ++round)
    {
        chdb_connection * victim = open_memory_connection();
        if (!victim)
        {
            CHECK(0, "victim connection opens");
            break;
        }
        chdb_connection stale_copy = *victim;
        struct close_race_ctx ctx = {stale_copy, 0};
        pthread_t worker;
        pthread_create(&worker, NULL, close_race_query_worker, &ctx);
        while (!__atomic_load_n(&ctx.started, __ATOMIC_ACQUIRE))
        {
        }
        chdb_close_conn(victim);
        pthread_join(worker, NULL);

        /* Using stale_copy AFTER chdb_close_conn freed the handle must return an error, never UAF. */
        chdb_result * after = chdb_query(stale_copy, "SELECT 1", "CSV");
        CHECK(after != NULL && chdb_result_error(after) != NULL,
              "query on stale closed connection handle returns clean error");
        chdb_destroy_query_result(after);
    }

    chdb_close_conn(anchor);
}

/* [C6] Closing a connection without unregistering chdb_arrow_array_scan frees its owned wrapper. */
static void test_unregistered_arrow_array_freed_on_close(void)
{
    size_t (*live_bytes)(void) = (size_t (*)(void))dlsym(RTLD_DEFAULT, "__sanitizer_get_current_allocated_bytes");

    printf("test_unregistered_arrow_array_freed_on_close\n");
    if (!live_bytes)
    {
        printf("  skip (not an AddressSanitizer build)\n");
        return;
    }

    chdb_connection * anchor = open_memory_connection();
    if (!anchor)
        return;

    struct ArrowSchema schema;
    struct ArrowArray array;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    schema.format = "+s";
    schema.release = noop_schema_release;
    array.release = noop_array_release;

    /* Warm up */
    for (int i = 0; i < 5; ++i)
    {
        chdb_connection * c = open_memory_connection();
        chdb_arrow_array_scan(*c, "leaked_on_close", (chdb_arrow_schema)&schema, (chdb_arrow_array)&array);
        chdb_close_conn(c);
    }

    const int cycles = 200;
    size_t before = live_bytes();
    for (int i = 0; i < cycles; ++i)
    {
        chdb_connection * c = open_memory_connection();
        chdb_arrow_array_scan(*c, "leaked_on_close", (chdb_arrow_schema)&schema, (chdb_arrow_array)&array);
        chdb_close_conn(c);
    }
    size_t after = live_bytes();
    double per_cycle = ((double)after - (double)before) / (double)cycles;
    printf("  %.1f bytes retained per un-unregistered arrow_array_scan close cycle\n", per_cycle);
    CHECK(per_cycle < 16.0, "closing a connection frees owned ArrowStreamRegistry entries");

    chdb_close_conn(anchor);
}

int main(int argc, char ** argv)
{
    if (argc > 3 && strcmp(argv[1], "--exit-with-connection-open") == 0)
        return exit_with_connection_open_child(argv[2], strcmp(argv[3], "restart") == 0);
    setvbuf(stdout, NULL, _IOLBF, 0);
    printf("chdb %s\n", chdb_version());
    test_repeated_connect();
    test_insert_stream_worker();
    test_result_buffer_is_length_delimited();
    test_no_output_after_failed_query();
    test_repeated_query_cmdline();
    test_exit_with_connection_open(argv[0]);
    test_engine_start_leak_budget();
    test_failed_arrow_array_scan_leak();
    test_query_cmdline_refused_while_connection_open();
    test_concurrent_close_and_stale_handle();
    test_unregistered_arrow_array_freed_on_close();
    printf("\nfailures: %d\n", failures);
    return failures == 0 ? 0 : 1;
}
