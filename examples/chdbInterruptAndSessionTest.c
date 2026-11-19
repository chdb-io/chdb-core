/**
 * chdbInterruptAndSessionTest.c
 *
 * Regression tests for P2 EmbeddedSession, self-contained concurrent stream
 * cursors, single-connection concurrent read queries, chdb_interrupt(), and
 * chdb_result_error_code().
 *
 * Build (against libchdb.so):
 *   clang examples/chdbInterruptAndSessionTest.c -I./programs/local \
 *         -L. -lchdb -lpthread -o examples/chdbInterruptAndSessionTest
 *   LD_LIBRARY_PATH=. ./examples/chdbInterruptAndSessionTest
 */

#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "chdb.h"

static int g_failures = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (cond) {                                                        \
            printf("  ok   %s\n", (msg));                                  \
        } else {                                                           \
            fprintf(stderr, "  FAIL %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            g_failures += 1;                                               \
        }                                                                  \
    } while (0)

static double monotonic_seconds(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void test_error_codes(chdb_connection conn)
{
    printf("== test_error_codes ==\n");

    CHECK(chdb_result_error_code(NULL) == 0, "NULL result has error_code == 0");

    chdb_result * ok = chdb_query(conn, "SELECT 42", "CSV");
    CHECK(ok != NULL && chdb_result_error(ok) == NULL, "valid query succeeds");
    CHECK(chdb_result_error_code(ok) == 0, "valid query has error_code == 0");
    chdb_destroy_query_result(ok);

    /* SYNTAX_ERROR == 62 in ClickHouse ErrorCodes */
    chdb_result * syn = chdb_query(conn, "SELEC broken syntax FROM", "CSV");
    CHECK(syn != NULL && chdb_result_error(syn) != NULL, "syntax error sets error message");
    CHECK(chdb_result_error_code(syn) == 62, "syntax error reports ErrorCodes::SYNTAX_ERROR (62)");
    chdb_destroy_query_result(syn);

    /* UNKNOWN_TABLE == 60 in ClickHouse ErrorCodes */
    chdb_result * tbl = chdb_query(conn, "SELECT * FROM no_such_table_p2_xyz", "CSV");
    CHECK(tbl != NULL && chdb_result_error(tbl) != NULL, "missing table sets error message");
    CHECK(chdb_result_error_code(tbl) == 60, "missing table reports ErrorCodes::UNKNOWN_TABLE (60)");
    chdb_destroy_query_result(tbl);

    /* UNKNOWN_TYPE == 50 in ClickHouse ErrorCodes */
    chdb_result * typ = chdb_query(conn, "SELECT CAST(1, 'NoSuchTypeXYZ')", "CSV");
    CHECK(typ != NULL && chdb_result_error(typ) != NULL, "bad type sets error message");
    CHECK(chdb_result_error_code(typ) == 50, "bad type reports ErrorCodes::UNKNOWN_TYPE (50)");
    chdb_destroy_query_result(typ);

    /* Null connection error result also carries a non-zero error code */
    chdb_result * bad_conn = chdb_query(NULL, "SELECT 1", "CSV");
    CHECK(bad_conn != NULL && chdb_result_error(bad_conn) != NULL, "NULL conn returns error result");
    CHECK(chdb_result_error_code(bad_conn) != 0, "NULL conn error result has non-zero error_code");
    chdb_destroy_query_result(bad_conn);
}

struct interrupt_watchdog_ctx
{
    chdb_connection conn;
    int query_started;
};

static void * interrupt_watchdog_thread(void * arg)
{
    struct interrupt_watchdog_ctx * ctx = (struct interrupt_watchdog_ctx *)arg;
    while (!__atomic_load_n(&ctx->query_started, __ATOMIC_ACQUIRE))
        usleep(1000);
    /* Let the query enter sleepEachRow execution, then interrupt lock-free. */
    usleep(120 * 1000);
    chdb_interrupt(ctx->conn);
    return NULL;
}

static void test_interrupt_cancels_running_query(chdb_connection conn)
{
    printf("== test_interrupt_cancels_running_query ==\n");

    /* Safe no-op on NULL and idle connection */
    chdb_interrupt(NULL);
    chdb_interrupt(conn);

    struct interrupt_watchdog_ctx ctx = {conn, 0};
    pthread_t watchdog;
    pthread_create(&watchdog, NULL, interrupt_watchdog_thread, &ctx);

    double t0 = monotonic_seconds();
    __atomic_store_n(&ctx.query_started, 1, __ATOMIC_RELEASE);
    /* Uninterrupted this would sleep 100 * 0.1s = 10s */
    chdb_result * r = chdb_query(
        conn,
        "SELECT sum(sleepEachRow(0.1)) FROM numbers(100) SETTINGS max_block_size = 1",
        "CSV");
    double elapsed = monotonic_seconds() - t0;
    pthread_join(watchdog, NULL);

    CHECK(r != NULL && chdb_result_error(r) != NULL, "interrupted query returns error");
    /* QUERY_WAS_CANCELLED == 394 in ClickHouse ErrorCodes */
    CHECK(r != NULL && chdb_result_error_code(r) == 394,
          "interrupted query reports ErrorCodes::QUERY_WAS_CANCELLED (394)");
    CHECK(elapsed < 4.0, "interrupted query aborts promptly instead of sleeping 10s");
    chdb_destroy_query_result(r);

    /* Connection remains healthy immediately after interrupt */
    chdb_result * follow = chdb_query(conn, "SELECT 7 * 6", "TSVRaw");
    CHECK(follow != NULL && chdb_result_error(follow) == NULL
              && chdb_result_length(follow) == 3
              && memcmp(chdb_result_buffer(follow), "42\n", 3) == 0,
          "connection executes follow-up query cleanly after interrupt");
    chdb_destroy_query_result(follow);
}

static void test_interleaved_multiple_streams_on_one_connection(chdb_connection conn)
{
    printf("== test_interleaved_multiple_streams_on_one_connection ==\n");

    chdb_result * s1 = chdb_stream_query(
        conn, "SELECT number FROM numbers(150000) SETTINGS max_block_size = 20000", "CSV");
    chdb_result * s2 = chdb_stream_query(
        conn, "SELECT number * 2 FROM numbers(90000) SETTINGS max_block_size = 15000", "CSV");
    CHECK(s1 != NULL && chdb_result_error(s1) == NULL, "stream 1 initialized");
    CHECK(s2 != NULL && chdb_result_error(s2) == NULL, "stream 2 initialized on same connection");

    uint64_t rows1 = 0;
    uint64_t rows2 = 0;
    int done1 = 0;
    int done2 = 0;

    while (!done1 || !done2) {
        if (!done1) {
            chdb_result * c1 = chdb_stream_fetch_result(conn, s1);
            CHECK(c1 != NULL && chdb_result_error(c1) == NULL, "stream 1 interleaved chunk ok");
            uint64_t n = c1 ? chdb_result_rows_read(c1) : 0;
            if (n == 0)
                done1 = 1;
            else
                rows1 += n;
            chdb_destroy_query_result(c1);
        }

        /* Run a materialized query mid-stream on the same connection */
        chdb_result * mid = chdb_query(conn, "SELECT 99", "TSVRaw");
        if (!mid || chdb_result_error(mid) != NULL)
            CHECK(0, "mid-stream materialized query failed");
        chdb_destroy_query_result(mid);

        if (!done2) {
            chdb_result * c2 = chdb_stream_fetch_result(conn, s2);
            CHECK(c2 != NULL && chdb_result_error(c2) == NULL, "stream 2 interleaved chunk ok");
            uint64_t n = c2 ? chdb_result_rows_read(c2) : 0;
            if (n == 0)
                done2 = 1;
            else
                rows2 += n;
            chdb_destroy_query_result(c2);
        }
    }

    CHECK(rows1 == 150000, "stream 1 exact total rows (150000)");
    CHECK(rows2 == 90000, "stream 2 exact total rows (90000)");
    chdb_destroy_query_result(s1);
    chdb_destroy_query_result(s2);
}

struct concurrent_reader_arg
{
    chdb_connection conn;
    int thread_id;
    int failures;
};

static void * concurrent_reader_worker(void * p)
{
    struct concurrent_reader_arg * arg = (struct concurrent_reader_arg *)p;
    for (int i = 0; i < 25; ++i) {
        char val_str[32];
        int expected_val = arg->thread_id * 1000 + i;
        snprintf(val_str, sizeof(val_str), "%d", expected_val);
        const char * names[] = {"v"};
        const char * values[] = {val_str};

        chdb_result * r = chdb_query_with_params(
            arg->conn,
            "SELECT {v:Int64} + count() FROM numbers(100)",
            "TSVRaw",
            names,
            values,
            1);

        char want[32];
        snprintf(want, sizeof(want), "%d\n", expected_val + 100);
        if (!r || chdb_result_error(r) != NULL
            || chdb_result_length(r) != strlen(want)
            || memcmp(chdb_result_buffer(r), want, strlen(want)) != 0) {
            arg->failures += 1;
        }
        chdb_destroy_query_result(r);
    }
    return NULL;
}

static void test_concurrent_reads_on_single_connection(chdb_connection conn)
{
    printf("== test_concurrent_reads_on_single_connection ==\n");
    pthread_t threads[6];
    struct concurrent_reader_arg args[6];
    for (int t = 0; t < 6; ++t) {
        args[t].conn = conn;
        args[t].thread_id = t + 1;
        args[t].failures = 0;
        pthread_create(&threads[t], NULL, concurrent_reader_worker, &args[t]);
    }
    int total_worker_failures = 0;
    for (int t = 0; t < 6; ++t) {
        pthread_join(threads[t], NULL);
        total_worker_failures += args[t].failures;
    }
    CHECK(total_worker_failures == 0, "6 concurrent reader threads on 1 connection produced exact results");
}

int main(void)
{
    char arg0[] = "clickhouse";
    char * argv[] = {arg0};
    chdb_connection * conn = chdb_connect(1, argv);
    if (!conn) {
        fprintf(stderr, "chdb_connect failed\n");
        return 1;
    }

    test_error_codes(*conn);
    test_interrupt_cancels_running_query(*conn);
    test_interleaved_multiple_streams_on_one_connection(*conn);
    test_concurrent_reads_on_single_connection(*conn);

    chdb_connection stale = *conn;
    chdb_close_conn(conn);
    /* Interrupting a stale handle after close must be a safe no-op */
    chdb_interrupt(stale);

    printf("\nfailures: %d\n", g_failures);
    return g_failures == 0 ? 0 : 1;
}
