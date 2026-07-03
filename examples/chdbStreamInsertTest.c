/**
 * chdbStreamInsertTest.c
 *
 * Native C test for the streaming INSERT API (chdb_stream_insert family) —
 * the write-side dual of chdb_stream_query.
 *
 * Verifies:
 *   1. Full lifecycle: insert -> append -> done reports rows_written, and the
 *      data reads back with exact values.
 *   2. True multi-chunk streaming: many small appends (one row per append)
 *      arrive intact — exact count and sum after finish.
 *   3. Appends are binary-safe: only `len` bytes are consumed even when the
 *      buffer carries trailing garbage.
 *   4. Init errors (bad target table) surface via chdb_stream_insert_error on
 *      a non-NULL handle; destroy is safe afterwards.
 *   5. cancel-then-destroy releases the stream and the connection stays usable.
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbStreamInsertTest.c -I./programs/local \
 *         -L. -lchdb -o examples/chdbStreamInsertTest
 *   LD_LIBRARY_PATH=. ./examples/chdbStreamInsertTest
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "chdb.h"

static int g_failed_assertions = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "  ASSERT FAIL: %s (%s:%d)\n",                 \
                    (msg), __FILE__, __LINE__);                            \
            g_failed_assertions += 1;                                      \
        }                                                                  \
    } while (0)

/* Runs a query, asserts it succeeded, and returns the result handle. */
static chdb_result * run(chdb_connection conn, const char * query)
{
    chdb_result * result = chdb_query(conn, query, "CSV");
    CHECK(result != NULL, query);
    if (result) {
        const char * error = chdb_result_error(result);
        if (error) {
            fprintf(stderr, "  query error: %s\n  query: %s\n", error, query);
            g_failed_assertions += 1;
        }
    }
    return result;
}

/* Runs a query and asserts its CSV output equals `expected`. */
static void expect_query(chdb_connection conn, const char * query, const char * expected)
{
    chdb_result * result = run(conn, query);
    if (!result)
        return;
    char * buf = chdb_result_buffer(result);
    size_t len = chdb_result_length(result);
    int matches = buf && len == strlen(expected) && memcmp(buf, expected, len) == 0;
    if (!matches) {
        fprintf(stderr, "  MISMATCH for %s\n    expected: %s    got: %.*s\n",
                query, expected, (int)(len > 256 ? 256 : len), buf ? buf : "(null)");
        g_failed_assertions += 1;
    }
    chdb_destroy_query_result(result);
}

static void test_full_lifecycle(chdb_connection conn)
{
    printf("== test_full_lifecycle ==\n");
    chdb_destroy_query_result(run(conn, "CREATE TABLE st1 (a UInt64, b String) ENGINE = Memory"));

    chdb_insert_stream stream = chdb_stream_insert(conn, "INSERT INTO st1 (a, b)", "CSV");
    CHECK(stream != NULL, "stream handle is non-NULL");
    CHECK(chdb_stream_insert_error(stream) == NULL, "no init error");

    CHECK(chdb_stream_append(stream, "1,one\n", 6) == CHDBSuccess, "append #1");
    CHECK(chdb_stream_append(stream, "2,two\n", 6) == CHDBSuccess, "append #2");

    chdb_result * result = chdb_stream_done(stream);
    CHECK(result != NULL, "done returns a result");
    if (result) {
        CHECK(chdb_result_error(result) == NULL, "done reports no error");
        CHECK(chdb_result_rows_written(result) == 2, "rows_written == 2");
        chdb_destroy_query_result(result);
    }
    chdb_destroy_insert_stream(stream);

    expect_query(conn, "SELECT a, b FROM st1 ORDER BY a", "1,\"one\"\n2,\"two\"\n");
}

static void test_multi_chunk_streaming(chdb_connection conn)
{
    printf("== test_multi_chunk_streaming ==\n");
    chdb_destroy_query_result(run(conn, "CREATE TABLE st2 (a UInt64) ENGINE = MergeTree ORDER BY a"));

    chdb_insert_stream stream = chdb_stream_insert(conn, "INSERT INTO st2 (a)", "CSV");
    CHECK(stream != NULL && chdb_stream_insert_error(stream) == NULL, "init ok");

    /* One row per append: the shape of a record-by-record producer. */
    const uint64_t n = 20000;
    char row[32];
    for (uint64_t i = 0; i < n; i++) {
        int len = snprintf(row, sizeof(row), "%" PRIu64 "\n", i);
        if (chdb_stream_append(stream, row, (size_t)len) != CHDBSuccess) {
            const char * err = chdb_stream_insert_error(stream);
            fprintf(stderr, "  append failed at row %" PRIu64 ": %s\n",
                    i, err ? err : "(no message)");
            g_failed_assertions += 1;
            break;
        }
    }

    chdb_result * result = chdb_stream_done(stream);
    CHECK(result != NULL, "done returns a result");
    if (result) {
        CHECK(chdb_result_error(result) == NULL, "done reports no error");
        CHECK(chdb_result_rows_written(result) == n, "rows_written == n");
        chdb_destroy_query_result(result);
    }
    chdb_destroy_insert_stream(stream);

    /* sum(0..n-1) = n*(n-1)/2 */
    expect_query(conn, "SELECT count(), sum(a) FROM st2", "20000,199990000\n");
}

static void test_binary_safe_length(chdb_connection conn)
{
    printf("== test_binary_safe_length ==\n");
    chdb_destroy_query_result(run(conn, "CREATE TABLE st3 (a UInt64) ENGINE = Memory"));

    chdb_insert_stream stream = chdb_stream_insert(conn, "INSERT INTO st3 (a)", "CSV");
    CHECK(stream != NULL && chdb_stream_insert_error(stream) == NULL, "init ok");

    /* Only the first 6 bytes are valid; the rest must be ignored. */
    const char buf[] = "1\n2\n3\n\0GARBAGE_BEYOND_LEN";
    CHECK(chdb_stream_append(stream, buf, 6) == CHDBSuccess, "length-bounded append");

    chdb_result * result = chdb_stream_done(stream);
    if (result) {
        CHECK(chdb_result_error(result) == NULL, "done reports no error");
        CHECK(chdb_result_rows_written(result) == 3, "rows_written == 3");
        chdb_destroy_query_result(result);
    }
    chdb_destroy_insert_stream(stream);

    expect_query(conn, "SELECT sum(a) FROM st3", "6\n");
}

static void test_init_error_surfaces(chdb_connection conn)
{
    printf("== test_init_error_surfaces ==\n");
    chdb_insert_stream stream = chdb_stream_insert(conn, "INSERT INTO no_such_table (a)", "CSV");
    CHECK(stream != NULL, "handle is non-NULL even on init failure");
    if (stream) {
        const char * err = chdb_stream_insert_error(stream);
        CHECK(err != NULL, "init error message is set");
        chdb_destroy_insert_stream(stream);
    }
}

static void test_cancel_then_destroy(chdb_connection conn)
{
    printf("== test_cancel_then_destroy ==\n");
    chdb_destroy_query_result(run(conn, "CREATE TABLE st4 (a UInt64) ENGINE = Memory"));

    chdb_insert_stream stream = chdb_stream_insert(conn, "INSERT INTO st4 (a)", "CSV");
    CHECK(stream != NULL && chdb_stream_insert_error(stream) == NULL, "init ok");
    CHECK(chdb_stream_append(stream, "1\n", 2) == CHDBSuccess, "append before cancel");

    chdb_stream_cancel_insert(stream);
    chdb_destroy_insert_stream(stream);

    /* Unfinalized single-block insert commits nothing, and the connection
     * must remain usable for both queries and a fresh insert stream. */
    expect_query(conn, "SELECT count() FROM st4", "0\n");

    chdb_insert_stream stream2 = chdb_stream_insert(conn, "INSERT INTO st4 (a)", "CSV");
    CHECK(stream2 != NULL && chdb_stream_insert_error(stream2) == NULL, "new stream after cancel");
    CHECK(chdb_stream_append(stream2, "7\n", 2) == CHDBSuccess, "append on new stream");
    chdb_result * result = chdb_stream_done(stream2);
    if (result) {
        CHECK(chdb_result_rows_written(result) == 1, "rows_written == 1 after re-insert");
        chdb_destroy_query_result(result);
    }
    chdb_destroy_insert_stream(stream2);

    expect_query(conn, "SELECT a FROM st4", "7\n");
}

int main(int argc, char ** argv)
{
    (void)argc; (void)argv;

    char arg0[] = "clickhouse";
    char arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    chdb_connection * conn = chdb_connect(2, args);
    if (!conn) {
        fprintf(stderr, "chdb_connect failed\n");
        return 1;
    }

    test_full_lifecycle(*conn);
    test_multi_chunk_streaming(*conn);
    test_binary_safe_length(*conn);
    test_init_error_surfaces(*conn);
    test_cancel_then_destroy(*conn);

    chdb_close_conn(conn);

    printf("\n== summary: %d failed assertions ==\n", g_failed_assertions);
    return g_failed_assertions == 0 ? 0 : 1;
}
