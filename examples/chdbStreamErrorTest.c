/**
 * chdbStreamErrorTest.c
 *
 * Regression coverage for issue #612: a streaming INSERT that fails during
 * the sample-block handshake used to report a fixed, reason-free string
 * ("Failed to receive table structure") and drop the real exception. This
 * verifies the concrete cause is now surfaced, and locks in that the
 * streaming/materialized QUERY paths (which already reported the real reason)
 * keep doing so.
 *
 * All failures are triggered locally and deterministically (no network), so
 * the test is fast and stable in CI:
 *   - a bad structure type -> UNKNOWN_TYPE (Code 50), names "Nonexistent"
 *   - an unknown input format -> UNKNOWN_FORMAT (Code 73), names "NoSuchFormat"
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbStreamErrorTest.c -I./programs/local \
 *         -L. -lchdb -o examples/chdbStreamErrorTest
 *   LD_LIBRARY_PATH=. ./examples/chdbStreamErrorTest
 */

#include <stdio.h>
#include <string.h>

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

/* The misleading generic message that used to mask the real cause. It must no
 * longer appear in any streaming error string (issue #612). */
static const char * MASKED = "Failed to receive table structure";

/* Streaming INSERT init failures must carry the concrete engine error, and no
 * longer the misleading "table structure" message that masked it. */
static void test_insert_init_error_carries_real_reason(chdb_connection conn)
{
    printf("== test_insert_init_error_carries_real_reason ==\n");

    /* Bad structure type -> UNKNOWN_TYPE (Code 50); the reason names the type. */
    chdb_insert_stream s1 = chdb_stream_insert(
        conn,
        "INSERT INTO FUNCTION file('chdb_stream_err1.csv', 'CSV', 'id Nonexistent')",
        "CSV");
    CHECK(s1 != NULL, "handle is non-NULL");
    const char * e1 = chdb_stream_insert_error(s1);
    CHECK(e1 != NULL, "bad-type init error is set");
    CHECK(e1 && strstr(e1, "Nonexistent") != NULL,
          "bad-type error names the offending type");
    CHECK(e1 && strstr(e1, MASKED) == NULL,
          "bad-type error is not masked by the generic message");
    if (e1)
        fprintf(stderr, "  bad-type reason: %s\n", e1);
    chdb_destroy_insert_stream(s1);

    /* Unknown input format -> UNKNOWN_FORMAT (Code 73); a distinct failure
     * class, to show the real reason isn't hardcoded to one error. */
    chdb_insert_stream s2 = chdb_stream_insert(
        conn,
        "INSERT INTO FUNCTION file('chdb_stream_err2.x', 'NoSuchFormat', 'id UInt64')",
        "CSV");
    CHECK(s2 != NULL, "handle is non-NULL");
    const char * e2 = chdb_stream_insert_error(s2);
    CHECK(e2 != NULL, "unknown-format init error is set");
    CHECK(e2 && strstr(e2, "NoSuchFormat") != NULL,
          "unknown-format error names the format");
    CHECK(e2 && strstr(e2, MASKED) == NULL,
          "unknown-format error is not masked by the generic message");
    if (e2)
        fprintf(stderr, "  unknown-format reason: %s\n", e2);
    chdb_destroy_insert_stream(s2);
}

/* Lock-in: the materialized query path surfaces the real reason. */
static void test_materialized_query_error_reason(chdb_connection conn)
{
    printf("== test_materialized_query_error_reason ==\n");
    chdb_result * r = chdb_query(
        conn, "SELECT * FROM file('chdb_none.csv', 'CSV', 'id Nonexistent')", "CSV");
    CHECK(r != NULL, "result is non-NULL");
    const char * e = r ? chdb_result_error(r) : NULL;
    CHECK(e != NULL, "materialized query error is set");
    CHECK(e && strstr(e, "Nonexistent") != NULL, "materialized query names the type");
    CHECK(e && strstr(e, MASKED) == NULL, "not masked by the generic message");
    chdb_destroy_query_result(r);
}

/* Lock-in: the streaming query path surfaces the real reason (here it arrives
 * on the first fetched chunk rather than at init, which is expected). */
static void test_streaming_query_error_reason(chdb_connection conn)
{
    printf("== test_streaming_query_error_reason ==\n");
    chdb_result * stream = chdb_stream_query(
        conn, "SELECT * FROM file('chdb_none.csv', 'CSV', 'id Nonexistent')", "CSV");
    CHECK(stream != NULL, "stream handle is non-NULL");

    const char * err = stream ? chdb_result_error(stream) : NULL;
    if (!err) {
        chdb_result * chunk = chdb_stream_fetch_result(conn, stream);
        err = chunk ? chdb_result_error(chunk) : NULL;
        CHECK(err != NULL, "streaming query error surfaces on fetch");
        CHECK(err && strstr(err, "Nonexistent") != NULL, "streaming query names the type");
        CHECK(err && strstr(err, MASKED) == NULL, "not masked by the generic message");
        chdb_destroy_query_result(chunk);
    } else {
        CHECK(strstr(err, "Nonexistent") != NULL, "streaming query names the type at init");
    }
    chdb_destroy_query_result(stream);
}

int main(void)
{
    char arg0[] = "clickhouse";
    char arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    chdb_connection * conn = chdb_connect(2, args);
    if (!conn) {
        fprintf(stderr, "chdb_connect failed\n");
        return 1;
    }

    test_insert_init_error_carries_real_reason(*conn);
    test_materialized_query_error_reason(*conn);
    test_streaming_query_error_reason(*conn);

    chdb_close_conn(conn);

    printf("\n== summary: %d failed assertions ==\n", g_failed_assertions);
    return g_failed_assertions == 0 ? 0 : 1;
}
