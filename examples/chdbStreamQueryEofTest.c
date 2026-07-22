/**
 * chdbStreamQueryEofTest.c
 *
 * End-of-stream semantics for streaming queries (chdb_stream_query /
 * chdb_stream_fetch_result).
 *
 * Verifies:
 *   1. Natural end of stream: the final fetch returns an empty result with
 *      no error, after exact row accounting.
 *   2. Fetch after natural EOF is idempotent and non-error: every further
 *      fetch returns a fresh empty result (read(2)-style EOF), so pull-style
 *      consumers may safely over-fetch.
 *   3. The exhausted handle stays inert once a NEW stream is opened on the
 *      same connection: fetching the old handle still returns empty and does
 *      not disturb the active stream.
 *   4. Cancel keeps its semantics: fetch after chdb_stream_cancel_query()
 *      returns an error result.
 *   5. A mid-stream execution error surfaces on a fetch as an error result;
 *      only NATURAL end-of-stream is idempotent — fetching again after an
 *      error still errors.
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbStreamQueryEofTest.c -I./programs/local \
 *         -L. -lchdb -o examples/chdbStreamQueryEofTest
 *   LD_LIBRARY_PATH=. ./examples/chdbStreamQueryEofTest
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

/* Drains a stream to its natural end, returning the total row count.
 * Asserts that the terminating fetch is empty AND error-free. */
static uint64_t drain_stream(chdb_connection conn, chdb_result * stream)
{
    uint64_t rows = 0;
    for (;;) {
        chdb_result * chunk = chdb_stream_fetch_result(conn, stream);
        CHECK(chunk != NULL, "fetch never returns NULL");
        if (!chunk)
            return rows;
        const char * err = chdb_result_error(chunk);
        if (err) {
            fprintf(stderr, "  unexpected fetch error: %s\n", err);
            g_failed_assertions += 1;
            chdb_destroy_query_result(chunk);
            return rows;
        }
        uint64_t got = chdb_result_rows_read(chunk);
        if (got == 0) {
            CHECK(chdb_result_length(chunk) == 0, "terminating chunk carries no data");
            chdb_destroy_query_result(chunk);
            return rows;
        }
        rows += got;
        chdb_destroy_query_result(chunk);
    }
}

static void test_fetch_after_eof_is_idempotent_empty(chdb_connection conn)
{
    printf("== test_fetch_after_eof_is_idempotent_empty ==\n");
    chdb_result * stream = chdb_stream_query(conn, "SELECT number FROM numbers(200000)", "CSV");
    CHECK(stream != NULL && chdb_result_error(stream) == NULL, "stream init ok");

    CHECK(drain_stream(conn, stream) == 200000, "exact row count");

    /* Over-fetch three times: each call must return a fresh empty result
     * with NO error. */
    for (int i = 0; i < 3; i++) {
        chdb_result * extra = chdb_stream_fetch_result(conn, stream);
        CHECK(extra != NULL, "post-EOF fetch returns a result");
        if (!extra)
            continue;
        const char * err = chdb_result_error(extra);
        if (err) {
            fprintf(stderr, "  post-EOF fetch #%d error: %s\n", i + 1, err);
            g_failed_assertions += 1;
        }
        CHECK(chdb_result_rows_read(extra) == 0, "post-EOF fetch is empty (rows)");
        CHECK(chdb_result_length(extra) == 0, "post-EOF fetch is empty (bytes)");
        chdb_destroy_query_result(extra);
    }
    chdb_destroy_query_result(stream);
}

static void test_exhausted_handle_inert_while_new_stream_active(chdb_connection conn)
{
    printf("== test_exhausted_handle_inert_while_new_stream_active ==\n");
    chdb_result * old_stream = chdb_stream_query(conn, "SELECT number FROM numbers(1000)", "CSV");
    CHECK(old_stream != NULL && chdb_result_error(old_stream) == NULL, "old stream init ok");
    CHECK(drain_stream(conn, old_stream) == 1000, "old stream row count");

    chdb_result * new_stream = chdb_stream_query(conn, "SELECT number FROM numbers(3000)", "CSV");
    CHECK(new_stream != NULL && chdb_result_error(new_stream) == NULL, "new stream init ok");

    /* The exhausted old handle answers empty and must not disturb the new
     * stream's state. */
    chdb_result * stale = chdb_stream_fetch_result(conn, old_stream);
    CHECK(stale != NULL && chdb_result_error(stale) == NULL,
          "exhausted handle keeps returning non-error empty");
    CHECK(stale == NULL || chdb_result_rows_read(stale) == 0, "exhausted handle returns no rows");
    chdb_destroy_query_result(stale);

    CHECK(drain_stream(conn, new_stream) == 3000, "new stream unaffected, exact rows");
    chdb_destroy_query_result(new_stream);
    chdb_destroy_query_result(old_stream);
}

static void test_fetch_after_cancel_still_errors(chdb_connection conn)
{
    printf("== test_fetch_after_cancel_still_errors ==\n");
    chdb_result * stream = chdb_stream_query(conn, "SELECT number FROM numbers(1000000)", "CSV");
    CHECK(stream != NULL && chdb_result_error(stream) == NULL, "stream init ok");

    chdb_result * first = chdb_stream_fetch_result(conn, stream);
    CHECK(first != NULL && chdb_result_error(first) == NULL, "first fetch ok");
    CHECK(first == NULL || chdb_result_rows_read(first) > 0, "first fetch has data");
    chdb_destroy_query_result(first);

    chdb_stream_cancel_query(conn, stream);

    chdb_result * after = chdb_stream_fetch_result(conn, stream);
    CHECK(after != NULL && chdb_result_error(after) != NULL,
          "fetch after cancel reports an error (not silent EOF)");
    chdb_destroy_query_result(after);
    chdb_destroy_query_result(stream);
}

static void test_mid_stream_error_not_idempotent(chdb_connection conn)
{
    printf("== test_mid_stream_error_not_idempotent ==\n");
    /* The error fires past the first block, so at least one fetch succeeds
     * before a fetch surfaces the failure. */
    chdb_result * stream = chdb_stream_query(
        conn,
        "SELECT throwIf(number = 200000, 'mid-stream boom') FROM numbers(1000000)",
        "CSV");
    CHECK(stream != NULL && chdb_result_error(stream) == NULL, "stream init ok");

    int saw_data = 0;
    int saw_error = 0;
    /* The error fires at row 200000; with the default max_block_size (~65k
     * rows/fetch) it surfaces within ~4 fetches, so 64 is ample headroom.
     * The cap is only a safety net — if a smaller block size ever made the
     * loop exit without the error, saw_error stays 0 and the CHECK below
     * flags it (and we cancel the still-active stream so none is left behind). */
    for (int i = 0; i < 64 && !saw_error; i++) {
        chdb_result * chunk = chdb_stream_fetch_result(conn, stream);
        CHECK(chunk != NULL, "fetch never returns NULL");
        if (!chunk)
            break;
        const char * err = chdb_result_error(chunk);
        if (err) {
            saw_error = 1;
            CHECK(strstr(err, "mid-stream boom") != NULL, "error carries the real message");
        } else if (chdb_result_rows_read(chunk) > 0) {
            saw_data = 1;
        } else {
            /* Empty non-error before any error would be a wrong clean EOF. */
            fprintf(stderr, "  stream ended cleanly before the expected error\n");
            g_failed_assertions += 1;
            chdb_destroy_query_result(chunk);
            break;
        }
        chdb_destroy_query_result(chunk);
    }
    CHECK(saw_data, "data flowed before the error");
    CHECK(saw_error, "mid-stream error surfaced on a fetch");

    if (!saw_error) {
        /* Loop escaped without the expected error: cancel so we don't leave an
         * active streaming query behind on the connection. */
        chdb_stream_cancel_query(conn, stream);
        chdb_destroy_query_result(stream);
        return;
    }

    /* Only NATURAL end-of-stream is idempotent: after an error the stream is
     * gone and further fetches must keep reporting an error. */
    chdb_result * after = chdb_stream_fetch_result(conn, stream);
    CHECK(after != NULL && chdb_result_error(after) != NULL,
          "fetch after a mid-stream error reports an error");
    chdb_destroy_query_result(after);
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

    test_fetch_after_eof_is_idempotent_empty(*conn);
    test_exhausted_handle_inert_while_new_stream_active(*conn);
    test_fetch_after_cancel_still_errors(*conn);
    test_mid_stream_error_not_idempotent(*conn);

    chdb_close_conn(conn);

    printf("\n== summary: %d failed assertions ==\n", g_failed_assertions);
    return g_failed_assertions == 0 ? 0 : 1;
}
