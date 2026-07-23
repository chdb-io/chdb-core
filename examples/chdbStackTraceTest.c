/**
 * chdbStackTraceTest.c
 *
 * The `--stacktrace` connect flag controls whether chdb error strings carry a
 * stack trace, matching clickhouse-client / clickhouse-local:
 *   - default (no flag): errors are clean, e.g.
 *       "Code: 50. DB::Exception: Unknown data type family: Nonexistent. (UNKNOWN_TYPE)"
 *   - with --stacktrace: the same errors additionally include a "Stack trace" block.
 *
 * Verifies both a query error and a streaming-INSERT init error, under both a
 * default connection and a --stacktrace connection.
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbStackTraceTest.c -I./programs/local \
 *         -L. -lchdb -o examples/chdbStackTraceTest
 *   LD_LIBRARY_PATH=. ./examples/chdbStackTraceTest
 */

#include <stdio.h>
#include <string.h>

#include "chdb.h"

static int g_failed = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "  ASSERT FAIL: %s (%s:%d)\n",                 \
                    (msg), __FILE__, __LINE__);                            \
            g_failed += 1;                                                 \
        }                                                                  \
    } while (0)

/* A query that always fails at analysis time (bad type -> UNKNOWN_TYPE), and
 * a streaming INSERT that fails at init (same bad type). Both produce a real
 * DB::Exception whose formatting is what the flag controls. */
static const char * BAD_QUERY =
    "SELECT * FROM file('chdb_st_none.csv', 'CSV', 'id Nonexistent')";
static const char * BAD_INSERT =
    "INSERT INTO FUNCTION file('chdb_st_none.csv', 'CSV', 'id Nonexistent')";

/* Runs one bad query + one bad insert on `conn` and reports, for each, whether
 * the error string contained a stack trace. want_trace drives the assertions. */
static void check_conn(const char * label, chdb_connection * conn, int want_trace)
{
    printf("== %s ==\n", label);

    chdb_result * q = chdb_query(*conn, BAD_QUERY, "CSV");
    const char * qe = q ? chdb_result_error(q) : NULL;
    CHECK(qe != NULL, "query error is set");
    CHECK(qe && strstr(qe, "Nonexistent") != NULL, "query error names the real cause");
    int q_has_trace = qe && strstr(qe, "Stack trace") != NULL;
    CHECK(q_has_trace == want_trace,
          want_trace ? "query error includes a stack trace" : "query error omits the stack trace");
    chdb_destroy_query_result(q);

    chdb_insert_stream s = chdb_stream_insert(*conn, BAD_INSERT, "CSV");
    const char * se = chdb_stream_insert_error(s);
    CHECK(se != NULL, "insert init error is set");
    CHECK(se && strstr(se, "Nonexistent") != NULL, "insert error names the real cause");
    int s_has_trace = se && strstr(se, "Stack trace") != NULL;
    CHECK(s_has_trace == want_trace,
          want_trace ? "insert error includes a stack trace" : "insert error omits the stack trace");
    chdb_destroy_insert_stream(s);
}

int main(void)
{
    /* Default connection: errors must be clean (no stack trace). */
    {
        char a0[] = "chdb", a1[] = "--multiquery";
        char * argv[] = {a0, a1};
        chdb_connection * conn = chdb_connect(2, argv);
        if (!conn) { fprintf(stderr, "connect failed\n"); return 1; }
        check_conn("default (no --stacktrace)", conn, /*want_trace=*/0);
        chdb_close_conn(conn);
    }

    /* --stacktrace connection: same errors must include a stack trace. Runs
     * after the default connection is closed (one embedded server at a time). */
    {
        char a0[] = "chdb", a1[] = "--multiquery", a2[] = "--stacktrace";
        char * argv[] = {a0, a1, a2};
        chdb_connection * conn = chdb_connect(3, argv);
        if (!conn) { fprintf(stderr, "connect with --stacktrace failed\n"); return 1; }
        check_conn("--stacktrace", conn, /*want_trace=*/1);
        chdb_close_conn(conn);
    }

    printf("\n== summary: %d failed ==\n", g_failed);
    return g_failed ? 1 : 0;
}
