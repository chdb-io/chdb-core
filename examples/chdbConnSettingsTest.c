/**
 * chdbConnSettingsTest.c
 *
 * Connection-argv settings through the C API (chdb-io/chdb-core#191):
 * arguments naming a query-level setting (--key=value, --key value, bare
 * --key) must be applied to that connection's own session, so concurrent
 * connections to the same path keep isolated settings, exactly as if each
 * had executed SET right after connecting.
 *
 * Covers the forms only reachable from the raw C API (the Python DSN always
 * emits --key=value):
 *   - --key=value, incl. a value on an aliased/experimental setting
 *   - two-token "--key value", incl. a negative value ("--key -1")
 *   - bare boolean flag ("--final")
 *   - isolation between two simultaneous connections on the same path
 *   - per-query SETTINGS clause still overriding the connection-level value
 *   - invalid value: chdb_connect returns NULL, existing connections keep
 *     working, and the failed connect does not pin the engine's refcount
 *     (a later connect succeeds).
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbConnSettingsTest.c -I./programs/local -L. -lchdb -o examples/chdbConnSettingsTest
 *   LD_LIBRARY_PATH=. ./examples/chdbConnSettingsTest
 */

#include <stdio.h>
#include <string.h>

#include "chdb.h"

static int g_failed = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "  ASSERT FAIL: %s (%s:%d)\n",                 \
                    (msg), __FILE__, __LINE__);                           \
            g_failed = 1;                                                  \
        }                                                                  \
    } while (0)

/// Runs a query and copies its output (or "<ERR>" on error) into buf.
static void query_into(chdb_connection conn, const char * sql, const char * fmt, char * buf, size_t buf_size)
{
    buf[0] = '\0';
    chdb_result * r = chdb_query(conn, sql, fmt);
    if (!r)
    {
        snprintf(buf, buf_size, "<ERR>");
        return;
    }
    const char * err = chdb_result_error(r);
    if (err)
        snprintf(buf, buf_size, "<ERR>");
    else
    {
        size_t len = chdb_result_length(r);
        if (len >= buf_size)
            len = buf_size - 1;
        memcpy(buf, chdb_result_buffer(r), len);
        buf[len] = '\0';
        /// Trim trailing newline for easy strcmp.
        while (len > 0 && buf[len - 1] == '\n')
            buf[--len] = '\0';
    }
    chdb_destroy_query_result(r);
}

int main(void)
{
    char buf[256];

    /// Connection A: =-form values, an experimental (aliased) setting, and a
    /// bare boolean flag.
    char * argv_a[] = {
        "clickhouse",
        "--output_format_json_quote_denormals=1",
        "--allow_experimental_nullable_tuple_type=1",
        "--max_threads=1",
        "--final"};
    chdb_connection * a = chdb_connect(5, argv_a);
    CHECK(a != NULL, "connection A with settings argv connects");
    if (!a)
        return 1;

    /// Connection B: two-token form with a negative value, on the same
    /// (default) path while A is open.
    char * argv_b[] = {"clickhouse", "--max_threads", "2", "--max_partitions_to_read", "-1"};
    chdb_connection * b = chdb_connect(5, argv_b);
    CHECK(b != NULL, "connection B with two-token argv connects");
    if (!b)
        return 1;

    /// A's settings took effect...
    query_into(*a, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":\"nan\"}") == 0, "A: output_format_json_quote_denormals=1 quotes nan");
    query_into(*a, "SELECT getSetting('allow_experimental_nullable_tuple_type')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "true") == 0 || strcmp(buf, "1") == 0, "A: experimental setting applied");
    query_into(*a, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "1") == 0, "A: max_threads=1 applied");
    query_into(*a, "SELECT getSetting('final')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "true") == 0 || strcmp(buf, "1") == 0, "A: bare --final means 1");

    /// ...and did not leak into B, whose own settings also took effect.
    query_into(*b, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":null}") == 0, "B: default JSON denormal rendering (no leak from A)");
    query_into(*b, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "2") == 0, "B: two-token --max_threads 2 applied");
    query_into(*b, "SELECT getSetting('max_partitions_to_read')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "-1") == 0, "B: two-token negative value parsed as -1, not mangled to 1");

    /// Per-query SETTINGS clause still overrides the connection-level value,
    /// without changing it.
    query_into(*a, "SELECT nan AS x SETTINGS output_format_json_quote_denormals = 0", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":null}") == 0, "A: per-query SETTINGS overrides connection argv");
    query_into(*a, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":\"nan\"}") == 0, "A: connection-level value intact after the override");

    /// An invalid value for a known setting must fail the connect...
    char * argv_c[] = {"clickhouse", "--max_threads=bogus"};
    chdb_connection * c = chdb_connect(2, argv_c);
    CHECK(c == NULL, "invalid setting value is rejected at connect time");

    /// ...without harming live connections or pinning the engine's refcount
    /// (a later connect must still succeed).
    query_into(*a, "SELECT 1", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "1") == 0, "A still works after the rejected connect");
    char * argv_d[] = {"clickhouse"};
    chdb_connection * d = chdb_connect(1, argv_d);
    CHECK(d != NULL, "a later plain connect succeeds (no refcount leak from the failure)");
    if (d)
    {
        query_into(*d, "SELECT changed FROM system.settings WHERE name = 'max_threads'", "CSV", buf, sizeof(buf));
        CHECK(strcmp(buf, "0") == 0, "plain connection inherited neither A's nor B's max_threads");
        chdb_close_conn(d);
    }

    chdb_close_conn(b);
    chdb_close_conn(a);

    if (g_failed)
    {
        fprintf(stderr, "chdbConnSettingsTest: FAILED\n");
        return 1;
    }
    printf("chdbConnSettingsTest: OK\n");
    return 0;
}
