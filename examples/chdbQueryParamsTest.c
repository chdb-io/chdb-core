/**
 * chdbQueryParamsTest.c — usage examples for the parameter-binding C API
 * (chdb_query_with_params family).
 *
 * Queries use {name:Type} placeholders. Parameters are two parallel string
 * arrays: names[i] pairs with values[i]. Values are always strings — the
 * {name:Type} annotation does the typing — and matching is by name, so
 * array order doesn't matter.
 *
 * Build: clang examples/chdbQueryParamsTest.c -I./programs/local -L. -lchdb \
 *          -o examples/chdbQueryParamsTest
 * Run:   LD_LIBRARY_PATH=. ./examples/chdbQueryParamsTest
 */

#include <stdio.h>
#include <string.h>
#include "chdb.h"

static int g_failed = 0;

/* Run a parameterized query and check its CSV output. */
static void expect(chdb_connection conn, const char * label, const char * query,
                   const char * const * names, const char * const * values,
                   size_t count, const char * expected)
{
    chdb_result * r = chdb_query_with_params(conn, query, "CSV", names, values, count);
    const char * err = r ? chdb_result_error(r) : "null result";
    int ok = !err && chdb_result_length(r) == strlen(expected)
                  && memcmp(chdb_result_buffer(r), expected, strlen(expected)) == 0;
    printf("%-24s %s\n", label, ok ? "OK" : (err ? err : "MISMATCH"));
    if (!ok) g_failed++;
    chdb_destroy_query_result(r);
}

int main(void)
{
    char arg0[] = "clickhouse", arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    chdb_connection * conn = chdb_connect(2, args);
    if (!conn) return 1;

    chdb_result * r = chdb_query(*conn,
        "CREATE TABLE users (id UInt32, name String) ENGINE = Memory;"
        "INSERT INTO users VALUES (1,'Alice'),(2,'Bob'),(3,'O''Hara')", "CSV");
    chdb_destroy_query_result(r);

    /* Basic scalars. */
    {
        const char * n[] = {"id", "name"}, * v[] = {"42", "Alice"};
        expect(*conn, "scalars",
               "SELECT {id:UInt32} AS id, {name:String} AS name", n, v, 2, "42,\"Alice\"\n");
    }
    /* No escaping, no SQL injection — the main reason to use parameters. */
    {
        const char * n[] = {"who"}, * v[] = {"O'Hara"};
        expect(*conn, "quote-safe WHERE",
               "SELECT id FROM users WHERE name = {who:String}", n, v, 1, "3\n");
    }
    /* Array parameter for IN. */
    {
        const char * n[] = {"ids"}, * v[] = {"[1,3]"};
        expect(*conn, "array / IN",
               "SELECT count() FROM users WHERE id IN {ids:Array(UInt32)}", n, v, 1, "2\n");
    }
    /* Identifier parameter: bind a table (or column) name. */
    {
        const char * n[] = {"tbl"}, * v[] = {"users"};
        expect(*conn, "identifier",
               "SELECT count() FROM {tbl:Identifier}", n, v, 1, "3\n");
    }
    /* By-name matching: array order != SQL order; one param used twice. */
    {
        const char * n[] = {"name", "id"}, * v[] = {"Alice", "7"};
        expect(*conn, "by-name + reuse",
               "SELECT {id:UInt32} + {id:UInt32}, {name:String}", n, v, 2, "14,\"Alice\"\n");
    }
    /* _n variant: explicit lengths, values may contain NUL bytes. */
    {
        const char * n[] = {"s"}, * v[] = {"a\0b"};
        const size_t nl[] = {1}, vl[] = {3};
        const char * q = "SELECT length({s:String})";
        chdb_result * res = chdb_query_with_params_n(*conn, q, strlen(q), "CSV", 3, n, nl, v, vl, 1);
        int ok = res && !chdb_result_error(res) && memcmp(chdb_result_buffer(res), "3\n", 2) == 0;
        printf("%-24s %s\n", "_n (embedded NUL)", ok ? "OK" : "FAIL");
        if (!ok) g_failed++;
        chdb_destroy_query_result(res);
    }
    /* Streaming query with parameters: same arrays, then the usual fetch loop. */
    {
        const char * n[] = {"limit"}, * v[] = {"100000"};
        chdb_result * stream = chdb_stream_query_with_params(*conn,
            "SELECT number FROM numbers({limit:UInt64})", "CSV", n, v, 1);
        size_t rows = 0;
        for (;;) {
            chdb_result * chunk = chdb_stream_fetch_result(*conn, stream);
            size_t got = chdb_result_rows_read(chunk);
            chdb_destroy_query_result(chunk);
            if (got == 0) break;
            rows += got;
        }
        chdb_destroy_query_result(stream);
        printf("%-24s %s\n", "streaming + params", rows == 100000 ? "OK" : "FAIL");
        if (rows != 100000) g_failed++;
    }
    /* A missing parameter is a clean error. */
    {
        chdb_result * res = chdb_query_with_params(*conn, "SELECT {nope:UInt32}", "CSV", NULL, NULL, 0);
        int ok = res && chdb_result_error(res) != NULL;
        printf("%-24s %s\n", "missing param -> error", ok ? "OK" : "FAIL");
        if (!ok) g_failed++;
        chdb_destroy_query_result(res);
    }

    chdb_close_conn(conn);
    printf("summary: %d failed\n", g_failed);
    return g_failed ? 1 : 0;
}
