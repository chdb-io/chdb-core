/**
 * chdbQueryParamsTest.c — chdb_query_with_params usage at a glance.
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

/* Print one call's result and check it against the expected CSV output. */
static void show(const char * label, const char * expected, chdb_result * r)
{
    const char * err = r ? chdb_result_error(r) : "null result";
    if (err) { printf("%-22s ERROR: %s\n", label, err); g_failed++; }
    else {
        int ok = chdb_result_length(r) == strlen(expected)
              && memcmp(chdb_result_buffer(r), expected, strlen(expected)) == 0;
        printf("%-22s -> %.*s%s", label,
               (int)chdb_result_length(r), chdb_result_buffer(r), ok ? "" : "   [MISMATCH]\n");
        if (!ok) g_failed++;
    }
    chdb_destroy_query_result(r);
}

int main(void)
{
    char arg0[] = "chdb", arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    chdb_connection * conn = chdb_connect(2, args);
    if (!conn) return 1;

    chdb_result * setup = chdb_query(*conn,
        "CREATE TABLE users (id UInt32, name String) ENGINE = Memory;"
        "INSERT INTO users VALUES (1,'Alice'),(2,'Bob'),(3,'O''Hara')", "CSV");
    chdb_destroy_query_result(setup);

    /* 1. Scalars: values are strings, {name:Type} does the typing. */
    {
        const char * names[]  = {"id", "name"};
        const char * values[] = {"42", "Alice"};
        show("1. scalars", "42,\"Alice\"\n", chdb_query_with_params(*conn,
            "SELECT {id:UInt32} AS id, {name:String} AS name",
            "CSV", names, values, 2));
    }

    /* 2. Strings need no escaping — and can't inject. */
    {
        const char * names[]  = {"who"};
        const char * values[] = {"O'Hara"};
        show("2. quote-safe WHERE", "3\n", chdb_query_with_params(*conn,
            "SELECT id FROM users WHERE name = {who:String}",
            "CSV", names, values, 1));
    }

    /* 3. Array parameter for IN. */
    {
        const char * names[]  = {"ids"};
        const char * values[] = {"[1,3]"};
        show("3. array / IN", "2\n", chdb_query_with_params(*conn,
            "SELECT count() FROM users WHERE id IN {ids:Array(UInt32)}",
            "CSV", names, values, 1));
    }

    /* 4. Identifier parameter: bind a table (or column) name. */
    {
        const char * names[]  = {"tbl"};
        const char * values[] = {"users"};
        show("4. identifier", "3\n", chdb_query_with_params(*conn,
            "SELECT count() FROM {tbl:Identifier}",
            "CSV", names, values, 1));
    }

    /* 5. Matching is by name: array order is independent of SQL order,
     *    and one parameter can be referenced multiple times. */
    {
        const char * names[]  = {"name", "id"};
        const char * values[] = {"Alice", "7"};
        show("5. by-name + reuse", "14,\"Alice\"\n", chdb_query_with_params(*conn,
            "SELECT {id:UInt32} + {id:UInt32} AS twice, {name:String} AS name",
            "CSV", names, values, 2));
    }

    /* 6. _n variant: explicit lengths, values may contain NUL bytes. */
    {
        const char * names[]     = {"s"};
        const char * values[]    = {"a\0b"};                /* 3 bytes, embedded NUL */
        const size_t name_lens[]  = {1};
        const size_t value_lens[] = {3};
        const char * query = "SELECT length({s:String}) AS len";
        show("6. _n, embedded NUL", "3\n", chdb_query_with_params_n(*conn,
            query, strlen(query), "CSV", 3,
            names, name_lens, values, value_lens, 1));
    }

    /* 7. Streaming query with parameters: same arrays, then the fetch loop. */
    {
        const char * names[]  = {"limit"};
        const char * values[] = {"100000"};
        chdb_result * stream = chdb_stream_query_with_params(*conn,
            "SELECT number FROM numbers({limit:UInt64})",
            "CSV", names, values, 1);
        size_t rows = 0;
        for (;;) {
            chdb_result * chunk = chdb_stream_fetch_result(*conn, stream);
            size_t got = chdb_result_rows_read(chunk);
            chdb_destroy_query_result(chunk);
            if (got == 0) break;
            rows += got;
        }
        chdb_destroy_query_result(stream);
        printf("%-22s -> %zu rows\n", "7. streaming + params", rows);
        if (rows != 100000) g_failed++;
    }

    /* 8. A missing parameter is a clean error (BAD_QUERY_PARAMETER). */
    {
        chdb_result * r = chdb_query_with_params(*conn,
            "SELECT {nope:UInt32}", "CSV", NULL, NULL, 0);
        printf("%-22s -> %s\n", "8. missing param",
               (r && chdb_result_error(r)) ? "error, as expected" : "NO ERROR (bug!)");
        if (!(r && chdb_result_error(r))) g_failed++;
        chdb_destroy_query_result(r);
    }

    chdb_close_conn(conn);
    printf("summary: %d failed\n", g_failed);
    return g_failed ? 1 : 0;
}
