/**
 * chdbVersionTest.c
 *
 * Version SQL functions through the C API:
 *   - SELECT chdb()     -> the chDB release version (CHDB_VERSION_STRING).
 *   - SELECT version()  -> the underlying ClickHouse engine version.
 *
 * Regression guard: the chDB version must be injected at build time. It was
 * previously left as the literal "None" in libchdb.so builds because the build
 * scripts fed CHDB_VERSION="None" (see chdb/vars.sh). This asserts chdb()
 * returns a real, non-"None" version string.
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbVersionTest.c -I./programs/local -L. -lchdb -o examples/chdbVersionTest
 *   LD_LIBRARY_PATH=. ./examples/chdbVersionTest
 */

#include <ctype.h>
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

static int has_digit(const char * s, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (isdigit((unsigned char) s[i]))
            return 1;
    return 0;
}

/* Runs `sql` (TabSeparated so a single string value comes back unquoted),
 * asserts it succeeded, and returns whether the trimmed output looks like a
 * version (non-empty, not "None", contains a digit). */
static void check_version(chdb_connection conn, const char * label, const char * sql)
{
    chdb_result * r = chdb_query(conn, sql, "TabSeparated");
    const char * err = r ? chdb_result_error(r) : "null result";
    if (err) {
        fprintf(stderr, "  %s ERROR: %s\n", label, err);
        g_failed += 1;
        chdb_destroy_query_result(r);
        return;
    }
    size_t len = chdb_result_length(r);
    const char * buf = chdb_result_buffer(r);
    /* Drop the trailing newline TabSeparated appends. */
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r'))
        len--;

    CHECK(len > 0, "version string is non-empty");
    CHECK(!(len == 4 && memcmp(buf, "None", 4) == 0), "version is not the literal \"None\"");
    CHECK(has_digit(buf, len), "version contains a digit");
    printf("%-10s -> %.*s\n", label, (int) len, buf);
    chdb_destroy_query_result(r);
}

int main(void)
{
    char arg0[] = "chdb";
    char * argv[] = {arg0};
    chdb_connection * conn = chdb_connect(1, argv);
    if (!conn) {
        fprintf(stderr, "chdb_connect failed\n");
        return 1;
    }

    check_version(*conn, "chdb()", "SELECT chdb()");
    check_version(*conn, "version()", "SELECT version()");

    chdb_close_conn(conn);

    printf("\n== summary: %d failed ==\n", g_failed);
    return g_failed ? 1 : 0;
}
