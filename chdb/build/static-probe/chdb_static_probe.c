/* Runtime half of the libchdb.a release gates (issue #198).
 *
 * Deliberately plain C and deliberately minimal: the point is not API coverage but that a
 * statically linked libchdb.a reaches a working engine. The historical failure mode was a
 * binary that built fine and then hung on the first chdb_connect, because dyld had bound
 * the bundled libc++/libc++abi weak definitions to the system runtime instead.
 *
 * check_static_lib_hermetic.sh additionally links this with -u for every symbol in the
 * checked-in C API allow-list, so the probe covers the whole contract at link time even
 * though it only calls a few functions at run time.
 */

#include <stdio.h>
#include <string.h>

#include "chdb.h"

int main(void)
{
    char * argv[] = {"chdb_static_probe"};
    chdb_connection * conn = chdb_connect(1, argv);
    if (!conn)
    {
        fprintf(stderr, "probe: chdb_connect returned NULL\n");
        return 1;
    }

    chdb_result * result = chdb_query(*conn, "SELECT 1 + 1", "CSV");
    if (!result)
    {
        fprintf(stderr, "probe: chdb_query returned NULL\n");
        chdb_close_conn(conn);
        return 1;
    }

    const char * error = chdb_result_error(result);
    if (error)
    {
        fprintf(stderr, "probe: query failed: %s\n", error);
        chdb_destroy_query_result(result);
        chdb_close_conn(conn);
        return 1;
    }

    const char * buffer = chdb_result_buffer(result);
    size_t length = chdb_result_length(result);
    if (!buffer || length < 1 || strncmp(buffer, "2", 1) != 0)
    {
        fprintf(stderr, "probe: expected \"2\", got \"%.*s\"\n", (int)length, buffer ? buffer : "");
        chdb_destroy_query_result(result);
        chdb_close_conn(conn);
        return 1;
    }

    printf("probe: chdb_version=%s, SELECT 1 + 1 -> %.*s", chdb_version(), (int)length, buffer);
    chdb_destroy_query_result(result);
    chdb_close_conn(conn);
    return 0;
}
