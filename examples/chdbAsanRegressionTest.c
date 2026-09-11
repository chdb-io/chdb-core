/* C-side regressions for the defects the ASan+UBSan build surfaced.
 *
 * On the unfixed engine this binary aborts:
 *   - the second chdb_connect() in the process trips
 *     chassert(!background_context_instance) in Context::makeBackgroundContext()
 *   - the streaming INSERT worker trips chassert(!current_thread) in ThreadStatus,
 *     because startThreadFromGlobalPool already built one for that thread
 *
 * It also pins the result-buffer contract (buf is chdb_result_length() bytes and is not
 * NUL-terminated, so every consumer has to carry the length) and, when run under
 * AddressSanitizer, bounds what one engine start/stop cycle may leave behind.
 */
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include "chdb.h"

static int failures = 0;

#define CHECK(cond, what) \
    do { \
        if (cond) \
            printf("  ok   %s\n", (what)); \
        else { printf("  FAIL %s\n", (what)); failures++; } \
    } while (0)

static chdb_connection * open_memory_connection(void)
{
    char arg0[] = "clickhouse";
    char arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    return chdb_connect(2, args);
}

/* Engine restart inside one process. */
static void test_repeated_connect(void)
{
    printf("test_repeated_connect\n");
    for (int i = 0; i < 3; i++)
    {
        chdb_connection * conn = open_memory_connection();
        CHECK(conn != NULL, "connection opens");
        if (!conn)
            return;

        chdb_result * r = chdb_query(*conn, "SELECT 1", "CSV");
        CHECK(r != NULL && chdb_result_error(r) == NULL, "query succeeds on a restarted engine");
        chdb_destroy_query_result(r);
        chdb_close_conn(conn);
    }
}

/* Streaming INSERT: the worker runs on a pooled thread that already owns a ThreadStatus. */
static void test_insert_stream_worker(void)
{
    printf("test_insert_stream_worker\n");
    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    chdb_destroy_query_result(
        chdb_query(*conn, "CREATE TABLE asan_ins (a UInt64, b String) ENGINE = Memory", "CSV"));

    const char * rows = "1,\"one\"\n2,\"two\"\n";
    chdb_insert_stream stream = chdb_stream_insert(*conn, "INSERT INTO asan_ins (a, b)", "CSV");
    CHECK(stream != NULL && chdb_stream_insert_error(stream) == NULL, "insert stream opens");
    CHECK(chdb_stream_append(stream, rows, strlen(rows)) == CHDBSuccess, "rows appended");
    chdb_destroy_query_result(chdb_stream_done(stream));
    chdb_destroy_insert_stream(stream);

    chdb_result * r = chdb_query(*conn, "SELECT a, b FROM asan_ins ORDER BY a", "CSV");
    CHECK(r != NULL && chdb_result_error(r) == NULL, "streamed rows are queryable");
    if (r)
    {
        const char * expected = "1,\"one\"\n2,\"two\"\n";
        size_t len = chdb_result_length(r);
        CHECK(len == strlen(expected) && memcmp(chdb_result_buffer(r), expected, len) == 0,
              "streamed rows read back intact");
    }
    chdb_destroy_query_result(r);
    chdb_close_conn(conn);
}

/* The buffer is not a C string: length and content must agree without a terminator. */
static void test_result_buffer_is_length_delimited(void)
{
    printf("test_result_buffer_is_length_delimited\n");
    chdb_connection * conn = open_memory_connection();
    CHECK(conn != NULL, "connection opens");
    if (!conn)
        return;

    chdb_result * r = chdb_query(*conn, "SELECT 'abc'", "CSV");
    CHECK(r != NULL && chdb_result_error(r) == NULL, "query succeeds");
    if (r)
    {
        const char * expected = "\"abc\"\n";
        size_t len = chdb_result_length(r);
        /* memcmp over exactly len bytes -- strcmp/strlen here would read out of bounds. */
        CHECK(len == strlen(expected) && memcmp(chdb_result_buffer(r), expected, len) == 0,
              "buffer holds exactly chdb_result_length() bytes");
    }
    chdb_destroy_query_result(r);
    chdb_close_conn(conn);
}


/* Per-cycle growth of the engine. Only meaningful under AddressSanitizer, so the counter
 * is looked up at run time and the check is skipped where it is absent.
 *
 * Budget: re-parsing the built-in users XML on every start used to leak 40,736 bytes per
 * cycle on its own. What remains is the context shared-part family, measured at ~29 KiB
 * per cycle; the budget sits above that and below the regression it guards against. */
#define LEAK_BUDGET_PER_CYCLE (64 * 1024)

static void test_engine_start_leak_budget(void)
{
    size_t (*live_bytes)(void) = (size_t (*)(void))dlsym(RTLD_DEFAULT, "__sanitizer_get_current_allocated_bytes");

    printf("test_engine_start_leak_budget\n");
    if (!live_bytes)
    {
        printf("  skip (not an AddressSanitizer build)\n");
        return;
    }

    /* One cycle first: the engine's one-time lazy globals are not a per-cycle cost. */
    for (int warm = 0; warm < 1; warm++)
    {
        chdb_connection * conn = open_memory_connection();
        if (conn)
        {
            chdb_destroy_query_result(chdb_query(*conn, "SELECT 1", "CSV"));
            chdb_close_conn(conn);
        }
    }

    const int cycles = 10;
    size_t before = live_bytes();
    for (int i = 0; i < cycles; i++)
    {
        chdb_connection * conn = open_memory_connection();
        if (!conn)
        {
            CHECK(0, "connection opens during the leak measurement");
            return;
        }
        chdb_destroy_query_result(chdb_query(*conn, "SELECT count() FROM numbers(100)", "CSV"));
        chdb_close_conn(conn);
    }
    size_t after = live_bytes();

    /* Convert before subtracting: an unrelated free between the two reads would make
     * a size_t subtraction wrap into a huge positive delta. */
    double per_cycle = ((double)after - (double)before) / (double)cycles;
    printf("  %.0f bytes retained per connect/close cycle (budget %d)\n", per_cycle, LEAK_BUDGET_PER_CYCLE);
    CHECK(per_cycle < (double)LEAK_BUDGET_PER_CYCLE, "engine start/stop stays within its leak budget");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    printf("chdb %s\n", chdb_version());
    test_repeated_connect();
    test_insert_stream_worker();
    test_result_buffer_is_length_delimited();
    test_engine_start_leak_budget();
    printf("\nfailures: %d\n", failures);
    return failures == 0 ? 0 : 1;
}
