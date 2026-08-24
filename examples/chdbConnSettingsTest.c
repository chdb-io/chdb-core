/**
 * chdbConnSettingsTest.c
 *
 * Connection-argv settings through the C API (chdb-io/chdb-core#191):
 * arguments naming a query-level setting (--key=value, --key value, bare
 * --key) must be applied to that connection's own session, so concurrent
 * connections to the same path keep isolated settings, exactly as if each
 * had executed SET right after connecting.
 *
 * Mirrors tests/test_connection_argv_settings.py case for case, plus the
 * forms only reachable from the raw C API (the Python DSN always emits
 * --key=value):
 *   - --key=value, incl. negative values and an aliased/experimental
 *     setting asserted functionally (Nullable(Tuple) usable), not just via
 *     its reported value
 *   - two-token "--key value", incl. a negative value ("--key -1")
 *   - bare boolean flag ("--final")
 *   - isolation between two simultaneous connections on the same path,
 *     and no inheritance by a later plain connection
 *   - per-query SETTINGS clause overriding the connection-level value
 *     without changing it
 *   - SET on one connection persisting there and not leaking to another
 *   - settings not surviving an engine restart (all connections closed)
 *   - unknown --keys ignored (left to the server-option config layer)
 *   - invalid value: chdb_connect returns NULL, existing connections keep
 *     working, and the failed connect does not pin the engine's refcount
 *   - --readonly=2 on a file-backed path: writes rejected, reads and
 *     per-query SETTINGS still allowed (what the Python DSN mode=ro maps to)
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

/// Runs a query and copies its output (or "<ERR:message>" on error) into buf.
static void query_into(chdb_connection conn, const char * sql, const char * fmt, char * buf, size_t buf_size)
{
    buf[0] = '\0';
    chdb_result * r = chdb_query(conn, sql, fmt);
    if (!r)
    {
        snprintf(buf, buf_size, "<ERR:null result>");
        return;
    }
    const char * err = chdb_result_error(r);
    if (err)
        snprintf(buf, buf_size, "<ERR:%s>", err);
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

static int is_error(const char * buf)
{
    return strncmp(buf, "<ERR:", 5) == 0;
}

static int is_true(const char * buf)
{
    return strcmp(buf, "true") == 0 || strcmp(buf, "1") == 0;
}

/// value,changed of a setting as seen by this connection, e.g. "8,0".
static void setting_row(chdb_connection conn, const char * name, char * buf, size_t buf_size)
{
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT changed FROM system.settings WHERE name = '%s'", name);
    query_into(conn, sql, "CSV", buf, buf_size);
}

/// Phase 1: everything on the shared default (:memory:) path.
static void test_memory_path(void)
{
    char buf[512];

    /// Connection A: =-form values (incl. a negative one), an experimental
    /// (aliased) setting, and a bare boolean flag — the reporter's exact
    /// flags from #191 among them.
    char * argv_a[] = {
        "clickhouse",
        "--output_format_json_quote_denormals=1",
        "--allow_experimental_nullable_tuple_type=1",
        "--max_threads=1",
        "--max_partitions_to_read=-1",
        "--final"};
    chdb_connection * a = chdb_connect(6, argv_a);
    CHECK(a != NULL, "connection A with settings argv connects");
    if (!a)
        return;

    /// Connection B: two-token form with a negative value, open on the same
    /// path while A is open.
    char * argv_b[] = {"clickhouse", "--max_threads", "2", "--max_partitions_to_read", "-1"};
    chdb_connection * b = chdb_connect(5, argv_b);
    CHECK(b != NULL, "connection B with two-token argv connects");
    if (!b)
        return;

    /// A's settings took effect: reported values...
    query_into(*a, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "1") == 0, "A: max_threads=1 applied");
    query_into(*a, "SELECT getSetting('max_partitions_to_read')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "-1") == 0, "A: =-form negative value applied as -1");
    query_into(*a, "SELECT getSetting('final')", "CSV", buf, sizeof(buf));
    CHECK(is_true(buf), "A: bare --final means 1");
    setting_row(*a, "output_format_json_quote_denormals", buf, sizeof(buf));
    CHECK(strcmp(buf, "1") == 0, "A: setting is marked changed in system.settings");

    /// ...and actual query behavior.
    query_into(*a, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":\"nan\"}") == 0, "A: output_format_json_quote_denormals=1 quotes nan");
    query_into(*a, "SELECT CAST(NULL, 'Nullable(Tuple(Int32))') AS t", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "\\N") == 0, "A: experimental Nullable(Tuple) actually usable");

    /// B has its own settings and inherited nothing from A.
    query_into(*b, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":null}") == 0, "B: default JSON denormal rendering (no leak from A)");
    query_into(*b, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "2") == 0, "B: two-token --max_threads 2 applied");
    query_into(*b, "SELECT getSetting('max_partitions_to_read')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "-1") == 0, "B: two-token negative value parsed as -1, not mangled to 1");
    query_into(*b, "SELECT CAST(NULL, 'Nullable(Tuple(Int32))') AS t", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "B: experimental type still rejected without the setting");

    /// A's isolated view is intact after B's queries.
    query_into(*a, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "1") == 0, "A: value unchanged after B ran queries");

    /// Per-query SETTINGS clause overrides the connection-level value,
    /// without changing it.
    query_into(*a, "SELECT nan AS x SETTINGS output_format_json_quote_denormals = 0", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":null}") == 0, "A: per-query SETTINGS overrides connection argv");
    query_into(*a, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":\"nan\"}") == 0, "A: connection-level value intact after the override");

    /// SET persists on the issuing connection and stays isolated from others.
    query_into(*b, "SET output_format_json_quote_denormals = 1", "CSV", buf, sizeof(buf));
    CHECK(!is_error(buf), "B: SET accepted");
    query_into(*b, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":\"nan\"}") == 0, "B: SET persists for later queries on the same connection");

    /// Unknown --keys are ignored, not errors (they belong to the
    /// server-option config layer).
    char * argv_u[] = {"clickhouse", "--definitely_not_a_chdb_setting=42"};
    chdb_connection * u = chdb_connect(2, argv_u);
    CHECK(u != NULL, "unknown option is ignored, connect succeeds");
    if (u)
    {
        query_into(*u, "SELECT 1", "CSV", buf, sizeof(buf));
        CHECK(strcmp(buf, "1") == 0, "connection with unknown option works");
        chdb_close_conn(u);
    }

    /// An invalid value for a known setting must fail the connect...
    char * argv_c[] = {"clickhouse", "--max_threads=bogus"};
    chdb_connection * c = chdb_connect(2, argv_c);
    CHECK(c == NULL, "invalid setting value is rejected at connect time");

    /// ...without harming live connections; a later same-path connect must
    /// succeed and inherit nothing. (A same-path connect cannot detect a
    /// leaked engine reference — the discriminating check is phase 2's
    /// different-path connect after full shutdown.)
    query_into(*a, "SELECT 1", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "1") == 0, "A still works after the rejected connect");
    char * argv_d[] = {"clickhouse"};
    chdb_connection * d = chdb_connect(1, argv_d);
    CHECK(d != NULL, "a later plain connect succeeds while A/B stay open");
    if (d)
    {
        setting_row(*d, "max_threads", buf, sizeof(buf));
        CHECK(strcmp(buf, "0") == 0, "plain connection inherited neither A's nor B's max_threads");
        chdb_close_conn(d);
    }

    chdb_close_conn(b);
    chdb_close_conn(a);

    /// All connections are gone, so the engine restarted: settings must not
    /// survive into a fresh connection.
    chdb_connection * e = chdb_connect(1, argv_d);
    CHECK(e != NULL, "reconnect after full shutdown succeeds");
    if (e)
    {
        setting_row(*e, "max_threads", buf, sizeof(buf));
        CHECK(strcmp(buf, "0") == 0, "settings do not survive an engine restart");
        setting_row(*e, "output_format_json_quote_denormals", buf, sizeof(buf));
        CHECK(strcmp(buf, "0") == 0, "format setting does not survive an engine restart");
        chdb_close_conn(e);
    }
}

/// Phase 2: file-backed path (runs after phase 1 released the engine).
/// --readonly=2 is what the Python DSN mode=ro maps to: writes and DDL are
/// rejected, but reads and per-query settings tuning still work.
static void test_readonly_file_path(void)
{
    char buf[512];
    static const char * dir = "chdb_conn_settings_test_db"; /// cleaned by runConnSettingsTest.sh

    /// This different-path connect doubles as the refcount-leak regression
    /// check: phase 1's rejected connect (--max_threads=bogus) must have
    /// released its engine reference, otherwise the engine is still pinned
    /// to ':memory:' and connecting to this path throws BAD_ARGUMENTS.
    char * argv_rw[] = {"clickhouse", "--path=chdb_conn_settings_test_db"};
    chdb_connection * rw = chdb_connect(2, argv_rw);
    CHECK(rw != NULL, "different-path connect succeeds: the failed connect did not pin the engine");
    if (!rw)
        return;
    (void)dir;
    query_into(*rw, "CREATE TABLE conn_settings_ro_t (x Int32) ENGINE = MergeTree() ORDER BY x", "CSV", buf, sizeof(buf));
    CHECK(!is_error(buf), "rw: CREATE TABLE works");
    query_into(*rw, "INSERT INTO conn_settings_ro_t VALUES (7)", "CSV", buf, sizeof(buf));
    CHECK(!is_error(buf), "rw: INSERT works");
    chdb_close_conn(rw);

    char * argv_ro[] = {"clickhouse", "--path=chdb_conn_settings_test_db", "--readonly=2"};
    chdb_connection * ro = chdb_connect(3, argv_ro);
    CHECK(ro != NULL, "file-backed readonly connection connects");
    if (!ro)
        return;
    query_into(*ro, "SELECT x FROM conn_settings_ro_t", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "ro: reads work");
    query_into(*ro, "SELECT x FROM conn_settings_ro_t SETTINGS max_threads = 1", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "ro: per-query SETTINGS still allowed under readonly=2");
    query_into(*ro, "INSERT INTO conn_settings_ro_t VALUES (8)", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "ro: writes are rejected");
    chdb_close_conn(ro);
}

int main(void)
{
    test_memory_path();
    if (!g_failed)
        test_readonly_file_path();

    if (g_failed)
    {
        fprintf(stderr, "chdbConnSettingsTest: FAILED\n");
        return 1;
    }
    printf("chdbConnSettingsTest: OK\n");
    return 0;
}
