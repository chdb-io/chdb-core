/**
 * chdbDatabaseAndMultiPathTest.c
 *
 * Phase 3 (P3) C ABI verification suite for:
 *   1. Two-level `chdb_database` & `chdb_connection` ABI (`chdb_open`,
 *      `chdb_close_database`, `chdb_connect_database`)
 *   2. Zero-disk isolated `:memory:` `DatabaseMemory` namespaces vs legacy
 *      shared `chdb_connect(":memory:")`
 *   3. Single-process multi-path coexistence (`/dir/A`, `/dir/B`, `:memory:`),
 *      `<path>/status` cross-process lock acquisition & release on close, and
 *      persistence across remounts
 *
 * Build (against libchdb.so):
 *   clang examples/chdbDatabaseAndMultiPathTest.c -I./programs/local \
 *         -L. -lchdb -lpthread -o examples/chdbDatabaseAndMultiPathTest
 *   LD_LIBRARY_PATH=. ./examples/chdbDatabaseAndMultiPathTest
 */

#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <dirent.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "chdb.h"

static int g_failures = 0;

#define CHECK(cond, msg)                                                           \
    do {                                                                           \
        if (cond) {                                                                \
            printf("  ok   %s\n", (msg));                                          \
        } else {                                                                   \
            fprintf(stderr, "  FAIL %s (%s:%d)\n", (msg), __FILE__, __LINE__);     \
            g_failures += 1;                                                       \
        }                                                                          \
    } while (0)

static int run_scalar(chdb_connection conn, const char * sql, char * out, size_t out_cap)
{
    out[0] = '\0';
    chdb_result * r = chdb_query(conn, sql, "TSVRaw");
    if (!r)
        return -1;
    const char * err = chdb_result_error(r);
    if (err)
    {
        snprintf(out, out_cap, "ERR:%s", err);
        chdb_destroy_query_result(r);
        return -1;
    }
    size_t len = chdb_result_length(r);
    if (len >= out_cap)
        len = out_cap - 1;
    if (len > 0)
        memcpy(out, chdb_result_buffer(r), len);
    out[len] = '\0';
    while (len > 0 && (out[len - 1] == '\n' || out[len - 1] == '\r'))
        out[--len] = '\0';
    chdb_destroy_query_result(r);
    return 0;
}

static size_t count_embedded_temp_dirs(const char * tmp_root)
{
    DIR * dir = opendir(tmp_root);
    if (!dir)
        return 0;
    size_t count = 0;
    struct dirent * ent;
    while ((ent = readdir(dir)) != NULL)
    {
        if (strncmp(ent->d_name, "clickhouse-embedded-", 20) == 0)
            count += 1;
    }
    closedir(dir);
    return count;
}

static void test_abi_argument_validation(void)
{
    printf("== test_abi_argument_validation ==\n");

    CHECK(chdb_open(":memory:", NULL, NULL) == CHDBError, "chdb_open rejects NULL out_db");

    chdb_open_options bad_open;
    memset(&bad_open, 0, sizeof(bad_open));
    bad_open.struct_size = 4;
    chdb_database db = NULL;
    CHECK(chdb_open(":memory:", &bad_open, &db) == CHDBError && db == NULL,
          "chdb_open rejects undersized struct_size");

    bad_open.struct_size = sizeof(bad_open);
    bad_open.argc = 2;
    bad_open.argv = NULL;
    CHECK(chdb_open(":memory:", &bad_open, &db) == CHDBError && db == NULL,
          "chdb_open rejects positive argc with NULL argv");

    chdb_connection conn = NULL;
    CHECK(chdb_connect_database(NULL, NULL, &conn) == CHDBError && conn == NULL,
          "chdb_connect_database rejects NULL database");

    /* Idempotent NULL closes */
    chdb_close_database(NULL);
    chdb_close_database(&db);
    chdb_close_conn(NULL);
    chdb_close_conn(&conn);
    CHECK(db == NULL && conn == NULL, "NULL / repeated closes are safe no-ops");
}

static void test_zero_disk_isolated_memory_databases(void)
{
    printf("== test_zero_disk_isolated_memory_databases ==\n");

    const char * tmp_root = getenv("TMPDIR");
    if (!tmp_root || tmp_root[0] == '\0')
        tmp_root = "/tmp";
    const size_t before_dirs = count_embedded_temp_dirs(tmp_root);

    chdb_database db1 = NULL;
    chdb_database db2 = NULL;
    CHECK(chdb_open(":memory:", NULL, &db1) == CHDBSuccess && db1 != NULL, "open isolated :memory: db1");
    CHECK(chdb_open(NULL, NULL, &db2) == CHDBSuccess && db2 != NULL, "open isolated :memory: db2 (NULL path)");

    chdb_connection c1a = NULL;
    chdb_connection c1b = NULL;
    chdb_connection c2 = NULL;
    CHECK(chdb_connect_database(db1, NULL, &c1a) == CHDBSuccess && c1a != NULL, "connect c1a on db1");

    const char * ro_argv[] = {"--max_threads=2"};
    chdb_connect_options c1b_opts = {sizeof(chdb_connect_options), 1, 1, ro_argv};
    CHECK(chdb_connect_database(db1, &c1b_opts, &c1b) == CHDBSuccess && c1b != NULL,
          "connect read-only c1b with --max_threads=2 on db1");
    CHECK(chdb_connect_database(db2, NULL, &c2) == CHDBSuccess && c2 != NULL, "connect c2 on db2");

    char buf[256];
    CHECK(run_scalar(c1a, "SELECT currentDatabase()", buf, sizeof(buf)) == 0 && strcmp(buf, "default") == 0,
          "c1a currentDatabase() is 'default'");
    CHECK(run_scalar(c1a, "CREATE TABLE kv (k UInt32, v String) ENGINE = Memory", buf, sizeof(buf)) == 0,
          "create kv in db1");
    CHECK(run_scalar(c1a, "INSERT INTO kv VALUES (1, 'alpha'), (2, 'beta')", buf, sizeof(buf)) == 0,
          "insert into db1.kv");
    CHECK(run_scalar(c1a, "CREATE DATABASE tenant_one", buf, sizeof(buf)) == 0,
          "create user database tenant_one in db1");

    /* c1b shares db1's catalog, respects readonly=2, and keeps its own max_threads=2 */
    CHECK(run_scalar(c1b, "SELECT count() FROM kv", buf, sizeof(buf)) == 0 && strcmp(buf, "2") == 0,
          "c1b sees db1.kv rows");
    CHECK(run_scalar(c1b, "SELECT getSetting('max_threads')", buf, sizeof(buf)) == 0 && strcmp(buf, "2") == 0,
          "c1b applied per-connection max_threads=2");
    CHECK(run_scalar(c1b, "INSERT INTO kv VALUES (3, 'denied')", buf, sizeof(buf)) != 0,
          "read-only c1b rejects INSERT");

    /* db2 is 100% isolated from db1 */
    CHECK(run_scalar(c2, "SELECT count() FROM system.tables WHERE database = 'default' AND name = 'kv'", buf, sizeof(buf)) == 0
              && strcmp(buf, "0") == 0,
          "db2 does not see db1's default.kv");
    CHECK(run_scalar(c2, "SELECT count() FROM system.databases WHERE name = 'tenant_one'", buf, sizeof(buf)) == 0
              && strcmp(buf, "0") == 0,
          "db2 SHOW DATABASES / system.databases does not see db1's tenant_one");
    CHECK(run_scalar(c2, "CREATE TABLE kv (only_id Int64) ENGINE = Memory", buf, sizeof(buf)) == 0
              && run_scalar(c2, "INSERT INTO kv VALUES (999)", buf, sizeof(buf)) == 0,
          "db2 defines its own incompatible default.kv schema");
    CHECK(run_scalar(c2, "SELECT sum(only_id) FROM kv", buf, sizeof(buf)) == 0 && strcmp(buf, "999") == 0,
          "db2 queries its own default.kv");

    const size_t during_dirs = count_embedded_temp_dirs(tmp_root);
    CHECK(during_dirs == before_dirs, "zero /tmp/clickhouse-embedded-* directories created for :memory: databases");

    /* Reverse close order on db2: close db2 handle while c2 is still open */
    chdb_database stale_db2 = db2;
    chdb_close_database(&db2);
    CHECK(db2 == NULL, "chdb_close_database nulls handle");

    chdb_connection rejected = NULL;
    CHECK(chdb_connect_database(stale_db2, NULL, &rejected) == CHDBError && rejected == NULL,
          "chdb_connect_database rejects closed database handle");
    CHECK(run_scalar(c2, "SELECT sum(only_id) FROM kv", buf, sizeof(buf)) == 0 && strcmp(buf, "999") == 0,
          "existing connection c2 stays usable until chdb_close_conn");

    chdb_close_conn(&c2);
    CHECK(c2 == NULL, "chdb_close_conn(&c2) nulls stack connection handle");
    chdb_close_conn(&c2); /* idempotent second close */

    chdb_close_conn(&c1b);
    chdb_close_conn(&c1a);
    chdb_close_database(&db1);
}

static void test_multipath_coexistence_and_status_lock(void)
{
    printf("== test_multipath_coexistence_and_status_lock ==\n");

    char root_tpl[] = "/tmp/chdb_p3_multipath_XXXXXX";
    char * root = mkdtemp(root_tpl);
    CHECK(root != NULL, "mkdtemp scratch root");
    if (!root)
        return;

    char dir_a[512];
    char dir_b[512];
    snprintf(dir_a, sizeof(dir_a), "%s/dir_a", root);
    snprintf(dir_b, sizeof(dir_b), "%s/dir_b", root);

    chdb_database db_a = NULL;
    chdb_database db_b = NULL;
    chdb_database db_mem = NULL;

    CHECK(chdb_open(dir_a, NULL, &db_a) == CHDBSuccess && db_a != NULL, "open persistent dir_a");
    CHECK(chdb_open(dir_b, NULL, &db_b) == CHDBSuccess && db_b != NULL, "open persistent dir_b alongside dir_a");
    CHECK(chdb_open(":memory:", NULL, &db_mem) == CHDBSuccess && db_mem != NULL, "open :memory: alongside dir_a and dir_b");

    chdb_connection ca = NULL;
    chdb_connection cb = NULL;
    chdb_connection cm = NULL;
    CHECK(chdb_connect_database(db_a, NULL, &ca) == CHDBSuccess, "connect ca");
    CHECK(chdb_connect_database(db_b, NULL, &cb) == CHDBSuccess, "connect cb");
    CHECK(chdb_connect_database(db_mem, NULL, &cm) == CHDBSuccess, "connect cm");

    char buf[256];
    CHECK(run_scalar(ca, "CREATE TABLE events (id UInt64, src String) ENGINE = MergeTree ORDER BY id", buf, sizeof(buf)) == 0
              && run_scalar(ca, "INSERT INTO events VALUES (10, 'from_A')", buf, sizeof(buf)) == 0,
          "write MergeTree table default.events in dir_a");
    CHECK(run_scalar(cb, "CREATE TABLE events (id UInt64, src String) ENGINE = MergeTree ORDER BY id", buf, sizeof(buf)) == 0
              && run_scalar(cb, "INSERT INTO events VALUES (20, 'from_B'), (21, 'from_B2')", buf, sizeof(buf)) == 0,
          "write MergeTree table default.events in dir_b");
    CHECK(run_scalar(cm, "CREATE TABLE events (id UInt64, src String) ENGINE = Memory", buf, sizeof(buf)) == 0
              && run_scalar(cm, "INSERT INTO events VALUES (30, 'from_MEM')", buf, sizeof(buf)) == 0,
          "write Memory table default.events in :memory:");

    CHECK(run_scalar(ca, "SELECT sum(id) FROM events", buf, sizeof(buf)) == 0 && strcmp(buf, "10") == 0,
          "dir_a reads sum(id) == 10");
    CHECK(run_scalar(cb, "SELECT sum(id) FROM events", buf, sizeof(buf)) == 0 && strcmp(buf, "41") == 0,
          "dir_b reads sum(id) == 41");
    CHECK(run_scalar(cm, "SELECT sum(id) FROM events", buf, sizeof(buf)) == 0 && strcmp(buf, "30") == 0,
          ":memory: reads sum(id) == 30");

    /* Cross-process lock check: while dir_a is open, another process cannot acquire dir_a/status */
    fflush(stdout);
    pid_t child = fork();
    if (child == 0)
    {
        chdb_database child_db = NULL;
        chdb_state s = chdb_open(dir_a, NULL, &child_db);
        if (s == CHDBSuccess && child_db != NULL)
        {
            chdb_close_database(&child_db);
            _exit(2); /* Unexpected success on locked path */
        }
        _exit(0);
    }
    int status = 0;
    waitpid(child, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "cross-process StatusFile lock refuses concurrent process on dir_a");

    /* Close dir_a while dir_b and db_mem remain open! */
    chdb_close_conn(&ca);
    chdb_close_database(&db_a);

    /* Now a child process (and parent reopen) CAN open dir_a and read persisted MergeTree data */
    fflush(stdout);
    child = fork();
    if (child == 0)
    {
        chdb_database child_db = NULL;
        if (chdb_open(dir_a, NULL, &child_db) != CHDBSuccess || !child_db)
            _exit(3);
        chdb_connection child_conn = NULL;
        if (chdb_connect_database(child_db, NULL, &child_conn) != CHDBSuccess || !child_conn)
            _exit(4);
        char cbuf[128];
        int rc = (run_scalar(child_conn, "SELECT sum(id) FROM events", cbuf, sizeof(cbuf)) == 0
                  && strcmp(cbuf, "10") == 0)
            ? 0
            : 5;
        chdb_close_conn(&child_conn);
        chdb_close_database(&child_db);
        _exit(rc);
    }
    status = 0;
    waitpid(child, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "closing dir_a releases dir_a/status while dir_b stays open; child reads persisted rows");

    /* dir_b and :memory: are completely unaffected */
    CHECK(run_scalar(cb, "SELECT sum(id) FROM events", buf, sizeof(buf)) == 0 && strcmp(buf, "41") == 0,
          "dir_b still active and intact after dir_a unmounted");
    CHECK(run_scalar(cm, "SELECT sum(id) FROM events", buf, sizeof(buf)) == 0 && strcmp(buf, "30") == 0,
          ":memory: still active and intact after dir_a unmounted");

    chdb_close_conn(&cm);
    chdb_close_database(&db_mem);
    chdb_close_conn(&cb);
    chdb_close_database(&db_b);

    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", root);
    (void)!system(cmd);
}

int main(void)
{
    test_abi_argument_validation();
    test_zero_disk_isolated_memory_databases();
    test_multipath_coexistence_and_status_lock();

    if (g_failures != 0)
    {
        fprintf(stderr, "\nchdbDatabaseAndMultiPathTest: %d failure(s)\n", g_failures);
        return 1;
    }
    printf("\nchdbDatabaseAndMultiPathTest: ALL PASSED\n");
    return 0;
}
