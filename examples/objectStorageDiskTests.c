/**
 * objectStorageDiskTests.c - disk operations of chdbObjectStorageTest beyond INSERT and SELECT:
 * the plain_rewritable gauges, a second disk under a key_prefix, the removal of an outdated part
 * (host-side copies), and DETACH / ATTACH of the merged part with one failing ATTACH in between.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "chdb.h"
#include "objectStorageMemStore.h"
#include "objectStorageTestUtil.h"

void metrics_test(chdb_connection conn)
{
    expect(conn, "callback disk has its own plain_rewritable gauges",
        "SELECT value > 0 FROM system.metrics WHERE metric = 'DiskPlainRewritableCallbackFileCount'", "1");
    expect(conn, "local plain_rewritable gauges stay at zero",
        "SELECT value FROM system.metrics WHERE metric = 'DiskPlainRewritableLocalFileCount'", "0");
}

/* key_prefix gives a second disk its own namespace in the same store. DROP removes the table's
 * blobs; only the directory markers of the store/ ancestors stay under the prefix. */
void prefixed_table_test(chdb_connection conn)
{
    char err[512] = "";
    free(run(conn, "CREATE TABLE db.t3 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS disk = disk(type = 'callback')", err, sizeof(err)));
    check(strstr(err, "storage_name") && strstr(err, "chdb_register_object_storage"), "disk() without storage_name names the remedy");
    check(run_ok(conn,
              "CREATE TABLE db.t2 (k UInt64) ENGINE = MergeTree ORDER BY k"
              " SETTINGS disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'pg_1')"),
        "CREATE TABLE with key_prefix");
    check(run_ok(conn, "INSERT INTO db.t2 SELECT number FROM numbers(10)"), "INSERT into the prefixed table");
    expect(conn, "prefixed table holds its rows", "SELECT count() FROM db.t2", "10");
    check(mem_store_count_keys("pg_1/", NULL) > 0, "key_prefix shapes the keys");
    check(run_ok(conn, "DROP TABLE db.t2 SYNC"), "DROP TABLE SYNC on the prefixed disk");
    check(mem_store_count_keys("pg_1/", "/prefix.path") == 0, "DROP leaves only directory markers under the prefix");
    verify(conn, "after dropping the prefixed table", "100000", "100");
}

/* Removing an outdated part unlinks its files one by one, and plain_rewritable copies each blob
 * aside first: with the optional copy callback the host duplicates them, otherwise the engine
 * streams them out through read and back through write_*. (FREEZE, the other hard-link user, is
 * refused on plain metadata.) The cleanup thread removes the part on its next pass. */
void part_removal_test(chdb_connection conn, int has_copy)
{
    check(run_ok(conn,
              "CREATE TABLE db.t4 (k UInt64) ENGINE = MergeTree ORDER BY k"
              " SETTINGS old_parts_lifetime = 0, disk = disk(type = 'callback', storage_name = 'mem_store', key_prefix = 'main')"),
        "CREATE TABLE for part removal");
    check(run_ok(conn, "INSERT INTO db.t4 SELECT number FROM numbers(100000)"), "INSERT into t4");
    struct mem_store_counts before, after;
    mem_store_snapshot(&before);
    check(run_ok(conn, "ALTER TABLE db.t4 DROP PART 'all_1_1_0'"), "DROP PART on callback disk");
    int gone = 0;
    for (int i = 0; i < 600 && !gone; ++i)
    {
        char * parts = run(conn, "SELECT count() FROM system.parts WHERE database = 'db' AND table = 't4'", NULL, 0);
        gone = parts && strcmp(parts, "0") == 0;
        free(parts);
        if (!gone)
            usleep(100 * 1000);
    }
    check(gone, "outdated part removed from the disk");
    mem_store_snapshot(&after);
    size_t read_delta = after.bytes_read - before.bytes_read;
    /* Directory markers are read while the part is renamed; the part's data is far larger. */
    int copied = after.copies > before.copies && read_delta < 16384;
    int streamed = after.copies == before.copies && read_delta > 65536;
    check(has_copy ? copied : streamed, has_copy ? "part removal copies blobs host-side" : "part removal streams blobs without a copy callback");
    if (!(has_copy ? copied : streamed))
        printf("  copies %zu -> %zu, bytes read +%zu\n", before.copies, after.copies, read_delta);
    check(run_ok(conn, "DROP TABLE db.t4 SYNC"), "DROP TABLE t4 SYNC");
}

/* DETACH the merged part, fail one ATTACH inside the host, then ATTACH it for real. */
void detach_attach_test(chdb_connection conn)
{
    char * part = run(conn,
        "SELECT name FROM system.parts WHERE database = 'db' AND table = 't' AND active AND NOT startsWith(name, 'patch-')", NULL, 0);
    int one_part = part && *part && !strchr(part, '\n');
    check(one_part, "one active data part to detach");
    if (one_part)
    {
        char sql[256];
        snprintf(sql, sizeof(sql), "ALTER TABLE t DETACH PART '%s'", part);
        check(run_ok(conn, sql), "DETACH PART on callback disk");
        expect(conn, "no rows while the part is detached", "SELECT count() FROM t", "0");
        attach_fault_test(conn, part);
        snprintf(sql, sizeof(sql), "ALTER TABLE t ATTACH PART '%s'", part);
        check(run_ok(conn, sql), "ATTACH PART on callback disk");
        verify(conn, "after ATTACH", "99000", "99");
    }
    free(part);
}
