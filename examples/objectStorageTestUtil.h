#pragma once

#include <stddef.h>
#include "chdb.h"

/* Shared by chdbObjectStorageTest.c (the definitions) and the objectStorage*Tests.c files. */

extern int g_failed;

/* Prints label with ok/FAIL and counts the failures. */
void check(int ok, const char * label);
/* Runs a query; returns the result text (caller frees) or NULL, copying the error into err when given. */
char * run(chdb_connection conn, const char * sql, char * err, size_t err_len);
int run_ok(chdb_connection conn, const char * sql);
/* Runs sql and checks that the result equals want. */
int expect(chdb_connection conn, const char * label, const char * sql, const char * want);

/* An 8-dimensional pseudo-random vector derived from k (hashes keep the points apart, so the
 * approximate index finds the exact match reliably), and the bulk INSERT both files use. */
#define VEC(k) "arrayMap(i -> toFloat32((cityHash64(toUInt64(" #k "), i) % 1000) / 1000), range(8))"
#define INSERT_100K_ROWS \
    "INSERT INTO t SELECT number, concat('word', toString(number % 1000), ' common'), " VEC(number) " FROM numbers(100000)"
/* Active data parts of t, excluding the patch parts lightweight DELETE leaves until they are folded in. */
#define ACTIVE_DATA_PARTS \
    "SELECT count() FROM system.parts WHERE database = 'db' AND table = 't' AND active AND NOT startsWith(name, 'patch-')"

/* The queries that must give the same answers before and after a restart. */
void verify(chdb_connection conn, const char * phase, const char * rows, const char * word7);

/* objectStorageDiskTests.c: disk operations beyond INSERT and SELECT. */
void metrics_test(chdb_connection conn);
void prefixed_table_test(chdb_connection conn);
void part_removal_test(chdb_connection conn, int has_copy);
void detach_attach_test(chdb_connection conn);

/* objectStorageFaultTests.c: injected host failures. */
void fault_tests(chdb_connection conn);
void attach_fault_test(chdb_connection conn, const char * part);
void list_fault_at_reconnect(int argc, char ** argv);

/* objectStorageLifecycleTests.c: registration, keyspaces, re-entrancy and fencing. */
void registration_api_tests(const chdb_object_storage_callbacks * cb);
void keyspace_tests(chdb_connection conn);
void reentrancy_test(chdb_connection conn);
void fence_test(chdb_connection conn);
void fence_reregister(void);
void fence_test_after_reconnect(chdb_connection conn);
