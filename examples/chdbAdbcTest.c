/**
 * Drives the ADBC driver the way non-Python driver managers do: dlopen +
 * chdb_adbc_init + the AdbcDriver function table — the C-ABI contract every
 * language binding sits on.
 *
 *   clang -O2 -I./programs/local examples/chdbAdbcTest.c -ldl -o examples/chdbAdbcTest
 *   ./examples/chdbAdbcTest ./libchdb.so
 */

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "adbc/adbc.h"

static int failures = 0;

#define CHECK(cond, msg)                                              \
    do {                                                              \
        if (!(cond)) {                                                \
            fprintf(stderr, "  FAIL: %s (%s:%d)\n", (msg), __FILE__, __LINE__); \
            failures += 1;                                            \
        }                                                             \
    } while (0)

static void print_adbc_error(const char * where, struct AdbcError * err)
{
    fprintf(stderr, "  %s: %s\n", where, err->message ? err->message : "(no message)");
    if (err->release)
        err->release(err);
    memset(err, 0, sizeof(*err));
}

int main(int argc, char ** argv)
{
    const char * lib_path = argc > 1 ? argv[1] : "./libchdb.so";

    void * handle = dlopen(lib_path, RTLD_LOCAL | RTLD_NOW);
    if (!handle) {
        fprintf(stderr, "dlopen(%s) failed: %s\n", lib_path, dlerror());
        return 2;
    }

    typedef AdbcStatusCode (*InitFunc)(int, void *, struct AdbcError *);
    InitFunc init = (InitFunc)dlsym(handle, "chdb_adbc_init");
    if (!init) {
        fprintf(stderr, "chdb_adbc_init not exported by %s\n", lib_path);
        return 2;
    }

    struct AdbcError error;
    memset(&error, 0, sizeof(error));
    struct AdbcDriver driver;
    memset(&driver, 0, sizeof(driver));

    CHECK(init(ADBC_VERSION_1_1_0, &driver, &error) == ADBC_STATUS_OK, "driver init (1.1.0)");

    /* Database + Connection */
    struct AdbcDatabase database;
    memset(&database, 0, sizeof(database));
    CHECK(driver.DatabaseNew(&database, &error) == ADBC_STATUS_OK, "DatabaseNew");
    CHECK(driver.DatabaseSetOption(&database, "path", ":memory:", &error) == ADBC_STATUS_OK, "SetOption(path)");
    CHECK(driver.DatabaseInit(&database, &error) == ADBC_STATUS_OK, "DatabaseInit");

    struct AdbcConnection connection;
    memset(&connection, 0, sizeof(connection));
    CHECK(driver.ConnectionNew(&connection, &error) == ADBC_STATUS_OK, "ConnectionNew");
    CHECK(driver.ConnectionInit(&connection, &database, &error) == ADBC_STATUS_OK, "ConnectionInit");

    /* Plain streamed query */
    {
        struct AdbcStatement stmt;
        memset(&stmt, 0, sizeof(stmt));
        CHECK(driver.StatementNew(&connection, &stmt, &error) == ADBC_STATUS_OK, "StatementNew");
        CHECK(driver.StatementSetSqlQuery(&stmt, "SELECT number FROM numbers(200000)", &error)
                  == ADBC_STATUS_OK,
              "SetSqlQuery");

        struct ArrowArrayStream stream;
        memset(&stream, 0, sizeof(stream));
        int64_t rows_affected = 0;
        AdbcStatusCode st = driver.StatementExecuteQuery(&stmt, &stream, &rows_affected, &error);
        if (st != ADBC_STATUS_OK)
            print_adbc_error("ExecuteQuery", &error);
        CHECK(st == ADBC_STATUS_OK, "ExecuteQuery");

        struct ArrowSchema schema;
        memset(&schema, 0, sizeof(schema));
        CHECK(stream.get_schema(&stream, &schema) == 0, "stream.get_schema");
        CHECK(schema.n_children == 1, "one result column");
        if (schema.release)
            schema.release(&schema);

        long long total = 0;
        int batches = 0;
        for (;;) {
            struct ArrowArray array;
            memset(&array, 0, sizeof(array));
            if (stream.get_next(&stream, &array) != 0) {
                CHECK(0, "stream.get_next");
                break;
            }
            if (!array.release)
                break; /* end of stream */
            total += array.length;
            batches += 1;
            array.release(&array);
        }
        stream.release(&stream);
        printf("streamed query: %lld rows in %d batches\n", total, batches);
        CHECK(total == 200000, "row count");
        CHECK(batches > 1, "arrived in multiple batches");

        CHECK(driver.StatementRelease(&stmt, &error) == ADBC_STATUS_OK, "StatementRelease");
    }

    /* GetTableTypes through the metadata path */
    {
        struct ArrowArrayStream stream;
        memset(&stream, 0, sizeof(stream));
        CHECK(driver.ConnectionGetTableTypes(&connection, &stream, &error) == ADBC_STATUS_OK,
              "GetTableTypes");
        struct ArrowArray array;
        memset(&array, 0, sizeof(array));
        CHECK(stream.get_next(&stream, &array) == 0 && array.release, "table types batch");
        CHECK(array.length == 2, "BASE TABLE + VIEW");
        if (array.release)
            array.release(&array);
        stream.release(&stream);
    }

    /* DDL through ExecuteQuery (no result set -> empty zero-column stream) */
    {
        struct AdbcStatement stmt;
        memset(&stmt, 0, sizeof(stmt));
        CHECK(driver.StatementNew(&connection, &stmt, &error) == ADBC_STATUS_OK, "StatementNew(ddl)");
        CHECK(driver.StatementSetSqlQuery(
                  &stmt, "CREATE TABLE cabi_t (x Int64) ENGINE = MergeTree() ORDER BY x", &error)
                  == ADBC_STATUS_OK,
              "SetSqlQuery(ddl)");
        struct ArrowArrayStream stream;
        memset(&stream, 0, sizeof(stream));
        AdbcStatusCode st = driver.StatementExecuteQuery(&stmt, &stream, NULL, &error);
        if (st != ADBC_STATUS_OK)
            print_adbc_error("ExecuteQuery(ddl)", &error);
        CHECK(st == ADBC_STATUS_OK, "ExecuteQuery(ddl)");
        struct ArrowSchema schema;
        memset(&schema, 0, sizeof(schema));
        CHECK(stream.get_schema(&stream, &schema) == 0 && schema.n_children == 0,
              "DDL yields zero-column schema");
        if (schema.release)
            schema.release(&schema);
        stream.release(&stream);
        CHECK(driver.StatementRelease(&stmt, &error) == ADBC_STATUS_OK, "StatementRelease(ddl)");
    }

    CHECK(driver.ConnectionRelease(&connection, &error) == ADBC_STATUS_OK, "ConnectionRelease");
    CHECK(driver.DatabaseRelease(&database, &error) == ADBC_STATUS_OK, "DatabaseRelease");
    CHECK(driver.release(&driver, &error) == ADBC_STATUS_OK, "driver release");

    if (failures) {
        printf("FAILED: %d assertion(s)\n", failures);
        return 1;
    }
    printf("all C-ABI checks passed\n");
    return 0;
}
