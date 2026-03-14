#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>

#include "../programs/local/chdb.h"
#include "arrow_c_abi.h"

/*===----------------------------------------------------------------------===*/
/* Test Utilities                                                              */
/*===----------------------------------------------------------------------===*/

static int tests_passed = 0;
static int tests_failed = 0;

static void test_assert(bool condition, const char * test_name, const char * message)
{
    if (condition)
    {
        printf("  PASS: %s\n", test_name);
        tests_passed++;
    }
    else
    {
        printf("  FAIL: %s", test_name);
        if (message && strlen(message) > 0)
            printf(" - %s", message);
        printf("\n");
        tests_failed++;
    }
}

/*===----------------------------------------------------------------------===*/
/* Helper: consume an ArrowArrayStream and count total rows                    */
/*===----------------------------------------------------------------------===*/

static int64_t consume_stream_row_count(struct ArrowArrayStream * stream)
{
    int64_t total = 0;
    while (1)
    {
        struct ArrowArray array;
        memset(&array, 0, sizeof(array));

        int rc = stream->get_next(stream, &array);
        if (rc != 0)
        {
            printf("    get_next returned error %d\n", rc);
            return -1;
        }
        if (array.release == NULL)
            break; /* end of stream */

        total += array.length;
        array.release(&array);
    }
    return total;
}

/*===----------------------------------------------------------------------===*/
/* Test 1: Basic query — schema + row count                                    */
/*===----------------------------------------------------------------------===*/

static void test_basic_query(chdb_connection conn)
{
    struct ArrowArrayStream stream;
    struct ArrowSchema schema;
    chdb_state state;
    int64_t row_count;
    int rc;
    char msg[256];

    printf("\n--- Test: Basic query ---\n");

    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(
        conn, "SELECT number, toString(number) AS str FROM numbers(100)",
        (chdb_arrow_stream)&stream);
    test_assert(state == CHDBSuccess, "chdb_query_arrow_stream returns CHDBSuccess", "");
    test_assert(stream.release != NULL, "Stream release callback is set", "");

    /* Verify schema */
    memset(&schema, 0, sizeof(schema));
    rc = stream.get_schema(&stream, &schema);
    test_assert(rc == 0, "get_schema returns 0", "");
    test_assert(schema.n_children == 2, "Schema has 2 columns",
                schema.n_children != 2 ? "wrong column count" : "");

    if (schema.n_children == 2)
    {
        test_assert(strcmp(schema.children[0]->name, "number") == 0,
                    "First column is 'number'", schema.children[0]->name);
        test_assert(strcmp(schema.children[1]->name, "str") == 0,
                    "Second column is 'str'", schema.children[1]->name);
    }

    if (schema.release)
        schema.release(&schema);

    /* Count rows */
    row_count = consume_stream_row_count(&stream);
    snprintf(msg, sizeof(msg), "expected 100, got %lld", (long long)row_count);
    test_assert(row_count == 100, "Row count is 100", msg);

    stream.release(&stream);
}

/*===----------------------------------------------------------------------===*/
/* Test 2: Empty result                                                        */
/*===----------------------------------------------------------------------===*/

static void test_empty_result(chdb_connection conn)
{
    struct ArrowArrayStream stream;
    chdb_state state;
    int64_t row_count;
    char msg[256];

    printf("\n--- Test: Empty result ---\n");

    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(
        conn, "SELECT number FROM numbers(0)",
        (chdb_arrow_stream)&stream);
    test_assert(state == CHDBSuccess, "Empty query returns CHDBSuccess", "");
    test_assert(stream.release != NULL, "Stream is valid", "");

    row_count = consume_stream_row_count(&stream);
    snprintf(msg, sizeof(msg), "expected 0, got %lld", (long long)row_count);
    test_assert(row_count == 0, "Row count is 0", msg);

    stream.release(&stream);
}

/*===----------------------------------------------------------------------===*/
/* Test 3: Large result                                                        */
/*===----------------------------------------------------------------------===*/

static void test_large_result(chdb_connection conn)
{
    struct ArrowArrayStream stream;
    chdb_state state;
    int64_t row_count;
    char msg[256];

    printf("\n--- Test: Large result (1M rows) ---\n");

    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(
        conn, "SELECT number FROM numbers(1000000)",
        (chdb_arrow_stream)&stream);
    test_assert(state == CHDBSuccess, "Large query returns CHDBSuccess", "");

    row_count = consume_stream_row_count(&stream);
    snprintf(msg, sizeof(msg), "expected 1000000, got %lld", (long long)row_count);
    test_assert(row_count == 1000000, "Row count is 1,000,000", msg);

    stream.release(&stream);
}

/*===----------------------------------------------------------------------===*/
/* Test 4: Type coverage                                                       */
/*===----------------------------------------------------------------------===*/

static void test_type_coverage(chdb_connection conn)
{
    struct ArrowArrayStream stream;
    struct ArrowSchema schema;
    chdb_state state;
    int rc;

    printf("\n--- Test: Type coverage ---\n");

    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(
        conn,
        "SELECT "
        "  toInt64(1)        AS col_int64, "
        "  toFloat64(3.14)   AS col_float64, "
        "  'hello'           AS col_string, "
        "  toDate('2024-01-15')     AS col_date, "
        "  toDateTime('2024-01-15 12:30:00') AS col_datetime",
        (chdb_arrow_stream)&stream);
    test_assert(state == CHDBSuccess, "Type coverage query returns CHDBSuccess", "");

    memset(&schema, 0, sizeof(schema));
    rc = stream.get_schema(&stream, &schema);
    test_assert(rc == 0, "get_schema returns 0", "");
    test_assert(schema.n_children == 5, "Schema has 5 columns", "");

    if (schema.n_children == 5)
    {
        /* Verify column names */
        test_assert(strcmp(schema.children[0]->name, "col_int64") == 0,
                    "Column 0 name is 'col_int64'", schema.children[0]->name);
        test_assert(strcmp(schema.children[1]->name, "col_float64") == 0,
                    "Column 1 name is 'col_float64'", schema.children[1]->name);
        test_assert(strcmp(schema.children[2]->name, "col_string") == 0,
                    "Column 2 name is 'col_string'", schema.children[2]->name);
        test_assert(strcmp(schema.children[3]->name, "col_date") == 0,
                    "Column 3 name is 'col_date'", schema.children[3]->name);
        test_assert(strcmp(schema.children[4]->name, "col_datetime") == 0,
                    "Column 4 name is 'col_datetime'", schema.children[4]->name);

        /* Verify Arrow format strings */
        /* Int64 → "l", Float64 → "g", String → "u" or "U",
           Date32 → "tdD", DateTime(UInt32) → "tsu:" or "tdm" depending on ClickHouse version */
        test_assert(strcmp(schema.children[0]->format, "l") == 0,
                    "col_int64 format is 'l' (int64)", schema.children[0]->format);
        test_assert(strcmp(schema.children[1]->format, "g") == 0,
                    "col_float64 format is 'g' (float64)", schema.children[1]->format);
        /* String could be "u" (utf8) or "U" (large_utf8) */
        test_assert(schema.children[2]->format[0] == 'u' || schema.children[2]->format[0] == 'U',
                    "col_string format starts with 'u' or 'U'", schema.children[2]->format);
    }

    if (schema.release)
        schema.release(&schema);

    /* Consume to clean up */
    consume_stream_row_count(&stream);
    stream.release(&stream);
}

/*===----------------------------------------------------------------------===*/
/* Test 5: Round-trip (Arrow output -> chdb_arrow_scan -> query)                */
/*===----------------------------------------------------------------------===*/

static void test_round_trip(chdb_connection conn)
{
    struct ArrowArrayStream stream;
    chdb_state state;
    chdb_result * count_result;
    const char * error;
    char * buffer;
    char * end;
    uint64_t actual_rows;
    char msg[256];

    printf("\n--- Test: Round-trip (output -> scan -> query) ---\n");

    /* Step 1: Get Arrow output */
    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(
        conn, "SELECT number AS id, toString(number) AS value FROM numbers(500)",
        (chdb_arrow_stream)&stream);
    test_assert(state == CHDBSuccess, "Arrow output query succeeds", "");

    /* Step 2: Register the stream as a table */
    state = chdb_arrow_scan(conn, "roundtrip_table", (chdb_arrow_stream)&stream);
    test_assert(state == CHDBSuccess, "chdb_arrow_scan registration succeeds", "");

    /* Step 3: Query the registered table */
    count_result = chdb_query(conn, "SELECT COUNT(*) FROM arrowstream(roundtrip_table)", "CSV");
    test_assert(count_result != NULL, "Count query result is not null", "");

    error = chdb_result_error(count_result);
    test_assert(error == NULL, "Count query has no error", error ? error : "");

    if (!error)
    {
        buffer = chdb_result_buffer(count_result);
        test_assert(buffer != NULL, "Result buffer is not null", "");

        if (buffer)
        {
            /* Parse the count — trim trailing whitespace */
            char result_str[64];
            strncpy(result_str, buffer, sizeof(result_str) - 1);
            result_str[sizeof(result_str) - 1] = '\0';
            end = result_str + strlen(result_str) - 1;
            while (end > result_str && (*end == ' ' || *end == '\n' || *end == '\r'))
            {
                *end = '\0';
                end--;
            }
            actual_rows = strtoull(result_str, NULL, 10);
            snprintf(msg, sizeof(msg), "expected 500, got %llu", (unsigned long long)actual_rows);
            test_assert(actual_rows == 500, "Round-trip row count is 500", msg);
        }
    }

    chdb_destroy_query_result(count_result);

    /* Cleanup */
    chdb_arrow_unregister_table(conn, "roundtrip_table");

    /* The stream may already have been consumed by the scan; release only if still valid */
    if (stream.release)
        stream.release(&stream);
}

/*===----------------------------------------------------------------------===*/
/* Test 6: Error handling                                                      */
/*===----------------------------------------------------------------------===*/

static void test_error_handling(chdb_connection conn)
{
    struct ArrowArrayStream stream;
    chdb_state state;

    printf("\n--- Test: Error handling ---\n");

    /* Null connection */
    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(NULL, "SELECT 1", (chdb_arrow_stream)&stream);
    test_assert(state == CHDBError, "Null connection returns CHDBError", "");

    /* Null stream */
    state = chdb_query_arrow_stream(conn, "SELECT 1", NULL);
    test_assert(state == CHDBError, "Null stream returns CHDBError", "");

    /* Invalid SQL */
    memset(&stream, 0, sizeof(stream));
    state = chdb_query_arrow_stream(
        conn, "THIS IS NOT VALID SQL", (chdb_arrow_stream)&stream);
    test_assert(state == CHDBError, "Invalid SQL returns CHDBError", "");
}

/*===----------------------------------------------------------------------===*/
/* Main                                                                        */
/*===----------------------------------------------------------------------===*/

int main(void)
{
    char * argv[] = {"clickhouse", "--multiquery"};
    int argc = sizeof(argv) / sizeof(argv[0]);
    chdb_connection * conn_ptr;
    chdb_connection conn;

    printf("=== chDB Arrow Output Test ===\n");

    conn_ptr = chdb_connect(argc, argv);
    if (!conn_ptr || !*conn_ptr)
    {
        printf("Failed to create chDB connection\n");
        return 1;
    }
    conn = *conn_ptr;
    printf("Connection established.\n");

    test_basic_query(conn);
    test_empty_result(conn);
    test_large_result(conn);
    test_type_coverage(conn);
    test_round_trip(conn);
    test_error_handling(conn);

    chdb_close_conn(conn_ptr);

    printf("\n=== Results: %d passed, %d failed ===\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
