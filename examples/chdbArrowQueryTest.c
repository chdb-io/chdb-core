/**
 * chdbArrowQueryTest.c
 *
 * End-to-end smoke test for the Arrow C Data Interface output API
 * (chdb_query_arrow / chdb_stream_query_arrow / chdb_stream_fetch_arrow).
 *
 * Unlike chdbArrowStreamParse.c — which goes through ArrowStream IPC
 * serialization and decodes with nanoarrow — this test gets the Arrow
 * C Data Interface stream directly, skipping the IPC layer entirely.
 *
 * Build (Linux, against an already-built libchdb.so and nanoarrow):
 *   clang -O2 -I./examples -I./examples/nanoarrow-0.8.0 \
 *         examples/chdbArrowQueryTest.c \
 *         examples/nanoarrow-0.8.0/nanoarrow.c \
 *         -L. -lchdb -o examples/chdbArrowQueryTest
 *   LD_LIBRARY_PATH=. ./examples/chdbArrowQueryTest
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nanoarrow/nanoarrow.h"
#include "chdb.h"

static int g_failed_assertions = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "  ASSERT FAIL: %s (%s:%d)\n",                 \
                    (msg), __FILE__, __LINE__);                            \
            g_failed_assertions += 1;                                      \
        }                                                                  \
    } while (0)

static void print_schema(const struct ArrowSchema * schema)
{
    printf("  Schema (%lld columns):\n", (long long)schema->n_children);
    for (int64_t i = 0; i < schema->n_children; i++) {
        const struct ArrowSchema * child = schema->children[i];
        printf("    [%lld] %-20s format=%s\n",
               (long long)i,
               child->name ? child->name : "(null)",
               child->format ? child->format : "?");
    }
}

static int read_one_batch_into_view(
    struct ArrowArrayStream * stream,
    struct ArrowSchema * schema_out,
    struct ArrowArray * array_out)
{
    int rc = stream->get_schema(stream, schema_out);
    if (rc != 0) {
        fprintf(stderr, "  get_schema failed: rc=%d msg=%s\n", rc,
                stream->get_last_error ? stream->get_last_error(stream) : "?");
        return rc;
    }
    rc = stream->get_next(stream, array_out);
    if (rc != 0) {
        fprintf(stderr, "  get_next failed: rc=%d msg=%s\n", rc,
                stream->get_last_error ? stream->get_last_error(stream) : "?");
        return rc;
    }
    return 0;
}

/* Test 1: chdb_query_arrow (materialized) — basic SELECT, default options. */
static void test_basic_select(chdb_connection conn)
{
    printf("== test_basic_select ==\n");
    struct ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));

    chdb_result * result = chdb_query_arrow(
        conn,
        "SELECT number AS n, toString(number) AS s FROM numbers(5)",
        (chdb_arrow_stream)&stream,
        NULL);

    const char * err = chdb_result_error(result);
    if (err) {
        fprintf(stderr, "  ERROR: %s\n", err);
        CHECK(0, "chdb_query_arrow returned error");
        chdb_destroy_query_result(result);
        return;
    }

    CHECK(stream.release != NULL, "out_stream release must be set");

    struct ArrowSchema schema;
    struct ArrowArray array;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));

    int rc = read_one_batch_into_view(&stream, &schema, &array);
    CHECK(rc == 0, "read_one_batch_into_view");

    print_schema(&schema);

    CHECK(schema.n_children == 2, "two columns");
    CHECK(array.length == 5, "five rows");

    /* Inspect first column values via nanoarrow view. */
    struct ArrowArrayView view;
    ArrowArrayViewInitFromSchema(&view, schema.children[0], NULL);
    ArrowArrayViewSetArray(&view, array.children[0], NULL);
    for (int64_t i = 0; i < array.length; i++) {
        int64_t v = ArrowArrayViewGetIntUnsafe(&view, i);
        if (v != i) {
            fprintf(stderr, "  row %lld expected %lld got %lld\n",
                    (long long)i, (long long)i, (long long)v);
            CHECK(0, "value mismatch");
            break;
        }
    }
    ArrowArrayViewReset(&view);

    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    if (stream.release) stream.release(&stream);

    printf("  -> elapsed=%.4fs rows_read=%llu\n",
           chdb_result_elapsed(result),
           (unsigned long long)chdb_result_rows_read(result));

    chdb_destroy_query_result(result);
}

/* Test 2: chdb_query_arrow with empty result (LIMIT 0). */
static void test_empty_result(chdb_connection conn)
{
    printf("== test_empty_result ==\n");
    struct ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));

    chdb_result * result = chdb_query_arrow(
        conn,
        "SELECT number FROM numbers(10) LIMIT 0",
        (chdb_arrow_stream)&stream,
        NULL);

    const char * err = chdb_result_error(result);
    if (err) {
        fprintf(stderr, "  ERROR: %s\n", err);
        CHECK(0, "chdb_query_arrow returned error on empty result");
        chdb_destroy_query_result(result);
        return;
    }
    CHECK(stream.release != NULL, "stream filled for empty result");

    struct ArrowSchema schema;
    memset(&schema, 0, sizeof(schema));
    int rc = stream.get_schema(&stream, &schema);
    CHECK(rc == 0, "schema available even for empty result");
    print_schema(&schema);
    if (schema.release) schema.release(&schema);

    /* get_next on an empty result should return either zero-length batch or released. */
    struct ArrowArray array;
    memset(&array, 0, sizeof(array));
    rc = stream.get_next(&stream, &array);
    CHECK(rc == 0, "get_next on empty result succeeds");
    /* Could be a 0-row batch or a released-callback signal. Both are valid. */
    if (array.release) array.release(&array);

    if (stream.release) stream.release(&stream);
    chdb_destroy_query_result(result);
}

/* Test 3: chdb_stream_query_arrow + repeated chdb_stream_fetch_arrow. */
static void test_streaming(chdb_connection conn)
{
    printf("== test_streaming ==\n");

    chdb_result * stream_result = chdb_stream_query_arrow(
        conn,
        "SELECT number AS n FROM numbers(7)",
        NULL);

    const char * err = chdb_result_error(stream_result);
    if (err) {
        fprintf(stderr, "  ERROR: %s\n", err);
        CHECK(0, "chdb_stream_query_arrow returned error");
        chdb_destroy_query_result(stream_result);
        return;
    }

    int64_t total_rows = 0;
    int fetched_batches = 0;
    for (int i = 0; i < 100; i++) {
        struct ArrowArrayStream batch;
        memset(&batch, 0, sizeof(batch));

        chdb_state st = chdb_stream_fetch_arrow(conn, stream_result,
                                                (chdb_arrow_stream)&batch);
        if (st != CHDBSuccess) {
            fprintf(stderr, "  fetch returned non-success at iter %d\n", i);
            break;
        }

        struct ArrowSchema schema;
        struct ArrowArray array;
        memset(&schema, 0, sizeof(schema));
        memset(&array, 0, sizeof(array));

        int rc = batch.get_schema(&batch, &schema);
        if (rc != 0) {
            /* End of stream — no schema cached or stream empty. */
            if (batch.release) batch.release(&batch);
            break;
        }

        rc = batch.get_next(&batch, &array);
        if (rc != 0 || array.release == NULL) {
            /* End of stream marker. */
            if (schema.release) schema.release(&schema);
            if (batch.release) batch.release(&batch);
            break;
        }

        fetched_batches += 1;
        total_rows += array.length;

        if (array.release) array.release(&array);
        if (schema.release) schema.release(&schema);
        if (batch.release) batch.release(&batch);
    }

    printf("  fetched %d batches, total %lld rows\n", fetched_batches, (long long)total_rows);
    CHECK(total_rows == 7, "streaming got expected total rows");
    CHECK(fetched_batches >= 1, "streaming yielded at least one batch");

    chdb_destroy_query_result(stream_result);
}

/* Test 4: Custom options — string_as_string=0 (binary instead of utf8). */
static void test_string_as_binary(chdb_connection conn)
{
    printf("== test_string_as_binary ==\n");
    struct ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));

    chdb_arrow_options opts;
    opts.unsupported_as_binary = 0;
    opts.low_cardinality_as_dictionary = 0;
    opts.string_as_string = 0; /* binary, not utf8 */

    chdb_result * result = chdb_query_arrow(
        conn,
        "SELECT 'hello' AS s",
        (chdb_arrow_stream)&stream,
        &opts);

    const char * err = chdb_result_error(result);
    if (err) {
        fprintf(stderr, "  ERROR: %s\n", err);
        CHECK(0, "chdb_query_arrow returned error");
        chdb_destroy_query_result(result);
        return;
    }

    struct ArrowSchema schema;
    memset(&schema, 0, sizeof(schema));
    int rc = stream.get_schema(&stream, &schema);
    CHECK(rc == 0, "got schema");

    if (schema.n_children >= 1) {
        const char * fmt = schema.children[0]->format;
        printf("  column format: %s\n", fmt ? fmt : "(null)");
        /* "z" = binary; "u" = utf8. We expect binary with this option. */
        CHECK(fmt && fmt[0] == 'z', "string_as_string=0 yields arrow binary");
    }

    if (schema.release) schema.release(&schema);
    if (stream.release) stream.release(&stream);
    chdb_destroy_query_result(result);
}

/* Test 5: DateTime maps to Arrow uint32 (ClickHouse ArrowStream parity). */
static void test_datetime_is_uint32(chdb_connection conn)
{
    printf("== test_datetime_is_uint32 ==\n");
    struct ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));

    chdb_result * result = chdb_query_arrow(
        conn,
        "SELECT toDateTime('2024-01-02 03:04:05', 'UTC') AS dt",
        (chdb_arrow_stream)&stream,
        NULL);

    const char * err = chdb_result_error(result);
    if (err) {
        fprintf(stderr, "  ERROR: %s\n", err);
        CHECK(0, "datetime query failed");
        chdb_destroy_query_result(result);
        return;
    }

    struct ArrowSchema schema;
    memset(&schema, 0, sizeof(schema));
    int rc = stream.get_schema(&stream, &schema);
    CHECK(rc == 0, "got schema");

    if (schema.n_children >= 1) {
        const char * fmt = schema.children[0]->format;
        printf("  column format: %s\n", fmt ? fmt : "(null)");
        /* "I" = uint32. ClickHouse maps DateTime -> arrow::uint32 (seconds). */
        CHECK(fmt && fmt[0] == 'I' && fmt[1] == '\0',
              "DateTime maps to arrow uint32 (matches ArrowStream)");
    }

    if (schema.release) schema.release(&schema);
    if (stream.release) stream.release(&stream);
    chdb_destroy_query_result(result);
}

int main(int argc, char ** argv)
{
    (void)argc; (void)argv;

    /* Standard chdb connection on :memory:. */
    char arg0[] = "clickhouse";
    char arg1[] = "--multiquery";
    char * args[] = {arg0, arg1};
    chdb_connection * conn = chdb_connect(2, args);
    if (!conn) {
        fprintf(stderr, "chdb_connect failed\n");
        return 1;
    }

    test_basic_select(*conn);
    test_empty_result(*conn);
    test_streaming(*conn);
    test_string_as_binary(*conn);
    test_datetime_is_uint32(*conn);

    chdb_close_conn(conn);

    printf("\n== summary: %d failed assertions ==\n", g_failed_assertions);
    return g_failed_assertions == 0 ? 0 : 1;
}
