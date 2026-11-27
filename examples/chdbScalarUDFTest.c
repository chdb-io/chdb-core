/**
 * chdbScalarUDFTest.c
 *
 * End-to-end test for scalar user-defined functions registered through the C API
 * (chdb_create_scalar_function / chdb_drop_scalar_function). The callbacks read
 * their Arrow arguments and build their Arrow results with nanoarrow.
 *
 * Build and run: examples/runScalarUDFTest.sh
 */

#include <stdint.h>
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

/* What a callback saw, shared by every call of one registered function. */
typedef struct
{
    int64_t calls;
    int64_t rows;
    int64_t max_batch_rows;
    int64_t nulls_seen;
    int64_t destroyed;
    int64_t value; /* what constant_flag returns */
} udf_stats;

static void record_call(udf_stats * stats, int64_t rows)
{
    __atomic_fetch_add(&stats->calls, 1, __ATOMIC_SEQ_CST);
    __atomic_fetch_add(&stats->rows, rows, __ATOMIC_SEQ_CST);
    int64_t seen = __atomic_load_n(&stats->max_batch_rows, __ATOMIC_SEQ_CST);
    while (rows > seen
           && !__atomic_compare_exchange_n(&stats->max_batch_rows, &seen, rows, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
    {
    }
}

static void destroy_stats(void * user_data)
{
    __atomic_fetch_add(&((udf_stats *)user_data)->destroyed, 1, __ATOMIC_SEQ_CST);
}

/* Opens the argument struct array for reading. Fails the call on error. */
static int open_arguments(
    chdb_arrow_schema schema, chdb_arrow_array array, struct ArrowArrayView * view, char * error, size_t error_size)
{
    struct ArrowError arrow_error;
    if (ArrowArrayViewInitFromSchema(view, (struct ArrowSchema *)schema, &arrow_error) != NANOARROW_OK
        || ArrowArrayViewSetArray(view, (struct ArrowArray *)array, &arrow_error) != NANOARROW_OK)
    {
        snprintf(error, error_size, "cannot read the arguments: %s", arrow_error.message);
        return 1;
    }
    return 0;
}

static int finish_result(
    struct ArrowArray * result, chdb_arrow_schema result_schema, enum ArrowType type, char * error, size_t error_size)
{
    struct ArrowError arrow_error;
    if (ArrowArrayFinishBuildingDefault(result, &arrow_error) != NANOARROW_OK)
    {
        snprintf(error, error_size, "cannot build the result: %s", arrow_error.message);
        return 1;
    }
    if (ArrowSchemaInitFromType((struct ArrowSchema *)result_schema, type) != NANOARROW_OK)
    {
        snprintf(error, error_size, "cannot describe the result");
        return 1;
    }
    return 0;
}

/* plus_one(Int64) -> Int64; counts the NULLs it is handed. */
static int plus_one(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    udf_stats * stats = (udf_stats *)user_data;
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;

    const int64_t rows = view.length;
    record_call(stats, rows);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_INT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < rows; i++)
    {
        if (ArrowArrayViewIsNull(view.children[0], i))
        {
            __atomic_fetch_add(&stats->nulls_seen, 1, __ATOMIC_SEQ_CST);
            ArrowArrayAppendNull(result, 1);
        }
        else
            ArrowArrayAppendInt(result, ArrowArrayViewGetIntUnsafe(view.children[0], i) + 1);
    }
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_INT64, error, error_size);
}

/* is_null(Int64) -> UInt8, registered with SPECIAL_NULL_HANDLING. */
static int is_null(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_UINT8);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendUInt(result, ArrowArrayViewIsNull(view.children[0], i) ? 1 : 0);
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_UINT8, error, error_size);
}

/* concat_dash(String, String) -> String, as Arrow UTF-8. */
static int concat_dash(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_STRING);
    ArrowArrayStartAppending(result);
    char buffer[256];
    for (int64_t i = 0; i < view.length; i++)
    {
        struct ArrowStringView left = ArrowArrayViewGetStringUnsafe(view.children[0], i);
        struct ArrowStringView right = ArrowArrayViewGetStringUnsafe(view.children[1], i);
        int n = snprintf(buffer, sizeof(buffer), "%.*s-%.*s",
                         (int)left.size_bytes, left.data, (int)right.size_bytes, right.data);
        if (n < 0)
            n = 0;
        if (n >= (int)sizeof(buffer)) /* truncated */
            n = (int)sizeof(buffer) - 1;
        struct ArrowStringView value = {buffer, n};
        ArrowArrayAppendString(result, value);
    }
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_STRING, error, error_size);
}

/* byte_length(String) -> UInt64, registered with BINARY_STRINGS: checks the argument is binary. */
static int byte_length(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    const char * format = ((struct ArrowSchema *)schema)->children[0]->format;
    if (strcmp(format, "z") != 0)
    {
        snprintf(error, error_size, "expected Arrow binary, got format %s", format);
        return 1;
    }
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_UINT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendUInt(result, (uint64_t)ArrowArrayViewGetBytesUnsafe(view.children[0], i).size_bytes);
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_UINT64, error, error_size);
}

/* batch_rows() -> UInt64: every row gets the size of the batch it was evaluated in. */
static int batch_rows(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArray * arguments = (struct ArrowArray *)array;
    if (((struct ArrowSchema *)schema)->n_children != 0)
    {
        snprintf(error, error_size, "expected no argument columns");
        return 1;
    }
    record_call((udf_stats *)user_data, arguments->length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_UINT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < arguments->length; i++)
        ArrowArrayAppendUInt(result, (uint64_t)arguments->length);
    return finish_result(result, result_schema, NANOARROW_TYPE_UINT64, error, error_size);
}

/* fail_on_zero(Int64) -> Int64: fails a batch holding a 0, otherwise returns its argument. */
static int fail_on_zero(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    for (int64_t i = 0; i < view.length; i++)
    {
        if (ArrowArrayViewGetIntUnsafe(view.children[0], i) == 0)
        {
            ArrowArrayViewReset(&view);
            snprintf(error, error_size, "boom: zero at row %lld", (long long)i);
            return 1;
        }
    }

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_INT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendInt(result, ArrowArrayViewGetIntUnsafe(view.children[0], i));
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_INT64, error, error_size);
}

/* one_row_too_many(Int64) -> Int64: returns a result one row longer than its input. */
static int one_row_too_many(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    (void)user_data;
    (void)schema;
    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_INT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i <= ((struct ArrowArray *)array)->length; i++)
        ArrowArrayAppendInt(result, i);
    return finish_result(result, result_schema, NANOARROW_TYPE_INT64, error, error_size);
}

/* times_hundred(Int64) -> UInt8: computes in Int64, so the engine has to narrow the result. */
static int times_hundred(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_INT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendInt(result, ArrowArrayViewGetIntUnsafe(view.children[0], i) * 100);
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_INT64, error, error_size);
}

/* constant_flag(Int64) -> UInt8: every row gets stats->value. */
static int constant_flag(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    (void)schema;
    udf_stats * stats = (udf_stats *)user_data;
    const int64_t rows = ((struct ArrowArray *)array)->length;
    record_call(stats, rows);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_UINT8);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < rows; i++)
        ArrowArrayAppendUInt(result, (uint64_t)stats->value);
    return finish_result(result, result_schema, NANOARROW_TYPE_UINT8, error, error_size);
}

/* length_alternating(String) -> Int64: the byte length of its argument, as Arrow int32 on every
 * other call. */
static int length_alternating(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    udf_stats * stats = (udf_stats *)user_data;
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call(stats, view.length);

    const enum ArrowType type = __atomic_load_n(&stats->calls, __ATOMIC_SEQ_CST) % 2 ? NANOARROW_TYPE_INT32 : NANOARROW_TYPE_INT64;
    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, type);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendInt(result, ArrowArrayViewGetStringUnsafe(view.children[0], i).size_bytes);
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, type, error, error_size);
}

/* nested_bytes(Array(String)) -> UInt64, registered with BINARY_STRINGS: the bytes in each array. */
static int nested_bytes(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    const char * format = ((struct ArrowSchema *)schema)->children[0]->children[0]->format;
    if (strcmp(format, "z") != 0)
    {
        snprintf(error, error_size, "expected Arrow binary elements, got format %s", format);
        return 1;
    }
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    const struct ArrowArrayView * list = view.children[0];
    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_UINT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
    {
        uint64_t bytes = 0;
        for (int64_t j = ArrowArrayViewListChildOffset(list, i); j < ArrowArrayViewListChildOffset(list, i + 1); j++)
            bytes += (uint64_t)ArrowArrayViewGetBytesUnsafe(list->children[0], j).size_bytes;
        ArrowArrayAppendUInt(result, bytes);
    }
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_UINT64, error, error_size);
}

/* list_length(Array(Int64)) -> UInt64. */
static int list_length(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_UINT64);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendUInt(result, (uint64_t)(ArrowArrayViewListChildOffset(view.children[0], i + 1)
                                               - ArrowArrayViewListChildOffset(view.children[0], i)));
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_UINT64, error, error_size);
}

/* third(Float32) -> BFloat16: computes in float32, which the engine narrows. */
static int third(
    void * user_data, chdb_arrow_schema schema, chdb_arrow_array array,
    chdb_arrow_schema result_schema, chdb_arrow_array result_array, char * error, size_t error_size)
{
    struct ArrowArrayView view;
    if (open_arguments(schema, array, &view, error, error_size))
        return 1;
    record_call((udf_stats *)user_data, view.length);

    struct ArrowArray * result = (struct ArrowArray *)result_array;
    ArrowArrayInitFromType(result, NANOARROW_TYPE_FLOAT);
    ArrowArrayStartAppending(result);
    for (int64_t i = 0; i < view.length; i++)
        ArrowArrayAppendDouble(result, ArrowArrayViewGetDoubleUnsafe(view.children[0], i) / 3);
    ArrowArrayViewReset(&view);
    return finish_result(result, result_schema, NANOARROW_TYPE_FLOAT, error, error_size);
}

/* strdup is POSIX, not C99. */
static char * copy_string(const char * s)
{
    size_t length = strlen(s);
    char * out = malloc(length + 1);
    memcpy(out, s, length + 1);
    return out;
}

/* Runs a query and returns its CSV output (caller frees), or NULL with the error in *error_out. */
static char * query_csv(chdb_connection conn, const char * sql, char ** error_out)
{
    chdb_result * result = chdb_query(conn, sql, "CSV");
    char * out = NULL;
    if (error_out)
        *error_out = NULL;
    const char * error = chdb_result_error(result);
    if (error)
    {
        if (error_out)
            *error_out = copy_string(error);
    }
    else
    {
        size_t length = chdb_result_length(result);
        out = malloc(length + 1);
        memcpy(out, chdb_result_buffer(result), length);
        out[length] = '\0';
    }
    chdb_destroy_query_result(result);
    return out;
}

static void expect_output(chdb_connection conn, const char * sql, const char * expected)
{
    char * error = NULL;
    char * out = query_csv(conn, sql, &error);
    if (!out)
    {
        fprintf(stderr, "  query failed: %s\n    %s\n", sql, error ? error : "?");
        CHECK(0, "query succeeds");
    }
    else if (strcmp(out, expected) != 0)
    {
        fprintf(stderr, "  query: %s\n    expected: %s    got: %s\n", sql, expected, out);
        CHECK(0, "query output matches");
    }
    free(out);
    free(error);
}

static void expect_error(chdb_connection conn, const char * sql, const char * fragment)
{
    char * error = NULL;
    char * out = query_csv(conn, sql, &error);
    if (out)
    {
        fprintf(stderr, "  query unexpectedly succeeded: %s\n    got: %s\n", sql, out);
        CHECK(0, "query fails");
    }
    else if (!error || !strstr(error, fragment))
    {
        fprintf(stderr, "  query: %s\n    expected an error containing: %s\n    got: %s\n", sql, fragment, error ? error : "(none)");
        CHECK(0, "error message matches");
    }
    free(out);
    free(error);
}

/* Registers a function and returns the error message (caller frees), or NULL on success. */
static char * create_function(
    chdb_connection conn, const char * name, const char * const * argument_types, size_t argument_count,
    const char * return_type, uint32_t flags, chdb_scalar_function_callback callback, udf_stats * stats)
{
    chdb_scalar_function_options options;
    memset(&options, 0, sizeof(options));
    options.struct_size = sizeof(options);
    options.argument_types = argument_types;
    options.argument_count = argument_count;
    options.return_type = return_type;
    options.flags = flags;
    options.callback = callback;
    options.user_data = stats;
    options.destroy = destroy_stats;

    chdb_result * result = chdb_create_scalar_function(conn, name, &options);
    const char * error = chdb_result_error(result);
    char * message = error ? copy_string(error) : NULL;
    chdb_destroy_query_result(result);
    return message;
}

static void must_create(
    chdb_connection conn, const char * name, const char * const * argument_types, size_t argument_count,
    const char * return_type, uint32_t flags, chdb_scalar_function_callback callback, udf_stats * stats)
{
    char * error = create_function(conn, name, argument_types, argument_count, return_type, flags, callback, stats);
    if (error)
        fprintf(stderr, "  registering %s failed: %s\n", name, error);
    CHECK(error == NULL, "registration succeeds");
    free(error);
}

static void expect_create_error(
    chdb_connection conn, const char * name, const char * const * argument_types, size_t argument_count,
    const char * return_type, uint32_t flags, const char * fragment)
{
    udf_stats stats;
    memset(&stats, 0, sizeof(stats));
    char * error = create_function(conn, name, argument_types, argument_count, return_type, flags, plus_one, &stats);
    if (!error || !strstr(error, fragment))
    {
        fprintf(stderr, "  registering %s: expected an error containing: %s\n    got: %s\n",
                name, fragment, error ? error : "(success)");
        CHECK(0, "registration error matches");
    }
    CHECK(stats.destroyed == 0, "a failed registration does not destroy user_data");
    free(error);
}

static char * drop_function(chdb_connection conn, const char * name)
{
    chdb_result * result = chdb_drop_scalar_function(conn, name);
    const char * error = chdb_result_error(result);
    char * message = error ? copy_string(error) : NULL;
    chdb_destroy_query_result(result);
    return message;
}

static const char * const INT64_ARG[] = {"Int64"};
static const char * const STRING_ARG[] = {"String"};
static const char * const TWO_STRING_ARGS[] = {"String", "String"};

static udf_stats plus_one_stats;

static void test_basic(chdb_connection conn)
{
    printf("== test_basic ==\n");
    must_create(conn, "c_plus_one", INT64_ARG, 1, "Int64", 0, plus_one, &plus_one_stats);

    expect_output(conn, "SELECT c_plus_one(41)", "42\n");
    expect_output(conn, "SELECT c_plus_one(toInt64(number)) FROM numbers(3)", "1\n2\n3\n");
    /* Many blocks, evaluated a block at a time. */
    int64_t calls_before = plus_one_stats.calls;
    expect_output(conn, "SELECT sum(c_plus_one(toInt64(number))) FROM numbers(200000)", "20000100000\n");
    CHECK(plus_one_stats.calls - calls_before >= 2, "a large input takes several calls");
    CHECK(plus_one_stats.max_batch_rows > 1, "a call covers a whole batch of rows");
    /* Concurrent calls from several threads. */
    expect_output(conn,
        "SELECT sum(c_plus_one(toInt64(number))) FROM numbers_mt(1000000) SETTINGS max_threads = 8, max_block_size = 8192",
        "500000500000\n");
    expect_output(conn, "SELECT toTypeName(c_plus_one(1))", "\"Int64\"\n");
}

static void test_argument_types(chdb_connection conn)
{
    printf("== test_argument_types ==\n");
    /* An Int32 converts to the declared Int64 without loss; a String does not. */
    expect_output(conn, "SELECT c_plus_one(toInt32(-5))", "-4\n");
    expect_error(conn, "SELECT c_plus_one('x')", "does not convert to the declared Int64");
    expect_error(conn, "SELECT c_plus_one(toUInt64(1))", "does not convert to the declared Int64");
    expect_error(conn, "SELECT c_plus_one(1, 2)", "NUMBER_OF_ARGUMENTS_DOESNT_MATCH");
}

static void test_null_handling(chdb_connection conn)
{
    printf("== test_null_handling ==\n");
    int64_t calls_before = plus_one_stats.calls;
    /* Rows with a NULL argument never reach the callback and yield NULL. */
    expect_output(conn, "SELECT c_plus_one(if(number % 2 = 0, NULL, toInt64(number))) FROM numbers(4)", "\\N\n2\n\\N\n4\n");
    expect_output(conn, "SELECT toTypeName(c_plus_one(toNullable(toInt64(1))))", "\"Nullable(Int64)\"\n");
    /* A block of NULLs only is not evaluated at all. */
    int64_t calls_mid = plus_one_stats.calls;
    expect_output(conn, "SELECT c_plus_one(NULL)", "\\N\n");
    CHECK(plus_one_stats.calls == calls_mid, "an all-NULL batch does not call the function");
    CHECK(plus_one_stats.calls > calls_before, "the non-NULL rows were evaluated");
    CHECK(plus_one_stats.nulls_seen == 0, "default NULL handling never passes a NULL");

    static udf_stats stats;
    must_create(conn, "c_is_null", INT64_ARG, 1, "UInt8", CHDB_SCALAR_FUNCTION_SPECIAL_NULL_HANDLING, is_null, &stats);
    expect_output(conn, "SELECT c_is_null(if(number = 1, NULL, toInt64(number))) FROM numbers(3)", "0\n1\n0\n");
    expect_output(conn, "SELECT toTypeName(c_is_null(toNullable(toInt64(1))))", "\"UInt8\"\n");
    expect_output(conn, "SELECT c_is_null(NULL)", "1\n");
}

static void test_strings(chdb_connection conn)
{
    printf("== test_strings ==\n");
    static udf_stats concat_stats;
    static udf_stats length_stats;
    must_create(conn, "c_concat_dash", TWO_STRING_ARGS, 2, "String", 0, concat_dash, &concat_stats);
    expect_output(conn, "SELECT c_concat_dash('a', 'b')", "\"a-b\"\n");
    expect_output(conn, "SELECT c_concat_dash(toString(number), 'héllo') FROM numbers(2)", "\"0-héllo\"\n\"1-héllo\"\n");
    /* A FixedString converts to String, and a LowCardinality column reaches the function materialized. */
    expect_output(conn, "SELECT c_concat_dash(toFixedString('xy', 2), 'z')", "\"xy-z\"\n");
    expect_output(conn, "SELECT c_concat_dash(toLowCardinality(toString(number)), 'z') FROM numbers(2)", "\"0-z\"\n\"1-z\"\n");
    /* A UTF-8 string argument must hold valid UTF-8... */
    expect_error(conn, "SELECT c_concat_dash(unhex('FF'), 'z')", "not valid UTF-8");
    /* ...unless the function takes binary strings. */
    must_create(conn, "c_byte_length", STRING_ARG, 1, "UInt64", CHDB_SCALAR_FUNCTION_BINARY_STRINGS, byte_length, &length_stats);
    expect_output(conn, "SELECT c_byte_length(unhex('FF00FF'))", "3\n");
    expect_output(conn, "SELECT c_byte_length('héllo')", "6\n");
}

static void test_volatile(chdb_connection conn)
{
    printf("== test_volatile ==\n");
    static udf_stats deterministic_stats;
    static udf_stats volatile_stats;
    must_create(conn, "c_batch_rows", NULL, 0, "UInt64", 0, batch_rows, &deterministic_stats);
    must_create(conn, "c_batch_rows_volatile", NULL, 0, "UInt64", CHDB_SCALAR_FUNCTION_VOLATILE, batch_rows, &volatile_stats);
    /* A deterministic call without arguments is folded into one value; a volatile one is evaluated per row. */
    expect_output(conn, "SELECT c_batch_rows(), c_batch_rows_volatile() FROM numbers(3)", "1,3\n1,3\n1,3\n");
    CHECK(volatile_stats.rows == 3, "the volatile function saw every row");
}

static void test_failures(chdb_connection conn)
{
    printf("== test_failures ==\n");
    static udf_stats fail_stats;
    static udf_stats length_stats;
    static udf_stats cast_stats;
    must_create(conn, "c_fail_on_zero", INT64_ARG, 1, "Int64", 0, fail_on_zero, &fail_stats);
    expect_error(conn, "SELECT c_fail_on_zero(toInt64(number)) FROM numbers(3)", "boom: zero at row 0");
    expect_error(conn, "SELECT c_fail_on_zero(toInt64(number)) FROM numbers(3)", "UDF_EXECUTION_FAILED");
    /* Short-circuit evaluation hands the function only the rows its branch takes. */
    expect_output(conn, "SELECT if(number > 0, c_fail_on_zero(toInt64(number)), -1) FROM numbers(3)", "-1\n1\n2\n");

    must_create(conn, "c_one_row_too_many", INT64_ARG, 1, "Int64", 0, one_row_too_many, &length_stats);
    expect_error(conn, "SELECT c_one_row_too_many(1)", "returned 2 rows for a batch of 1 rows");

    /* The result is narrowed to the declared type, and a value that does not fit is an error. */
    must_create(conn, "c_times_hundred", INT64_ARG, 1, "UInt8", 0, times_hundred, &cast_stats);
    expect_output(conn, "SELECT c_times_hundred(2), toTypeName(c_times_hundred(2))", "200,\"UInt8\"\n");
    expect_error(conn, "SELECT c_times_hundred(3)", "while converting the result of Scalar function 'c_times_hundred'");
}

static void test_registration_errors(chdb_connection conn)
{
    printf("== test_registration_errors ==\n");
    expect_create_error(conn, "c_plus_one", INT64_ARG, 1, "Int64", 0, "is already registered");
    expect_create_error(conn, "plus", INT64_ARG, 1, "Int64", 0, "a built-in function with that name already exists");
    expect_create_error(conn, "LOWER", INT64_ARG, 1, "Int64", 0, "a built-in function with that name already exists");
    expect_create_error(conn, "sum", INT64_ARG, 1, "Int64", 0, "an aggregate function with that name already exists");
    expect_create_error(conn, "c_bad_type", (const char * const[]){"NotAType"}, 1, "Int64", 0, "NotAType");
    expect_create_error(conn, "c_bad_return", INT64_ARG, 1, "AggregateFunction(sum, UInt64)", 0, "has no Arrow form");
    expect_create_error(conn, "c_no_return", INT64_ARG, 1, NULL, 0, "return type is missing");
    expect_create_error(conn, "c_bad_flags", INT64_ARG, 1, "Int64", 1u << 30, "Unknown chdb_scalar_function_flag");

    udf_stats stats;
    memset(&stats, 0, sizeof(stats));
    chdb_scalar_function_options options;
    memset(&options, 0, sizeof(options));
    options.struct_size = sizeof(options);
    options.argument_types = INT64_ARG;
    options.argument_count = 1;
    options.return_type = "Int64";
    options.user_data = &stats;
    options.destroy = destroy_stats;

    chdb_result * result = chdb_create_scalar_function(conn, "c_null_callback", &options);
    CHECK(chdb_result_error(result) != NULL, "a null callback is rejected");
    chdb_destroy_query_result(result);

    options.callback = plus_one;
    options.struct_size = sizeof(options) - 1;
    result = chdb_create_scalar_function(conn, "c_small_struct", &options);
    CHECK(chdb_result_error(result) != NULL && strstr(chdb_result_error(result), "struct_size"), "a short struct is rejected");
    chdb_destroy_query_result(result);

    result = chdb_create_scalar_function(conn, "", &options);
    CHECK(chdb_result_error(result) != NULL, "an empty name is rejected");
    chdb_destroy_query_result(result);
    CHECK(stats.destroyed == 0, "rejected registrations do not destroy user_data");

    /* A SQL function cannot take the name later: it would be expanded instead, e.g. in a DELETE. */
    expect_error(conn, "CREATE FUNCTION c_plus_one AS x -> x", "registered through the C API already exists");
    expect_output(conn, "SELECT c_plus_one(1)", "2\n");

    /* A result type chDB cannot read back from Arrow is refused up front. */
    expect_create_error(conn, "c_variant", INT64_ARG, 1, "Variant(Int64, String)", 0, "that chDB can read back");
}

static void test_types(chdb_connection conn)
{
    printf("== test_types ==\n");
    static const char * const STRING_ARRAY_ARG[] = {"Array(String)"};
    static const char * const INT64_ARRAY_ARG[] = {"Array(Int64)"};
    static const char * const FLOAT32_ARG[] = {"Float32"};
    static udf_stats nested_stats, lc_stats, length_stats, third_stats;

    /* Binary strings reach the function as Arrow binary inside an Array too. */
    must_create(conn, "c_nested_bytes", STRING_ARRAY_ARG, 1, "UInt64", CHDB_SCALAR_FUNCTION_BINARY_STRINGS, nested_bytes, &nested_stats);
    expect_output(conn, "SELECT c_nested_bytes([unhex('FF'), 'ab'])", "3\n");

    /* A LowCardinality return type means its base type, and the function only sees real rows. */
    must_create(conn, "c_concat_lc", TWO_STRING_ARGS, 2, "LowCardinality(String)", 0, concat_dash, &lc_stats);
    expect_output(conn, "SELECT toTypeName(c_concat_lc('a', 'b'))", "\"String\"\n");
    int64_t rows_before = lc_stats.rows;
    expect_output(conn, "SELECT c_concat_lc(toLowCardinality(toString(number)), 'x') FROM numbers(2)", "\"0-x\"\n\"1-x\"\n");
    CHECK(lc_stats.rows - rows_before == 2, "the function sees the two rows, not a dictionary");

    /* With special NULL handling an argument that can be NULL needs a declared type that holds one. */
    must_create(conn, "c_list_length", INT64_ARRAY_ARG, 1, "UInt64", CHDB_SCALAR_FUNCTION_SPECIAL_NULL_HANDLING, list_length, &length_stats);
    expect_output(conn, "SELECT c_list_length([1, 2, 3])", "3\n");
    expect_error(conn, "SELECT c_list_length(NULL)", "cannot hold a NULL");

    /* A float result narrows to BFloat16 like to any float type, without an accuracy error. */
    must_create(conn, "c_third", FLOAT32_ARG, 1, "BFloat16", 0, third, &third_stats);
    expect_output(conn, "SELECT c_third(toFloat32(1)) BETWEEN 0.33 AND 0.34, toTypeName(c_third(toFloat32(1)))", "1,\"BFloat16\"\n");
}

/* A block whose strings exceed one Arrow record batch takes one call per batch, and the calls may
 * answer in different Arrow types. */
static void test_split_batches(chdb_connection conn)
{
    printf("== test_split_batches ==\n");
    static udf_stats stats;
    must_create(conn, "c_length_alternating", STRING_ARG, 1, "Int64", 0, length_alternating, &stats);
    expect_output(conn,
        "SELECT sum(c_length_alternating(repeat('x', 100 + number % 2))), count() FROM numbers(100000) "
        "SETTINGS output_format_arrow_max_record_batch_size_bytes = 65536, max_block_size = 65536, max_threads = 1",
        "10050000,100000\n");
    CHECK(stats.calls >= 4, "blocks were split into several calls");
}

static void test_tables(chdb_connection conn)
{
    printf("== test_tables ==\n");
    expect_output(conn, "CREATE TABLE udf_t (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1024", "");
    expect_output(conn, "INSERT INTO udf_t SELECT number FROM numbers(100000)", "");
    /* PREWHERE resolves its functions again by name. */
    expect_output(conn, "SELECT count() FROM udf_t PREWHERE c_plus_one(x) > 2", "99998\n");

    /* A call is expensive, so PREWHERE evaluates it after the cheap condition and never on the rows that drops. */
    expect_output(conn, "CREATE TABLE udf_g (id Int64, v Int64) ENGINE = MergeTree ORDER BY id", "");
    expect_output(conn, "INSERT INTO udf_g VALUES (-1, 0), (1, 5)", "");
    expect_output(conn, "SELECT id FROM udf_g WHERE id > 0 AND c_fail_on_zero(v) > 0", "1\n");

    /* A function registered again under its name is not answered from the query condition cache. */
    static udf_stats first, second;
    first.value = 0;
    second.value = 1;
    const char * filtered = "SELECT count() FROM udf_t WHERE c_flag(x) SETTINGS use_query_condition_cache = 1";
    must_create(conn, "c_flag", INT64_ARG, 1, "UInt8", 0, constant_flag, &first);
    expect_output(conn, filtered, "0\n");
    char * error = drop_function(conn, "c_flag");
    free(error);
    must_create(conn, "c_flag", INT64_ARG, 1, "UInt8", 0, constant_flag, &second);
    expect_output(conn, filtered, "100000\n");
    error = drop_function(conn, "c_flag");
    free(error);

    expect_output(conn, "DROP TABLE udf_t", "");
    expect_output(conn, "DROP TABLE udf_g", "");
}

/* The query result cache only checks for non-deterministic functions when it can store a result,
 * which in chDB takes the on-disk cache that the configuration in argv[1] declares. */
static void test_query_cache(chdb_connection conn, int has_cache)
{
    printf("== test_query_cache ==\n");
    if (!has_cache)
    {
        printf("  skipped: no configuration with a query result cache\n");
        return;
    }
    /* A function can be replaced under its name, so its results are never cached. */
    expect_error(conn,
        "SELECT c_plus_one(1) SETTINGS use_query_cache = 1, query_cache_on_disk_cache_name = 'query_results', "
        "query_cache_nondeterministic_function_handling = 'throw'",
        "non-deterministic");
}

static void test_drop(chdb_connection conn)
{
    printf("== test_drop ==\n");
    CHECK(plus_one_stats.destroyed == 0, "a registered function keeps its user_data");
    char * error = drop_function(conn, "c_plus_one");
    CHECK(error == NULL, "drop succeeds");
    free(error);
    CHECK(plus_one_stats.destroyed == 1, "dropping destroys user_data exactly once");
    expect_error(conn, "SELECT c_plus_one(1)", "UNKNOWN_FUNCTION");

    error = drop_function(conn, "c_plus_one");
    CHECK(error != NULL && strstr(error, "is not registered"), "dropping twice fails");
    free(error);
    CHECK(plus_one_stats.destroyed == 1, "a failed drop destroys nothing");

    /* The name is free again. */
    static udf_stats again;
    must_create(conn, "c_plus_one", INT64_ARG, 1, "Int64", 0, plus_one, &again);
    expect_output(conn, "SELECT c_plus_one(1)", "2\n");
    error = drop_function(conn, "c_plus_one");
    free(error);
    CHECK(again.destroyed == 1, "the second registration is destroyed on its own drop");

    /* Registrations outlive connections, so dropping needs none. */
    static udf_stats without_connection;
    must_create(conn, "c_drop_without_connection", INT64_ARG, 1, "Int64", 0, plus_one, &without_connection);
    error = drop_function(NULL, "c_drop_without_connection");
    CHECK(error == NULL, "drop without a connection succeeds");
    free(error);
    CHECK(without_connection.destroyed == 1, "dropping without a connection destroys user_data");
}

int main(int argc, char ** argv)
{
    /* argv[1], if given, is a configuration file declaring the `query_results` filesystem cache. */
    char arg0[] = "clickhouse";
    char arg1[] = "--multiquery";
    char config[4096] = "";
    char * args[] = {arg0, arg1, config};
    const int has_cache = argc > 1;
    if (has_cache)
        snprintf(config, sizeof(config), "--config-file=%s", argv[1]);
    chdb_connection * conn = chdb_connect(has_cache ? 3 : 2, args);
    if (!conn)
    {
        fprintf(stderr, "chdb_connect failed\n");
        return 1;
    }

    test_basic(*conn);
    test_argument_types(*conn);
    test_null_handling(*conn);
    test_strings(*conn);
    test_volatile(*conn);
    test_failures(*conn);
    test_registration_errors(*conn);
    test_types(*conn);
    test_split_batches(*conn);
    test_tables(*conn);
    test_query_cache(*conn, has_cache);
    test_drop(*conn);

    chdb_close_conn(conn);

    printf("\n== summary: %d failed assertions ==\n", g_failed_assertions);
    return g_failed_assertions == 0 ? 0 : 1;
}
