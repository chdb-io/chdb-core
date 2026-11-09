/**
 * chdbResultSchemaTest.c
 *
 * Result schema metadata through the C API (chdb-io/chdb-core#16):
 * chdb_result_column_count / chdb_result_column_name / chdb_result_column_type
 * report the engine's own column names and ClickHouse type strings, so a host
 * never parses DESCRIBE output or a TabSeparatedWithNamesAndTypes header.
 *
 * Covers:
 *   - exact names and type strings for scalar, Nullable, Decimal, DateTime64,
 *     LowCardinality, Array, Tuple and Map columns
 *   - the same schema whatever the output format (text, Arrow, Parquet, Native)
 *   - a result with zero rows still reporting its columns
 *   - schema inference from a Parquet file agreeing with DESCRIBE, the
 *     pg_clickhouse use case from the issue
 *   - DDL, INSERT, failed queries and NULL handles reporting no columns
 *   - out-of-range indexes returning NULL
 *   - multi-statement text reporting the last result set
 *   - parameterized queries and every fetched batch of a streaming query
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbResultSchemaTest.c -I./programs/local -L. -lchdb -o examples/chdbResultSchemaTest
 *   LD_LIBRARY_PATH=. ./examples/chdbResultSchemaTest
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

static int str_eq(const char * a, const char * b)
{
    return a && b && strcmp(a, b) == 0;
}

/// Asserts that result r has exactly the columns in names/types, in order.
static void check_schema(chdb_result * r, int n, const char * const * names, const char * const * types, const char * what)
{
    char msg[512];
    snprintf(msg, sizeof(msg), "%s: no error (%s)", what, r && chdb_result_error(r) ? chdb_result_error(r) : "");
    CHECK(r && !chdb_result_error(r), msg);
    if (!r)
        return;

    snprintf(msg, sizeof(msg), "%s: column count is %d, got %d", what, n, chdb_result_column_count(r));
    CHECK(chdb_result_column_count(r) == n, msg);
    for (int i = 0; i < n && i < chdb_result_column_count(r); ++i)
    {
        snprintf(msg, sizeof(msg), "%s: column %d name is '%s', got '%s'", what, i, names[i],
                 chdb_result_column_name(r, i) ? chdb_result_column_name(r, i) : "(null)");
        CHECK(str_eq(chdb_result_column_name(r, i), names[i]), msg);
        snprintf(msg, sizeof(msg), "%s: column %d type is '%s', got '%s'", what, i, types[i],
                 chdb_result_column_type(r, i) ? chdb_result_column_type(r, i) : "(null)");
        CHECK(str_eq(chdb_result_column_type(r, i), types[i]), msg);
    }
}

static void check_no_schema(chdb_result * r, const char * what)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "%s: column count is 0, got %d", what, chdb_result_column_count(r));
    CHECK(chdb_result_column_count(r) == 0, msg);
    snprintf(msg, sizeof(msg), "%s: column 0 name is NULL", what);
    CHECK(chdb_result_column_name(r, 0) == NULL, msg);
    snprintf(msg, sizeof(msg), "%s: column 0 type is NULL", what);
    CHECK(chdb_result_column_type(r, 0) == NULL, msg);
}

static const char * TYPED_QUERY =
    "SELECT toUInt64(1) AS id, CAST(NULL, 'Nullable(String)') AS note, "
    "toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC') AS ts, "
    "CAST(1.5, 'Nullable(Decimal(18, 4))') AS amount, toLowCardinality('x') AS tag, "
    "[1, 2]::Array(Int32) AS xs, (1, 'a')::Tuple(a UInt8, b String) AS t, "
    "map('k', 1)::Map(String, UInt16) AS m";

static const char * TYPED_NAMES[] = {"id", "note", "ts", "amount", "tag", "xs", "t", "m"};
static const char * TYPED_TYPES[] = {
    "UInt64", "Nullable(String)", "DateTime64(3, 'UTC')", "Nullable(Decimal(18, 4))",
    "LowCardinality(String)", "Array(Int32)", "Tuple(a UInt8, b String)", "Map(String, UInt16)"};

/// Reads DESCRIBE's "name\ttype" rows into names/types; returns the count.
static int describe(chdb_connection conn, const char * sql, char names[][64], char types[][128], int max)
{
    chdb_result * r = chdb_query(conn, sql, "TSVRaw");
    int n = 0;
    if (r && !chdb_result_error(r))
    {
        const char * p = chdb_result_buffer(r);
        const char * end = p + chdb_result_length(r);
        while (p < end && n < max)
        {
            const char * eol = memchr(p, '\n', (size_t)(end - p));
            if (!eol)
                eol = end;
            const char * tab1 = memchr(p, '\t', (size_t)(eol - p));
            if (!tab1)
                break;
            const char * tab2 = memchr(tab1 + 1, '\t', (size_t)(eol - tab1 - 1));
            if (!tab2)
                tab2 = eol;
            snprintf(names[n], 64, "%.*s", (int)(tab1 - p), p);
            snprintf(types[n], 128, "%.*s", (int)(tab2 - tab1 - 1), tab1 + 1);
            ++n;
            p = eol + 1;
        }
    }
    chdb_destroy_query_result(r);
    return n;
}

static void test_types_and_formats(chdb_connection conn)
{
    /// The engine's own DESCRIBE of the same query must agree, column for column. The API reports
    /// canonical type names, which DESCRIBE prints once print_pretty_type_names stops it from
    /// spreading a named Tuple over several lines.
    char want_names[16][64];
    char want_types[16][128];
    char sql[1024];
    snprintf(sql, sizeof(sql), "DESCRIBE (%s) SETTINGS print_pretty_type_names = 0", TYPED_QUERY);
    int n = describe(conn, sql, want_names, want_types, 16);
    CHECK(n == 8, "DESCRIBE of the typed query reports 8 columns");
    for (int i = 0; i < n && i < 8; ++i)
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "DESCRIBE column %d type '%s' matches expected '%s'", i, want_types[i], TYPED_TYPES[i]);
        CHECK(strcmp(want_types[i], TYPED_TYPES[i]) == 0, msg);
    }
    const char * const * types = TYPED_TYPES;
    chdb_result * r;

    const char * formats[] = {"CSV", "TabSeparated", "JSONEachRow", "Arrow", "Parquet", "Native", "Pretty"};
    for (size_t i = 0; i < sizeof(formats) / sizeof(formats[0]); ++i)
    {
        char what[64];
        snprintf(what, sizeof(what), "typed query in %s", formats[i]);
        r = chdb_query(conn, TYPED_QUERY, formats[i]);
        check_schema(r, 8, TYPED_NAMES, types, what);
        chdb_destroy_query_result(r);
    }

    /// Zero rows still has a schema.
    static const char * empty_names[] = {"n", "s"};
    static const char * empty_types[] = {"UInt64", "String"};
    r = chdb_query(conn, "SELECT number AS n, toString(number) AS s FROM numbers(10) WHERE n > 100", "CSV");
    check_schema(r, 2, empty_names, empty_types, "zero-row result");
    CHECK(r && chdb_result_length(r) == 0, "zero-row result: CSV buffer is empty");
    chdb_destroy_query_result(r);
}

static void test_no_result_set(chdb_connection conn)
{
    chdb_result * r = chdb_query(conn, "CREATE TABLE schema_t (a UInt32, b String) ENGINE = Memory", "CSV");
    CHECK(r && !chdb_result_error(r), "CREATE TABLE succeeds");
    check_no_schema(r, "CREATE TABLE");
    chdb_destroy_query_result(r);

    r = chdb_query(conn, "INSERT INTO schema_t VALUES (1, 'x'), (2, 'y')", "CSV");
    CHECK(r && !chdb_result_error(r), "INSERT succeeds");
    check_no_schema(r, "INSERT");
    chdb_destroy_query_result(r);

    r = chdb_query(conn, "SELECT * FROM table_that_does_not_exist", "CSV");
    CHECK(r && chdb_result_error(r) != NULL, "query on a missing table fails");
    check_no_schema(r, "failed query");
    chdb_destroy_query_result(r);

    check_no_schema(NULL, "NULL result");

    /// A failed query must not hand back the schema of the query before it.
    r = chdb_query(conn, "SELECT 1 AS one", "CSV");
    chdb_destroy_query_result(r);
    r = chdb_query(conn, "SELECT throwIf(1) AS boom", "CSV");
    CHECK(r && chdb_result_error(r) != NULL, "throwIf query fails");
    check_no_schema(r, "failed query after a successful one");
    chdb_destroy_query_result(r);

    /// Nor may a statement without a result set.
    r = chdb_query(conn, "SELECT 1 AS one", "CSV");
    chdb_destroy_query_result(r);
    r = chdb_query(conn, "INSERT INTO schema_t VALUES (3, 'z')", "CSV");
    check_no_schema(r, "INSERT after a SELECT");
    chdb_destroy_query_result(r);
}

static void test_table_and_index_bounds(chdb_connection conn)
{
    static const char * names[] = {"a", "b", "c"};
    static const char * types[] = {"UInt32", "String", "UInt64"};
    chdb_result * r = chdb_query(conn, "SELECT a, b, a + 1 AS c FROM schema_t ORDER BY a", "JSONEachRow");
    check_schema(r, 3, names, types, "SELECT from a table");
    CHECK(chdb_result_column_name(r, -1) == NULL, "negative index: name is NULL");
    CHECK(chdb_result_column_type(r, -1) == NULL, "negative index: type is NULL");
    CHECK(chdb_result_column_name(r, 3) == NULL, "index == count: name is NULL");
    CHECK(chdb_result_column_type(r, 3) == NULL, "index == count: type is NULL");
    /// The strings stay put while the result lives.
    const char * first = chdb_result_column_name(r, 0);
    chdb_result * other = chdb_query(conn, "SELECT 'q' AS zzz", "CSV");
    CHECK(str_eq(first, "a"), "column name pointer is still valid after another query");
    chdb_destroy_query_result(other);
    chdb_destroy_query_result(r);

    /// DESCRIBE is just another result set.
    r = chdb_query(conn, "DESCRIBE TABLE schema_t", "TSV");
    CHECK(r && !chdb_result_error(r), "DESCRIBE succeeds");
    CHECK(chdb_result_column_count(r) >= 2, "DESCRIBE has at least name and type columns");
    CHECK(str_eq(chdb_result_column_name(r, 0), "name"), "DESCRIBE column 0 is name");
    CHECK(str_eq(chdb_result_column_type(r, 0), "String"), "DESCRIBE column 0 is a String");
    CHECK(str_eq(chdb_result_column_name(r, 1), "type"), "DESCRIBE column 1 is type");
    chdb_destroy_query_result(r);
}

static void test_parquet_inference_matches_describe(chdb_connection conn, const char * dir)
{
    char sql[1024];
    snprintf(sql, sizeof(sql),
             "INSERT INTO FUNCTION file('%s/infer.parquet', 'Parquet', "
             "'id UInt64, name Nullable(String), price Decimal(18, 4), at DateTime64(3, \\'UTC\\'), tags Array(String)') "
             "VALUES (1, 'a', 1.25, '2024-01-01 00:00:00.000', ['x']), (2, NULL, 2.5, '2024-01-02 00:00:00.000', [])",
             dir);
    chdb_result * r = chdb_query(conn, sql, "CSV");
    CHECK(r && !chdb_result_error(r), "writing the Parquet file succeeds");
    if (r && chdb_result_error(r))
        fprintf(stderr, "    %s\n", chdb_result_error(r));
    chdb_destroy_query_result(r);

    char want_names[16][64];
    char want_types[16][128];
    snprintf(sql, sizeof(sql), "DESCRIBE TABLE file('%s/infer.parquet')", dir);
    int n = describe(conn, sql, want_names, want_types, 16);
    CHECK(n == 5, "DESCRIBE of the Parquet file reports 5 columns");

    const char * names[16];
    const char * types[16];
    for (int i = 0; i < n; ++i)
    {
        names[i] = want_names[i];
        types[i] = want_types[i];
    }

    /// LIMIT 0 is how a host infers a schema without reading data.
    snprintf(sql, sizeof(sql), "SELECT * FROM file('%s/infer.parquet') LIMIT 0", dir);
    r = chdb_query(conn, sql, "Arrow");
    check_schema(r, n, names, types, "Parquet inference with LIMIT 0 matches DESCRIBE");
    chdb_destroy_query_result(r);

    snprintf(sql, sizeof(sql), "SELECT * FROM file('%s/infer.parquet') ORDER BY id", dir);
    r = chdb_query(conn, sql, "CSV");
    check_schema(r, n, names, types, "Parquet read matches DESCRIBE");
    chdb_destroy_query_result(r);
}

static void test_multi_statement_and_params(chdb_connection conn)
{
    static const char * last_names[] = {"b", "c"};
    static const char * last_types[] = {"String", "Float64"};
    chdb_result * r = chdb_query(conn, "SELECT 1 AS a; SELECT 'x' AS b, 1.5 AS c", "CSV");
    check_schema(r, 2, last_names, last_types, "multi-statement reports the last result set");
    chdb_destroy_query_result(r);

    static const char * mixed_names[] = {"a"};
    static const char * mixed_types[] = {"UInt8"};
    r = chdb_query(conn, "SELECT 1 AS a; CREATE TABLE IF NOT EXISTS schema_t2 (x UInt8) ENGINE = Memory", "CSV");
    check_schema(r, 1, mixed_names, mixed_types, "result set followed by DDL keeps the result set's schema");
    chdb_destroy_query_result(r);

    const char * pnames[] = {"lim", "label"};
    const char * pvalues[] = {"3", "hello"};
    static const char * param_names[] = {"n", "label"};
    static const char * param_types[] = {"UInt64", "String"};
    r = chdb_query_with_params(
        conn, "SELECT number AS n, {label:String} AS label FROM numbers({lim:UInt64})", "CSV", pnames, pvalues, 2);
    check_schema(r, 2, param_names, param_types, "parameterized query");
    chdb_destroy_query_result(r);
}

static void test_streaming(chdb_connection conn)
{
    static const char * names[] = {"number", "sq"};
    static const char * types[] = {"UInt64", "UInt64"};

    chdb_result * stream = chdb_stream_query(
        conn, "SELECT number, number * number AS sq FROM numbers(200000) SETTINGS max_block_size = 65536", "CSV");
    CHECK(stream && !chdb_result_error(stream), "streaming query starts");
    if (!stream || chdb_result_error(stream))
    {
        chdb_destroy_query_result(stream);
        return;
    }
    check_no_schema(stream, "streaming handle itself");

    int batches = 0;
    for (;;)
    {
        chdb_result * batch = chdb_stream_fetch_result(conn, stream);
        CHECK(batch && !chdb_result_error(batch), "stream fetch succeeds");
        if (!batch || chdb_result_error(batch) || chdb_result_length(batch) == 0)
        {
            if (batch && !chdb_result_error(batch))
                check_no_schema(batch, "final empty batch");
            chdb_destroy_query_result(batch);
            break;
        }
        char what[64];
        snprintf(what, sizeof(what), "stream batch %d", batches);
        check_schema(batch, 2, names, types, what);
        chdb_destroy_query_result(batch);
        ++batches;
    }
    CHECK(batches >= 2, "stream delivered several batches");
    chdb_destroy_query_result(stream);
}

int main(void)
{
    char dir[] = "/tmp/chdb_result_schema_XXXXXX";
    if (!mkdtemp(dir))
    {
        perror("mkdtemp");
        return 1;
    }

    char * argv[] = {"clickhouse"};
    chdb_connection * conn = chdb_connect(1, argv);
    CHECK(conn != NULL, "connect");
    if (!conn)
        return 1;

    test_types_and_formats(*conn);
    test_no_result_set(*conn);
    test_table_and_index_bounds(*conn);
    test_parquet_inference_matches_describe(*conn, dir);
    test_multi_statement_and_params(*conn);
    test_streaming(*conn);

    chdb_close_conn(conn);

    char path[256];
    snprintf(path, sizeof(path), "%s/infer.parquet", dir);
    unlink(path);
    rmdir(dir);

    if (g_failed)
    {
        fprintf(stderr, "chdbResultSchemaTest: FAILED\n");
        return 1;
    }
    printf("chdbResultSchemaTest: OK\n");
    return 0;
}
