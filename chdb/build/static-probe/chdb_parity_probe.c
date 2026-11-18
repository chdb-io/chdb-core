/* What a static libchdb.a registers and how its C API behaves, as a report that
 * check_static_lib_parity.sh compares between two archives of the same build: the prelinked
 * libchdb.a and the split one (chdb-io/chdb-core#342).
 *
 * The failure it is for links, connects and answers SELECT 1 and is still wrong: a member
 * that holds only static registrations (a function, a format, a table engine) left out of
 * the link, or registered twice. So the report has every registry's names, sorted, and the
 * results of queries over the paths a consumer uses: plain, failing, streaming, and Arrow
 * export, scan and insert. Nothing in it depends on the version - no counts are written down
 * here - since the baseline comes from the same build.
 *
 * Exits non-zero when a check fails on its own (an empty registry, a query that should have
 * succeeded or failed and did not), so a bad archive fails even without a baseline to diff.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chdb.h"

/* The Arrow C stream interface (arrow/c/abi.h). chdb.h passes it around as an opaque
   handle; this probe only moves it between chDB calls and releases it. */
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream
{
    int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema * out);
    int (*get_next)(struct ArrowArrayStream *, struct ArrowArray * out);
    const char * (*get_last_error)(struct ArrowArrayStream *);
    void (*release)(struct ArrowArrayStream *);
    void * private_data;
};

static chdb_connection conn;
static int failures;

static void fail(const char * what, const char * detail)
{
    fprintf(stderr, "parity probe: %s: %s\n", what, detail ? detail : "(no detail)");
    failures++;
}

/* Prints a result's payload under a heading, with a terminating newline of its own. */
static void print_payload(const char * heading, chdb_result * result)
{
    const char * buffer = chdb_result_buffer(result);
    size_t length = chdb_result_length(result);
    printf("## %s\n", heading);
    if (buffer && length)
        fwrite(buffer, 1, length, stdout);
    if (!length || buffer[length - 1] != '\n')
        printf("\n");
}

/* Runs a query that must succeed and print at least one row, and reports its output. */
static void report(const char * heading, const char * query)
{
    chdb_result * result = chdb_query(conn, query, "TSVRaw");
    if (!result)
    {
        fail(heading, "chdb_query returned NULL");
        return;
    }
    const char * error = chdb_result_error(result);
    if (error)
        fail(heading, error);
    else if (chdb_result_length(result) == 0)
        fail(heading, "returned no rows");
    else
        print_payload(heading, result);
    chdb_destroy_query_result(result);
}

/* Runs a query that must fail, and reports the error (code and message). */
static void report_error(const char * heading, const char * query)
{
    chdb_result * result = chdb_query(conn, query, "TSVRaw");
    if (!result)
    {
        fail(heading, "chdb_query returned NULL");
        return;
    }
    const char * error = chdb_result_error(result);
    if (!error)
        fail(heading, "succeeded, but must fail");
    else
        printf("## %s\n%s\n", heading, error);
    chdb_destroy_query_result(result);
}

/* Reports a query that must succeed and checks its output too. */
static void expect(const char * heading, const char * query, const char * expected)
{
    chdb_result * result = chdb_query(conn, query, "TSVRaw");
    if (!result)
    {
        fail(heading, "chdb_query returned NULL");
        return;
    }
    const char * error = chdb_result_error(result);
    const char * buffer = chdb_result_buffer(result);
    size_t length = chdb_result_length(result);
    if (error)
        fail(heading, error);
    else if (length != strlen(expected) || (length && memcmp(buffer, expected, length) != 0))
    {
        fprintf(stderr, "parity probe: %s: expected \"%s\", got \"%.*s\"\n", heading, expected, (int)length, buffer ? buffer : "");
        failures++;
    }
    else
        print_payload(heading, result);
    chdb_destroy_query_result(result);
}

/* FNV-1a, to put a streamed payload into the report without its chunk boundaries. */
static uint64_t fnv1a(uint64_t hash, const char * data, size_t length)
{
    for (size_t i = 0; i < length; i++)
        hash = (hash ^ (unsigned char)data[i]) * 1099511628211ULL;
    return hash;
}

static void report_stream(void)
{
    const char * heading = "stream query";
    chdb_result * stream = chdb_stream_query(conn,
        "SELECT number, toString(number * 7) FROM numbers(200000) SETTINGS max_block_size = 8192", "TSVRaw");
    if (!stream)
    {
        fail(heading, "chdb_stream_query returned NULL");
        return;
    }
    if (chdb_result_error(stream))
    {
        fail(heading, chdb_result_error(stream));
        chdb_destroy_query_result(stream);
        return;
    }
    uint64_t hash = 14695981039346656037ULL;
    size_t bytes = 0, rows = 0, chunks = 0;
    for (;;)
    {
        chdb_result * chunk = chdb_stream_fetch_result(conn, stream);
        if (!chunk)
        {
            fail(heading, "chdb_stream_fetch_result returned NULL");
            break;
        }
        if (chdb_result_error(chunk))
        {
            fail(heading, chdb_result_error(chunk));
            chdb_destroy_query_result(chunk);
            break;
        }
        size_t length = chdb_result_length(chunk);
        if (length == 0)
        {
            chdb_destroy_query_result(chunk);
            break;
        }
        const char * buffer = chdb_result_buffer(chunk);
        hash = fnv1a(hash, buffer, length);
        for (size_t i = 0; i < length; i++)
            rows += buffer[i] == '\n';
        bytes += length;
        chunks++;
        chdb_destroy_query_result(chunk);
    }
    chdb_destroy_query_result(stream);
    /* More than one chunk, or the query did not stream at all. */
    if (rows != 200000 || chunks < 2)
    {
        fprintf(stderr, "parity probe: %s: %zu rows in %zu chunks, expected 200000 rows in several\n", heading, rows, chunks);
        failures++;
    }
    printf("## %s\nrows %zu, bytes %zu, fnv1a %016llx\n", heading, rows, bytes, (unsigned long long)hash);
}

/* Exports a query's result as an Arrow stream into `stream`. */
static int export_arrow(const char * heading, const char * query, struct ArrowArrayStream * stream)
{
    memset(stream, 0, sizeof(*stream));
    chdb_result * result = chdb_query_arrow(conn, query, (chdb_arrow_stream)stream, NULL);
    if (!result)
    {
        fail(heading, "chdb_query_arrow returned NULL");
        return 0;
    }
    const char * error = chdb_result_error(result);
    if (error)
        fail(heading, error);
    else if (!stream->release)
        fail(heading, "chdb_query_arrow filled no stream");
    chdb_destroy_query_result(result);
    return !error && stream->release;
}

static void release_arrow(struct ArrowArrayStream * stream)
{
    if (stream->release)
        stream->release(stream);
}

static const char * ARROW_SOURCE =
    "SELECT number AS id, toString(number) AS s, toDecimal64(number, 2) / 3 AS d, "
    "toDateTime64(number, 3, 'UTC') AS t, [number, number + 1] AS a "
    "FROM numbers(10000)";
static const char * ARROW_SUMMARY = "count(), sum(id), max(s), sum(d), max(t), sum(arraySum(a))";

static void report_arrow(void)
{
    struct ArrowArrayStream stream;
    char query[512];

    /* Export, then scan the exported stream back as a table function. */
    if (export_arrow("arrow export", ARROW_SOURCE, &stream))
    {
        if (chdb_arrow_scan(conn, "parity_arrow", (chdb_arrow_stream)&stream) != CHDBSuccess)
            fail("arrow scan", "chdb_arrow_scan failed");
        else
        {
            snprintf(query, sizeof(query), "SELECT %s FROM arrowstream(parity_arrow)", ARROW_SUMMARY);
            report("arrow export + scan", query);
            if (chdb_arrow_unregister_table(conn, "parity_arrow") != CHDBSuccess)
                fail("arrow scan", "chdb_arrow_unregister_table failed");
        }
        release_arrow(&stream);
    }

    /* Export, then insert the stream into a MergeTree table. */
    report_error("arrow insert: table does not exist yet", "SELECT * FROM parity_arrow_insert");
    chdb_result * created = chdb_query(conn,
        "CREATE TABLE parity_arrow_insert (id UInt64, s String, d Decimal64(2), t DateTime64(3, 'UTC'), a Array(UInt64)) "
        "ENGINE = MergeTree ORDER BY id", "TSVRaw");
    if (!created || chdb_result_error(created))
        fail("arrow insert", created ? chdb_result_error(created) : "CREATE TABLE returned NULL");
    if (created)
        chdb_destroy_query_result(created);
    if (export_arrow("arrow export for insert", ARROW_SOURCE, &stream))
    {
        chdb_result * inserted = chdb_insert_arrow_stream(conn, "parity_arrow_insert", (chdb_arrow_stream)&stream, NULL);
        if (!inserted || chdb_result_error(inserted))
            fail("arrow insert", inserted ? chdb_result_error(inserted) : "chdb_insert_arrow_stream returned NULL");
        if (inserted)
            chdb_destroy_query_result(inserted);
        release_arrow(&stream);
        snprintf(query, sizeof(query), "SELECT %s FROM parity_arrow_insert", ARROW_SUMMARY);
        report("arrow insert", query);
    }
}

int main(void)
{
    char * argv[] = {"chdb_parity_probe"};
    chdb_connection * connection = chdb_connect(1, argv);
    if (!connection)
    {
        fprintf(stderr, "parity probe: chdb_connect returned NULL\n");
        return 1;
    }
    conn = *connection;

    /* Registries: one line per name (and, where there is one, per kind), sorted. A missing
       or duplicated registration shows as a line of its own in the diff. */
    report("functions", "SELECT name, is_aggregate, case_insensitive, alias_to FROM system.functions ORDER BY ALL");
    report("aggregate function combinators", "SELECT name FROM system.aggregate_function_combinators ORDER BY ALL");
    report("table functions", "SELECT name FROM system.table_functions ORDER BY ALL");
    report("table engines", "SELECT name FROM system.table_engines ORDER BY ALL");
    report("formats", "SELECT name, is_input, is_output FROM system.formats ORDER BY ALL");
    report("data type families", "SELECT name, case_insensitive, alias_to FROM system.data_type_families ORDER BY ALL");
    report("codecs", "SELECT name FROM system.codecs ORDER BY ALL");
    report("settings", "SELECT name FROM system.settings ORDER BY ALL");
    report("merge tree settings", "SELECT name FROM system.merge_tree_settings ORDER BY ALL");
    report("system tables", "SELECT name FROM system.tables WHERE database = 'system' ORDER BY ALL");
    /* Every registration done once: no name twice in a registry. */
    expect("no duplicate registrations",
           "SELECT (SELECT count() - uniqExact(name) FROM system.functions) + "
           "(SELECT count() - uniqExact(name) FROM system.formats) + "
           "(SELECT count() - uniqExact(name) FROM system.table_functions) + "
           "(SELECT count() - uniqExact(name) FROM system.table_engines) + "
           "(SELECT count() - uniqExact(name) FROM system.data_type_families)",
           "0\n");

    /* Queries that succeed, with their exact results. */
    report("arithmetic and hashing",
           "SELECT number, number * 3, sipHash64(number), cityHash64(toString(number)), hex(MD5(toString(number))), "
           "hex(SHA256(toString(number))), xxHash64(number) FROM numbers(3)");
    report("aggregation", "SELECT number % 7 AS k, count(), sum(number), uniqExact(number), quantileExact(0.5)(number) "
                          "FROM numbers(100000) GROUP BY k ORDER BY k");
    report("formats round trip",
           "SELECT * FROM format(JSONEachRow, '{\"a\": 1, \"b\": \"x\"}\\n{\"a\": 2, \"b\": \"y\"}') ORDER BY a");
    expect("compression codecs: create",
           "CREATE TABLE parity_codecs (k UInt64 CODEC(Delta, ZSTD(3)), v String CODEC(LZ4HC), "
           "f Float64 CODEC(Gorilla, LZ4), t UInt32 CODEC(T64, ZSTD)) ENGINE = MergeTree ORDER BY k",
           "");
    expect("compression codecs: insert",
           "INSERT INTO parity_codecs SELECT number, toString(number), number / 4, number FROM numbers(50000)", "");
    report("compression codecs", "SELECT count(), sum(k), max(v), sum(f), sum(t) FROM parity_codecs");
    /* Queries that fail: the error text, which names the code, is compared as well. */
    report_error("unknown table", "SELECT * FROM parity_missing_table");
    report_error("unknown function", "SELECT parity_missing_function(1)");
    report_error("syntax error", "SELEC 1");
    report_error("type mismatch", "SELECT 1 + 'a'");
    /* The engine still works after a failure. */
    expect("after failures", "SELECT 40 + 2", "42\n");

    report_stream();
    report_arrow();

    chdb_close_conn(connection);
    if (failures)
    {
        fprintf(stderr, "parity probe: %d check(s) failed\n", failures);
        return 1;
    }
    return 0;
}
