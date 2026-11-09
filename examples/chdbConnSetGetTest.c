/**
 * chdbConnSetGetTest.c
 *
 * chdb_set / chdb_get (chdb-io/chdb-core#17): settings and param_<name>
 * connection parameters, incl. S3 credentials checked against a local mock
 * endpoint that records each request's Authorization header.
 *
 * Build (against an already-built libchdb.so):
 *   clang examples/chdbConnSetGetTest.c -I./programs/local -L. -lchdb -lpthread -o examples/chdbConnSetGetTest
 *   LD_LIBRARY_PATH=. ./examples/chdbConnSetGetTest
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "chdb.h"

#ifndef MSG_NOSIGNAL
#    define MSG_NOSIGNAL 0 /* macOS: SIGPIPE is ignored in main() instead */
#endif

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
        while (len > 0 && buf[len - 1] == '\n')
            buf[--len] = '\0';
    }
    chdb_destroy_query_result(r);
}

static int is_error(const char * buf)
{
    return strncmp(buf, "<ERR:", 5) == 0;
}

static int get_eq(chdb_connection conn, const char * key, const char * want)
{
    const char * v = chdb_get(conn, key);
    return v && strcmp(v, want) == 0;
}

//===--------------------------------------------------------------------===//
// A mock S3 endpoint: answers every request with a one-line CSV object and
// records the request headers, so a test can see how the engine signed it.
//===--------------------------------------------------------------------===//

static pthread_mutex_t g_mock_mutex = PTHREAD_MUTEX_INITIALIZER;
static char g_mock_headers[1 << 16];
static size_t g_mock_headers_len = 0;
static int g_mock_requests = 0;
static int g_mock_listen_fd = -1;
static int g_mock_port = 0;

static void mock_reset(void)
{
    pthread_mutex_lock(&g_mock_mutex);
    g_mock_headers_len = 0;
    g_mock_headers[0] = '\0';
    g_mock_requests = 0;
    pthread_mutex_unlock(&g_mock_mutex);
}

/// Whether any request since mock_reset() carried `needle` in its headers.
static int mock_saw(const char * needle)
{
    pthread_mutex_lock(&g_mock_mutex);
    int found = strstr(g_mock_headers, needle) != NULL;
    pthread_mutex_unlock(&g_mock_mutex);
    return found;
}

static void * mock_serve(void * arg)
{
    (void)arg;
    static const char body[] = "7\n";
    for (;;)
    {
        int fd = accept(g_mock_listen_fd, NULL, NULL);
        if (fd < 0)
            return NULL;

        /// Keep-alive: serve requests on this socket until the client closes it.
        char req[16384];
        size_t have = 0;
        for (;;)
        {
            char * end = NULL;
            while (!(end = (have ? strstr(req, "\r\n\r\n") : NULL)))
            {
                if (have + 1 >= sizeof(req))
                    break;
                ssize_t n = recv(fd, req + have, sizeof(req) - 1 - have, 0);
                if (n <= 0)
                    break;
                have += (size_t)n;
                req[have] = '\0';
            }
            if (!end)
                break;
            size_t head_len = (size_t)(end - req) + 4;

            pthread_mutex_lock(&g_mock_mutex);
            if (g_mock_headers_len + head_len + 1 < sizeof(g_mock_headers))
            {
                memcpy(g_mock_headers + g_mock_headers_len, req, head_len);
                g_mock_headers_len += head_len;
                g_mock_headers[g_mock_headers_len] = '\0';
            }
            ++g_mock_requests;
            pthread_mutex_unlock(&g_mock_mutex);

            int is_head = strncmp(req, "HEAD ", 5) == 0;
            char resp[512];
            int resp_len = snprintf(resp, sizeof(resp),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/csv\r\n"
                "Content-Length: %zu\r\n"
                "ETag: \"0123456789abcdef\"\r\n"
                "Last-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\n"
                "Accept-Ranges: bytes\r\n"
                "\r\n", sizeof(body) - 1);
            send(fd, resp, (size_t)resp_len, MSG_NOSIGNAL);
            if (!is_head)
                send(fd, body, sizeof(body) - 1, MSG_NOSIGNAL);

            /// Requests carry no body here (GET/HEAD); drop the consumed head.
            memmove(req, req + head_len, have - head_len);
            have -= head_len;
            req[have] = '\0';
        }
        close(fd);
    }
    return NULL;
}

static int mock_start(void)
{
    g_mock_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_mock_listen_fd < 0)
        return -1;
    int one = 1;
    setsockopt(g_mock_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(g_mock_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(g_mock_listen_fd, 16) != 0)
        return -1;
    socklen_t len = sizeof(addr);
    getsockname(g_mock_listen_fd, (struct sockaddr *)&addr, &len);
    g_mock_port = ntohs(addr.sin_port);
    pthread_t t;
    if (pthread_create(&t, NULL, mock_serve, NULL) != 0)
        return -1;
    pthread_detach(t);
    return 0;
}

//===--------------------------------------------------------------------===//

static void test_settings(void)
{
    char buf[512];

    char * argv_a[] = {"clickhouse", "--max_block_size=4321"};
    chdb_connection * a = chdb_connect(2, argv_a);
    CHECK(a != NULL, "connection A connects");
    char * argv_b[] = {"clickhouse"};
    chdb_connection * b = chdb_connect(1, argv_b);
    CHECK(b != NULL, "connection B connects");
    if (!a || !b)
        return;

    /// Before any query: a default, and a value given at connect time.
    char default_threads[64] = {0};
    query_into(*b, "SELECT getSetting('max_threads')", "TSVRaw", buf, sizeof(buf));
    snprintf(default_threads, sizeof(default_threads), "%s", buf);
    CHECK(get_eq(*a, "max_block_size", "4321"), "A: chdb_get sees the connect-time value");
    CHECK(get_eq(*b, "max_block_size", "65409"), "B: chdb_get sees the default max_block_size");
    CHECK(get_eq(*b, "max_threads", default_threads), "B: chdb_get matches getSetting() for max_threads");

    /// chdb_set on a connection that has not run a query yet.
    chdb_connection * fresh = chdb_connect(1, argv_b);
    CHECK(fresh != NULL, "fresh connection connects");
    if (fresh)
    {
        CHECK(chdb_set(*fresh, "max_threads", "5") == CHDBSuccess, "fresh: chdb_set before the first query");
        query_into(*fresh, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
        CHECK(strcmp(buf, "5") == 0, "fresh: the first query sees the value");
        chdb_close_conn(fresh);
    }

    /// chdb_set after queries have run.
    CHECK(chdb_set(*a, "max_threads", "3") == CHDBSuccess, "A: chdb_set max_threads");
    CHECK(get_eq(*a, "max_threads", "3"), "A: chdb_get reads it back");
    query_into(*a, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "3") == 0, "A: getSetting sees it");
    query_into(*a, "SELECT value, changed FROM system.settings WHERE name = 'max_threads'", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "\"3\",1") == 0, "A: system.settings shows it as changed");

    /// Behavior, through a format setting the connection caches.
    query_into(*a, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":null}") == 0, "A: default JSON rendering of nan");
    CHECK(chdb_set(*a, "output_format_json_quote_denormals", "1") == CHDBSuccess, "A: chdb_set a format setting");
    query_into(*a, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":\"nan\"}") == 0, "A: format setting takes effect on the next query");
    CHECK(chdb_set(*a, "output_format_json_quote_denormals", "true") == CHDBSuccess, "A: a Bool accepts 'true'");
    CHECK(get_eq(*a, "output_format_json_quote_denormals", "1"), "A: Bool reads back as 1");

    /// Isolation.
    CHECK(get_eq(*b, "max_threads", default_threads), "B: max_threads untouched by A");
    query_into(*b, "SELECT nan AS x", "JSONEachRow", buf, sizeof(buf));
    CHECK(strcmp(buf, "{\"x\":null}") == 0, "B: A's format setting does not leak");

    /// A query's SETTINGS clause wins for that query only.
    query_into(*a, "SELECT getSetting('max_threads') SETTINGS max_threads = 7", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "A: SETTINGS clause overrides for the query");
    CHECK(get_eq(*a, "max_threads", "3"), "A: value intact after the override");

    /// SET is seen by chdb_get, and chdb_set wins over it afterwards.
    query_into(*a, "SET max_threads = 9", "CSV", buf, sizeof(buf));
    CHECK(!is_error(buf), "A: SET accepted");
    CHECK(get_eq(*a, "max_threads", "9"), "A: chdb_get sees the SET");
    CHECK(chdb_set(*a, "max_threads", "4") == CHDBSuccess, "A: chdb_set after SET");
    query_into(*a, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "4") == 0, "A: query sees chdb_set over the earlier SET");

    /// Refusals leave the value as it was.
    CHECK(chdb_set(*a, "max_threads", "bogus") == CHDBError, "A: invalid value refused");
    CHECK(get_eq(*a, "max_threads", "4"), "A: value unchanged after the invalid value");
    CHECK(chdb_set(*a, "definitely_not_a_setting", "1") == CHDBError, "A: unknown key refused");
    CHECK(chdb_get(*a, "definitely_not_a_setting") == NULL, "A: chdb_get of an unknown key is NULL");
    CHECK(chdb_set(*a, "definitely_not_a_setting", NULL) == CHDBError, "A: resetting an unknown key refused");
    CHECK(chdb_set(NULL, "max_threads", "1") == CHDBError, "NULL connection refused by chdb_set");
    CHECK(chdb_set(*a, NULL, "1") == CHDBError, "NULL key refused by chdb_set");
    CHECK(chdb_get(NULL, "max_threads") == NULL, "NULL connection: chdb_get is NULL");
    CHECK(chdb_get(*a, NULL) == NULL, "NULL key: chdb_get is NULL");

    /// A custom_ key is accepted exactly when SET accepts it; chDB configures no custom prefixes.
    query_into(*a, "SET custom_tenant = 'acme'", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "A: SET custom_tenant refused (no custom_settings_prefixes)");
    CHECK(chdb_set(*a, "custom_tenant", "acme") == CHDBError, "A: chdb_set custom_tenant refused like SET");
    CHECK(chdb_get(*a, "custom_tenant") == NULL, "A: chdb_get custom_tenant is NULL");

    /// A merge_tree_ key is a MergeTree setting kept as a custom one; NULL must reset it too.
    CHECK(chdb_set(*a, "merge_tree_allow_nullable_key", "1") == CHDBSuccess, "A: set a merge_tree_ setting");
    CHECK(get_eq(*a, "merge_tree_allow_nullable_key", "1"), "A: merge_tree_ setting reads back");
    CHECK(chdb_set(*a, "merge_tree_allow_nullable_key", NULL) == CHDBSuccess, "A: reset a merge_tree_ setting");
    {
        const char * v = chdb_get(*a, "merge_tree_allow_nullable_key");
        CHECK(!v || strcmp(v, "1") != 0, "A: merge_tree_ setting no longer 1 after reset");
    }

    /// NULL resets to the default.
    CHECK(chdb_set(*a, "max_threads", NULL) == CHDBSuccess, "A: chdb_set NULL resets");
    CHECK(get_eq(*a, "max_threads", default_threads), "A: back to the default");
    query_into(*a, "SELECT changed FROM system.settings WHERE name = 'max_threads'", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "0") == 0, "A: no longer marked changed");

    /// A change between fetches of a streaming query is kept after the stream ends.
    chdb_result * stream = chdb_stream_query(*a, "SELECT number FROM numbers(300000) SETTINGS max_block_size = 65536", "CSV");
    CHECK(stream && !chdb_result_error(stream), "A: streaming query starts");
    if (stream && !chdb_result_error(stream))
    {
        chdb_result * first = chdb_stream_fetch_result(*a, stream);
        CHECK(first && chdb_result_length(first) > 0, "A: first batch has data");
        chdb_destroy_query_result(first);
        CHECK(chdb_set(*a, "max_threads", "2") == CHDBSuccess, "A: chdb_set during a streaming query");
        for (;;)
        {
            chdb_result * batch = chdb_stream_fetch_result(*a, stream);
            int done = !batch || chdb_result_error(batch) || chdb_result_length(batch) == 0;
            chdb_destroy_query_result(batch);
            if (done)
                break;
        }
    }
    chdb_destroy_query_result(stream);
    CHECK(get_eq(*a, "max_threads", "2"), "A: change made mid-stream survives the stream");
    query_into(*a, "SELECT getSetting('max_threads')", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "2") == 0, "A: and later queries see it");

    /// Refused while a streaming insert is active.
    query_into(*a, "CREATE TABLE set_get_t (x UInt32) ENGINE = Memory", "CSV", buf, sizeof(buf));
    chdb_insert_stream ins = chdb_stream_insert(*a, "INSERT INTO set_get_t", "CSV");
    CHECK(ins && !chdb_stream_insert_error(ins), "A: streaming insert starts");
    if (ins && !chdb_stream_insert_error(ins))
    {
        CHECK(chdb_stream_append(ins, "1\n2\n", 4) == CHDBSuccess, "A: append");
        CHECK(chdb_set(*a, "max_threads", "6") == CHDBError, "A: chdb_set refused during a streaming insert");
        chdb_result * done = chdb_stream_done(ins);
        CHECK(done && !chdb_result_error(done), "A: streaming insert finishes");
        chdb_destroy_query_result(done);
    }
    chdb_destroy_insert_stream(ins);
    CHECK(get_eq(*a, "max_threads", "2"), "A: value untouched by the refused change");
    CHECK(chdb_set(*a, "max_threads", "6") == CHDBSuccess, "A: chdb_set works again after the insert");

    /// The chdb_get pointer outlives the connection it came from.
    chdb_connection * temp = chdb_connect(2, argv_a);
    const char * kept = temp ? chdb_get(*temp, "max_block_size") : NULL;
    if (temp)
        chdb_close_conn(temp);
    CHECK(kept && strcmp(kept, "4321") == 0, "chdb_get value still readable after its connection closed");

    /// --readonly=1 refuses every change, chdb_set included; reading still works.
    char * argv_ro[] = {"clickhouse", "--readonly=1"};
    chdb_connection * ro = chdb_connect(2, argv_ro);
    CHECK(ro != NULL, "readonly connection connects");
    if (ro)
    {
        CHECK(chdb_set(*ro, "max_threads", "2") == CHDBError, "readonly: chdb_set refused");
        CHECK(get_eq(*ro, "readonly", "1"), "readonly: chdb_get works");
        CHECK(get_eq(*ro, "max_threads", default_threads), "readonly: value unchanged");
        chdb_close_conn(ro);
    }

    chdb_close_conn(b);
    chdb_close_conn(a);
}

/// Reads `object` from the mock with s3(), passing the credentials as the connection's
/// {s3_key:String} / {s3_secret:String} query parameters.
static void s3_query(chdb_connection conn, const char * object, char * buf, size_t buf_size)
{
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT * FROM s3('http://127.0.0.1:%d/bucket/%s.csv', {s3_key:String}, {s3_secret:String}, 'CSV', 'a UInt32') "
             "SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 0",
             g_mock_port, object);
    query_into(conn, sql, "CSV", buf, buf_size);
}

struct overrider_args
{
    chdb_connection conn;
    const char * value; /// passed with chdb_query_with_params, or NULL for a plain query
    const char * want;
    int mismatches;
};

/// Runs SELECT {tenant:String} many times, counting answers other than `want`.
static void * overrider(void * p)
{
    struct overrider_args * args = p;
    const char * names[] = {"tenant"};
    const char * values[] = {args->value};
    char want[32];
    snprintf(want, sizeof(want), "%s\n", args->want);
    for (int i = 0; i < 100; ++i)
    {
        chdb_result * r = args->value
            ? chdb_query_with_params(args->conn, "SELECT {tenant:String}", "TSVRaw", names, values, 1)
            : chdb_query(args->conn, "SELECT {tenant:String}", "TSVRaw");
        if (!r || chdb_result_error(r) || chdb_result_length(r) != strlen(want)
            || memcmp(chdb_result_buffer(r), want, strlen(want)) != 0)
            ++args->mismatches;
        chdb_destroy_query_result(r);
    }
    return NULL;
}

static void test_connection_parameters(void)
{
    char buf[1024];

    if (mock_start() != 0)
    {
        CHECK(0, "mock S3 endpoint starts");
        return;
    }

    char * argv[] = {"clickhouse"};
    chdb_connection * a = chdb_connect(1, argv);
    chdb_connection * b = chdb_connect(1, argv);
    CHECK(a && b, "two connections for the parameter tests");
    if (!a || !b)
        return;

    /// Any value, any name: stored once, used by every later query.
    CHECK(chdb_get(*a, "param_tenant") == NULL, "A: unset parameter reads as NULL");
    CHECK(chdb_set(*a, "param_tenant", "acme") == CHDBSuccess, "A: set param_tenant");
    CHECK(get_eq(*a, "param_tenant", "acme"), "A: chdb_get reads it back");
    query_into(*a, "SELECT {tenant:String}", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "\"acme\"") == 0, "A: query uses {tenant:String}");
    query_into(*a, "SELECT concat({tenant:String}, '-', toString({tenant:String} = 'acme'))", "TSVRaw", buf, sizeof(buf));
    CHECK(strcmp(buf, "acme-1") == 0, "A: and again in a later query");
    CHECK(chdb_set(*a, "param_limit", "3") == CHDBSuccess, "A: set param_limit");
    query_into(*a, "SELECT count() FROM numbers({limit:UInt64})", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "3") == 0, "A: parameter typed as UInt64 where the query needs it");

    /// Not a setting: system.settings does not list it.
    query_into(*a, "SELECT count() FROM system.settings WHERE name LIKE '%tenant%' OR value = 'acme'", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "0") == 0, "A: parameter absent from system.settings");

    /// SET param_x and chdb_set "param_x" are the same store.
    query_into(*a, "SET param_region = 'eu-west-3'", "CSV", buf, sizeof(buf));
    CHECK(!is_error(buf), "A: SET param_region accepted");
    CHECK(get_eq(*a, "param_region", "eu-west-3"), "A: chdb_get sees a SET param_");

    /// Isolation.
    CHECK(chdb_get(*b, "param_tenant") == NULL, "B: A's parameter not visible");
    query_into(*b, "SELECT {tenant:String}", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "B: query using A's parameter fails");

    /// chdb_query_with_params overrides for one query, then the connection's value is back,
    /// and a parameter only the call supplied does not stay behind.
    const char * pnames[] = {"tenant", "extra"};
    const char * pvalues[] = {"other", "x"};
    chdb_result * r = chdb_query_with_params(*a, "SELECT {tenant:String}, {extra:String}", "CSV", pnames, pvalues, 2);
    CHECK(r && !chdb_result_error(r) && chdb_result_length(r) == 12 && memcmp(chdb_result_buffer(r), "\"other\",\"x\"\n", 12) == 0,
          "A: with_params overrides the connection's parameter for its query");
    chdb_destroy_query_result(r);
    CHECK(get_eq(*a, "param_tenant", "acme"), "A: connection value restored after with_params");
    CHECK(chdb_get(*a, "param_extra") == NULL, "A: per-query parameter does not stay behind");
    CHECK(get_eq(*a, "param_region", "eu-west-3"), "A: SET param_ survives a with_params call");
    query_into(*a, "SELECT {tenant:String}", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "\"acme\"") == 0, "A: next plain query sees the connection value");

    /// Concurrent with_params calls each see their own value, a plain query beside them sees the
    /// connection's, and the connection's is intact afterwards.
    {
        struct overrider_args args[3] = {{*a, "t1", "t1", 0}, {*a, "t2", "t2", 0}, {*a, NULL, "acme", 0}};
        pthread_t threads[3];
        for (int i = 0; i < 3; ++i)
            pthread_create(&threads[i], NULL, overrider, &args[i]);
        for (int i = 0; i < 3; ++i)
            pthread_join(threads[i], NULL);
        CHECK(args[0].mismatches == 0 && args[1].mismatches == 0, "A: each concurrent with_params call saw its own value");
        CHECK(args[2].mismatches == 0, "A: a concurrent plain query saw the connection's value");
        CHECK(get_eq(*a, "param_tenant", "acme"), "A: connection value intact after concurrent with_params calls");
    }

    /// A SET param_ run by the call itself persists, even when it writes the call's own value.
    {
        const char * n[] = {"tenant", "fresh"};
        const char * v[] = {"same", "same"};
        chdb_result * r = chdb_query_with_params(*a, "SET param_tenant = 'same', param_fresh = 'same'", "CSV", n, v, 2);
        CHECK(r && !chdb_result_error(r), "A: SET param_ through with_params succeeds");
        chdb_destroy_query_result(r);
        CHECK(get_eq(*a, "param_tenant", "same"), "A: SET of an overridden name to the override value persists");
        CHECK(get_eq(*a, "param_fresh", "same"), "A: SET of a call-only name persists");
        CHECK(chdb_set(*a, "param_tenant", "acme") == CHDBSuccess, "A: put param_tenant back");
        CHECK(chdb_set(*a, "param_fresh", NULL) == CHDBSuccess, "A: drop param_fresh");
    }

    /// NULL removes.
    CHECK(chdb_set(*a, "param_tenant", NULL) == CHDBSuccess, "A: remove param_tenant");
    CHECK(chdb_get(*a, "param_tenant") == NULL, "A: removed parameter reads as NULL");
    query_into(*a, "SELECT {tenant:String}", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "A: query using a removed parameter fails");

    /// The issue's case: S3 credentials set once, used by s3() on every query, never in the SQL.
    CHECK(chdb_set(*a, "param_s3_key", "AKIACONNECTIONA") == CHDBSuccess, "A: set param_s3_key");
    CHECK(chdb_set(*a, "param_s3_secret", "secretOfConnectionA") == CHDBSuccess, "A: set param_s3_secret");
    for (int i = 0; i < 2; ++i)
    {
        mock_reset();
        s3_query(*a, i == 0 ? "first" : "second", buf, sizeof(buf));
        CHECK(strcmp(buf, "7") == 0, "A: s3() read through the mock returns the object");
        if (strcmp(buf, "7") != 0)
            fprintf(stderr, "    got: %s\n", buf);
        CHECK(mock_saw("Credential=AKIACONNECTIONA/"), "A: request signed with the stored key");
        CHECK(!mock_saw("secretOfConnectionA"), "A: the secret itself is never sent");
    }

    /// An error does not print the stored secret.
    query_into(*a, "SELECT * FROM s3('http://127.0.0.1:1/bucket/x.csv', {s3_key:String}, {s3_secret:String}, 'CSV', 'a UInt32') "
                   "SETTINGS s3_max_single_read_retries = 0, s3_retry_attempts = 0", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "A: unreachable endpoint fails");
    CHECK(!strstr(buf, "secretOfConnectionA"), "A: error message carries no secret");

    /// B has none and cannot borrow A's; once it sets its own, each signs with its own.
    s3_query(*b, "b", buf, sizeof(buf));
    CHECK(is_error(buf), "B: s3() with unset parameters fails");
    CHECK(chdb_set(*b, "param_s3_key", "AKIACONNECTIONB") == CHDBSuccess, "B: set its own key");
    CHECK(chdb_set(*b, "param_s3_secret", "secretOfConnectionB") == CHDBSuccess, "B: set its own secret");
    mock_reset();
    s3_query(*b, "b", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "B: read with its own credentials");
    CHECK(mock_saw("Credential=AKIACONNECTIONB/") && !mock_saw("AKIACONNECTIONA"), "B: signed with B's key only");
    mock_reset();
    s3_query(*a, "a_again", buf, sizeof(buf));
    CHECK(mock_saw("Credential=AKIACONNECTIONA/"), "A: still signed with A's key");

    /// Rotating the value takes effect on the next query.
    CHECK(chdb_set(*a, "param_s3_key", "AKIAROTATEDKEY") == CHDBSuccess, "A: rotate key");
    mock_reset();
    s3_query(*a, "rotated", buf, sizeof(buf));
    CHECK(mock_saw("Credential=AKIAROTATEDKEY/"), "A: next query signs with the rotated key");

    chdb_close_conn(b);
    chdb_close_conn(a);
}

int main(void)
{
    /// Keep the SDK's fallback chain off the network: with no connection credentials an
    /// s3() request may otherwise wait on an instance metadata endpoint.
    signal(SIGPIPE, SIG_IGN);
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    unsetenv("AWS_ACCESS_KEY_ID");
    unsetenv("AWS_SECRET_ACCESS_KEY");
    unsetenv("AWS_SESSION_TOKEN");

    test_settings();
    test_connection_parameters();

    if (g_failed)
    {
        fprintf(stderr, "chdbConnSetGetTest: FAILED\n");
        return 1;
    }
    printf("chdbConnSetGetTest: OK\n");
    return 0;
}
