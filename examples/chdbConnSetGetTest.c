/**
 * chdbConnSetGetTest.c
 *
 * Connection-level settings through the C API (chdb-io/chdb-core#17):
 * chdb_set / chdb_get change and read a setting of an open connection the way
 * `SET key = value` would, and carry S3 credentials that the s3() table
 * function signs with, so they never have to be pasted into SQL.
 *
 * Covers:
 *   - chdb_get of a default and of a value given at connect time
 *   - chdb_set before the first query and after queries have run, observed
 *     through chdb_get, getSetting(), system.settings and query behavior
 *     (including a format setting)
 *   - isolation between two connections on the same path
 *   - a query's SETTINGS clause overriding the value without changing it
 *   - a SET statement observed by chdb_get
 *   - NULL value resetting to the default
 *   - unknown keys, invalid values, NULL arguments, a --readonly=1 connection
 *     and an active streaming insert all refused, leaving the value as it was
 *   - a change made between fetches of a streaming query surviving the stream
 *   - the chdb_get pointer outliving the connection it was read from
 *   - S3 credentials: readable through chdb_get, absent from system.settings,
 *     used to sign s3() requests (checked against a local mock endpoint that
 *     records each request's Authorization header), overridden by credentials
 *     written in the query, isolated per connection, and cleared by NULL
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

static int mock_request_count(void)
{
    pthread_mutex_lock(&g_mock_mutex);
    int n = g_mock_requests;
    pthread_mutex_unlock(&g_mock_mutex);
    return n;
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

static void s3_query(chdb_connection conn, const char * object, char * buf, size_t buf_size)
{
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT * FROM s3('http://127.0.0.1:%d/bucket/%s.csv', 'CSV', 'a UInt32') "
             "SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 0",
             g_mock_port, object);
    query_into(conn, sql, "CSV", buf, buf_size);
}

static void test_s3_credentials(void)
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
    CHECK(a && b, "two connections for the S3 tests");
    if (!a || !b)
        return;

    /// Unset keys read as empty.
    CHECK(get_eq(*a, "s3_access_key_id", ""), "unset s3_access_key_id reads as empty");
    CHECK(get_eq(*a, "s3_secret_access_key", ""), "unset s3_secret_access_key reads as empty");
    CHECK(get_eq(*a, "s3_session_token", ""), "unset s3_session_token reads as empty");
    CHECK(get_eq(*a, "s3_region", ""), "unset s3_region reads as empty");

    CHECK(chdb_set(*a, "s3_access_key_id", "AKIACONNECTIONA") == CHDBSuccess, "A: set access key id");
    CHECK(chdb_set(*a, "s3_secret_access_key", "secretOfConnectionA") == CHDBSuccess, "A: set secret");
    CHECK(chdb_set(*a, "s3_session_token", "tokenOfConnectionA") == CHDBSuccess, "A: set session token");
    CHECK(chdb_set(*a, "s3_region", "eu-west-3") == CHDBSuccess, "A: set region");
    CHECK(get_eq(*a, "s3_access_key_id", "AKIACONNECTIONA"), "A: chdb_get access key id");
    CHECK(get_eq(*a, "s3_secret_access_key", "secretOfConnectionA"), "A: chdb_get secret");
    CHECK(get_eq(*a, "s3_session_token", "tokenOfConnectionA"), "A: chdb_get session token");
    CHECK(get_eq(*a, "s3_region", "eu-west-3"), "A: chdb_get region");
    CHECK(get_eq(*b, "s3_access_key_id", ""), "B: credentials of A not visible");

    /// Not settings: SQL cannot see them.
    query_into(*a, "SELECT count() FROM system.settings WHERE name LIKE 's3_access_key%' OR name LIKE 's3_secret%' "
                   "OR value LIKE '%ConnectionA%' OR value LIKE '%CONNECTIONA%'", "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "0") == 0, "A: credentials absent from system.settings");
    query_into(*a, "SELECT getSetting('s3_access_key_id')", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "A: getSetting cannot read a credential");

    /// A bare URL is signed with the connection's credentials, and the read succeeds.
    mock_reset();
    s3_query(*a, "data", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "A: s3() read through the mock returns the object");
    if (strcmp(buf, "7") != 0)
        fprintf(stderr, "    got: %s\n", buf);
    CHECK(mock_request_count() > 0, "A: the mock was reached");
    CHECK(mock_saw("Credential=AKIACONNECTIONA/"), "A: request signed with the connection's access key");
    CHECK(mock_saw("/eu-west-3/s3/aws4_request"), "A: signature scoped to the connection's region");
    CHECK(mock_saw("tokenOfConnectionA"), "A: session token sent");
    CHECK(!mock_saw("secretOfConnectionA"), "A: the secret itself is never sent");

    /// The error path does not print credentials either.
    query_into(*a, "SELECT * FROM s3('http://127.0.0.1:1/bucket/x.csv', 'CSV', 'a UInt32') "
                   "SETTINGS s3_max_single_read_retries = 0, s3_retry_attempts = 0", "CSV", buf, sizeof(buf));
    CHECK(is_error(buf), "A: unreachable endpoint fails");
    CHECK(!strstr(buf, "secretOfConnectionA") && !strstr(buf, "tokenOfConnectionA"), "A: error message carries no secret");

    /// Credentials in the query win.
    mock_reset();
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT * FROM s3('http://127.0.0.1:%d/bucket/inline.csv', 'AKIAINLINEKEY', 'inlineSecret', 'CSV', 'a UInt32')",
             g_mock_port);
    query_into(*a, sql, "CSV", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "A: s3() with inline credentials reads");
    CHECK(mock_saw("Credential=AKIAINLINEKEY/"), "A: inline key used");
    CHECK(!mock_saw("Credential=AKIACONNECTIONA/"), "A: connection key not used when the query has its own");
    CHECK(!mock_saw("tokenOfConnectionA"), "A: connection token not paired with the query's keys");

    /// Another connection does not borrow them.
    mock_reset();
    s3_query(*b, "other", buf, sizeof(buf));
    CHECK(mock_request_count() > 0, "B: the mock was reached");
    CHECK(!mock_saw("AKIACONNECTIONA"), "B: not signed with A's key");
    CHECK(!mock_saw("tokenOfConnectionA"), "B: A's token not sent");

    /// B sets its own.
    CHECK(chdb_set(*b, "s3_access_key_id", "AKIACONNECTIONB") == CHDBSuccess, "B: set its own key");
    CHECK(chdb_set(*b, "s3_secret_access_key", "secretOfConnectionB") == CHDBSuccess, "B: set its own secret");
    mock_reset();
    s3_query(*b, "b", buf, sizeof(buf));
    CHECK(strcmp(buf, "7") == 0, "B: read with its own credentials");
    CHECK(mock_saw("Credential=AKIACONNECTIONB/"), "B: signed with B's key");
    mock_reset();
    s3_query(*a, "a_again", buf, sizeof(buf));
    CHECK(mock_saw("Credential=AKIACONNECTIONA/"), "A: still signed with A's key after B set its own");

    /// Changing the key between queries takes effect on the next one.
    CHECK(chdb_set(*a, "s3_access_key_id", "AKIAROTATEDKEY") == CHDBSuccess, "A: rotate key");
    mock_reset();
    s3_query(*a, "rotated", buf, sizeof(buf));
    CHECK(mock_saw("Credential=AKIAROTATEDKEY/"), "A: next query signs with the rotated key");

    /// NULL clears.
    CHECK(chdb_set(*a, "s3_access_key_id", NULL) == CHDBSuccess, "A: clear key");
    CHECK(chdb_set(*a, "s3_secret_access_key", NULL) == CHDBSuccess, "A: clear secret");
    CHECK(chdb_set(*a, "s3_session_token", NULL) == CHDBSuccess, "A: clear token");
    CHECK(get_eq(*a, "s3_access_key_id", ""), "A: cleared key reads as empty");
    mock_reset();
    s3_query(*a, "cleared", buf, sizeof(buf));
    CHECK(mock_request_count() > 0, "A: the mock was reached after clearing");
    CHECK(!mock_saw("AKIAROTATEDKEY") && !mock_saw("AKIACONNECTIONA"), "A: no longer signed with a cleared key");

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
    test_s3_credentials();

    if (g_failed)
    {
        fprintf(stderr, "chdbConnSetGetTest: FAILED\n");
        return 1;
    }
    printf("chdbConnSetGetTest: OK\n");
    return 0;
}
