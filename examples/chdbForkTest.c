/**
 * chdbForkTest.c
 *
 * Regression test for POSIX fork() safety in libchdb:
 *   1. Fork BEFORE first engine init (library loaded + chdb_version() called,
 *      no connection/query yet): child initializes chDB and runs SELECT 42 cleanly.
 *   2. Parent opens connection and runs a query, keeps connection open across
 *      fork(): child exits cleanly via exit(0) (both without touching chDB and
 *      after calling chdb_close_conn()) without deadlocking in teardown/atexit.
 *   3. Parent has initialized engine -> fork(): child calls chdb_connect(),
 *      chdb_query(), chdb_stream_query(), chdb_query_cmdline(), chdb_shutdown()
 *      and fails fast with a descriptive fork error instead of hanging.
 *   4. Parent background thread actively executes a query across fork():
 *      child fails fast on chdb_query()/chdb_connect(), returns cleanly from
 *      chdb_close_conn(), and exits cleanly without deadlocking on inherited
 *      mutexes, while the parent's query completes and subsequent parent queries
 *      keep working.
 *
 * Build (against an already-built libchdb.so / libchdb.dylib):
 *   clang examples/chdbForkTest.c -I./programs/local -L. -lchdb -lpthread \
 *         -o examples/chdbForkTest
 *   LD_LIBRARY_PATH=. ./examples/chdbForkTest
 */

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "chdb.h"

#define CHILD_TIMEOUT_SEC 15

static int g_failed_assertions = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "  ASSERT FAIL: %s (%s:%d)\n",                 \
                    (msg), __FILE__, __LINE__);                            \
            g_failed_assertions += 1;                                      \
        }                                                                  \
    } while (0)

static int wait_child_with_timeout(pid_t pid, const char * label, int timeout_sec)
{
    int status = 0;
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (;;) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
                return 0;
            if (WIFEXITED(status))
                fprintf(stderr, "  FAIL [%s]: child exited with code %d\n", label, WEXITSTATUS(status));
            else if (WIFSIGNALED(status))
                fprintf(stderr, "  FAIL [%s]: child killed by signal %d\n", label, WTERMSIG(status));
            else
                fprintf(stderr, "  FAIL [%s]: unexpected wait status 0x%x\n", label, status);
            return -1;
        }
        if (r < 0) {
            if (errno == EINTR)
                continue;
            perror("waitpid");
            return -1;
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double elapsed = (double)(now.tv_sec - start.tv_sec)
                         + (double)(now.tv_nsec - start.tv_nsec) / 1e9;
        if (elapsed >= (double)timeout_sec) {
            fprintf(stderr, "  FAIL [%s]: child timed out after %ds (deadlock!)\n", label, timeout_sec);
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
            return -1;
        }

        struct timespec req = {0, 20 * 1000 * 1000}; /* 20ms */
        nanosleep(&req, NULL);
    }
}

struct bg_query_args {
    chdb_connection conn;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int started;
    int done;
    int ok;
};

static void * bg_query_thread(void * raw)
{
    struct bg_query_args * args = (struct bg_query_args *)raw;
    pthread_mutex_lock(&args->mu);
    args->started = 1;
    pthread_cond_signal(&args->cv);
    pthread_mutex_unlock(&args->mu);

    chdb_result * res = chdb_query(
        args->conn,
        "SELECT sleepEachRow(0.25) FROM numbers(12) /* chdb_fork_active_marker */",
        "CSV");
    int ok = (res != NULL && chdb_result_error(res) == NULL);
    if (res)
        chdb_destroy_query_result(res);

    pthread_mutex_lock(&args->mu);
    args->ok = ok;
    args->done = 1;
    pthread_mutex_unlock(&args->mu);
    return NULL;
}

static int query_is_active_in_process_list(chdb_connection observer_conn)
{
    /* Build the marker via concat() so this observer query never matches itself. */
    chdb_result * res = chdb_query(
        observer_conn,
        "SELECT count() FROM system.processes "
        "WHERE position(query, concat('chdb_fork_', 'active_marker')) > 0",
        "CSV");
    if (!res || chdb_result_error(res) != NULL) {
        if (res)
            chdb_destroy_query_result(res);
        return 0;
    }
    int active = 0;
    size_t len = chdb_result_length(res);
    char * buf = chdb_result_buffer(res);
    if (buf && len > 0) {
        char tmp[32];
        size_t copy_len = len < sizeof(tmp) - 1 ? len : sizeof(tmp) - 1;
        memcpy(tmp, buf, copy_len);
        tmp[copy_len] = '\0';
        active = (atoi(tmp) > 0);
    }
    chdb_destroy_query_result(res);
    return active;
}

int main(void)
{
    /* ---- Scenario 1: Fork BEFORE engine initialization ---- */
    printf("1. Fork before engine init -> child queries cleanly\n");
    const char * ver = chdb_version();
    CHECK(ver != NULL && ver[0] != '\0', "chdb_version() works before init");

    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork (scenario 1)");
        return 1;
    }
    if (pid == 0) {
        alarm(CHILD_TIMEOUT_SEC);
        char * argv[] = {"chdb", "--path=:memory:"};
        chdb_connection * conn = chdb_connect(2, argv);
        if (!conn || !*conn) {
            fprintf(stderr, "  child1: chdb_connect failed\n");
            exit(10);
        }
        chdb_result * res = chdb_query(*conn, "SELECT 42", "CSV");
        if (!res || chdb_result_error(res) != NULL) {
            fprintf(stderr, "  child1: query error: %s\n", res ? chdb_result_error(res) : "null");
            exit(11);
        }
        size_t len = chdb_result_length(res);
        char * buf = chdb_result_buffer(res);
        if (!buf || len < 2 || strncmp(buf, "42", 2) != 0) {
            fprintf(stderr, "  child1: unexpected output\n");
            exit(12);
        }
        chdb_destroy_query_result(res);
        chdb_close_conn(conn);
        exit(0);
    }
    CHECK(wait_child_with_timeout(pid, "fork-before-init", CHILD_TIMEOUT_SEC) == 0,
          "child forked before engine init runs query and exits cleanly");

    /* ---- Boot engine in parent ---- */
    printf("2. Boot engine in parent and keep connection open across fork\n");
    char * argv[] = {"chdb", "--path=:memory:"};
    chdb_connection * conn = chdb_connect(2, argv);
    CHECK(conn != NULL && *conn != NULL, "parent chdb_connect succeeds");
    if (!conn || !*conn)
        return 1;

    chdb_result * parent_res = chdb_query(*conn, "SELECT 1", "CSV");
    CHECK(parent_res != NULL && chdb_result_error(parent_res) == NULL, "parent SELECT 1 succeeds");
    if (parent_res)
        chdb_destroy_query_result(parent_res);

    /* ---- Scenario 2a: Child exits without touching chDB (atexit / static destructors) ---- */
    printf("2a. Fork after init -> child exits without calling chDB\n");
    fflush(NULL);
    pid = fork();
    if (pid < 0) {
        perror("fork (scenario 2a)");
        return 1;
    }
    if (pid == 0) {
        alarm(CHILD_TIMEOUT_SEC);
        exit(0);
    }
    CHECK(wait_child_with_timeout(pid, "child-exit-idle", CHILD_TIMEOUT_SEC) == 0,
          "child exits cleanly without deadlock in atexit/static destructors");

    /* ---- Scenario 2b: Child closes inherited connection and exits ---- */
    printf("2b. Fork after init -> child calls chdb_close_conn and exits\n");
    fflush(NULL);
    pid = fork();
    if (pid < 0) {
        perror("fork (scenario 2b)");
        return 1;
    }
    if (pid == 0) {
        alarm(CHILD_TIMEOUT_SEC);
        chdb_close_conn(conn);
        exit(0);
    }
    CHECK(wait_child_with_timeout(pid, "child-close-and-exit", CHILD_TIMEOUT_SEC) == 0,
          "child calling chdb_close_conn exits cleanly without deadlock");

    /* ---- Scenario 3: Child attempts connect / query / stream / cmdline / shutdown -> fail fast ---- */
    printf("3. Fork after init -> child API calls fail fast with fork error\n");
    fflush(NULL);
    pid = fork();
    if (pid < 0) {
        perror("fork (scenario 3)");
        return 1;
    }
    if (pid == 0) {
        alarm(CHILD_TIMEOUT_SEC);

        chdb_connection * c2 = chdb_connect(2, argv);
        if (c2 != NULL) {
            fprintf(stderr, "  child3: chdb_connect unexpectedly succeeded\n");
            exit(20);
        }

        chdb_result * r1 = chdb_query(*conn, "SELECT 1", "CSV");
        if (!r1 || !chdb_result_error(r1) || !strstr(chdb_result_error(r1), "fork")) {
            fprintf(stderr, "  child3: chdb_query missing fork error: %s\n",
                    r1 && chdb_result_error(r1) ? chdb_result_error(r1) : "(none)");
            exit(21);
        }
        chdb_destroy_query_result(r1);

        chdb_result * r2 = chdb_stream_query(*conn, "SELECT 1", "CSV");
        if (!r2 || !chdb_result_error(r2) || !strstr(chdb_result_error(r2), "fork")) {
            fprintf(stderr, "  child3: chdb_stream_query missing fork error: %s\n",
                    r2 && chdb_result_error(r2) ? chdb_result_error(r2) : "(none)");
            exit(22);
        }
        chdb_stream_cancel_query(*conn, r2);
        chdb_destroy_query_result(r2);

        char * cmd_argv[] = {"clickhouse", "--query=SELECT 1", "--output-format=CSV"};
        chdb_result * r3 = chdb_query_cmdline(3, cmd_argv);
        if (!r3 || !chdb_result_error(r3) || !strstr(chdb_result_error(r3), "fork")) {
            fprintf(stderr, "  child3: chdb_query_cmdline missing fork error: %s\n",
                    r3 && chdb_result_error(r3) ? chdb_result_error(r3) : "(none)");
            exit(23);
        }
        chdb_destroy_query_result(r3);

        if (chdb_shutdown() != CHDBError) {
            fprintf(stderr, "  child3: chdb_shutdown unexpectedly succeeded\n");
            exit(24);
        }

        exit(0);
    }
    CHECK(wait_child_with_timeout(pid, "child-fail-fast", CHILD_TIMEOUT_SEC) == 0,
          "child fails fast on all C ABI entry points after fork");

    /* ---- Scenario 4: Fork while background thread holds client_mutex & runs query ---- */
    printf("4. Fork during active background query -> child fails fast & exits cleanly\n");
    chdb_connection * observer_conn = chdb_connect(2, argv);
    CHECK(observer_conn != NULL && *observer_conn != NULL, "observer chdb_connect succeeds");
    if (!observer_conn || !*observer_conn) {
        chdb_close_conn(conn);
        return 1;
    }

    struct bg_query_args bg;
    bg.conn = *conn;
    bg.started = 0;
    bg.done = 0;
    bg.ok = 0;
    pthread_mutex_init(&bg.mu, NULL);
    pthread_cond_init(&bg.cv, NULL);

    pthread_t th;
    int thread_rc = pthread_create(&th, NULL, bg_query_thread, &bg);
    CHECK(thread_rc == 0, "spawn background query thread");
    if (thread_rc != 0) {
        pthread_mutex_destroy(&bg.mu);
        pthread_cond_destroy(&bg.cv);
        chdb_close_conn(observer_conn);
        chdb_close_conn(conn);
        return 1;
    }

    pthread_mutex_lock(&bg.mu);
    while (!bg.started)
        pthread_cond_wait(&bg.cv, &bg.mu);
    pthread_mutex_unlock(&bg.mu);

    /* Wait until system.processes confirms bg_query_thread is actively executing
     * inside ChdbClient::executeMaterializedQuery (holding conn's client_mutex). */
    int observed_active = 0;
    struct timespec wait_start;
    clock_gettime(CLOCK_MONOTONIC, &wait_start);
    for (;;) {
        if (query_is_active_in_process_list(*observer_conn)) {
            observed_active = 1;
            break;
        }
        pthread_mutex_lock(&bg.mu);
        int bg_done = bg.done;
        pthread_mutex_unlock(&bg.mu);
        if (bg_done)
            break;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double elapsed = (double)(now.tv_sec - wait_start.tv_sec)
                         + (double)(now.tv_nsec - wait_start.tv_nsec) / 1e9;
        if (elapsed >= 5.0)
            break;

        struct timespec poll_pause = {0, 10 * 1000 * 1000}; /* 10ms */
        nanosleep(&poll_pause, NULL);
    }
    CHECK(observed_active == 1, "background query observed active in system.processes before fork");

    pthread_mutex_lock(&bg.mu);
    int running_before_fork = !bg.done;
    pthread_mutex_unlock(&bg.mu);
    CHECK(running_before_fork == 1, "background query still running right before fork");

    fflush(NULL);
    pid = fork();
    if (pid < 0) {
        perror("fork (scenario 4)");
        pthread_join(th, NULL);
        pthread_mutex_destroy(&bg.mu);
        pthread_cond_destroy(&bg.cv);
        chdb_close_conn(observer_conn);
        chdb_close_conn(conn);
        return 1;
    }
    if (pid == 0) {
        alarm(CHILD_TIMEOUT_SEC);

        chdb_result * r = chdb_query(*conn, "SELECT 1", "CSV");
        if (!r || !chdb_result_error(r) || !strstr(chdb_result_error(r), "fork")) {
            fprintf(stderr, "  child4: chdb_query missing fork error\n");
            exit(30);
        }
        chdb_destroy_query_result(r);

        if (chdb_connect(2, argv) != NULL) {
            fprintf(stderr, "  child4: chdb_connect unexpectedly succeeded\n");
            exit(31);
        }

        chdb_close_conn(conn);
        exit(0);
    }

    pthread_mutex_lock(&bg.mu);
    int running_after_fork = !bg.done;
    pthread_mutex_unlock(&bg.mu);
    CHECK(running_after_fork == 1, "background query remained active across fork");

    CHECK(wait_child_with_timeout(pid, "fork-during-active-query", CHILD_TIMEOUT_SEC) == 0,
          "child forked during active background query fails fast and exits cleanly");

    pthread_join(th, NULL);
    pthread_mutex_destroy(&bg.mu);
    pthread_cond_destroy(&bg.cv);
    chdb_close_conn(observer_conn);
    CHECK(bg.ok == 1, "parent background query succeeded");

    /* Parent connection still works after all forks */
    chdb_result * after_res = chdb_query(*conn, "SELECT 99", "CSV");
    CHECK(after_res != NULL && chdb_result_error(after_res) == NULL
              && chdb_result_length(after_res) >= 2
              && strncmp(chdb_result_buffer(after_res), "99", 2) == 0,
          "parent connection still works after forks");
    if (after_res)
        chdb_destroy_query_result(after_res);

    chdb_close_conn(conn);

    if (g_failed_assertions != 0) {
        fprintf(stderr, "FAILED: %d assertion(s) failed\n", g_failed_assertions);
        return 1;
    }
    printf("PASSED: all fork safety checks succeeded\n");
    return 0;
}
