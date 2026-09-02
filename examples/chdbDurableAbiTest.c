/**
 * chdbDurableAbiTest.c — chdb_backup_database_n, chdb_restore_database_n and
 * chdb_classify_query_n, the three primitives a durable-object control plane
 * needs from the engine.
 *
 * The control plane lives in the binding: leases, manifests, WAL segments and
 * object storage are all above this line. What it cannot do for itself is
 * quote a database name into a BACKUP statement without risking injection, or
 * decide whether a statement its caller handed it is a read, a write it should
 * log, or something it should refuse. Both are the parser's job, so both live
 * here.
 *
 * Build: clang examples/chdbDurableAbiTest.c -I./programs/local -L. -lchdb \
 *          -o examples/chdbDurableAbiTest
 * Run:   LD_LIBRARY_PATH=. ./examples/chdbDurableAbiTest
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "chdb.h"

static int g_failed = 0;

static void fail(const char * label, const char * detail)
{
    printf("%-46s FAIL: %s\n", label, detail);
    g_failed++;
}

static void pass(const char * label)
{
    printf("%-46s ok\n", label);
}

/* Run the analysis, or report why it could not run. */
static int analyze(chdb_connection conn, const char * sql, const char * target_db, chdb_query_analysis_v1 * out)
{
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    return chdb_classify_query_n(
               conn, sql, strlen(sql), target_db, target_db ? strlen(target_db) : 0, out)
        == CHDBSuccess;
}

/* A statement's class, or a note about why we could not get one. */
static void expect_class(chdb_connection conn, const char * label, const char * sql, chdb_query_class want)
{
    chdb_query_analysis_v1 a;
    if (!analyze(conn, sql, NULL, &a))
    {
        fail(label, "chdb_classify_query_n returned CHDBError");
        return;
    }
    if (a.query_class != (uint32_t)want)
    {
        char detail[128];
        snprintf(detail, sizeof(detail), "want class %d, got %u", (int)want, a.query_class);
        fail(label, detail);
        return;
    }
    pass(label);
}

/* How many executable statements the text holds. */
static void expect_count(chdb_connection conn, const char * label, const char * sql, uint32_t want)
{
    chdb_query_analysis_v1 a;
    if (!analyze(conn, sql, NULL, &a))
    {
        fail(label, "chdb_classify_query_n returned CHDBError");
        return;
    }
    if (a.statement_count != want)
    {
        char detail[128];
        snprintf(detail, sizeof(detail), "want %u statements, got %u", want, a.statement_count);
        fail(label, detail);
        return;
    }
    pass(label);
}

/* Whether every persistent write lands in target_db. */
static void expect_contained(
    chdb_connection conn, const char * label, const char * sql, const char * target_db, int want)
{
    chdb_query_analysis_v1 a;
    if (!analyze(conn, sql, target_db, &a))
    {
        fail(label, "chdb_classify_query_n returned CHDBError");
        return;
    }
    const int got = (a.flags & CHDB_QUERY_WRITES_ONLY_TARGET_DATABASE) != 0;
    if (got != want)
    {
        char detail[160];
        snprintf(detail, sizeof(detail), "want contained=%d, got %d for `%.80s`", want, got, sql);
        fail(label, detail);
        return;
    }
    pass(label);
}

/* Whether the statement acts on a database as a whole. */
static void expect_lifecycle(chdb_connection conn, const char * label, const char * sql, int want)
{
    chdb_query_analysis_v1 a;
    if (!analyze(conn, sql, "mem", &a))
    {
        fail(label, "chdb_classify_query_n returned CHDBError");
        return;
    }
    const int got = (a.flags & CHDB_QUERY_CHANGES_DATABASE_LIFECYCLE) != 0;
    if (got != want)
    {
        char detail[160];
        snprintf(detail, sizeof(detail), "want lifecycle=%d, got %d for `%.80s`", want, got, sql);
        fail(label, detail);
        return;
    }
    pass(label);
}

/* Run a statement and require it to succeed. */
static int run(chdb_connection conn, const char * sql)
{
    chdb_result * r = chdb_query(conn, sql, "CSV");
    const char * err = r ? chdb_result_error(r) : "null result";
    if (err)
    {
        printf("setup failed for `%s`: %s\n", sql, err);
        chdb_destroy_query_result(r);
        return 0;
    }
    chdb_destroy_query_result(r);
    return 1;
}

/* Read one scalar as CSV text and compare it. */
static void expect_scalar(chdb_connection conn, const char * label, const char * sql, const char * want)
{
    chdb_result * r = chdb_query(conn, sql, "CSV");
    const char * err = r ? chdb_result_error(r) : "null result";
    if (err)
    {
        fail(label, err);
        chdb_destroy_query_result(r);
        return;
    }
    if (chdb_result_length(r) != strlen(want) || memcmp(chdb_result_buffer(r), want, strlen(want)) != 0)
    {
        char detail[256];
        snprintf(detail, sizeof(detail), "want `%s`, got `%.*s`", want, (int)chdb_result_length(r), chdb_result_buffer(r));
        fail(label, detail);
        chdb_destroy_query_result(r);
        return;
    }
    chdb_destroy_query_result(r);
    pass(label);
}

static void expect_backup_error(chdb_connection conn, const char * label, const char * db, const char * path)
{
    chdb_result * r = chdb_backup_database_n(conn, db, strlen(db), path, strlen(path), NULL, 0);
    const char * err = r ? chdb_result_error(r) : NULL;
    if (!err)
        fail(label, "expected an error, got success");
    else
        pass(label);
    chdb_destroy_query_result(r);
}


/* Every shape ParserQuery and ParserQueryWithOutput can produce, and the class
 * it must get. When upstream adds a statement type, its AST arrives here with
 * no QueryKind and no entry in the classifier, so it classifies UNKNOWN and
 * this table is what says so. */
struct statement_case
{
    /// The ParserQuery / ParserQueryWithOutput alternative that produces this
    /// statement. check_statement_coverage.py reads these and fails when a
    /// parser in the engine has no row here -- which is what makes a
    /// newly-added statement type show up as a failure rather than as an
    /// UNKNOWN in someone's log.
    const char * parser;
    const char * sql;
    chdb_query_class want;
};

static const struct statement_case k_all_statements[] = {
    /* Reads */
    {"ParserExplainQuery", "EXPLAIN SELECT 1", CHDB_QUERY_READ_ONLY},
    {"ParserSelectWithUnionQuery", "SELECT 1", CHDB_QUERY_READ_ONLY},
    {"ParserShowTablesQuery", "SHOW TABLES", CHDB_QUERY_READ_ONLY},
    {"ParserShowColumnsQuery", "SHOW COLUMNS FROM t", CHDB_QUERY_READ_ONLY},
    {"ParserShowEnginesQuery", "SHOW ENGINES", CHDB_QUERY_READ_ONLY},
    {"ParserShowFunctionsQuery", "SHOW FUNCTIONS", CHDB_QUERY_READ_ONLY},
    {"ParserShowIndexesQuery", "SHOW INDEXES FROM t", CHDB_QUERY_READ_ONLY},
    {"ParserShowSettingQuery", "SHOW SETTING max_threads", CHDB_QUERY_READ_ONLY},
    {"ParserShowProcesslistQuery", "SHOW PROCESSLIST", CHDB_QUERY_READ_ONLY},
    {"ParserShowAccessQuery", "SHOW ACCESS", CHDB_QUERY_READ_ONLY},
    {"ParserShowAccessEntitiesQuery", "SHOW USERS", CHDB_QUERY_READ_ONLY},
    {"ParserShowGrantsQuery", "SHOW GRANTS", CHDB_QUERY_READ_ONLY},
    {"ParserShowPrivilegesQuery", "SHOW PRIVILEGES", CHDB_QUERY_READ_ONLY},
    {"ParserShowCreateAccessEntityQuery", "SHOW CREATE USER u", CHDB_QUERY_READ_ONLY},
    {"ParserShowCreateAccessEntityQuery", "SHOW CREATE ROLE r", CHDB_QUERY_READ_ONLY},
    {"ParserTablePropertiesQuery", "SHOW CREATE TABLE t", CHDB_QUERY_READ_ONLY},
    {"ParserTablePropertiesQuery", "SHOW CREATE DATABASE d", CHDB_QUERY_READ_ONLY},
    {"ParserTablePropertiesQuery", "EXISTS TABLE t", CHDB_QUERY_READ_ONLY},
    {"ParserDescribeTableQuery", "DESCRIBE TABLE t", CHDB_QUERY_READ_ONLY},
    {"ParserDescribeCacheQuery", "DESCRIBE FILESYSTEM CACHE 'x'", CHDB_QUERY_READ_ONLY},
    {"ParserCheckQuery", "CHECK TABLE t", CHDB_QUERY_READ_ONLY},
    {"ParserCheckQuery", "CHECK ALL TABLES", CHDB_QUERY_READ_ONLY},

    /* Writes a checkpoint of the database carries */
    {"ParserInsertQuery", "INSERT INTO t VALUES (1)", CHDB_QUERY_MUTATING},
    {"ParserCreateQuery", "CREATE TABLE t (a Int) ENGINE=Memory", CHDB_QUERY_MUTATING},
    {"ParserAlterQuery", "ALTER TABLE t ADD COLUMN b Int", CHDB_QUERY_MUTATING},
    {"ParserAlterQuery", "ALTER DATABASE d MODIFY SETTING x = 1", CHDB_QUERY_MUTATING},
    {"ParserRenameQuery", "RENAME TABLE a TO b", CHDB_QUERY_MUTATING},
    {"ParserDropQuery", "DROP TABLE t", CHDB_QUERY_MUTATING},
    {"ParserDropQuery", "TRUNCATE TABLE t", CHDB_QUERY_MUTATING},
    {"ParserUndropQuery", "UNDROP TABLE t", CHDB_QUERY_MUTATING},
    {"ParserOptimizeQuery", "OPTIMIZE TABLE t", CHDB_QUERY_MUTATING},
    {"ParserDeleteQuery", "DELETE FROM t WHERE 1", CHDB_QUERY_MUTATING},
    {"ParserUpdateQuery", "UPDATE t SET a = 1 WHERE 1", CHDB_QUERY_MUTATING},
    {"ParserCreateIndexQuery", "CREATE INDEX i ON t (a) TYPE minmax", CHDB_QUERY_MUTATING},
    {"ParserDropIndexQuery", "DROP INDEX i ON t", CHDB_QUERY_MUTATING},
    /* An ingest path feeding rows through a table function in the SELECT
     * still writes into the database, so a checkpoint carries it. Only
     * INSERT INTO FUNCTION writes somewhere else. */
    {"ParserInsertQuery", "INSERT INTO t SELECT * FROM format(JSONEachRow, '{\"a\":1}')", CHDB_QUERY_MUTATING},
    {"ParserInsertQuery", "INSERT INTO t SELECT * FROM url('http://x', JSONEachRow)", CHDB_QUERY_MUTATING},

    /* Reaching the `system` database is global whatever the verb, because no
     * `BACKUP DATABASE` covers it. */
    {"ParserInsertQuery", "INSERT INTO system.wasm_modules VALUES ('m', 'b')", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserAlterQuery", "ALTER TABLE system.t MODIFY COLUMN c String", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropQuery", "DROP TABLE system.t", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropQuery", "TRUNCATE TABLE system.t", CHDB_QUERY_MUTATING_GLOBAL},

    /* A temporary table lives outside every database, so no backup of one
     * holds it; CHECK GRANT only evaluates and changes nothing. */
    {"ParserCreateQuery", "CREATE TEMPORARY TABLE tmp (a Int32)", CHDB_QUERY_CONTROL},
    {"ParserDropQuery", "DROP TEMPORARY TABLE tmp", CHDB_QUERY_CONTROL},
    {"ParserCheckGrantQuery", "CHECK GRANT SELECT ON *.*", CHDB_QUERY_READ_ONLY},

    /* A partition move names its destination on the command, not on the
     * statement; `SETTINGS x = DEFAULT` resets the connection's choice by
     * another spelling. */
    {"ParserAlterQuery", "ALTER TABLE t MOVE PARTITION 1 TO TABLE other.t", CHDB_QUERY_MUTATING},
    {"ParserInsertQuery", "INSERT INTO t SETTINGS async_insert = DEFAULT VALUES (1)", CHDB_QUERY_CONTROL},
    {"ParserInsertQuery", "INSERT INTO t SETTINGS mutations_sync = DEFAULT VALUES (1)", CHDB_QUERY_CONTROL},

    /* Session and server state */
    {"ParserUseQuery", "USE d", CHDB_QUERY_CONTROL},
    {"ParserSetQuery", "SET max_threads=1", CHDB_QUERY_CONTROL},
    {"ParserSetRoleQuery", "SET ROLE r", CHDB_QUERY_CONTROL},
    {"ParserSystemQuery", "SYSTEM FLUSH LOGS", CHDB_QUERY_CONTROL},
    {"ParserSystemQuery", "SYSTEM FLUSH ASYNC INSERT QUEUE", CHDB_QUERY_CONTROL},
    {"ParserKillQueryQuery", "KILL QUERY WHERE 1", CHDB_QUERY_CONTROL},
    {"ParserCreateQuery", "ATTACH TABLE t", CHDB_QUERY_CONTROL},
    {"ParserCreateQuery", "ATTACH DATABASE d", CHDB_QUERY_CONTROL},
    {"ParserDropQuery", "DETACH TABLE t", CHDB_QUERY_CONTROL},
    {"ParserBackupQuery", "BACKUP DATABASE d TO File('x')", CHDB_QUERY_CONTROL},
    {"ParserBackupQuery", "RESTORE DATABASE d FROM File('x')", CHDB_QUERY_CONTROL},
    {"ParserTransactionControl", "BEGIN TRANSACTION", CHDB_QUERY_CONTROL},
    {"ParserTransactionControl", "COMMIT", CHDB_QUERY_CONTROL},
    {"ParserTransactionControl", "ROLLBACK", CHDB_QUERY_CONTROL},
    {"ParserTransactionControl", "SET TRANSACTION SNAPSHOT 1", CHDB_QUERY_CONTROL},
    {"ParserWatchQuery", "WATCH lv", CHDB_QUERY_CONTROL},
    {"ParserExecuteAsQuery", "EXECUTE AS u SELECT 1", CHDB_QUERY_CONTROL},
    /* Partition moves touch the filesystem, not the table's content */
    {"ParserAlterQuery", "ALTER TABLE t FREEZE", CHDB_QUERY_CONTROL},
    {"ParserAlterQuery", "ALTER TABLE t MOVE PARTITION 1 TO DISK 'd'", CHDB_QUERY_CONTROL},
    /* Objects that live beside the databases, so no checkpoint holds them */
    {"ParserCreateFunctionQuery", "CREATE FUNCTION f AS (x) -> x", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropFunctionQuery", "DROP FUNCTION f", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateWorkloadQuery", "CREATE WORKLOAD w", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropWorkloadQuery", "DROP WORKLOAD w", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateResourceQuery", "CREATE RESOURCE r (WRITE DISK d)", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropResourceQuery", "DROP RESOURCE r", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateNamedCollectionQuery", "CREATE NAMED COLLECTION nc AS a = 1", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropNamedCollectionQuery", "DROP NAMED COLLECTION nc", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserAlterNamedCollectionQuery", "ALTER NAMED COLLECTION nc SET a = 2", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserHypotheticalIndexQuery", "CREATE HYPOTHETICAL INDEX hi ON t (a) TYPE minmax", CHDB_QUERY_CONTROL},
    {"ParserHypotheticalIndexQuery", "DROP ALL HYPOTHETICAL INDEXES", CHDB_QUERY_CONTROL},
    /* Access management */
    {"ParserCreateUserQuery", "CREATE USER u IDENTIFIED WITH no_password", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateRoleQuery", "CREATE ROLE r", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateQuotaQuery", "CREATE QUOTA q", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateRowPolicyQuery", "CREATE ROW POLICY p ON t", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserCreateSettingsProfileQuery", "CREATE SETTINGS PROFILE sp", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserDropAccessEntityQuery", "DROP USER u", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserMoveAccessEntityQuery", "MOVE USER u TO local_directory", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserGrantQuery", "GRANT SELECT ON *.* TO u", CHDB_QUERY_MUTATING_GLOBAL},
    {"ParserGrantQuery", "REVOKE SELECT ON *.* FROM u", CHDB_QUERY_MUTATING_GLOBAL},

    /* PARALLEL WITH takes the class of its most restricted arm */
    {"ParserParallelWithQuery", "SELECT 1 PARALLEL WITH SELECT 2", CHDB_QUERY_READ_ONLY},
    {"ParserParallelWithQuery", "INSERT INTO t VALUES (1) PARALLEL WITH SELECT 2", CHDB_QUERY_MUTATING},
    {"ParserParallelWithQuery", "SELECT 1 PARALLEL WITH USE d", CHDB_QUERY_CONTROL},

    /* Statements the table did not reach before. COPY serves the PostgreSQL
     * wire protocol and names its endpoint as an identifier, not a string. */
    {"ParserCopyQuery", "COPY t TO STDOUT", CHDB_QUERY_CONTROL},
    {"ParserCopyQuery", "COPY t FROM STDIN", CHDB_QUERY_CONTROL},
    {"ParserSnapshotQuery", "SNAPSHOT TABLE t TO Disk('d','p')", CHDB_QUERY_CONTROL},
    {"ParserUndropQuery", "UNDROP TABLE t", CHDB_QUERY_MUTATING},
    {"ParserDeleteQuery", "DELETE FROM t WHERE 1", CHDB_QUERY_MUTATING},
    {"ParserUpdateQuery", "UPDATE t SET a = 1 WHERE 1", CHDB_QUERY_MUTATING},
    /* An implicit SELECT: chdb lets a bare expression stand as a query. */
    {"ParserSelectQuery", "1 + 2", CHDB_QUERY_READ_ONLY},
};

int main(void)
{
    /* BACKUP TO File() only writes under backups.allowed_path, so a durable
     * binding sets it to the scratch directory it owns. That constraint is
     * unchanged by this API -- the engine refuses a path outside it.
     *
     * Absolute, because a relative allowed_path resolves against the data
     * directory and a relative backup path then resolves against that, which
     * is not where anyone means. A binding managing its own scratch directory
     * knows the absolute path already. */
    char cwd[512];
    if (!getcwd(cwd, sizeof(cwd)))
    {
        printf("getcwd failed\n");
        return 1;
    }

    char arg0[] = "chdb";
    char arg1[600];
    char arg2[600];
    char backups_dir[600];
    snprintf(arg1, sizeof(arg1), "--path=%s/chdb_durable_abi_test_db", cwd);
    snprintf(backups_dir, sizeof(backups_dir), "%s/chdb_durable_abi_test_backups", cwd);
    snprintf(arg2, sizeof(arg2), "--backups.allowed_path=%s", backups_dir);
    char * args[] = {arg0, arg1, arg2};
    chdb_connection * conn = chdb_connect(3, args);
    if (!conn)
    {
        printf("chdb_connect failed\n");
        return 1;
    }

    /* A database name that would break naive string concatenation: a dash
     * needs backticks, and the embedded backtick has to be doubled. */
    const char * db = "durable-obj`1";
    if (!run(*conn, "CREATE DATABASE `durable-obj``1`")
        || !run(*conn, "CREATE TABLE `durable-obj``1`.t (id UInt32, name String) ENGINE = MergeTree ORDER BY id")
        || !run(*conn, "INSERT INTO `durable-obj``1`.t VALUES (1,'a'),(2,'b'),(3,'c')"))
    {
        chdb_close_conn(conn);
        return 1;
    }

    /* 1. Classification: the four classes, decided by the parser. */
    expect_class(*conn, "1a. SELECT is READ_ONLY", "SELECT count() FROM `durable-obj``1`.t", CHDB_QUERY_READ_ONLY);
    expect_class(*conn, "1b. INSERT is MUTATING", "INSERT INTO t VALUES (4,'d')", CHDB_QUERY_MUTATING);
    expect_class(*conn, "1c. CREATE TABLE is MUTATING", "CREATE TABLE u (id UInt32) ENGINE = Memory", CHDB_QUERY_MUTATING);
    expect_class(*conn, "1d. ALTER UPDATE is MUTATING", "ALTER TABLE t UPDATE name = 'z' WHERE id = 1", CHDB_QUERY_MUTATING);
    expect_class(*conn, "1e. USE is CONTROL", "USE other", CHDB_QUERY_CONTROL);
    expect_class(*conn, "1f. SET is CONTROL", "SET max_threads = 4", CHDB_QUERY_CONTROL);
    expect_class(*conn, "1g. ATTACH is CONTROL", "ATTACH TABLE t", CHDB_QUERY_CONTROL);
    expect_class(*conn, "1h. DETACH is CONTROL", "DETACH TABLE t", CHDB_QUERY_CONTROL);
    expect_class(*conn, "1i. SYSTEM is CONTROL", "SYSTEM DROP MARK CACHE", CHDB_QUERY_CONTROL);
    expect_class(*conn, "1j. BACKUP is CONTROL", "BACKUP DATABASE d TO File('x')", CHDB_QUERY_CONTROL);
    expect_class(*conn, "1k. RESTORE is CONTROL", "RESTORE DATABASE d FROM File('x')", CHDB_QUERY_CONTROL);
    /* chdb runs with implicit_select on, so a bare expression is a SELECT.
     * Unparseable means unparseable. */
    expect_class(*conn, "1l. gibberish is UNKNOWN", "SELECT FROM WHERE ((", CHDB_QUERY_UNKNOWN);
    expect_class(*conn, "1m. empty input is UNKNOWN", "   ", CHDB_QUERY_UNKNOWN);

    /* A read that writes a file is not a read. */
    expect_class(*conn, "1n. SELECT INTO OUTFILE is CONTROL", "SELECT 1 INTO OUTFILE 'out.csv'", CHDB_QUERY_CONTROL);
    expect_class(
        *conn, "1o. INSERT INTO FUNCTION is CONTROL", "INSERT INTO FUNCTION file('o.csv') SELECT 1", CHDB_QUERY_CONTROL);

    /* Statements outside the database a checkpoint captures. */
    expect_class(
        *conn, "1p. CREATE FUNCTION is MUTATING_GLOBAL", "CREATE FUNCTION f AS (x) -> x + 1", CHDB_QUERY_MUTATING_GLOBAL);
    expect_class(
        *conn,
        "1q. CREATE USER is MUTATING_GLOBAL",
        "CREATE USER u IDENTIFIED WITH no_password",
        CHDB_QUERY_MUTATING_GLOBAL);
    expect_class(*conn, "1r. GRANT is MUTATING_GLOBAL", "GRANT SELECT ON *.* TO u", CHDB_QUERY_MUTATING_GLOBAL);
    expect_class(
        *conn,
        "1s. write into system is MUTATING_GLOBAL",
        "INSERT INTO system.wasm_modules VALUES ('m', 'bytes')",
        CHDB_QUERY_MUTATING_GLOBAL);

    /* 2. A batch is as restricted as its most restricted statement. */
    expect_class(*conn, "2a. SELECT; SELECT stays READ_ONLY", "SELECT 1; SELECT 2", CHDB_QUERY_READ_ONLY);
    expect_class(*conn, "2b. SELECT; INSERT is MUTATING", "SELECT 1; INSERT INTO t VALUES (9,'i')", CHDB_QUERY_MUTATING);
    expect_class(*conn, "2c. INSERT; USE is CONTROL", "INSERT INTO t VALUES (9,'i'); USE other", CHDB_QUERY_CONTROL);
    expect_class(*conn, "2d. SELECT; gibberish is UNKNOWN", "SELECT 1; SELECT FROM WHERE ((", CHDB_QUERY_UNKNOWN);
    expect_class(*conn, "2e. trailing `;` is not a statement", "SELECT 1;", CHDB_QUERY_READ_ONLY);
    expect_class(*conn, "2f. trailing comment is not a statement", "SELECT 1; -- done", CHDB_QUERY_READ_ONLY);
    /* Inlined data must be stepped over as data, and what follows it must
     * still be classified -- otherwise a CONTROL statement hides behind rows. */
    expect_class(
        *conn, "2g. INSERT VALUES then SELECT", "INSERT INTO t VALUES (7,'g'); SELECT 1", CHDB_QUERY_MUTATING);
    expect_class(
        *conn, "2h. USE hidden behind VALUES rows", "INSERT INTO t VALUES (7,'g'); USE other", CHDB_QUERY_CONTROL);
    expect_class(
        *conn,
        "2i. USE hidden behind FORMAT CSV rows",
        "INSERT INTO t FORMAT CSV\n7,g\n\nUSE other",
        CHDB_QUERY_CONTROL);
    expect_class(
        *conn,
        "2j. USE hidden behind EXPLAIN INSERT rows",
        "EXPLAIN INSERT INTO t VALUES (7,'g'); USE other",
        CHDB_QUERY_CONTROL);

    /* 3. Classification runs the parser, not the statement. */
    expect_scalar(*conn, "3a. classify did not run the INSERT", "SELECT count() FROM `durable-obj``1`.t", "3\n");

    /* 4. Backup and restore round-trip, with a name and a path that both need
     *    quoting and a path holding a space and an apostrophe. */
    char backup_path[700];
    snprintf(backup_path, sizeof(backup_path), "%s/it's a backup.tar.gz", backups_dir);
    {
        chdb_result * r = chdb_backup_database_n(*conn, db, strlen(db), backup_path, strlen(backup_path), NULL, 0);
        const char * err = r ? chdb_result_error(r) : "null result";
        if (err)
            fail("4a. backup an awkward name to an awkward path", err);
        else
            pass("4a. backup an awkward name to an awkward path");
        chdb_destroy_query_result(r);
    }

    /* A name and a path that are both non-ASCII. */
    const char * utf8_db = "数据库-α";
    char utf8_path[700];
    snprintf(utf8_path, sizeof(utf8_path), "%s/备份-β.tar.gz", backups_dir);
    if (!run(*conn, "CREATE DATABASE `数据库-α`")
        || !run(*conn, "CREATE TABLE `数据库-α`.`данные` (id UInt32) ENGINE = MergeTree ORDER BY id")
        || !run(*conn, "INSERT INTO `数据库-α`.`данные` VALUES (10),(20)"))
    {
        chdb_close_conn(conn);
        return 1;
    }
    {
        chdb_result * r = chdb_backup_database_n(*conn, utf8_db, strlen(utf8_db), utf8_path, strlen(utf8_path), NULL, 0);
        const char * err = r ? chdb_result_error(r) : "null result";
        if (err)
            fail("4b. backup a non-ASCII name to a non-ASCII path", err);
        else
            pass("4b. backup a non-ASCII name to a non-ASCII path");
        chdb_destroy_query_result(r);
    }

    /* 5. An existing destination is refused rather than overwritten. */
    expect_backup_error(*conn, "5a. backup refuses an existing file", db, backup_path);

    /* 6. Drop the database and restore it from the archive. RESTORE names the
     *    database as it appears in the backup, which is how a durable object
     *    recovers into a scratch directory under its own name. */
    if (!run(*conn, "DROP DATABASE `durable-obj``1`"))
    {
        chdb_close_conn(conn);
        return 1;
    }
    {
        chdb_result * r = chdb_restore_database_n(*conn, db, strlen(db), backup_path, strlen(backup_path));
        const char * err = r ? chdb_result_error(r) : "null result";
        if (err)
            fail("6a. restore the dropped database", err);
        else
            pass("6a. restore the dropped database");
        chdb_destroy_query_result(r);
    }
    expect_scalar(*conn, "6b. restored rows", "SELECT count() FROM `durable-obj``1`.t", "3\n");
    expect_scalar(
        *conn, "6c. restored values", "SELECT name FROM `durable-obj``1`.t ORDER BY id", "\"a\"\n\"b\"\n\"c\"\n");
    expect_scalar(
        *conn,
        "6d. restored schema",
        "SELECT name, type FROM system.columns WHERE database = 'durable-obj`1' AND table = 't' ORDER BY position",
        "\"id\",\"UInt32\"\n\"name\",\"String\"\n");
    /* RESTORE must not move the session onto the restored database. */
    expect_scalar(*conn, "6e. current database unchanged", "SELECT currentDatabase()", "\"default\"\n");

    /* 7. Paths the engine could not read the way the caller meant. */
    expect_backup_error(*conn, "7a. path outside allowed_path refused", db, "/tmp/chdb-durable-escape.tar.gz");
    {
        char relative[700];
        snprintf(relative, sizeof(relative), "chdb_durable_abi_test_backups/relative.tar.gz");
        expect_backup_error(*conn, "7b. relative path refused", db, relative);
    }
    {
        char missing_dir[700];
        snprintf(missing_dir, sizeof(missing_dir), "%s/no-such-dir/x.tar.gz", backups_dir);
        expect_backup_error(*conn, "7c. missing directory refused", db, missing_dir);
    }

    /* 8. An incremental backup writes only what changed since its base. */
    {
        char incr_path[700];
        snprintf(incr_path, sizeof(incr_path), "%s/incremental.tar.gz", backups_dir);
        chdb_result * r = chdb_backup_database_n(
            *conn, db, strlen(db), incr_path, strlen(incr_path), backup_path, strlen(backup_path));
        const char * err = r ? chdb_result_error(r) : "null result";
        if (err)
            fail("8a. incremental backup against a base", err);
        else
            pass("8a. incremental backup against a base");
        chdb_destroy_query_result(r);
    }
    {
        /* A base that is not there is an error, not a silent full backup. */
        char incr2[700], nobase[700];
        snprintf(incr2, sizeof(incr2), "%s/incremental2.tar.gz", backups_dir);
        snprintf(nobase, sizeof(nobase), "%s/not-a-base.tar.gz", backups_dir);
        chdb_result * r = chdb_backup_database_n(
            *conn, db, strlen(db), incr2, strlen(incr2), nobase, strlen(nobase));
        if (!chdb_result_error(r))
            fail("8b. missing base refused", "expected an error, got success");
        else
            pass("8b. missing base refused");
        chdb_destroy_query_result(r);
    }

    /* 9. Argument validation, before anything reaches the parser. */
    expect_backup_error(*conn, "9a. empty database name refused", "", backup_path);
    expect_backup_error(*conn, "9b. empty file path refused", db, "");
    {
        chdb_query_analysis_v1 a;
        memset(&a, 0, sizeof(a));
        a.struct_size = sizeof(a);
        if (chdb_classify_query_n(NULL, "SELECT 1", 8, NULL, 0, &a) != CHDBError)
            fail("9c. analysis on a null connection errors", "expected CHDBError");
        else
            pass("9c. analysis on a null connection errors");
        if (chdb_classify_query_n(*conn, "SELECT 1", 8, NULL, 0, NULL) != CHDBError)
            fail("9d. analysis without an out param errors", "expected CHDBError");
        else
            pass("9d. analysis without an out param errors");
        /* A struct_size the engine would write past is a mismatched build. */
        memset(&a, 0, sizeof(a));
        a.struct_size = sizeof(a) - 1;
        if (chdb_classify_query_n(*conn, "SELECT 1", 8, NULL, 0, &a) != CHDBError)
            fail("9g. undersized struct_size refused", "expected CHDBError");
        else
            pass("9g. undersized struct_size refused");
    }

    /* Secrets: what a caller must not write into a durable log. */
    {
        struct { const char * sql; int want; } cases[] = {
            {"CREATE NAMED COLLECTION nc AS access_key_id = 'AKIA', secret_access_key = 's3cr3t'", 1},
            {"CREATE USER u IDENTIFIED WITH sha256_password BY 'hunter2'", 1},
            {"SELECT * FROM s3('https://b/k', 'AKIA', 's3cr3t')", 1},
            {"CREATE USER u IDENTIFIED WITH no_password", 0},
            {"CREATE FUNCTION f AS (x) -> x + 1", 0},
            {"SELECT 1", 0},
            {"INSERT INTO t VALUES (1)", 0},
            /* Text that did not parse proves nothing, so it must not claim to. */
            {"SELECT FROM WHERE ((", 0},
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); i++)
        {
            chdb_query_analysis_v1 a;
            char label[96];
            snprintf(label, sizeof(label), "9e.%zu has_secrets", i + 1);
            int has_secrets = -1;
            if (!analyze(*conn, cases[i].sql, NULL, &a))
                fail(label, "chdb_classify_query_n returned CHDBError");
            else if ((has_secrets = (a.flags & CHDB_QUERY_HAS_SECRETS) != 0) != cases[i].want)
            {
                char detail[256];
                snprintf(detail, sizeof(detail), "want %d, got %d for `%.90s`", cases[i].want, has_secrets, cases[i].sql);
                fail(label, detail);
            }
            else
                pass(label);
        }
        /* One secret anywhere in a batch taints the batch. */
        chdb_query_analysis_v1 batch_a;
        const char * batch = "SELECT 1; CREATE USER u IDENTIFIED WITH sha256_password BY 'p'";
        analyze(*conn, batch, NULL, &batch_a);
        if (!(batch_a.flags & CHDB_QUERY_HAS_SECRETS))
            fail("9f. a secret anywhere taints the batch", "expected has_secrets");
        else
            pass("9f. a secret anywhere taints the batch");
    }

    /* 9h. Statement count: only a count of one is a statement a caller can
     *     replay on its own. PARALLEL WITH runs both arms. */
    expect_count(*conn, "9h.1 one statement", "SELECT 1", 1);
    expect_count(*conn, "9h.2 two statements", "SELECT 1; SELECT 2", 2);
    expect_count(*conn, "9h.3 trailing semicolon", "SELECT 1;", 1);
    expect_count(*conn, "9h.4 trailing comment", "SELECT 1; -- done", 1);
    expect_count(*conn, "9h.5 PARALLEL WITH counts arms", "SELECT 1 PARALLEL WITH SELECT 2", 2);
    expect_count(*conn, "9h.6 inlined rows are data", "INSERT INTO t VALUES (1),(2),(3)", 1);
    expect_count(*conn, "9h.7 statement behind rows", "INSERT INTO t VALUES (1); SELECT 1", 2);
    expect_count(*conn, "9h.8 unparseable is zero", "SELECT FROM WHERE ((", 0);
    expect_count(*conn, "9h.9 empty is zero", "   ", 0);

    /* 9i. Write containment. The engine resolves an unqualified name through
     *     the connection's current database before judging. */
    expect_contained(*conn, "9i.1 qualified write to target", "INSERT INTO mem.t VALUES (1)", "mem", 1);
    expect_contained(*conn, "9i.2 write to another database", "INSERT INTO other.t VALUES (1)", "mem", 0);
    expect_contained(*conn, "9i.3 write into system", "INSERT INTO system.wasm_modules VALUES ('m','b')", "mem", 0);
    expect_contained(*conn, "9i.4 INSERT INTO FUNCTION", "INSERT INTO FUNCTION file('o.csv') SELECT 1", "mem", 0);
    expect_contained(*conn, "9i.5 INTO OUTFILE", "SELECT 1 INTO OUTFILE 'o.csv'", "mem", 0);
    expect_contained(*conn, "9i.6 cross-database RENAME", "RENAME TABLE mem.a TO other.b", "mem", 0);
    expect_contained(*conn, "9i.7 in-database RENAME", "RENAME TABLE mem.a TO mem.b", "mem", 1);
    expect_contained(*conn, "9i.8 EXCHANGE across databases", "EXCHANGE TABLES mem.a AND other.b", "mem", 0);
    expect_contained(*conn, "9i.9 multi-target DROP in target", "DROP TABLE mem.a, mem.b", "mem", 1);
    expect_contained(*conn, "9i.10 multi-target DROP straying", "DROP TABLE mem.a, other.b", "mem", 0);
    expect_contained(*conn, "9i.11 ALTER in target", "ALTER TABLE mem.t ADD COLUMN c String", "mem", 1);
    expect_contained(
        *conn, "9i.14 MOVE PARTITION within target", "ALTER TABLE mem.a MOVE PARTITION 1 TO TABLE mem.b", "mem", 1);
    expect_contained(
        *conn, "9i.15 MOVE PARTITION straying", "ALTER TABLE mem.a MOVE PARTITION 1 TO TABLE other.b", "mem", 0);
    expect_contained(
        *conn, "9i.16 MOVE PARTITION into system", "ALTER TABLE mem.a MOVE PARTITION 1 TO TABLE system.b", "mem", 0);
    expect_contained(*conn, "9i.12 a read writes nothing", "SELECT 1", "mem", 1);
    expect_contained(*conn, "9i.13 no target named", "INSERT INTO mem.t VALUES (1)", NULL, 0);

    /* 9j. Database lifecycle: acting on the container, not inside it. */
    expect_lifecycle(*conn, "9j.1 CREATE DATABASE", "CREATE DATABASE d", 1);
    expect_lifecycle(*conn, "9j.2 DROP DATABASE", "DROP DATABASE d", 1);
    expect_lifecycle(*conn, "9j.3 RENAME DATABASE", "RENAME DATABASE a TO b", 1);
    expect_lifecycle(*conn, "9j.4 CREATE TABLE is not", "CREATE TABLE mem.t (a Int32) ENGINE = Memory", 0);
    expect_lifecycle(*conn, "9j.5 DROP TABLE is not", "DROP TABLE mem.t", 0);
    expect_lifecycle(*conn, "9j.6 INSERT is not", "INSERT INTO mem.t VALUES (1)", 0);

    /* 9k. A statement setting that moves a write off the connection's
     *     synchronous-completion policy belongs to the connection. */
    expect_class(
        *conn, "9k.1 async_insert override", "INSERT INTO t SETTINGS async_insert = 1 VALUES (1)", CHDB_QUERY_CONTROL);
    expect_class(
        *conn,
        "9k.2 wait_for_async_insert override",
        "INSERT INTO t SETTINGS wait_for_async_insert = 0 VALUES (1)",
        CHDB_QUERY_CONTROL);
    expect_class(
        *conn,
        "9k.3 mutations_sync override",
        "ALTER TABLE t UPDATE a = 1 WHERE 1 SETTINGS mutations_sync = 0",
        CHDB_QUERY_CONTROL);
    expect_class(
        *conn, "9k.4 an unrelated setting is fine", "INSERT INTO t SETTINGS max_threads = 4 VALUES (1)",
        CHDB_QUERY_MUTATING);
    expect_class(
        *conn, "9k.5 = DEFAULT is the same override", "INSERT INTO t SETTINGS async_insert = DEFAULT VALUES (1)",
        CHDB_QUERY_CONTROL);

    /* 10. Every top-level statement shape gets a class, and the right one. */
    {
        const size_t count = sizeof(k_all_statements) / sizeof(*k_all_statements);
        size_t wrong = 0;
        for (size_t i = 0; i < count; i++)
        {
            chdb_query_analysis_v1 a;
            if (!analyze(*conn, k_all_statements[i].sql, NULL, &a) || a.query_class != (uint32_t)k_all_statements[i].want)
            {
                printf("    %-28s %-44s want %d, got %u\n",
                       k_all_statements[i].parser, k_all_statements[i].sql,
                       (int)k_all_statements[i].want, a.query_class);
                wrong++;
            }
        }
        if (wrong)
        {
            char detail[128];
            snprintf(detail, sizeof(detail), "%zu of %zu statement shapes misclassified", wrong, count);
            fail("10a. every top-level statement shape", detail);
        }
        else
        {
            char label[128];
            snprintf(label, sizeof(label), "10a. all %zu top-level statement shapes", count);
            pass(label);
        }
    }

    /* 11. The archive outlives the connection that wrote it: close the engine,
     *     open it again on the same path, and restore there. This is the shape
     *     a durable object actually recovers in. */
    chdb_close_conn(conn);
    conn = chdb_connect(3, args);
    if (!conn)
    {
        printf("reconnect failed\n");
        return 1;
    }
    if (!run(*conn, "DROP DATABASE `数据库-α`"))
    {
        chdb_close_conn(conn);
        return 1;
    }
    {
        chdb_result * r = chdb_restore_database_n(*conn, utf8_db, strlen(utf8_db), utf8_path, strlen(utf8_path));
        const char * err = r ? chdb_result_error(r) : "null result";
        if (err)
            fail("11a. restore on a reopened connection", err);
        else
            pass("11a. restore on a reopened connection");
        chdb_destroy_query_result(r);
    }
    expect_scalar(*conn, "11b. restored rows", "SELECT sum(id) FROM `数据库-α`.`данные`", "30\n");

    /* 12. The new entry points do not disturb the engine's path model: one
     *     path per process, several connections to it, a different path only
     *     once they are all closed. */
    chdb_close_conn(conn);
    {
        char other[600];
        snprintf(other, sizeof(other), "--path=%s/chdb_durable_abi_test_db2", cwd);
        char * other_args[] = {arg0, other};

        chdb_connection * first = chdb_connect(3, args);
        chdb_connection * second = chdb_connect(3, args);
        if (!first || !second)
            fail("12a. two connections to one path", "expected both to open");
        else
            pass("12a. two connections to one path");

        /* Exercising the new ABI on both must not change what follows. */
        chdb_query_analysis_v1 a;
        if (first)
            analyze(*first, "SELECT 1", NULL, &a);
        if (second)
            analyze(*second, "SELECT 1", NULL, &a);

        chdb_connection * intruder = chdb_connect(2, other_args);
        if (intruder)
        {
            fail("12b. a second path is refused while one is open", "expected NULL");
            chdb_close_conn(intruder);
        }
        else
            pass("12b. a second path is refused while one is open");

        if (second)
            chdb_close_conn(second);
        if (first)
            chdb_close_conn(first);

        chdb_connection * after = chdb_connect(2, other_args);
        if (!after)
            fail("12c. a second path opens once the first is closed", "expected a connection");
        else
        {
            pass("12c. a second path opens once the first is closed");
            chdb_close_conn(after);
        }
    }

    if (g_failed)
    {
        printf("\n%d check(s) failed\n", g_failed);
        return 1;
    }
    printf("\nall checks passed\n");
    return 0;
}
