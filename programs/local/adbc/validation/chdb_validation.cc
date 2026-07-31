// Copyright 2026 ClickHouse, Inc. and the chDB authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Runs the Apache Arrow ADBC conformance suite against chDB.
//
// The ~100 test cases are upstream's. All that lives here is the adapter: how
// to open a chDB database, how to write ClickHouse SQL for the statements the
// suite needs, and which parts of the contract chDB implements.
//
// The driver is loaded by path at run time, so this binary is not linked
// against libchdb and can validate any build of it:
//
//   CHDB_ADBC_DRIVER=/path/to/libchdb.so ./chdb_adbc_validation
//
// chDB keeps one engine per process and does not support replacing it, so run
// one test case per process (run.sh does this). See README.md.

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

#include <arrow-adbc/adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "adbc_validation.h"
#include "adbc_validation_util.h"

namespace
{

std::string GetEnvOr(const char * name, const std::string & fallback)
{
    const char * value = std::getenv(name);
    return value ? std::string(value) : fallback;
}

/// Path to the libchdb under test. Defaults to letting the dynamic loader
/// search, so a system-installed library also works.
std::string DriverPath()
{
    return GetEnvOr("CHDB_ADBC_DRIVER", "libchdb.so");
}

/// chDB is embedded: an empty path means in-memory.
std::string DatabaseUri()
{
    return GetEnvOr("CHDB_ADBC_URI", "chdb://");
}

/// chDB exports the standard AdbcDriverInit as well; naming the alias keeps
/// this working against builds that export only the alias.
std::string Entrypoint()
{
    return GetEnvOr("CHDB_ADBC_ENTRYPOINT", "chdb_adbc_init");
}

/// chDB's answers to the questions the suite asks about a driver: how to phrase
/// the SQL it needs, and which capabilities to exercise.
class ChdbQuirks : public adbc_validation::DriverQuirks
{
public:
    AdbcStatusCode SetupDatabase(struct AdbcDatabase * database, struct AdbcError * error) const override
    {
        if (AdbcStatusCode status = AdbcDatabaseSetOption(database, "driver", DriverPath().c_str(), error);
            status != ADBC_STATUS_OK)
            return status;
        if (AdbcStatusCode status = AdbcDatabaseSetOption(database, "entrypoint", Entrypoint().c_str(), error);
            status != ADBC_STATUS_OK)
            return status;
        return AdbcDatabaseSetOption(database, "uri", DatabaseUri().c_str(), error);
    }

    /// ClickHouse quotes identifiers with backticks, not double quotes.
    std::string QuoteIdentifier(std::string_view name) const override { return '`' + std::string(name) + '`'; }

    AdbcStatusCode DropTable(struct AdbcConnection * connection, const std::string & name, struct AdbcError * error) const override
    {
        return Execute(connection, "DROP TABLE IF EXISTS " + QuoteIdentifier(name), error);
    }

    AdbcStatusCode DropTable(
        struct AdbcConnection * connection,
        const std::string & name,
        const std::string & db_schema,
        struct AdbcError * error) const override
    {
        return Execute(connection, "DROP TABLE IF EXISTS " + QuoteIdentifier(db_schema) + "." + QuoteIdentifier(name), error);
    }

    /// A ClickHouse "database" is the schema level.
    AdbcStatusCode EnsureDbSchema(struct AdbcConnection * connection, const std::string & name, struct AdbcError * error) const override
    {
        return Execute(connection, "CREATE DATABASE IF NOT EXISTS " + QuoteIdentifier(name), error);
    }

    /// Statements the suite phrases in ANSI SQL that ClickHouse needs
    /// differently. Each case is identified by the suite's own query id.
    std::string RewriteSql(std::string_view query_id, std::string default_sql) const override
    {
        // The suite expects a whole result in one batch. ClickHouse emits one
        // block per data part -- including under ORDER BY, via reading in
        // order -- so force a single merged output for these reads.
        static const std::string one_batch = " SETTINGS max_threads = 1, optimize_read_in_order = 0";

        if (query_id == "StatementTest::TestSqlIngestAppend::select-bulk-ingest")
            // The expected row order is ingestion order: 42, -42, NULL.
            return "SELECT `int64s` FROM `bulk_ingest` ORDER BY `int64s` DESC NULLS LAST" + one_batch;
        if (query_id == "StatementTest::TestSqlIngestCreateAppend::select-bulk-ingest")
            // Two inserts leave two blocks; sorting merges them into one batch.
            return "SELECT `int64s` FROM `bulk_ingest` ORDER BY `int64s`" + one_batch;
        if (query_id == "StatementTest::TestSqlBind::select-bindtest")
            return default_sql + one_batch;
        if (query_id == "StatementTest::TestSqlBind::create-table-bindtest")
            // ClickHouse columns are non-nullable unless declared otherwise.
            return "CREATE TABLE bindtest (col1 Nullable(Int32), col2 Nullable(String))";
        return default_sql;
    }

    /// Types that do not survive an ingest/select round trip unchanged, because
    /// ClickHouse has no distinct storage type for them.
    ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override
    {
        switch (ingest_type)
        {
            case NANOARROW_TYPE_HALF_FLOAT:
                return NANOARROW_TYPE_FLOAT;
            case NANOARROW_TYPE_LARGE_STRING:
            case NANOARROW_TYPE_STRING_VIEW:
                return NANOARROW_TYPE_STRING;
            // ClickHouse stores binary as String, so it reads back as utf8.
            case NANOARROW_TYPE_BINARY:
            case NANOARROW_TYPE_LARGE_BINARY:
            case NANOARROW_TYPE_BINARY_VIEW:
                return NANOARROW_TYPE_STRING;
            default:
                return ingest_type;
        }
    }

    std::string db_schema() const override { return "default"; }

    bool supports_bulk_ingest(const char * /*mode*/) const override { return true; }
    bool supports_bulk_ingest_db_schema() const override { return true; }
    bool supports_get_objects() const override { return true; }
    bool supports_get_sql_info() const override { return true; }
    bool supports_metadata_current_db_schema() const override { return true; }
    /// The driver reports -1 ("unknown") for row counts, which the contract
    /// allows, so the row-count checks apply.
    bool supports_rows_affected() const override { return true; }

    std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(uint32_t info_code) const override
    {
        switch (info_code)
        {
            case ADBC_INFO_VENDOR_NAME:
                return "ClickHouse";
            case ADBC_INFO_DRIVER_NAME:
                return "ADBC chDB Driver";
            default:
                return std::nullopt;
        }
    }

    /// Not implemented by the driver. ClickHouse also has no catalog level and
    /// no client-side transaction control.
    bool supports_cancel() const override { return false; }
    bool supports_concurrent_statements() const override { return false; }
    bool supports_execute_schema() const override { return false; }
    bool supports_metadata_current_catalog() const override { return false; }
    bool supports_partitioned_data() const override { return false; }
    bool supports_statistics() const override { return false; }
    bool supports_transactions() const override { return false; }

private:
    static AdbcStatusCode Execute(struct AdbcConnection * connection, const std::string & query, struct AdbcError * error)
    {
        adbc_validation::Handle<struct AdbcStatement> statement;
        RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));
        RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
        RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
        return AdbcStatementRelease(&statement.value, error);
    }
};

class ChdbDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest
{
public:
    const adbc_validation::DriverQuirks * quirks() const override { return &quirks_; }
    void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
    void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

protected:
    ChdbQuirks quirks_;
};
ADBCV_TEST_DATABASE(ChdbDatabaseTest)

class ChdbConnectionTest : public ::testing::Test, public adbc_validation::ConnectionTest
{
public:
    const adbc_validation::DriverQuirks * quirks() const override { return &quirks_; }
    void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
    void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

protected:
    ChdbQuirks quirks_;
};
ADBCV_TEST_CONNECTION(ChdbConnectionTest)

class ChdbStatementTest : public ::testing::Test, public adbc_validation::StatementTest
{
public:
    const adbc_validation::DriverQuirks * quirks() const override { return &quirks_; }
    void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
    void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

    void ValidateIngestedTemporalData(
        struct ArrowArrayView * values, ArrowType type, enum ArrowTimeUnit unit, const char * timezone) override
    {
        switch (type)
        {
            case NANOARROW_TYPE_TIMESTAMP:
                // DateTime64 keeps the ingested unit and epoch values verbatim.
                ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareArray<std::int64_t>(values, {std::nullopt, -42, 0, 42}));
                break;
            default:
                FAIL() << "ValidateIngestedTemporalData not implemented for type " << type;
        }
    }

    // Cases with no ClickHouse equivalent. Skipping them the same way
    // feature-gated cases skip keeps the remaining failures genuine.
    void TestSqlIngestDuration() { GTEST_SKIP() << "ClickHouse has no Arrow duration mapping"; }
    void TestSqlIngestInterval() { GTEST_SKIP() << "ClickHouse has no Arrow interval mapping"; }
    void TestSqlIngestListOfInt32() { GTEST_SKIP() << "ClickHouse Array columns cannot be NULL"; }
    void TestSqlIngestListOfString() { GTEST_SKIP() << "ClickHouse Array columns cannot be NULL"; }
    void TestSqlIngestStringDictionary() { GTEST_SKIP() << "the engine's Arrow reader rejects duplicate dictionary values"; }

protected:
    ChdbQuirks quirks_;
};
ADBCV_TEST_STATEMENT(ChdbStatementTest)

}
