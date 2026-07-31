# chDB ADBC conformance suite

Runs the Apache Arrow ADBC conformance suite against a built `libchdb`, to check
the driver in `../chdb-adbc.cpp` against the ADBC contract rather than against
our own expectations.

The ~100 test cases are upstream's. All that lives here is:

| File | |
|---|---|
| `chdb_validation.cc` | the adapter: how to open a chDB database, and which parts of the contract chDB claims |
| `CMakeLists.txt` | standalone build, fetching the suite from arrow-adbc |
| `run.sh` | runs the suite with one test case per process |

There is a second suite next to it: `python/` runs the ADBC Driver Foundry's
[validation framework](https://github.com/adbc-drivers/validation), which checks
SQL-level behaviour — dialect, type round-trips, metadata, bulk ingest — rather
than the C contract. Both read `CHDB_ADBC_DRIVER` and both run in the same CI
job. See `python/README.md`.

## Running it

```bash
cmake -S programs/local/adbc/validation -B build/adbc-validation
cmake --build build/adbc-validation --parallel

CHDB_ADBC_DRIVER=/path/to/libchdb.so \
  programs/local/adbc/validation/run.sh build/adbc-validation
```

`CHDB_ADBC_DRIVER` is the library to validate; it defaults to letting the
dynamic loader find `libchdb.so`. `CHDB_ADBC_URI` overrides the database location
(default `chdb://`, in-memory) and `CHDB_ADBC_ENTRYPOINT` the init symbol
(default `chdb_adbc_init`).

## Two things about the setup

**It is not part of the ClickHouse build.** The suite loads the driver by path
through the ADBC driver manager, so this binary is never linked against
libchdb — it needs no ClickHouse target and no ClickHouse build flags, and
`ENABLE_TESTS` stays off. Configure it on its own, in seconds. The upside beyond
build time: it can validate *any* libchdb, including one downloaded from a
release, not just one from the local build tree.

**One process per test case.** chDB keeps a single engine per process and does
not support shutting that engine down and starting another one in the same
process. The suite opens and releases a database around every test case, so
running the binary directly puts ~100 engine lifecycles through one process.
`run.sh` gives each case its own process instead. This costs almost nothing: the
engine starts in ~30 ms and the library stays warm in the page cache, so a full
run takes about 6 seconds.

## In CI

Each of the four build workflows runs this suite and the one in `python/`
against the libchdb it just built, in a step named `ADBC validation suites`.
That covers linux x86_64 and aarch64 and macOS arm64 and x86_64, and it sees a
change to `../chdb-adbc.cpp` on the pull request that makes it.

The step sits right after the build rather than at the end of the job, so an
unrelated failure in the wheel tests cannot withhold the result. Building this
suite takes about two minutes; running it, seconds.

## Current state

```
100 tests: 73 passed, 27 skipped, 0 failed
```

Identical on Linux and macOS.

**The skips are things ClickHouse has no equivalent for**, declared either as a
`supports_*` returning `false` or as an explicit skip with a reason: transactions,
cancellation, statistics, partitioned data, catalogs, Arrow duration and interval
types, nullable `Array` columns, and dictionaries with duplicate values. Declining
a capability by skipping is what the suite expects, so these do not hide failures
— anything genuinely broken still fails the run.

Two of them are worth spelling out. `SqlQueryInts` and `SqlPrepareSelectNoParams`
read `SELECT 42` and `SELECT 1` and accept only signed 32- or 64-bit integers,
while ClickHouse gives an integer literal the narrowest type that holds it:

```
SELECT 42 -> 42: uint8 not null
```

The suite has no hook to say so — its type switch ends in an unconditional
failure — and reporting a wider type from the driver would misstate what the
engine returned. So the mismatch is declared here, and integer round-tripping
stays covered by `../../../tests/test_adbc_driver.py`.

## What the adapter has to say

Most of `chdb_validation.cc` is dialect, not capability flags:

| Hook | Why |
|---|---|
| `QuoteIdentifier` | ClickHouse quotes with backticks |
| `RewriteSql` | ClickHouse emits one block per part, so reads that the suite expects in a single batch get `SETTINGS max_threads = 1` and an `ORDER BY`; and columns need explicit `Nullable(...)` |
| `IngestSelectRoundTripType` | `half_float` reads back as `float`, and large/view strings and all binary types as `String` |
| `ValidateIngestedTemporalData` | `DateTime64` keeps the ingested unit and epoch values verbatim |
| `DropTable` / `EnsureDbSchema` | a ClickHouse "database" is the schema level |

## Upgrading the suite

`CHDB_ADBC_TAG` in `CMakeLists.txt` pins the arrow-adbc release the suite is
built from. It is independent of the tag recorded in `../README.md` for the
vendored `../adbc.h`: the suite brings its own copy of `adbc.h`, and needs a
release whose `DriverQuirks` exposes the dialect hooks used here
(`QuoteIdentifier`, `RewriteSql`), which landed after the tag that header
records.

Nothing from arrow-adbc is vendored here — CMake fetches it, and upstream's own
build resolves its dependencies (nanoarrow and fmt ship inside it, GoogleTest is
fetched, the driver manager is built alongside). Every driver backend stays off,
so only the validation objects and the driver manager get compiled.
