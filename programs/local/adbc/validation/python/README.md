# chDB ADBC driver — Foundry validation suite

Runs the [adbc-drivers/validation][framework] suite against a built `libchdb`.
It is the harness the ADBC Driver Foundry uses to produce its per-driver
capability matrices.

It complements the gtest suite in `../`: that one checks the C contract from
arrow-adbc, this one checks SQL-level behaviour — dialect, type round-trips,
metadata and bulk ingest — through the Python driver manager.

[framework]: https://github.com/adbc-drivers/validation

| File | |
|---|---|
| `tests/chdb.py` | the quirks: which capabilities chDB claims, and its dialect |
| `tests/engine_version.py` | the engine version pin (see below) |
| `tests/test_*.py` | thin subclasses of the framework's test classes |
| `tests/generate_documentation.py` | renders the capability matrix from a run |
| `docs/chdb.md` | the matrix template: prose ours, tables generated |
| `queries/` | dialect overrides for upstream's `.txtcase` cases |
| `gen_overrides.py` | regenerates `queries/` against a built driver |
| `requirements.txt` | the framework, pinned by commit |

## Running it

The framework needs Python >= 3.13, which is newer than the 3.9 the wheel build
uses, so give it its own virtualenv.

```bash
python3.13 -m venv .validation-venv
.validation-venv/bin/pip install -r programs/local/adbc/validation/python/requirements.txt

cd programs/local/adbc/validation/python
TZ=UTC CHDB_ADBC_DRIVER=/path/to/libchdb.so \
  ../../../../../.validation-venv/bin/python -m pytest \
    -vvs --junit-xml=validation-report.xml -rfEsxX tests/
```

`CHDB_ADBC_DRIVER` is the library to validate — the same variable the gtest
suite reads. `CHDB_LIB_PATH` is accepted as well. With neither set, the suite
looks for `libchdb.so` and `buildlib/libchdb.so` in the checkout, then for an
installed `chdb` wheel, and skips if it finds nothing.

`TZ=UTC` matters: the timestamptz cases compare against UTC epochs.

Unlike the gtest suite, this one needs no process per test case: its fixtures
are session-scoped, so the run shares one chDB engine and never restarts it. It
takes about two seconds.

Latest run (macOS arm64, engine 26.5.1.1):
**214 passed, 27 skipped, 4 xfailed, 0 failed**. CI runs it in the same
`adbc-validation` job as the gtest suite against a downloaded release, and again
in each build workflow against the libchdb just built.

## The capability matrix

The matrix is rendered from a run, not maintained by hand: the framework reads
the JUnit XML and fills in the tables in `docs/chdb.md`. CI regenerates it on
every run and uploads it as an artifact.

```bash
../../../../../.validation-venv/bin/python -m tests.generate_documentation \
  --output /tmp/adbc-docs        # writes /tmp/adbc-docs/chdb.md
```

Reading it: a feature is green only if every case behind it passed, so `Get
Table Schema` shows ❌ for the one type that cannot work, `Time`. Skips are
declined capabilities and do not count against a feature; the reasons are in the
`Caveats` section of `docs/chdb.md`.

## How the overrides are generated

`gen_overrides.py` is local to this suite, not part of the framework. It replays
each upstream bind/ingest case through the driver, translating the standard-SQL
DDL into ClickHouse syntax (mostly wrapping column types in `Nullable`, since
ClickHouse columns are non-nullable by default), and records the round-trip
schema and values as the case's expected parts.

Recording what the driver did can enshrine a bug as the expectation, so the
script only rewrites *types*. Values stay upstream's, with two exceptions, both
engine limits and both applied by a named function: `date32` is clamped into
ClickHouse's 1900..2299 range, and non-UTF-8 binary payloads are swapped for
same-length ASCII because binary reads back as utf8. Review the diff.

```bash
cd programs/local/adbc/validation/python
CHDB_ADBC_DRIVER=/path/to/libchdb.so ../../../../../.validation-venv/bin/python gen_overrides.py
git diff queries/
```

## The engine version pin

`tests/engine_version.py` holds the engine `major.minor` the matrix above was
confirmed against, and the suite's GetInfo case compares the driver's reported
vendor version against it. On an upstream baseline sync the job stays red until
someone bumps the pin, reruns the suite, and updates this file with whatever
the new baseline actually does.
