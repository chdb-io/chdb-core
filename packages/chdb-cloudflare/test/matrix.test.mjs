// Type-matrix regression test for the chdb-cloudflare bundle: every case is a
// COMMON SQL shape x column type combination that must NOT be lite-cold.
//
// Why this exists: aggregation hash tables, join maps, sorters, IN-sets and
// aggregate function states all specialize PER COLUMN TYPE, and analyzer
// rewrite passes execute their rewrite bodies only when a query matches their
// pattern. A corpus that keys everything on numbers() output leaves `GROUP BY
// name`, time-bucket dashboards and string joins cold — found the hard way
// (17 of the first 18 probes below were COLD once). When a case fails here,
// extend tools/split/profile-corpus-lite.sql (see its §6b) and re-run the
// lite pipeline.
//
//   node test/matrix.test.mjs           # uses ../dist
//   CHDB_LITE_DIR=/path node ...        # or an assembled dist

import assert from 'node:assert';
import { existsSync, readFileSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
const dir = process.env.CHDB_LITE_DIR || join(pkgDir, 'dist');
if (!existsSync(join(dir, 'chdb.wasm')) || !existsSync(join(dir, 'chdb.mjs'))) {
  if (process.env.CHDB_LITE_DIR) {
    console.error(`FAIL matrix.test: CHDB_LITE_DIR=${dir} is set but the bundle is missing`);
    process.exit(1);
  }
  console.log('SKIP matrix.test: no dist/ — run npm run build first');
  process.exit(0);
}
const glueSrc = readFileSync(join(dir, 'chdb.mjs'), 'utf8');
const jspiFlags = glueSrc.includes('WebAssembly.promising') || glueSrc.includes('WebAssembly.Suspending')
  ? ['--experimental-wasm-jspi'] : [];

// One rich-typed row source reused by the promise-audit cases below.
const RICH = "SELECT toDate('2024-01-01') + number % 3 AS d, toDateTime('2024-01-01 00:00:00') + number AS t, toDateTime64('2024-01-01 00:00:00.000', 3) + number AS t64, toDecimal64(number, 2) AS dec, toFloat32(number / 2) AS f32, if(number % 2 = 0, NULL, toString(number)) AS ns, [number, number + 1] AS arr, ['a', 'b'] AS sarr, map('k', number) AS m, toLowCardinality(toString(number % 3)) AS lc, toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS u, toIPv4(concat('10.0.0.', toString(number % 5))) AS ip FROM numbers(30)";

// [label, sql] — every query must succeed (no lite-cold, no error).
const CASES = [
  // --- GROUP BY key types (keys are subquery-materialized on purpose: the
  //     optimizer eliminates injective functions from direct GROUP BY keys) --
  ['GROUP BY String', "SELECT s, count() FROM (SELECT toString(number % 3) AS s FROM numbers(100)) GROUP BY s ORDER BY s"],
  ['GROUP BY DateTime bucket', "SELECT toStartOfInterval(toDateTime('2024-03-15 12:00:00') + number * 600, INTERVAL 15 MINUTE) AS b, count() FROM numbers(60) GROUP BY b ORDER BY b"],
  ['GROUP BY Date', "SELECT d, count() FROM (SELECT toDate('2024-01-01') + number % 5 AS d FROM numbers(100)) GROUP BY d ORDER BY d"],
  ['GROUP BY DateTime64', "SELECT v, count() FROM (SELECT toDateTime64('2024-01-01 00:00:00.000', 3) + number % 5 AS v FROM numbers(100)) GROUP BY v ORDER BY v"],
  ['GROUP BY UInt32', "SELECT k, count() FROM (SELECT toUInt32(number % 7) AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Int32', "SELECT k, count() FROM (SELECT toInt32(number % 7) - 3 AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Int16', "SELECT k, count() FROM (SELECT toInt16(number % 5) AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Int8', "SELECT k, count() FROM (SELECT toInt8(number % 5) AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Int64', "SELECT k, count() FROM (SELECT toInt64(number % 5) - 2 AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY UInt16', "SELECT k, count() FROM (SELECT toUInt16(number % 5) AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Float32', "SELECT k, count() FROM (SELECT toFloat32(number % 5) AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Float64', "SELECT f, count() FROM (SELECT round(number / 7) AS f FROM numbers(100)) GROUP BY f ORDER BY f"],
  ['GROUP BY FixedString', "SELECT fs, count() FROM (SELECT toFixedString(toString(number % 3), 4) AS fs FROM numbers(100)) GROUP BY fs ORDER BY fs"],
  ['GROUP BY Decimal64', "SELECT d, count() FROM (SELECT toDecimal64(number % 5, 2) AS d FROM numbers(100)) GROUP BY d ORDER BY d"],
  ['GROUP BY UUID', "SELECT u, count() FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 2))) AS u FROM numbers(10)) GROUP BY u ORDER BY u"],
  ['GROUP BY IPv4', "SELECT ip, count() FROM (SELECT toIPv4(concat('10.0.0.', toString(number % 5))) AS ip FROM numbers(100)) GROUP BY ip ORDER BY ip"],
  ['GROUP BY Enum', "SELECT e, count() FROM (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS e FROM numbers(10)) GROUP BY e ORDER BY e"],
  ['GROUP BY Bool', "SELECT b, count() FROM (SELECT toBool(number % 2) AS b FROM numbers(10)) GROUP BY b ORDER BY b"],
  ['GROUP BY Nullable(UInt64)', "SELECT k, count() FROM (SELECT toNullable(number % 4) AS k FROM numbers(100)) GROUP BY k ORDER BY k"],
  ['GROUP BY Nullable(String)', "SELECT s, count() FROM (SELECT if(number % 3 = 0, NULL, toString(number % 2)) AS s FROM numbers(12)) GROUP BY s ORDER BY s"],
  ['GROUP BY Nullable(Float64)', "SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, round(number / 30)) AS v FROM numbers(100)) GROUP BY v ORDER BY v"],
  ['GROUP BY Nullable(DateTime)', "SELECT t, count() FROM (SELECT if(number % 7 = 0, NULL, toDateTime('2024-01-01 00:00:00') + number % 3) AS t FROM numbers(30)) GROUP BY t ORDER BY t"],
  ['GROUP BY LowCardinality', "SELECT lc, count() FROM (SELECT toLowCardinality(toString(number % 3)) AS lc FROM numbers(100)) GROUP BY lc ORDER BY lc"],
  // --- multi-key packed widths ------------------------------------------------
  ['GROUP BY Int16+UInt8', "SELECT k, j, count() FROM (SELECT toInt16(number % 5) AS k, toUInt8(number % 3) AS j FROM numbers(100)) GROUP BY k, j ORDER BY k, j"],
  ['GROUP BY UInt32+UInt16', "SELECT k, j, count() FROM (SELECT toUInt32(number % 5) AS k, toUInt16(number % 3) AS j FROM numbers(100)) GROUP BY k, j ORDER BY k, j"],
  ['GROUP BY UInt64+UInt32', "SELECT k, j, count() FROM (SELECT number % 5 AS k, toUInt32(number % 3) AS j FROM numbers(100)) GROUP BY k, j ORDER BY k, j"],
  ['GROUP BY Date+String', "SELECT d, s, count() FROM (SELECT toDate('2024-01-01') + number % 5 AS d, toString(number % 3) AS s FROM numbers(100)) GROUP BY d, s ORDER BY d, s"],
  ['GROUP BY Date+UInt8', "SELECT d, j, count() FROM (SELECT toDate('2024-01-01') + number % 5 AS d, toUInt8(number % 3) AS j FROM numbers(100)) GROUP BY d, j ORDER BY d, j"],
  ['GROUP BY String+num', "SELECT s, n, count() FROM (SELECT toString(number % 3) AS s, number % 2 AS n FROM numbers(100)) GROUP BY s, n ORDER BY s, n"],
  ['GROUP BY two Strings', "SELECT s1, s2, count() FROM (SELECT toString(number % 3) AS s1, toString(number % 2) AS s2 FROM numbers(100)) GROUP BY s1, s2 ORDER BY s1, s2"],
  // --- JOIN key types ----------------------------------------------------------
  ['JOIN ON String', "SELECT count() FROM (SELECT toString(number % 10) AS k FROM numbers(100)) a JOIN (SELECT toString(number) AS k FROM numbers(10)) b ON a.k = b.k"],
  ['JOIN ON UInt32', "SELECT count() FROM (SELECT toUInt32(number) AS k FROM numbers(100)) a JOIN (SELECT toUInt32(number * 2) AS k FROM numbers(50)) b ON a.k = b.k"],
  ['JOIN ON Int32', "SELECT count() FROM (SELECT toInt32(number % 10) AS k FROM numbers(100)) a JOIN (SELECT toInt32(number) AS k FROM numbers(10)) b ON a.k = b.k"],
  ['JOIN ON Int64', "SELECT count() FROM (SELECT toInt64(number % 10) AS k FROM numbers(100)) a JOIN (SELECT toInt64(number) AS k FROM numbers(10)) b ON a.k = b.k"],
  ['JOIN ON Date', "SELECT count() FROM (SELECT toDate('2024-01-01') + number % 10 AS d FROM numbers(100)) a JOIN (SELECT toDate('2024-01-01') + number AS d FROM numbers(10)) b ON a.d = b.d"],
  ['JOIN ON DateTime', "SELECT count() FROM (SELECT toDateTime('2024-01-01 00:00:00') + number % 10 AS t FROM numbers(100)) a JOIN (SELECT toDateTime('2024-01-01 00:00:00') + number AS t FROM numbers(10)) b ON a.t = b.t"],
  ['JOIN ON UUID', "SELECT count() FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 3))) AS k FROM numbers(30)) a JOIN (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 3))) AS k FROM numbers(3)) b ON a.k = b.k"],
  ['JOIN ON IPv4', "SELECT count() FROM (SELECT toIPv4(concat('10.0.0.', toString(number % 5))) AS k FROM numbers(50)) a JOIN (SELECT toIPv4(concat('10.0.0.', toString(number % 5))) AS k FROM numbers(5)) b ON a.k = b.k"],
  ['JOIN two keys String+num', "SELECT count() FROM (SELECT toString(number % 3) AS s, number % 2 AS n FROM numbers(100)) a JOIN (SELECT toString(number % 3) AS s, number % 2 AS n FROM numbers(6)) b ON a.s = b.s AND a.n = b.n"],
  ['LEFT JOIN ON String', "SELECT count() FROM (SELECT toString(number % 10) AS k FROM numbers(100)) a LEFT JOIN (SELECT toString(number) AS k, number AS v FROM numbers(5)) b ON a.k = b.k"],
  // --- IN-set / DISTINCT / ORDER BY types --------------------------------------
  ['String IN set', "SELECT count() FROM (SELECT toString(number % 10) AS s FROM numbers(100)) WHERE s IN (SELECT toString(number) FROM numbers(5))"],
  ['Date IN set', "SELECT count() FROM (SELECT toDate('2024-01-01') + number % 10 AS d FROM numbers(100)) WHERE d IN (SELECT toDate('2024-01-01') + number FROM numbers(3))"],
  ['Int32 IN set', "SELECT count() FROM (SELECT toInt32(number % 10) AS k FROM numbers(100)) WHERE k IN (SELECT toInt32(number) FROM numbers(3))"],
  ['DISTINCT String', "SELECT DISTINCT toString(number % 5) FROM numbers(100) ORDER BY 1"],
  ['DISTINCT Date', "SELECT count() FROM (SELECT DISTINCT toDate('2024-01-01') + number % 3 AS v FROM numbers(30))"],
  ['DISTINCT Float64', "SELECT count() FROM (SELECT DISTINCT round(number / 7) AS v FROM numbers(100))"],
  ['ORDER BY DateTime', "SELECT t FROM (SELECT toDateTime('2024-03-15 12:00:00') + number AS t FROM numbers(100)) ORDER BY t DESC LIMIT 5"],
  ['ORDER BY String+Float', "SELECT s, f FROM (SELECT toString(number % 10) AS s, number / 3 AS f FROM numbers(100)) ORDER BY s, f DESC LIMIT 5"],
  ['ORDER BY Nullable', "SELECT v FROM (SELECT toNullable(if(number % 3 = 0, NULL, number)) AS v FROM numbers(9)) ORDER BY v DESC NULLS LAST LIMIT 3"],
  // --- aggregates x types (beyond the UInt64 defaults) --------------------------
  ['min/max DateTime', "SELECT min(t), max(t) FROM (SELECT toDateTime('2024-01-01 00:00:00') + number AS t FROM numbers(100))"],
  ['sum/avg Nullable(Float64)', "SELECT sum(v), avg(v) FROM (SELECT toNullable(number / 3) AS v FROM numbers(100))"],
  ['quantile Int32', "SELECT quantile(0.5)(i) FROM (SELECT toInt32(number) AS i FROM numbers(100))"],
  ['quantile Float32', "SELECT quantile(0.5)(v) FROM (SELECT toFloat32(number / 7) AS v FROM numbers(100))"],
  ['uniq Date/DateTime', "SELECT uniq(d), uniqExact(t) FROM (SELECT toDate('2024-01-01') + number % 5 AS d, toDateTime('2024-01-01 00:00:00') + number % 7 AS t FROM numbers(100))"],
  ['topK String', "SELECT topK(2)(s) FROM (SELECT toString(number % 3) AS s FROM numbers(100))"],
  ['argMax String by Date', "SELECT argMax(s, d) FROM (SELECT toString(number) AS s, toDate('2024-01-01') + number AS d FROM numbers(10))"],
  ['groupArray Float64', "SELECT length(groupArray(f)) FROM (SELECT number / 2 AS f FROM numbers(10))"],
  ['aggs over UUID', "SELECT uniqExact(u), any(u) FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 3))) AS u FROM numbers(30))"],
  ['aggs over IPv4', "SELECT min(ip), max(ip), uniq(ip) FROM (SELECT toIPv4(concat('10.0.0.', toString(number % 5))) AS ip FROM numbers(100))"],
  ['aggs over Enum', "SELECT min(e), max(e), uniqExact(e) FROM (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS e FROM numbers(10))"],
  // --- analyzer rewrite passes ---------------------------------------------------
  ['sum(x*const)', "SELECT sum(number * 10), max(number * 600) FROM numbers(100)"],
  ['neg-const min/max swap', "SELECT max(toInt64(number) * -2), min(toInt64(number) * -2) FROM numbers(100)"],
  ['count(*) and count(1)', "SELECT count(*), count(1) FROM numbers(100)"],
  ['sum(if) -> sumIf/countIf', "SELECT sum(if(number % 2 = 0, 1, 0)), sum(if(number % 2 = 0, number, 0)) FROM numbers(100)"],
  // --- windows / vector scalar execution ---------------------------------------
  ['window PARTITION BY String', "SELECT s, sum(f) OVER (PARTITION BY s ORDER BY f) FROM (SELECT toString(number % 3) AS s, number / 2 AS f FROM numbers(12)) ORDER BY s, f"],
  ['rank PARTITION BY Date', "SELECT d, rank() OVER (PARTITION BY d ORDER BY n) FROM (SELECT toDate('2024-01-01') + number % 2 AS d, number AS n FROM numbers(6)) ORDER BY d, n"],
  ['window sum Float64', "SELECT number, sum(number / 2) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(10)"],
  ['upper/LIKE on column', "SELECT count() FROM (SELECT upper(concat('u', toString(number))) AS s FROM numbers(100)) WHERE s LIKE 'U1%'"],
  ['formatDateTime on column', "SELECT max(formatDateTime(toDateTime('2024-03-15 12:00:00') + number, '%Y-%m-%d %H:%M')) FROM numbers(100)"],
  ['JSONExtract on column', "SELECT sum(JSONExtractInt(j, 'v')) FROM (SELECT concat('{\"v\": ', toString(number), '}') AS j FROM numbers(100))"],
  // --- promise audit: every promised feature class over rich types (format
  //     parsers/serializers, Memory-table lifecycle, ARRAY JOIN/lambdas,
  //     scalar fns over typed columns, windows/set-ops/CTE, combinators,
  //     typed file()/gzip/Parquet/Native roundtrips). 61/63 of these were
  //     COLD when the corpus leaned on UInt64/String/Float64 defaults. ---

  // -- A. format() / file() INPUT parsing per format x type
  ['A csv Date/DateTime', "SELECT d, t FROM format(CSV, 'd Date, t DateTime', '2024-03-15,2024-03-15 12:00:00')"],
  ['A csv Decimal/Nullable', "SELECT dec, n FROM format(CSV, 'dec Decimal64(2), n Nullable(Int64)', concat('12.34,', char(92), 'N'))"],
  ['A csv UUID/IPv4', "SELECT u, ip FROM format(CSV, 'u UUID, ip IPv4', '61f0c404-5cb3-11e7-907b-a6006ad3dba0,10.0.0.1')"],
  ['A csv Float32/DateTime64', "SELECT f, t FROM format(CSV, 'f Float32, t DateTime64(3)', '1.5,2024-03-15 12:00:00.123')"],
  ['A csv Array/Bool', "SELECT a, b FROM format(CSV, 'a Array(Int64), b Bool', concat(char(34), '[1,2]', char(34), ',true'))"],
  ['A csv Enum', "SELECT e, b FROM format(CSV, 'e Enum(\\'a\\' = 1, \\'b\\' = 2), b Bool', 'a,true')"],
  ['A jsonl date/arr', `SELECT d, a FROM format(JSONEachRow, 'd Date, a Array(Int64)', '{"d": "2024-03-15", "a": [1, 2]}')`],
  ['A jsonl nullable/sarr', `SELECT s, a FROM format(JSONEachRow, 's Nullable(String), a Array(String)', '{"s": null, "a": ["x"]}')`],
  ['A jsonl map/float', `SELECT m, f FROM format(JSONEachRow, 'm Map(String, Int64), f Float32', '{"m": {"k": 1}, "f": 1.5}')`],
  ['A tsv dates', "SELECT d, t FROM format(TSV, 'd Date, t DateTime', concat('2024-03-15', char(9), '2024-03-15 12:00:00'))"],
  ['A inference dates', "DESCRIBE format(CSVWithNames, concat('d,t', char(10), '2024-03-15,2024-03-15 12:00:00'))"],
  ['A inference json rich', `DESCRIBE format(JSONEachRow, '{"d": "2024-03-15", "a": [1.5], "s": "x"}')`],
  // -- B. OUTPUT serialization per format x rich types
  ['B csv rich', RICH + ' FORMAT CSV'],
  ['B tsv rich', RICH + ' FORMAT TSV'],
  ['B jsoneachrow rich', RICH + ' FORMAT JSONEachRow'],
  ['B json rich', RICH + ' FORMAT JSON'],
  ['B jsoncompact rich', RICH + ' FORMAT JSONCompact'],
  ['B pretty rich', RICH + ' FORMAT PrettyCompact'],
  ['B vertical rich', RICH + ' FORMAT Vertical'],
  ['B markdown rich', RICH + ' FORMAT Markdown'],
  ['B values rich', RICH + ' FORMAT Values'],
  ['B rowbinary rich', RICH + ' FORMAT RowBinary'],
  ['B parquet out rich', RICH + ' FORMAT Parquet'],
  // -- C. Memory tables: DDL with rich columns + INSERT VALUES literal parsing
  ['C create rich', "CREATE TABLE audit_rich (d Date, t DateTime, t64 DateTime64(3), dec Decimal64(2), f32 Float32, ns Nullable(String), arr Array(Int64), sarr Array(String), m Map(String, Int64), lc LowCardinality(String), u UUID, ip IPv4, e Enum('a' = 1, 'b' = 2), b Bool) ENGINE = Memory"],
  ['C insert values rich', "INSERT INTO audit_rich VALUES ('2024-03-15', '2024-03-15 12:00:00', '2024-03-15 12:00:00.123', 12.34, 1.5, NULL, [1, 2], ['x'], {'k': 1}, 'tag', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '10.0.0.1', 'a', true)"],
  ['C insert select rich', "INSERT INTO audit_rich SELECT toDate('2024-01-01') + number, toDateTime('2024-01-01 00:00:00') + number, toDateTime64('2024-01-01 00:00:00.000', 3) + number, toDecimal64(number, 2), toFloat32(number), if(number % 2 = 0, NULL, toString(number)), [number], [toString(number)], map('k', number), toLowCardinality(toString(number % 3)), toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))), toIPv4(concat('10.0.0.', toString(number % 5))), CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)), number % 2 = 0 FROM numbers(20)"],
  ['C select back rich', 'SELECT count(), max(d), max(dec), uniqExact(lc), max(u), max(ip) FROM audit_rich'],
  ['C where on rich', "SELECT count() FROM audit_rich WHERE d >= '2024-01-01' AND ip = toIPv4('10.0.0.1') AND e = 'a'"],
  ['C view rich', 'CREATE VIEW audit_rich_v AS SELECT d, dec, lc FROM audit_rich'],
  ['C select view rich', 'SELECT count(), max(dec) FROM audit_rich_v'],
  // -- D. ARRAY JOIN / lambdas / array fns over typed arrays
  ['D array join typed', "SELECT s FROM (SELECT ['x', 'y'] AS a) ARRAY JOIN a AS s"],
  ['D array join dates', "SELECT d FROM (SELECT [toDate('2024-01-01'), toDate('2024-01-02')] AS a) ARRAY JOIN a AS d"],
  ['D arrayMap strings', "SELECT arrayMap(x -> upper(x), ['a', 'b']), arrayMap(x -> x + 0.5, [1.0, 2.0])"],
  ['D arraySort strings/dates', "SELECT arraySort(['c', 'a']), arraySort([toDate('2024-01-02'), toDate('2024-01-01')])"],
  ['D arrayFilter dates', "SELECT arrayFilter(x -> x > toDate('2024-01-01'), [toDate('2024-01-01'), toDate('2024-01-02')])"],
  ['D groupArray dates + arrayJoin', "SELECT arrayJoin(groupArray(d)) FROM (SELECT toDate('2024-01-01') + number AS d FROM numbers(3))"],
  // -- E. scalar fns over rich COLUMN inputs
  ['E string fns over Nullable', "SELECT count() FROM (SELECT upper(ns) AS x FROM (SELECT if(number % 2 = 0, NULL, toString(number)) AS ns FROM numbers(50))) WHERE x != ''"],
  ['E string fns over LC', "SELECT max(upper(lc)), max(length(lc)) FROM (SELECT toLowCardinality(toString(number % 3)) AS lc FROM numbers(50))"],
  ['E string fns over FixedString', "SELECT max(length(fs)), max(substring(fs, 1, 2)) FROM (SELECT toFixedString(toString(number % 3), 4) AS fs FROM numbers(50))"],
  ['E date fns over DateTime64 col', "SELECT max(toHour(t)), max(toDate(t)) FROM (SELECT toDateTime64('2024-01-01 06:00:00.000', 3) + number AS t FROM numbers(50))"],
  ['E formatDateTime over Date col', "SELECT max(formatDateTime(d, '%Y-%m')) FROM (SELECT toDate('2024-01-01') + number AS d FROM numbers(50))"],
  ['E math over Decimal col', "SELECT sum(dec + dec), max(dec * 2) FROM (SELECT toDecimal64(number, 2) AS dec FROM numbers(50))"],
  ['E if over typed cols', "SELECT max(if(number % 2 = 0, d, d + 1)) FROM (SELECT number, toDate('2024-01-01') + number % 5 AS d FROM numbers(50))"],
  ['E concat mixed types', "SELECT max(concat(toString(d), '-', lc)) FROM (SELECT toDate('2024-01-01') + number % 3 AS d, toLowCardinality(toString(number % 2)) AS lc FROM numbers(50))"],
  // -- F. windows / HAVING / ROLLUP / LIMIT BY / set ops over typed columns
  ['F window over Date col', "SELECT d, lagInFrame(d, 1) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (SELECT toDate('2024-01-01') + number AS d FROM numbers(5))"],
  ['F first_value String', "SELECT first_value(s) OVER (ORDER BY s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (SELECT toString(number % 5) AS s FROM numbers(10))"],
  ['F rollup String key', "SELECT s, count() FROM (SELECT toString(number % 3) AS s FROM numbers(30)) GROUP BY s WITH ROLLUP ORDER BY s"],
  ['F having on String key', "SELECT s, count() AS c FROM (SELECT toString(number % 3) AS s FROM numbers(30)) GROUP BY s HAVING c > 5 ORDER BY s"],
  ['F limit by String', "SELECT s, number FROM (SELECT toString(number % 3) AS s, number FROM numbers(30)) ORDER BY s, number LIMIT 2 BY s"],
  ['F union dates', "SELECT d FROM (SELECT toDate('2024-01-01') + number AS d FROM numbers(3) UNION ALL SELECT toDate('2024-02-01') + number FROM numbers(3)) ORDER BY d"],
  ['F intersect strings', "SELECT s FROM (SELECT toString(number % 10) AS s FROM numbers(20) INTERSECT SELECT toString(number) FROM numbers(5)) ORDER BY s"],
  ['F cte typed', "WITH x AS (SELECT toDate('2024-01-01') + number % 3 AS d, toDecimal64(number, 2) AS dec FROM numbers(30)) SELECT d, sum(dec) FROM x GROUP BY d ORDER BY d"],
  // -- G. aggregates: combinators / merge over typed args
  ['G sumIf Decimal', "SELECT sumIf(dec, number % 2 = 0), avgIf(f, number > 5) FROM (SELECT toDecimal64(number, 2) AS dec, number / 2 AS f, number FROM numbers(50))"],
  ['G minIf/maxIf dates', "SELECT minIf(d, number > 5), maxIf(t, number < 40) FROM (SELECT toDate('2024-01-01') + number % 7 AS d, toDateTime('2024-01-01 00:00:00') + number AS t, number FROM numbers(50))"],
  ['G state/merge typed', "SELECT maxMerge(s1), uniqMerge(s2) FROM (SELECT maxState(toDate('2024-01-01') + number % 5) AS s1, uniqState(toString(number % 7)) AS s2 FROM numbers(50))"],
  ['G quantile DateTime', "SELECT quantile(0.5)(t) FROM (SELECT toDateTime('2024-01-01 00:00:00') + number AS t FROM numbers(100))"],
  ['G sumMap typed', "SELECT sumMap(map(toString(number % 3), number)) FROM numbers(30)"],
  // -- H. streaming & sessions with rich types happen via C-API; approximate:
  ['H big rich stream shape', RICH.replace('numbers(30)', 'numbers(100000)') + ' FORMAT Null'],
  // -- I. gzip file roundtrip with typed columns
  ['I gz csv typed write', "INSERT INTO FUNCTION file('/audit_typed.csv.gz', CSV) SELECT toDate('2024-01-01') + number % 3 AS d, toDecimal64(number, 2) AS dec, if(number % 2 = 0, NULL, toString(number)) AS ns FROM numbers(50)"],
  ['I gz csv typed read', "SELECT count(), max(d), max(dec) FROM file('/audit_typed.csv.gz', CSV, 'd Date, dec Decimal64(2), ns Nullable(String)')"],
  ['I parquet typed write', "INSERT INTO FUNCTION file('/audit_typed.parquet', Parquet) " + RICH],
  ['I parquet typed read', "SELECT count(), max(d), max(dec), uniqExact(lc) FROM file('/audit_typed.parquet', Parquet)"],
  ['I native typed write', "INSERT INTO FUNCTION file('/audit_typed.native', Native) " + RICH],
  ['I native typed read', "SELECT count(), max(t64), max(u) FROM file('/audit_typed.native', Native)"],
  ['audit cleanup view', 'DROP VIEW audit_rich_v'],
  ['audit cleanup table', 'DROP TABLE audit_rich'],
  // --- table functions beyond the defaults --------------------------------------
  ['tf generateSeries', 'SELECT count() FROM generateSeries(1, 100)'],
  ['tf generate_series step', 'SELECT count() FROM generate_series(0, 99, 3)'],
  ['tf zeros', 'SELECT count() FROM zeros(1000)'],
  ['tf numbers stepped', 'SELECT count() FROM numbers(0, 100, 7)'],
  ['tf null sink', "INSERT INTO FUNCTION null('x UInt64') SELECT number FROM numbers(100)"],
  ['tf merge', "SELECT count(), sum(x) FROM merge(currentDatabase(), '^matrix_m')"],
  ['tf file glob', "SELECT count() FROM file('/mx/{a,b}.csv', CSVWithNames)"],
  ['tf url headers()', 'URLHDR'],
  ['tf s3 signed (SigV4)', 'S3SIGNED'],
  ['tf url typed structure', 'URLTYPED'],
  ['tf url headers+structure', 'URLHDRTYPED'],
  ['tf s3 signed typed', 'S3TYPED'],
  ['tf glob typed structure', "SELECT count(), max(d), sum(dec) FROM file('/mx/rich{1,2}.csv', CSVWithNames, 'd Date, dec Decimal64(2), ns Nullable(String), t DateTime64(3)')"],
  ['tf null sink rich', "INSERT INTO FUNCTION null('d Date, dec Decimal64(2), a Array(String)') SELECT toDate('2024-01-01') + number, toDecimal64(number, 2), [toString(number)] FROM numbers(20)"],
];

const tmpRoot = mkdtempSync(join(tmpdir(), 'chdb-matrix-'));
process.on('exit', () => rmSync(tmpRoot, { recursive: true, force: true }));

const script = `
  import { pathToFileURL } from 'node:url';
  const factory = (await import(${JSON.stringify(pathToFileURL(join(dir, 'chdb.mjs')).href)})).default;
  const mod = await factory();
  const num = (x) => (typeof x === 'bigint' ? Number(x) : x);
  async function q(sql) {
    const r = await mod.ccall('chdb_wasm_query', 'number', ['string', 'string'], [sql, 'CSV'], { async: true });
    const errPtr = num(mod.ccall('chdb_wasm_result_error', 'number', ['number'], [r]));
    const err = errPtr ? mod.UTF8ToString(errPtr) : null;
    mod.ccall('chdb_wasm_free_result', null, ['number'], [r]);
    return err;
  }
  // fixtures: MEMFS files for the glob case, Memory tables for merge(), and a
  // same-process HTTP server (JSPI frees the event loop) for url()/s3().
  mod.FS.mkdir('/mx');
  mod.FS.writeFile('/mx/a.csv', 'id,name' + String.fromCharCode(10) + '1,x' + String.fromCharCode(10));
  mod.FS.writeFile('/mx/b.csv', 'id,name' + String.fromCharCode(10) + '2,y' + String.fromCharCode(10));
  const RICHCSV = 'd,dec,ns,t' + String.fromCharCode(10) + '2024-03-15,12.34,,2024-03-15 12:00:00.123' + String.fromCharCode(10);
  mod.FS.writeFile('/mx/rich1.csv', RICHCSV);
  mod.FS.writeFile('/mx/rich2.csv', RICHCSV);
  await q('CREATE TABLE matrix_m1 (x Int64) ENGINE = Memory');
  await q('INSERT INTO matrix_m1 VALUES (1), (2)');
  const { createServer } = await import('node:http');
  const CSVBODY = 'id,name,score' + String.fromCharCode(10) + '1,alice,9.5' + String.fromCharCode(10);
  const srv = createServer((req, res) => { res.setHeader('content-type', 'text/csv'); res.end(req.url.includes('rich') ? RICHCSV : CSVBODY); });
  await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
  const base = 'http://127.0.0.1:' + srv.address().port;

  const cases = JSON.parse(${JSON.stringify(JSON.stringify(CASES))});
  for (let [label, sql] of cases) {
    if (sql === 'URLHDR') sql = "SELECT count() FROM url('" + base + "/p.csv', CSVWithNames, headers('X-Probe' = 'matrix'))";
    if (sql === 'S3SIGNED') sql = "SELECT count(), max(score) FROM s3('" + base + "/bucket/p.csv', 'matrixkey', 'matrixsecret', CSVWithNames)";
    const RSTRUCT = String.fromCharCode(39) + 'd Date, dec Decimal64(2), ns Nullable(String), t DateTime64(3)' + String.fromCharCode(39);
    if (sql === 'URLTYPED') sql = "SELECT max(d), sum(dec), count(ns), max(t) FROM url('" + base + "/rich.csv', CSVWithNames, " + RSTRUCT + ')';
    if (sql === 'URLHDRTYPED') sql = "SELECT max(d), sum(dec) FROM url('" + base + "/rich.csv', CSVWithNames, " + RSTRUCT + ", headers('X-T' = 'v'))";
    if (sql === 'S3TYPED') sql = "SELECT max(d), sum(dec), max(t) FROM s3('" + base + "/bkt/rich.csv', 'k', 's', 'CSVWithNames', " + RSTRUCT + ')';
    let err = null;
    try { err = await q(sql); } catch (e) { err = e.message; }
    const cold = err && err.includes('chdb-cloudflare');
    console.log((cold ? 'COLD' : err ? 'ERR' : 'PASS') + '\\t' + label + (err ? '\\t' + err.split(String.fromCharCode(10))[0].slice(0, 100) : ''));
  }
  srv.close();
  // control: the lite boundary must still exist
  let ctl = 'MISSING';
  try { const e = await q("SELECT toModifiedJulianDay('2024-03-15')"); ctl = e ? 'OK' : 'MISSING'; }
  catch (e) { ctl = e.message.includes('chdb-cloudflare') ? 'OK' : 'MISSING'; }
  console.log('CONTROL\\t' + ctl);
  process.exit(0);
`;
const file = join(tmpRoot, 'matrix-probe.mjs');
writeFileSync(file, script);
const r = spawnSync(process.execPath, [...jspiFlags, file], { encoding: 'utf8', timeout: 900000 });
if (r.error || r.signal) r.stderr = `spawn: ${r.error ?? ''} signal=${r.signal ?? ''}\n${r.stderr ?? ''}`;
assert.strictEqual(r.status, 0, `matrix probe failed: ${r.stderr?.slice(-400)}`);

const lines = r.stdout.trim().split('\n').filter(Boolean);
const bad = lines.filter((l) => l.startsWith('COLD') || l.startsWith('ERR'));
for (const l of bad) console.error('FAIL ' + l);
assert.strictEqual(bad.length, 0, `${bad.length} matrix case(s) cold/broken — extend profile-corpus-lite.sql §6b and re-run the lite pipeline`);
assert.ok(lines.some((l) => l === 'CONTROL\tOK'), 'the lite-cold control must still throw (boundary intact)');
const passCount = lines.filter((l) => l.startsWith('PASS')).length;
assert.strictEqual(passCount, CASES.length, `expected ${CASES.length} PASS, got ${passCount}`);
console.log(`PASS matrix.test (${CASES.length} shape x type cases + lite-cold control)`);
