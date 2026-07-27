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
  const cases = JSON.parse(${JSON.stringify(JSON.stringify(CASES))});
  for (const [label, sql] of cases) {
    let err = null;
    try { err = await q(sql); } catch (e) { err = e.message; }
    const cold = err && err.includes('chdb-cloudflare');
    console.log((cold ? 'COLD' : err ? 'ERR' : 'PASS') + '\\t' + label + (err ? '\\t' + err.split(String.fromCharCode(10))[0].slice(0, 100) : ''));
  }
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
